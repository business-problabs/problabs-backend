import os
import csv
import io
import hmac
from datetime import datetime

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import resend


# -------------------------------------------------
# App
# -------------------------------------------------
app = FastAPI(title="ProbLabs Backend", version="0.1.0")


# -------------------------------------------------
# Environment (non-secret ok to cache)
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "support@problabs.net")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://problabs.net")
TEST_TO_EMAIL = os.getenv("TEST_TO_EMAIL")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


# -------------------------------------------------
# CORS
# -------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------
# Database
# -------------------------------------------------
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -------------------------------------------------
# Models
# -------------------------------------------------
class LeadIn(BaseModel):
    email: EmailStr


# -------------------------------------------------
# Admin Auth (read env at request time)
# -------------------------------------------------
def require_admin_key(
    x_admin_key: str = Header(default=None, alias="X-Admin-Key")
):
    # Read from env at request-time to avoid any caching/stale issues
    server_key = (os.getenv("ADMIN_API_KEY") or "").strip()
    client_key = (x_admin_key or "").strip()

    if not server_key:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")

    if not client_key or not hmac.compare_digest(client_key, server_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------------------------------
# Email
# -------------------------------------------------
def send_welcome_email(to_email: str):
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")

    html = f"""
    <h1>Welcome to ProbLabs 🎯</h1>
    <p>You’re on the waitlist ✅</p>

    <h3>What to expect:</h3>
    <ul>
        <li>Florida Lottery insights (Fantasy 5, Pick 3, Pick 4, Cash Pop)</li>
        <li>Data-driven patterns and trend tracking</li>
        <li>Early access when we open the first tier</li>
    </ul>

    <p>
        Bookmark us:
        <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a>
    </p>

    <p style="color:#666;font-size:12px">
        If you didn’t sign up, you can ignore this email.
    </p>
    """

    return resend.Emails.send({
        "from": f"ProbLabs <{EMAIL_FROM}>",
        "to": to_email,
        "reply_to": EMAIL_REPLY_TO,
        "subject": "Welcome to ProbLabs 🎯 You’re on the waitlist",
        "html": html,
    })


# -------------------------------------------------
# Public Routes
# -------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/meta")
def meta():
    return {"service": "ProbLabs Backend", "version": "0.1.0"}


@app.get("/db-check")
def db_check(db=Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"db": "ok"}


@app.get("/leads/count")
def leads_count(db=Depends(get_db)):
    result = db.execute(text("SELECT COUNT(*) FROM leads"))
    return {"count": result.scalar()}


@app.post("/leads")
def create_lead(payload: LeadIn, db=Depends(get_db)):
    email = payload.email.lower().strip()

    db.execute(
        text("""
            INSERT INTO leads (email, created_at)
            VALUES (:email, NOW())
            ON CONFLICT (email) DO NOTHING
        """),
        {"email": email},
    )
    db.commit()

    try:
        send_welcome_email(email)
        email_sent = True
    except Exception:
        email_sent = False

    return {"ok": True, "email_sent": email_sent}


# -------------------------------------------------
# Admin Routes
# -------------------------------------------------
@app.get("/admin/leads", dependencies=[Depends(require_admin_key)])
def admin_leads(db=Depends(get_db)):
    result = db.execute(
        text("SELECT email, created_at FROM leads ORDER BY created_at DESC")
    )
    return [{"email": r[0], "created_at": r[1]} for r in result.fetchall()]


@app.get("/admin/leads.csv", dependencies=[Depends(require_admin_key)])
def admin_leads_csv(db=Depends(get_db)):
    result = db.execute(
        text("SELECT email, created_at FROM leads ORDER BY created_at DESC")
    )

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["email", "created_at"])

    for row in result.fetchall():
        writer.writerow(row)

    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


# -------------------------------------------------
# Debug (Protected)
# -------------------------------------------------
@app.get("/_debug/test-email", dependencies=[Depends(require_admin_key)])
def debug_test_email():
    if not TEST_TO_EMAIL:
        raise HTTPException(status_code=500, detail="TEST_TO_EMAIL not set")

    result = send_welcome_email(TEST_TO_EMAIL)
    return {"ok": True, "sent_to": TEST_TO_EMAIL, "result": result}

