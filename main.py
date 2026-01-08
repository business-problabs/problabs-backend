import os
import csv
import io
import time
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from fastapi import FastAPI, HTTPException, Header, Depends, Request
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
# Environment
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")

EMAIL_FROM = os.getenv("EMAIL_FROM", "support@problabs.net")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://problabs.net")

TEST_TO_EMAIL = os.getenv("TEST_TO_EMAIL")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY

# Simple, dependency-free rate limit (per instance)
# Defaults: 5 requests/minute per IP for POST /leads
LEADS_RL_MAX = int(os.getenv("LEADS_RL_MAX", "5"))
LEADS_RL_WINDOW_SEC = int(os.getenv("LEADS_RL_WINDOW_SEC", "60"))


# -------------------------------------------------
# Basic validations
# -------------------------------------------------
if not DATABASE_URL:
    # Fail fast so Render logs show an obvious configuration error
    raise RuntimeError("DATABASE_URL not configured")


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
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
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
# Admin Auth
# -------------------------------------------------
def require_admin_key(
    x_admin_key: str = Header(default=None, alias="X-Admin-Key")
):
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")
    if not x_admin_key or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------------------------------
# Rate Limiting (simple, per-instance)
# -------------------------------------------------
# NOTE: This is in-memory and resets on deploy / restart.
# It's still useful to prevent basic spam + protect Resend during bursts.
_ip_hits: Dict[str, List[float]] = {}


def rate_limit_leads(request: Request):
    ip = (request.client.host if request.client else "unknown") or "unknown"
    now = time.time()
    window_start = now - LEADS_RL_WINDOW_SEC

    hits = _ip_hits.get(ip, [])
    # keep only hits in window
    hits = [t for t in hits if t >= window_start]

    if len(hits) >= LEADS_RL_MAX:
        raise HTTPException(
            status_code=429,
            detail=f"Too many requests. Limit is {LEADS_RL_MAX} per {LEADS_RL_WINDOW_SEC}s.",
        )

    hits.append(now)
    _ip_hits[ip] = hits


# -------------------------------------------------
# Email
# -------------------------------------------------
def _resend_send(payload: dict):
    """
    Support multiple resend python library shapes (they've varied).
    Tries a couple common call patterns.
    """
    # Ensure API key is set at send-time as well
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")

    resend.api_key = RESEND_API_KEY

    # Common patterns:
    # - resend.Emails.send({...})
    # - resend.emails.send({...})
    if hasattr(resend, "Emails") and hasattr(resend.Emails, "send"):
        return resend.Emails.send(payload)

    if hasattr(resend, "emails") and hasattr(resend.emails, "send"):
        return resend.emails.send(payload)

    raise RuntimeError("Unsupported resend library version: cannot find send method")


def send_welcome_email(to_email: str):
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

    payload = {
        "from": f"ProbLabs <{EMAIL_FROM}>",
        "to": to_email,  # resend accepts string or list depending on version; string works in many versions
        "reply_to": EMAIL_REPLY_TO,
        "subject": "Welcome to ProbLabs 🎯 You’re on the waitlist",
        "html": html,
    }

    return _resend_send(payload)


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
def create_lead(payload: LeadIn, request: Request, db=Depends(get_db), _=Depends(rate_limit_leads)):
    email = payload.email.lower().strip()

    # Insert only if new; RETURNING lets us detect whether it inserted
    result = db.execute(
        text("""
            INSERT INTO leads (email, created_at)
            VALUES (:email, NOW())
            ON CONFLICT (email) DO NOTHING
            RETURNING email
        """),
        {"email": email},
    )
    inserted_row = result.fetchone()
    inserted = inserted_row is not None

    db.commit()

    email_sent = False
    email_error: Optional[str] = None

    if inserted:
        try:
            send_welcome_email(email)
            email_sent = True
        except Exception as e:
            # Don't fail lead capture if email fails — just report status.
            email_sent = False
            email_error = str(e)

    return {
        "ok": True,
        "inserted": inserted,          # tells frontend whether it was new
        "email_sent": email_sent,      # only true when inserted + send succeeded
        "email_error": email_error,    # helps debugging without breaking signup
    }


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
    result = db.execute(text("SELECT email, created_at FROM leads ORDER BY created_at DESC"))
    rows = result.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["email", "created_at"])
    for r in rows:
        writer.writerow([r[0], r[1]])

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


# -------------------------------------------------
# Debug (Protected)
# -------------------------------------------------
@app.get("/_debug/test-email", dependencies=[Depends(require_admin_key)])
def debug_test_email():
    """
    Sends a test welcome email to TEST_TO_EMAIL and returns detailed errors
    (instead of a generic Internal Server Error).
    """
    if not TEST_TO_EMAIL:
        raise HTTPException(status_code=500, detail="TEST_TO_EMAIL not set")

    try:
        result = send_welcome_email(TEST_TO_EMAIL)
        return {"ok": True, "sent_to": TEST_TO_EMAIL, "result": result}
    except Exception as e:
        tb = traceback.format_exc()
        # Render will show this in logs, and API caller gets a clear message.
        print("EMAIL DEBUG ERROR:\n", tb)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "hint": "Check RESEND_API_KEY, verified sender domain, and EMAIL_FROM.",
                "trace": tb,
            },
        )

