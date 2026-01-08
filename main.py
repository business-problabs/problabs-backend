import os
import csv
import io
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional

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
app = FastAPI(title="ProbLabs Backend", version="0.1.1")


# -------------------------------------------------
# Environment
# -------------------------------------------------
ENV = os.getenv("ENV", "production").lower()
ENABLE_DEBUG_ENDPOINTS = os.getenv("ENABLE_DEBUG_ENDPOINTS", "false").lower() == "true"

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")

EMAIL_FROM = os.getenv("EMAIL_FROM", "support@problabs.net")  # should be the raw email address
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
def require_admin_key(x_admin_key: str = Header(default=None, alias="X-Admin-Key")):
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")
    if not x_admin_key or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------------------------------
# Rate Limiting (simple, per-instance)
# -------------------------------------------------
_ip_hits: Dict[str, List[float]] = {}


def rate_limit_leads(request: Request):
    ip = (request.client.host if request.client else "unknown") or "unknown"
    now = time.time()
    window_start = now - LEADS_RL_WINDOW_SEC

    hits = _ip_hits.get(ip, [])
    hits = [t for t in hits if t >= window_start]

    if len(hits) >= LEADS_RL_MAX:
        raise HTTPException(
            status_code=429,
            detail=f"Too many requests. Limit is {LEADS_RL_MAX} per {LEADS_RL_WINDOW_SEC}s.",
        )

    hits.append(now)
    _ip_hits[ip] = hits


# -------------------------------------------------
# Email helpers
# -------------------------------------------------
def build_from_header(email_from_value: str) -> str:
    """
    Make FROM robust:
    - If EMAIL_FROM is already in 'Name <email@x.com>' format, use it as-is.
    - If it's only an email, wrap it as 'ProbLabs <email>'.
    """
    v = (email_from_value or "").strip()
    if "<" in v and ">" in v:
        return v
    return f"ProbLabs <{v}>"


def _resend_send(payload: dict):
    """
    Support multiple resend python library shapes (they've varied).
    Tries a couple common call patterns.
    """
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")

    resend.api_key = RESEND_API_KEY

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
        "from": build_from_header(EMAIL_FROM),
        "to": [to_email],
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
    return {"status": "ok", "env": ENV, "time": datetime.utcnow().isoformat()}


@app.get("/meta")
def meta():
    return {"service": "ProbLabs Backend", "version": "0.1.1"}


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

    result = db.execute(
        text(
            """
            INSERT INTO leads (email, created_at)
            VALUES (:email, NOW())
            ON CONFLICT (email) DO NOTHING
            RETURNING email
            """
        ),
        {"email": email},
    )
    inserted = result.fetchone() is not None
    db.commit()

    email_sent = False
    email_error: Optional[str] = None

    if inserted:
        try:
            send_welcome_email(email)
            email_sent = True
        except Exception as e:
            email_sent = False
            email_error = str(e)

    return {
        "ok": True,
        "inserted": inserted,
        "email_sent": email_sent,
        "email_error": email_error,
    }


# -------------------------------------------------
# Admin Routes
# -------------------------------------------------
@app.get("/admin/leads", dependencies=[Depends(require_admin_key)])
def admin_leads(db=Depends(get_db)):
    result = db.execute(text("SELECT email, created_at FROM leads ORDER BY created_at DESC"))
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
# Debug (Admin + explicitly enabled)
# -------------------------------------------------
if ENABLE_DEBUG_ENDPOINTS:
    @app.get("/_debug/test-email", dependencies=[Depends(require_admin_key)])
    def debug_test_email():
        """
        Sends a test welcome email to TEST_TO_EMAIL.

        Security hardening:
        - Only exists when ENABLE_DEBUG_ENDPOINTS=true
        - Still requires X-Admin-Key
        - Returns a minimal error to the caller (full traceback only in logs)
        """
        if not TEST_TO_EMAIL:
            raise HTTPException(status_code=500, detail="TEST_TO_EMAIL not set")

        try:
            result = send_welcome_email(TEST_TO_EMAIL)
            return {"ok": True, "sent_to": TEST_TO_EMAIL, "result": result}
        except Exception as e:
            tb = traceback.format_exc()
            print("EMAIL DEBUG ERROR:\n", tb)
            raise HTTPException(
                status_code=500,
                detail={
                    "error": str(e),
                    "hint": "Check RESEND_API_KEY, verified sender domain, and EMAIL_FROM.",
                },
            )

