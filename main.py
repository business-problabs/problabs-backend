import os
import re
import time
import hmac
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import resend
import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func, text
from sqlalchemy.orm import sessionmaker, declarative_base
from email_validator import validate_email, EmailNotValidError

# -------------------------------------------------
# Configuration
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not configured")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
ADMIN_PATH = os.getenv("ADMIN_PATH", "admin").strip().strip("/")
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY", "")
UNSUBSCRIBE_SECRET = os.getenv("UNSUBSCRIBE_SECRET", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "Probability AI Labs <welcome@problabs.net>")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://www.problabs.net")

# Full-logo (with text) for emails (absolute URL)
EMAIL_LOGO_URL = os.getenv(
    "EMAIL_LOGO_URL",
    "https://www.problabs.net/branding/logo-probability-ai-labs.png",
)

ENABLE_NURTURE_EMAILS = os.getenv("ENABLE_NURTURE_EMAILS", "false").lower() == "true"
NURTURE_BATCH_LIMIT = int(os.getenv("NURTURE_BATCH_LIMIT", "100"))

resend.api_key = RESEND_API_KEY

# -------------------------------------------------
# Database
# -------------------------------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class EmailEvent(Base):
    __tablename__ = "email_events"

    id = Column(Integer, primary_key=True)
    email = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


# -------------------------------------------------
# App
# -------------------------------------------------
app = FastAPI(title="ProbLabs Backend", version="0.1.8")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_admin(x_admin_key: Optional[str] = Header(None)):
    if not x_admin_key or not ADMIN_API_KEY or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------------------------------
# Utilities
# -------------------------------------------------
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: str) -> str:
    return email.strip().lower()


def verify_turnstile(token: str, ip: str) -> bool:
    if not TURNSTILE_SECRET_KEY:
        return True

    with httpx.Client(timeout=5) as client:
        resp = client.post(
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            data={
                "secret": TURNSTILE_SECRET_KEY,
                "response": token,
                "remoteip": ip,
            },
        )
        data = resp.json()
        return bool(data.get("success"))


def build_unsubscribe_url(email: str) -> str:
    if not UNSUBSCRIBE_SECRET:
        return ""
    sig = hmac.new(
        UNSUBSCRIBE_SECRET.encode(),
        email.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{PUBLIC_APP_URL}/unsubscribe?email={email}&sig={sig}"


def build_preferences_mailto() -> str:
    subj = "Update email preferences"
    body = "Please let us know which emails you'd like to receive."
    return f"mailto:{EMAIL_REPLY_TO}?subject={subj}&body={body}"


def _resend_send(payload: dict):
    resend.api_key = RESEND_API_KEY
    if hasattr(resend, "Emails") and hasattr(resend.Emails, "send"):
        return resend.Emails.send(payload)
    if hasattr(resend, "emails") and hasattr(resend.emails, "send"):
        return resend.emails.send(payload)
    raise RuntimeError("Unsupported resend library version")


def _email_header_logo_html() -> str:
    # Full logo header (email-safe HTML + absolute URL)
    return f"""
    <div style="margin:0 0 16px 0;">
      <img
        src="{EMAIL_LOGO_URL}"
        alt="Probability AI Labs"
        width="360"
        style="display:block;max-width:100%;height:auto;margin:0 0 16px 0;"
      />
    </div>
    """


# -------------------------------------------------
# Email footer (UPDATED DISCLAIMER)
# -------------------------------------------------
def _email_footer_html(email: str) -> str:
    prefs = build_preferences_mailto()
    unsub_url = build_unsubscribe_url(email)

    unsub_html = (
        f'<a href="{unsub_url}">Unsubscribe</a>'
        if unsub_url
        else f'Email us to unsubscribe: <a href="mailto:{EMAIL_REPLY_TO}?subject=Unsubscribe">{EMAIL_REPLY_TO}</a>'
    )

    return f"""
    <hr style="border:none;border-top:1px solid #eee;margin:24px 0;" />
    <p style="font-size:12px;color:#777;margin:0 0 6px 0;">
      Preferences: <a href="{prefs}">Update email preferences</a> &nbsp;|&nbsp; {unsub_html}
    </p>
    <p style="font-size:12px;color:#777;margin-top:10px;">
      Probability AI Labs is not affiliated with the Florida Lottery.
      We provide analytical and educational information only and do not guarantee lottery outcomes.
    </p>
    """


# -------------------------------------------------
# Email sends
# -------------------------------------------------
def send_welcome_email(to_email: str):
    html = f"""
    {_email_header_logo_html()}
    <h1>Welcome to ProbLabs</h1>
    <p>Thanks for joining the waitlist for <strong>Probability AI Labs</strong>.</p>
    <p>Florida-only analytics. No hype. No guarantees.</p>
    <p><a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a></p>
    {_email_footer_html(to_email)}
    """

    payload = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": "Welcome to ProbLabs — You’re on the waitlist",
        "html": html,
    }
    return _resend_send(payload)


# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.post("/leads")
def create_lead(payload: dict, request: Request, db=Depends(get_db)):
    email = normalize_email(payload.get("email", ""))
    token = payload.get("turnstile_token", "")

    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Invalid email")

    try:
        validate_email(email)
    except EmailNotValidError:
        raise HTTPException(status_code=400, detail="Invalid email")

    if not verify_turnstile(token, request.client.host):
        raise HTTPException(status_code=400, detail="Verification failed")

    lead = db.query(Lead).filter_by(email=email).first()
    if lead:
        return {"ok": True, "created": False, "email": email}

    lead = Lead(email=email)
    db.add(lead)
    db.commit()

    send_welcome_email(email)

    return {"ok": True, "created": True, "email": email}


@app.get(f"/{ADMIN_PATH}/leads.csv", dependencies=[Depends(require_admin)])
def export_leads_csv(db=Depends(get_db)):
    rows = db.execute(text("SELECT email, created_at FROM leads ORDER BY created_at DESC"))

    def gen():
        yield "email,created_at\n"
        for r in rows:
            yield f"{r.email},{r.created_at}\n"

    return StreamingResponse(gen(), media_type="text/csv")

