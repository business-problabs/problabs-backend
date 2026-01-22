import os
import re
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import httpx
import resend
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func, text
from sqlalchemy.orm import sessionmaker, declarative_base
from email_validator import validate_email, EmailNotValidError


# =================================================
# Config
# =================================================
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not configured")

ADMIN_PATH = (os.getenv("ADMIN_PATH", "admin") or "admin").strip().strip("/")

TURNSTILE_SECRET_KEY = (os.getenv("TURNSTILE_SECRET_KEY") or "").strip()
UNSUBSCRIBE_SECRET = (os.getenv("UNSUBSCRIBE_SECRET") or "").strip()

RESEND_API_KEY = (os.getenv("RESEND_API_KEY") or "").strip()
EMAIL_FROM = (os.getenv("EMAIL_FROM") or "Probability AI Labs <welcome@problabs.net>").strip()
EMAIL_REPLY_TO = (os.getenv("EMAIL_REPLY_TO") or "support@problabs.net").strip()

PUBLIC_APP_URL = (os.getenv("PUBLIC_APP_URL") or "https://www.problabs.net").strip().rstrip("/")
EMAIL_LOGO_URL = (os.getenv("EMAIL_LOGO_URL") or "https://www.problabs.net/branding/logo-probability-ai-labs.png").strip()

ENABLE_NURTURE_EMAILS = (os.getenv("ENABLE_NURTURE_EMAILS") or "false").lower() == "true"
NURTURE_BATCH_LIMIT = int((os.getenv("NURTURE_BATCH_LIMIT") or "25").strip())

LEADS_RATE_LIMIT_PER_IP_PER_DAY = int((os.getenv("LEADS_RATE_LIMIT_PER_IP_PER_DAY") or "25").strip())


# =================================================
# DB
# =================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EmailEvent(Base):
    __tablename__ = "email_events"
    id = Column(Integer, primary_key=True)
    email = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)  # welcome/day3/day7
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class LeadIpEvent(Base):
    __tablename__ = "lead_ip_events"
    id = Column(Integer, primary_key=True)
    ip = Column(String, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


def ensure_tables():
    Base.metadata.create_all(bind=engine)


# =================================================
# App
# =================================================
app = FastAPI(title="ProbLabs Backend", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.problabs.net", "https://problabs.net"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# =================================================
# Admin auth
# =================================================
def _admin_api_key() -> str:
    return (os.getenv("ADMIN_API_KEY") or "").strip()


def require_admin(x_admin_key: Optional[str] = Header(None)):
    if not x_admin_key or x_admin_key != _admin_api_key():
        raise HTTPException(status_code=401, detail="Unauthorized")


# =================================================
# Helpers
# =================================================
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def verify_turnstile(token: str, ip: str) -> bool:
    if not TURNSTILE_SECRET_KEY:
        return True
    if not token:
        return False
    try:
        with httpx.Client(timeout=8) as client:
            resp = client.post(
                "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                data={"secret": TURNSTILE_SECRET_KEY, "response": token, "remoteip": ip},
            )
        return bool(resp.json().get("success"))
    except Exception:
        return False


def _resend_send(payload: dict):
    resend.api_key = RESEND_API_KEY
    if hasattr(resend, "Emails"):
        return resend.Emails.send(payload)
    return resend.emails.send(payload)


def build_unsubscribe_url(email: str) -> str:
    sig = hmac.new(
        UNSUBSCRIBE_SECRET.encode(),
        email.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{PUBLIC_APP_URL}/unsubscribe?email={email}&sig={sig}"


def _email_header_logo_html() -> str:
    return f"""
    <div style="margin-bottom:16px;">
      <img src="{EMAIL_LOGO_URL}" alt="Probability AI Labs" width="360" />
    </div>
    """


def _email_footer_html(email: str) -> str:
    unsub_url = build_unsubscribe_url(email)
    return f"""
    <hr style="margin:24px 0;" />
    <p style="font-size:12px;color:#777;">
      <a href="{unsub_url}">Unsubscribe</a>
    </p>
    <p style="font-size:12px;color:#777;">
      Probability AI Labs is not affiliated with the Florida Lottery.
      We provide analytical and educational information only.
    </p>
    """


def record_email_event(db, email: str, event_type: str):
    db.add(EmailEvent(email=email, event_type=event_type))
    db.commit()


def is_unsubscribed(db, email: str) -> bool:
    return db.query(EmailUnsubscribe).filter_by(email=email).first() is not None


# =================================================
# Emails
# =================================================
def send_welcome_email(to_email: str):
    html = f"""
    {_email_header_logo_html()}
    <p>Hello,</p>
    <p>Thanks for joining <strong>Probability AI Labs</strong>.</p>
    <p>
      We focus on Florida Lottery analysis using mathematics and historical data —
      not hype or guarantees.
    </p>
    {_email_footer_html(to_email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": "Welcome to Probability AI Labs",
        "html": html,
    })


def send_day3_email(to_email: str):
    subject = "Why we focus on math, not hype"
    html = f"""
    {_email_header_logo_html()}
    <p>Hello,</p>

    <p>You’ll notice something different about <strong>Probability AI Labs</strong>.</p>

    <p>
      We don’t use hype.<br>
      We don’t promise wins.<br>
      And we don’t claim to “predict” lottery numbers.
    </p>

    <p><strong>That’s intentional — and here’s why.</strong></p>

    <p>
      Every Florida Lottery draw is random by design.
      Math cannot change that — but it can explain how probability behaves over time.
    </p>

    <p>
      Instead of predictions, we analyze historical data to understand frequency,
      distribution, variance, and long-term patterns.
    </p>

    <p>
      This provides clarity, not false confidence.
    </p>

    <p>— Probability AI Labs</p>
    {_email_footer_html(to_email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html,
    })


def send_day7_email(to_email: str):
    subject = "How Probability AI Labs analyzes Florida lottery data"
    html = f"""
    {_email_header_logo_html()}
    <p>Hello,</p>

    <p>
      Here’s how <strong>Probability AI Labs</strong> approaches Florida Lottery analysis.
    </p>

    <p>
      We work exclusively with verified historical draw data.
      Using statistical tools, we examine frequency, distribution,
      and long-term deviations.
    </p>

    <p>
      This does not predict future outcomes.
      It helps users understand the system they are interacting with.
    </p>

    <p>
      No guarantees. No shortcuts. Just math.
    </p>

    <p>— Probability AI Labs</p>
    {_email_footer_html(to_email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html,
    })


# =================================================
# Nurture + Routes (unchanged)
# =================================================
# (rest of file continues exactly as before)

