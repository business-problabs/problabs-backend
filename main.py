import os
import re
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import httpx
import resend
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func, text
from sqlalchemy.orm import sessionmaker, declarative_base
from email_validator import validate_email, EmailNotValidError


# =================================================
# Config
# =================================================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not configured")

ADMIN_PATH = os.getenv("ADMIN_PATH", "admin").strip().strip("/")
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY", "").strip()
UNSUBSCRIBE_SECRET = os.getenv("UNSUBSCRIBE_SECRET", "").strip()

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
EMAIL_FROM = os.getenv("EMAIL_FROM", "Probability AI Labs <welcome@problabs.net>").strip()
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net").strip()

PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://www.problabs.net").strip().rstrip("/")
EMAIL_LOGO_URL = os.getenv(
    "EMAIL_LOGO_URL",
    "https://www.problabs.net/branding/logo-probability-ai-labs.png",
).strip()

ENABLE_NURTURE_EMAILS = os.getenv("ENABLE_NURTURE_EMAILS", "false").lower() == "true"
NURTURE_BATCH_LIMIT = int(os.getenv("NURTURE_BATCH_LIMIT", "100"))

LEADS_RATE_LIMIT_PER_IP_PER_DAY = int(os.getenv("LEADS_RATE_LIMIT_PER_IP_PER_DAY", "25"))

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


# =================================================
# DB
# =================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EmailEvent(Base):
    __tablename__ = "email_events"
    id = Column(Integer, primary_key=True)
    email = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# =================================================
# App
# =================================================
app = FastAPI(title="ProbLabs Backend", version="0.1.8")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.problabs.net",
        "https://problabs.net",
    ],
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


def _admin_api_key() -> str:
    return (os.getenv("ADMIN_API_KEY", "") or "").strip()


def require_admin(x_admin_key: Optional[str] = Header(None)):
    if not x_admin_key or x_admin_key != _admin_api_key():
        raise HTTPException(status_code=401, detail="Unauthorized")


# =================================================
# Helpers
# =================================================
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _resend_send(payload: dict):
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")

    resend.api_key = RESEND_API_KEY

    if hasattr(resend, "Emails"):
        return resend.Emails.send(payload)
    if hasattr(resend, "emails"):
        return resend.emails.send(payload)

    raise RuntimeError("Unsupported resend library version")


def verify_turnstile(token: str, ip: str) -> bool:
    if not TURNSTILE_SECRET_KEY:
        return True
    if not token:
        return False

    try:
        with httpx.Client(timeout=8) as client:
            r = client.post(
                "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                data={"secret": TURNSTILE_SECRET_KEY, "response": token, "remoteip": ip},
            )
            return bool(r.json().get("success"))
    except Exception:
        return False


def build_unsubscribe_url(email: str) -> str:
    if not UNSUBSCRIBE_SECRET:
        return ""
    sig = hmac.new(
        UNSUBSCRIBE_SECRET.encode(),
        email.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{PUBLIC_APP_URL}/unsubscribe?email={email}&sig={sig}"


def _email_header_logo_html() -> str:
    return f"""
    <div style="margin-bottom:16px">
      <img src="{EMAIL_LOGO_URL}" alt="Probability AI Labs"
           style="max-width:360px;height:auto;display:block;" />
    </div>
    """


def _email_footer_html(email: str) -> str:
    unsub = build_unsubscribe_url(email)
    return f"""
    <hr />
    <p style="font-size:12px;color:#777">
      <a href="{unsub}">Unsubscribe</a>
    </p>
    <p style="font-size:12px;color:#777">
      Probability AI Labs is not affiliated with the Florida Lottery.
      We provide analytical and educational information only.
    </p>
    """


def ensure_tables(db):
    Base.metadata.create_all(bind=engine)


def record_email_event(db, email: str, event_type: str):
    db.add(EmailEvent(email=email, event_type=event_type))
    db.commit()


def is_unsubscribed(db, email: str) -> bool:
    return db.query(EmailUnsubscribe).filter_by(email=email).first() is not None


# =================================================
# Emails
# =================================================
def send_welcome_email(email: str):
    html = f"""
    {_email_header_logo_html()}
    <h1>Welcome to ProbLabs</h1>
    <p>You’re on the waitlist.</p>
    {_email_footer_html(email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": "Welcome to ProbLabs — You’re on the waitlist",
        "html": html,
    })


def send_day3_email(email: str):
    html = f"""
    {_email_header_logo_html()}
    <h2>How Probability AI Labs Works</h2>
    {_email_footer_html(email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": "How Probability AI Labs analyzes Florida lottery data",
        "html": html,
    })


def send_day7_email(email: str):
    html = f"""
    {_email_header_logo_html()}
    <h2>Why we focus on math, not hype</h2>
    {_email_footer_html(email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": "Why Probability AI Labs focuses on mathematics, not hype",
        "html": html,
    })


# =================================================
# Nurture
# =================================================
def _run_nurture_batch(db, now: datetime, limit: int) -> Dict[str, Any]:
    if not ENABLE_NURTURE_EMAILS:
        return {"enabled": False, "sent_day3": 0, "sent_day7": 0, "errors": 0}

    sent3 = sent7 = errors = 0

    day3 = db.execute(text(
        "SELECT email FROM leads WHERE created_at <= :c "
        "AND email NOT IN (SELECT email FROM email_events WHERE event_type='day3') "
        "LIMIT :l"
    ), {"c": now - timedelta(days=3), "l": limit}).fetchall()

    day7 = db.execute(text(
        "SELECT email FROM leads WHERE created_at <= :c "
        "AND email NOT IN (SELECT email FROM email_events WHERE event_type='day7') "
        "LIMIT :l"
    ), {"c": now - timedelta(days=7), "l": limit}).fetchall()

    for (email,) in day7:
        try:
            send_day7_email(email)
            record_email_event(db, email, "day7")
            sent7 += 1
        except Exception as ex:
            errors += 1
            print(f"[nurture-error] email={email} type={type(ex).__name__} msg={ex}")

    for (email,) in day3:
        try:
            send_day3_email(email)
            record_email_event(db, email, "day3")
            sent3 += 1
        except Exception as ex:
            errors += 1
            print(f"[nurture-error] email={email} type={type(ex).__name__} msg={ex}")

    return {"enabled": True, "sent_day3": sent3, "sent_day7": sent7, "errors": errors}


# =================================================
# Routes
# =================================================
@app.post("/leads")
def create_lead(payload: dict, request: Request, db=Depends(get_db)):
    ensure_tables(db)
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

    if db.query(Lead).filter_by(email=email).first():
        return {"ok": True, "inserted": False}

    db.add(Lead(email=email))
    db.commit()

    try:
        send_welcome_email(email)
        record_email_event(db, email, "welcome")
    except Exception:
        pass

    return {"ok": True, "inserted": True}


@app.post(f"/{ADMIN_PATH}/nurture/run", dependencies=[Depends(require_admin)])
def run_nurture(db=Depends(get_db)):
    ensure_tables(db)
    return {"ok": True, "nurture": _run_nurture_batch(db, utcnow(), NURTURE_BATCH_LIMIT)}

