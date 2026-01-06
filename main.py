import os
import io
import csv
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import requests
import resend

from fastapi import FastAPI, HTTPException, Request, Header, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel, EmailStr, Field

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func
from sqlalchemy.orm import sessionmaker, declarative_base


# ============================================================
# App
# ============================================================

app = FastAPI(title="ProbLabs API", version="0.1.0")


# ============================================================
# CORS
# ============================================================

cors_origins = os.getenv("CORS_ORIGINS", "")
origins = [o.strip() for o in cors_origins.split(",") if o.strip()]
if not origins:
    # Safe default: allow only nothing unless explicitly set
    # If you want permissive dev mode, set CORS_ORIGINS="*"
    origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins else ["*"] if os.getenv("CORS_ORIGINS") == "*" else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Database (SQLAlchemy)
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL environment variable")

# Render Postgres often requires sslmode=require
# If your DATABASE_URL already contains sslmode, this won't hurt.
connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(320), unique=True, index=True, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    ip = Column(String(64), nullable=True)
    user_agent = Column(Text, nullable=True)
    source = Column(String(128), nullable=True)  # optional (utm, ref, etc.)
    welcome_sent_at = Column(DateTime(timezone=True), nullable=True)
    welcome_error = Column(Text, nullable=True)


Base.metadata.create_all(bind=engine)


# ============================================================
# Models
# ============================================================

class LeadIn(BaseModel):
    email: EmailStr
    # Support both "turnstileToken" from frontend and "turnstile_token"
    turnstile_token: Optional[str] = Field(default=None, alias="turnstileToken")
    source: Optional[str] = None

    class Config:
        populate_by_name = True


# ============================================================
# Helpers
# ============================================================

def get_client_ip(request: Request) -> Optional[str]:
    # Render/Proxies often provide X-Forwarded-For
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


def require_admin(x_admin_key: Optional[str]) -> None:
    expected = os.getenv("ADMIN_API_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="Missing ADMIN_API_KEY on server")
    if not x_admin_key or x_admin_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def verify_turnstile(token: Optional[str], remote_ip: Optional[str]) -> None:
    secret = os.getenv("TURNSTILE_SECRET_KEY")
    if not secret:
        # In production you should set it; but we won't hard-fail
        # because you might temporarily test without it.
        return

    if not token:
        raise HTTPException(status_code=400, detail="Missing Turnstile token")

    url = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    data = {"secret": secret, "response": token}
    if remote_ip:
        data["remoteip"] = remote_ip

    try:
        r = requests.post(url, data=data, timeout=10)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Turnstile verification failed: {e}")

    if not payload.get("success"):
        # optional error-codes available in payload.get("error-codes")
        raise HTTPException(status_code=400, detail="Turnstile verification failed")


def render_welcome_email(app_url: str) -> Dict[str, str]:
    subject = "Welcome to ProbLabs 🎯 You’re on the waitlist"
    text = (
        "Welcome to ProbLabs!\n\n"
        "You're on the waitlist ✅\n\n"
        "What to expect:\n"
        "- Florida Lottery insights (Fantasy 5, Pick 3, Pick 4, Cash Pop)\n"
        "- Data-driven patterns and trend tracking\n"
        "- Early access when we open the first tier\n\n"
        f"Bookmark: {app_url}\n\n"
        "If you didn’t sign up, you can ignore this email.\n"
    )

    html = f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111;">
      <h2 style="margin: 0 0 12px;">Welcome to ProbLabs 🎯</h2>
      <p style="margin: 0 0 12px;">You’re on the waitlist <b>✅</b></p>

      <p style="margin: 0 0 8px;"><b>What to expect:</b></p>
      <ul style="margin: 0 0 16px; padding-left: 18px;">
        <li>Florida Lottery insights (Fantasy 5, Pick 3, Pick 4, Cash Pop)</li>
        <li>Data-driven patterns and trend tracking</li>
        <li>Early access when we open the first tier</li>
      </ul>

      <p style="margin: 0 0 16px;">
        Bookmark this: <a href="{app_url}">{app_url}</a>
      </p>

      <hr style="border: none; border-top: 1px solid #eee; margin: 16px 0;" />
      <p style="font-size: 12px; color: #666; margin: 0;">
        If you didn’t sign up, you can ignore this email.
      </p>
    </div>
    """
    return {"subject": subject, "text": text, "html": html}


def send_welcome_email(to_email: str) -> Dict[str, Any]:
    api_key = os.getenv("RESEND_API_KEY")
    email_from = os.getenv("EMAIL_FROM")
    reply_to = os.getenv("EMAIL_REPLY_TO")
    app_url = os.getenv("PUBLIC_APP_URL", "https://problabs.net")

    if not api_key:
        raise RuntimeError("Missing RESEND_API_KEY")
    if not email_from:
        raise RuntimeError("Missing EMAIL_FROM")

    resend.api_key = api_key
    tpl = render_welcome_email(app_url)

    payload: Dict[str, Any] = {
        "from": email_from,
        "to": [to_email],
        "subject": tpl["subject"],
        "text": tpl["text"],
        "html": tpl["html"],
    }
    if reply_to:
        payload["reply_to"] = reply_to

    return resend.Emails.send(payload)


def mark_welcome_status(email: str, sent_at: Optional[datetime], error: Optional[str]) -> None:
    db = SessionLocal()
    try:
        lead = db.query(Lead).filter(Lead.email == email).first()
        if lead:
            lead.welcome_sent_at = sent_at
            lead.welcome_error = error
            db.commit()
    finally:
        db.close()


def background_send_welcome(email: str) -> None:
    try:
        send_welcome_email(email)
        mark_welcome_status(email, datetime.now(timezone.utc), None)
    except Exception as e:
        mark_welcome_status(email, None, str(e))


# ============================================================
# Routes
# ============================================================

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/meta")
def meta():
    return {
        "service": "problabs-backend",
        "version": "0.1.0",
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/db-check")
def db_check():
    db = SessionLocal()
    try:
        # simple query
        count = db.query(func.count(Lead.id)).scalar()
        return {"ok": True, "leads": int(count or 0)}
    finally:
        db.close()


@app.get("/leads/count")
def leads_count():
    db = SessionLocal()
    try:
        count = db.query(func.count(Lead.id)).scalar()
        return {"count": int(count or 0)}
    finally:
        db.close()


@app.post("/leads")
def create_lead(payload: LeadIn, request: Request, background: BackgroundTasks):
    ip = get_client_ip(request)
    ua = request.headers.get("user-agent")

    # Turnstile verify (if TURNSTILE_SECRET_KEY is set)
    verify_turnstile(payload.turnstile_token, ip)

    db = SessionLocal()
    try:
        existing = db.query(Lead).filter(Lead.email == str(payload.email).lower()).first()
        if existing:
            # idempotent: treat as success
            return {"ok": True, "already": True}

        lead = Lead(
            email=str(payload.email).lower(),
            ip=ip,
            user_agent=ua,
            source=payload.source,
            created_at=datetime.now(timezone.utc),
        )
        db.add(lead)
        db.commit()

    finally:
        db.close()

    # Send welcome email in background (fast response)
    background.add_task(background_send_welcome, str(payload.email).lower())

    return {"ok": True, "already": False}


@app.get("/admin/leads")
def admin_leads(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")):
    require_admin(x_admin_key)

    db = SessionLocal()
    try:
        rows = db.query(Lead).order_by(Lead.id.desc()).all()
        data = []
        for r in rows:
            data.append({
                "id": r.id,
                "email": r.email,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "ip": r.ip,
                "user_agent": r.user_agent,
                "source": r.source,
                "welcome_sent_at": r.welcome_sent_at.isoformat() if r.welcome_sent_at else None,
                "welcome_error": r.welcome_error,
            })
        return JSONResponse(content=data)
    finally:
        db.close()


@app.get("/admin/leads.csv")
def admin_leads_csv(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")):
    require_admin(x_admin_key)

    db = SessionLocal()
    try:
        rows = db.query(Lead).order_by(Lead.id.desc()).all()
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "id",
            "email",
            "created_at",
            "ip",
            "user_agent",
            "source",
            "welcome_sent_at",
            "welcome_error",
        ])

        for r in rows:
            writer.writerow([
                r.id,
                r.email,
                r.created_at.isoformat() if r.created_at else "",
                r.ip or "",
                (r.user_agent or "").replace("\n", " ").replace("\r", " "),
                r.source or "",
                r.welcome_sent_at.isoformat() if r.welcome_sent_at else "",
                (r.welcome_error or "").replace("\n", " ").replace("\r", " "),
            ])

        csv_bytes = output.getvalue().encode("utf-8")
        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="leads.csv"'},
        )
    finally:
        db.close()


# ============================================================
# Debug Route (REMOVE AFTER TESTING)
# ============================================================

@app.get("/_debug/test-email")
def debug_test_email():
    """
    Sends a test email to TEST_TO_EMAIL using Resend.
    Required env:
      - RESEND_API_KEY
      - EMAIL_FROM (format: Name <email>)
      - TEST_TO_EMAIL
    """
    to_email = os.getenv("TEST_TO_EMAIL")
    if not to_email:
        raise HTTPException(status_code=500, detail="Missing TEST_TO_EMAIL")

    try:
        result = send_welcome_email(to_email)
        return {"ok": True, "sent_to": to_email, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
