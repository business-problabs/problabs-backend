import os
import io
import csv
from datetime import datetime, timezone
from typing import Optional, Dict, Any

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins else ["*"] if cors_origins == "*" else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Database
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

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
    created_at = Column(DateTime(timezone=True), nullable=False)
    ip = Column(String(64))
    user_agent = Column(Text)
    source = Column(String(128))
    welcome_sent_at = Column(DateTime(timezone=True))
    welcome_error = Column(Text)


Base.metadata.create_all(bind=engine)


# ============================================================
# Models
# ============================================================

class LeadIn(BaseModel):
    email: EmailStr
    turnstile_token: Optional[str] = Field(default=None, alias="turnstileToken")
    source: Optional[str] = None

    class Config:
        populate_by_name = True


# ============================================================
# Helpers
# ============================================================

def get_client_ip(request: Request) -> Optional[str]:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


def require_admin(x_admin_key: Optional[str]) -> None:
    expected = os.getenv("ADMIN_API_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")
    if not x_admin_key or x_admin_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def verify_turnstile(token: Optional[str], remote_ip: Optional[str]) -> None:
    secret = os.getenv("TURNSTILE_SECRET_KEY")
    if not secret:
        return

    if not token:
        raise HTTPException(status_code=400, detail="Missing Turnstile token")

    r = requests.post(
        "https://challenges.cloudflare.com/turnstile/v0/siteverify",
        data={"secret": secret, "response": token, "remoteip": remote_ip},
        timeout=10,
    )
    payload = r.json()

    if not payload.get("success"):
        raise HTTPException(status_code=400, detail="Turnstile verification failed")


def render_welcome_email(app_url: str) -> Dict[str, str]:
    return {
        "subject": "Welcome to ProbLabs 🎯 You’re on the waitlist",
        "text": (
            "Welcome to ProbLabs!\n\n"
            "You're on the waitlist ✅\n\n"
            "What to expect:\n"
            "- Florida Lottery insights (Fantasy 5, Pick 3, Pick 4, Cash Pop)\n"
            "- Data-driven patterns and trend tracking\n"
            "- Early access when we open the first tier\n\n"
            f"Bookmark: {app_url}\n\n"
            "If you didn’t sign up, you can ignore this email."
        ),
        "html": f"""
        <h2>Welcome to ProbLabs 🎯</h2>
        <p>You’re on the waitlist ✅</p>
        <ul>
          <li>Florida Lottery insights (Fantasy 5, Pick 3, Pick 4, Cash Pop)</li>
          <li>Data-driven patterns and trend tracking</li>
          <li>Early access when we open the first tier</li>
        </ul>
        <p><a href="{app_url}">{app_url}</a></p>
        <p style="font-size:12px;color:#666;">
          If you didn’t sign up, you can ignore this email.
        </p>
        """,
    }


def send_welcome_email(to_email: str) -> Dict[str, Any]:
    resend.api_key = os.getenv("RESEND_API_KEY")
    email_from = os.getenv("EMAIL_FROM")
    reply_to = os.getenv("EMAIL_REPLY_TO")
    app_url = os.getenv("PUBLIC_APP_URL", "https://problabs.net")

    if not resend.api_key or not email_from:
        raise RuntimeError("Email environment variables not configured")

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


def mark_welcome_status(email: str, sent_at: Optional[datetime], error: Optional[str]):
    db = SessionLocal()
    try:
        lead = db.query(Lead).filter(Lead.email == email).first()
        if lead:
            lead.welcome_sent_at = sent_at
            lead.welcome_error = error
            db.commit()
    finally:
        db.close()


def background_send_welcome(email: str):
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
    return {"service": "problabs-backend", "version": "0.1.0"}


@app.get("/db-check")
def db_check():
    db = SessionLocal()
    try:
        count = db.query(func.count(Lead.id)).scalar()
        return {"ok": True, "leads": count}
    finally:
        db.close()


@app.get("/leads/count")
def leads_count():
    db = SessionLocal()
    try:
        return {"count": db.query(func.count(Lead.id)).scalar()}
    finally:
        db.close()


@app.post("/leads")
def create_lead(payload: LeadIn, request: Request, background: BackgroundTasks):
    ip = get_client_ip(request)
    ua = request.headers.get("user-agent")

    verify_turnstile(payload.turnstile_token, ip)

    db = SessionLocal()
    try:
        existing = db.query(Lead).filter(Lead.email == payload.email.lower()).first()
        if existing:
            return {"ok": True, "already": True}

        lead = Lead(
            email=payload.email.lower(),
            created_at=datetime.now(timezone.utc),
            ip=ip,
            user_agent=ua,
            source=payload.source,
        )
        db.add(lead)
        db.commit()
    finally:
        db.close()

    background.add_task(background_send_welcome, payload.email.lower())
    return {"ok": True, "already": False}


@app.get("/admin/leads")
def admin_leads(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")):
    require_admin(x_admin_key)

    db = SessionLocal()
    try:
        leads = db.query(Lead).order_by(Lead.id.desc()).all()
        return [
            {
                "id": l.id,
                "email": l.email,
                "created_at": l.created_at.isoformat(),
                "ip": l.ip,
                "user_agent": l.user_agent,
                "source": l.source,
                "welcome_sent_at": l.welcome_sent_at.isoformat() if l.welcome_sent_at else None,
                "welcome_error": l.welcome_error,
            }
            for l in leads
        ]
    finally:
        db.close()


@app.get("/admin/leads.csv")
def admin_leads_csv(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")):
    require_admin(x_admin_key)

    db = SessionLocal()
    try:
        leads = db.query(Lead).order_by(Lead.id.desc()).all()
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "id", "email", "created_at", "ip",
            "user_agent", "source",
            "welcome_sent_at", "welcome_error",
        ])

        for l in leads:
            writer.writerow([
                l.id,
                l.email,
                l.created_at.isoformat(),
                l.ip or "",
                (l.user_agent or "").replace("\n", " "),
                l.source or "",
                l.welcome_sent_at.isoformat() if l.welcome_sent_at else "",
                l.welcome_error or "",
            ])

        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=leads.csv"},
        )
    finally:
        db.close()
