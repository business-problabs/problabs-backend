# [UNCHANGED HEADER + IMPORTS]
import os
import csv
import io
import time
import json
import hmac
import hashlib
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from urllib import request as urlrequest
from urllib.parse import urlencode, quote_plus

from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import resend

# -------------------------------------------------
# App
# -------------------------------------------------
app = FastAPI(title="ProbLabs Backend", version="0.1.8")

# -------------------------------------------------
# Environment
# -------------------------------------------------
ENV = os.getenv("ENV", "production").lower()
ENABLE_DEBUG_ENDPOINTS = os.getenv("ENABLE_DEBUG_ENDPOINTS", "false").lower() == "true"

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")

EMAIL_FROM = os.getenv("EMAIL_FROM", "support@problabs.net")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://problabs.net")
BACKEND_PUBLIC_URL = os.getenv("BACKEND_PUBLIC_URL", "https://problabs-backend.onrender.com")

UNSUBSCRIBE_SECRET = os.getenv("UNSUBSCRIBE_SECRET", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
TEST_TO_EMAIL = os.getenv("TEST_TO_EMAIL")

TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY")
TURNSTILE_VERIFY_URL = os.getenv(
    "TURNSTILE_VERIFY_URL",
    "https://challenges.cloudflare.com/turnstile/v0/siteverify",
)

ADMIN_PATH = os.getenv("ADMIN_PATH", "admin").strip().strip("/")

LEADS_RL_MAX = int(os.getenv("LEADS_RL_MAX", "5"))
LEADS_RL_WINDOW_SEC = int(os.getenv("LEADS_RL_WINDOW_SEC", "60"))

ENABLE_NURTURE_EMAILS = os.getenv("ENABLE_NURTURE_EMAILS", "false").lower() == "true"
NURTURE_BATCH_LIMIT = int(os.getenv("NURTURE_BATCH_LIMIT", "25"))
NURTURE_SEND_DELAY_SEC = float(os.getenv("NURTURE_SEND_DELAY_SEC", "0.6"))

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY

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
# Email footer (UPDATED DISCLAIMER ONLY)
# -------------------------------------------------
def _email_footer_html(email: str) -> str:
    """
    Footer goes at the very bottom of every email.
    Includes: preferences + unsubscribe (signed) + 1-line disclaimer.
    """
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

