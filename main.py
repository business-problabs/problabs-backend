import os
import csv
import io
import time
import json
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib import request as urlrequest
from urllib.parse import urlencode

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
app = FastAPI(title="ProbLabs Backend", version="0.1.3")


# -------------------------------------------------
# Environment
# -------------------------------------------------
ENV = os.getenv("ENV", "production").lower()
ENABLE_DEBUG_ENDPOINTS = os.getenv("ENABLE_DEBUG_ENDPOINTS", "false").lower() == "true"

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")

EMAIL_FROM = os.getenv("EMAIL_FROM", "support@problabs.net")  # ideally raw email
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://problabs.net")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY

TEST_TO_EMAIL = os.getenv("TEST_TO_EMAIL")

# Turnstile
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY")
TURNSTILE_VERIFY_URL = os.getenv(
    "TURNSTILE_VERIFY_URL",
    "https://challenges.cloudflare.com/turnstile/v0/siteverify",
)

# Optional: hide admin routes behind a non-obvious prefix.
# Default stays "admin" so nothing breaks unless you set it.
ADMIN_PATH = os.getenv("ADMIN_PATH", "admin").strip().strip("/")

# Simple, dependency-free rate limit (per instance)
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
    # Frontend must send Turnstile token here.
    # Example JSON: {"email":"a@b.com", "turnstile_token":"<token>"}
    turnstile_token: Optional[str] = None


# -------------------------------------------------
# Admin Auth
# -------------------------------------------------
def require_admin_key(x_admin_key: str = Header(default=None, alias="X-Admin-Key")):
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")
    if not x_admin_key or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def is_valid_admin_key(x_admin_key: Optional[str]) -> bool:
    return bool(ADMIN_API_KEY and x_admin_key and x_admin_key == ADMIN_API_KEY)


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
# Turnstile verification (backend)
# -------------------------------------------------
def verify_turnstile(token: str, remoteip: Optional[str] = None) -> dict:
    """
    Verifies a Cloudflare Turnstile token server-side.
    Uses stdlib urllib to avoid adding dependencies.
    Returns the parsed verification response dict.
    """
    if not TURNSTILE_SECRET_KEY:
        raise RuntimeError("TURNSTILE_SECRET_KEY not configured")

    data = {
        "secret": TURNSTILE_SECRET_KEY,
        "response": token,
    }
    if remoteip:
        data["remoteip"] = remoteip

    body = urlencode(data).encode("utf-8")
    req = urlrequest.Request(
        TURNSTILE_VERIFY_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    with urlrequest.urlopen(req, timeout=10) as resp:
        raw = resp.read().decode("utf-8")

    try:
        return json.loads(raw)
    except Exception:
        return {"success": False, "error-codes": ["invalid-json"], "raw": raw}


# -------------------------------------------------
# Email helpers
# -------------------------------------------------
def build_from_header(email_from_value: str) -> str:
    """
    Robust FROM:
    - If EMAIL_FROM already looks like 'Name <email@x.com>', use it as-is.
    - Else wrap: 'ProbLabs <email>'.
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
    return {"service": "ProbLabs Backend", "version": "0.1.3"}


@app.get("/db-check")
def db_check(db=Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"db": "ok"}


@app.get("/leads/count")
def leads_count(db=Depends(get_db)):
    result = db.execute(text("SELECT COUNT(*) FROM leads"))
    return {"count": result.scalar()}


@app.post("/leads")
def create_lead(
    payload: LeadIn,
    request: Request,
    db=Depends(get_db),
    _=Depends(rate_limit_leads),
    x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key"),
):
    """
    Public signup endpoint.
    Security:
      - Rate limited (per-instance)
      - Turnstile verified server-side (prevents direct bot POSTs)
      - Optional admin-key bypass for manual curl testing
    """
    email = payload.email.lower().strip()

    # Turnstile verification unless admin bypass is provided
    if not is_valid_admin_key(x_admin_key):
        token = (payload.turnstile_token or "").strip()
        if not token:
            raise HTTPException(
                status_code=400,
                detail="Missing turnstile_token. Please complete the verification and try again.",
            )

        remoteip = request.client.host if request.client else None
        result = verify_turnstile(token=token, remoteip=remoteip)

        if not result.get("success", False):
            # Avoid leaking too much detail, but include error codes for debugging
            raise HTTPException(
                status_code=403,
                detail={
                    "message": "Turnstile verification failed.",
                    "error_codes": result.get("error-codes") or result.get("error_codes"),
                },
            )

    # Insert only if new; RETURNING lets us detect whether it inserted
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
# Admin Routes (optionally hidden prefix)
# -------------------------------------------------
@app.get(f"/{ADMIN_PATH}/leads", dependencies=[Depends(require_admin_key)])
def admin_leads(db=Depends(get_db)):
    result = db.execute(text("SELECT email, created_at FROM leads ORDER BY created_at DESC"))
    return [{"email": r[0], "created_at": r[1]} for r in result.fetchall()]


@app.get(f"/{ADMIN_PATH}/leads.csv", dependencies=[Depends(require_admin_key)])
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


@app.get(f"/{ADMIN_PATH}/stats", dependencies=[Depends(require_admin_key)])
def admin_stats(db=Depends(get_db)):
    """
    Daily signup counts for the last 30 days (UTC), including days with 0 signups.
    """
    days = 30
    start_dt = datetime.utcnow().date() - timedelta(days=days - 1)
    # Use Postgres generate_series to fill in missing days with 0
    q = text(
        """
        WITH days AS (
          SELECT generate_series(:start_date::date, CURRENT_DATE::date, interval '1 day')::date AS day
        ),
        counts AS (
          SELECT created_at::date AS day, COUNT(*)::int AS cnt
          FROM leads
          WHERE created_at >= (:start_date::date)
          GROUP BY 1
        )
        SELECT d.day, COALESCE(c.cnt, 0) AS count
        FROM days d
        LEFT JOIN counts c ON c.day = d.day
        ORDER BY d.day ASC;
        """
    )

    rows = db.execute(q, {"start_date": str(start_dt)}).fetchall()

    series = [{"date": str(r[0]), "count": int(r[1])} for r in rows]
    total_30d = sum(item["count"] for item in series)

    total_all = db.execute(text("SELECT COUNT(*) FROM leads")).scalar()

    return {
        "range_days": days,
        "start_date": str(start_dt),
        "end_date": str(datetime.utcnow().date()),
        "total_30d": int(total_30d),
        "total_all": int(total_all),
        "daily": series,
    }


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
        - Returns minimal error to caller (traceback only in logs)
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

