import os
import csv
import io
import time
import json
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
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
app = FastAPI(title="ProbLabs Backend", version="0.1.6")


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
ADMIN_PATH = os.getenv("ADMIN_PATH", "admin").strip().strip("/")

# Simple, dependency-free rate limit (per instance)
LEADS_RL_MAX = int(os.getenv("LEADS_RL_MAX", "5"))
LEADS_RL_WINDOW_SEC = int(os.getenv("LEADS_RL_WINDOW_SEC", "60"))

# Nurture emails
ENABLE_NURTURE_EMAILS = os.getenv("ENABLE_NURTURE_EMAILS", "false").lower() == "true"
NURTURE_BATCH_LIMIT = int(os.getenv("NURTURE_BATCH_LIMIT", "25"))


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


def ensure_email_events_table(db) -> None:
    """
    Ensures email_events table exists. Safe to run multiple times.
    """
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS email_events (
              id SERIAL PRIMARY KEY,
              email TEXT NOT NULL,
              event_type TEXT NOT NULL, -- welcome | day3 | day7
              sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
              UNIQUE (email, event_type)
            );
            """
        )
    )
    db.commit()


# -------------------------------------------------
# Models
# -------------------------------------------------
class LeadIn(BaseModel):
    email: EmailStr
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
    if not TURNSTILE_SECRET_KEY:
        raise RuntimeError("TURNSTILE_SECRET_KEY not configured")

    data = {"secret": TURNSTILE_SECRET_KEY, "response": token}
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
    v = (email_from_value or "").strip()
    if "<" in v and ">" in v:
        return v
    return f"ProbLabs <{v}>"


def _resend_send(payload: dict):
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")

    resend.api_key = RESEND_API_KEY

    if hasattr(resend, "Emails") and hasattr(resend.Emails, "send"):
        return resend.Emails.send(payload)
    if hasattr(resend, "emails") and hasattr(resend.emails, "send"):
        return resend.emails.send(payload)

    raise RuntimeError("Unsupported resend library version: cannot find send method")


def _record_email_event(db, email: str, event_type: str) -> None:
    """
    Record that an email was sent. Deduped by UNIQUE(email,event_type).
    """
    db.execute(
        text(
            """
            INSERT INTO email_events (email, event_type, sent_at)
            VALUES (:email, :event_type, NOW())
            ON CONFLICT (email, event_type) DO NOTHING;
            """
        ),
        {"email": email, "event_type": event_type},
    )
    db.commit()


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


def send_day3_email(to_email: str):
    subject = "How ProbLabs analyzes Florida lottery data"
    html = f"""
    <h2>How ProbLabs Works</h2>

    <p>You signed up for ProbLabs because you play Florida daily lottery games.</p>

    <p>Here’s what we actually do (and what we don’t):</p>

    <ul>
      <li>We analyze historical Florida Lottery data for games like Fantasy 5, Pick 3, Pick 4, and Cash Pop.</li>
      <li>We look for frequency patterns, timing behaviors, and distribution shifts over time.</li>
      <li>We do <strong>not</strong> claim to predict guaranteed winning numbers.</li>
    </ul>

    <p>ProbLabs is about <strong>better awareness</strong>, not false promises.</p>

    <p>
      Bookmark us:
      <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a>
    </p>

    <p>– The ProbLabs Team</p>
    """

    payload = {
        "from": build_from_header(EMAIL_FROM),
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": subject,
        "html": html,
    }
    return _resend_send(payload)


def send_day7_email(to_email: str):
    subject = "Why ProbLabs focuses on data, not hype"
    html = f"""
    <h2>Why You Can Trust ProbLabs</h2>

    <p>Most lottery-related content online is built for clicks, not accuracy.</p>

    <p>ProbLabs takes a different approach:</p>

    <ul>
      <li>We focus on Florida daily games only.</li>
      <li>We work with real historical draw data.</li>
      <li>We avoid sensational claims and guarantees.</li>
    </ul>

    <p>Lottery games are random — but player behavior and number distributions over time can still be studied.</p>

    <p>
      Thanks for being an early supporter.<br />
      – The ProbLabs Team
    </p>

    <p>
      Visit:
      <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a>
    </p>
    """

    payload = {
        "from": build_from_header(EMAIL_FROM),
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": subject,
        "html": html,
    }
    return _resend_send(payload)


def _get_due_nurture_emails(db, now_utc: datetime, batch_limit: int) -> Tuple[List[str], List[str]]:
    """
    Returns (day3_emails, day7_emails) that are due and not yet sent.
    Uses email_events to dedupe.
    """
    ensure_email_events_table(db)

    day3_cutoff = now_utc - timedelta(days=3)
    day7_cutoff = now_utc - timedelta(days=7)

    # Due for Day 7
    day7_rows = db.execute(
        text(
            """
            SELECT l.email
            FROM leads l
            LEFT JOIN email_events e
              ON e.email = l.email AND e.event_type = 'day7'
            WHERE l.created_at <= :cutoff
              AND e.email IS NULL
            ORDER BY l.created_at ASC
            LIMIT :lim;
            """
        ),
        {"cutoff": day7_cutoff, "lim": batch_limit},
    ).fetchall()
    day7_emails = [r[0] for r in day7_rows]

    # Due for Day 3 (exclude anyone already sent day3)
    day3_rows = db.execute(
        text(
            """
            SELECT l.email
            FROM leads l
            LEFT JOIN email_events e
              ON e.email = l.email AND e.event_type = 'day3'
            WHERE l.created_at <= :cutoff
              AND e.email IS NULL
            ORDER BY l.created_at ASC
            LIMIT :lim;
            """
        ),
        {"cutoff": day3_cutoff, "lim": batch_limit},
    ).fetchall()
    day3_emails = [r[0] for r in day3_rows]

    return day3_emails, day7_emails


def process_nurture_emails(db) -> dict:
    """
    Sends due nurture emails (day3/day7). Never raises outward.
    Returns a small summary for admin visibility.
    """
    if not ENABLE_NURTURE_EMAILS:
        return {"enabled": False, "sent_day3": 0, "sent_day7": 0, "errors": 0}

    now_utc = datetime.utcnow()

    sent_day3 = 0
    sent_day7 = 0
    errors = 0

    try:
        day3_emails, day7_emails = _get_due_nurture_emails(db, now_utc, NURTURE_BATCH_LIMIT)

        # Send Day 7 first (older cohort first)
        for email in day7_emails:
            try:
                send_day7_email(email)
                _record_email_event(db, email=email, event_type="day7")
                sent_day7 += 1
            except Exception:
                errors += 1
                print("NURETURE DAY7 ERROR:\n", traceback.format_exc())

        # Then Day 3
        for email in day3_emails:
            try:
                send_day3_email(email)
                _record_email_event(db, email=email, event_type="day3")
                sent_day3 += 1
            except Exception:
                errors += 1
                print("NURETURE DAY3 ERROR:\n", traceback.format_exc())

    except Exception:
        errors += 1
        print("NURETURE PROCESS ERROR:\n", traceback.format_exc())

    return {"enabled": True, "sent_day3": sent_day3, "sent_day7": sent_day7, "errors": errors}


# -------------------------------------------------
# Public Routes
# -------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "env": ENV, "time": datetime.utcnow().isoformat()}


@app.get("/meta")
def meta():
    return {"service": "ProbLabs Backend", "version": "0.1.6"}


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
    email = payload.email.lower().strip()

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
            raise HTTPException(
                status_code=403,
                detail={
                    "message": "Turnstile verification failed.",
                    "error_codes": result.get("error-codes") or result.get("error_codes"),
                },
            )

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

            # Track welcome send (deduped)
            ensure_email_events_table(db)
            _record_email_event(db, email=email, event_type="welcome")

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
    Daily signup counts for the last 30 days, plus rollups.
    Also triggers nurture email processing (safe, kill-switchable).
    """
    try:
        days = 30
        end_day: date = datetime.utcnow().date()
        start_day: date = end_day - timedelta(days=days - 1)

        q = text(
            """
            SELECT CAST(created_at AS date) AS day, COUNT(*)::int AS cnt
            FROM leads
            WHERE created_at >= :start_ts
            GROUP BY 1
            ORDER BY 1 ASC;
            """
        )
        start_ts = datetime.combine(start_day, datetime.min.time())
        rows = db.execute(q, {"start_ts": start_ts}).fetchall()
        by_day = {str(r[0]): int(r[1]) for r in rows}

        daily = []
        total_30d = 0
        for i in range(days):
            d = start_day + timedelta(days=i)
            key = str(d)
            c = int(by_day.get(key, 0))
            total_30d += c
            daily.append({"date": key, "count": c})

        total_all = int(db.execute(text("SELECT COUNT(*) FROM leads")).scalar())

        today_key = str(end_day)
        yesterday_key = str(end_day - timedelta(days=1))
        today_count = int(by_day.get(today_key, 0))
        yesterday_count = int(by_day.get(yesterday_key, 0))

        last_7d_start = end_day - timedelta(days=6)
        last_7d_total = 0
        for i in range(7):
            d = last_7d_start + timedelta(days=i)
            last_7d_total += int(by_day.get(str(d), 0))

        avg_7d_per_day = last_7d_total / 7.0
        avg_30d_per_day = total_30d / float(days)

        # Trigger nurture processing (never breaks stats)
        nurture_summary = process_nurture_emails(db)

        return {
            "range_days": days,
            "start_date": str(start_day),
            "end_date": str(end_day),
            "total_30d": int(total_30d),
            "total_all": int(total_all),
            "today_count": int(today_count),
            "yesterday_count": int(yesterday_count),
            "last_7d_total": int(last_7d_total),
            "avg_7d_per_day": avg_7d_per_day,
            "avg_30d_per_day": avg_30d_per_day,
            "daily": daily,
            "nurture": nurture_summary,
        }

    except Exception as e:
        tb = traceback.format_exc()
        print("ADMIN STATS ERROR:\n", tb)
        raise HTTPException(status_code=500, detail={"error": str(e), "hint": "Stats failed. Check logs."})


# -------------------------------------------------
# Debug (Admin + explicitly enabled)
# -------------------------------------------------
if ENABLE_DEBUG_ENDPOINTS:
    @app.get("/_debug/test-email", dependencies=[Depends(require_admin_key)])
    def debug_test_email():
        if not TEST_TO_EMAIL:
            raise HTTPException(status_code=500, detail="TEST_TO_EMAIL not set")

        try:
            result = send_welcome_email(TEST_TO_EMAIL)
            return {"ok": True, "sent_to": TEST_TO_EMAIL, "result": result}
        except Exception:
            print("EMAIL DEBUG ERROR:\n", traceback.format_exc())
            raise HTTPException(status_code=500, detail={"error": "Email send failed. Check logs."})

