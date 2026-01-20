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

# Admin path + key
ADMIN_PATH = os.getenv("ADMIN_PATH", "admin").strip().strip("/")

# Turnstile
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY", "").strip()

# Unsubscribe signing
UNSUBSCRIBE_SECRET = os.getenv("UNSUBSCRIBE_SECRET", "").strip()

# Resend
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
EMAIL_FROM = os.getenv("EMAIL_FROM", "Probability AI Labs <welcome@problabs.net>").strip()
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net").strip()

# Public app URL (used in emails + unsubscribe links)
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://www.problabs.net").strip().rstrip("/")

# Email branding: full logo (absolute URL)
EMAIL_LOGO_URL = os.getenv(
    "EMAIL_LOGO_URL",
    "https://www.problabs.net/branding/logo-probability-ai-labs.png",
).strip()

# Nurture switches
ENABLE_NURTURE_EMAILS = os.getenv("ENABLE_NURTURE_EMAILS", "false").lower() == "true"
NURTURE_BATCH_LIMIT = int(os.getenv("NURTURE_BATCH_LIMIT", "100"))

# Rate limit (simple)
LEADS_RATE_LIMIT_PER_IP_PER_DAY = int(os.getenv("LEADS_RATE_LIMIT_PER_IP_PER_DAY", "25"))

# Resend setup
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
    event_type = Column(String, nullable=False, index=True)  # welcome/day3/day7
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

# ✅ CORS (Fixes browser preflight OPTIONS returning 405)
# Add any production domains you use here.
CORS_ORIGINS = [
    "https://www.problabs.net",
    "https://problabs.net",
    # If you ever use Vercel preview domains, you can add them explicitly.
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],   # includes OPTIONS (preflight)
    allow_headers=["*"],   # includes content-type
    expose_headers=["Content-Disposition"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _admin_api_key() -> str:
    # Read at request time (supports rotation without redeploy, depending on host)
    return (os.getenv("ADMIN_API_KEY", "") or "").strip()


def require_admin(x_admin_key: Optional[str] = Header(None)):
    key = _admin_api_key()
    if not x_admin_key or not key or x_admin_key != key:
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

    # Support both resend-python variants
    if hasattr(resend, "Emails") and hasattr(resend.Emails, "send"):
        return resend.Emails.send(payload)
    if hasattr(resend, "emails") and hasattr(resend.emails, "send"):
        return resend.emails.send(payload)

    raise RuntimeError("Unsupported resend library version: cannot find send method")


def verify_turnstile(token: str, ip: str) -> bool:
    # If secret not set, treat as disabled (dev)
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
            data = resp.json()
            return bool(data.get("success"))
    except Exception:
        return False


def build_preferences_mailto() -> str:
    subj = "Update email preferences"
    body = "Please tell us which emails you'd like to receive (welcome, product updates, tips)."
    return f"mailto:{EMAIL_REPLY_TO}?subject={subj}&body={body}"


def build_unsubscribe_url(email: str) -> str:
    if not UNSUBSCRIBE_SECRET:
        return ""
    sig = hmac.new(
        UNSUBSCRIBE_SECRET.encode("utf-8"),
        email.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{PUBLIC_APP_URL}/unsubscribe?email={email}&sig={sig}"


def _email_header_logo_html() -> str:
    # Full-logo header for emails (absolute URL)
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
      Probability AI Labs is not affiliated with the Florida Lottery. We provide analytical and educational information only and do not guarantee lottery outcomes.
    </p>
    """


def ensure_tables(db):
    # Safe to call repeatedly
    Base.metadata.create_all(bind=engine)


def record_email_event(db, email: str, event_type: str):
    db.add(EmailEvent(email=email, event_type=event_type))
    db.commit()


def is_unsubscribed(db, email: str) -> bool:
    row = db.query(EmailUnsubscribe).filter(EmailUnsubscribe.email == email).first()
    return row is not None


# =================================================
# Emails
# =================================================
def send_welcome_email(to_email: str):
    html = f"""
    {_email_header_logo_html()}
    <h1>Welcome to ProbLabs</h1>

    <p>
      Thanks for joining the waitlist. ProbLabs is developed by <strong>Probability AI Labs</strong>,
      focused on Florida Lottery daily games.
    </p>

    <p>
      We build tools and insights grounded in mathematical analysis of historical draw data—designed to provide clearer
      context for regular players.
    </p>

    <h3>What to expect</h3>
    <ul>
      <li>Florida-only focus: Fantasy 5, Pick 3, Pick 4, and Cash Pop</li>
      <li>Clear analytics (frequency, distribution, trends) without hype or guarantees</li>
      <li>Early access as we release the first tier</li>
    </ul>

    <p>
      Visit anytime: <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a>
    </p>

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


def send_day3_email(to_email: str):
    subject = "How Probability AI Labs analyzes Florida lottery data"
    html = f"""
    {_email_header_logo_html()}
    <h2>How Probability AI Labs Works</h2>

    <p>
      You joined ProbLabs, developed by <strong>Probability AI Labs</strong>, because you play Florida Lottery daily games
      and want insight grounded in data—not hype.
    </p>

    <p>
      Probability AI Labs focuses on mathematically calculated analysis of historical lottery data.
      Our goal is to help players better understand patterns, distributions, and trends that emerge over time.
    </p>

    <p>Here’s what our analysis includes:</p>
    <ul>
      <li>Mathematical evaluation of historical Florida Lottery data</li>
      <li>Frequency and distribution analysis across Fantasy 5, Pick 3, Pick 4, and Cash Pop</li>
      <li>Timing and trend observations based on long-term draw behavior</li>
    </ul>

    <p>Equally important, here’s what we do not claim:</p>
    <ul>
      <li>No guaranteed outcomes</li>
      <li>No promises of winning numbers</li>
      <li>No reliance on superstition or numerology</li>
    </ul>

    <p>
      Lottery draws are random, but the data surrounding those draws can still be examined mathematically.
      That analytical approach is the foundation of Probability AI Labs.
    </p>

    <p>
      Visit us anytime:<br />
      <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a>
    </p>

    <p>– Probability AI Labs</p>

    {_email_footer_html(to_email)}
    """

    payload = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": subject,
        "html": html,
    }
    return _resend_send(payload)


def send_day7_email(to_email: str):
    subject = "Why Probability AI Labs focuses on mathematics, not hype"
    html = f"""
    {_email_header_logo_html()}
    <h2>Why You Can Trust Probability AI Labs</h2>

    <p>
      Much of the lottery-related content online is designed to attract attention rather than provide clarity.
      Probability AI Labs was built to take a more disciplined approach.
    </p>

    <ul>
      <li>We focus exclusively on Florida Lottery daily games</li>
      <li>We work with verified historical draw data</li>
      <li>We apply mathematical analysis to study distributions and trends over time</li>
    </ul>

    <p>
      While lottery outcomes are random, probability theory and statistical analysis can still provide meaningful context—
      especially for players who participate consistently.
    </p>

    <p>
      We do not guarantee outcomes. We develop tools and insights that help users understand the data behind the games
      they choose to play.
    </p>

    <p>
      Visit us anytime:<br />
      <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a>
    </p>

    <p>– Probability AI Labs</p>

    {_email_footer_html(to_email)}
    """

    payload = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": subject,
        "html": html,
    }
    return _resend_send(payload)


# =================================================
# Nurture scheduler helpers
# =================================================
def _get_due_nurture_emails(
    db,
    now_utc: datetime,
    batch_limit: int,
) -> Tuple[List[str], List[str]]:
    """
    Returns (day3_emails, day7_emails) due to send now.
    Excludes unsubscribed and excludes already-sent events.
    """
    ensure_tables(db)

    day3_cutoff = now_utc - timedelta(days=3)
    day7_cutoff = now_utc - timedelta(days=7)

    # Day 7 due
    day7_rows = db.execute(
        text(
            """
            SELECT l.email
            FROM leads l
            LEFT JOIN email_events e
              ON e.email = l.email AND e.event_type = 'day7'
            LEFT JOIN email_unsubscribes u
              ON u.email = l.email
            WHERE l.created_at <= :cutoff
              AND e.email IS NULL
              AND u.email IS NULL
            ORDER BY l.created_at ASC
            LIMIT :lim;
            """
        ),
        {"cutoff": day7_cutoff, "lim": batch_limit},
    ).fetchall()

    # Day 3 due
    day3_rows = db.execute(
        text(
            """
            SELECT l.email
            FROM leads l
            LEFT JOIN email_events e
              ON e.email = l.email AND e.event_type = 'day3'
            LEFT JOIN email_unsubscribes u
              ON u.email = l.email
            WHERE l.created_at <= :cutoff
              AND e.email IS NULL
              AND u.email IS NULL
            ORDER BY l.created_at ASC
            LIMIT :lim;
            """
        ),
        {"cutoff": day3_cutoff, "lim": batch_limit},
    ).fetchall()

    return ([r[0] for r in day3_rows], [r[0] for r in day7_rows])


def _run_nurture_batch(db, now_utc: datetime, batch_limit: int) -> Dict[str, Any]:
    """
    Sends due nurture emails, records events. Returns counters.
    """
    ensure_tables(db)

    if not ENABLE_NURTURE_EMAILS:
        return {"enabled": False, "sent_day3": 0, "sent_day7": 0, "errors": 0}

    sent_day3 = 0
    sent_day7 = 0
    errors = 0

    day3_emails, day7_emails = _get_due_nurture_emails(db, now_utc, batch_limit)

    # Send Day 7 first (older leads)
    for email in day7_emails:
        try:
            if is_unsubscribed(db, email):
                continue
            send_day7_email(email)
            record_email_event(db, email, "day7")
            sent_day7 += 1
        except Exception:
            errors += 1

    # Then Day 3
    for email in day3_emails:
        try:
            if is_unsubscribed(db, email):
                continue
            send_day3_email(email)
            record_email_event(db, email, "day3")
            sent_day3 += 1
        except Exception:
            errors += 1

    return {"enabled": True, "sent_day3": sent_day3, "sent_day7": sent_day7, "errors": errors}


# =================================================
# Routes
# =================================================
@app.post("/leads")
def create_lead(payload: dict, request: Request, db=Depends(get_db)):
    """
    payload: { email, turnstile_token }
    """
    ensure_tables(db)

    email = normalize_email(payload.get("email", ""))
    token = (payload.get("turnstile_token") or "").strip()
    ip = request.client.host if request.client else ""

    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Invalid email")

    try:
        validate_email(email)
    except EmailNotValidError:
        raise HTTPException(status_code=400, detail="Invalid email")

    if not verify_turnstile(token, ip):
        raise HTTPException(status_code=400, detail="Verification failed")

    existing = db.query(Lead).filter(Lead.email == email).first()
    if existing:
        return {"ok": True, "inserted": False, "message": "You’re already on the waitlist."}

    db.add(Lead(email=email))
    db.commit()

    # Send welcome immediately on first insert (and record event)
    try:
        send_welcome_email(email)
        record_email_event(db, email, "welcome")
    except Exception:
        # Do not fail lead capture if email fails
        return {"ok": True, "inserted": True, "email_sent": False, "email_error": "welcome send failed"}

    return {"ok": True, "inserted": True, "email_sent": True, "email_error": None}


@app.get(f"/{ADMIN_PATH}/leads.csv", dependencies=[Depends(require_admin)])
def export_leads_csv(db=Depends(get_db)):
    ensure_tables(db)

    rows = db.execute(
        text("SELECT email, created_at FROM leads ORDER BY created_at DESC")
    ).fetchall()

    def gen():
        yield "email,created_at\n"
        for r in rows:
            yield f"{r[0]},{r[1]}\n"

    return StreamingResponse(
        gen(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="leads.csv"'},
    )


@app.get(f"/{ADMIN_PATH}/leads.json", dependencies=[Depends(require_admin)])
def export_leads_json(db=Depends(get_db)):
    ensure_tables(db)
    rows = db.execute(
        text("SELECT email, created_at FROM leads ORDER BY created_at DESC")
    ).fetchall()
    return {"ok": True, "leads": [{"email": r[0], "created_at": str(r[1])} for r in rows]}


@app.get(f"/{ADMIN_PATH}/stats", dependencies=[Depends(require_admin)])
def admin_stats(db=Depends(get_db)):
    ensure_tables(db)

    now = utcnow()
    start_today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_yesterday = start_today - timedelta(days=1)
    start_7d = start_today - timedelta(days=7)

    total = db.execute(text("SELECT COUNT(*) FROM leads")).scalar() or 0
    today = db.execute(
        text("SELECT COUNT(*) FROM leads WHERE created_at >= :s"),
        {"s": start_today},
    ).scalar() or 0
    yesterday = db.execute(
        text("SELECT COUNT(*) FROM leads WHERE created_at >= :s AND created_at < :e"),
        {"s": start_yesterday, "e": start_today},
    ).scalar() or 0
    last7d = db.execute(
        text("SELECT COUNT(*) FROM leads WHERE created_at >= :s"),
        {"s": start_7d},
    ).scalar() or 0

    return {
        "ok": True,
        "total": int(total),
        "today": int(today),
        "yesterday": int(yesterday),
        "last_7_days": int(last7d),
        "nurture_enabled": ENABLE_NURTURE_EMAILS,
        "batch_limit": NURTURE_BATCH_LIMIT,
    }


@app.post(f"/{ADMIN_PATH}/nurture/run", dependencies=[Depends(require_admin)])
def run_nurture(db=Depends(get_db)):
    ensure_tables(db)
    result = _run_nurture_batch(db, utcnow(), NURTURE_BATCH_LIMIT)
    return {"ok": True, "nurture": result}


@app.get("/unsubscribe")
def unsubscribe(email: str, sig: str, db=Depends(get_db)):
    """
    Signed unsubscribe link. Adds email to email_unsubscribes.
    """
    ensure_tables(db)

    email_norm = normalize_email(email)
    if not EMAIL_RE.match(email_norm):
        raise HTTPException(status_code=400, detail="Invalid email")

    if not UNSUBSCRIBE_SECRET:
        raise HTTPException(status_code=500, detail="Unsubscribe not configured")

    expected = hmac.new(
        UNSUBSCRIBE_SECRET.encode("utf-8"),
        email_norm.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=400, detail="Invalid signature")

    existing = db.query(EmailUnsubscribe).filter(EmailUnsubscribe.email == email_norm).first()
    if not existing:
        db.add(EmailUnsubscribe(email=email_norm))
        db.commit()

    return JSONResponse(
        {
            "ok": True,
            "message": "You have been unsubscribed. You will no longer receive emails from Probability AI Labs.",
            "email": email_norm,
        }
    )

