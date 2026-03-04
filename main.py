import os
import re
import hmac
import hashlib
import time
import csv
import io
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

# Load .env early (uvicorn won't load it automatically)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # If python-dotenv isn't installed, env vars just won't be loaded from .env.
    # We'll still run if env vars are set another way.
    pass

import httpx
import resend
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func, text, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from email_validator import validate_email, EmailNotValidError


# =================================================
# Config
# =================================================
def _getenv(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


DATABASE_URL = _getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not configured (set it in .env or environment)")

ADMIN_PATH = (_getenv("ADMIN_PATH", "admin") or "admin").strip().strip("/")

ADMIN_API_KEY = _getenv("ADMIN_API_KEY")  # required for admin endpoints
TURNSTILE_SECRET_KEY = _getenv("TURNSTILE_SECRET_KEY")
UNSUBSCRIBE_SECRET = _getenv("UNSUBSCRIBE_SECRET")

RESEND_API_KEY = _getenv("RESEND_API_KEY")
EMAIL_FROM = _getenv("EMAIL_FROM", "Probability AI Labs <welcome@problabs.net>")
EMAIL_REPLY_TO = _getenv("EMAIL_REPLY_TO", "support@problabs.net")

PUBLIC_APP_URL = _getenv("PUBLIC_APP_URL", "https://www.problabs.net").rstrip("/")
EMAIL_LOGO_URL = _getenv("EMAIL_LOGO_URL", "https://www.problabs.net/branding/logo-probability-ai-labs.png")

ENABLE_NURTURE_EMAILS = _getenv("ENABLE_NURTURE_EMAILS", "false").lower() == "true"
NURTURE_BATCH_LIMIT = int((_getenv("NURTURE_BATCH_LIMIT", "25") or "25"))
NURTURE_SEND_DELAY_SEC = float((_getenv("NURTURE_SEND_DELAY_SEC", "0") or "0"))

LEADS_RATE_LIMIT_PER_IP_PER_DAY = int((_getenv("LEADS_RATE_LIMIT_PER_IP_PER_DAY", "25") or "25"))


# =================================================
# DB
# =================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EmailEvent(Base):
    __tablename__ = "email_events"
    id = Column(Integer, primary_key=True)
    email = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)  # welcome/day3/day7
    # Existing schema column is `sent_at` (timestamp without time zone).
    sent_at = Column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("email", "event_type", name="email_events_email_event_type_key"),
    )


class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"
    # Existing schema uses `email` as the primary key and has no `id` column.
    email = Column(String, primary_key=True)
    # Existing schema column is `unsubscribed_at` (timestamp without time zone).
    unsubscribed_at = Column(DateTime(timezone=False), server_default=func.now(), nullable=False)


class LeadIpEvent(Base):
    __tablename__ = "lead_ip_events"
    id = Column(Integer, primary_key=True)
    ip = Column(String, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


def ensure_tables() -> None:
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# =================================================
# App
# =================================================
app = FastAPI(title="ProbLabs Backend", version="0.3.0")

# CORS: allow prod + local dev
allow_origins = [
    "https://www.problabs.net",
    "https://problabs.net",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


@app.on_event("startup")
def _startup():
    ensure_tables()


# =================================================
# Admin auth
# =================================================
def require_admin(x_admin_key: Optional[str] = Header(None)):
    # If ADMIN_API_KEY isn't set, admin endpoints should NOT be callable.
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured on server")
    if not x_admin_key or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# =================================================
# Helpers
# =================================================
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def verify_turnstile(token: str, ip: str) -> bool:
    # Local/dev convenience: if secret not configured, treat as disabled
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
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")
    resend.api_key = RESEND_API_KEY
    if hasattr(resend, "Emails"):
        return resend.Emails.send(payload)
    return resend.emails.send(payload)


def build_unsubscribe_url(email: str) -> str:
    if not UNSUBSCRIBE_SECRET:
        raise RuntimeError("UNSUBSCRIBE_SECRET not configured")
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
    # If unsubscribe secret isn't configured, still send a footer without a link
    unsub_block = ""
    if UNSUBSCRIBE_SECRET:
        unsub_url = build_unsubscribe_url(email)
        unsub_block = f'<p style="font-size:12px;color:#777;"><a href="{unsub_url}">Unsubscribe</a></p>'

    return f"""
    <hr style="margin:24px 0;" />
    {unsub_block}
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


def _get_client_ip(request: Request) -> str:
    return (
        request.headers.get("cf-connecting-ip")
        or request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        or (request.client.host if request.client else "")
        or "0.0.0.0"
    )


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
        "reply_to": EMAIL_REPLY_TO,
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
    <p>This provides clarity, not false confidence.</p>
    <p>— Probability AI Labs</p>
    {_email_footer_html(to_email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
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
    <p>No guarantees. No shortcuts. Just math.</p>
    <p>— Probability AI Labs</p>
    {_email_footer_html(to_email)}
    """
    return _resend_send({
        "from": EMAIL_FROM,
        "to": [to_email],
        "reply_to": EMAIL_REPLY_TO,
        "subject": subject,
        "html": html,
    })


# =================================================
# Public routes
# =================================================
@app.get("/")
def root():
    return {"ok": True}


@app.get("/health")
def health():
    return {"ok": True, "admin_path": ADMIN_PATH}


@app.post("/leads")
async def create_lead(request: Request, db=Depends(get_db)):
    # Parse JSON safely
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    email = normalize_email((body or {}).get("email", ""))
    token = (body or {}).get("turnstileToken", "") or ""
    ip = _get_client_ip(request)

    # Validate email
    try:
        validate_email(email, check_deliverability=False)
    except EmailNotValidError:
        raise HTTPException(status_code=400, detail="Invalid email")

    # Turnstile
    if not verify_turnstile(token, ip):
        raise HTTPException(status_code=400, detail="Turnstile verification failed")

    # Rate limit per IP/day
    today_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    count = db.query(LeadIpEvent).filter(LeadIpEvent.ip == ip, LeadIpEvent.created_at >= today_start).count()
    if count >= LEADS_RATE_LIMIT_PER_IP_PER_DAY:
        raise HTTPException(status_code=429, detail="Too many requests")

    db.add(LeadIpEvent(ip=ip))
    db.commit()

    # Insert lead (idempotent)
    existing = db.query(Lead).filter_by(email=email).first()
    if existing:
        return {"ok": True, "inserted": False, "email_sent": False, "email_error": None}

    lead = Lead(email=email)
    db.add(lead)
    db.commit()

    # Welcome email (skip if unsubscribed)
    email_sent = False
    email_error = None

    if not is_unsubscribed(db, email):
        try:
            send_welcome_email(email)
            record_email_event(db, email, "welcome")
            email_sent = True
        except Exception as ex:
            email_error = str(ex)

    return {"ok": True, "inserted": True, "email_sent": email_sent, "email_error": email_error}


# =================================================
# Lottery Results API
# =================================================
SUPPORTED_GAMES = ["pick-3", "pick-4", "pick-5", "fantasy-5", "cash-pop"]

@app.get("/api/v1/results/{game_name}/latest")
def get_latest_results(game_name: str, db=Depends(get_db)):
    """
    Dynamically fetches the latest draw results for the specified game.
    """
    if game_name not in SUPPORTED_GAMES:
        raise HTTPException(status_code=404, detail="Game not found or not supported.")

    # ---------------------------------------------------------
    # TODO: Replace this mock block with your actual SQLAlchemy query 
    # Example raw SQL approach:
    # row = db.execute(text("SELECT * FROM your_lottery_table WHERE game = :g ORDER BY date DESC LIMIT 1"), {"g": game_name}).fetchone()
    # ---------------------------------------------------------
    
    # Mock response structure until your database query is plugged in
    mock_data = {
        "game": game_name,
        "date": "2026-03-04",
        "midday": ["7", "3", "6", "0", "5"] if game_name == "pick-5" else ["-", "-", "-"],
        "evening": ["8", "7", "6", "3", "4"] if game_name == "pick-5" else ["-", "-", "-"],
        "variance": {
            "hot_digit": "7",
            "hot_rate": "14.2%",
            "cold_digit": "2",
            "cold_rate": "4.1%"
        }
    }
    
    return mock_data


@app.get("/unsubscribe")
def unsubscribe(email: str, sig: str, db=Depends(get_db)):
    if not UNSUBSCRIBE_SECRET:
        raise HTTPException(status_code=500, detail="UNSUBSCRIBE_SECRET not configured")

    email = normalize_email(email)

    expected = hmac.new(
        UNSUBSCRIBE_SECRET.encode(),
        email.encode(),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=400, detail="Invalid signature")

    # Upsert unsubscribe
    if not is_unsubscribed(db, email):
        db.add(EmailUnsubscribe(email=email))
        db.commit()

    return {
        "ok": True,
        "message": "You have been unsubscribed. You will no longer receive emails from Probability AI Labs.",
    }


# =================================================
# Nurture internals
# =================================================
def _get_due_nurture_emails(db, now_utc: datetime, batch_limit: int) -> Tuple[List[str], List[str]]:
    day3_cutoff = now_utc - timedelta(days=3)
    day7_cutoff = now_utc - timedelta(days=7)

    day3_rows = db.execute(
        text("""
        SELECT l.email
        FROM leads l
        LEFT JOIN email_events e
          ON e.email = l.email AND e.event_type = 'day3'
        LEFT JOIN email_unsubscribes u
          ON u.email = l.email
        WHERE l.created_at <= :cutoff
          AND e.id IS NULL
          AND u.email IS NULL
        ORDER BY l.created_at
        LIMIT :lim
        """),
        {"cutoff": day3_cutoff, "lim": batch_limit},
    ).fetchall()

    day7_rows = db.execute(
        text("""
        SELECT l.email
        FROM leads l
        LEFT JOIN email_events e
          ON e.email = l.email AND e.event_type = 'day7'
        LEFT JOIN email_unsubscribes u
          ON u.email = l.email
        WHERE l.created_at <= :cutoff
          AND e.id IS NULL
          AND u.email IS NULL
        ORDER BY l.created_at
        LIMIT :lim
        """),
        {"cutoff": day7_cutoff, "lim": batch_limit},
    ).fetchall()

    return ([r[0] for r in day3_rows], [r[0] for r in day7_rows])


def _run_nurture_batch(db, now_utc: datetime, batch_limit: int) -> Dict[str, Any]:
    if not ENABLE_NURTURE_EMAILS:
        return {"enabled": False, "sent_day3": 0, "sent_day7": 0, "errors": 0}

    sent_day3 = 0
    sent_day7 = 0
    errors = 0

    day3_emails, day7_emails = _get_due_nurture_emails(db, now_utc, batch_limit)

    for email in day3_emails:
        try:
            send_day3_email(email)
            record_email_event(db, email, "day3")
            sent_day3 += 1
            if NURTURE_SEND_DELAY_SEC > 0:
                time.sleep(NURTURE_SEND_DELAY_SEC)
        except Exception as ex:
            errors += 1
            print(f"[nurture-error] email={email} type={type(ex).__name__} msg={ex}")

    for email in day7_emails:
        try:
            send_day7_email(email)
            record_email_event(db, email, "day7")
            sent_day7 += 1
            if NURTURE_SEND_DELAY_SEC > 0:
                time.sleep(NURTURE_SEND_DELAY_SEC)
        except Exception as ex:
            errors += 1
            print(f"[nurture-error] email={email} type={type(ex).__name__} msg={ex}")

    return {"enabled": True, "sent_day3": sent_day3, "sent_day7": sent_day7, "errors": errors}


# =================================================
# Admin routes
# =================================================
@app.get(f"/{ADMIN_PATH}/stats", dependencies=[Depends(require_admin)])
def admin_stats(db=Depends(get_db)):
    # Minimal stats for your /admin-stats page
    leads_total = db.query(Lead).count()
    unsub_total = db.query(EmailUnsubscribe).count()
    events_total = db.query(EmailEvent).count()

    # Sent counts by type
    by_type = db.execute(text("""
        SELECT event_type, COUNT(*) as cnt
        FROM email_events
        GROUP BY event_type
        ORDER BY event_type
    """)).fetchall()

    return {
        "ok": True,
        "admin_path": ADMIN_PATH,
        "db": "sqlite" if DATABASE_URL.startswith("sqlite") else "sql",
        "counts": {
            "leads": leads_total,
            "unsubscribes": unsub_total,
            "email_events": events_total,
        },
        "email_events_by_type": {row[0]: int(row[1]) for row in by_type},
        "nurture": {
            "enabled": ENABLE_NURTURE_EMAILS,
            "batch_limit": NURTURE_BATCH_LIMIT,
            "send_delay_sec": NURTURE_SEND_DELAY_SEC,
        },
    }


@app.get(f"/{ADMIN_PATH}/leads", dependencies=[Depends(require_admin)])
def admin_list_leads(limit: int = 50, offset: int = 0, db=Depends(get_db)):
    lim = max(1, min(int(limit), 500))
    off = max(0, int(offset))

    rows = db.execute(
        text("""
        SELECT id, email, created_at
        FROM leads
        ORDER BY created_at DESC
        LIMIT :lim OFFSET :off
        """),
        {"lim": lim, "off": off},
    ).fetchall()

    return {
        "ok": True,
        "limit": lim,
        "offset": off,
        "items": [{"id": r[0], "email": r[1], "created_at": str(r[2])} for r in rows],
    }


@app.get(f"/{ADMIN_PATH}/leads.csv", dependencies=[Depends(require_admin)])
def admin_export_leads_csv(db=Depends(get_db)):
    rows = db.execute(
        text("""
        SELECT
          l.id,
          l.email,
          l.created_at,
          CASE WHEN u.email IS NULL THEN 0 ELSE 1 END AS unsubscribed,

          -- Sent flags (0/1)
          SUM(CASE WHEN e.event_type = 'welcome' THEN 1 ELSE 0 END) AS welcome_sent,
          SUM(CASE WHEN e.event_type = 'day3' THEN 1 ELSE 0 END) AS day3_sent,
          SUM(CASE WHEN e.event_type = 'day7' THEN 1 ELSE 0 END) AS day7_sent,

          -- Sent timestamps (if multiple, take earliest)
          MIN(CASE WHEN e.event_type = 'welcome' THEN e.sent_at ELSE NULL END) AS welcome_sent_at,
          MIN(CASE WHEN e.event_type = 'day3' THEN e.sent_at ELSE NULL END) AS day3_sent_at,
          MIN(CASE WHEN e.event_type = 'day7' THEN e.sent_at ELSE NULL END) AS day7_sent_at

        FROM leads l
        LEFT JOIN email_events e ON e.email = l.email
        LEFT JOIN email_unsubscribes u ON u.email = l.email
        GROUP BY l.id, l.email, l.created_at, u.email
        ORDER BY l.created_at DESC
        """)
    ).fetchall()

    now_utc = utcnow()
    day3_cutoff = now_utc - timedelta(days=3)
    day7_cutoff = now_utc - timedelta(days=7)

    buf = io.StringIO()
    w = csv.writer(buf)

    w.writerow([
        "id",
        "email",
        "created_at",
        "unsubscribed",
        "welcome_sent",
        "welcome_sent_at",
        "day3_sent",
        "day3_sent_at",
        "day7_sent",
        "day7_sent_at",
        "nurture_due_day3",
        "nurture_due_day7",
    ])

    def _as_utc(dt_val):
        if dt_val is None:
            return None
        if isinstance(dt_val, datetime):
            return dt_val if dt_val.tzinfo else dt_val.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(dt_val).replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    for r in rows:
        lead_id = r[0]
        email = r[1]
        created_at_raw = r[2]
        unsubscribed = bool(r[3])

        welcome_sent = int(r[4] or 0) > 0
        day3_sent = int(r[5] or 0) > 0
        day7_sent = int(r[6] or 0) > 0

        welcome_sent_at = _as_utc(r[7])
        day3_sent_at = _as_utc(r[8])
        day7_sent_at = _as_utc(r[9])

        created_at_dt = _as_utc(created_at_raw)

        due_day3 = False
        due_day7 = False
        if created_at_dt and not unsubscribed:
            due_day3 = (created_at_dt <= day3_cutoff) and (not day3_sent)
            due_day7 = (created_at_dt <= day7_cutoff) and (not day7_sent)

        w.writerow([
            lead_id,
            email,
            created_at_dt.isoformat() if created_at_dt else str(created_at_raw),
            "yes" if unsubscribed else "no",
            "yes" if welcome_sent else "no",
            welcome_sent_at.isoformat() if welcome_sent_at else "",
            "yes" if day3_sent else "no",
            day3_sent_at.isoformat() if day3_sent_at else "",
            "yes" if day7_sent else "no",
            day7_sent_at.isoformat() if day7_sent_at else "",
            "yes" if due_day3 else "no",
            "yes" if due_day7 else "no",
        ])

    data = buf.getvalue().encode("utf-8")
    return StreamingResponse(
        iter([data]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@app.post(f"/{ADMIN_PATH}/nurture/run", dependencies=[Depends(require_admin)])
def run_nurture(db=Depends(get_db)):
    result = _run_nurture_batch(db, utcnow(), NURTURE_BATCH_LIMIT)
    return {"ok": True, "nurture": result}
