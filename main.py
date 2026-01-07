import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import resend
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, EmailStr

app = FastAPI(title="ProbLabs Backend", version="0.1.0")

# -------------------------
# Environment variables
# -------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY", "")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "support@problabs.net")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@problabs.net")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://problabs.net")
TEST_TO_EMAIL = os.getenv("TEST_TO_EMAIL", "")

# -------------------------
# CORS
# -------------------------
origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# -------------------------
# Resend client
# -------------------------
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


# -------------------------
# Models
# -------------------------
class LeadIn(BaseModel):
    email: EmailStr


# -------------------------
# Admin auth dependency
# -------------------------
def require_admin_key(x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key")):
    # If ADMIN_API_KEY is missing, treat as misconfiguration
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not set on server")

    if not x_admin_key or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# -------------------------
# Helpers
# -------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_welcome_email_html() -> str:
    # Keep this simple + deliverability friendly
    return f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5;">
      <h2>Welcome to ProbLabs 🎯</h2>
      <p>You’re on the waitlist ✅</p>

      <p><strong>What to expect:</strong></p>
      <ul>
        <li>Florida Lottery insights (Fantasy 5, Pick 3, Pick 4, Cash Pop)</li>
        <li>Data-driven patterns and trend tracking</li>
        <li>Early access when we open the first tier</li>
      </ul>

      <p>Bookmark this: <a href="{PUBLIC_APP_URL}">{PUBLIC_APP_URL}</a></p>

      <hr />
      <p style="font-size: 12px; color: #666;">
        If you didn’t sign up, you can ignore this email.
      </p>
    </div>
    """


def send_welcome_email(to_email: str):
    if not RESEND_API_KEY:
        raise HTTPException(status_code=500, detail="RESEND_API_KEY not set")

    html = build_welcome_email_html()

    # ✅ FIX: EMAIL_FROM can be either "support@problabs.net" OR "ProbLabs <support@problabs.net>"
    from_addr = (EMAIL_FROM or "").strip()
    if "<" not in from_addr:
        from_addr = f"ProbLabs <{from_addr}>"

    payload = {
        "from": from_addr,
        "to": to_email,
        "subject": "Welcome to ProbLabs 🎯 You’re on the waitlist",
        "html": html,
    }
    if EMAIL_REPLY_TO:
        payload["reply_to"] = EMAIL_REPLY_TO

    return resend.Emails.send(payload)


# -------------------------
# Routes
# -------------------------
@app.get("/health")
def health():
    return {"ok": True, "ts": utc_now_iso()}


@app.get("/meta")
def meta():
    return {
        "service": "problabs-backend",
        "version": "0.1.0",
        "ts": utc_now_iso(),
    }


@app.get("/db-check")
def db_check():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.get("/leads/count")
def leads_count():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM leads;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.post("/leads")
def create_lead(payload: LeadIn):
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")

    email = payload.email.strip().lower()

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO leads (email, created_at)
            VALUES (%s, NOW())
            ON CONFLICT (email) DO NOTHING
            RETURNING id;
            """,
            (email,),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        # If inserted (row exists), send welcome email
        if row is not None:
            try:
                send_welcome_email(email)
            except Exception:
                # Don’t fail lead capture because email failed
                pass

        return {"ok": True, "inserted": row is not None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.get("/admin/leads", dependencies=[Depends(require_admin_key)])
def admin_leads():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT email, created_at FROM leads ORDER BY created_at DESC LIMIT 1000;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"email": r[0], "created_at": r[1].isoformat() if r[1] else None} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.get("/admin/leads.csv", dependencies=[Depends(require_admin_key)])
def admin_leads_csv():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT email, created_at FROM leads ORDER BY created_at DESC LIMIT 10000;")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        lines = ["email,created_at"]
        for email, created_at in rows:
            created = created_at.isoformat() if created_at else ""
            lines.append(f"{email},{created}")

        return PlainTextResponse("\n".join(lines), media_type="text/csv")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.get("/_debug/test-email", dependencies=[Depends(require_admin_key)])
def debug_test_email():
    if not TEST_TO_EMAIL:
        raise HTTPException(status_code=500, detail="TEST_TO_EMAIL not set")

    result = send_welcome_email(TEST_TO_EMAIL)
    return {"ok": True, "sent_to": TEST_TO_EMAIL, "result": result}

