"""
Magic-link authentication + JWT session management for ProbLabs.

Flow:
  1. POST /auth/magic-link  { email }          → sends sign-in email
  2. GET  /auth/callback    ?token=<jwt>        → verifies token, upserts User,
                                                  returns session JWT as JSON
  3. Frontend /api/auth/callback receives JWT, sets httpOnly cookie, redirects
  4. GET  /auth/me                              → returns current user (cookie auth)
  5. POST /auth/logout                          → clears session cookie

Required env vars (add to .env):
  JWT_SECRET                – secret used to sign both magic-link and session tokens
  MAGIC_LINK_EXPIRY_MINUTES – default 15
  SESSION_EXPIRY_DAYS       – default 30
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
import resend as _resend
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import User

router = APIRouter(prefix="/auth", tags=["auth"])


# =============================================================================
# Config
# =============================================================================
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


JWT_SECRET          = _env("JWT_SECRET")
MAGIC_EXPIRY_MIN    = int(_env("MAGIC_LINK_EXPIRY_MINUTES", "15"))
SESSION_EXPIRY_DAYS = int(_env("SESSION_EXPIRY_DAYS", "30"))
SESSION_COOKIE      = "problabs_session"
PUBLIC_APP_URL      = _env("PUBLIC_APP_URL", "https://www.problabs.net").rstrip("/")
RESEND_API_KEY      = _env("RESEND_API_KEY")
EMAIL_FROM          = _env("EMAIL_FROM", "Probability AI Labs <welcome@problabs.net>")


# =============================================================================
# DB (sync, mirrors main.py's pattern — avoids circular import)
# =============================================================================
_engine       = None
_SessionLocal = None


def _get_sessionmaker():
    global _engine, _SessionLocal
    if _SessionLocal is None:
        url = _env("DATABASE_URL") or os.environ["DATABASE_URL"]
        _engine       = create_engine(url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
    return _SessionLocal


def get_db():
    db = _get_sessionmaker()()
    try:
        yield db
    finally:
        db.close()


# =============================================================================
# JWT helpers
# =============================================================================
def _secret() -> str:
    if not JWT_SECRET:
        raise RuntimeError("JWT_SECRET is not configured — add it to .env")
    return JWT_SECRET


def encode_magic_token(email: str) -> str:
    """Short-lived token embedded in the sign-in email link."""
    payload = {
        "sub": email,
        "typ": "magic",
        "exp": datetime.now(timezone.utc) + timedelta(minutes=MAGIC_EXPIRY_MIN),
    }
    return jwt.encode(payload, _secret(), algorithm="HS256")


def decode_magic_token(token: str) -> str:
    """Return email if valid; raise HTTPException otherwise."""
    try:
        payload = jwt.decode(token, _secret(), algorithms=["HS256"])
        if payload.get("typ") != "magic":
            raise ValueError("wrong token type")
        return payload["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=400, detail="Magic link has expired. Please request a new one.")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid magic link token.")


def encode_session_token(user_id: int, email: str) -> str:
    """Long-lived session JWT returned to the frontend."""
    payload = {
        "sub":   str(user_id),
        "email": email,
        "typ":   "session",
        "exp":   datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRY_DAYS),
    }
    return jwt.encode(payload, _secret(), algorithm="HS256")


def _decode_session_cookie(request: Request) -> Optional[dict]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    try:
        payload = jwt.decode(token, _secret(), algorithms=["HS256"])
        return payload if payload.get("typ") == "session" else None
    except Exception:
        return None


def require_session(request: Request) -> dict:
    payload = _decode_session_cookie(request)
    if not payload:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            try:
                p = jwt.decode(token, _secret(), algorithms=["HS256"])
                if p.get("typ") == "session":
                    payload = p
            except Exception:
                pass
    if not payload:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return payload


# =============================================================================
# Email
# =============================================================================
def _send_magic_link_email(to_email: str, token: str) -> None:
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY not configured")
    _resend.api_key = RESEND_API_KEY

    callback_url = f"{PUBLIC_APP_URL}/api/auth/callback?token={token}"
    html = f"""
    <div style="font-family:sans-serif;max-width:480px;margin:0 auto;">
      <p>Click the button below to sign in to <strong>Probability AI Labs</strong>.</p>
      <p style="margin:24px 0;">
        <a href="{callback_url}"
           style="background:#111827;color:#fff;padding:12px 28px;border-radius:6px;
                  text-decoration:none;font-weight:600;display:inline-block;">
          Sign in to ProbLabs
        </a>
      </p>
      <p style="color:#6b7280;font-size:13px;">
        This link expires in {MAGIC_EXPIRY_MIN} minutes.<br>
        If you did not request this, you can safely ignore this email.
      </p>
    </div>
    """
    payload = {
        "from":    EMAIL_FROM,
        "to":      [to_email],
        "subject": "Your sign-in link for Probability AI Labs",
        "html":    html,
    }
    if hasattr(_resend, "Emails"):
        _resend.Emails.send(payload)
    else:
        _resend.emails.send(payload)


# =============================================================================
# Routes
# =============================================================================
class MagicLinkRequest(BaseModel):
    email: str


@router.post("/magic-link")
def request_magic_link(body: MagicLinkRequest, db: Session = Depends(get_db)):
    """
    Send a magic-link sign-in email.
    Always returns 200 — never confirms whether the address exists.
    """
    email = body.email.strip().lower()
    token = encode_magic_token(email)
    try:
        _send_magic_link_email(email, token)
    except Exception as exc:
        print(f"[auth] magic-link send error for {email}: {type(exc).__name__}: {exc}")
    return {"ok": True, "detail": "If that address is registered, a sign-in link has been sent."}


@router.get("/callback")
def auth_callback(token: str, db: Session = Depends(get_db)):
    """
    Called by the Next.js /api/auth/callback route handler.
    Verifies the magic token, upserts the User row, and returns a session JWT.
    The frontend is responsible for setting the cookie and redirecting the user.
    """
    email = decode_magic_token(token)

    user = db.query(User).filter_by(email=email).first()
    if not user:
        user = User(email=email)
        db.add(user)

    user.last_login_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)

    session_token = encode_session_token(user.id, user.email)
    return {
        "ok":      True,
        "token":   session_token,
        "email":   user.email,
        "user_id": user.id,
    }


@router.get("/me")
def get_me(session: dict = Depends(require_session), db: Session = Depends(get_db)):
    """Return the currently authenticated user from the session cookie or Bearer token."""
    user = db.query(User).filter_by(id=int(session["sub"])).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user_id": session["sub"], "email": session["email"], "is_pro": user.is_pro}


@router.post("/logout")
def logout(response: Response):
    """Clear the session cookie."""
    response.delete_cookie(SESSION_COOKIE, httponly=True, secure=True, samesite="lax")
    return {"ok": True}
