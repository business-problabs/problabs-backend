"""
Public "opt in to Pro for free" endpoint.

ProbLabs Pro is currently offered at no cost. Anyone can claim it by email —
no payment, no Square checkout involved. The opt-in is tracked the same way
an admin-gifted Pro grant is (User.pro_gifted / pro_gifted_at / pro_gifted_note),
so every user who claims free Pro shows up for the admin in:
  GET /{ADMIN_PATH}/users
  and the /admin-pro dashboard on the frontend.

Flow:
  1. POST /pro/claim-free  { email }  -> upserts the User row with is_pro=True
     and pro_gifted=True, then emails a magic sign-in link.
  2. The user clicks the magic link (see api/auth.py) to actually log in to
     their now-Pro account. There is no separate "Pro password" — access is
     always via the magic-link session, same as the rest of the site.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from email_validator import validate_email, EmailNotValidError
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import User

router = APIRouter(prefix="/pro", tags=["pro"])

FREE_PRO_NOTE = "Free plan opt-in"


# =============================================================================
# DB (sync, self-contained — mirrors api/auth.py's pattern to avoid circular imports)
# =============================================================================
_engine = None
_SessionLocal = None


def _get_sessionmaker():
    global _engine, _SessionLocal
    if _SessionLocal is None:
        url = (os.getenv("DATABASE_URL") or "").strip() or os.environ["DATABASE_URL"]
        _engine = create_engine(url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
    return _SessionLocal


def get_db():
    db = _get_sessionmaker()()
    try:
        yield db
    finally:
        db.close()


class ClaimFreeProRequest(BaseModel):
    email: str


@router.post("/claim-free")
def claim_free_pro(body: ClaimFreeProRequest, db: Session = Depends(get_db)):
    """
    Public: opt in to Pro at no cost.

    Creates the user if needed, grants Pro (recorded the same way as an
    admin gift so it's visible to the admin), and emails a magic sign-in
    link so the user can actually log in and use it.
    """
    email = (body.email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email is required.")
    try:
        validate_email(email, check_deliverability=False)
    except EmailNotValidError:
        raise HTTPException(status_code=400, detail="Invalid email address.")

    user = db.query(User).filter_by(email=email).first()
    if not user:
        user = User(email=email)
        db.add(user)
        db.flush()

    user.is_pro = True
    if not user.pro_gifted:
        user.pro_gifted = True
        user.pro_gifted_at = datetime.now(timezone.utc)
        user.pro_gifted_note = FREE_PRO_NOTE
    # Self-serve free opt-in is permanent — there is no Square subscription
    # backing it, so there's nothing to expire.
    user.subscription_ends_at = None
    db.commit()

    # Local import avoids a circular import between api.auth and api.pro.
    from api.auth import encode_magic_token, _send_magic_link_email

    token = encode_magic_token(email)
    try:
        _send_magic_link_email(email, token)
    except Exception as exc:
        print(f"[pro] claim-free magic-link send error for {email}: {type(exc).__name__}: {exc}")

    return {
        "ok": True,
        "detail": "You're on Pro. Check your email for a sign-in link to access your account.",
    }
