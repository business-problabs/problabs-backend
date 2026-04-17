import hashlib
import hmac
import json
import os
import uuid
import base64
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from db import SessionLocal
from models import User

router = APIRouter(prefix="/square", tags=["square"])


def _getenv(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


SQUARE_ACCESS_TOKEN           = _getenv("SQUARE_ACCESS_TOKEN")
SQUARE_WEBHOOK_SIGNATURE_KEY  = _getenv("SQUARE_WEBHOOK_SIGNATURE_KEY")
SQUARE_LOCATION_ID            = _getenv("SQUARE_LOCATION_ID")
SQUARE_ENVIRONMENT            = _getenv("SQUARE_ENVIRONMENT", "sandbox")
SQUARE_SUBSCRIPTION_PLAN_ID   = _getenv("SQUARE_SUBSCRIPTION_PLAN_ID")  # set after setup-plan
PUBLIC_APP_URL                = _getenv("PUBLIC_APP_URL", "https://www.problabs.net").rstrip("/")
ADMIN_API_KEY                 = _getenv("ADMIN_API_KEY")

SQUARE_API_BASE = (
    "https://connect.squareupsandbox.com"
    if SQUARE_ENVIRONMENT == "sandbox"
    else "https://connect.squareup.com"
)


async def get_db():
    async with SessionLocal() as session:
        yield session


def _square_headers() -> dict:
    return {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Square-Version": "2024-01-18",
    }


def _verify_square_signature(payload: bytes, signature: str, url: str) -> bool:
    if not SQUARE_WEBHOOK_SIGNATURE_KEY:
        return False
    # Per Square docs: HMAC-SHA256(key, notification_url + raw_body)
    msg = url + payload.decode("utf-8")
    expected = hmac.new(
        SQUARE_WEBHOOK_SIGNATURE_KEY.encode(), msg.encode(), hashlib.sha256
    ).digest()
    return hmac.compare_digest(base64.b64encode(expected).decode(), signature)


def _parse_square_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse Square's YYYY-MM-DD date string to end-of-day UTC datetime."""
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        # Access ends at end of that day in UTC
        return d.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    except ValueError:
        return None


# =============================================================================
# Admin: create Square subscription catalog plan (run once after deploy)
# =============================================================================
@router.post("/admin/setup-plan")
async def admin_setup_plan(request: Request):
    """
    One-time setup: creates a Square Catalog subscription plan for ProbLabs Pro ($9.99/mo).
    Requires ADMIN_API_KEY header. Returns the plan_id to set as SQUARE_SUBSCRIPTION_PLAN_ID env var.
    """
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")
    key = request.headers.get("X-Admin-Key", "")
    if key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
    if not SQUARE_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Square not configured on server")

    payload = {
        "idempotency_key": str(uuid.uuid4()),
        "object": {
            "type": "SUBSCRIPTION_PLAN",
            "id": "#problabs_pro_monthly",
            "subscription_plan_data": {
                "name": "ProbLabs Pro Monthly",
                "phases": [
                    {
                        "cadence": "MONTHLY",
                        "recurring_price_money": {"amount": 999, "currency": "USD"},
                    }
                ],
            },
        },
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SQUARE_API_BASE}/v2/catalog/object",
            headers=_square_headers(),
            json=payload,
        )

    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=502, detail=f"Square error: {resp.text}")

    data = resp.json()
    plan_id = data.get("catalog_object", {}).get("id")
    return {
        "ok": True,
        "plan_id": plan_id,
        "message": f"Set SQUARE_SUBSCRIPTION_PLAN_ID={plan_id} in Render environment variables.",
    }


# =============================================================================
# Checkout — creates a Square payment link
# If SQUARE_SUBSCRIPTION_PLAN_ID is set, uses subscription plan (recurring).
# Falls back to one-time $9.99 payment if not configured.
# =============================================================================
@router.post("/checkout")
async def create_checkout(request: Request, db: AsyncSession = Depends(get_db)):
    if not SQUARE_ACCESS_TOKEN or not SQUARE_LOCATION_ID:
        raise HTTPException(status_code=500, detail="Square not configured on server")

    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email required")

    if SQUARE_SUBSCRIPTION_PLAN_ID:
        # Recurring subscription checkout
        payload = {
            "idempotency_key": str(uuid.uuid4()),
            "order": {
                "location_id": SQUARE_LOCATION_ID,
                "line_items": [
                    {
                        "name": "ProbLabs Pro",
                        "quantity": "1",
                        "base_price_money": {"amount": 999, "currency": "USD"},
                    }
                ],
            },
            "checkout_options": {
                "redirect_url": f"{PUBLIC_APP_URL}/dashboard?upgraded=true",
                "ask_for_shipping_address": False,
                "subscription_plan_id": SQUARE_SUBSCRIPTION_PLAN_ID,
            },
            "pre_populated_data": {"buyer_email": email},
        }
    else:
        # Fallback: one-time payment
        payload = {
            "idempotency_key": str(uuid.uuid4()),
            "order": {
                "location_id": SQUARE_LOCATION_ID,
                "line_items": [
                    {
                        "name": "ProbLabs Pro",
                        "quantity": "1",
                        "base_price_money": {"amount": 999, "currency": "USD"},
                    }
                ],
            },
            "checkout_options": {
                "redirect_url": f"{PUBLIC_APP_URL}/dashboard?upgraded=true",
                "ask_for_shipping_address": False,
            },
            "pre_populated_data": {"buyer_email": email},
        }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SQUARE_API_BASE}/v2/online-checkout/payment-links",
            headers=_square_headers(),
            json=payload,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Square error: {resp.text}")

    data = resp.json()
    checkout_url = data.get("payment_link", {}).get("url")
    if not checkout_url:
        raise HTTPException(status_code=502, detail="No checkout URL returned")

    return {"ok": True, "checkout_url": checkout_url}


# =============================================================================
# Cancel subscription — called from the dashboard
# =============================================================================
@router.post("/cancel-subscription")
async def cancel_subscription(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Cancel the authenticated user's Square subscription.
    Access continues until end of current billing period (charged_through_date).
    """
    from api.auth import require_session  # local import to avoid circular

    session = require_session(request)
    user_id = int(session["sub"])

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if not user.square_subscription_id:
        raise HTTPException(status_code=400, detail="No active subscription found")

    if not SQUARE_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Square not configured on server")

    # Cancel with Square
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SQUARE_API_BASE}/v2/subscriptions/{user.square_subscription_id}/cancel",
            headers=_square_headers(),
        )

    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=502, detail=f"Square error: {resp.text}")

    data = resp.json()
    subscription = data.get("subscription", {})
    charged_through_date = subscription.get("charged_through_date")
    ends_at = _parse_square_date(charged_through_date)

    # Record grace period — is_pro stays True until ends_at passes
    user.subscription_ends_at = ends_at
    await db.commit()

    return {
        "ok": True,
        "subscription_ends_at": ends_at.isoformat() if ends_at else None,
    }


# =============================================================================
# Subscription status — lightweight check for the dashboard
# =============================================================================
@router.get("/subscription-status")
async def get_subscription_status(request: Request, db: AsyncSession = Depends(get_db)):
    """Return current subscription state for the authenticated user."""
    from api.auth import require_session

    session = require_session(request)
    user_id = int(session["sub"])

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    now = datetime.now(timezone.utc)
    effective_pro = user.is_pro and (
        user.subscription_ends_at is None or user.subscription_ends_at > now
    )

    if not user.is_pro:
        status = "inactive"
    elif user.subscription_ends_at is not None:
        status = "cancelling"
    else:
        status = "active"

    return {
        "is_pro": effective_pro,
        "status": status,
        "subscription_ends_at": (
            user.subscription_ends_at.isoformat() if user.subscription_ends_at else None
        ),
    }


# =============================================================================
# Webhook — handles Square subscription lifecycle events
# =============================================================================
@router.post("/webhook")
async def square_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    payload = await request.body()
    signature = request.headers.get("x-square-hmacsha256-signature", "")
    url = str(request.url)

    if SQUARE_WEBHOOK_SIGNATURE_KEY and not _verify_square_signature(payload, signature, url):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    event = json.loads(payload)
    event_type = event.get("type", "")
    data = event.get("data", {}).get("object", {})

    # ── One-time payment completed ──────────────────────────────────────────
    if event_type == "payment.updated" and (
        data.get("payment", {}).get("status") == "COMPLETED"
    ):
        payment = data.get("payment", {})
        email = (payment.get("buyer_email_address") or "").strip()
        square_customer_id = payment.get("customer_id")
        if email:
            result = await db.execute(select(User).where(User.email == email))
            user = result.scalar_one_or_none()
            if not user:
                user = User(email=email)
                db.add(user)
                await db.flush()
            user.is_pro = True
            user.subscription_ends_at = None  # one-time payment — no expiry
            if square_customer_id:
                user.square_customer_id = square_customer_id
            await db.commit()

    # ── Subscription created or updated ────────────────────────────────────
    elif event_type in ("subscription.created", "subscription.updated"):
        subscription = data.get("subscription", {})
        square_customer_id = subscription.get("customer_id")
        subscription_id = subscription.get("id")
        status = subscription.get("status", "")
        charged_through_date = subscription.get("charged_through_date")

        if square_customer_id:
            result = await db.execute(
                select(User).where(User.square_customer_id == square_customer_id)
            )
            user = result.scalar_one_or_none()
            if user:
                if status == "ACTIVE":
                    user.is_pro = True
                    user.subscription_ends_at = None  # active — no expiry
                    user.square_subscription_id = subscription_id
                elif status in ("CANCELED", "DEACTIVATED", "PAUSED"):
                    # Keep is_pro True with grace period from charged_through_date
                    ends_at = _parse_square_date(charged_through_date)
                    user.subscription_ends_at = ends_at
                    user.square_subscription_id = subscription_id
                await db.commit()

    # ── Subscription canceled ───────────────────────────────────────────────
    elif event_type == "subscription.canceled":
        subscription = data.get("subscription", {})
        subscription_id = subscription.get("id")
        charged_through_date = subscription.get("charged_through_date")

        if subscription_id:
            result = await db.execute(
                select(User).where(User.square_subscription_id == subscription_id)
            )
            user = result.scalar_one_or_none()
            if user:
                ends_at = _parse_square_date(charged_through_date)
                # Grace period: access continues until end of billing period
                user.subscription_ends_at = ends_at
                # is_pro stays True; /auth/me computes effective access via ends_at
                await db.commit()

    return {"ok": True}
