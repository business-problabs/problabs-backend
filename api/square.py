import hashlib
import hmac
import json
import os
import uuid
import base64
import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from db import SessionLocal
from models import User

router = APIRouter(prefix="/square", tags=["square"])

def _getenv(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

SQUARE_ACCESS_TOKEN = _getenv("SQUARE_ACCESS_TOKEN")
SQUARE_WEBHOOK_SIGNATURE_KEY = _getenv("SQUARE_WEBHOOK_SIGNATURE_KEY")
SQUARE_LOCATION_ID = _getenv("SQUARE_LOCATION_ID")
SQUARE_ENVIRONMENT = _getenv("SQUARE_ENVIRONMENT", "sandbox")
PUBLIC_APP_URL = _getenv("PUBLIC_APP_URL", "https://www.problabs.net").rstrip("/")
ADMIN_API_KEY = _getenv("ADMIN_API_KEY")
SQUARE_API_BASE = "https://connect.squareupsandbox.com" if SQUARE_ENVIRONMENT == "sandbox" else "https://connect.squareup.com"

async def get_db():
    async with SessionLocal() as session:
        yield session

def _verify_square_signature(payload: bytes, signature: str, url: str) -> bool:
    if not SQUARE_WEBHOOK_SIGNATURE_KEY:
        return False
    combined = SQUARE_WEBHOOK_SIGNATURE_KEY + url + payload.decode("utf-8")
    expected = hmac.new(SQUARE_WEBHOOK_SIGNATURE_KEY.encode(), combined.encode(), hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(expected).decode(), signature)

@router.post("/checkout")
async def create_checkout(request: Request, db: AsyncSession = Depends(get_db)):
    if not SQUARE_ACCESS_TOKEN or not SQUARE_LOCATION_ID:
        raise HTTPException(status_code=500, detail="Square not configured on server")
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    payload = {
        "idempotency_key": str(uuid.uuid4()),
        "order": {"location_id": SQUARE_LOCATION_ID, "line_items": [{"name": "ProbLabs Pro", "quantity": "1", "base_price_money": {"amount": 999, "currency": "USD"}}]},
        "checkout_options": {"redirect_url": f"{PUBLIC_APP_URL}/dashboard?upgraded=true", "ask_for_shipping_address": False},
        "pre_populated_data": {"buyer_email": email},
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{SQUARE_API_BASE}/v2/online-checkout/payment-links", headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Content-Type": "application/json", "Square-Version": "2024-01-18"}, json=payload)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Square error: {resp.text}")
    data = resp.json()
    checkout_url = data.get("payment_link", {}).get("url")
    if not checkout_url:
        raise HTTPException(status_code=502, detail="No checkout URL returned")
    return {"ok": True, "checkout_url": checkout_url}

@router.post("/webhook")
async def square_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    payload = await request.body()
    signature = request.headers.get("x-square-hmacsha256-signature", "")
    if SQUARE_WEBHOOK_SIGNATURE_KEY and not _verify_square_signature(payload, signature, str(request.url)):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
    event = json.loads(payload)
    event_type = event.get("type", "")
    data = event.get("data", {}).get("object", {})
    if event_type == "payment.completed":
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
            if square_customer_id:
                user.square_customer_id = square_customer_id
            await db.commit()
    elif event_type in ("subscription.created", "subscription.updated"):
        subscription = data.get("subscription", {})
        square_customer_id = subscription.get("customer_id")
        subscription_id = subscription.get("id")
        status = subscription.get("status", "")
        if square_customer_id:
            result = await db.execute(select(User).where(User.square_customer_id == square_customer_id))
            user = result.scalar_one_or_none()
            if user:
                user.is_pro = status == "ACTIVE"
                user.square_subscription_id = subscription_id
                await db.commit()
    elif event_type == "subscription.canceled":
        subscription = data.get("subscription", {})
        subscription_id = subscription.get("id")
        if subscription_id:
            result = await db.execute(select(User).where(User.square_subscription_id == subscription_id))
            user = result.scalar_one_or_none()
            if user:
                user.is_pro = False
                user.square_subscription_id = None
                await db.commit()
    return {"ok": True}
