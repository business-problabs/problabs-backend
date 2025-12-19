
import os

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address


# --- Database setup ---
DATABASE_URL = os.environ["DATABASE_URL"].replace(
    "postgresql://", "postgresql+asyncpg://"
)
engine = create_async_engine(DATABASE_URL, echo=False)

# --- App setup ---
app = FastAPI(title="ProbLabs API")

# --- CORS ---
cors_origins_raw = os.getenv("CORS_ORIGINS", "")
allowed_origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]
if not allowed_origins:
    allowed_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Turnstile ---
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY", "").strip()
TURNSTILE_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"


def get_client_ip(request: Request) -> str:
    """
    Best-effort client IP extraction behind proxies (Render / Vercel / etc.).
    Uses X-Forwarded-For when present; falls back to FastAPI/Starlette remote address.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # "client, proxy1, proxy2"
        return xff.split(",")[0].strip()
    return get_remote_address(request)


# --- Rate limiting (per client IP) ---
limiter = Limiter(key_func=get_client_ip, default_limits=[])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


async def verify_turnstile(token: str, remote_ip: str | None = None) -> None:
    """
    Verifies the Turnstile token with Cloudflare.
    Raises HTTPException if invalid.
    """
    if not TURNSTILE_SECRET_KEY:
        raise HTTPException(
            status_code=500,
            detail="TURNSTILE_SECRET_KEY is not set on the server.",
        )

    data = {"secret": TURNSTILE_SECRET_KEY, "response": token}
    if remote_ip:
        data["remoteip"] = remote_ip

    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(TURNSTILE_VERIFY_URL, data=data)

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Turnstile verification service error.")

    payload = resp.json()
    if not payload.get("success"):
        raise HTTPException(status_code=403, detail="Turnstile verification failed.")


# --- Schemas ---
class LeadIn(BaseModel):
    email: EmailStr
    turnstile_token: str


# --- Routes ---
@app.get("/health")
async def health():
    return {"status": "ok", "service": "problabs-backend"}


@app.get("/meta")
async def meta():
    return {
        "render_git_commit": os.getenv("RENDER_GIT_COMMIT"),
        "service": os.getenv("RENDER_SERVICE_NAME"),
    }


@app.get("/db-check")
async def db_check():
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        value = result.scalar_one()
    return {"db": "ok", "select": value}


@app.get("/leads/count")
async def leads_count():
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM leads"))
        total = result.scalar_one()
    return {"ok": True, "count": total}


# ✅ Rate limit ONLY this endpoint:
# - default: 5 requests per minute per IP
@app.post("/leads")
@limiter.limit("5/minute")
async def create_lead(payload: LeadIn, request: Request):
    """
    Verifies Turnstile server-side, normalizes email, then inserts lead.
    Idempotent via unique constraint on email.
    """

    # 1) Verify Turnstile server-side
    client_ip = get_client_ip(request)
    await verify_turnstile(payload.turnstile_token, remote_ip=client_ip)

    # 2) Normalize email (backend-level)
    email = str(payload.email).strip().lower()

    # 3) Insert
    async with engine.begin() as conn:
        inserted = await conn.execute(
            text(
                """
                INSERT INTO leads (email)
                VALUES (:email)
                ON CONFLICT (email) DO NOTHING
                RETURNING id, email, created_at
                """
            ),
            {"email": email},
        )
        row = inserted.mappings().first()

        if row:
            return {
                "ok": True,
                "created": True,
                "message": "✅ You’re on the list. Launching soon!",
                "lead": dict(row),
            }

        return {
            "ok": True,
            "created": False,
            "message": "ℹ️ You’re already on the list.",
            "email": email,
        }
