
import os
import csv
import io

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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

# --- Admin key (for exports) ---
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()


def get_client_ip(request: Request) -> str:
    """
    Best-effort client IP extraction behind proxies (Render / Vercel / etc.).
    Uses X-Forwarded-For when present; falls back to FastAPI/Starlette remote address.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return get_remote_address(request)


# --- Rate limiting (per client IP) ---
limiter = Limiter(key_func=get_client_ip, default_limits=[])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


def require_admin(request: Request) -> None:
    """
    Simple header-based auth for admin endpoints.
    Send: X-Admin-Key: <ADMIN_API_KEY>
    """
    if not ADMIN_API_KEY:
        # Fail closed if not configured
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY is not set on the server.")

    provided = request.headers.get("x-admin-key") or request.headers.get("X-Admin-Key")
    if not provided or provided.strip() != ADMIN_API_KEY:
        # 404 is sometimes used to hide endpoint existence; 401 is clearer.
        raise HTTPException(status_code=401, detail="Unauthorized.")


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


# ✅ Rate limit ONLY this endpoint: 5 requests per minute per IP
@app.post("/leads")
@limiter.limit("5/minute")
async def create_lead(payload: LeadIn, request: Request):
    """
    Verifies Turnstile server-side, normalizes email, then inserts lead.
    Idempotent via unique constraint on email.
    """
    client_ip = get_client_ip(request)
    await verify_turnstile(payload.turnstile_token, remote_ip=client_ip)

    email = str(payload.email).strip().lower()

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


# -------------------------
# Admin export endpoints
# -------------------------

@app.get("/admin/leads")
@limiter.limit("30/minute")
async def admin_leads(request: Request, limit: int = 2000, offset: int = 0):
    """
    Returns leads as JSON (admin only).
    """
    require_admin(request)

    # Safety caps
    limit = max(1, min(limit, 20000))
    offset = max(0, min(offset, 500000))

    async with engine.connect() as conn:
        res = await conn.execute(
            text(
                """
                SELECT id, email, created_at
                FROM leads
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        )
        rows = [dict(r) for r in res.mappings().all()]

    return {"ok": True, "count": len(rows), "limit": limit, "offset": offset, "leads": rows}


@app.get("/admin/leads.csv")
@limiter.limit("10/minute")
async def admin_leads_csv(request: Request):
    """
    Downloads all leads as CSV (admin only).
    """
    require_admin(request)

    async with engine.connect() as conn:
        res = await conn.execute(
            text(
                """
                SELECT id, email, created_at
                FROM leads
                ORDER BY id ASC
                """
            )
        )
        rows = res.mappings().all()

    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "email", "created_at"])

        for r in rows:
            writer.writerow([r["id"], r["email"], r["created_at"]])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    headers = {
        "Content-Disposition": 'attachment; filename="problabs_leads.csv"'
    }
    return StreamingResponse(generate(), media_type="text/csv", headers=headers)
