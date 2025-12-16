import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# --- Database setup ---
# Render gives DATABASE_URL like: postgresql://...
# Async SQLAlchemy needs: postgresql+asyncpg://...
DATABASE_URL = os.environ["DATABASE_URL"].replace(
    "postgresql://", "postgresql+asyncpg://"
)

# NOTE: echo=True is noisy in production. Keep it False.
engine = create_async_engine(DATABASE_URL, echo=False)

# --- App setup ---
app = FastAPI(title="ProbLabs API")

# --- CORS ---
# Put your allowed origins in Render env var, comma-separated, e.g.:
# CORS_ORIGINS=https://yourapp.vercel.app,https://problabs.ai
cors_origins_raw = os.getenv("CORS_ORIGINS", "")
allowed_origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]

# If you haven't set CORS_ORIGINS yet, this keeps dev easy while you wire things up.
# For production, set CORS_ORIGINS and remove the "*" fallback if you want strict mode.
if not allowed_origins:
    allowed_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Schemas ---
class LeadIn(BaseModel):
    email: EmailStr


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


@app.post("/leads")
async def create_lead(payload: LeadIn):
    """
    Inserts a lead email into the leads table.
    If it already exists, returns created:false (idempotent behavior).
    """
    async with engine.begin() as conn:
        # Insert (ignore duplicates by unique email constraint)
        inserted = await conn.execute(
            text(
                """
                INSERT INTO leads (email)
                VALUES (:email)
                ON CONFLICT (email) DO NOTHING
                RETURNING id, email, created_at
                """
            ),
            {"email": str(payload.email)},
        )
        row = inserted.mappings().first()

        if row:
            return {"ok": True, "created": True, "lead": dict(row)}

        return {"ok": True, "created": False, "email": str(payload.email)}
