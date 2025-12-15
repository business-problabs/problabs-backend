import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# --- DB ---
DATABASE_URL = os.environ["DATABASE_URL"].replace(
    "postgresql://", "postgresql+asyncpg://"
)
engine = create_async_engine(DATABASE_URL, echo=False)  # echo=False for production

app = FastAPI(title="ProbLabs API")

# --- CORS ---
# Put your Vercel domain here (you can add more later)
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "https://problabs.net",
    "https://www.problabs.net",
    # add your Vercel preview + production domains when you have them
    # "https://your-app.vercel.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
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

@app.get("/db-check")
async def db_check():
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        value = result.scalar()
    return {"db": "ok", "select": value}

@app.get("/meta")
async def meta():
    return {
        "render_git_commit": os.getenv("RENDER_GIT_COMMIT"),
        "service": os.getenv("RENDER_SERVICE_NAME"),
    }

@app.post("/leads")
async def create_lead(payload: LeadIn):
    email = payload.email.strip().lower()

    async with engine.begin() as conn:
        # Insert; if email already exists, do nothing
        res = await conn.execute(
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
        row = res.mappings().first()

    if row is None:
        # already existed
        return {"ok": True, "created": False, "email": email}

    return {
        "ok": True,
        "created": True,
        "lead": {
            "id": row["id"],
            "email": row["email"],
            "created_at": str(row["created_at"]),
        },
    }
