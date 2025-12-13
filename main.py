import os

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = os.environ["DATABASE_URL"].replace(
    "postgresql://", "postgresql+asyncpg://"
)

engine = create_async_engine(DATABASE_URL, echo=True)

app = FastAPI(title="ProbLabs API")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "problabs-backend"}


@app.get("/db-check")
async def db_check():
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        value = result.scalar()
    return {"db": "ok", "select": value}
