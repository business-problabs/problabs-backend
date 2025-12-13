from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine
import os

DATABASE_URL = os.environ["DATABASE_URL"].replace(
    "postgresql://", "postgresql+asyncpg://"
)

engine = create_async_engine(DATABASE_URL, echo=True)

app = FastAPI(title="ProbLabs API")

@app.get("/health")
def health():
    return {"status": "ok", "service": "problabs-backend"}
