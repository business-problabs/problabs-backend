from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from db import SessionLocal
from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5
from typing import Any, Dict

router = APIRouter()

# Mapping of game_type strings to models
GAME_MODELS = {
    "pick3": DrawPick3,
    "pick4": DrawPick4,
    "pick5": DrawPick5,
    "fantasy5": DrawFantasy5,
}

# Dependency to get the async database session
async def get_db():
    async with SessionLocal() as session:
        yield session

@router.get("/health-check")
async def health_check():
    return {"status": "ok"}

@router.get("/latest/{game_type}")
async def get_latest_draw(game_type: str, db: AsyncSession = Depends(get_db)):
    # Validate game_type
    model = GAME_MODELS.get(game_type.lower())
    if not model:
        raise HTTPException(status_code=404, detail=f"Game type '{game_type}' not found. Supported types: {list(GAME_MODELS.keys())}")

    # Query for the most recent draw based on draw_datetime
    stmt = select(model).order_by(model.draw_datetime.desc()).limit(1)
    result = await db.execute(stmt)
    latest_draw = result.scalar_one_or_none()

    if not latest_draw:
        raise HTTPException(status_code=404, detail=f"No draw records found for '{game_type}'.")

    # Construct clean response
    response_data: Dict[str, Any] = {
        "game_type": game_type,
        "draw_datetime": latest_draw.draw_datetime,
    }

    # Add game-specific winning numbers
    if game_type.lower() == "fantasy5":
        response_data["numbers"] = latest_draw.numbers
    elif game_type.lower() == "pick3":
        response_data["numbers"] = [latest_draw.digit_1, latest_draw.digit_2, latest_draw.digit_3]
    elif game_type.lower() == "pick4":
        response_data["numbers"] = [latest_draw.digit_1, latest_draw.digit_2, latest_draw.digit_3, latest_draw.digit_4]
    elif game_type.lower() == "pick5":
        response_data["numbers"] = [latest_draw.digit_1, latest_draw.digit_2, latest_draw.digit_3, latest_draw.digit_4, latest_draw.digit_5]

    return response_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
