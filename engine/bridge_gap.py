import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.dialects.postgresql import insert
from db import SessionLocal
from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EASTERN_TZ = ZoneInfo("US/Eastern")

# Data provided by user for Feb 24, 2026
# (Fantasy 5, Pick 3, Pick 4, Pick 5)
# Assuming M=Midday (13:30/13:05), E=Evening (21:45/23:15)

MANUAL_DRAWS = {
    DrawPick3: [
        {"draw_datetime": datetime(2026, 2, 24, 13, 30, tzinfo=EASTERN_TZ), "digit_1": 0, "digit_2": 0, "digit_3": 7}, # Midday
        {"draw_datetime": datetime(2026, 2, 24, 21, 45, tzinfo=EASTERN_TZ), "digit_1": 5, "digit_2": 3, "digit_3": 6}, # Evening
    ],
    DrawPick4: [
        {"draw_datetime": datetime(2026, 2, 24, 13, 30, tzinfo=EASTERN_TZ), "digit_1": 1, "digit_2": 3, "digit_3": 4, "digit_4": 7}, # Midday
        {"draw_datetime": datetime(2026, 2, 24, 21, 45, tzinfo=EASTERN_TZ), "digit_1": 5, "digit_2": 5, "digit_3": 8, "digit_4": 9}, # Evening
    ],
    DrawPick5: [
        {"draw_datetime": datetime(2026, 2, 24, 13, 30, tzinfo=EASTERN_TZ), "digit_1": 9, "digit_2": 3, "digit_3": 9, "digit_4": 2, "digit_5": 6}, # Midday
        {"draw_datetime": datetime(2026, 2, 24, 21, 45, tzinfo=EASTERN_TZ), "digit_1": 8, "digit_2": 7, "digit_3": 1, "digit_4": 3, "digit_5": 5}, # Evening
    ],
    DrawFantasy5: [
        {"draw_datetime": datetime(2026, 2, 24, 13, 5, tzinfo=EASTERN_TZ), "numbers": [2, 16, 21, 25, 32]}, # Midday
        {"draw_datetime": datetime(2026, 2, 24, 23, 15, tzinfo=EASTERN_TZ), "numbers": [6, 11, 28, 29, 30]}, # Evening
    ]
}

async def bridge_gap():
    logger.info("Starting manual gap bridge for Feb 24, 2026 draws...")
    
    total_processed = 0
    async with SessionLocal() as session:
        for model, draws in MANUAL_DRAWS.items():
            if not draws:
                continue
            
            # Sort by draw_datetime
            draws.sort(key=lambda x: x["draw_datetime"])
            
            # Use ON CONFLICT DO NOTHING to respect unique constraint on draw_datetime
            stmt = insert(model).values(draws)
            stmt = stmt.on_conflict_do_nothing(index_elements=["draw_datetime"])
            
            await session.execute(stmt)
            total_processed += len(draws)
            logger.info(f"Processed {len(draws)} manual records for {model.__tablename__}.")
            
        await session.commit()
    
    logger.info(f"Gap bridge complete. Total manual records processed: {total_processed}")

if __name__ == "__main__":
    asyncio.run(bridge_gap())
