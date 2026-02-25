import asyncio
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
from sqlalchemy.dialects.postgresql import insert
from db import SessionLocal
from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EASTERN_TZ = ZoneInfo("US/Eastern")
CUTOFF_DATE = datetime(2025, 8, 28, tzinfo=EASTERN_TZ)

# Construct today's URL for Florida Lottery winning numbers
TODAY_STR = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
WINNING_NUMBERS_URL = f"https://floridalottery.com/games/winning-numbers?game=all&searchBy=date&date={TODAY_STR}"

GAME_MAPPING = {
    "Pick 3": {"model": DrawPick3, "type": "pick3"},
    "Pick 4": {"model": DrawPick4, "type": "pick4"},
    "Pick 5": {"model": DrawPick5, "type": "pick5"},
    "Fantasy 5": {"model": DrawFantasy5, "type": "fantasy5"},
}

DRAW_TIMES = {
    "pick3": (21, 45),
    "pick4": (21, 45),
    "pick5": (21, 45),
    "fantasy5": (23, 15),
}

async def fetch_and_parse():
    parsed_data = {game: [] for game in GAME_MAPPING.keys()}
    
    async with async_playwright() as p:
        logger.info("Launching headless Chromium...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        logger.info(f"Navigating to {WINNING_NUMBERS_URL}...")
        try:
            await page.goto(WINNING_NUMBERS_URL, wait_until="networkidle", timeout=60000)
            
            # 1. Targeted wait for results to appear
            logger.info("Waiting for .cmp-numbersearch__results-draw-date selector...")
            await page.wait_for_selector('.cmp-numbersearch__results-draw-date', timeout=60000)
            
            # Find all draw result blocks
            # We'll use a locator that finds containers which likely hold the game name and results
            results = await page.locator(".cmp-numbersearch__results-draw-game").all()
            
            for res in results:
                text = await res.inner_text()
                if not text:
                    continue
                
                # Identify game
                game_name = None
                for g in GAME_MAPPING.keys():
                    if g in text:
                        game_name = g
                        break
                
                if not game_name:
                    continue

                # Extract date from the specific date element if possible, or from text
                date_el = res.locator(".cmp-numbersearch__results-draw-date")
                if await date_el.count() > 0:
                    date_str = await date_el.inner_text()
                else:
                    date_match = re.search(r'([A-Z][a-z]+ \d{1,2}, 20\d{2})', text)
                    date_str = date_match.group(1) if date_match else None

                if not date_str:
                    continue
                
                try:
                    draw_date = datetime.strptime(date_str.strip(), "%B %d, %Y").replace(tzinfo=EASTERN_TZ)
                    if draw_date < CUTOFF_DATE:
                        continue
                except ValueError:
                    continue

                # Extract winning numbers
                # Look for digits in spans or the general block text
                nums = [int(n) for n in re.findall(r'\b\d{1,2}\b', text.replace("2025", "").replace("2026", ""))]
                
                g_cfg = GAME_MAPPING[game_name]
                g_type = g_cfg["type"]
                expected = 5 if g_type in ["pick5", "fantasy5"] else (4 if g_type == "pick4" else 3)
                
                if len(nums) < expected:
                    continue
                
                winning_numbers = nums[:expected]
                hour, minute = DRAW_TIMES.get(g_type, (21, 45))
                draw_datetime = draw_date.replace(hour=hour, minute=minute)

                row = {"draw_datetime": draw_datetime}
                if g_type == "fantasy5":
                    row["numbers"] = winning_numbers
                else:
                    for i in range(expected):
                        row[f"digit_{i+1}"] = winning_numbers[i]
                
                # Avoid duplicates in same run
                if not any(d["draw_datetime"] == draw_datetime for d in parsed_data[game_name]):
                    parsed_data[game_name].append(row)

            await browser.close()
        except Exception as e:
            logger.error(f"Playwright error: {e}")
            await browser.close()

    return parsed_data

async def ingest_daily():
    logger.info("Starting daily ingestion...")
    all_draws = await fetch_and_parse()
    total_new = 0
    
    # 2. Database block using 'async with'
    async with SessionLocal() as session:
        for game_name, draws in all_draws.items():
            if not draws:
                continue
            
            model = GAME_MAPPING[game_name]["model"]
            draws.sort(key=lambda x: x["draw_datetime"])
            
            chunk_size = 1000
            for i in range(0, len(draws), chunk_size):
                chunk = draws[i : i + chunk_size]
                stmt = insert(model).values(chunk)
                stmt = stmt.on_conflict_do_nothing(index_elements=["draw_datetime"])
                await session.execute(stmt)
            
            total_new += len(draws)
            
        await session.commit()
    
    print(f"Success: Processed {total_new} new draws from 2025-2026.")

if __name__ == "__main__":
    # 3. Called with asyncio.run()
    asyncio.run(ingest_daily())
