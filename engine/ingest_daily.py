import asyncio
import logging
import re
from datetime import datetime, timedelta
from collections import Counter
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select

from db import SessionLocal
from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5, DrawCashPop, ComputedStatistic

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EASTERN_TZ = ZoneInfo("US/Eastern")
CUTOFF_DATE = datetime(2024, 1, 1, tzinfo=EASTERN_TZ)

# If the script runs at 6:00 AM EST (cron time), today's draws haven't happened yet!
# We dynamically switch to scrape yesterday's date if it's before 8:00 AM EST.
now_est = datetime.now(EASTERN_TZ)
target_date = now_est - timedelta(days=1) if now_est.hour < 8 else now_est
TODAY_STR = target_date.strftime("%Y-%m-%d")

URLS_TO_SCRAPE = [
    f"https://floridalottery.com/games/winning-numbers?game=all&searchBy=date&date={TODAY_STR}",
    f"https://floridalottery.com/games/winning-numbers?game=cashPop&searchBy=date&date={TODAY_STR}",
    f"https://floridalottery.com/games/winning-numbers?game=cash-pop&searchBy=date&date={TODAY_STR}"
]

GAME_MAPPING = {
    "Pick 3": {"model": DrawPick3, "type": "pick3"},
    "Pick 4": {"model": DrawPick4, "type": "pick4"},
    "Pick 5": {"model": DrawPick5, "type": "pick5"},
    "Fantasy 5": {"model": DrawFantasy5, "type": "fantasy5"},
    "Cash Pop": {"model": DrawCashPop, "type": "cashpop"},
}

DRAW_TIMES = {
    "pick3": (21, 45),
    "pick4": (21, 45),
    "pick5": (21, 45),
    "fantasy5": (23, 15),
    "cashpop": (23, 15),
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
        
        for url in URLS_TO_SCRAPE:
            logger.info(f"Navigating to {url}...")
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
                
                # 1. Targeted wait for results to appear
                logger.info("Waiting for .cmp-numbersearch__results-draw-date selector...")
                await page.wait_for_selector('.cmp-numbersearch__results-draw-date', timeout=60000)
                
                # Find all draw result blocks
                results = await page.locator(".cmp-numbersearch__results-draw-game").all()
                
                for res in results:
                    text = await res.inner_text()
                    if not text:
                        continue
                    
                    # Identify game
                    game_name = None
                    text_upper = text.upper()
                    for g in GAME_MAPPING.keys():
                        if g.upper() in text_upper:
                            game_name = g
                            break
                    
                    # FALLBACK: If game name missing from text, but URL is explicitly Cash Pop
                    if not game_name and ("cashPop" in url or "cash-pop" in url):
                        game_name = "Cash Pop"

                    if not game_name:
                        continue

                    # Extract date from the specific date element if possible, or from text
                    date_el = res.locator(".cmp-numbersearch__results-draw-date")
                    if await date_el.count() > 0:
                        date_str = await date_el.inner_text()
                    else:
                        date_match = re.search(r'([A-Z][a-z]+ \d{1,2}, 20\d{2})', text)
                        date_str = date_match.group(1) if date_match else None

                    # FALLBACK: If date missing from text, use the requested target date
                    if not date_str:
                        date_str = target_date.strftime("%B %d, %Y")
                    
                    try:
                        draw_date = datetime.strptime(date_str.strip(), "%B %d, %Y").replace(tzinfo=EASTERN_TZ)
                        if draw_date < CUTOFF_DATE:
                            continue
                    except ValueError:
                        continue

                    # Remove the date string so we don't accidentally extract the day of the month as a winning number
                    text_no_date = re.sub(r'[A-Za-z]+ \d{1,2}, \d{4}', '', text)
                    
                    nums = [int(n) for n in re.findall(r'\b\d{1,2}\b', text_no_date)]
                    
                    g_cfg = GAME_MAPPING[game_name]
                    g_type = g_cfg["type"]
                    
                    if g_type == "cashpop":
                        expected = 1
                    elif g_type in ["pick5", "fantasy5"]:
                        expected = 5
                    else:
                        expected = 4 if g_type == "pick4" else 3
                    
                    if len(nums) < expected:
                        continue
                    
                    winning_numbers = nums[:expected]
                    hour, minute = DRAW_TIMES.get(g_type, (21, 45))

                    if g_type == "cashpop":
                        if "MORNING" in text_upper: hour, minute = 8, 45
                        elif "MATINEE" in text_upper: hour, minute = 13, 0
                        elif "AFTERNOON" in text_upper: hour, minute = 16, 45
                        elif "EVENING" in text_upper: hour, minute = 20, 45
                        elif "LATE NIGHT" in text_upper: hour, minute = 23, 45
                    elif g_type in ["pick3", "pick4", "pick5"]:
                        if "MIDDAY" in text_upper: hour, minute = 13, 30
                        elif "EVENING" in text_upper: hour, minute = 21, 45

                    logger.info(f"Parsed {game_name}: {winning_numbers} for {draw_date.strftime('%Y-%m-%d')} at {hour:02d}:{minute:02d}")
                    draw_datetime = draw_date.replace(hour=hour, minute=minute)

                    row = {"draw_datetime": draw_datetime}
                    if g_type == "fantasy5":
                        row["numbers"] = winning_numbers
                    elif g_type == "cashpop":
                        row["number"] = winning_numbers[0]
                    else:
                        for i in range(expected):
                            row[f"digit_{i+1}"] = winning_numbers[i]
                    
                    # Avoid duplicates in same run
                    if not any(d["draw_datetime"] == draw_datetime for d in parsed_data[game_name]):
                        parsed_data[game_name].append(row)
                        
                # ULTIMATE FALLBACK: If standard block parsing failed to find Cash Pop draws on a Cash Pop URL
                if not parsed_data["Cash Pop"] and ("cashPop" in url or "cash-pop" in url):
                    logger.warning(f"Standard parsing yielded no Cash Pop data on {url}. Using raw text fallback.")
                    try:
                        body_text = await page.inner_text("body")
                        for draw_name in ["Morning", "Matinee", "Afternoon", "Evening", "Late Night"]:
                            # Matches draw name followed by up to 150 chars and a valid Cash Pop number (1-15)
                            match = re.search(fr'{draw_name}.{{0,150}}?\b([1-9]|1[0-5])\b', body_text, re.IGNORECASE | re.DOTALL)
                            if match:
                                num = int(match.group(1))
                                hour, minute = 23, 45
                                d_upper = draw_name.upper()
                                if "MORNING" in d_upper: hour, minute = 8, 45
                                elif "MATINEE" in d_upper: hour, minute = 13, 0
                                elif "AFTERNOON" in d_upper: hour, minute = 16, 45
                                elif "EVENING" in d_upper: hour, minute = 20, 45
                                elif "LATE NIGHT" in d_upper: hour, minute = 23, 45
                                
                                draw_datetime = target_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                                if not any(d["draw_datetime"] == draw_datetime for d in parsed_data["Cash Pop"]):
                                    parsed_data["Cash Pop"].append({"draw_datetime": draw_datetime, "number": num})
                                    logger.info(f"Fallback parsed Cash Pop: [{num}] for {draw_datetime}")
                    except Exception as fallback_e:
                        logger.error(f"Raw text fallback failed: {fallback_e}")

            except Exception as e:
                logger.error(f"Playwright error on {url}: {e}")
                
        # Close the browser AFTER both URLs have been fully scraped
        await browser.close()

    return parsed_data

async def compute_and_store_statistics(session, api_game_name, model):
    """Calculates the 30-day Hot/Cold digits and saves them to ComputedStatistic."""
    logger.info(f"Calculating 30-day variance for {api_game_name}...")
    
    # Get cutoff date (30 days ago)
    cutoff = datetime.now(EASTERN_TZ) - timedelta(days=30)
    
    # Query last 30 days of draws
    stmt = select(model).where(model.draw_datetime >= cutoff)
    result = await session.execute(stmt)
    draws = result.scalars().all()
    
    if not draws:
        logger.warning(f"No draws found in the last 30 days for {api_game_name}")
        return

    counts = Counter()
    total_digits = 0
    
    # Count the digits
    if api_game_name == "fantasy-5":
        for draw in draws:
            if draw.numbers:
                counts.update(draw.numbers)
                total_digits += len(draw.numbers)
    elif api_game_name == "cash-pop":
        for draw in draws:
            if draw.number is not None:
                counts[draw.number] += 1
                total_digits += 1
    else:
        # For Pick 3, Pick 4, Pick 5
        expected = 5 if api_game_name == "pick-5" else (4 if api_game_name == "pick-4" else 3)
        for draw in draws:
            for i in range(1, expected + 1):
                val = getattr(draw, f"digit_{i}", None)
                if val is not None:
                    counts[val] += 1
                    total_digits += 1
                    
    if not counts:
        return

    # Determine Hot/Cold
    most_common = counts.most_common()
    hot_val, hot_count = most_common[0]
    cold_val, cold_count = most_common[-1]
    
    hot_rate = f"{(hot_count / total_digits) * 100:.1f}%" if total_digits > 0 else "0%"
    cold_rate = f"{(cold_count / total_digits) * 100:.1f}%" if total_digits > 0 else "0%"
    
    metric_value = {
        "hot_digit": str(hot_val),
        "hot_rate": hot_rate,
        "cold_digit": str(cold_val),
        "cold_rate": cold_rate
    }
    
    # Insert into database
    new_stat = ComputedStatistic(
        game_type=api_game_name,
        metric_name="variance_30_day",
        metric_value=metric_value
    )
    session.add(new_stat)

async def ingest_daily():
    logger.info("Starting daily ingestion...")
    all_draws = await fetch_and_parse()
    total_new = 0
    
    # Database block
    async with SessionLocal() as session:
        for game_name, draws in all_draws.items():
            model = GAME_MAPPING[game_name]["model"]
            api_game_name = game_name.lower().replace(" ", "-")

            if draws:
                draws.sort(key=lambda x: x["draw_datetime"])
                
                chunk_size = 1000
                for i in range(0, len(draws), chunk_size):
                    chunk = draws[i : i + chunk_size]
                    stmt = insert(model).values(chunk)
                    stmt = stmt.on_conflict_do_nothing(index_elements=["draw_datetime"])
                    await session.execute(stmt)
                
                total_new += len(draws)
            
            # Always compute fresh stats, even if no new draws happened today
            await compute_and_store_statistics(session, api_game_name, model)
            
        await session.commit()
    
    print(f"Success: Processed {total_new} new draws from 2025-2026 and updated 30-day stats.")

if __name__ == "__main__":
    asyncio.run(ingest_daily())
