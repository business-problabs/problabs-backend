import asyncio
import httpx
import logging
import ssl
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.dialects.postgresql import insert
from db import SessionLocal
from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URLS = {
    "pick3": "https://www.flalottery.com/exptkt/p3.htm",
    "pick4": "https://www.flalottery.com/exptkt/p4.htm",
    "pick5": "https://www.flalottery.com/exptkt/p5.htm",
    "fantasy5": "https://www.flalottery.com/exptkt/ff.htm",
}

EASTERN_TZ = ZoneInfo("US/Eastern")

# Draw times based on game type and indicator
DRAW_TIMES = {
    "pick": {
        "M": (13, 30),  # Midday: 13:30 ET
        "E": (21, 45),  # Evening: 21:45 ET
    },
    "fantasy5": {
        "M": (13, 5),   # Midday: 13:05 ET
        "E": (23, 15),  # Evening: 23:15 ET
    }
}

async def fetch_and_parse(url, game_type):
    context = ssl.create_default_context()
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    async with httpx.AsyncClient(verify=context, headers=headers) as client:
        try:
            response = await client.get(url, timeout=10.0, follow_redirects=True)
            if response.status_code != 200:
                logger.error(f"Failed to fetch {url}: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Exception fetching {url}: {e}")
            return []

    # 1. Strip all HTML tags first
    clean_content = re.sub(r'<[^>]+>', ' ', response.text)
    # 2. Replace &nbsp; and normalize whitespace
    clean_content = clean_content.replace('&nbsp;', ' ')
    clean_content = re.sub(r'\s+', ' ', clean_content).strip()
    
    logger.info(f"Cleaned content sample: {clean_content[500:1000]}")
    
    data = []
    
    # Map game type to number of digits/numbers expected
    expected_counts = {
        "pick3": 3,
        "pick4": 4,
        "pick5": 5,
        "fantasy5": 5,
    }
    
    count = expected_counts.get(game_type)
    if not count:
        logger.error(f"Unknown game type: {game_type}")
        return []

    # Get the appropriate draw times for this game
    draw_times_cfg = DRAW_TIMES.get("fantasy5" if game_type == "fantasy5" else "pick")

    # 3. Simple regex for the cleaned string
    if game_type == "fantasy5":
        # Fantasy 5 might not have M/E indicator in some files, just Date + Numbers
        pattern = re.compile(r'(\d{1,2}/\d{1,2}/\d{2,4})\s+([\d\s-]+)', re.IGNORECASE)
    else:
        # Pick games: Date, Space, M/E, Space, Numbers
        pattern = re.compile(r'(\d{1,2}/\d{1,2}/\d{2,4})\s+([ME])\s+([\d\s-]+)', re.IGNORECASE)

    for match in pattern.finditer(clean_content):
        if game_type == "fantasy5":
            date_str = match.group(1)
            # For Fantasy 5, we use a constant draw time as requested
            hour, minute = 23, 15
            nums_str = match.group(2)
            draw_indicator = "E"
        else:
            date_str = match.group(1)
            draw_indicator = match.group(2).upper()
            nums_str = match.group(3)
            
            if draw_indicator not in draw_times_cfg:
                continue
            hour, minute = draw_times_cfg[draw_indicator]
        
        logger.info(f"Raw Date String: {date_str}")
        
        # Extract digits
        nums = [int(n) for n in re.findall(r'\d+', nums_str)]
        
        if len(nums) < count:
            continue
            
        winning_numbers = nums[:count]

        try:
            # Parse date
            try:
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                date_obj = datetime.strptime(date_str, "%m/%d/%y")

            draw_datetime = date_obj.replace(hour=hour, minute=minute, tzinfo=EASTERN_TZ)

            if len(data) < 5:
                print(f"Sample draw for {game_type}: {draw_datetime}")

            if game_type == "fantasy5":
                data.append({
                    "draw_datetime": draw_datetime,
                    "numbers": winning_numbers
                })
            else:
                row = {"draw_datetime": draw_datetime}
                for i in range(count):
                    row[f"digit_{i+1}"] = winning_numbers[i]
                data.append(row)
        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse match: {date_str} {draw_indicator}. Error: {e}")
            continue

    logger.info(f"Regex found {len(data)} potential draws for {game_type}")
    return data

async def ingest_game(game_type):
    url = URLS.get(game_type)
    if not url:
        logger.error(f"No URL for game type: {game_type}")
        return

    model = {
        "pick3": DrawPick3,
        "pick4": DrawPick4,
        "pick5": DrawPick5,
        "fantasy5": DrawFantasy5,
    }.get(game_type)
    
    if not model:
        logger.error(f"No model for game type: {game_type}")
        return
    
    logger.info(f"Fetching {game_type} from {url}...")
    draws = await fetch_and_parse(url, game_type)
    
    if not draws:
        logger.info(f"No new draws found for {game_type}.")
        return

    async with SessionLocal() as session:
        draws.sort(key=lambda x: x["draw_datetime"])
        
        # Insert in chunks of 1000
        chunk_size = 1000
        for i in range(0, len(draws), chunk_size):
            chunk = draws[i : i + chunk_size]
            stmt = insert(model).values(chunk)
            stmt = stmt.on_conflict_do_nothing(index_elements=["draw_datetime"])
            await session.execute(stmt)
            
        await session.commit()
        logger.info(f"Successfully processed {len(draws)} records for {game_type}.")

async def main():
    tasks = [ingest_game(g) for g in URLS.keys()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
