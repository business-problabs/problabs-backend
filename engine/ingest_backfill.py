#!/usr/bin/env python3
"""
ingest_backfill.py — Backfill missing lottery draws.

Usage:
    python engine/ingest_backfill.py                        # Feb 26 2026 → yesterday
    python engine/ingest_backfill.py --start 2026-02-26     # custom start
    python engine/ingest_backfill.py --start 2026-02-26 --end 2026-04-30

This works by scraping floridalottery.com for each missing date and inserting
any new draws into the database (ON CONFLICT DO NOTHING keeps it idempotent).
"""

import sys
import os
# Allow imports from the project root (db.py, models.py) regardless of where the script is invoked from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import argparse
import logging
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import Counter
from playwright.async_api import async_playwright
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select

from db import SessionLocal
from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5, DrawCashPop, ComputedStatistic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EASTERN_TZ = ZoneInfo("US/Eastern")

GAME_MAPPING = {
    "Pick 3":    {"model": DrawPick3,    "type": "pick3"},
    "Pick 4":    {"model": DrawPick4,    "type": "pick4"},
    "Pick 5":    {"model": DrawPick5,    "type": "pick5"},
    "Fantasy 5": {"model": DrawFantasy5, "type": "fantasy5"},
    "Cash Pop":  {"model": DrawCashPop,  "type": "cashpop"},
}

DRAW_TIMES = {
    "pick3":    (21, 45),
    "pick4":    (21, 45),
    "pick5":    (21, 45),
    "fantasy5": (23, 15),
    "cashpop":  (23, 15),
}


async def scrape_date(page, date_str: str) -> dict:
    """Scrape all games for a single date. Returns {game_name: [row, ...]}."""
    parsed_data = {g: [] for g in GAME_MAPPING}
    target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)

    urls = [
        f"https://floridalottery.com/games/winning-numbers?game=all&searchBy=date&date={date_str}",
        "https://floridalottery.com/games/cash-pop",
    ]

    for url in urls:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            try:
                await page.wait_for_selector(".cmp-numbersearch__results-draw-date", timeout=15000)
            except Exception:
                pass

            results = await page.locator(".cmp-numbersearch__results-draw-game").all()

            for res in results:
                text = await res.inner_text()
                if not text:
                    continue

                game_name = None
                text_upper = text.upper()
                for g in GAME_MAPPING:
                    if g.upper() in text_upper:
                        game_name = g
                        break
                if not game_name and "cash-pop" in url:
                    game_name = "Cash Pop"
                if not game_name:
                    continue

                date_el = res.locator(".cmp-numbersearch__results-draw-date")
                date_str_raw = (await date_el.inner_text()) if await date_el.count() > 0 else None
                if not date_str_raw:
                    m = re.search(r"([A-Z][a-z]+ \d{1,2}, 20\d{2})", text)
                    date_str_raw = m.group(1) if m else target_date.strftime("%B %d, %Y")

                try:
                    draw_date = datetime.strptime(date_str_raw.strip(), "%B %d, %Y").replace(tzinfo=EASTERN_TZ)
                except ValueError:
                    continue

                # Only keep draws for the target date
                if draw_date.date() != target_date.date():
                    continue

                text_no_date = re.sub(r"[A-Za-z]+ \d{1,2}, \d{4}", "", text)
                nums = [int(n) for n in re.findall(r"\b\d{1,2}\b", text_no_date)]

                g_type = GAME_MAPPING[game_name]["type"]
                expected = {"cashpop": 1, "pick3": 3, "pick4": 4, "pick5": 5, "fantasy5": 5}[g_type]
                if len(nums) < expected:
                    continue

                winning = nums[:expected]
                hour, minute = DRAW_TIMES.get(g_type, (21, 45))

                if g_type == "cashpop":
                    if "MORNING" in text_upper:    hour, minute = 8,  45
                    elif "MATINEE" in text_upper:  hour, minute = 13,  0
                    elif "AFTERNOON" in text_upper: hour, minute = 16, 45
                    elif "EVENING" in text_upper:  hour, minute = 20, 45
                    elif "LATE NIGHT" in text_upper: hour, minute = 23, 45
                elif g_type in ("pick3", "pick4", "pick5"):
                    if "MIDDAY" in text_upper:  hour, minute = 13, 30
                    elif "EVENING" in text_upper: hour, minute = 21, 45

                draw_datetime = draw_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                row = {"draw_datetime": draw_datetime}
                if g_type == "fantasy5":
                    row["numbers"] = winning
                elif g_type == "cashpop":
                    row["number"] = winning[0]
                else:
                    for i, v in enumerate(winning, 1):
                        row[f"digit_{i}"] = v

                if not any(d["draw_datetime"] == draw_datetime for d in parsed_data[game_name]):
                    parsed_data[game_name].append(row)

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    return parsed_data


async def run_backfill(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    logger.info(f"Backfilling {len(dates)} dates: {dates[0]} → {dates[-1]}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        total_inserted = 0

        for date_str in dates:
            logger.info(f"── Scraping {date_str} ──")
            data = await scrape_date(page, date_str)

            async with SessionLocal() as session:
                for game_name, draws in data.items():
                    if not draws:
                        continue
                    model = GAME_MAPPING[game_name]["model"]
                    stmt = insert(model).values(draws).on_conflict_do_nothing(index_elements=["draw_datetime"])
                    await session.execute(stmt)
                    total_inserted += len(draws)
                    logger.info(f"   {game_name}: {len(draws)} draw(s) inserted")
                await session.commit()

            # Brief pause between days to be polite to the server
            await asyncio.sleep(2)

        await browser.close()

    logger.info(f"\n✅ Backfill complete. Total new rows inserted: {total_inserted}")


def main():
    parser = argparse.ArgumentParser(description="Backfill missing lottery draws")
    yesterday = (datetime.now(EASTERN_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--start", default="2026-02-26", help="Start date YYYY-MM-DD (default: 2026-02-26)")
    parser.add_argument("--end",   default=yesterday,    help=f"End date YYYY-MM-DD (default: {yesterday})")
    args = parser.parse_args()
    asyncio.run(run_backfill(args.start, args.end))


if __name__ == "__main__":
    main()
