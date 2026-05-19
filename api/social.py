"""
api/social.py
Social media post scheduler — FastAPI router + APScheduler.

Uses its own sync SQLAlchemy engine to avoid circular imports with main.py.
"""
from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Own DB engine (avoids circular import with main.py's SessionLocal)
# ---------------------------------------------------------------------------
_engine = None
_SessionLocal = None


def _get_engine():
    global _engine, _SessionLocal
    if _engine is None:
        url = (os.getenv("DATABASE_URL") or "").strip()
        if not url:
            raise RuntimeError("DATABASE_URL not set")
        _engine = create_engine(url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
    return _engine


def _get_db():
    _get_engine()
    db = _SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Admin auth (mirrors main.py — no import needed)
# ---------------------------------------------------------------------------
def require_admin(x_admin_key: Optional[str] = Header(None)):
    key = (os.getenv("ADMIN_API_KEY") or "").strip()
    if not key:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not configured")
    if not x_admin_key or x_admin_key != key:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scheduled_posts (
    id           SERIAL PRIMARY KEY,
    platform     VARCHAR(20)  NOT NULL,          -- 'x' | 'facebook' | 'reddit'
    content      TEXT         NOT NULL,
    scheduled_at TIMESTAMPTZ  NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending|sent|failed|paused
    game_ref     VARCHAR(50),
    subreddit    VARCHAR(100),
    visual_type  VARCHAR(50),
    visual_path  TEXT,
    visual_url   TEXT,
    week_batch   VARCHAR(20),                    -- e.g. '2025-W21'
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_sp_status       ON scheduled_posts(status);
CREATE INDEX IF NOT EXISTS idx_sp_scheduled_at ON scheduled_posts(scheduled_at);
CREATE INDEX IF NOT EXISTS idx_sp_week_batch   ON scheduled_posts(week_batch);
"""


def ensure_social_table() -> None:
    eng = _get_engine()
    with eng.connect() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.commit()


# ---------------------------------------------------------------------------
# Weekly calendar constants
# ---------------------------------------------------------------------------
# 7 daily X posts — one game per day (Mon-Sun)
_X_GAME_ROTATION = [
    "pick-3",    # Mon
    "pick-4",    # Tue
    "pick-5",    # Wed
    "fantasy-5", # Thu
    "cash-pop",  # Fri
    "pick-3",    # Sat
    "fantasy-5", # Sun
]

# Facebook: Mon(0), Wed(2), Fri(4), Sun(6) — index = weekday
_FB_SCHEDULE = [
    (0, "pick-5"),
    (2, "fantasy-5"),
    (4, "cash-pop"),
    (6, "pick-3"),
]

# Reddit: Tue(1), Thu(3), Sat(5)
_REDDIT_SCHEDULE = [
    (1, "pick-4",    "r/dataisbeautiful"),
    (3, "fantasy-5", "r/florida"),
    (5, "cash-pop",  "r/dataisbeautiful"),
]

EASTERN = timezone(timedelta(hours=-4))  # EDT (UTC-4); adjust to -5 for EST


# ---------------------------------------------------------------------------
# Content formatters
# ---------------------------------------------------------------------------
def _format_content(platform: str, game_ref: str, draw=None, subreddit: str = "") -> str:
    game_name = {
        "pick-3":    "Pick 3",
        "pick-4":    "Pick 4",
        "pick-5":    "Pick 5",
        "fantasy-5": "Fantasy 5",
        "cash-pop":  "Cash Pop",
    }.get(game_ref, game_ref)

    if platform == "x":
        return (
            f"📊 {game_name} digit frequency analysis — "
            f"which numbers are running hot this week?\n\n"
            f"#FloridaLottery #{game_ref.replace('-','')} "
            f"#ProbabilityAI #DataScience #LotteryStats"
        )
    if platform == "facebook":
        return (
            f"🎯 Florida {game_name} — Weekly Statistical Breakdown\n\n"
            f"Our AI engine has processed the latest draw history. "
            f"See which digits are appearing most often and which ones are overdue.\n\n"
            f"📈 Full analysis at problabs.net\n"
            f"#FloridaLottery #Statistics #ProbabilityAI"
        )
    if platform == "reddit":
        sub = subreddit or "r/dataisbeautiful"
        return (
            f"Florida {game_name} digit frequency heatmap — last 30 draws [OC]\n\n"
            f"Built with Python/matplotlib. Data sourced from the Florida Lottery "
            f"historical draw archive. Hot/cold analysis based on rolling 30-day window.\n\n"
            f"Full stats: problabs.net | Posted to {sub}"
        )
    return f"{game_name} — statistical update from ProbLabs"


# ---------------------------------------------------------------------------
# Row → dict
# ---------------------------------------------------------------------------
def _row_to_dict(row) -> dict:
    def _iso(val):
        if val is None:
            return None
        if isinstance(val, datetime):
            return val.isoformat()
        return str(val)

    return {
        "id":           row[0],
        "platform":     row[1],
        "content":      row[2],
        "scheduled_at": _iso(row[3]),
        "status":       row[4],
        "game_ref":     row[5],
        "subreddit":    row[6],
        "visual_type":  row[7],
        "visual_path":  row[8],
        "visual_url":   row[9],
        "week_batch":   row[10],
        "created_at":   _iso(row[11]),
        "sent_at":      _iso(row[12]),
    }


# ---------------------------------------------------------------------------
# Week plan builder
# ---------------------------------------------------------------------------
def _build_week_plan(db: Session, start: date, week_batch: str) -> list[dict]:
    """Return list of post dicts (not yet inserted) for the 7-day week."""
    posts: list[dict] = []

    for day_offset in range(7):
        day = start + timedelta(days=day_offset)
        weekday = day.weekday()  # 0=Mon … 6=Sun

        # --- X post (every day 10:00 ET) ---
        game_ref = _X_GAME_ROTATION[weekday]
        posts.append({
            "platform":     "x",
            "content":      _format_content("x", game_ref),
            "scheduled_at": datetime(day.year, day.month, day.day, 10, 0, tzinfo=EASTERN),
            "game_ref":     game_ref,
            "subreddit":    None,
            "visual_type":  "frequency_bar",
            "week_batch":   week_batch,
        })

        # --- Facebook (Mon/Wed/Fri/Sun 12:00 ET) ---
        for fb_day, fb_game in _FB_SCHEDULE:
            if weekday == fb_day:
                posts.append({
                    "platform":     "facebook",
                    "content":      _format_content("facebook", fb_game),
                    "scheduled_at": datetime(day.year, day.month, day.day, 12, 0, tzinfo=EASTERN),
                    "game_ref":     fb_game,
                    "subreddit":    None,
                    "visual_type":  "heatmap",
                    "week_batch":   week_batch,
                })

        # --- Reddit (Tue/Thu/Sat 11:00 ET) ---
        for rd_day, rd_game, rd_sub in _REDDIT_SCHEDULE:
            if weekday == rd_day:
                posts.append({
                    "platform":     "reddit",
                    "content":      _format_content("reddit", rd_game, subreddit=rd_sub),
                    "scheduled_at": datetime(day.year, day.month, day.day, 11, 0, tzinfo=EASTERN),
                    "game_ref":     rd_game,
                    "subreddit":    rd_sub,
                    "visual_type":  "variance_trend",
                    "week_batch":   week_batch,
                })

    posts.sort(key=lambda p: p["scheduled_at"])
    return posts


# ---------------------------------------------------------------------------
# Platform dispatchers
# ---------------------------------------------------------------------------
def _post_to_x(content: str, image_path: str) -> str:
    import tweepy  # type: ignore
    consumer_key    = os.environ["X_CONSUMER_KEY"]
    consumer_secret = os.environ["X_CONSUMER_SECRET"]
    access_token    = os.environ["X_ACCESS_TOKEN"]
    access_secret   = os.environ["X_ACCESS_TOKEN_SECRET"]

    auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret,
                                    access_token, access_secret)
    api  = tweepy.API(auth)
    client = tweepy.Client(
        consumer_key=consumer_key, consumer_secret=consumer_secret,
        access_token=access_token, access_token_secret=access_secret,
    )
    media = api.media_upload(filename=image_path)
    resp  = client.create_tweet(text=content, media_ids=[media.media_id])
    return f"https://twitter.com/i/web/status/{resp.data['id']}"


def _post_to_reddit(content: str, image_path: str, subreddit: str) -> str:
    import praw  # type: ignore
    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        username=os.environ["REDDIT_USERNAME"],
        password=os.environ["REDDIT_PASSWORD"],
        user_agent="ProbLabs social bot v1.0",
    )
    lines  = content.splitlines()
    title  = lines[0][:300] if lines else "ProbLabs Analysis"
    sub    = subreddit.lstrip("r/")
    submission = reddit.subreddit(sub).submit_image(
        title=title, image_path=image_path, nsfw=False,
    )
    return f"https://reddit.com{submission.permalink}"


def _post_to_facebook(content: str, image_path: str) -> str:
    import requests  # type: ignore
    page_id    = os.environ["FACEBOOK_PAGE_ID"]
    page_token = os.environ["FACEBOOK_PAGE_TOKEN"]
    with open(image_path, "rb") as fh:
        resp = requests.post(
            f"https://graph.facebook.com/v19.0/{page_id}/photos",
            data={"caption": content, "access_token": page_token},
            files={"source": fh},
            timeout=30,
        )
    resp.raise_for_status()
    data = resp.json()
    return f"https://www.facebook.com/{data.get('post_id', data.get('id', ''))}"


# ---------------------------------------------------------------------------
# Scheduler job
# ---------------------------------------------------------------------------
_scheduler: Optional[BackgroundScheduler] = None


def _dispatch_due_posts():
    """Called every 5 minutes by APScheduler."""
    try:
        _get_engine()
        db = _SessionLocal()
        try:
            now = datetime.now(timezone.utc)
            rows = db.execute(
                text("""
                    SELECT id, platform, content, scheduled_at, status,
                           game_ref, subreddit, visual_type, visual_path,
                           visual_url, week_batch, created_at, sent_at
                    FROM scheduled_posts
                    WHERE status = 'pending' AND scheduled_at <= :now
                    ORDER BY scheduled_at
                """),
                {"now": now},
            ).fetchall()

            for row in rows:
                post_id     = row[0]
                platform    = row[1]
                content     = row[2]
                game_ref    = row[5]
                subreddit   = row[6]
                visual_type = row[7]
                visual_path = row[8]

                try:
                    # Ensure visual exists
                    if not visual_path or not os.path.exists(visual_path):
                        from api.visuals import generate_visual
                        visual_path = generate_visual(db, game_ref, visual_type or "frequency_bar")
                        db.execute(
                            text("UPDATE scheduled_posts SET visual_path=:p WHERE id=:id"),
                            {"p": visual_path, "id": post_id},
                        )

                    # Dispatch
                    if platform == "x":
                        url = _post_to_x(content, visual_path)
                    elif platform == "reddit":
                        url = _post_to_reddit(content, visual_path, subreddit or "r/dataisbeautiful")
                    elif platform == "facebook":
                        url = _post_to_facebook(content, visual_path)
                    else:
                        raise ValueError(f"Unknown platform: {platform!r}")

                    db.execute(
                        text("""
                            UPDATE scheduled_posts
                            SET status='sent', visual_url=:url, sent_at=:now
                            WHERE id=:id
                        """),
                        {"url": url, "now": datetime.now(timezone.utc), "id": post_id},
                    )
                except Exception as exc:
                    logger.error("Failed to dispatch post %s: %s", post_id, exc)
                    db.execute(
                        text("UPDATE scheduled_posts SET status='failed' WHERE id=:id"),
                        {"id": post_id},
                    )

                db.commit()
        finally:
            db.close()
    except Exception as exc:
        logger.error("_dispatch_due_posts error: %s", exc)


def start_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(_dispatch_due_posts, "interval", minutes=5, id="dispatch_social")
    _scheduler.start()
    logger.info("Social post scheduler started (5-min interval)")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Social post scheduler stopped")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/api/social", tags=["social"])


# ── GET /api/social/weeks ──────────────────────────────────────────────────
@router.get("/weeks", dependencies=[Depends(require_admin)])
def list_weeks(db: Session = Depends(_get_db)):
    rows = db.execute(
        text("""
            SELECT week_batch,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN status='paused'  THEN 1 ELSE 0 END) AS paused,
                   SUM(CASE WHEN status='sent'    THEN 1 ELSE 0 END) AS sent,
                   SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
                   MIN(scheduled_at) AS first_post,
                   MAX(scheduled_at) AS last_post
            FROM scheduled_posts
            GROUP BY week_batch
            ORDER BY week_batch DESC
        """)
    ).fetchall()
    return [
        {
            "week_batch": r[0], "total": r[1],
            "pending": r[2], "paused": r[3], "sent": r[4], "failed": r[5],
            "first_post": r[6].isoformat() if r[6] else None,
            "last_post":  r[7].isoformat() if r[7] else None,
        }
        for r in rows
    ]


# ── GET /api/social/posts ──────────────────────────────────────────────────
@router.get("/posts", dependencies=[Depends(require_admin)])
def list_posts(
    week_batch: Optional[str] = None,
    platform:   Optional[str] = None,
    status:     Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(_get_db),
):
    where = ["1=1"]
    params: dict[str, Any] = {"lim": min(limit, 500), "off": max(offset, 0)}
    if week_batch:
        where.append("week_batch = :wb"); params["wb"] = week_batch
    if platform:
        where.append("platform = :plat"); params["plat"] = platform
    if status:
        where.append("status = :st"); params["st"] = status

    q = f"""
        SELECT id, platform, content, scheduled_at, status,
               game_ref, subreddit, visual_type, visual_path,
               visual_url, week_batch, created_at, sent_at
        FROM scheduled_posts
        WHERE {' AND '.join(where)}
        ORDER BY scheduled_at
        LIMIT :lim OFFSET :off
    """
    rows = db.execute(text(q), params).fetchall()
    return [_row_to_dict(r) for r in rows]


# ── GET /api/social/next-up ────────────────────────────────────────────────
@router.get("/next-up", dependencies=[Depends(require_admin)])
def next_up(db: Session = Depends(_get_db)):
    rows = db.execute(
        text("""
            SELECT id, platform, content, scheduled_at, status,
                   game_ref, subreddit, visual_type, visual_path,
                   visual_url, week_batch, created_at, sent_at
            FROM scheduled_posts
            WHERE status = 'pending'
            ORDER BY scheduled_at
            LIMIT 5
        """)
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


# ── POST /api/social/schedule-week ────────────────────────────────────────
@router.post("/schedule-week", dependencies=[Depends(require_admin)])
async def schedule_week(request_body: dict, db: Session = Depends(_get_db)):
    """
    Body: { "start_date": "YYYY-MM-DD", "dry_run": false, "force": false }
    If omitted, start_date defaults to next Monday.
    """
    from fastapi import Request
    dry_run = bool(request_body.get("dry_run", False))
    force   = bool(request_body.get("force", False))
    start_s = (request_body.get("start_date") or "").strip()

    if start_s:
        try:
            start = date.fromisoformat(start_s)
        except ValueError:
            raise HTTPException(status_code=400, detail="start_date must be YYYY-MM-DD")
    else:
        today = datetime.now(EASTERN).date()
        days_until_monday = (7 - today.weekday()) % 7 or 7
        start = today + timedelta(days=days_until_monday)

    # ISO week label e.g. "2025-W21"
    week_batch = f"{start.isocalendar().year}-W{start.isocalendar().week:02d}"

    # Conflict check
    existing = db.execute(
        text("SELECT COUNT(*) FROM scheduled_posts WHERE week_batch=:wb"),
        {"wb": week_batch},
    ).scalar()
    if existing and not force:
        raise HTTPException(
            status_code=409,
            detail=f"Week {week_batch} already has {existing} posts. "
                   f"Pass force=true to replace, or DELETE /api/social/week/{week_batch} first.",
        )
    if existing and force:
        db.execute(
            text("DELETE FROM scheduled_posts WHERE week_batch=:wb"),
            {"wb": week_batch},
        )
        db.commit()

    plan = _build_week_plan(db, start, week_batch)

    if dry_run:
        return {"dry_run": True, "week_batch": week_batch, "count": len(plan), "posts": plan}

    # Insert
    for p in plan:
        db.execute(
            text("""
                INSERT INTO scheduled_posts
                    (platform, content, scheduled_at, status, game_ref,
                     subreddit, visual_type, week_batch)
                VALUES
                    (:platform, :content, :scheduled_at, 'pending', :game_ref,
                     :subreddit, :visual_type, :week_batch)
            """),
            {
                "platform":     p["platform"],
                "content":      p["content"],
                "scheduled_at": p["scheduled_at"],
                "game_ref":     p["game_ref"],
                "subreddit":    p["subreddit"],
                "visual_type":  p["visual_type"],
                "week_batch":   p["week_batch"],
            },
        )
    db.commit()
    return {"ok": True, "week_batch": week_batch, "inserted": len(plan)}


# ── DELETE /api/social/week/{week_batch} ──────────────────────────────────
@router.delete("/week/{week_batch}", dependencies=[Depends(require_admin)])
def delete_week(week_batch: str, db: Session = Depends(_get_db)):
    result = db.execute(
        text("DELETE FROM scheduled_posts WHERE week_batch=:wb AND status!='sent'"),
        {"wb": week_batch},
    )
    db.commit()
    return {"ok": True, "deleted": result.rowcount}


# ── POST /api/social/generate ─────────────────────────────────────────────
@router.post("/generate", dependencies=[Depends(require_admin)])
async def generate_post(request_body: dict, db: Session = Depends(_get_db)):
    """
    Body: {
      "platform": "x"|"facebook"|"reddit",
      "game_ref": "pick-3"|...,
      "scheduled_at": "2025-05-20T10:00:00-04:00",
      "visual_type": "frequency_bar"|...,
      "subreddit": "r/dataisbeautiful"  (optional, reddit only)
    }
    """
    platform    = request_body.get("platform", "x")
    game_ref    = request_body.get("game_ref", "pick-3")
    scheduled_s = request_body.get("scheduled_at")
    visual_type = request_body.get("visual_type", "frequency_bar")
    subreddit   = request_body.get("subreddit") or None

    if not scheduled_s:
        raise HTTPException(status_code=400, detail="scheduled_at is required")
    try:
        sched = datetime.fromisoformat(scheduled_s)
    except ValueError:
        raise HTTPException(status_code=400, detail="scheduled_at must be ISO-8601")

    content = _format_content(platform, game_ref, subreddit=subreddit or "")
    row = db.execute(
        text("""
            INSERT INTO scheduled_posts
                (platform, content, scheduled_at, status, game_ref,
                 subreddit, visual_type)
            VALUES
                (:platform, :content, :scheduled_at, 'pending', :game_ref,
                 :subreddit, :visual_type)
            RETURNING id
        """),
        {
            "platform": platform, "content": content, "scheduled_at": sched,
            "game_ref": game_ref, "subreddit": subreddit, "visual_type": visual_type,
        },
    ).fetchone()
    db.commit()
    return {"ok": True, "id": row[0]}


# ── POST /api/social/posts/{id}/pause ─────────────────────────────────────
@router.post("/posts/{post_id}/pause", dependencies=[Depends(require_admin)])
def pause_post(post_id: int, db: Session = Depends(_get_db)):
    db.execute(
        text("UPDATE scheduled_posts SET status='paused' WHERE id=:id AND status='pending'"),
        {"id": post_id},
    )
    db.commit()
    return {"ok": True}


# ── POST /api/social/posts/{id}/resume ────────────────────────────────────
@router.post("/posts/{post_id}/resume", dependencies=[Depends(require_admin)])
def resume_post(post_id: int, db: Session = Depends(_get_db)):
    db.execute(
        text("UPDATE scheduled_posts SET status='pending' WHERE id=:id AND status='paused'"),
        {"id": post_id},
    )
    db.commit()
    return {"ok": True}


# ── DELETE /api/social/posts/{id} ─────────────────────────────────────────
@router.delete("/posts/{post_id}", dependencies=[Depends(require_admin)])
def delete_post(post_id: int, db: Session = Depends(_get_db)):
    db.execute(
        text("DELETE FROM scheduled_posts WHERE id=:id AND status!='sent'"),
        {"id": post_id},
    )
    db.commit()
    return {"ok": True}


# ── POST /api/social/posts/{id}/retry ─────────────────────────────────────
@router.post("/posts/{post_id}/retry", dependencies=[Depends(require_admin)])
def retry_post(post_id: int, db: Session = Depends(_get_db)):
    db.execute(
        text("UPDATE scheduled_posts SET status='pending' WHERE id=:id AND status='failed'"),
        {"id": post_id},
    )
    db.commit()
    return {"ok": True}


# ── POST /api/social/pause-all ────────────────────────────────────────────
@router.post("/pause-all", dependencies=[Depends(require_admin)])
def pause_all(platform: Optional[str] = None, db: Session = Depends(_get_db)):
    q = "UPDATE scheduled_posts SET status='paused' WHERE status='pending'"
    params: dict = {}
    if platform:
        q += " AND platform=:plat"
        params["plat"] = platform
    db.execute(text(q), params)
    db.commit()
    return {"ok": True}


# ── POST /api/social/resume-all ───────────────────────────────────────────
@router.post("/resume-all", dependencies=[Depends(require_admin)])
def resume_all(platform: Optional[str] = None, db: Session = Depends(_get_db)):
    q = "UPDATE scheduled_posts SET status='pending' WHERE status='paused'"
    params: dict = {}
    if platform:
        q += " AND platform=:plat"
        params["plat"] = platform
    db.execute(text(q), params)
    db.commit()
    return {"ok": True}


# ── GET /api/social/visual/{post_id} ──────────────────────────────────────
@router.get("/visual/{post_id}", dependencies=[Depends(require_admin)])
def get_visual(post_id: int, db: Session = Depends(_get_db)):
    row = db.execute(
        text("SELECT visual_path, game_ref, visual_type FROM scheduled_posts WHERE id=:id"),
        {"id": post_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    visual_path, game_ref, visual_type = row

    if not visual_path or not os.path.exists(visual_path):
        from api.visuals import generate_visual
        visual_path = generate_visual(db, game_ref, visual_type or "frequency_bar")
        db.execute(
            text("UPDATE scheduled_posts SET visual_path=:p WHERE id=:id"),
            {"p": visual_path, "id": post_id},
        )
        db.commit()

    return FileResponse(visual_path, media_type="image/png")
