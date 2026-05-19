"""
api/visuals.py
Visual generation engine for ProbLabs social media posts.
Produces PNG charts via matplotlib (Agg backend — no display required).
"""
from __future__ import annotations

import os
import time
import math
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VISUAL_DIR = "/tmp/social_visuals"
os.makedirs(VISUAL_DIR, exist_ok=True)

ACCENT_PU = "#6366f1"
ACCENT_GR = "#22c55e"
ACCENT_AM = "#f59e0b"
BG_DARK   = "#0f172a"
BG_CARD   = "#1e293b"
TEXT_PRI  = "#f8fafc"
TEXT_MUT  = "#94a3b8"
GRID_LINE = "#334155"

GAME_LABELS = {
    "pick-3":    "Pick 3",
    "pick-4":    "Pick 4",
    "pick-5":    "Pick 5",
    "fantasy-5": "Fantasy 5",
    "cash-pop":  "Cash Pop",
}

# ---------------------------------------------------------------------------
# Theme helper
# ---------------------------------------------------------------------------
def _rcparams() -> dict:
    return {
        "figure.facecolor":  BG_DARK,
        "axes.facecolor":    BG_CARD,
        "axes.edgecolor":    GRID_LINE,
        "axes.labelcolor":   TEXT_PRI,
        "xtick.color":       TEXT_MUT,
        "ytick.color":       TEXT_MUT,
        "text.color":        TEXT_PRI,
        "grid.color":        GRID_LINE,
        "grid.linewidth":    0.6,
        "font.family":       "DejaVu Sans",
        "axes.titlesize":    14,
        "axes.titleweight":  "bold",
        "axes.titlecolor":   TEXT_PRI,
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _get_draws(db, game_ref: str, limit: int = 30):
    """Return up to *limit* draw rows ordered newest-first."""
    from models import DrawPick3, DrawPick4, DrawPick5, DrawFantasy5, DrawCashPop

    model_map = {
        "pick-3":    DrawPick3,
        "pick-4":    DrawPick4,
        "pick-5":    DrawPick5,
        "fantasy-5": DrawFantasy5,
        "cash-pop":  DrawCashPop,
    }
    model = model_map.get(game_ref)
    if model is None:
        return []
    return db.query(model).order_by(model.draw_datetime.desc()).limit(limit).all()


def _extract_digits(draw, game_ref: str) -> list[int]:
    if game_ref == "cash-pop":
        return [int(draw.number)] if draw.number is not None else []
    if game_ref == "fantasy-5":
        return [int(n) for n in (draw.numbers or [])]
    digit_count = {"pick-3": 3, "pick-4": 4, "pick-5": 5}.get(game_ref, 0)
    return [int(getattr(draw, f"digit_{i}")) for i in range(1, digit_count + 1)
            if getattr(draw, f"digit_{i}", None) is not None]


# ---------------------------------------------------------------------------
# Output path
# ---------------------------------------------------------------------------
def _out_path(game_ref: str, vtype: str) -> str:
    ts = int(time.time())
    return os.path.join(VISUAL_DIR, f"{game_ref}_{vtype}_{ts}.png")


# ---------------------------------------------------------------------------
# 1. Stat card  (1200 × 675)
# ---------------------------------------------------------------------------
def generate_stat_card(db, game_ref: str) -> str:
    draws = _get_draws(db, game_ref, limit=50)
    label = GAME_LABELS.get(game_ref, game_ref)
    out   = _out_path(game_ref, "stat_card")

    from collections import Counter
    counts: Counter = Counter()
    for d in draws:
        counts.update(_extract_digits(d, game_ref))

    if not counts:
        counts = Counter({i: 0 for i in range(10)})

    total = sum(counts.values()) or 1
    digits = sorted(counts.keys())
    max_freq = max(counts.values()) if counts else 1

    with plt.rc_context(_rcparams()):
        fig, ax = plt.subplots(figsize=(12, 6.75))
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

        # Top bar
        fig.add_axes([0, 0.88, 1, 0.12]).set_axis_off()
        fig.axes[-1].set_facecolor(ACCENT_PU)
        fig.axes[-1].text(
            0.5, 0.5, f"ProbLabs · {label} · Frequency Analysis",
            ha="center", va="center", fontsize=16, fontweight="bold",
            color="white", transform=fig.axes[-1].transAxes,
        )

        # Bubble layout
        n = len(digits)
        cols = min(n, 10)
        rows = math.ceil(n / cols)
        for idx, digit in enumerate(digits):
            col = idx % cols
            row = idx // cols
            cx = 0.05 + col * (0.9 / max(cols - 1, 1))
            cy = 0.72 - row * 0.28
            freq = counts[digit]
            radius = 0.04 + 0.035 * (freq / max_freq)
            color = ACCENT_GR if freq == max(counts.values()) else (
                "#ef4444" if freq == min(counts.values()) else ACCENT_PU
            )
            circle = mpatches.Circle(
                (cx, cy), radius, color=color, alpha=0.85, transform=ax.transData,
            )
            ax.add_patch(circle)
            ax.text(cx, cy + 0.001, str(digit),
                    ha="center", va="center", fontsize=13, fontweight="bold", color="white")
            ax.text(cx, cy - radius - 0.025, f"{freq}×",
                    ha="center", va="center", fontsize=9, color=TEXT_MUT)

        # Bottom bar — variance
        most_common = counts.most_common()
        hot_d, hot_c = most_common[0]
        cold_d, cold_c = most_common[-1]
        ax.text(0.05, 0.08,
                f"🔥 Hot: {hot_d}  ({hot_c/total*100:.1f}%)    "
                f"❄  Cold: {cold_d}  ({cold_c/total*100:.1f}%)",
                fontsize=12, color=TEXT_MUT, va="center")
        ax.text(0.95, 0.08, f"Last {len(draws)} draws",
                fontsize=10, color=TEXT_MUT, va="center", ha="right")

        fig.savefig(out, dpi=96, bbox_inches="tight",
                    facecolor=BG_DARK, edgecolor="none")
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# 2. Frequency bar  (1200 × 900)
# ---------------------------------------------------------------------------
def generate_frequency_bar(db, game_ref: str) -> str:
    draws = _get_draws(db, game_ref, limit=50)
    label = GAME_LABELS.get(game_ref, game_ref)
    out   = _out_path(game_ref, "frequency_bar")

    from collections import Counter
    counts: Counter = Counter()
    latest_digits: set = set()
    if draws:
        latest_digits = set(_extract_digits(draws[0], game_ref))
    for d in draws:
        counts.update(_extract_digits(d, game_ref))

    if not counts:
        counts = Counter({i: 0 for i in range(10)})

    digits = sorted(counts.keys())
    freqs  = [counts[d] for d in digits]
    colors = [ACCENT_GR if d in latest_digits else ACCENT_PU for d in digits]

    with plt.rc_context(_rcparams()):
        fig, ax = plt.subplots(figsize=(12, 9))
        bars = ax.bar([str(d) for d in digits], freqs, color=colors, width=0.65, zorder=3)
        ax.yaxis.grid(True, zorder=0)
        ax.set_xlabel("Digit", fontsize=12)
        ax.set_ylabel("Frequency", fontsize=12)
        ax.set_title(f"{label} — Digit Frequency (last {len(draws)} draws)", pad=14)

        for bar, freq in zip(bars, freqs):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    str(freq), ha="center", va="bottom", fontsize=10, color=TEXT_PRI)

        legend_elems = [
            mpatches.Patch(color=ACCENT_GR, label="In latest draw"),
            mpatches.Patch(color=ACCENT_PU, label="Historical"),
        ]
        ax.legend(handles=legend_elems, loc="upper right", framealpha=0.3)

        fig.tight_layout()
        fig.savefig(out, dpi=96, bbox_inches="tight",
                    facecolor=BG_DARK, edgecolor="none")
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# 3. Heatmap  (1200 × 900)
# ---------------------------------------------------------------------------
def generate_heatmap(db, game_ref: str) -> str:
    draws = _get_draws(db, game_ref, limit=60)
    label = GAME_LABELS.get(game_ref, game_ref)
    out   = _out_path(game_ref, "heatmap")

    n_draws = min(len(draws), 30)
    matrix  = np.zeros((n_draws, 10), dtype=float)
    for i, draw in enumerate(draws[:n_draws]):
        for dg in _extract_digits(draw, game_ref):
            if 0 <= dg <= 9:
                matrix[i, dg] += 1

    with plt.rc_context(_rcparams()):
        fig, ax = plt.subplots(figsize=(12, 9))
        im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd",
                       interpolation="nearest", vmin=0)
        ax.set_xticks(range(10))
        ax.set_xticklabels([str(i) for i in range(10)])
        ax.set_yticks(range(n_draws))
        ax.set_yticklabels([f"Draw -{i}" for i in range(n_draws)], fontsize=8)
        ax.set_xlabel("Digit", fontsize=12)
        ax.set_title(f"{label} — Draw Heatmap (last {n_draws} draws)", pad=14)

        for row in range(n_draws):
            for col in range(10):
                val = int(matrix[row, col])
                if val:
                    ax.text(col, row, str(val),
                            ha="center", va="center", fontsize=8,
                            color="black" if val > matrix.max() * 0.5 else TEXT_PRI)

        fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="Appearances")
        fig.tight_layout()
        fig.savefig(out, dpi=96, bbox_inches="tight",
                    facecolor=BG_DARK, edgecolor="none")
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# 4. Variance trend  (1200 × 675)
# ---------------------------------------------------------------------------
def generate_variance_trend(db, game_ref: str) -> str:
    draws = _get_draws(db, game_ref, limit=50)
    label = GAME_LABELS.get(game_ref, game_ref)
    out   = _out_path(game_ref, "variance_trend")

    if not draws:
        with plt.rc_context(_rcparams()):
            fig, ax = plt.subplots(figsize=(12, 6.75))
            ax.text(0.5, 0.5, "No data available", ha="center", va="center",
                    fontsize=18, color=TEXT_MUT, transform=ax.transAxes)
            ax.set_axis_off()
            fig.savefig(out, dpi=96, facecolor=BG_DARK, edgecolor="none")
            plt.close(fig)
        return out

    from collections import Counter
    trend: list[float] = []
    all_digits: list[int] = []

    for draw in reversed(draws):  # oldest first
        all_digits.extend(_extract_digits(draw, game_ref))
        if all_digits:
            c = Counter(all_digits)
            hot_freq = c.most_common(1)[0][1]
            trend.append(hot_freq / len(all_digits) * 100)

    x = list(range(len(trend)))
    y = np.array(trend)

    window = 5
    if len(y) >= window:
        kernel = np.ones(window) / window
        ma = np.convolve(y, kernel, mode="valid")
        ma_x = x[window - 1:]
    else:
        ma = y
        ma_x = x

    with plt.rc_context(_rcparams()):
        fig, ax = plt.subplots(figsize=(12, 6.75))
        ax.fill_between(x, y, alpha=0.18, color=ACCENT_PU)
        ax.plot(x, y, color=ACCENT_PU, linewidth=1.5, label="Hot digit %", zorder=3)
        ax.plot(ma_x, ma, color=ACCENT_AM, linewidth=2.2, linestyle="--",
                label=f"{window}-draw MA", zorder=4)

        ax.annotate(f"{y[-1]:.1f}%",
                    xy=(x[-1], y[-1]),
                    xytext=(x[-1] - 2, y[-1] + 1.5),
                    fontsize=9, color=TEXT_PRI,
                    arrowprops={"arrowstyle": "->", "color": TEXT_MUT, "lw": 1})

        ax.yaxis.grid(True)
        ax.set_xlabel("Draw index (oldest → newest)", fontsize=11)
        ax.set_ylabel("Hot digit cumulative %", fontsize=11)
        ax.set_title(f"{label} — Variance Trend", pad=14)
        ax.legend(loc="upper left", framealpha=0.3)

        fig.tight_layout()
        fig.savefig(out, dpi=96, bbox_inches="tight",
                    facecolor=BG_DARK, edgecolor="none")
        plt.close(fig)

    return out


# ---------------------------------------------------------------------------
# Public dispatcher
# ---------------------------------------------------------------------------
VISUAL_TYPES = ("stat_card", "frequency_bar", "heatmap", "variance_trend")

def generate_visual(db, game_ref: str, visual_type: str) -> str:
    """
    Generate a chart PNG and return the absolute path.
    Raises ValueError for unknown visual_type.
    """
    if visual_type == "stat_card":
        return generate_stat_card(db, game_ref)
    if visual_type == "frequency_bar":
        return generate_frequency_bar(db, game_ref)
    if visual_type == "heatmap":
        return generate_heatmap(db, game_ref)
    if visual_type == "variance_trend":
        return generate_variance_trend(db, game_ref)
    raise ValueError(f"Unknown visual_type: {visual_type!r}. "
                     f"Choose from: {VISUAL_TYPES}")
