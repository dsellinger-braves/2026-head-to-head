"""
mlb_schedule_check.py
─────────────────────
Drop-in module for active-stats-pull.py.
Queries the MLB Stats API to decide whether the scraper should run,
and which scoring periods to focus on.

Usage in active-stats-pull.py:
    from mlb_schedule_check import check_game_status, should_scrape, log_status

    if __name__ == "__main__":
        import sys
        mode = sys.argv[1] if len(sys.argv) > 1 else "auto"
        status = check_game_status()
        log_status(status)
        if should_scrape(status, mode):
            main()
        else:
            print("Scraper skipped — no completed games to process yet.")
"""

import sys
import requests
from datetime import date, datetime, timezone, timedelta

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"


def check_game_status(target_date: date | None = None) -> dict:
    """
    Query the MLB schedule for today (or a specific date) and return a
    summary of game completion status.

    Returns dict with:
        total        — total games scheduled today
        final        — games with abstractGameState == "Final"
        live         — games currently in progress
        not_started  — games not yet started
        all_final    — True if every game today has finished
        any_final    — True if at least one game has finished
        no_games     — True if no MLB games scheduled today (off day)
        first_pitch  — UTC datetime of earliest game today (or None)
        last_expected_end — estimated end of last game (start + 3.5 hrs)
        games        — raw list of game dicts from the API
    """
    d = (target_date or date.today()).strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            f"{MLB_API_BASE}/schedule",
            params={"sportId": 1, "date": d, "hydrate": "linescore"},
            timeout=10,
        )
        resp.raise_for_status()
        data  = resp.json()
        games = [g for entry in data.get("dates", []) for g in entry.get("games", [])]
    except Exception as e:
        print(f"  ⚠️  MLB schedule fetch failed: {e} — proceeding with scrape anyway")
        return {"total": 0, "final": 0, "live": 0, "not_started": 0,
                "all_final": False, "any_final": False, "no_games": True,
                "first_pitch": None, "last_expected_end": None, "games": []}

    if not games:
        return {"total": 0, "final": 0, "live": 0, "not_started": 0,
                "all_final": False, "any_final": False, "no_games": True,
                "first_pitch": None, "last_expected_end": None, "games": games}

    n_final       = sum(1 for g in games if g.get("status", {}).get("abstractGameState") == "Final")
    n_live        = sum(1 for g in games if g.get("status", {}).get("abstractGameState") == "Live")
    n_not_started = len(games) - n_final - n_live

    # Parse game start times to estimate when the last game will finish
    start_times = []
    for g in games:
        gt = g.get("gameDate", "")
        if gt:
            try:
                start_times.append(datetime.fromisoformat(gt.replace("Z", "+00:00")))
            except Exception:
                pass

    first_pitch       = min(start_times) if start_times else None
    last_start        = max(start_times) if start_times else None
    last_expected_end = (last_start + timedelta(hours=3, minutes=30)) if last_start else None

    return {
        "total":             len(games),
        "final":             n_final,
        "live":              n_live,
        "not_started":       n_not_started,
        "all_final":         n_live == 0 and n_not_started == 0 and n_final > 0,
        "any_final":         n_final > 0,
        "no_games":          False,
        "first_pitch":       first_pitch,
        "last_expected_end": last_expected_end,
        "games":             games,
    }


def log_status(status: dict) -> None:
    """Print a human-readable summary of today's game status."""
    if status["no_games"]:
        print("📅 No MLB games scheduled today.")
        return

    now_utc = datetime.now(timezone.utc)
    print(f"⚾ MLB today: {status['total']} games — "
          f"{status['final']} final, {status['live']} live, "
          f"{status['not_started']} not started")

    if status["first_pitch"]:
        fp_et = status["first_pitch"].astimezone(timezone(timedelta(hours=-4)))
        print(f"   First pitch: {fp_et.strftime('%I:%M %p ET')}")
    if status["last_expected_end"]:
        le_et = status["last_expected_end"].astimezone(timezone(timedelta(hours=-4)))
        print(f"   Last game est. end: {le_et.strftime('%I:%M %p ET')}")

    now_et = now_utc.astimezone(timezone(timedelta(hours=-4)))
    print(f"   Current time: {now_et.strftime('%I:%M %p ET')}")

    if status["all_final"]:
        print("✅ All games finished — full scrape will run.")
    elif status["live"] > 0:
        print(f"🔴 {status['live']} game(s) still live — partial scrape (will re-run later).")
    elif status["not_started"] > 0 and status["any_final"]:
        print(f"⏳ Some games finished, {status['not_started']} not started — partial scrape.")
    elif not status["any_final"]:
        print("⏰ No games finished yet.")


def should_scrape(status: dict, mode: str = "auto") -> bool:
    """
    Decide whether the scraper should proceed.

    mode="force" — always run regardless of game status
    mode="auto"  — skip only if no games are finished at all today

    We prefer scraping partial stats over skipping entirely, because:
    - A later run (or next morning's run) will overwrite with complete stats
    - Partial stats are useful for /live context and mid-game standings
    - The only case worth skipping is truly "nothing to scrape" (no games started)
    """
    if mode == "force":
        print("🚀 Force mode — skipping game status check.")
        return True

    if status["no_games"]:
        print("📅 Off day — no scrape needed.")
        return False

    if not status["any_final"] and status["live"] == 0:
        # No games have finished AND none are in progress — too early
        print("⏰ No games finished or in progress yet — skipping this run.")
        return False

    # Games are either live or finished — proceed
    return True


def get_periods_to_focus(status: dict, current_period: int) -> list[int]:
    """
    Return the list of scoring periods worth re-scraping on this run.
    Focus on recent periods to avoid reprocessing the full season every run.

    - Always include today's period and yesterday's
    - If any games are still live, also include the last 2 days
      (late west coast stats sometimes affect the previous scoring period)
    """
    focus = list({current_period, max(1, current_period - 1)})
    if status.get("live", 0) > 0:
        focus.append(max(1, current_period - 2))
    return sorted(set(focus))
