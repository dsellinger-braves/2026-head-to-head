"""
HEFTYSTRONG Fantasy Baseball Discord Bot
/ask [question] slash command — powered by Supabase + Gemini + MLB Stats API
"""

import os
import re
import json
import time
import asyncio
import requests
import discord
from collections import defaultdict
from discord import app_commands
from datetime import date, timedelta, datetime, timezone
from supabase import create_client, Client
from historical import (
    load_historical_data, format_owner_history,
    format_league_champions, format_all_active_owner_summaries,
    HISTORICAL_OWNER_MAP,
)
import google.genai as genai

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
SUPABASE_URL      = os.environ.get("SUPABASE_URL")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY")

# ESPN Fantasy API — used for real-time roster lookups in /live
# Add ESPN_S2 and ESPN_SWID to Railway env vars (same values as GitHub Actions secrets).
# If the league is public these can be empty strings; if private they are required.
LEAGUE_ID  = 130215
YEAR       = 2026
ESPN_S2    = os.environ.get("ESPN_S2",   "")
ESPN_SWID  = os.environ.get("ESPN_SWID", "")

TEAM_NAMES = {
    1:  "Tim",
    2:  "Adrian",
    3:  "Garrett",
    5:  "Dan",
    6:  "Anil",
    8:  "Alex",
    12: "Will",
    13: "Mark",
    14: "Preston",
}

DISCORD_TO_OWNER = {
    "dsellinger":  "Dan",
    "aznchuy":     "Tim",
    "adriaxx":     "Adrian",
    "ghutch":      "Garrett",
    "anilbhairo":  "Anil",
    "ay0h":        "Alex",
    "senorspice":  "Will",
    "mrussell38":  "Mark",
    "pston3":      "Preston",
}

TEAM_NAME_TO_ID = {v.lower(): k for k, v in TEAM_NAMES.items()}
TEAM_ALIASES    = {"daniel": 5, "danny": 5}
TEAM_NAME_TO_ID.update(TEAM_ALIASES)

TRANSACTION_KEYWORDS = [
    "trade", "traded", "add", "added", "drop", "dropped",
    "pickup", "waiver", "acquire", "acquisition", "grade",
    "worth it", "good move", "bad move", "fa", "free agent",
]
HISTORY_KEYWORDS = [
    "history", "historical", "all time", "all-time", "ever",
    "championship", "championships", "won", "champion",
    "best season", "worst season", "years", "since the start",
    "back in", "previous", "past seasons",
]
TREND_KEYWORDS = [
    "recent", "lately", "this week", "last week", "trending",
    "hot", "cold", "streak", "momentum", "moving",
]
PLAYER_KEYWORDS = [
    "who leads", "who has the most", "who has the best", "top player",
    "best pitcher", "best hitter", "which player", "who is leading",
    "strikeout leader", "home run leader", "hr leader", "era", "whip",
]
LIVE_KEYWORDS = [
    "live", "right now", "tonight", "today", "in game", "currently",
    "in progress", "playing now", "happening", "going on", "score",
    "game today", "games today", "pitching today", "starting today",
    "how is", "how's", "what is", "what's", "probable", "starter",
]

ROTO_CATS = ["R", "HR", "RBI", "OBP", "SB", "QS", "ERA", "WHIP", "K", "SV_HD"]
CAT_HIGHER_IS_BETTER = {
    "R": True, "HR": True, "RBI": True, "OBP": True, "SB": True,
    "QS": True, "ERA": False, "WHIP": False, "K": True, "SV_HD": True,
}
CAT_DISPLAY = {
    "R": "R", "HR": "HR", "RBI": "RBI", "OBP": "OBP", "SB": "SB",
    "QS": "QS", "ERA": "ERA", "WHIP": "WHIP", "K": "K", "SV_HD": "SV+H",
}

BENCH_IL_SLOTS = {16, 17, 20, 21, 22}
SEASON_START   = date(2026, 3, 25)

# ---------------------------------------------------------------------------
# MLB STATS API  (free, no key required)
# ---------------------------------------------------------------------------

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"

# Stable MLB team ID → abbreviation lookup.
# The schedule API's hydrate=teams doesn't reliably return abbreviation,
# so we use this as the primary source and fall back to the API value.
MLB_TEAM_ABBREV = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC",
    113: "CIN", 114: "CLE", 115: "COL", 116: "DET", 117: "HOU",
    118: "KC",  119: "LAD", 120: "WSH", 121: "NYM", 133: "OAK",
    134: "PIT", 135: "SD",  136: "SEA", 137: "SF",  138: "STL",
    139: "TB",  140: "TEX", 141: "TOR", 142: "MIN", 143: "PHI",
    144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}

# In-memory cache — TTL 2 minutes so repeated /ask calls don't hammer the API
_mlb_cache: dict[str, tuple[float, object]] = {}
MLB_CACHE_TTL = 120


def _mlb_get(url: str, params: dict | None = None) -> dict | None:
    cache_key = url + str(params or "")
    if cache_key in _mlb_cache:
        ts, data = _mlb_cache[cache_key]
        if time.time() - ts < MLB_CACHE_TTL:
            return data
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        _mlb_cache[cache_key] = (time.time(), data)
        return data
    except Exception as e:
        print(f"  MLB API error ({url}): {e}")
        return None


def _team_abbrev(team_dict: dict) -> str:
    """
    Resolve a team dict from the MLB API to a 2-3 letter abbreviation.
    Prefers the hardcoded lookup (more reliable) over the API's abbreviation field.
    """
    team_id = team_dict.get("id", 0)
    if team_id in MLB_TEAM_ABBREV:
        return MLB_TEAM_ABBREV[team_id]
    abbrev = team_dict.get("abbreviation", "")
    if abbrev:
        return abbrev
    return team_dict.get("name", "???")[:3].upper()


def _mlb_ip_to_decimal(ip_str) -> float:
    """
    MLB API returns IP as 'X.Y' where Y is outs (0, 1, or 2), not fractions.
    '6.2' = 6 innings + 2 outs = 6.667 real innings.
    """
    try:
        s = str(ip_str)
        if "." in s:
            inn, outs = s.split(".", 1)
            return int(inn) + int(outs) / 3
        return float(s)
    except Exception:
        return 0.0


def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation and suffixes for fuzzy matching."""
    name = name.lower().strip()
    name = re.sub(r"[.\'\-]", "", name)
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", name).strip()
    return name


def build_roster_name_map(records: list[dict]) -> dict[str, dict]:
    """
    Build {normalized_name: {owner, team_id, full_name}} from a set of records.
    Pass the CURRENT PERIOD'S active records so roster is up-to-date.
    """
    roster: dict[str, dict] = {}
    for row in records:
        key = normalize_name(row["full_name"])
        if key not in roster:
            roster[key] = {
                "full_name": row["full_name"],
                "owner":     TEAM_NAMES.get(row["team_id"], f"T{row['team_id']}"),
                "team_id":   row["team_id"],
            }
    return roster


def _match_player(mlb_name: str, roster_map: dict) -> dict | None:
    """Try to match an MLB API player name to a fantasy roster entry."""
    key = normalize_name(mlb_name)
    if key in roster_map:
        return roster_map[key]
    # Fallback: first initial + last name
    parts = key.split()
    if len(parts) >= 2:
        abbrev = parts[0][0] + " " + parts[-1]
        for rkey, rval in roster_map.items():
            rparts = rkey.split()
            if len(rparts) >= 2 and rparts[0][0] + " " + rparts[-1] == abbrev:
                return rval
    return None


def fetch_mlb_schedule_today() -> list[dict]:
    """Return today's MLB games with status, linescore, and probable pitchers."""
    today_str = date.today().strftime("%Y-%m-%d")
    data = _mlb_get(
        f"{MLB_API_BASE}/schedule",
        params={
            "sportId": 1,
            "date":    today_str,
            "hydrate": "probablePitcher,linescore,teams",
        },
    )
    if not data:
        return []
    games = []
    for date_entry in data.get("dates", []):
        games.extend(date_entry.get("games", []))
    return games


def fetch_mlb_boxscore(game_pk: int) -> dict | None:
    """Return live/final feed for a game."""
    return _mlb_get(f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live")


def _game_time_et(game: dict) -> str:
    """Parse the game's UTC start time and return a human-readable ET string."""
    game_time = game.get("gameDate", "")
    if not game_time:
        return ""
    try:
        dt = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
        et = dt.astimezone(timezone(timedelta(hours=-4)))  # EDT
        return et.strftime("%-I:%M %p ET")
    except Exception:
        return ""


def _game_status_label(game: dict) -> str:
    """Return a short status string: inning, FINAL, or start time."""
    state = game.get("status", {}).get("abstractGameState", "")
    if state == "Live":
        inning = game.get("linescore", {}).get("currentInning", "?")
        half   = game.get("linescore", {}).get("inningHalf", "")[:3].upper()
        return f"{half} {inning}"
    if state == "Final":
        return "FINAL"
    t = _game_time_et(game)
    return t if t else "TBD"


def _rostered_players_in_game(game_pk: int, roster_map: dict) -> dict:
    """
    Fetch boxscore and return rostered players split into hitters and pitchers.
    Hitters: starters only (battingOrder % 100 == 0).
    Pitchers: anyone who has actually thrown a pitch (IP > 0).
    Returns {"hitters": [str], "pitchers": [str]}
    """
    feed = fetch_mlb_boxscore(game_pk)
    if not feed:
        return {"hitters": [], "pitchers": []}

    hitters, pitchers = [], []
    box = feed.get("liveData", {}).get("boxscore", {})

    for side in ("away", "home"):
        players = box.get("teams", {}).get(side, {}).get("players", {})
        for _, pdata in players.items():
            mlb_name = pdata.get("person", {}).get("fullName", "")
            fantasy  = _match_player(mlb_name, roster_map)
            if not fantasy:
                continue

            stats = pdata.get("stats", {})
            bat   = stats.get("batting",  {})
            pit   = stats.get("pitching", {})

            # --- Hitters: starters only via battingOrder ---
            batting_order = pdata.get("battingOrder", "")
            is_starter = (
                batting_order != ""
                and str(batting_order).isdigit()
                and int(batting_order) % 100 == 0
            )
            ab = bat.get("atBats", 0)
            pa = bat.get("plateAppearances", 0)

            if is_starter and (ab > 0 or pa > 0):
                h   = bat.get("hits",       0)
                hr  = bat.get("homeRuns",   0)
                rbi = bat.get("rbi",        0)
                r   = bat.get("runs",       0)
                sb  = bat.get("stolenBases",0)
                bb  = bat.get("baseOnBalls",0)
                line = f"{mlb_name} ({fantasy['owner']}): {h}/{ab}"
                extras = []
                if hr:  extras.append(f"{hr}HR")
                if rbi: extras.append(f"{rbi}RBI")
                if r:   extras.append(f"{r}R")
                if sb:  extras.append(f"{sb}SB")
                if bb:  extras.append(f"{bb}BB")
                if extras:
                    line += " " + " ".join(extras)
                hitters.append(line)

            # --- Pitchers: anyone who threw at least one pitch ---
            elif pit.get("inningsPitched") and pit["inningsPitched"] not in ("0.0", "0"):
                ip_str = pit.get("inningsPitched", "0.0")
                ip_dec = _mlb_ip_to_decimal(ip_str)
                k      = pit.get("strikeOuts", 0)
                er     = pit.get("earnedRuns", 0)
                svhd   = pit.get("saves", 0) + pit.get("holds", 0)
                qs     = ip_dec >= 6.0 and er <= 3
                line   = f"{mlb_name} ({fantasy['owner']}): {ip_str}IP {k}K {er}ER"
                if qs:   line += " ✓QS"
                if svhd: line += f" {svhd}SV+H"
                pitchers.append(line)

    return {"hitters": hitters, "pitchers": pitchers}


def build_mlb_live_data(roster_map: dict) -> dict:
    """
    Fetch today's schedule and boxscores, return structured data:
    {
        "live":      [{"header": str, "hitters": [str], "pitchers": [str]}],
        "final":     [{"header": str, "hitters": [str], "pitchers": [str]}],
        "probables": [str],   # only rostered probable starters
        "date_str":  str,
    }
    Only includes live/final games that have at least one rostered player.
    Only includes upcoming entries if a probable pitcher is rostered.
    """
    games  = fetch_mlb_schedule_today()
    result = {
        "live": [], "final": [], "probables": [],
        "date_str": date.today().strftime("%a %b %-d"),
    }

    for game in games:
        state   = game.get("status", {}).get("abstractGameState", "")
        teams   = game.get("teams", {})
        away_t  = teams.get("away", {}).get("team", {})
        home_t  = teams.get("home", {}).get("team", {})
        a_abbr  = _team_abbrev(away_t)
        h_abbr  = _team_abbrev(home_t)
        a_score = teams.get("away", {}).get("score", "")
        h_score = teams.get("home", {}).get("score", "")
        status  = _game_status_label(game)

        if state in ("Live", "Final"):
            players = _rostered_players_in_game(game["gamePk"], roster_map)
            if not players["hitters"] and not players["pitchers"]:
                continue  # skip games with no rostered players
            if state == "Live":
                header = f"🔴 {a_abbr} {a_score} · {h_abbr} {h_score} — {status}"
                result["live"].append({"header": header, **players})
            else:
                header = f"✅ {a_abbr} {a_score} · {h_abbr} {h_score} — FINAL"
                result["final"].append({"header": header, **players})
        else:
            # Upcoming — only include if a rostered player is a probable
            for side_key in ("away", "home"):
                prob = teams.get(side_key, {}).get("probablePitcher", {})
                if prob:
                    pname   = prob.get("fullName", "")
                    fantasy = _match_player(pname, roster_map)
                    if fantasy:
                        result["probables"].append(
                            f"{pname} ({fantasy['owner']}) — {a_abbr} @ {h_abbr} {status}"
                        )

    return result


def _game_field_value(game_entry: dict) -> str:
    """Format a game's rostered player lines into an embed field value."""
    lines = list(game_entry.get("hitters", [])) + list(game_entry.get("pitchers", []))
    return "\n".join(lines) if lines else "*(no rostered starters yet)*"


def build_mlb_live_embed_fields(live_data: dict) -> list[dict]:
    """
    Convert structured live data to Discord embed field dicts.
    Each game gets its own field; probables share one field.
    """
    fields = []
    for entry in live_data["live"]:
        fields.append({"name": entry["header"], "value": _game_field_value(entry)[:1024], "inline": False})
    for entry in live_data["final"]:
        fields.append({"name": entry["header"], "value": _game_field_value(entry)[:1024], "inline": False})
    if live_data["probables"]:
        fields.append({
            "name":   "🕐 Probable Starters (rostered)",
            "value":  "\n".join(live_data["probables"])[:1024],
            "inline": False,
        })
    return fields


def build_mlb_live_context(roster_map: dict) -> str:
    """Compact text version of live data for use in /ask Gemini context."""
    live_data = build_mlb_live_data(roster_map)
    if not live_data["live"] and not live_data["final"] and not live_data["probables"]:
        return f"MLB LIVE DATA ({live_data['date_str']}): No rostered players found in today's games."

    lines = [f"MLB LIVE DATA — {live_data['date_str']}:"]
    for entry in live_data["live"]:
        lines.append(entry["header"])
        lines.extend(f"  {h}" for h in entry["hitters"])
        lines.extend(f"  {p}" for p in entry["pitchers"])
    for entry in live_data["final"]:
        lines.append(entry["header"])
        lines.extend(f"  {h}" for h in entry["hitters"])
        lines.extend(f"  {p}" for p in entry["pitchers"])
    if live_data["probables"]:
        lines.append("Probable starters (rostered):")
        lines.extend(f"  {p}" for p in live_data["probables"])
    return "\n".join(lines)


def fetch_current_roster_records() -> list[dict]:
    """
    Fetch the most recent scoring period's active records for roster identification.
    Falls back to yesterday's period if today's hasn't been scraped yet.
    """
    period  = current_scoring_period()
    records = fetch_stats_for_periods([period])
    if not records:
        records = fetch_stats_for_periods([max(1, period - 1)])
    return filter_active(records)


def fetch_espn_live_rosters() -> dict[str, dict]:
    """
    Query ESPN Fantasy API directly for current roster assignments.
    Used by /live so the roster reflects adds/drops made today right up
    until game time — regardless of when the scraper last ran.

    Returns {normalized_name: {full_name, owner, team_id}}, same shape
    as build_roster_name_map(). Falls back to empty dict on any error.
    """
    url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{YEAR}/segments/0/leagues/{LEAGUE_ID}?view=mRoster"
    )
    cookies = {}
    if ESPN_S2:   cookies["espn_s2"] = ESPN_S2
    if ESPN_SWID: cookies["SWID"]    = ESPN_SWID

    try:
        resp = requests.get(url, cookies=cookies, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ESPN roster fetch failed: {e}")
        return {}

    roster_map: dict[str, dict] = {}
    for team in data.get("teams", []):
        team_id = team.get("id")
        owner   = TEAM_NAMES.get(team_id, f"T{team_id}")
        for entry in team.get("roster", {}).get("entries", []):
            player = entry.get("playerPoolEntry", {}).get("player", {})
            name   = player.get("fullName", "")
            if not name:
                continue
            key = normalize_name(name)
            roster_map[key] = {"full_name": name, "owner": owner, "team_id": team_id}

    print(f"  ESPN live roster: {len(roster_map)} players across {len(data.get('teams', []))} teams")
    return roster_map


def build_live_roster_map() -> dict[str, dict]:
    """
    Best-available roster map for /live.
    Tries ESPN directly first (real-time), falls back to Supabase.
    """
    roster_map = fetch_espn_live_rosters()
    if roster_map:
        return roster_map
    print("  ESPN unavailable — falling back to Supabase roster records...")
    records = fetch_current_roster_records()
    return build_roster_name_map(records)


# ---------------------------------------------------------------------------
# SCORING PERIOD HELPERS
# ---------------------------------------------------------------------------

def current_scoring_period() -> int:
    delta = (date.today() - SEASON_START).days
    return max(1, delta)


def scoring_period_for_date(d: date) -> int:
    return max(1, (d - SEASON_START).days + 1)


# ---------------------------------------------------------------------------
# SUPABASE
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# SUPABASE
# ---------------------------------------------------------------------------

def get_supabase() -> Client:
    url = SUPABASE_URL or "NOT SET"
    key = SUPABASE_KEY or "NOT SET"
    print(f"Supabase URL: '{url}' (len={len(url)})")
    print(f"Supabase Key starts with: '{key[:20]}' (len={len(key)})")
    return create_client(url, key)


def fetch_stats_up_to_period(max_period: int) -> list[dict]:
    all_records = []
    offset, page_size = 0, 1000
    while True:
        batch = (
            get_supabase()
            .table("player_daily_stats")
            .select("*")
            .lte("scoring_period_id", max_period)
            .order("scoring_period_id", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records


def fetch_stats_for_periods(periods: list[int]) -> list[dict]:
    if not periods:
        return []
    all_records = []
    offset, page_size = 0, 1000
    while True:
        batch = (
            get_supabase()
            .table("player_daily_stats")
            .select("*")
            .in_("scoring_period_id", periods)
            .order("scoring_period_id", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records


def fetch_recent_transactions(days: int = 14) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    all_records = []
    offset, page_size = 0, 500
    while True:
        batch = (
            get_supabase()
            .table("transactions")
            .select("*")
            .gte("transaction_date", cutoff)
            .order("transaction_date", desc=True)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records


def fetch_team_transactions(team_id: int, days: int = 365) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    all_records = []
    for field in ["to_team_id", "from_team_id"]:
        offset, page_size = 0, 500
        while True:
            batch = (
                get_supabase()
                .table("transactions")
                .select("*")
                .eq(field, team_id)
                .gte("transaction_date", cutoff)
                .order("transaction_date", desc=True)
                .range(offset, offset + page_size - 1)
                .execute()
                .data or []
            )
            all_records.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
            
    seen, deduped = set(), []
    for r in all_records:
        if r["espn_transaction_id"] not in seen:
            seen.add(r["espn_transaction_id"])
            deduped.append(r)
    return sorted(deduped, key=lambda x: x["transaction_date"], reverse=True)

# ---------------------------------------------------------------------------
# AGGREGATION + STANDINGS
# ---------------------------------------------------------------------------

def filter_active(records: list[dict]) -> list[dict]:
    """Remove bench and IL player records — their stats don't count in roto."""
    return [r for r in records if r.get("lineup_slot_id") not in BENCH_IL_SLOTS]


def aggregate_by_team(records: list[dict]) -> dict:
    totals: dict[int, dict] = {}
    for row in records:
        tid   = row["team_id"]
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        if tid not in totals:
            totals[tid] = {}
        for stat, val in stats.items():
            if isinstance(val, (int, float)):
                totals[tid][stat] = totals[tid].get(stat, 0) + val
    return totals


def espn_ip_to_innings(ip_val: float) -> float:
    """ESPN stores IP as total outs. Divide by 3 for decimal innings."""
    return ip_val / 3.0


def compute_roto_standings(records: list[dict]) -> dict:
    raw = aggregate_by_team(records)
    team_cats: dict[int, dict] = {}

    for tid, stats in raw.items():
        pa     = stats.get("PA", 0)
        obp    = round((stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
        ip_dec = espn_ip_to_innings(stats.get("IP", 0))
        era    = round((stats.get("ER", 0) / ip_dec) * 9, 2) if ip_dec > 0 else 0.0
        whip   = round((stats.get("H_Allowed", 0) + stats.get("BB_Allowed", 0)) / ip_dec, 3) if ip_dec > 0 else 0.0

        team_cats[tid] = {
            "R":     int(stats.get("R", 0)),
            "HR":    int(stats.get("HR", 0)),
            "RBI":   int(stats.get("RBI", 0)),
            "OBP":   obp,
            "SB":    int(stats.get("SB", 0)),
            "QS":    int(stats.get("QS", 0)),
            "ERA":   era,
            "WHIP":  whip,
            "K":     int(stats.get("K", 0)),
            "SV_HD": int(stats.get("SV", 0) + stats.get("HD", 0)),
        }

    n = len(team_cats)
    cat_points: dict[int, dict] = {tid: {} for tid in team_cats}
    for cat, higher in CAT_HIGHER_IS_BETTER.items():
        sorted_teams = sorted(team_cats.items(), key=lambda x: x[1][cat], reverse=higher)
        i = 0
        while i < len(sorted_teams):
            j = i
            while j < len(sorted_teams) - 1 and sorted_teams[j][1][cat] == sorted_teams[j+1][1][cat]:
                j += 1
            avg = sum(n - k for k in range(i, j+1)) / (j - i + 1)
            for k in range(i, j+1):
                cat_points[sorted_teams[k][0]][cat] = round(avg, 1)
            i = j + 1

    for tid in team_cats:
        team_cats[tid]["cat_points"]  = cat_points[tid]
        team_cats[tid]["roto_points"] = round(sum(cat_points[tid].values()), 1)

    for rank, (tid, _) in enumerate(
        sorted(team_cats.items(), key=lambda x: x[1]["roto_points"], reverse=True), 1
    ):
        team_cats[tid]["standing"] = rank

    return team_cats


def compute_standings_delta(prev: dict, curr: dict) -> dict:
    delta = {}
    for tid, curr_data in curr.items():
        if tid not in prev:
            continue
        prev_data = prev[tid]
        delta[tid] = {
            "rank_change":   prev_data["standing"] - curr_data["standing"],
            "points_change": round(curr_data["roto_points"] - prev_data["roto_points"], 1),
            "cat_changes": {
                cat: round(curr_data["cat_points"].get(cat, 0) - prev_data["cat_points"].get(cat, 0), 1)
                for cat in ROTO_CATS
            },
        }
    return delta


# ---------------------------------------------------------------------------
# PLAYER-LEVEL AGGREGATION
# ---------------------------------------------------------------------------

def aggregate_by_player(records: list[dict]) -> dict:
    players: dict[int, dict] = {}
    for row in records:
        pid   = row["player_id"]
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        if pid not in players:
            players[pid] = {"full_name": row["full_name"], "team_id": row["team_id"]}
        for stat, val in stats.items():
            if isinstance(val, (int, float)):
                players[pid][stat] = players[pid].get(stat, 0) + val
    return players


def get_player_leaders_block(records: list[dict], top_n: int = 5) -> str:
    players = aggregate_by_player(records)
    for p in players.values():
        ip_dec = p.get("IP", 0) / 3.0
        if ip_dec > 0:
            p["ERA"]  = round((p.get("ER", 0) / ip_dec) * 9, 2)
            p["WHIP"] = round((p.get("H_Allowed", 0) + p.get("BB_Allowed", 0)) / ip_dec, 3)
        else:
            p["ERA"] = p["WHIP"] = None

    lines = [f"TOP {top_n} PLAYERS BY STAT (period covered):"]
    for stat in ["HR", "RBI", "R", "SB", "K", "QS", "SV", "HD"]:
        top = sorted(players.values(), key=lambda x: x.get(stat, 0), reverse=True)[:top_n]
        top = [p for p in top if p.get(stat, 0) > 0]
        if top:
            lines.append(f"  {stat}: " + ", ".join(
                f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {int(p[stat])}"
                for p in top
            ))
    eligible = [p for p in players.values() if p.get("IP", 0) >= 30 and p["ERA"] is not None]
    if eligible:
        best_era  = sorted(eligible, key=lambda x: x["ERA"])[:top_n]
        worst_era = sorted(eligible, key=lambda x: x["ERA"], reverse=True)[:top_n]
        best_whip = sorted(eligible, key=lambda x: x["WHIP"])[:top_n]
        lines.append("  ERA (best): "  + ", ".join(f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {p['ERA']:.2f}"  for p in best_era))
        lines.append("  ERA (worst): " + ", ".join(f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {p['ERA']:.2f}"  for p in worst_era))
        lines.append("  WHIP (best): " + ", ".join(f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {p['WHIP']:.3f}" for p in best_whip))
    return "\n".join(lines)


def get_team_player_block(records: list[dict], team_id: int, label: str) -> str:
    team_records = [r for r in records if r["team_id"] == team_id]
    if not team_records:
        return f"{label}: no data found."
    players  = aggregate_by_player(team_records)
    hitters, pitchers = [], []
    for p in players.values():
        ip_dec = p.get("IP", 0) / 3.0
        ab     = p.get("AB", 0)
        if ip_dec > 0:
            ip_outs       = int(round(p.get("IP", 0)))
            innings_whole = ip_outs // 3
            extra_outs    = ip_outs % 3
            era  = round((p.get("ER", 0) / ip_dec) * 9, 2)
            whip = round((p.get("H_Allowed", 0) + p.get("BB_Allowed", 0)) / ip_dec, 3)
            pitchers.append({
                "name": p["full_name"], "ip": f"{innings_whole}.{extra_outs}",
                "k": int(p.get("K", 0)), "er": int(p.get("ER", 0)),
                "era": era, "whip": whip,
                "qs": int(p.get("QS", 0)), "svhd": int(p.get("SV", 0) + p.get("HD", 0)),
            })
        elif ab > 0:
            pa  = p.get("PA", 0)
            obp = round((p.get("H", 0) + p.get("BB", 0) + p.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
            hitters.append({
                "name": p["full_name"], "ab": int(ab), "h": int(p.get("H", 0)),
                "hr": int(p.get("HR", 0)), "rbi": int(p.get("RBI", 0)),
                "r": int(p.get("R", 0)), "sb": int(p.get("SB", 0)), "obp": obp,
            })
    lines = [f"{label} — PLAYER BREAKDOWN:"]
    if hitters:
        hitters.sort(key=lambda x: -(x["hr"] * 4 + x["rbi"] * 2 + x["r"] + x["sb"] * 2))
        lines.append("  HITTERS:")
        for h in hitters:
            lines.append(f"    {h['name']}: {h['ab']} AB, {h['h']} H, {h['hr']} HR, "
                         f"{h['rbi']} RBI, {h['r']} R, {h['sb']} SB, {h['obp']:.3f} OBP")
    if pitchers:
        pitchers.sort(key=lambda x: x["era"])
        lines.append("  PITCHERS (sorted by ERA, best→worst):")
        for p in pitchers:
            lines.append(f"    {p['name']}: {p['ip']} IP, {p['k']} K, {p['er']} ER, "
                         f"{p['era']:.2f} ERA, {p['whip']:.3f} WHIP, {p['qs']} QS, {p['svhd']} SV+H")
    return "\n".join(lines)


def get_trend_block(current_period: int) -> str:
    recent_periods = list(range(max(1, current_period - 6), current_period + 1))
    prior_periods  = list(range(max(1, current_period - 13), max(1, current_period - 6)))
    recent_records = fetch_stats_for_periods(recent_periods)
    prior_records  = fetch_stats_for_periods(prior_periods)
    if not recent_records or not prior_records:
        return ""
    recent_standings = compute_roto_standings(filter_active(recent_records))
    prior_standings  = compute_roto_standings(filter_active(prior_records))
    delta            = compute_standings_delta(prior_standings, recent_standings)
    lines = ["LAST 7 DAYS ROTO MOVEMENT (vs. prior 7 days):"]
    for tid in sorted(recent_standings, key=lambda x: recent_standings[x]["standing"]):
        if tid not in delta:
            continue
        d     = delta[tid]
        name  = TEAM_NAMES.get(tid, f"Team {tid}")
        rc, pc = d["rank_change"], d["points_change"]
        arrow = f"▲{rc}" if rc > 0 else (f"▼{abs(rc)}" if rc < 0 else "—")
        pts   = f"+{pc}" if pc > 0 else str(pc)
        gains  = [CAT_DISPLAY[c] for c, v in d["cat_changes"].items() if v > 0]
        losses = [CAT_DISPLAY[c] for c, v in d["cat_changes"].items() if v < 0]
        g_str  = f"gained {', '.join(gains)}" if gains else ""
        l_str  = f"lost {', '.join(losses)}"  if losses else ""
        move   = "; ".join(filter(None, [g_str, l_str])) or "no change"
        lines.append(f"  {name} ({arrow}, {pts} pts this week): {move}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# TRANSACTION FORMATTING
# ---------------------------------------------------------------------------

def format_transaction_context(transactions: list[dict], label: str = "RECENT TRANSACTIONS") -> str:
    if not transactions:
        return f"{label}: No transactions found."
    lines = [f"{label} ({len(transactions)} moves):"]
    for t in sorted(transactions, key=lambda x: x["transaction_date"], reverse=True)[:200]:
        date_str  = t["transaction_date"][:10]
        to_name   = TEAM_NAMES.get(t.get("to_team_id"), f"T{t.get('to_team_id')}")
        from_name = TEAM_NAMES.get(t.get("from_team_id"), "Free Agent") if t.get("from_team_id", -1) != -1 else "Free Agent/Waivers"
        txn_type  = t.get("transaction_type", "?")
        player    = t.get("player_name", f"Player {t.get('player_id')}")
        if txn_type == "TRADE":
            lines.append(f"  {date_str} TRADE: {player} — {from_name} → {to_name}")
        elif txn_type in ("ADD", "WAIVER_ADD"):
            lines.append(f"  {date_str} ADD:   {to_name} added {player} from {from_name}")
        elif txn_type == "DROP":
            lines.append(f"  {date_str} DROP:  {to_name} dropped {player}")
        else:
            lines.append(f"  {date_str} {txn_type}: {player} ({from_name} → {to_name})")
    return "\n".join(lines)


def format_transaction_with_stats(transactions: list[dict], active_records: list[dict]) -> str:
    player_stats: dict[int, dict] = {}
    for row in active_records:
        pid   = row["player_id"]
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        if pid not in player_stats:
            player_stats[pid] = {"team_id": row["team_id"], "name": row["full_name"]}
        for s, v in stats.items():
            if isinstance(v, (int, float)):
                player_stats[pid][s] = player_stats[pid].get(s, 0) + v
    lines = ["TRANSACTION GRADES (with post-acquisition stats):"]
    adds  = [t for t in transactions if t.get("transaction_type") in ("ADD", "WAIVER_ADD", "TRADE")]
    for t in adds[:20]:
        pid      = t.get("player_id")
        player   = t.get("player_name", f"Player {pid}")
        to_name  = TEAM_NAMES.get(t.get("to_team_id"), "?")
        date_str = t["transaction_date"][:10]
        txn_type = t.get("transaction_type", "?")
        p = player_stats.get(pid)
        if p:
            ip_dec    = p.get("IP", 0) / 3.0
            era       = round((p.get("ER", 0) / ip_dec) * 9, 2) if ip_dec > 0 else None
            whip      = round((p.get("H_Allowed", 0) + p.get("BB_Allowed", 0)) / ip_dec, 3) if ip_dec > 0 else None
            stats_str = (f"HR:{int(p.get('HR',0))} RBI:{int(p.get('RBI',0))} "
                         f"R:{int(p.get('R',0))} SB:{int(p.get('SB',0))} "
                         f"K:{int(p.get('K',0))} QS:{int(p.get('QS',0))}")
            if era is not None:
                ip_outs    = int(round(p.get("IP", 0)))
                stats_str += f" ERA:{era} WHIP:{whip} IP:{ip_outs//3}.{ip_outs%3}"
        else:
            stats_str = "(no stats recorded yet)"
        lines.append(f"  {date_str} {txn_type}: {to_name} acquired {player}")
        lines.append(f"    Season stats: {stats_str}")
    return "\n".join(lines)


def format_trades_block(trades: list[dict]) -> str:
    if not trades:
        return ""
    grouped: dict[str, list] = defaultdict(list)
    for t in trades:
        txn_uuid = t["espn_transaction_id"].rsplit("_", 2)[0]
        grouped[txn_uuid].append(t)
    lines = [f"ALL SEASON TRADES ({len(grouped)} trades):"]
    for uuid, items in sorted(grouped.items(), key=lambda x: x[1][0]["transaction_date"], reverse=True):
        date_str = items[0]["transaction_date"][:10]
        by_team: dict[int, list[str]] = defaultdict(list)
        for item in items:
            by_team[item["to_team_id"]].append(item["player_name"])
        sides = [f"{TEAM_NAMES.get(tid, f'T{tid}')} gets {', '.join(players)}"
                 for tid, players in by_team.items()]
        lines.append(f"  {date_str}: " + " | ".join(sides))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CONTEXT BUILDER
# ---------------------------------------------------------------------------

def _parse_window(q: str, current_period: int) -> tuple[list[int] | None, str]:
    if re.search(r"last\s+2\s+weeks|past\s+2\s+weeks|two\s+weeks", q):
        return list(range(max(1, current_period - 13), current_period + 1)), "LAST 2 WEEKS"
    if re.search(r"last\s+3\s+weeks|past\s+3\s+weeks|three\s+weeks", q):
        return list(range(max(1, current_period - 20), current_period + 1)), "LAST 3 WEEKS"
    if re.search(r"last\s+month|past\s+month|this\s+month", q):
        return list(range(max(1, current_period - 29), current_period + 1)), "LAST 30 DAYS"
    if re.search(r"last\s+week|past\s+week|this\s+week", q):
        return list(range(max(1, current_period - 6), current_period + 1)), "LAST 7 DAYS"
    if re.search(r"yesterday|last\s+night", q):
        return [current_period - 1] if current_period > 1 else [1], "YESTERDAY"
    return None, "FULL SEASON"


def build_context(question: str, current_period: int, asking_owner: str | None = None) -> str:
    q     = question.lower()
    parts = []

    first_person_triggers = [
        "my team", "my players", "my pitcher", "my hitter",
        "my roster", "my stats", "i have", "do i", "am i",
        "my era", "my whip", "my trade", "my adds",
    ]

    # --- Always: current standings (active players only) ---
    print(f"  Fetching cumulative records through period {current_period}...")
    cumulative        = fetch_stats_up_to_period(current_period)
    active_cumulative = filter_active(cumulative)
    standings         = compute_roto_standings(active_cumulative)

    sorted_teams = sorted(standings.items(), key=lambda x: x[1]["standing"])
    header = f"{'#':<3} {'Team':<14} {'Pts':>5}  " + "  ".join(f"{CAT_DISPLAY[c]:>5}" for c in ROTO_CATS)
    rows   = [header]
    for tid, data in sorted_teams:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        cats = "  ".join(f"{data['cat_points'].get(c, 0):>5.1f}" for c in ROTO_CATS)
        rows.append(f"{data['standing']:<3} {name:<14} {data['roto_points']:>5.1f}  {cats}")
    parts.append("CURRENT ROTO STANDINGS (roto points per category):\n" + "\n".join(rows))

    fmt = {"R":"d","HR":"d","RBI":"d","OBP":".3f","SB":"d","QS":"d","ERA":".2f","WHIP":".3f","K":"d","SV_HD":"d"}
    val_header = f"{'#':<3} {'Team':<14}  " + "  ".join(f"{CAT_DISPLAY[c]:>7}" for c in ROTO_CATS)
    val_rows   = [val_header]
    for tid, data in sorted_teams:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        vals = "  ".join(f"{data[c]:{fmt[c]}}".rjust(7) for c in ROTO_CATS)
        val_rows.append(f"{data['standing']:<3} {name:<14}  {vals}")
    parts.append("ACTUAL CATEGORY VALUES:\n" + "\n".join(val_rows))

    # --- Detect mentioned team names ---
    window_periods, window_label = _parse_window(q, current_period)
    mentioned_teams = [
        tid for name, tid in TEAM_NAME_TO_ID.items()
        if re.search(r'\b' + re.escape(name) + r'\b', q)
    ]

    if asking_owner and any(t in q for t in first_person_triggers):
        owner_team_id = TEAM_NAME_TO_ID.get(asking_owner.lower())
        if owner_team_id and owner_team_id not in mentioned_teams:
            mentioned_teams.append(owner_team_id)

    if mentioned_teams:
        print(f"  Building player breakdowns for {len(mentioned_teams)} team(s) [{window_label}]...")
        for tid in mentioned_teams:
            team_name = TEAM_NAMES.get(tid, f"Team {tid}")
            scoped = ([r for r in active_cumulative if r["scoring_period_id"] in set(window_periods)]
                      if window_periods else active_cumulative)
            parts.append(get_team_player_block(scoped, tid, f"{team_name} ({window_label})"))

    # --- Historical context ---
    if any(kw in q for kw in HISTORY_KEYWORDS):
        print("  Loading league history...")
        try:
            parts.append(format_league_champions())
            parts.append(format_all_active_owner_summaries())
        except Exception as e:
            print(f"  History load failed: {e}")

    if mentioned_teams:
        try:
            for tid in mentioned_teams:
                owner_name = TEAM_NAMES.get(tid, "")
                if owner_name:
                    parts.append(format_owner_history(owner_name))
        except Exception as e:
            print(f"  Owner history load failed: {e}")

    # --- Transactions ---
    if any(kw in q for kw in TRANSACTION_KEYWORDS):
        print("  Fetching transactions...")
        try:
            if mentioned_teams:
                all_txns = []
                for tid in mentioned_teams:
                    all_txns.extend(fetch_team_transactions(tid, days=365))
            else:
                all_txns = fetch_recent_transactions(days=14)
            if all_txns:
                parts.append(format_transaction_context(all_txns))
                if any(kw in q for kw in ["grade", "worth it", "good move", "bad move", "how has", "analyze", "evaluate"]):
                    parts.append(format_transaction_with_stats(all_txns, active_cumulative))
            if any(kw in q for kw in ["trade", "traded", "trades"]):
                print("  Fetching all season trades...")
                all_trades = (
                    get_supabase()
                    .table("transactions")
                    .select("*")
                    .eq("transaction_type", "TRADE")
                    .order("transaction_date", desc=True)
                    .execute()
                    .data or []
                )
                if all_trades:
                    parts.append(format_trades_block(all_trades))
        except Exception as e:
            print(f"  Transaction fetch failed: {e}")

    # --- Trends ---
    if any(kw in q for kw in TREND_KEYWORDS):
        print("  Fetching trend data...")
        trend_block = get_trend_block(current_period)
        if trend_block:
            parts.append(trend_block)

    # --- League-wide player leaders ---
    if any(kw in q for kw in PLAYER_KEYWORDS) or "who" in q:
        print("  Computing player leaders...")
        scoped = ([r for r in active_cumulative if r["scoring_period_id"] in set(window_periods)]
                  if window_periods else active_cumulative)
        parts.append(get_player_leaders_block(scoped))

    # --- MLB live data ---
    if any(kw in q for kw in LIVE_KEYWORDS):
        print("  Fetching MLB live data...")
        try:
            # Use ESPN directly for real-time roster; falls back to Supabase
            roster_map = build_live_roster_map()
            live_block     = build_mlb_live_context(roster_map)
            if live_block:
                parts.append(live_block)
        except Exception as e:
            print(f"  MLB live fetch failed: {e}")

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# GEMINI
# ---------------------------------------------------------------------------

def generate_answer(question: str, context: str, asking_owner: str | None = None) -> str:
    n_teams = len(TEAM_NAMES)

    if asking_owner:
        owner_context = (
            f"The person asking this question is {asking_owner}, "
            f"who manages their own team in this league. "
            f"If they use first-person ('my team', 'my players', 'I'), "
            f"they are referring to {asking_owner}'s team."
        )
    else:
        owner_context = "The person asking is not identified as a league member."

    prompt = f"""You are the HEFTYSTRONG fantasy baseball league's stats bot.
Answer the following question using ONLY the data provided. Be direct, concise, and a little snarky.
Keep your answer under 300 words so it fits comfortably in Discord.

League format: {n_teams}-team roto league.
Scoring categories: R, HR, RBI, OBP, SB, QS, ERA, WHIP, K, SV+Holds.
ERA and WHIP: lower = better. All other categories: higher = better.
Roto points: 1 (worst in category) to {n_teams} (best in category). Max possible score = {n_teams * 10}.
All cumulative stats reflect ACTIVE lineup players only (bench/IL excluded).
Live game stats are pulled directly from the MLB Stats API and reflect what has happened so far today.

ABOUT THE PERSON ASKING: {owner_context}

QUESTION: {question}

LEAGUE DATA:
{context}

Answer the question directly. If the data doesn't support a specific claim, say so rather than guessing."""

    client   = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
    return response.text


# ---------------------------------------------------------------------------
# DISCORD BOT
# ---------------------------------------------------------------------------

class HEFTYBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()
        print("Slash commands synced with Discord.")

    async def on_ready(self):
        print(f"Bot online: {self.user} (ID: {self.user.id})")
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="HEFTYSTRONG standings 👀"
            )
        )
        try:
            result = get_supabase().table("player_daily_stats").select("scoring_period_id").limit(1).execute()
            print(f"Supabase OK — sample row: {result.data}")
        except Exception as e:
            print(f"Supabase FAILED — {e}")


bot = HEFTYBot()


@bot.tree.command(name="ping", description="Test bot and database connectivity")
async def ping_command(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        result = (
            get_supabase()
            .table("player_daily_stats")
            .select("id, scoring_period_id, full_name")
            .limit(1)
            .execute()
        )
        await interaction.followup.send(f"✅ DB connected. Sample row: `{result.data}`")
    except Exception as e:
        await interaction.followup.send(f"❌ DB failed: `{type(e).__name__}: {str(e)[:400]}`")


@bot.tree.command(name="live", description="Show today's games and how your rostered players are doing right now")
async def live_command(interaction: discord.Interaction):
    """Dedicated live game command — always pulls MLB data, no keyword detection needed."""
    await interaction.response.defer(thinking=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] /live from {interaction.user}")
    try:
        # Use ESPN directly for real-time roster (reflects today's adds/drops up to game time).
        # Falls back to Supabase if ESPN is unreachable.
        roster_map = await asyncio.to_thread(build_live_roster_map)
        live_data      = await asyncio.to_thread(build_mlb_live_data, roster_map)
        fields         = build_mlb_live_embed_fields(live_data)

        if not fields:
            await interaction.followup.send(
                f"⚾ No rostered players found in today's MLB games ({live_data['date_str']})."
            )
            return

        embed = discord.Embed(
            title = f"⚾ HEFTYSTRONG Live — {live_data['date_str']}",
            color = 0xFF4500,
        )
        for field in fields:
            embed.add_field(name=field["name"], value=field["value"], inline=field["inline"])
        embed.set_footer(text=f"HEFTYSTRONG • updated {datetime.now().strftime('%H:%M ET')} • starters only")
        await interaction.followup.send(embed=embed)
    except Exception as e:
        print(f"Error handling /live: {e}")
        await interaction.followup.send(f"⚠️ Live data fetch failed: `{str(e)[:200]}`")


@bot.tree.command(name="ask", description="Ask about HEFTYSTRONG stats, standings, trends, and more")
@app_commands.describe(question="Your question about the league — e.g. 'Who leads in HR?' or 'How is Mookie doing today?'")
async def ask_command(interaction: discord.Interaction, question: str):
    await interaction.response.defer(thinking=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] /ask from {interaction.user}: {question}")

    try:
        period           = current_scoring_period()
        discord_username = str(interaction.user.name).lower()
        asking_owner     = DISCORD_TO_OWNER.get(discord_username)

        context = await asyncio.to_thread(build_context, question, period, asking_owner)
        answer  = await asyncio.to_thread(generate_answer, question, context, asking_owner)

        if len(answer) > 1900:
            answer = answer[:1897] + "..."

        embed = discord.Embed(description=answer, color=0x1E90FF)
        embed.set_footer(text=f"Asked by {interaction.user.display_name} • HEFTYSTRONG")
        embed.set_author(name=f"❓ {question[:200]}")
        await interaction.followup.send(embed=embed)

    except Exception as e:
        print(f"Error handling /ask: {e}")
        await interaction.followup.send(
            f"⚠️ Something went wrong: `{str(e)[:200]}`\nTry again or check the logs."
        )


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        raise ValueError("DISCORD_BOT_TOKEN environment variable is not set.")
    bot.run(DISCORD_BOT_TOKEN)
