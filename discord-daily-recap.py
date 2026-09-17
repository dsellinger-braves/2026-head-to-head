"""
HEFTYSTRONG Fantasy Baseball — Discord Daily Recap
Produces three Discord posts per day:
  1. AI narrative recap (with recent trades + historical context)
  2. Best / Worst 5 hitters and pitchers for the day
  3. Roto standings changes summary
"""

import os
import json
import requests
from collections import defaultdict
from datetime import datetime, timedelta, date, timezone
from supabase import create_client, Client
import google.genai as genai
from google.genai import types
from historical import (
    format_owner_history,
    format_league_champions,
    format_all_active_owner_summaries,
)
try:
    from pipelines.build_recap_context import format_recap_context
except ImportError:
    try:
        from build_recap_context import format_recap_context
    except ImportError:
        def format_recap_context(*args, **kwargs): return ""


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

LEAGUE_ID = 130215
YEAR      = 2026

SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY")

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

# ---------------------------------------------------------------------------
# LINEUP SLOT IDs
# ---------------------------------------------------------------------------

BENCH_IL_SLOTS  = {16, 17, 20, 21, 22}

# These must stay in sync with HITTING_SLOT_IDS / PITCHING_SLOT_IDS below
# and with the slot filtering in active-stats-pull.py.
HITTING_SLOT_IDS  = {0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 19}
PITCHING_SLOT_IDS = {13, 14, 15}

ROTO_CATS = ["R", "HR", "RBI", "OBP", "SB", "QS", "ERA", "WHIP", "K", "SV_HD"]
CAT_HIGHER_IS_BETTER = {
    "R": True, "HR": True, "RBI": True, "OBP": True, "SB": True,
    "QS": True, "ERA": False, "WHIP": False, "K": True, "SV_HD": True,
}
CAT_DISPLAY = {
    "R": "R", "HR": "HR", "RBI": "RBI", "OBP": "OBP", "SB": "SB",
    "QS": "QS", "ERA": "ERA", "WHIP": "WHIP", "K": "K", "SV_HD": "SV+H",
}

SEASON_START = date(2026, 3, 25)

def scoring_period_for_date(target_date: date) -> int:
    return max(1, (target_date - SEASON_START).days + 1)

# ---------------------------------------------------------------------------
# SUPABASE
# ---------------------------------------------------------------------------

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_stats_up_to_period(max_period: int) -> list[dict]:
    all_records = []
    offset, page_size = 0, 1000
    sb = get_supabase()
    while True:
        batch = (
            sb
            .table("player_daily_stats")
            .select("id, league_id, team_id, scoring_period_id, player_id, full_name, lineup_slot_id, stats")
            .eq("league_id", LEAGUE_ID)
            .lte("scoring_period_id", max_period)
            .order("id", desc=False)
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
    sb = get_supabase()
    while True:
        batch = (
            sb
            .table("player_daily_stats")
            .select("id, league_id, team_id, scoring_period_id, player_id, full_name, lineup_slot_id, stats")
            .eq("league_id", LEAGUE_ID)
            .in_("scoring_period_id", periods)
            .order("id", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records

def fetch_all_trades() -> list[dict]:
    """Fetch all TRADE transactions for the season."""
    all_records = []
    offset, page_size = 0, 500
    sb = get_supabase()
    while True:
        batch = (
            sb
            .table("transactions")
            .select("*")
            .eq("league_id", LEAGUE_ID)
            .eq("transaction_type", "TRADE")
            .order("espn_transaction_id", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records

# ---------------------------------------------------------------------------
# AGGREGATION HELPERS
# ---------------------------------------------------------------------------

def filter_active(records: list[dict]) -> list[dict]:
    """Remove bench and IL player records — their stats don't count in roto."""
    return [r for r in records if r.get("lineup_slot_id") not in BENCH_IL_SLOTS]

HITTING_STATS  = {'R', 'HR', 'RBI', 'SB', 'H', 'BB', 'HBP', 'PA', 'AB', 'SF'}
PITCHING_STATS = {'K', 'QS', 'IP', 'ER', 'H_Allowed', 'BB_Allowed', 'SV', 'HD'}

def aggregate_by_team(records: list[dict]) -> dict:
    totals: dict[int, dict] = {}
    for row in records:
        # EXPLICIT GUARDRAIL: Skip Cumulative Season Stats
        if row.get("scoring_period_id") == 0:
            continue

        tid  = row["team_id"]
        slot = row.get("lineup_slot_id")
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)

        # Only count stats that belong to this slot type
        if slot in HITTING_SLOT_IDS:
            allowed = HITTING_STATS
        elif slot in PITCHING_SLOT_IDS:
            allowed = PITCHING_STATS
        else:
            continue  # bench/IL/unknown — skip entirely

        if tid not in totals:
            totals[tid] = {}
        for stat, val in stats.items():
            if stat in allowed and isinstance(val, (int, float)):
                totals[tid][stat] = totals[tid].get(stat, 0) + val
    return totals

def compute_averages(totals: dict) -> dict:
    for tid, stats in totals.items():
        if stats.get("AB", 0) > 0:
            stats["AVG"] = round(stats["H"] / stats["AB"], 3)
        if stats.get("PA", 0) > 0:
            stats["OBP"] = round(
                (stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / stats["PA"], 3
            )
    return totals

def espn_ip_to_innings(ip_val: float) -> float:
    """ESPN stores IP as total outs. Divide by 3 for decimal innings."""
    return ip_val / 3.0

# ---------------------------------------------------------------------------
# ROTO STANDINGS ENGINE
# ---------------------------------------------------------------------------

def compute_roto_standings(records: list[dict]) -> dict:
    """Compute roto standings. Pass filter_active(records) before calling."""
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
        sorted_t = sorted(team_cats.items(), key=lambda x: x[1][cat], reverse=higher)
        i = 0
        while i < len(sorted_t):
            j = i
            while j < len(sorted_t) - 1 and sorted_t[j][1][cat] == sorted_t[j+1][1][cat]:
                j += 1
            avg = sum(n - k for k in range(i, j+1)) / (j - i + 1)
            for k in range(i, j+1):
                cat_points[sorted_t[k][0]][cat] = round(avg, 1)
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
    for tid, curr_d in curr.items():
        if tid not in prev:
            continue
        prev_d = prev[tid]
        delta[tid] = {
            "prev_standing":  prev_d["standing"],
            "curr_standing":  curr_d["standing"],
            "rank_change":    prev_d["standing"] - curr_d["standing"],
            "points_change":  round(curr_d["roto_points"] - prev_d["roto_points"], 1),
            "cat_changes": {
                cat: round(curr_d["cat_points"].get(cat, 0) - prev_d["cat_points"].get(cat, 0), 1)
                for cat in ROTO_CATS
            },
        }
    return delta

# ---------------------------------------------------------------------------
# TRANSACTION HELPERS
# ---------------------------------------------------------------------------

def filter_trades_by_days(all_trades: list[dict], days: int) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    return [t for t in all_trades if t.get("transaction_date", "") >= cutoff]

def format_trades_block(trades: list[dict], label: str = "RECENT TRADES") -> str:
    if not trades:
        return ""

    grouped: dict[str, list] = defaultdict(list)
    for t in trades:
        txn_uuid = t["espn_transaction_id"].rsplit("_", 2)[0]
        grouped[txn_uuid].append(t)

    lines = [f"{label} ({len(grouped)} trade(s)):"]
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
# DAILY PERFORMANCE TABLES  (active players only)
# ---------------------------------------------------------------------------

def _hitter_score(stats: dict) -> float:
    return (stats.get("HR", 0) * 4 + stats.get("RBI", 0) * 2 +
            stats.get("R",  0) * 1 + stats.get("SB",  0) * 2 +
            stats.get("H",  0) * 0.5)

def _pitcher_score(stats: dict) -> float:
    return (stats.get("K",  0) * 1 + stats.get("QS", 0) * 10 +
            (stats.get("SV", 0) + stats.get("HD", 0)) * 5 -
            stats.get("ER", 0) * 3)

def get_best_worst_players(records: list[dict], n: int = 5):
    active   = filter_active(records)
    hitters, pitchers = [], []

    for row in active:
        # EXPLICIT GUARDRAIL: Skip Cumulative Season Stats for Daily Tables
        if row.get("scoring_period_id") == 0:
            continue
            
        slot  = row.get("lineup_slot_id")
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        name = row["full_name"]
        team = TEAM_NAMES.get(row["team_id"], f"T{row['team_id']}")

        if slot in HITTING_SLOT_IDS:
            ab = int(stats.get("AB", 0))
            pa = int(stats.get("PA", 0))
            if ab + pa < 1:
                continue
            hitters.append({
                "name": name, "team": team, "score": _hitter_score(stats),
                "ab": ab, "h": int(stats.get("H", 0)), "hr": int(stats.get("HR", 0)),
                "rbi": int(stats.get("RBI", 0)), "r": int(stats.get("R", 0)),
                "sb": int(stats.get("SB", 0)),
            })
        elif slot in PITCHING_SLOT_IDS:
            ip_raw = stats.get("IP", 0)
            if ip_raw <= 0:
                continue
            ip_outs       = int(round(ip_raw))
            innings_whole = ip_outs // 3
            extra_outs    = ip_outs % 3
            pitchers.append({
                "name": name, "team": team, "score": _pitcher_score(stats),
                "ip": f"{innings_whole}.{extra_outs}", "k": int(stats.get("K", 0)),
                "er": int(stats.get("ER", 0)), "qs": int(stats.get("QS", 0)),
                "svhd": int(stats.get("SV", 0) + stats.get("HD", 0)),
            })

    best_hitters  = sorted(hitters,  key=lambda x: (-x["score"], -x["ab"]))[:n]
    worst_hitters = sorted([h for h in hitters if h["ab"] >= 1],
                           key=lambda x: (x["score"], -x["ab"]))[:n]
    best_pitchers  = sorted(pitchers, key=lambda x: -x["score"])[:n]
    worst_pitchers = sorted(pitchers, key=lambda x:  x["score"])[:n]
    return best_hitters, worst_hitters, best_pitchers, worst_pitchers

def get_weekly_top_players(records: list[dict], n: int = 5):
    """Aggregate active player stats across the 7-day period and find the best performers."""
    active = filter_active(records)
    hitters_acc = defaultdict(lambda: defaultdict(float))
    pitchers_acc = defaultdict(lambda: defaultdict(float))
    player_meta = {}

    for row in active:
        if row.get("scoring_period_id") == 0:
            continue
        pid = row["player_id"]
        name = row["full_name"]
        tid = row["team_id"]
        slot = row.get("lineup_slot_id")
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)

        player_meta[pid] = (name, TEAM_NAMES.get(tid, f"T{tid}"))

        if slot in HITTING_SLOT_IDS:
            for k in ["AB", "H", "HR", "RBI", "R", "SB", "PA", "BB"]:
                hitters_acc[pid][k] += stats.get(k, 0)
        elif slot in PITCHING_SLOT_IDS:
            for k in ["IP", "K", "ER", "QS", "SV", "HD", "H_Allowed", "BB_Allowed"]:
                pitchers_acc[pid][k] += stats.get(k, 0)

    hitters = []
    for pid, s in hitters_acc.items():
        ab = int(s.get("AB", 0))
        if ab < 1:
            continue
        name, team = player_meta[pid]
        hitters.append({
            "name": name, "team": team, "score": _hitter_score(s),
            "ab": ab, "h": int(s.get("H", 0)), "hr": int(s.get("HR", 0)),
            "rbi": int(s.get("RBI", 0)), "r": int(s.get("R", 0)),
            "sb": int(s.get("SB", 0)),
        })

    pitchers = []
    for pid, s in pitchers_acc.items():
        ip_raw = s.get("IP", 0)
        if ip_raw <= 0:
            continue
        ip_outs = int(round(ip_raw))
        innings_whole = ip_outs // 3
        extra_outs = ip_outs % 3
        name, team = player_meta[pid]
        pitchers.append({
            "name": name, "team": team, "score": _pitcher_score(s),
            "ip": f"{innings_whole}.{extra_outs}", "k": int(s.get("K", 0)),
            "er": int(s.get("ER", 0)), "qs": int(s.get("QS", 0)),
            "svhd": int(s.get("SV", 0) + s.get("HD", 0)),
        })

    best_hitters = sorted(hitters, key=lambda x: (-x["score"], -x["ab"]))[:n]
    best_pitchers = sorted(pitchers, key=lambda x: -x["score"])[:n]
    return best_hitters, best_pitchers

def _hitter_table(players: list[dict], title: str) -> str:
    hdr  = f"{'Player':<18} {'Team':<11} {'H':>2} {'HR':>2} {'RBI':>3} {'R':>2} {'SB':>2}"
    rows = [title, hdr, "─" * len(hdr)]
    for p in players:
        rows.append(f"{p['name'][:17]:<18} {p['team'][:10]:<11} "
                    f"{p['h']:>2} {p['hr']:>2} {p['rbi']:>3} {p['r']:>2} {p['sb']:>2}")
    return "\n".join(rows)

def _pitcher_table(players: list[dict], title: str) -> str:
    hdr  = f"{'Player':<18} {'Team':<11} {'IP':>4} {'K':>2} {'ER':>3} {'QS':>2} {'SV+H':>4}"
    rows = [title, hdr, "─" * len(hdr)]
    for p in players:
        qs_val = p.get("qs", 0)
        qs = str(qs_val) if qs_val > 1 else ("✓" if qs_val == 1 else "·")
        rows.append(f"{p['name'][:17]:<18} {p['team'][:10]:<11} "
                    f"{p['ip']:>4} {p['k']:>2} {p['er']:>3} {qs:>2} {p['svhd']:>4}")
    return "\n".join(rows)

def format_performance_embed_body(best_h, worst_h, best_p, worst_p) -> str:
    sections = [
        _hitter_table(best_h,   "🟢 BEST HITTERS"),
        _hitter_table(worst_h,  "🔴 WORST HITTERS"),
        _pitcher_table(best_p,  "🟢 BEST PITCHERS"),
        _pitcher_table(worst_p, "🔴 WORST PITCHERS"),
    ]
    
    # Ensure this return statement is entirely on one line!
    return "```text\n" + "\n\n".join(sections) + "\n```"

def format_weekly_performance_embed_body(best_h: list[dict], best_p: list[dict]) -> str:
    sections = [
        _hitter_table(best_h, "🟢 BEST HITTERS OF THE WEEK"),
        _pitcher_table(best_p, "🟢 BEST PITCHERS OF THE WEEK"),
    ]
    return "```text\n" + "\n\n".join(sections) + "\n```"

# ---------------------------------------------------------------------------
# STANDINGS CHANGES EMBED
# ---------------------------------------------------------------------------

def format_standings_changes_body(standings: dict, delta: dict, timeframe: str = "today") -> str:
    lines  = []
    movers = [(tid, d) for tid, d in delta.items() if d["rank_change"] != 0]
    movers.sort(key=lambda x: abs(x[1]["rank_change"]), reverse=True)

    if movers:
        lines.append("**Overall rank changes**")
        for tid, d in movers:
            name   = TEAM_NAMES.get(tid, f"Team {tid}")
            pts    = standings[tid]["roto_points"]
            rc, pc = d["rank_change"], d["points_change"]
            arrow  = "▲" if rc > 0 else "▼"
            pc_str = f"+{pc}" if pc > 0 else str(pc)
            lines.append(f"{arrow} **{name}** #{d['prev_standing']} → #{d['curr_standing']} "
                         f"({pc_str} pts {timeframe}, {pts:.1f} total)")
    else:
        lines.append(f"**Overall rank changes**\n*No position changes {timeframe}*")

    lines.append("")
    cat_moved = False
    cat_lines = ["**Category roto point changes**"]

    for cat in ROTO_CATS:
        gainers = sorted([(tid, d["cat_changes"][cat]) for tid, d in delta.items()
                          if d["cat_changes"].get(cat, 0) > 0], key=lambda x: -x[1])
        losers  = sorted([(tid, d["cat_changes"][cat]) for tid, d in delta.items()
                          if d["cat_changes"].get(cat, 0) < 0], key=lambda x:  x[1])
        if not gainers and not losers:
            continue
        cat_moved = True
        g_str = ", ".join(f"{TEAM_NAMES.get(tid, f'T{tid}')} +{v:.1f}" for tid, v in gainers)
        l_str = ", ".join(f"{TEAM_NAMES.get(tid, f'T{tid}')} {v:.1f}"  for tid, v in losers)
        parts = []
        if g_str: parts.append(f"▲ {g_str}")
        if l_str: parts.append(f"▼ {l_str}")
        cat_lines.append(f"`{CAT_DISPLAY[cat]:>5}` {' | '.join(parts)}")

    lines.extend(cat_lines if cat_moved else
                 [f"**Category roto point changes**\n*No category movement {timeframe}*"])
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# STANDINGS SUMMARY BLOCKS  (for AI prompt context)
# ---------------------------------------------------------------------------

def format_standings_block(standings: dict) -> str:
    sorted_t = sorted(standings.items(), key=lambda x: x[1]["standing"])
    header   = f"{'#':<3} {'Team':<14} {'Pts':>5}  " + "  ".join(f"{CAT_DISPLAY[c]:>5}" for c in ROTO_CATS)
    rows     = [header]
    for tid, data in sorted_t:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        cats = "  ".join(f"{data['cat_points'].get(c, 0):>5.1f}" for c in ROTO_CATS)
        rows.append(f"{data['standing']:<3} {name:<14} {data['roto_points']:>5.1f}  {cats}")
    return "CURRENT ROTO STANDINGS:\n" + "\n".join(rows)

def format_cat_values_block(standings: dict) -> str:
    fmt      = {"R":"d","HR":"d","RBI":"d","OBP":".3f","SB":"d","QS":"d","ERA":".2f","WHIP":".3f","K":"d","SV_HD":"d"}
    sorted_t = sorted(standings.items(), key=lambda x: x[1]["standing"])
    header   = f"{'#':<3} {'Team':<14}  " + "  ".join(f"{CAT_DISPLAY[c]:>7}" for c in ROTO_CATS)
    rows     = [header]
    for tid, data in sorted_t:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        vals = "  ".join(f"{data[c]:{fmt[c]}}".rjust(7) for c in ROTO_CATS)
        rows.append(f"{data['standing']:<3} {name:<14}  {vals}")
    return "ACTUAL CATEGORY VALUES:\n" + "\n".join(rows)

def format_delta_block(standings: dict, delta: dict) -> str:
    sorted_t = sorted(standings.items(), key=lambda x: x[1]["standing"])
    lines    = ["TODAY'S ROTO POINT CHANGES PER CATEGORY:"]
    for tid, _ in sorted_t:
        if tid not in delta:
            continue
        d     = delta[tid]
        name  = TEAM_NAMES.get(tid, f"Team {tid}")
        rc, pc = d["rank_change"], d["points_change"]
        arrow = f"▲{rc}" if rc > 0 else (f"▼{abs(rc)}" if rc < 0 else "—")
        pc_s  = f"+{pc}" if pc > 0 else str(pc)
        notable = [f"{CAT_DISPLAY[c]}: {'+' if v > 0 else ''}{v}" for c, v in d["cat_changes"].items() if v != 0]
        lines.append(f"  {name} ({arrow}, {pc_s} pts): {', '.join(notable) if notable else 'no movement'}")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# AI SUMMARY
# ---------------------------------------------------------------------------
# ROTATING DAILY PERSONAS & ANTI-REPETITION CONSTRAINTS
# ---------------------------------------------------------------------------

ROTATING_DAILY_PERSONAS = {
    0: {  # Monday (reviewing Sunday games)
        "role_name": "The Sunday Box Score Coroner",
        "perspective": "Conducting a dry, clinical, slightly morbid post-mortem of weekend box scores. Treat ERA blowups and blown saves as medical malpractice or crime scene investigations. Identify cause of death for sinking teams and autopsies of pitching disasters.",
        "sample_motifs": ["toxic WHIP levels", "certified time of death", "pitching malpractice", "blunt-force trauma to the standings", "cardiac arrest in the 9th inning"]
    },
    1: {  # Tuesday (early week / waiver moves)
        "role_name": "The Wall Street Quant & Distressed Asset Trader",
        "perspective": "Viewing fantasy rosters strictly through the lens of capital efficiency, risk-adjusted returns, and market arbitrage. Treat players as volatile equities or distressed assets, call out inefficient streaming spending, and identify toxic assets ripe for the waiver scrapheap.",
        "sample_motifs": ["margin calls on your bullpen", "liquidity crisis", "shorting slumping sluggers", "roster arbitrage", "negative ROI streaming", "mean reversion"]
    },
    2: {  # Wednesday (mid-week standings shifts)
        "role_name": "The Vegas Oddsmaker & Sharp Bettor",
        "perspective": "Framing the league race in terms of betting lines, futures movement, bad beats, backdoor covers, and closing-line value. Look at team totals as point spreads and roast managers taking sucker bets on fading aces.",
        "sample_motifs": ["taking the chalk", "backdoor cover", "bad beat of the century", "implied title probability", "parlay buster", "the sharps are fading you"]
    },
    3: {  # Thursday (clubhouse vibes & tension)
        "role_name": "The Dugout Beat Writer & Clubhouse Insider",
        "perspective": "Writing like an embedded, sourced baseball journalist who hears the locker room whispers, dugout tension, and manager press conference subtext. Quote 'anonymous front office sources' and speculate on fractured team chemistry.",
        "sample_motifs": ["closed-door manager meetings", "clubhouse body language", "front office sources say", "veteran benching rumors", "lost the locker room"]
    },
    4: {  # Friday (heading into weekend action)
        "role_name": "The Drive-Time Sports Radio Shock Jock",
        "perspective": "Screaming hot-take radio energy, taking furious calls, slamming the panic button on underperforming stars, and demanding immediate, irrational managerial overhauls ahead of the weekend.",
        "sample_motifs": ["caller on line 4 has had enough", "hit the emergency siren", "smash the panic button", "unforgivable managing", "turn up the heat on the hot seat"]
    },
    5: {  # Saturday (deep weekend matchups)
        "role_name": "The Gritty Scout & Sabermetric Purist",
        "perspective": "Focusing on underlying metrics, batted-ball quality, spin rates, launch angles, and calling out managers whose luck is running out versus those who are getting robbed by the BABIP gods.",
        "sample_motifs": ["hard-hit merchant", "smoke and mirrors ERA", "the BABIP regression Reaper", "barrel rate reality check", "feasting on soft contact"]
    },
    6: {  # Sunday (weekly climax & finish line)
        "role_name": "The Live Chaos Desk & Decimal Tracker",
        "perspective": "Breathless, second-by-second drama tracking razor-thin decimal margins, eleventh-hour stolen bases in late west coast games, and the gut-wrenching pain of a single strikeout flipping the podium.",
        "sample_motifs": ["down to the final out", "thousandths of an OBP point", "razor wire finish", "midnight raid on the leaderboard", "photo finish at the wire"]
    }
}

BANNED_CLICHES_BLOCK = """STRICT VOCABULARY & STYLE RULES:
- BANNED CLICHÉS (NEVER USE ANY OF THESE PHRASES):
  "dogfight", "trading blows", "firing on all cylinders", "juggernaut", "crying in their beer",
  "living rent-free", "dumpster fire", "statement win", "another day in the books", "when all is said and done",
  "make no mistake", "bloodbath", "slugfest", "rollercoaster", "feast or famine", "silver lining", "wake-up call",
  "at the end of the day", "battleground", "tug-of-war", "neck-and-neck", "clash of titans".
- BANNED FORMULAIC OPENINGS:
  Never start with "Another day...", "Welcome back...", "Well, well, well...", "In what can only be described as...",
  "It was a day of...", "Grab your popcorn...", or "There's no love lost...".
- MANDATORY IN MEDIA RES OPENING:
  Begin sentence 1 immediately with a specific action, an arresting stat line, or a vivid concrete scene.
- UNCONVENTIONAL METAPHORS:
  Use fresh, unexpected metaphors (e.g. maritime catastrophes, failed municipal zoning, high-stakes poker bluffs, Michelin-star kitchen meltdowns) rather than generic sports clichés."""

# ---------------------------------------------------------------------------
# AI SUMMARY
# ---------------------------------------------------------------------------

def generate_ai_summary(prompt: str) -> str:
    client = genai.Client(api_key=GEMINI_API_KEY)
    config = types.GenerateContentConfig(
        temperature=0.88,
        top_p=0.95,
    )
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=config
    )
    return response.text

def build_daily_prompt(
    period_date: date,
    team_totals: dict,
    today_records: list[dict],
    standings: dict,
    delta: dict,
    recent_trades: list[dict] | None = None,
) -> str:
    active = filter_active(today_records)

    def stat_val(row, s):
        st = row["stats"]
        if isinstance(st, str): st = json.loads(st)
        return st.get(s, 0)

    top_hr  = max(active, key=lambda r: stat_val(r, "HR"),  default=None)
    top_k   = max(active, key=lambda r: stat_val(r, "K"),   default=None)
    top_rbi = max(active, key=lambda r: stat_val(r, "RBI"), default=None)

    notes = []
    if top_hr  and stat_val(top_hr,  "HR")  > 0:
        notes.append(f"{top_hr['full_name']} ({TEAM_NAMES.get(top_hr['team_id'], '?')}) hit {int(stat_val(top_hr, 'HR'))} HR")
    if top_k   and stat_val(top_k,   "K")   > 0:
        notes.append(f"{top_k['full_name']} ({TEAM_NAMES.get(top_k['team_id'], '?')}) had {int(stat_val(top_k, 'K'))} K")
    if top_rbi and stat_val(top_rbi, "RBI") > 0:
        notes.append(f"{top_rbi['full_name']} ({TEAM_NAMES.get(top_rbi['team_id'], '?')}) drove in {int(stat_val(top_rbi, 'RBI'))} runs")
    standouts = "\n".join(f"  - {n}" for n in notes) if notes else "  (no standout performances)"

    n_teams = len(standings)
    trades_section = ""
    if recent_trades:
        block = format_trades_block(recent_trades, "TRADES IN THE LAST 7 DAYS")
        if block:
            trades_section = f"\n{block}\n"

    # Ingest real MLB news overlap and league context lore
    involved_tids = []
    if top_hr and top_hr.get("team_id"): involved_tids.append(top_hr["team_id"])
    if top_k and top_k.get("team_id"): involved_tids.append(top_k["team_id"])
    if top_rbi and top_rbi.get("team_id"): involved_tids.append(top_rbi["team_id"])
    for tid, d in (delta or {}).items():
        if abs(d.get("rank_change", 0)) > 0 or abs(d.get("points_change", 0)) >= 2.0:
            involved_tids.append(tid)

    recap_context = format_recap_context(today_records, involved_tids, standings=standings, delta=delta)
    context_section = f"\n{recap_context}\n" if recap_context else ""

    persona = ROTATING_DAILY_PERSONAS.get(period_date.weekday(), ROTATING_DAILY_PERSONAS[0])
    motifs_str = ", ".join(f'"{m}"' for m in persona["sample_motifs"][:3])

    return f"""You are the commissioner's voice for the HEFTYSTRONG fantasy baseball league.
TODAY'S EDITORIAL LENS: **{persona['role_name']}**
Tone & Perspective: {persona['perspective']}
Atmospheric motifs to draw from: {motifs_str}

Write a short narrative daily recap for {period_date.strftime('%A, %B %d, %Y')}.
Keep it under 275 words and fun — sharp, observant, and opinionated.
Do NOT include a title. Format for Discord (plain text, no markdown headers).

{BANNED_CLICHES_BLOCK}

CORE NARRATIVE INSTRUCTIONS:
- Tell the story of the day through your assigned persona ({persona['role_name']}).
- Reference 2-3 standout individual performances and weave in what the day meant for the standings.
- Weave real MLB news and headlines with fantasy roster performance when relevant.
- Focus heavily on active category volatility, point swings, and decimal-level margins between rival managers.
- Leverage manager personas and rivalries for authentic commissioner banter.
- Specific stat tables and standings changes will be shown separately in Discord, so do NOT list every team's line.
- All stats reflect active lineup players only (bench/IL excluded).

TOP INDIVIDUAL PERFORMANCES TODAY:
{standouts}

{format_standings_block(standings)}

{format_delta_block(standings, delta)}
{trades_section}{context_section}
Write the narrative recap now:"""

def compute_weekly_team_rates(records: list[dict]) -> dict[int, dict]:
    """Compute exact weekly counting and rate stats for all teams from active player records."""
    raw = aggregate_by_team(filter_active(records))
    weekly_teams = {}
    for tid, stats in raw.items():
        pa     = stats.get("PA", 0)
        obp    = round((stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
        ip_dec = espn_ip_to_innings(stats.get("IP", 0))
        era    = round((stats.get("ER", 0) / ip_dec) * 9, 2) if ip_dec > 0 else 0.0
        whip   = round((stats.get("H_Allowed", 0) + stats.get("BB_Allowed", 0)) / ip_dec, 3) if ip_dec > 0 else 0.0
        sv_hd  = int(stats.get("SV", 0) + stats.get("HD", 0))

        weekly_teams[tid] = {
            "R":      int(stats.get("R", 0)),
            "HR":     int(stats.get("HR", 0)),
            "RBI":    int(stats.get("RBI", 0)),
            "OBP":    obp,
            "SB":     int(stats.get("SB", 0)),
            "IP":     round(ip_dec, 1),
            "QS":     int(stats.get("QS", 0)),
            "ERA":    era,
            "WHIP":   whip,
            "K":      int(stats.get("K", 0)),
            "SV_HD":  sv_hd,
            "PA":     int(pa),
        }
    return weekly_teams

def get_weekly_category_leaders(weekly_teams: dict[int, dict]) -> list[str]:
    """Find the leader in each of the 10 roto categories for the week."""
    leader_lines = []
    for cat in ROTO_CATS:
        higher_better = CAT_HIGHER_IS_BETTER[cat]
        eligible = []
        for tid, s in weekly_teams.items():
            if cat == "OBP" and s.get("PA", 0) < 15:
                continue
            if cat in ("ERA", "WHIP") and s.get("IP", 0) < 10.0:
                continue
            val = s.get(cat, 0)
            eligible.append((tid, val))
        
        if not eligible:
            continue
            
        if higher_better:
            best_val = max(v for _, v in eligible)
            leaders = [tid for tid, v in eligible if v == best_val and v > 0]
        else:
            best_val = min(v for _, v in eligible)
            leaders = [tid for tid, v in eligible if v == best_val]

        if not leaders:
            continue

        names = "/".join(TEAM_NAMES.get(tid, f"T{tid}") for tid in leaders)
        disp_val = f"{best_val:.3f}" if cat in ("OBP", "WHIP") else (f"{best_val:.2f}" if cat == "ERA" else str(best_val))
        leader_lines.append(f"  {CAT_DISPLAY[cat]:>5}: {names} ({disp_val})")
    return leader_lines

def format_weekly_production_block(weekly_teams: dict[int, dict]) -> str:
    header = f"{'Team':<11} {'R':>3} {'HR':>3} {'RBI':>4} {'OBP':>6} {'SB':>3} | {'IP':>6} {'K':>4} {'QS':>3} {'ERA':>6} {'WHIP':>6} {'SV+H':>5}"
    rows = [header, "─" * len(header)]
    for tid in sorted(weekly_teams, key=lambda x: TEAM_NAMES.get(x, "")):
        s = weekly_teams[tid]
        name = TEAM_NAMES.get(tid, f"T{tid}")
        rows.append(
            f"{name:<11} {s['R']:>3} {s['HR']:>3} {s['RBI']:>4} {s['OBP']:>6.3f} {s['SB']:>3} | "
            f"{s['IP']:>6.1f} {s['K']:>4} {s['QS']:>3} {s['ERA']:>6.2f} {s['WHIP']:>6.3f} {s['SV_HD']:>5}"
        )
    return "WEEKLY TEAM PRODUCTION (Active Lineups This Week):\n" + "\n".join(rows)

def build_weekly_prompt(
    week_start_date: date,
    week_end_date: date,
    weekly_teams: dict[int, dict],
    leader_lines: list[str],
    best_h: list[dict],
    best_p: list[dict],
    best_day_info: tuple,
    standings: dict,
    weekly_delta: dict,
    weekly_trades: list[dict] | None = None,
    history_block: str = "",
    mover_histories: list[str] | None = None,
    records: list[dict] | None = None,
) -> str:
    n_teams = len(standings)

    standout_lines = []
    for h in best_h[:3]:
        standout_lines.append(f"  - {h['name']} ({h['team']}): {h['h']}/{h['ab']}, {h['hr']} HR, {h['rbi']} RBI, {h['r']} R, {h['sb']} SB")
    for p in best_p[:3]:
        qs_str = ", QS" if p["qs"] else ""
        sv_str = f", {p['svhd']} SV+H" if p['svhd'] > 0 else ""
        standout_lines.append(f"  - {p['name']} ({p['team']}): {p['ip']} IP, {p['k']} K, {p['er']} ER{qs_str}{sv_str}")
    standouts_text = "\n".join(standout_lines) if standout_lines else "  (no standout performances)"

    best_day_text = f"{best_day_info[0]} (period {best_day_info[1]}, HR+RBI+R = {best_day_info[2]})" if best_day_info and best_day_info[0] else "None"

    movers = []
    for tid, d in sorted(weekly_delta.items(), key=lambda x: abs(x[1]["rank_change"]), reverse=True):
        if d["rank_change"] != 0:
            name = TEAM_NAMES.get(tid, f"Team {tid}")
            dir_ = "up" if d["rank_change"] > 0 else "down"
            pc   = d["points_change"]
            movers.append(f"  {name} moved {dir_} {abs(d['rank_change'])} spot(s) ({'+' if pc > 0 else ''}{pc} pts)")
    movers_text = "\n".join(movers) if movers else "  No rank changes this week"

    trades_section = ""
    if weekly_trades:
        block = format_trades_block(weekly_trades, "TRADES THIS WEEK")
        if block:
            trades_section = f"\n{block}\n"

    history_section = f"\n{history_block}\n" if history_block else ""
    movers_hist_section = f"\n{chr(10).join(mover_histories)}\n" if mover_histories else ""

    # Ingest real MLB news overlap and league context lore
    involved_tids = list(weekly_delta.keys())
    recap_context = format_recap_context(records or [], involved_tids, standings=standings, delta=weekly_delta)
    context_section = f"\n{recap_context}\n" if recap_context else ""

    return f"""You are the commissioner's voice for the HEFTYSTRONG fantasy baseball league.
EDITORIAL LENS: **The League Commissioner Feature Columnist**
Tone & Perspective: Writing an authoritative Sunday-night longform sports feature breaking down the week's strategic narrative arc, catastrophic collapses, and the evolving playoff landscape with high-literary wit and biting honesty.

Generate a WEEKLY RECAP for the week of {week_start_date.strftime('%b %d')} – {week_end_date.strftime('%b %d, %Y')}.
Keep it under 350 words. Fun, opinionated, and narrative-driven. No title. Format for Discord (plain text).
Our 10 roto scoring categories: R, HR, RBI, OBP, SB, QS, ERA, WHIP, K, SV+Holds.
Roto points: 1 (worst) to {n_teams} (best) per category. All stats are active-lineup only.

{BANNED_CLICHES_BLOCK}

CORE NARRATIVE INSTRUCTIONS:
- DO NOT list every team's stats. Tell the overarching story of the week!
- Reference 2-3 standout individual players from this week (e.g. {best_h[0]['name'] if best_h else 'top hitters'}).
- Call out who dominated categories this week, who suffered an embarrassing collapse, and who gained/lost the most roto ground.
- Weave real MLB news and headlines with fantasy roster performance when relevant.
- Focus heavily on in-season trends, pattern shifts over the course of the season, and category swings where managers are actively trading points.
- Leverage manager personas and category rivalries for authentic commissioner banter.
- IMPORTANT: Use the WEEKLY TEAM PRODUCTION and CATEGORY LEADERS tables below for THIS WEEK'S stats.
- The CUMULATIVE SEASON STANDINGS table at the bottom shows season-long cumulative totals — DO NOT cite season totals as this week's numbers!
- Use historical pedigree for sharp context (e.g. championships, past collapses).

{format_weekly_production_block(weekly_teams)}

CATEGORY LEADERS THIS WEEK (All 10 Categories):
{chr(10).join(leader_lines)}

BEST SINGLE-DAY BATTING: {best_day_text}

TOP INDIVIDUAL STANDOUTS THIS WEEK:
{standouts_text}

WEEK-OVER-WEEK ROTO STANDINGS MOVEMENT:
{movers_text}
{trades_section}{context_section}{history_section}{movers_hist_section}
CUMULATIVE SEASON ROTO STANDINGS (Reference for overall title race):
{format_standings_block(standings)}

Write the weekly recap now:"""

# ---------------------------------------------------------------------------
# DISCORD POSTING
# ---------------------------------------------------------------------------

def post_to_discord(title: str, body: str, color: int = 0x1DB954):
    if not DISCORD_WEBHOOK_URL:
        print(f"\n{'='*60}\n{title}\n{'='*60}\n{body}\n")
        return
    if len(body) > 4000:
        body = body[:3997] + "..."
    payload = {
        "embeds": [{
            "title":       title,
            "description": body,
            "color":       color,
            "footer":      {"text": f"HEFTYSTRONG • {datetime.now().strftime('%Y-%m-%d %H:%M')} ET"},
        }]
    }
    resp = requests.post(DISCORD_WEBHOOK_URL, json=payload,
                         headers={"Content-Type": "application/json"})
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {resp.status_code} {resp.text}")
    print(f"Posted: {title}")

# ---------------------------------------------------------------------------
# MAIN ENTRYPOINTS
# ---------------------------------------------------------------------------

def run_daily_recap(target_date: date | None = None):
    if target_date is None:
        # If running today (N), the target text describes yesterday (N-1)
        target_date = date.today() - timedelta(days=1)

    period_id      = scoring_period_for_date(target_date) # Yesterday (N-1)
    prev_period_id = period_id - 1                        # Day Before Yesterday (N-2)
    print(f"Running daily recap for {target_date} (Scoring Period {period_id})")
    print(f"Baseline comparison window: End of Period {prev_period_id} -> End of Period {period_id}")

    today_records = fetch_stats_for_periods([period_id])
    if not today_records:
        print(f"No records found for target period {period_id}. Skipping.")
        return

    print(f"Fetching cumulative standings data through period {period_id}...")
    all_curr = fetch_stats_up_to_period(period_id)
    
    print(f"Extracting baseline cumulative standings data through period {prev_period_id}...")
    all_prev = [r for r in all_curr if r.get("scoring_period_id", 0) <= prev_period_id] if prev_period_id >= 1 else []

    # Filter active spots and compute distinct, independent snapshots
    standings_curr = compute_roto_standings(filter_active(all_curr))
    standings_prev = compute_roto_standings(filter_active(all_prev)) if all_prev else standings_curr
    
    # Calculate the exact shifts in points and ranks from N-2 to N-1
    delta          = compute_standings_delta(standings_prev, standings_curr)

    totals = aggregate_by_team(filter_active(today_records))
    totals = compute_averages(totals)

    print("Fetching recent trades for context...")
    try:
        all_trades    = fetch_all_trades()
        recent_trades = filter_trades_by_days(all_trades, days=7)
        print(f"  Found {len(recent_trades)} trade rows in last 7 days.")
    except Exception as e:
        print(f"  Trade fetch failed (non-fatal): {e}")
        recent_trades = []

    # --- Post 1: AI narrative recap ---
    print("Generating AI recap...")
    prompt  = build_daily_prompt(target_date, totals, today_records, standings_curr, delta,
                                  recent_trades=recent_trades)
    summary = generate_ai_summary(prompt)
    post_to_discord(f"⚾  Daily Recap — {target_date.strftime('%A, %b %d')}", summary, color=0x1E90FF)

    # --- Post 2: Best / Worst performers ---
    print("Building performance tables...")
    best_h, worst_h, best_p, worst_p = get_best_worst_players(today_records, n=5)
    post_to_discord("📊  Today's Top & Bottom Performers",
                    format_performance_embed_body(best_h, worst_h, best_p, worst_p), color=0x2ECC71)

    # --- Post 3: Standings changes ---
    print("Building standings changes...")
    post_to_discord("📈  Roto Standings Update",
                    format_standings_changes_body(standings_curr, delta, timeframe="today"), color=0xFFD700)

    print("Daily recap complete.")

def run_weekly_recap(week_end_date: date | None = None):
    if week_end_date is None:
        today         = date.today()
        week_end_date = today - timedelta(days=today.weekday() + 1)

    week_start_date = week_end_date - timedelta(days=6)
    periods = [scoring_period_for_date(week_start_date + timedelta(days=i)) for i in range(7)]
    print(f"Running weekly recap for {week_start_date} – {week_end_date} (periods {periods})")

    records = fetch_stats_for_periods(periods)
    if not records:
        print("No records found for this week. Skipping.")
        return

    # 1. Weekly team stats & leaders across all 10 roto categories
    weekly_teams = compute_weekly_team_rates(records)
    leader_lines = get_weekly_category_leaders(weekly_teams)

    # 2. Weekly top performers (hitters & pitchers)
    best_h, best_p = get_weekly_top_players(records, n=5)

    # 3. Best single-day batting team
    daily_by_period = {
        pid: aggregate_by_team([r for r in records if r["scoring_period_id"] == pid])
        for pid in periods
    }
    best_day_team, best_day_score, best_day_period = None, -1, None
    for pid, day_totals in daily_by_period.items():
        for tid, stats in day_totals.items():
            score = stats.get("HR", 0) + stats.get("RBI", 0) + stats.get("R", 0)
            if score > best_day_score:
                best_day_score, best_day_team, best_day_period = score, TEAM_NAMES.get(tid, f"Team {tid}"), pid
    best_day_info = (best_day_team, best_day_period, best_day_score)

    period_end   = max(periods)
    period_start = min(periods) - 1

    print("Fetching cumulative standings for weekly comparison...")
    all_curr     = fetch_stats_up_to_period(period_end)
    all_week_ago = [r for r in all_curr if r.get("scoring_period_id", 0) <= period_start] if period_start >= 1 else []

    # Apply active filter before computing standings
    standings_curr     = compute_roto_standings(filter_active(all_curr))
    standings_week_ago = compute_roto_standings(filter_active(all_week_ago)) if all_week_ago else standings_curr
    weekly_delta       = compute_standings_delta(standings_week_ago, standings_curr)

    # Trades this week
    print("Fetching trades for weekly context...")
    try:
        all_trades    = fetch_all_trades()
        weekly_trades = filter_trades_by_days(all_trades, days=7)
        print(f"  Found {len(weekly_trades)} trade rows this week.")
    except Exception as e:
        print(f"  Trade fetch failed (non-fatal): {e}")
        weekly_trades = []

    # Historical context
    print("Loading historical context...")
    try:
        history_block = format_league_champions() + "\n\n" + format_all_active_owner_summaries()
    except Exception as e:
        print(f"  History load failed (non-fatal): {e}")
        history_block = ""

    # Owner history for big movers
    print("Loading mover owner histories...")
    mover_histories = []
    try:
        big_movers = [tid for tid, d in weekly_delta.items()
                      if abs(d["rank_change"]) >= 2 or abs(d["points_change"]) >= 5]
        for tid in big_movers:
            owner_name = TEAM_NAMES.get(tid, "")
            if owner_name:
                mover_histories.append(format_owner_history(owner_name))
    except Exception as e:
        print(f"  Mover history load failed (non-fatal): {e}")

    prompt = build_weekly_prompt(
        week_start_date=week_start_date,
        week_end_date=week_end_date,
        weekly_teams=weekly_teams,
        leader_lines=leader_lines,
        best_h=best_h,
        best_p=best_p,
        best_day_info=best_day_info,
        standings=standings_curr,
        weekly_delta=weekly_delta,
        weekly_trades=weekly_trades,
        history_block=history_block,
        mover_histories=mover_histories,
        records=records,
    )

    # --- Post 1: AI narrative recap ---
    print("Generating weekly AI recap...")
    summary = generate_ai_summary(prompt)
    title   = f"📊  Weekly Recap — {week_start_date.strftime('%b %d')}–{week_end_date.strftime('%b %d')}"
    post_to_discord(title, summary, color=0xFFD700)

    # --- Post 2: Week's Top Performers table ---
    print("Posting weekly performers table...")
    post_to_discord("⭐  Week's Top Performers",
                    format_weekly_performance_embed_body(best_h, best_p), color=0x2ECC71)

    # --- Post 3: Weekly Standings Update ---
    print("Posting weekly standings update...")
    post_to_discord("📈  Weekly Standings Update",
                    format_standings_changes_body(standings_curr, weekly_delta, timeframe="this week"), color=0xE67E22)
    print("Weekly recap complete.")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    target_arg = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].strip() else None
    target_date = date.fromisoformat(target_arg) if target_arg else None
    if mode == "weekly":
        run_weekly_recap(target_date)
    else:
        run_daily_recap(target_date)
