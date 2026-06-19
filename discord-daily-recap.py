"""
HEFTYSTRONG Fantasy Baseball — Discord Daily Recap
Produces three Discord posts per day:
  1. AI narrative recap
  2. Best / Worst 5 hitters and pitchers for the day
  3. Roto standings changes summary
"""

import os
import json
import requests
from datetime import datetime, timedelta, date
from supabase import create_client, Client
import google.genai as genai

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

LEAGUE_ID = 130215
YEAR      = 2025

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
    14: "Preston"
}

# ---------------------------------------------------------------------------
# LINEUP SLOT IDs
# Verify these against your league by running:
#   SELECT DISTINCT lineup_slot_id FROM player_daily_stats ORDER BY lineup_slot_id;
# Standard ESPN MLB slot IDs:
#   0=C  1=1B  2=2B  3=3B  4=SS  5-7=OF  8=UTIL
#   9-12=SP  13-15=RP/P
#   16=IL  17=IL+  20-22=Bench (BE)
# ---------------------------------------------------------------------------

BENCH_IL_SLOTS  = {16, 17, 20, 21, 22}   # excluded from daily tables
HITTING_SLOTS   = {0, 1, 2, 3, 4, 5, 6, 7, 8}
PITCHING_SLOTS  = {9, 10, 11, 12, 13, 14, 15}

# Roto categories
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
# SUPABASE  (paginated to handle 50k+ rows)
# ---------------------------------------------------------------------------

def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase credentials not configured.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _paginated_fetch(qb, page_size: int = 1000) -> list[dict]:
    all_records, offset = [], 0
    while True:
        batch = (qb.range(offset, offset + page_size - 1).execute().data or [])
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records


def fetch_stats_for_periods(periods: list[int]) -> list[dict]:
    qb = get_supabase().table("player_daily_stats").select("*").in_("scoring_period_id", periods)
    return _paginated_fetch(qb)


def fetch_stats_up_to_period(max_period: int) -> list[dict]:
    qb = get_supabase().table("player_daily_stats").select("*").lte("scoring_period_id", max_period)
    return _paginated_fetch(qb)


# ---------------------------------------------------------------------------
# AGGREGATION HELPERS
# ---------------------------------------------------------------------------

def filter_active(records: list[dict]) -> list[dict]:
    """Remove bench and IL players from a set of records."""
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
    whole = int(ip_val)
    outs  = round((ip_val - whole) * 10)
    return whole + outs / 3.0


# ---------------------------------------------------------------------------
# ROTO STANDINGS ENGINE
# ---------------------------------------------------------------------------

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
            "R": int(stats.get("R", 0)),   "HR": int(stats.get("HR", 0)),
            "RBI": int(stats.get("RBI", 0)), "OBP": obp,
            "SB": int(stats.get("SB", 0)),  "QS": int(stats.get("QS", 0)),
            "ERA": era, "WHIP": whip,
            "K": int(stats.get("K", 0)),
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
# DAILY PERFORMANCE TABLES  (active players only)
# ---------------------------------------------------------------------------

def _hitter_score(stats: dict) -> float:
    """Composite score for ranking a hitter's day. Higher = better."""
    return (stats.get("HR", 0)  * 4 +
            stats.get("RBI", 0) * 2 +
            stats.get("R",   0) * 1 +
            stats.get("SB",  0) * 2 +
            stats.get("H",   0) * 0.5)


def _pitcher_score(stats: dict) -> float:
    """Composite score for ranking a pitcher's day. Higher = better."""
    return (stats.get("K",  0)  * 1 +
            stats.get("QS", 0)  * 10 +
            (stats.get("SV", 0) + stats.get("HD", 0)) * 5 -
            stats.get("ER", 0)  * 3)


def get_best_worst_players(records: list[dict], n: int = 5):
    """
    Returns (best_hitters, worst_hitters, best_pitchers, worst_pitchers)
    using active-roster players only (bench/IL filtered out).
    Hitters must have at least 1 AB; pitchers must have pitched.
    """
    active   = filter_active(records)
    hitters  = []
    pitchers = []

    for row in active:
        slot  = row.get("lineup_slot_id")
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)

        name    = row["full_name"]
        team    = TEAM_NAMES.get(row["team_id"], f"T{row['team_id']}")

        if slot in HITTING_SLOTS:
            ab = int(stats.get("AB", 0))
            pa = int(stats.get("PA", 0))
            if ab + pa < 1:      # didn't bat at all — skip
                continue
            hitters.append({
                "name":  name,
                "team":  team,
                "score": _hitter_score(stats),
                "ab":    ab,
                "h":     int(stats.get("H",   0)),
                "hr":    int(stats.get("HR",  0)),
                "rbi":   int(stats.get("RBI", 0)),
                "r":     int(stats.get("R",   0)),
                "sb":    int(stats.get("SB",  0)),
            })

        elif slot in PITCHING_SLOTS:
            ip_raw = stats.get("IP", 0)
            if ip_raw <= 0:      # didn't pitch — skip
                continue
            whole  = int(ip_raw)
            outs   = round((ip_raw - whole) * 10)
            pitchers.append({
                "name":  name,
                "team":  team,
                "score": _pitcher_score(stats),
                "ip":    f"{whole}.{outs}",
                "k":     int(stats.get("K",  0)),
                "er":    int(stats.get("ER", 0)),
                "qs":    int(stats.get("QS", 0)),
                "svhd":  int(stats.get("SV", 0) + stats.get("HD", 0)),
            })

    # Best: highest score, tiebreak by AB desc for hitters
    best_hitters   = sorted(hitters,  key=lambda x: (-x["score"], -x["ab"]))[:n]
    # Worst: lowest score among those with enough ABs (min 2 to exclude pinch hitters)
    worst_hitters  = sorted(
        [h for h in hitters if h["ab"] >= 2],
        key=lambda x: (x["score"], -x["ab"])
    )[:n]
    best_pitchers  = sorted(pitchers, key=lambda x: -x["score"])[:n]
    worst_pitchers = sorted(pitchers, key=lambda x:  x["score"])[:n]

    return best_hitters, worst_hitters, best_pitchers, worst_pitchers


def _hitter_table(players: list[dict], title: str) -> str:
    hdr  = f"{'Player':<18} {'Team':<11} {'H':>2} {'HR':>2} {'RBI':>3} {'R':>2} {'SB':>2}"
    sep  = "─" * len(hdr)
    rows = [title, hdr, sep]
    for p in players:
        rows.append(
            f"{p['name'][:17]:<18} {p['team'][:10]:<11} "
            f"{p['h']:>2} {p['hr']:>2} {p['rbi']:>3} {p['r']:>2} {p['sb']:>2}"
        )
    return "\n".join(rows)


def _pitcher_table(players: list[dict], title: str) -> str:
    hdr  = f"{'Player':<18} {'Team':<11} {'IP':>4} {'K':>2} {'ER':>3} {'QS':>2} {'SV+H':>4}"
    sep  = "─" * len(hdr)
    rows = [title, hdr, sep]
    for p in players:
        qs = "✓" if p["qs"] else "·"
        rows.append(
            f"{p['name'][:17]:<18} {p['team'][:10]:<11} "
            f"{p['ip']:>4} {p['k']:>2} {p['er']:>3} {qs:>2} {p['svhd']:>4}"
        )
    return "\n".join(rows)


def format_performance_embed_body(
    best_h, worst_h, best_p, worst_p
) -> str:
    """Combine all four tables into one code-block string for a Discord embed."""
    sections = [
        _hitter_table(best_h,  "🟢 BEST HITTERS"),
        _hitter_table(worst_h, "🔴 WORST HITTERS"),
        _pitcher_table(best_p,  "🟢 BEST PITCHERS"),
        _pitcher_table(worst_p, "🔴 WORST PITCHERS"),
    ]
    return "```\n" + "\n\n".join(sections) + "\n```"


# ---------------------------------------------------------------------------
# STANDINGS CHANGES EMBED
# ---------------------------------------------------------------------------

def format_standings_changes_body(standings: dict, delta: dict) -> str:
    """
    Two sections:
      1. Overall rank movers (teams that changed position)
      2. Category-level roto point changes
    """
    lines = []

    # --- Overall rank movers ---
    movers = [(tid, d) for tid, d in delta.items() if d["rank_change"] != 0]
    movers.sort(key=lambda x: abs(x[1]["rank_change"]), reverse=True)

    if movers:
        lines.append("**Overall rank changes**")
        for tid, d in movers:
            name  = TEAM_NAMES.get(tid, f"Team {tid}")
            pts   = standings[tid]["roto_points"]
            rc    = d["rank_change"]
            pc    = d["points_change"]
            arrow = "▲" if rc > 0 else "▼"
            pc_str = f"+{pc}" if pc > 0 else str(pc)
            lines.append(
                f"{arrow} **{name}** #{d['prev_standing']} → #{d['curr_standing']} "
                f"({pc_str} pts today, {pts:.1f} total)"
            )
    else:
        lines.append("**Overall rank changes**\n*No position changes today*")

    lines.append("")

    # --- Category movers ---
    # For each roto category, list teams that gained or lost roto points
    cat_moved = False
    cat_lines = ["**Category roto point changes**"]

    for cat in ROTO_CATS:
        gainers = sorted(
            [(tid, d["cat_changes"][cat]) for tid, d in delta.items() if d["cat_changes"].get(cat, 0) > 0],
            key=lambda x: -x[1]
        )
        losers  = sorted(
            [(tid, d["cat_changes"][cat]) for tid, d in delta.items() if d["cat_changes"].get(cat, 0) < 0],
            key=lambda x:  x[1]
        )
        if not gainers and not losers:
            continue

        cat_moved = True
        g_str = ", ".join(
            f"{TEAM_NAMES.get(tid, f'T{tid}')} +{v:.1f}" for tid, v in gainers
        )
        l_str = ", ".join(
            f"{TEAM_NAMES.get(tid, f'T{tid}')} {v:.1f}" for tid, v in losers
        )
        parts = []
        if g_str: parts.append(f"▲ {g_str}")
        if l_str: parts.append(f"▼ {l_str}")
        cat_lines.append(f"`{CAT_DISPLAY[cat]:>5}` {' | '.join(parts)}")

    if cat_moved:
        lines.extend(cat_lines)
    else:
        lines.append("**Category roto point changes**\n*No category movement today*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# STANDINGS SUMMARY BLOCK  (for AI prompt context — not the embed)
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
        d    = delta[tid]
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        rc   = d["rank_change"]
        pc   = d["points_change"]
        arrow = f"▲{rc}" if rc > 0 else (f"▼{abs(rc)}" if rc < 0 else "—")
        pc_s  = f"+{pc}" if pc > 0 else str(pc)
        notable = [f"{CAT_DISPLAY[c]}: {'+' if v > 0 else ''}{v}" for c, v in d["cat_changes"].items() if v != 0]
        cat_s = ", ".join(notable) if notable else "no movement"
        lines.append(f"  {name} ({arrow}, {pc_s} pts): {cat_s}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# AI SUMMARY
# ---------------------------------------------------------------------------

def generate_ai_summary(prompt: str) -> str:
    client   = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    return response.text


def build_daily_prompt(
    period_date: date,
    team_totals: dict,
    today_records: list[dict],
    standings: dict,
    delta: dict,
) -> str:
    """
    Prompt for the narrative recap only.
    Stat tables and standings changes are posted as separate embeds,
    so this prompt focuses on storytelling, not listing numbers.
    """
    # Find individual standouts among active players only
    active   = filter_active(today_records)
    top_hr   = max(active, key=lambda r: json.loads(r["stats"]).get("HR", 0) if isinstance(r["stats"], str) else r["stats"].get("HR", 0), default=None)
    top_k    = max(active, key=lambda r: json.loads(r["stats"]).get("K", 0)  if isinstance(r["stats"], str) else r["stats"].get("K", 0),  default=None)
    top_rbi  = max(active, key=lambda r: json.loads(r["stats"]).get("RBI", 0) if isinstance(r["stats"], str) else r["stats"].get("RBI", 0), default=None)

    def stat_val(row, s):
        st = row["stats"]
        if isinstance(st, str): st = json.loads(st)
        return st.get(s, 0)

    notes = []
    if top_hr  and stat_val(top_hr,  "HR")  > 0:
        notes.append(f"{top_hr['full_name']} ({TEAM_NAMES.get(top_hr['team_id'], '?')}) hit {int(stat_val(top_hr, 'HR'))} HR")
    if top_k   and stat_val(top_k,   "K")   > 0:
        notes.append(f"{top_k['full_name']} ({TEAM_NAMES.get(top_k['team_id'], '?')}) had {int(stat_val(top_k, 'K'))} K")
    if top_rbi and stat_val(top_rbi, "RBI") > 0:
        notes.append(f"{top_rbi['full_name']} ({TEAM_NAMES.get(top_rbi['team_id'], '?')}) drove in {int(stat_val(top_rbi, 'RBI'))} runs")
    standouts = "\n".join(f"  - {n}" for n in notes) if notes else "  (no standout performances)"

    n_teams = len(standings)

    return f"""You are the commissioner's snarky, trash-talking fantasy baseball bot for the HEFTYSTRONG league.
Write a short narrative daily recap for {period_date.strftime('%A, %B %d, %Y')}.

Keep it under 250 words and fun — roast the losers, hype the winners.
Do NOT include a title. Format for Discord (plain text, no markdown headers).

IMPORTANT: Specific stat tables and standings changes will be shown separately in Discord.
Do NOT list every team's stat line. Instead, tell the story — reference 2-3 notable things
and weave in what the day meant for the roto race.

TOP INDIVIDUAL PERFORMANCES TODAY:
{standouts}

{format_standings_block(standings)}

{format_delta_block(standings, delta)}

Write the narrative recap now:"""


# ---------------------------------------------------------------------------
# DISCORD POSTING
# ---------------------------------------------------------------------------

def post_to_discord(title: str, body: str, color: int = 0x1DB954):
    if not DISCORD_WEBHOOK_URL:
        print(f"\n{'='*60}\n{title}\n{'='*60}\n{body}\n")
        return

    # Discord embed description limit is 4096 chars
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
    resp = requests.post(
        DISCORD_WEBHOOK_URL, json=payload,
        headers={"Content-Type": "application/json"}
    )
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {resp.status_code} {resp.text}")
    print(f"Posted: {title}")


# ---------------------------------------------------------------------------
# MAIN ENTRYPOINTS
# ---------------------------------------------------------------------------

def run_daily_recap(target_date: date | None = None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    period_id      = scoring_period_for_date(target_date)
    prev_period_id = period_id - 1

    print(f"Running daily recap for {target_date} (scoring period {period_id})")

    today_records = fetch_stats_for_periods([period_id])
    if not today_records:
        print("No records found for this period. Skipping.")
        return

    print(f"Fetching cumulative standings data through period {period_id}...")
    all_curr = fetch_stats_up_to_period(period_id)
    all_prev = fetch_stats_up_to_period(prev_period_id) if prev_period_id >= 1 else []

    standings_curr = compute_roto_standings(all_curr)
    standings_prev = compute_roto_standings(all_prev) if all_prev else standings_curr
    delta          = compute_standings_delta(standings_prev, standings_curr)

    totals = aggregate_by_team(filter_active(today_records))
    totals = compute_averages(totals)

    # --- Post 1: AI narrative recap ---
    print("Generating AI recap...")
    prompt  = build_daily_prompt(target_date, totals, today_records, standings_curr, delta)
    summary = generate_ai_summary(prompt)
    post_to_discord(
        f"⚾  Daily Recap — {target_date.strftime('%A, %b %d')}",
        summary,
        color=0x1E90FF,
    )

    # --- Post 2: Best / Worst performers ---
    print("Building performance tables...")
    best_h, worst_h, best_p, worst_p = get_best_worst_players(today_records, n=5)
    perf_body = format_performance_embed_body(best_h, worst_h, best_p, worst_p)
    post_to_discord(
        "📊  Today's Top & Bottom Performers",
        perf_body,
        color=0x2ECC71,
    )

    # --- Post 3: Standings changes ---
    print("Building standings changes...")
    standings_body = format_standings_changes_body(standings_curr, delta)
    post_to_discord(
        "📈  Roto Standings Update",
        standings_body,
        color=0xFFD700,
    )

    print("Daily recap complete.")


def run_weekly_recap(week_end_date: date | None = None):
    if week_end_date is None:
        today         = date.today()
        week_end_date = today - timedelta(days=today.weekday() + 1)

    week_start_date = week_end_date - timedelta(days=6)
    periods = [
        scoring_period_for_date(week_start_date + timedelta(days=i))
        for i in range(7)
    ]
    print(f"Running weekly recap for {week_start_date} – {week_end_date} (periods {periods})")

    records = fetch_stats_for_periods(periods)
    if not records:
        print("No records found for this week. Skipping.")
        return

    weekly_totals = aggregate_by_team(filter_active(records))
    weekly_totals = compute_averages(weekly_totals)

    daily_by_period = {
        pid: aggregate_by_team([r for r in records if r["scoring_period_id"] == pid])
        for pid in periods
    }

    period_end   = max(periods)
    period_start = min(periods) - 1

    print("Fetching cumulative standings for weekly comparison...")
    all_curr     = fetch_stats_up_to_period(period_end)
    all_week_ago = fetch_stats_up_to_period(period_start) if period_start >= 1 else []

    standings_curr     = compute_roto_standings(all_curr)
    standings_week_ago = compute_roto_standings(all_week_ago) if all_week_ago else standings_curr
    weekly_delta       = compute_standings_delta(standings_week_ago, standings_curr)

    # Weekly category leaders
    leaders = {}
    for cat in ["HR", "RBI", "R", "SB", "K", "QS"]:
        best = max(weekly_totals.items(), key=lambda kv: kv[1].get(cat, 0), default=None)
        if best and best[1].get(cat, 0) > 0:
            leaders[cat] = (TEAM_NAMES.get(best[0], f"Team {best[0]}"), best[1][cat])
    leader_lines = [f"  {cat}: {name} ({val})" for cat, (name, val) in leaders.items()]

    # Best single day
    best_day_team, best_day_score, best_day_period = None, -1, None
    for pid, day_totals in daily_by_period.items():
        for tid, stats in day_totals.items():
            score = stats.get("HR", 0) + stats.get("RBI", 0) + stats.get("R", 0)
            if score > best_day_score:
                best_day_score  = score
                best_day_team   = TEAM_NAMES.get(tid, f"Team {tid}")
                best_day_period = pid

    # Weekly movers for prompt
    movers = []
    for tid, d in sorted(weekly_delta.items(), key=lambda x: abs(x[1]["rank_change"]), reverse=True):
        if d["rank_change"] != 0:
            name = TEAM_NAMES.get(tid, f"Team {tid}")
            dir_ = "up" if d["rank_change"] > 0 else "down"
            pc   = d["points_change"]
            movers.append(f"  {name} moved {dir_} {abs(d['rank_change'])} spot(s) ({'+' if pc > 0 else ''}{pc} pts)")

    n_teams = len(standings_curr)

    prompt = f"""You are the commissioner's snarky, trash-talking fantasy baseball bot for the HEFTYSTRONG league.
Generate a WEEKLY RECAP for the week of {week_start_date.strftime('%b %d')} – {week_end_date.strftime('%b %d, %Y')}.

Under 400 words. Fun and opinionated. No title. Format for Discord (plain text).
Our 10 roto scoring categories: R, HR, RBI, OBP, SB, QS, ERA, WHIP, K, SV+Holds.
Roto points: 1 (worst) to {n_teams} (best) per category.

Cover: weekly category leaders, best single-day performance, standings movement, who to watch next week.

CATEGORY LEADERS THIS WEEK:
{chr(10).join(leader_lines)}

BEST SINGLE DAY: {best_day_team} (period {best_day_period}, HR+RBI+R = {best_day_score})

{format_standings_block(standings_curr)}

{format_cat_values_block(standings_curr)}

WEEKLY STANDINGS MOVEMENT:
{chr(10).join(movers) if movers else '  No rank changes this week'}

Write the weekly recap now:"""

    print("Generating weekly AI recap...")
    summary = generate_ai_summary(prompt)

    title = f"📊  Weekly Recap — {week_start_date.strftime('%b %d')}–{week_end_date.strftime('%b %d')}"
    post_to_discord(title, summary, color=0xFFD700)
    post_to_discord(
        "📈  Weekly Standings Update",
        format_standings_changes_body(standings_curr, weekly_delta),
        color=0xE67E22,
    )
    print("Weekly recap complete.")


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    if mode == "weekly":
        run_weekly_recap()
    else:
        run_daily_recap()
