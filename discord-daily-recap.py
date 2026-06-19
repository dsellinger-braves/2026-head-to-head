import os
import json
import requests
from datetime import datetime, timedelta, date
from supabase import create_client, Client
import google.genai as genai

# --- CONFIGURATION ---
LEAGUE_ID = 130215
YEAR = 2026

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Hardcoded team name mapping: ESPN team_id -> owner name
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


# Raw stat buckets used for daily team summaries
HITTING_CATS  = ["HR", "RBI", "R", "SB", "H", "AVG", "OBP"]
PITCHING_CATS = ["K", "IP", "QS", "SV", "HD", "ER", "H_Allowed", "BB_Allowed"]

# The 10 roto scoring categories
ROTO_CATS = ["R", "HR", "RBI", "OBP", "SB", "QS", "ERA", "WHIP", "K", "SV_HD"]

# True  = higher value is better for that category
# False = lower value is better (ERA, WHIP)
CAT_HIGHER_IS_BETTER = {
    "R":     True,
    "HR":    True,
    "RBI":   True,
    "OBP":   True,
    "SB":    True,
    "QS":    True,
    "ERA":   False,
    "WHIP":  False,
    "K":     True,
    "SV_HD": True,
}

CAT_DISPLAY = {
    "R":     "R",
    "HR":    "HR",
    "RBI":   "RBI",
    "OBP":   "OBP",
    "SB":    "SB",
    "QS":    "QS",
    "ERA":   "ERA",
    "WHIP":  "WHIP",
    "K":     "K",
    "SV_HD": "SV+H",
}

# Adjust to your league's actual Opening Day (period 1 = this date)
SEASON_START = date(2026, 3, 25)


def scoring_period_for_date(target_date: date) -> int:
    delta = (target_date - SEASON_START).days + 1
    return max(1, delta)


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase credentials not set in environment.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# DATA FETCHING  (paginated to handle 50k+ rows)
# ---------------------------------------------------------------------------

def _paginated_fetch(query_builder, page_size: int = 1000) -> list[dict]:
    """Paginate through a Supabase select query and return all rows."""
    all_records = []
    offset = 0
    while True:
        batch = (query_builder.range(offset, offset + page_size - 1).execute().data or [])
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records


def fetch_stats_for_periods(periods: list[int]) -> list[dict]:
    """Pull player_daily_stats rows for specific scoring period IDs."""
    supabase = get_supabase_client()
    qb = supabase.table("player_daily_stats").select("*").in_("scoring_period_id", periods)
    return _paginated_fetch(qb)


def fetch_stats_up_to_period(max_period: int) -> list[dict]:
    """Pull all player_daily_stats rows from period 1 through max_period (cumulative)."""
    supabase = get_supabase_client()
    qb = supabase.table("player_daily_stats").select("*").lte("scoring_period_id", max_period)
    return _paginated_fetch(qb)


# ---------------------------------------------------------------------------
# AGGREGATION HELPERS
# ---------------------------------------------------------------------------

def aggregate_by_team(records: list[dict]) -> dict:
    """Sum numeric stats across all players on each team. Returns {team_id: {stat: total}}."""
    totals: dict[int, dict] = {}
    for row in records:
        team_id = row["team_id"]
        stats   = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        if team_id not in totals:
            totals[team_id] = {}
        for stat, val in stats.items():
            if isinstance(val, (int, float)):
                totals[team_id][stat] = totals[team_id].get(stat, 0) + val
    return totals


def compute_averages(totals: dict) -> dict:
    """Add AVG / OBP rate stats to an already-aggregated team totals dict."""
    for team_id, stats in totals.items():
        if stats.get("AB", 0) > 0:
            stats["AVG"] = round(stats.get("H", 0) / stats["AB"], 3)
        if stats.get("PA", 0) > 0:
            stats["OBP"] = round(
                (stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / stats["PA"], 3
            )
    return totals


def espn_ip_to_innings(ip_val: float) -> float:
    """
    ESPN stores IP as 6.1 meaning 6 innings + 1 out (not 6.1 decimal innings).
    Convert to true decimal innings for ERA / WHIP math.
    """
    whole = int(ip_val)
    outs  = round((ip_val - whole) * 10)
    return whole + outs / 3.0


def find_top_player(records: list[dict], stat: str) -> dict | None:
    """Return the single player record with the highest value for a given stat."""
    best     = None
    best_val = -1
    for row in records:
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        val = stats.get(stat, 0)
        if isinstance(val, (int, float)) and val > best_val:
            best_val = val
            best     = {**row, "_stat_val": val, "_stat_name": stat}
    return best


# ---------------------------------------------------------------------------
# ROTO STANDINGS ENGINE
# ---------------------------------------------------------------------------

def compute_roto_standings(records: list[dict]) -> dict:
    """
    Compute full roto standings from a set of player_daily_stats records.

    Returns:
        {
          team_id: {
            "R": int, "HR": int, ...,   # raw category values
            "cat_points": {cat: float}, # roto points per category (n_teams = best)
            "roto_points": float,       # sum of cat_points
            "standing": int,            # 1 = first place
          }
        }
    """
    raw = aggregate_by_team(records)

    team_cats: dict[int, dict] = {}
    for team_id, stats in raw.items():
        # --- Hitting rate stats ---
        pa  = stats.get("PA", 0)
        obp = round(
            (stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / pa, 3
        ) if pa > 0 else 0.0

        # --- Pitching rate stats (with proper ESPN IP conversion) ---
        ip_raw = stats.get("IP", 0)
        ip_dec = espn_ip_to_innings(ip_raw) if ip_raw > 0 else 0.0
        era    = round((stats.get("ER", 0) / ip_dec) * 9, 2)    if ip_dec > 0 else 0.0
        whip   = round(
            (stats.get("H_Allowed", 0) + stats.get("BB_Allowed", 0)) / ip_dec, 3
        ) if ip_dec > 0 else 0.0

        sv_hd = int(stats.get("SV", 0) + stats.get("HD", 0))

        team_cats[team_id] = {
            "R":     int(stats.get("R",   0)),
            "HR":    int(stats.get("HR",  0)),
            "RBI":   int(stats.get("RBI", 0)),
            "OBP":   obp,
            "SB":    int(stats.get("SB",  0)),
            "QS":    int(stats.get("QS",  0)),
            "ERA":   era,
            "WHIP":  whip,
            "K":     int(stats.get("K",   0)),
            "SV_HD": sv_hd,
        }

    n_teams = len(team_cats)
    cat_points: dict[int, dict] = {tid: {} for tid in team_cats}

    for cat, higher_better in CAT_HIGHER_IS_BETTER.items():
        # Sort: best value first
        sorted_teams = sorted(
            team_cats.items(),
            key=lambda x: x[1][cat],
            reverse=higher_better,
        )

        # Assign roto points with tie-splitting (average of tied positions)
        i = 0
        while i < len(sorted_teams):
            j = i
            while (j < len(sorted_teams) - 1 and
                   sorted_teams[j][1][cat] == sorted_teams[j + 1][1][cat]):
                j += 1
            # Best rank = n_teams points, worst = 1 point
            avg_pts = sum(n_teams - k for k in range(i, j + 1)) / (j - i + 1)
            for k in range(i, j + 1):
                cat_points[sorted_teams[k][0]][cat] = round(avg_pts, 1)
            i = j + 1

    for team_id in team_cats:
        team_cats[team_id]["cat_points"]  = cat_points[team_id]
        team_cats[team_id]["roto_points"] = round(sum(cat_points[team_id].values()), 1)

    # Assign overall standing (1 = most roto points)
    for rank_idx, (team_id, _) in enumerate(
        sorted(team_cats.items(), key=lambda x: x[1]["roto_points"], reverse=True), 1
    ):
        team_cats[team_id]["standing"] = rank_idx

    return team_cats


def compute_standings_delta(prev: dict, curr: dict) -> dict:
    """
    Diff two standings snapshots.
    Returns {team_id: {rank_change, points_change, cat_changes}}
    rank_change > 0 means the team moved UP in the standings.
    """
    delta = {}
    for team_id, curr_data in curr.items():
        if team_id not in prev:
            continue
        prev_data = prev[team_id]
        delta[team_id] = {
            "rank_change":   prev_data["standing"] - curr_data["standing"],
            "points_change": round(curr_data["roto_points"] - prev_data["roto_points"], 1),
            "cat_changes": {
                cat: round(
                    curr_data["cat_points"].get(cat, 0) - prev_data["cat_points"].get(cat, 0), 1
                )
                for cat in ROTO_CATS
            },
        }
    return delta


# ---------------------------------------------------------------------------
# STANDINGS FORMATTERS (produce text blocks for the AI prompt)
# ---------------------------------------------------------------------------

def format_standings_block(standings: dict) -> str:
    """Full standings table with per-category roto points."""
    sorted_teams = sorted(standings.items(), key=lambda x: x[1]["standing"])
    header = f"{'#':<3} {'Team':<14} {'Pts':>5}  " + "  ".join(
        f"{CAT_DISPLAY[c]:>5}" for c in ROTO_CATS
    )
    rows = [header]
    for team_id, data in sorted_teams:
        name = TEAM_NAMES.get(team_id, f"Team {team_id}")
        cats = "  ".join(f"{data['cat_points'].get(c, 0):>5.1f}" for c in ROTO_CATS)
        rows.append(f"{data['standing']:<3} {name:<14} {data['roto_points']:>5.1f}  {cats}")
    return "CURRENT ROTO STANDINGS:\n" + "\n".join(rows)


def format_cat_values_block(standings: dict) -> str:
    """Actual category stat values per team (separate from roto points)."""
    sorted_teams = sorted(standings.items(), key=lambda x: x[1]["standing"])
    fmt = {
        "R": "d", "HR": "d", "RBI": "d", "OBP": ".3f",
        "SB": "d", "QS": "d", "ERA": ".2f", "WHIP": ".3f",
        "K": "d", "SV_HD": "d",
    }
    header = f"{'#':<3} {'Team':<14}  " + "  ".join(
        f"{CAT_DISPLAY[c]:>7}" for c in ROTO_CATS
    )
    rows = [header]
    for team_id, data in sorted_teams:
        name = TEAM_NAMES.get(team_id, f"Team {team_id}")
        vals = "  ".join(f"{data[c]:{fmt[c]}}" .rjust(7) for c in ROTO_CATS)
        rows.append(f"{data['standing']:<3} {name:<14}  {vals}")
    return "ACTUAL CATEGORY VALUES:\n" + "\n".join(rows)


def format_delta_block(standings: dict, delta: dict) -> str:
    """Today's movement per team — rank changes and which categories shifted."""
    sorted_teams = sorted(standings.items(), key=lambda x: x[1]["standing"])
    lines = ["TODAY'S STANDINGS MOVEMENT:"]
    for team_id, _ in sorted_teams:
        if team_id not in delta:
            continue
        d    = delta[team_id]
        name = TEAM_NAMES.get(team_id, f"Team {team_id}")

        if d["rank_change"] > 0:
            rank_str = f"▲{d['rank_change']}"
        elif d["rank_change"] < 0:
            rank_str = f"▼{abs(d['rank_change'])}"
        else:
            rank_str = "—"

        pts_str = f"+{d['points_change']}" if d["points_change"] > 0 else str(d["points_change"])

        # Only show categories that actually changed
        notable = [
            f"{CAT_DISPLAY[c]}: {'+' if v > 0 else ''}{v}"
            for c, v in d["cat_changes"].items() if v != 0
        ]
        cat_str = ", ".join(notable) if notable else "no change"
        lines.append(f"  {name} ({rank_str}, {pts_str} pts today): {cat_str}")
    return "\n".join(lines)


def format_weekly_movers_block(delta: dict) -> str:
    """Week-over-week standing changes sorted by magnitude."""
    movers = sorted(delta.items(), key=lambda x: abs(x[1]["rank_change"]), reverse=True)
    lines  = ["WEEKLY STANDINGS MOVEMENT:"]
    for team_id, d in movers:
        if d["rank_change"] == 0:
            continue
        name      = TEAM_NAMES.get(team_id, f"Team {team_id}")
        direction = "up" if d["rank_change"] > 0 else "down"
        pts_str   = f"+{d['points_change']}" if d["points_change"] > 0 else str(d["points_change"])
        lines.append(
            f"  {name} moved {direction} {abs(d['rank_change'])} spot(s) "
            f"({pts_str} roto pts this week)"
        )
    if len(lines) == 1:
        lines.append("  No standing position changes this week")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# AI SUMMARY
# ---------------------------------------------------------------------------

def generate_ai_summary(prompt: str) -> str:
    client   = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
    return response.text


# ---------------------------------------------------------------------------
# PROMPT BUILDERS
# ---------------------------------------------------------------------------

def build_daily_prompt(
    period_date: date,
    team_totals: dict,
    records: list[dict],
    standings: dict,
    delta: dict,
) -> str:
    team_lines = []
    for team_id, stats in sorted(team_totals.items()):
        name  = TEAM_NAMES.get(team_id, f"Team {team_id}")
        h_str = ", ".join(f"{s}: {stats[s]}" for s in HITTING_CATS  if s in stats)
        p_str = ", ".join(f"{s}: {stats[s]}" for s in PITCHING_CATS if s in stats)
        team_lines.append(f"  {name}: [{h_str}] | [{p_str}]")

    top_hr  = find_top_player(records, "HR")
    top_k   = find_top_player(records, "K")
    top_rbi = find_top_player(records, "RBI")

    top_notes = []
    if top_hr  and top_hr["_stat_val"]  > 0:
        top_notes.append(
            f"{top_hr['full_name']} ({TEAM_NAMES.get(top_hr['team_id'], top_hr['team_id'])}) "
            f"went deep {int(top_hr['_stat_val'])}x"
        )
    if top_k   and top_k["_stat_val"]   > 0:
        top_notes.append(
            f"{top_k['full_name']} ({TEAM_NAMES.get(top_k['team_id'], top_k['team_id'])}) "
            f"struck out {int(top_k['_stat_val'])} batters"
        )
    if top_rbi and top_rbi["_stat_val"] > 0:
        top_notes.append(
            f"{top_rbi['full_name']} ({TEAM_NAMES.get(top_rbi['team_id'], top_rbi['team_id'])}) "
            f"drove in {int(top_rbi['_stat_val'])} runs"
        )

    individual_block = (
        "\n".join(f"  - {n}" for n in top_notes)
        if top_notes else "  (No standout individual performances today)"
    )

    return f"""You are the commissioner's snarky, trash-talking fantasy baseball bot for the HEFTYSTRONG league.
Generate a daily recap for {period_date.strftime('%A, %B %d, %Y')}.

Be fun, roast bad days, hype up good ones. Keep it under 350 words. Use emojis sparingly but effectively.
Do NOT include a title — just the body text. Format it for Discord (plain text, no markdown headers).

Structure the recap to cover:
1. Notable individual performances from today
2. How today shifted the roto standings — call out anyone who climbed, fell, or made a big category move
3. Any category races heating up or teams in danger of losing ground

Our 10 roto scoring categories are: R, HR, RBI, OBP, SB, QS, ERA, WHIP, K, SV+Holds.
ERA and WHIP are inverse (lower = better). All others: higher = better.
Roto points range from 1 (worst) to {len(standings)} (best) per category.

TODAY'S TEAM STATS:
{chr(10).join(team_lines)}

INDIVIDUAL STANDOUTS:
{individual_block}

{format_standings_block(standings)}

{format_cat_values_block(standings)}

{format_delta_block(standings, delta)}

Write the recap now:"""


def build_weekly_prompt(
    week_start: date,
    week_end: date,
    team_totals: dict,
    records: list[dict],
    daily_totals_by_period: dict,
    standings_curr: dict,
    standings_week_ago: dict,
) -> str:
    team_lines = []
    for team_id, stats in sorted(team_totals.items()):
        name  = TEAM_NAMES.get(team_id, f"Team {team_id}")
        h_str = ", ".join(f"{s}: {stats[s]}" for s in HITTING_CATS  if s in stats)
        p_str = ", ".join(f"{s}: {stats[s]}" for s in PITCHING_CATS if s in stats)
        team_lines.append(f"  {name}: [{h_str}] | [{p_str}]")

    # Weekly category leaders (by raw stats this week, not roto points)
    leaders = {}
    for cat in HITTING_CATS + PITCHING_CATS:
        best = max(team_totals.items(), key=lambda kv: kv[1].get(cat, 0), default=None)
        if best and best[1].get(cat, 0) > 0:
            leaders[cat] = (TEAM_NAMES.get(best[0], f"Team {best[0]}"), best[1][cat])
    leader_lines = [f"  {cat}: {name} ({val})" for cat, (name, val) in leaders.items()]

    # Best single-day team (highest HR+RBI+R in one scoring period)
    best_day_team, best_day_score, best_day_period = None, -1, None
    for period_id, day_totals in daily_totals_by_period.items():
        for team_id, stats in day_totals.items():
            score = stats.get("HR", 0) + stats.get("RBI", 0) + stats.get("R", 0)
            if score > best_day_score:
                best_day_score  = score
                best_day_team   = TEAM_NAMES.get(team_id, f"Team {team_id}")
                best_day_period = period_id

    weekly_delta = compute_standings_delta(standings_week_ago, standings_curr)

    return f"""You are the commissioner's snarky, trash-talking fantasy baseball bot for the HEFTYSTRONG league.
Generate a WEEKLY RECAP for the week of {week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}.

Be fun, roast the worst teams, crown the heroes. Under 450 words. Emojis welcome but not excessive.
Do NOT include a title — just the body text. Format it for Discord (plain text).

Our 10 roto scoring categories are: R, HR, RBI, OBP, SB, QS, ERA, WHIP, K, SV+Holds.
ERA and WHIP are inverse (lower = better). Roto points 1 (worst) to {len(standings_curr)} (best) per category.

Cover these naturally woven together:
1. Category leaders for the week and which teams dominated
2. Best single-day team performance
3. How the standings shifted — who climbed, who fell, who's at risk
4. Who looks dangerous or vulnerable heading into next week

WEEKLY TEAM TOTALS:
{chr(10).join(team_lines)}

CATEGORY LEADERS THIS WEEK:
{chr(10).join(leader_lines)}

BEST SINGLE DAY: {best_day_team} on scoring period {best_day_period} (HR+RBI+R = {best_day_score})

{format_standings_block(standings_curr)}

{format_cat_values_block(standings_curr)}

{format_weekly_movers_block(weekly_delta)}

Write the recap now:"""


# ---------------------------------------------------------------------------
# DISCORD POSTING
# ---------------------------------------------------------------------------

def post_to_discord(title: str, body: str, color: int = 0x1DB954):
    if not DISCORD_WEBHOOK_URL:
        print(f"\n{'='*60}\n{title}\n{'='*60}\n{body}\n")
        return
    payload = {
        "embeds": [{
            "title":       title,
            "description": body,
            "color":       color,
            "footer":      {"text": f"HEFTYSTRONG • {datetime.now().strftime('%Y-%m-%d %H:%M')} ET"},
        }]
    }
    resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers={"Content-Type": "application/json"})
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {resp.status_code} {resp.text}")
    print(f"Discord post successful: {title}")


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

    print(f"Fetching cumulative data through period {period_id} for standings...")
    all_records_curr = fetch_stats_up_to_period(period_id)
    all_records_prev = fetch_stats_up_to_period(prev_period_id) if prev_period_id >= 1 else []

    standings_curr = compute_roto_standings(all_records_curr)
    standings_prev = compute_roto_standings(all_records_prev) if all_records_prev else standings_curr
    delta          = compute_standings_delta(standings_prev, standings_curr)

    totals = aggregate_by_team(today_records)
    totals = compute_averages(totals)

    prompt  = build_daily_prompt(target_date, totals, today_records, standings_curr, delta)
    summary = generate_ai_summary(prompt)

    title = f"⚾ Daily Recap — {target_date.strftime('%A, %b %d')}"
    post_to_discord(title, summary, color=0x1E90FF)


def run_weekly_recap(week_end_date: date | None = None):
    if week_end_date is None:
        today         = date.today()
        week_end_date = today - timedelta(days=today.weekday() + 1)  # last Sunday

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

    weekly_totals = aggregate_by_team(records)
    weekly_totals = compute_averages(weekly_totals)

    daily_totals_by_period = {
        period_id: aggregate_by_team([r for r in records if r["scoring_period_id"] == period_id])
        for period_id in periods
    }

    period_end   = max(periods)
    period_start = min(periods) - 1

    print(f"Fetching cumulative data for standings comparison...")
    all_curr          = fetch_stats_up_to_period(period_end)
    all_week_ago      = fetch_stats_up_to_period(period_start) if period_start >= 1 else []

    standings_curr     = compute_roto_standings(all_curr)
    standings_week_ago = compute_roto_standings(all_week_ago) if all_week_ago else standings_curr

    prompt  = build_weekly_prompt(
        week_start_date, week_end_date,
        weekly_totals, records,
        daily_totals_by_period,
        standings_curr, standings_week_ago,
    )
    summary = generate_ai_summary(prompt)

    title = f"📊 Weekly Recap — {week_start_date.strftime('%b %d')}–{week_end_date.strftime('%b %d')}"
    post_to_discord(title, summary, color=0xFFD700)


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    if mode == "weekly":
        run_weekly_recap()
    else:
        run_daily_recap()
