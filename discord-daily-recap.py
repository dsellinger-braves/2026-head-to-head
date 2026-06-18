import os
import json
import requests
from datetime import datetime, timedelta, date
from supabase import create_client, Client
from anthropic import Anthropic

# --- CONFIGURATION ---
LEAGUE_ID = 130215
YEAR = 2026

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

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

# Hitting and pitching categories we care about for summaries
HITTING_CATS  = ["HR", "RBI", "R", "SB", "H", "AVG", "OBP"]
PITCHING_CATS = ["K", "IP", "QS", "SV", "HD", "ER", "H_Allowed", "BB_Allowed"]

# Scoring period -> calendar date mapping helper
# ESPN scoring periods for MLB typically start on Opening Day.
# Adjust SEASON_START to the actual first day of your league's season.
SEASON_START = date(2026, 3, 29)  # Update to your league's Opening Day


def scoring_period_for_date(target_date: date) -> int:
    """Return the ESPN scoring period ID for a given calendar date."""
    delta = (target_date - SEASON_START).days + 1
    return max(1, delta)


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase credentials not set in environment.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# DATA FETCHING
# ---------------------------------------------------------------------------

def fetch_stats_for_periods(periods: list[int]) -> list[dict]:
    """Pull all player_daily_stats rows for the given scoring period IDs."""
    supabase = get_supabase_client()
    # Supabase in() filter
    response = (
        supabase.table("player_daily_stats")
        .select("*")
        .in_("scoring_period_id", periods)
        .execute()
    )
    return response.data or []


def aggregate_by_team(records: list[dict]) -> dict:
    """
    Sum numeric stats across all players on each team for the given records.
    Returns { team_id: { stat_name: total, ... } }
    """
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
    """Add computed AVG / OBP across the team if raw components are present."""
    for team_id, stats in totals.items():
        if stats.get("AB", 0) > 0:
            stats["AVG"] = round(stats.get("H", 0) / stats["AB"], 3)
        if stats.get("PA", 0) > 0:
            stats["OBP"] = round(
                (stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0))
                / stats["PA"],
                3,
            )
    return totals


def find_top_player(records: list[dict], stat: str) -> dict | None:
    """Return the single player record with the highest value for a given stat."""
    best = None
    best_val = -1
    for row in records:
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)
        val = stats.get(stat, 0)
        if isinstance(val, (int, float)) and val > best_val:
            best_val = val
            best = {**row, "_stat_val": val, "_stat_name": stat}
    return best


# ---------------------------------------------------------------------------
# AI SUMMARY GENERATION
# ---------------------------------------------------------------------------

def generate_ai_summary(prompt: str) -> str:
    """Call Claude to generate the recap text."""
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def build_daily_prompt(period_date: date, team_totals: dict, records: list[dict]) -> str:
    team_lines = []
    for team_id, stats in sorted(team_totals.items()):
        name  = TEAM_NAMES.get(team_id, f"Team {team_id}")
        h_str = ", ".join(
            f"{s}: {stats[s]}" for s in HITTING_CATS if s in stats
        )
        p_str = ", ".join(
            f"{s}: {stats[s]}" for s in PITCHING_CATS if s in stats
        )
        team_lines.append(f"  {name}: [{h_str}] | [{p_str}]")

    team_block = "\n".join(team_lines)

    # Best individual performances
    top_hr   = find_top_player(records, "HR")
    top_k    = find_top_player(records, "K")
    top_rbi  = find_top_player(records, "RBI")

    top_notes = []
    if top_hr and top_hr["_stat_val"] > 0:
        top_notes.append(
            f"{top_hr['full_name']} ({TEAM_NAMES.get(top_hr['team_id'], top_hr['team_id'])})"
            f" went deep {int(top_hr['_stat_val'])}x"
        )
    if top_k and top_k["_stat_val"] > 0:
        top_notes.append(
            f"{top_k['full_name']} ({TEAM_NAMES.get(top_k['team_id'], top_k['team_id'])})"
            f" struck out {int(top_k['_stat_val'])} batters"
        )
    if top_rbi and top_rbi["_stat_val"] > 0:
        top_notes.append(
            f"{top_rbi['full_name']} ({TEAM_NAMES.get(top_rbi['team_id'], top_rbi['team_id'])})"
            f" drove in {int(top_rbi['_stat_val'])} runs"
        )

    individual_block = "\n".join(f"  - {n}" for n in top_notes) if top_notes else "  (No standout individual performances today)"

    return f"""You are the commissioner's snarky, trash-talking fantasy baseball bot for the HEFTYSTRONG league.
Generate a daily recap for {period_date.strftime('%A, %B %d, %Y')}.

Be fun, roast bad days, hype up good ones. Keep it under 300 words. Use emojis sparingly but effectively.
Do NOT include a title — just the body text. Format it for Discord (plain text, no markdown headers).

TEAM STAT TOTALS FOR THE DAY:
{team_block}

INDIVIDUAL STANDOUTS:
{individual_block}

Write the recap now:"""


def build_weekly_prompt(
    week_start: date,
    week_end: date,
    team_totals: dict,
    records: list[dict],
    daily_totals_by_period: dict,
) -> str:
    team_lines = []
    for team_id, stats in sorted(team_totals.items()):
        name  = TEAM_NAMES.get(team_id, f"Team {team_id}")
        h_str = ", ".join(
            f"{s}: {stats[s]}" for s in HITTING_CATS if s in stats
        )
        p_str = ", ".join(
            f"{s}: {stats[s]}" for s in PITCHING_CATS if s in stats
        )
        team_lines.append(f"  {name}: [{h_str}] | [{p_str}]")

    # Category leaders
    leaders = {}
    for cat in HITTING_CATS + PITCHING_CATS:
        best_team = max(team_totals.items(), key=lambda kv: kv[1].get(cat, 0), default=None)
        if best_team and best_team[1].get(cat, 0) > 0:
            leaders[cat] = (TEAM_NAMES.get(best_team[0], f"Team {best_team[0]}"), best_team[1][cat])

    leader_lines = [f"  {cat}: {name} ({val})" for cat, (name, val) in leaders.items()]

    # Best single-day performance (team with highest total HR + RBI + R in one day)
    best_day_team = None
    best_day_score = -1
    best_day_period = None
    for period_id, day_totals in daily_totals_by_period.items():
        for team_id, stats in day_totals.items():
            score = stats.get("HR", 0) + stats.get("RBI", 0) + stats.get("R", 0)
            if score > best_day_score:
                best_day_score  = score
                best_day_team   = TEAM_NAMES.get(team_id, f"Team {team_id}")
                best_day_period = period_id

    return f"""You are the commissioner's snarky, trash-talking fantasy baseball bot for the HEFTYSTRONG league.
Generate a WEEKLY RECAP for the week of {week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}.

Be fun, roast the worst teams, crown the heroes. Under 400 words. Emojis welcome but not excessive.
Do NOT include a title — just the body text. Format it for Discord (plain text).

Include all four of these sections naturally woven into the recap:
1. Category leaders for the week
2. Best single-day team performance
3. Standings movement (comment on who's rising/falling based on weekly totals)
4. Matchup projections / who looks dangerous heading into next week

WEEKLY TEAM TOTALS:
{chr(10).join(team_lines)}

CATEGORY LEADERS:
{chr(10).join(leader_lines)}

BEST SINGLE DAY: {best_day_team} on scoring period {best_day_period} (HR+RBI+R = {best_day_score})

Write the recap now:"""


# ---------------------------------------------------------------------------
# DISCORD POSTING
# ---------------------------------------------------------------------------

def post_to_discord(title: str, body: str, color: int = 0x1DB954):
    """Send an embed to Discord via webhook."""
    if not DISCORD_WEBHOOK_URL:
        print("No DISCORD_WEBHOOK_URL set — printing to stdout instead.")
        print(f"\n{'='*60}\n{title}\n{'='*60}\n{body}\n")
        return

    payload = {
        "embeds": [
            {
                "title": title,
                "description": body,
                "color": color,
                "footer": {"text": f"HEFTYSTRONG • {datetime.now().strftime('%Y-%m-%d %H:%M')} ET"},
            }
        ]
    }

    resp = requests.post(
        DISCORD_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code not in (200, 204):
        raise RuntimeError(f"Discord webhook failed: {resp.status_code} {resp.text}")
    print(f"Discord post successful: {title}")


# ---------------------------------------------------------------------------
# MAIN ENTRYPOINT
# ---------------------------------------------------------------------------

def run_daily_recap(target_date: date | None = None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)  # yesterday by default

    period_id = scoring_period_for_date(target_date)
    print(f"Running daily recap for {target_date} (scoring period {period_id})")

    records    = fetch_stats_for_periods([period_id])
    if not records:
        print("No records found for this period. Skipping.")
        return

    totals     = aggregate_by_team(records)
    totals     = compute_averages(totals)
    prompt     = build_daily_prompt(target_date, totals, records)
    summary    = generate_ai_summary(prompt)

    title = f"⚾ Daily Recap — {target_date.strftime('%A, %b %d')}"
    post_to_discord(title, summary, color=0x1E90FF)


def run_weekly_recap(week_end_date: date | None = None):
    """Run on Mondays to recap the previous week (Mon–Sun)."""
    if week_end_date is None:
        # Last Sunday
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

    # Aggregate totals for the whole week
    weekly_totals = aggregate_by_team(records)
    weekly_totals = compute_averages(weekly_totals)

    # Also break out daily totals for best-single-day detection
    daily_totals_by_period = {}
    for period_id in periods:
        day_records = [r for r in records if r["scoring_period_id"] == period_id]
        daily_totals_by_period[period_id] = aggregate_by_team(day_records)

    prompt  = build_weekly_prompt(
        week_start_date, week_end_date,
        weekly_totals, records,
        daily_totals_by_period,
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
