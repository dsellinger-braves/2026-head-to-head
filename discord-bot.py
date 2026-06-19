"""
HEFTYSTRONG Fantasy Baseball Discord Bot
/ask [question] slash command — powered by Supabase + Gemini
"""

import os
import json
import asyncio
import discord
from discord import app_commands
from datetime import date, timedelta, datetime
from supabase import create_client, Client
import google.genai as genai

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
SUPABASE_URL      = os.environ.get("SUPABASE_URL")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY")

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


# Reverse map so questions mentioning team names can be resolved to IDs
TEAM_NAME_TO_ID = {v.lower(): k for k, v in TEAM_NAMES.items()}

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


def current_scoring_period() -> int:
    delta = (date.today() - SEASON_START).days
    return max(1, delta)


def scoring_period_for_date(d: date) -> int:
    return max(1, (d - SEASON_START).days + 1)


# ---------------------------------------------------------------------------
# SUPABASE  (paginated to handle 50k+ rows)
# ---------------------------------------------------------------------------

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_stats_up_to_period(max_period: int) -> list[dict]:
    all_records, last_id, page_size = [], 0, 1000
    while True:
        batch = (
            get_supabase()
            .table("player_daily_stats")
            .select("*")
            .lte("scoring_period_id", max_period)
            .gt("id", last_id)
            .order("id", desc=False)
            .limit(page_size)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        last_id = batch[-1]["id"]
    return all_records


def fetch_stats_for_periods(periods: list[int]) -> list[dict]:
    all_records, last_id, page_size = [], 0, 1000
    while True:
        batch = (
            get_supabase()
            .table("player_daily_stats")
            .select("*")
            .in_("scoring_period_id", periods)
            .gt("id", last_id)
            .order("id", desc=False)
            .limit(page_size)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        last_id = batch[-1]["id"]
    return all_records


# ---------------------------------------------------------------------------
# AGGREGATION + STANDINGS  (shared logic with discord-daily-recap.py)
# ---------------------------------------------------------------------------

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
    whole = int(ip_val)
    outs  = round((ip_val - whole) * 10)
    return whole + outs / 3.0


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
            "R": int(stats.get("R", 0)), "HR": int(stats.get("HR", 0)),
            "RBI": int(stats.get("RBI", 0)), "OBP": obp,
            "SB": int(stats.get("SB", 0)), "QS": int(stats.get("QS", 0)),
            "ERA": era, "WHIP": whip,
            "K": int(stats.get("K", 0)), "SV_HD": int(stats.get("SV", 0) + stats.get("HD", 0)),
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
    """Aggregate stats per individual player across all records."""
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
    """Top N players per key stat for the AI to reference."""
    players = aggregate_by_player(records)
    key_stats = ["HR", "RBI", "R", "SB", "K", "QS", "SV", "HD"]
    lines = [f"TOP {top_n} PLAYERS BY STAT (season to date):"]
    for stat in key_stats:
        top = sorted(players.values(), key=lambda x: x.get(stat, 0), reverse=True)[:top_n]
        top = [p for p in top if p.get(stat, 0) > 0]
        if top:
            entries = ", ".join(
                f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {int(p.get(stat, 0))}"
                for p in top
            )
            lines.append(f"  {stat}: {entries}")
    return "\n".join(lines)


def get_trend_block(current_period: int) -> str:
    """Compare last 7 days vs the 7 days before that."""
    recent_periods = list(range(max(1, current_period - 6), current_period + 1))
    prior_periods  = list(range(max(1, current_period - 13), max(1, current_period - 6)))

    recent_records = fetch_stats_for_periods(recent_periods)
    prior_records  = fetch_stats_for_periods(prior_periods)

    if not recent_records or not prior_records:
        return ""

    recent_standings = compute_roto_standings(recent_records)
    prior_standings  = compute_roto_standings(prior_records)
    delta            = compute_standings_delta(prior_standings, recent_standings)

    lines = ["LAST 7 DAYS ROTO MOVEMENT (vs. prior 7 days):"]
    for tid in sorted(recent_standings, key=lambda x: recent_standings[x]["standing"]):
        if tid not in delta:
            continue
        d    = delta[tid]
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        rc   = d["rank_change"]
        pc   = d["points_change"]
        arrow = f"▲{rc}" if rc > 0 else (f"▼{abs(rc)}" if rc < 0 else "—")
        pts  = f"+{pc}" if pc > 0 else str(pc)
        gains   = [CAT_DISPLAY[c] for c, v in d["cat_changes"].items() if v > 0]
        losses  = [CAT_DISPLAY[c] for c, v in d["cat_changes"].items() if v < 0]
        g_str   = f"gained {', '.join(gains)}" if gains else ""
        l_str   = f"lost {', '.join(losses)}"  if losses else ""
        move    = "; ".join(filter(None, [g_str, l_str])) or "no change"
        lines.append(f"  {name} ({arrow}, {pts} pts this week): {move}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CONTEXT BUILDER  — selects what data to include based on the question
# ---------------------------------------------------------------------------

TREND_KEYWORDS    = ["recent", "lately", "this week", "last week", "trending",
                     "hot", "cold", "streak", "momentum", "moving"]
PLAYER_KEYWORDS   = ["who leads", "who has the most", "who has the best", "top player",
                     "best pitcher", "best hitter", "which player", "who is leading",
                     "strikeout leader", "home run leader", "hr leader"]
HISTORY_KEYWORDS  = ["history", "all season", "since the start", "beginning of",
                     "back in", "early season", "compare"]


def build_context(question: str, current_period: int) -> str:
    """
    Build a focused data context for Gemini based on what the question is asking.
    Always includes standings. Adds trend or player data as needed.
    """
    q      = question.lower()
    parts  = []

    # --- Core: current roto standings (always) ---
    print(f"  Fetching cumulative records through period {current_period}...")
    cumulative = fetch_stats_up_to_period(current_period)
    standings  = compute_roto_standings(cumulative)

    # Standings table with roto points per category
    sorted_teams = sorted(standings.items(), key=lambda x: x[1]["standing"])
    header = f"{'#':<3} {'Team':<14} {'Pts':>5}  " + "  ".join(f"{CAT_DISPLAY[c]:>5}" for c in ROTO_CATS)
    rows   = [header]
    for tid, data in sorted_teams:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        cats = "  ".join(f"{data['cat_points'].get(c, 0):>5.1f}" for c in ROTO_CATS)
        rows.append(f"{data['standing']:<3} {name:<14} {data['roto_points']:>5.1f}  {cats}")
    parts.append("CURRENT ROTO STANDINGS (roto points per category):\n" + "\n".join(rows))

    # Actual stat values
    fmt = {"R":"d","HR":"d","RBI":"d","OBP":".3f","SB":"d","QS":"d","ERA":".2f","WHIP":".3f","K":"d","SV_HD":"d"}
    val_header = f"{'#':<3} {'Team':<14}  " + "  ".join(f"{CAT_DISPLAY[c]:>7}" for c in ROTO_CATS)
    val_rows   = [val_header]
    for tid, data in sorted_teams:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        vals = "  ".join(f"{data[c]:{fmt[c]}}".rjust(7) for c in ROTO_CATS)
        val_rows.append(f"{data['standing']:<3} {name:<14}  {vals}")
    parts.append("ACTUAL CATEGORY VALUES:\n" + "\n".join(val_rows))

    # --- Trends: last 7 vs prior 7 ---
    if any(kw in q for kw in TREND_KEYWORDS):
        print("  Fetching trend data (last 14 periods)...")
        trend_block = get_trend_block(current_period)
        if trend_block:
            parts.append(trend_block)

    # --- Player leaders ---
    if any(kw in q for kw in PLAYER_KEYWORDS) or "who" in q:
        print("  Computing player leaders...")
        parts.append(get_player_leaders_block(cumulative))

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# GEMINI  — answer the question using the assembled context
# ---------------------------------------------------------------------------

def generate_answer(question: str, context: str) -> str:
    n_teams = len(TEAM_NAMES)
    prompt  = f"""You are the HEFTYSTRONG fantasy baseball league's stats bot.
Answer the following question using ONLY the data provided. Be direct, concise, and a little snarky.
Keep your answer under 300 words so it fits comfortably in Discord.

League format: {n_teams}-team roto league.
Scoring categories: R, HR, RBI, OBP, SB, QS, ERA, WHIP, K, SV+Holds.
ERA and WHIP: lower = better. All other categories: higher = better.
Roto points: 1 (worst in category) to {n_teams} (best in category). Max possible score = {n_teams * 10}.

QUESTION: {question}

LEAGUE DATA:
{context}

Answer the question directly. If the data doesn't support a specific claim, say so rather than guessing."""

    client   = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
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
    # Supabase connectivity test
    try:
        result = get_supabase().table("player_daily_stats").select("scoring_period_id").limit(1).execute()
        print(f"Supabase OK — sample row: {result.data}")
    except Exception as e:
        print(f"Supabase FAILED — {e}")


bot = HEFTYBot()


@bot.tree.command(name="ask", description="Ask about HEFTYSTRONG stats, standings, trends, and more")
@app_commands.describe(question="Your question about the league — e.g. 'Who leads in HR?' or 'Which team has the best ERA?'")
async def ask_command(interaction: discord.Interaction, question: str):
    # Defer immediately — Supabase + Gemini will take a few seconds
    await interaction.response.defer(thinking=True)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] /ask from {interaction.user}: {question}")

    try:
        period = current_scoring_period()

        # Run blocking I/O off the event loop so we don't freeze the bot
        context = await asyncio.to_thread(build_context, question, period)
        answer  = await asyncio.to_thread(generate_answer, question, context)

        # Discord messages cap at 2000 chars — truncate gracefully if needed
        if len(answer) > 1900:
            answer = answer[:1897] + "..."

        embed = discord.Embed(
            description = answer,
            color       = 0x1E90FF,
        )
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
