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
    url = SUPABASE_URL or "NOT SET"
    key = SUPABASE_KEY or "NOT SET"
    print(f"Supabase URL: '{url}' (len={len(url)})")
    print(f"Supabase Key starts with: '{key[:20]}' (len={len(key)})")
    return create_client(url, key)


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
    players = aggregate_by_player(records)

    # Compute rate stats for pitchers
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

    # ERA and WHIP leaders — require at least 10 IP (30 outs)
    eligible = [p for p in players.values() if p.get("IP", 0) >= 30 and p["ERA"] is not None]
    if eligible:
        best_era  = sorted(eligible, key=lambda x: x["ERA"])[:top_n]
        worst_era = sorted(eligible, key=lambda x: x["ERA"], reverse=True)[:top_n]
        best_whip = sorted(eligible, key=lambda x: x["WHIP"])[:top_n]

        lines.append("  ERA (best): " + ", ".join(
            f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {p['ERA']:.2f}" for p in best_era
        ))
        lines.append("  ERA (worst): " + ", ".join(
            f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {p['ERA']:.2f}" for p in worst_era
        ))
        lines.append("  WHIP (best): " + ", ".join(
            f"{p['full_name']} ({TEAM_NAMES.get(p['team_id'], '?')}): {p['WHIP']:.3f}" for p in best_whip
        ))

    return "\n".join(lines)

def get_team_player_block(records: list[dict], team_id: int, label: str) -> str:
    """Full player-level breakdown for a specific team over a set of records."""
    team_records = [r for r in records if r["team_id"] == team_id]
    if not team_records:
        return f"{label}: no data found."

    players = aggregate_by_player(team_records)
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
                "name": p["full_name"],
                "ip":   f"{innings_whole}.{extra_outs}",
                "k":    int(p.get("K", 0)),
                "er":   int(p.get("ER", 0)),
                "era":  era,
                "whip": whip,
                "qs":   int(p.get("QS", 0)),
                "svhd": int(p.get("SV", 0) + p.get("HD", 0)),
            })
        elif ab > 0:
            pa  = p.get("PA", 0)
            obp = round((p.get("H", 0) + p.get("BB", 0) + p.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
            hitters.append({
                "name": p["full_name"],
                "ab":   int(ab),
                "h":    int(p.get("H", 0)),
                "hr":   int(p.get("HR", 0)),
                "rbi":  int(p.get("RBI", 0)),
                "r":    int(p.get("R", 0)),
                "sb":   int(p.get("SB", 0)),
                "obp":  obp,
            })

    lines = [f"{label} — PLAYER BREAKDOWN:"]

    if hitters:
        hitters.sort(key=lambda x: -(x["hr"] * 4 + x["rbi"] * 2 + x["r"] + x["sb"] * 2))
        lines.append("  HITTERS:")
        for h in hitters:
            lines.append(
                f"    {h['name']}: {h['ab']} AB, {h['h']} H, {h['hr']} HR, "
                f"{h['rbi']} RBI, {h['r']} R, {h['sb']} SB, {h['obp']:.3f} OBP"
            )

    if pitchers:
        pitchers.sort(key=lambda x: x["era"])
        lines.append("  PITCHERS (sorted by ERA, best→worst):")
        for p in pitchers:
            lines.append(
                f"    {p['name']}: {p['ip']} IP, {p['k']} K, {p['er']} ER, "
                f"{p['era']:.2f} ERA, {p['whip']:.3f} WHIP, {p['qs']} QS, {p['svhd']} SV+H"
            )

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


import re

TREND_KEYWORDS   = ["recent", "lately", "this week", "last week", "trending",
                    "hot", "cold", "streak", "momentum", "moving"]
PLAYER_KEYWORDS  = ["who leads", "who has the most", "who has the best", "top player",
                    "best pitcher", "best hitter", "which player", "who is leading",
                    "strikeout leader", "home run leader", "hr leader", "era", "whip"]


def _parse_window(q: str, current_period: int) -> tuple[list[int] | None, str]:
    """Return (period list or None for full season, label)."""
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


def build_context(question: str, current_period: int) -> str:
    q     = question.lower()
    parts = []

    # --- Always: current standings ---
    print(f"  Fetching cumulative records through period {current_period}...")
    cumulative = fetch_stats_up_to_period(current_period)
    standings  = compute_roto_standings(cumulative)

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

    if mentioned_teams:
        print(f"  Building player breakdowns for {len(mentioned_teams)} team(s) [{window_label}]...")
        for tid in mentioned_teams:
            team_name = TEAM_NAMES.get(tid, f"Team {tid}")
            if window_periods:
                scoped = [r for r in cumulative if r["scoring_period_id"] in set(window_periods)]
            else:
                scoped = cumulative
            parts.append(get_team_player_block(scoped, tid, f"{team_name} ({window_label})"))

    # --- Trends ---
    if any(kw in q for kw in TREND_KEYWORDS):
        print("  Fetching trend data...")
        trend_block = get_trend_block(current_period)
        if trend_block:
            parts.append(trend_block)

    # --- League-wide player leaders ---
    if any(kw in q for kw in PLAYER_KEYWORDS) or "who" in q:
        print("  Computing player leaders...")
        if window_periods:
            scoped = [r for r in cumulative if r["scoring_period_id"] in set(window_periods)]
        else:
            scoped = cumulative
        parts.append(get_player_leaders_block(scoped))

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

    async def on_ready(self):                          # ← indented inside class
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


bot = HEFTYBot()                                       # ← bot defined here


@bot.tree.command(name="ping", description="Test bot and database connectivity")
async def ping_command(interaction: discord.Interaction):     # ← after bot
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
