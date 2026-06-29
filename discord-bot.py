"""
HEFTYSTRONG Fantasy Baseball Discord Bot
/ask [question] slash command — powered by Supabase + Gemini + Fangraphs ZiPS
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
    "willalexander": "Will",
    "adrianclark":   "Adrian",
    "tim":           "Tim",
    "alex":          "Alex",
    "dan":           "Dan",
    "garrett":       "Garrett",
    "anil":          "Anil",
    "mark":          "Mark",
    "preston":       "Preston",
}

SEASON_START = date(2026, 3, 25)

BENCH_IL_SLOTS  = {16, 17, 20, 21, 22}
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

def current_scoring_period() -> int:
    return max(1, (date.today() - SEASON_START).days + 1)

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# FANGRAPHS PROJECTIONS ENGINE
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation and suffixes for fuzzy matching."""
    if not name: return ""
    name = name.lower().strip()
    name = re.sub(r"[.\'\-]", "", name)
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", name).strip()
    return name

_proj_cache = {}
PROJ_CACHE_TTL = 3600 # Cache projections for 1 hour

def fetch_fangraphs_projections() -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    bat_url = "https://www.fangraphs.com/api/projections?type=rzips&stats=bat&pos=all&team=0&players=0&lg=all"
    pit_url = "https://www.fangraphs.com/api/projections?type=rzips&stats=pit&pos=all&team=0&players=0&lg=all"
    projections = {}
    try:
        bat_data = requests.get(bat_url, headers=headers, timeout=10).json()
        for p in bat_data:
            name = normalize_name(p.get("PlayerName", ""))
            if name:
                projections[name] = {
                    "PA": p.get("PA", 0), "AB": p.get("AB", 0), "H": p.get("H", 0),
                    "HR": p.get("HR", 0), "R": p.get("R", 0), "RBI": p.get("RBI", 0),
                    "SB": p.get("SB", 0), "BB": p.get("BB", 0), "HBP": p.get("HBP", 0)
                }
        pit_data = requests.get(pit_url, headers=headers, timeout=10).json()
        for p in pit_data:
            name = normalize_name(p.get("PlayerName", ""))
            if name:
                if name not in projections:
                    projections[name] = {}
                outs = p.get("IP", 0) * 3.0
                projections[name].update({
                    "IP": outs, "ER": p.get("ER", 0), "H_Allowed": p.get("H", 0),
                    "BB_Allowed": p.get("BB", 0), "K": p.get("SO", p.get("K", 0)),
                    "QS": p.get("QS", 0), "SV": p.get("SV", 0), "HD": p.get("HLD", 0)
                })
    except Exception as e:
        print(f"Error fetching Fangraphs projections: {e}")
    return projections

def get_cached_projections() -> dict:
    now = time.time()
    if "zips" in _proj_cache and now - _proj_cache["zips"][0] < PROJ_CACHE_TTL:
        return _proj_cache["zips"][1]
    proj = fetch_fangraphs_projections()
    _proj_cache["zips"] = (now, proj)
    return proj

# ---------------------------------------------------------------------------
# SUPABASE DATA FETCHING
# ---------------------------------------------------------------------------

def fetch_stats_up_to_period(max_period: int) -> list[dict]:
    all_records = []
    offset, page_size = 0, 1000
    while True:
        batch = (
            get_supabase()
            .table("player_daily_stats")
            .select("*")
            .lte("scoring_period_id", max_period)
            .order("id", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size: break
        offset += page_size
    return all_records

def fetch_stats_for_periods(periods: list[int]) -> list[dict]:
    if not periods: return []
    all_records = []
    offset, page_size = 0, 1000
    while True:
        batch = (
            get_supabase()
            .table("player_daily_stats")
            .select("*")
            .in_("scoring_period_id", periods)
            .order("id", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_records.extend(batch)
        if len(batch) < page_size: break
        offset += page_size
    return all_records

# ---------------------------------------------------------------------------
# AGGREGATION & STANDINGS
# ---------------------------------------------------------------------------

def filter_active(records: list[dict]) -> list[dict]:
    return [r for r in records if r.get("lineup_slot_id") not in BENCH_IL_SLOTS]

HITTING_STATS  = {'R', 'HR', 'RBI', 'SB', 'H', 'BB', 'HBP', 'PA', 'AB', 'SF'}
PITCHING_STATS = {'K', 'QS', 'IP', 'ER', 'H_Allowed', 'BB_Allowed', 'SV', 'HD'}

def aggregate_by_team(records: list[dict]) -> dict:
    totals: dict[int, dict] = {}
    for row in records:
        if row.get("scoring_period_id") == 0: continue

        tid  = row["team_id"]
        slot = row.get("lineup_slot_id")
        stats = row.get("stats", {})
        if isinstance(stats, str): stats = json.loads(stats)

        if slot in HITTING_SLOT_IDS:
            allowed = HITTING_STATS
        elif slot in PITCHING_SLOT_IDS:
            allowed = PITCHING_STATS
        else:
            continue

        if tid not in totals:
            totals[tid] = {}
        for stat, val in stats.items():
            if stat in allowed and isinstance(val, (int, float)):
                totals[tid][stat] = totals[tid].get(stat, 0) + val
    return totals

def aggregate_by_player(records: list[dict]) -> dict:
    players: dict[int, dict] = {}
    for row in records:
        if row.get("scoring_period_id") == 0: continue

        pid     = row["player_id"]
        slot    = row.get("lineup_slot_id")
        stats   = row.get("stats", {})
        if isinstance(stats, str): stats = json.loads(stats)

        if slot in HITTING_SLOT_IDS:
            allowed = HITTING_STATS
        elif slot in PITCHING_SLOT_IDS:
            allowed = PITCHING_STATS
        else:
            continue

        if pid not in players:
            players[pid] = {"full_name": row["full_name"], "team_id": row["team_id"]}
        for stat, val in stats.items():
            if stat in allowed and isinstance(val, (int, float)):
                players[pid][stat] = players[pid].get(stat, 0) + val
    return players

def espn_ip_to_innings(ip_val: float) -> float:
    return ip_val / 3.0

def compute_roto_from_totals(team_totals: dict) -> dict:
    team_cats: dict[int, dict] = {}
    for tid, stats in team_totals.items():
        pa     = stats.get("PA", 0)
        obp    = round((stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
        ip_dec = espn_ip_to_innings(stats.get("IP", 0))
        era    = round((stats.get("ER", 0) / ip_dec) * 9, 2) if ip_dec > 0 else 0.0
        whip   = round((stats.get("H_Allowed", 0) + stats.get("BB_Allowed", 0)) / ip_dec, 3) if ip_dec > 0 else 0.0

        team_cats[tid] = {
            "R":     int(stats.get("R", 0)), "HR":    int(stats.get("HR", 0)),
            "RBI":   int(stats.get("RBI", 0)), "OBP":   obp,
            "SB":    int(stats.get("SB", 0)), "QS":    int(stats.get("QS", 0)),
            "ERA":   era, "WHIP":  whip, "K":     int(stats.get("K", 0)),
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

    for rank, (tid, _) in enumerate(sorted(team_cats.items(), key=lambda x: x[1]["roto_points"], reverse=True), 1):
        team_cats[tid]["standing"] = rank

    return team_cats

def compute_roto_standings(records: list[dict]) -> dict:
    return compute_roto_from_totals(aggregate_by_team(filter_active(records)))

def compute_projected_standings(all_records: list[dict], projections: dict) -> dict:
    if not all_records or not projections:
        return compute_roto_standings(all_records)
    
    team_totals = aggregate_by_team(filter_active(all_records))
    latest_period = max((r["scoring_period_id"] for r in all_records), default=0)
    current_roster = [r for r in all_records if r["scoring_period_id"] == latest_period]
    
    for row in current_roster:
        tid  = row["team_id"]
        name = normalize_name(row.get("full_name", ""))
        proj = projections.get(name)
        if not proj: continue
            
        if tid not in team_totals:
            team_totals[tid] = {}
        for s in ['PA', 'AB', 'H', 'HR', 'R', 'RBI', 'SB', 'BB', 'HBP', 'IP', 'ER', 'H_Allowed', 'BB_Allowed', 'K', 'QS', 'SV', 'HD']:
            if s in proj:
                team_totals[tid][s] = team_totals[tid].get(s, 0) + proj[s]
                
    return compute_roto_from_totals(team_totals)


# ---------------------------------------------------------------------------
# FORMATTERS
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

# ---------------------------------------------------------------------------
# LLM / AI LOGIC
# ---------------------------------------------------------------------------

def generate_answer(question: str, context: str, asking_owner: str | None) -> str:
    prompt = f"""You are the commissioner's snarky, baseball-obsessed AI assistant for the HEFTYSTRONG fantasy baseball league.
The user is asking: "{question}"
{f"The user asking is: {asking_owner}." if asking_owner else ""}

Use ONLY the provided context below to answer. Do not guess stats if they aren't in the context.
If asked about projections, remind them it's based on Fangraphs ZiPS Rest-of-Season algorithms.
Keep it punchy, accurate, and under 250 words. Format cleanly for Discord.

=== CONTEXT ===
{context}
"""
    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
    return response.text.strip()

def build_context(question: str, current_period: int, asking_owner: str | None) -> str:
    parts = []
    q = question.lower()
    
    # Pre-fetch core DB records once
    cumulative = fetch_stats_up_to_period(current_period)

    # --- Standings & Projections ---
    if any(kw in q for kw in ["standings", "place", "rank", "winning", "losing", "score"]):
        print("  Fetching standings...")
        standings = compute_roto_standings(cumulative)
        parts.append(format_standings_block(standings))
        parts.append(format_cat_values_block(standings))

    if any(kw in q for kw in ["project", "forecast", "end of", "rest of", "zips", "final standing"]):
        print("  Fetching projected standings...")
        projs = get_cached_projections()
        proj_standings = compute_projected_standings(cumulative, projs)
        if proj_standings:
            parts.append(format_standings_block(proj_standings).replace("CURRENT ROTO STANDINGS", "PROJECTED FINAL ROTO STANDINGS (YTD + ZiPS RoS)"))

    # --- Yesterday / Daily ---
    if any(kw in q for kw in ["yesterday", "last night", "today", "did anyone"]):
        print("  Fetching yesterday stats...")
        yest_records = [r for r in cumulative if r["scoring_period_id"] == current_period - 1]
        active_yest  = filter_active(yest_records)
        player_agg   = aggregate_by_player(active_yest)
        
        lines = ["PLAYER STATS (YESTERDAY):"]
        for pid, stats in player_agg.items():
            tname = TEAM_NAMES.get(stats["team_id"], "?")
            lines.append(f"- {stats['full_name']} ({tname}): HR:{stats.get('HR',0)} RBI:{stats.get('RBI',0)} SB:{stats.get('SB',0)} K:{stats.get('K',0)} SV+H:{stats.get('SV',0)+stats.get('HD',0)}")
        parts.append("\n".join(lines[:50])) # Limit to prevent huge prompts

    # --- Historical ---
    if any(kw in q for kw in ["history", "champ", "past", "won", "last year", "title", "ring"]):
        try:
            print("  Fetching historical data...")
            load_historical_data()
            parts.append("LEAGUE HISTORY:\n" + format_league_champions() + "\n" + format_all_active_owner_summaries())
        except Exception as e:
            print(f"Error fetching history: {e}")

    if not parts:
        parts.append("No specific stats or standings were pulled because the question didn't match keywords (standings, projected, yesterday, history, etc.). Just chat normally.")

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# DISCORD BOT SETUP
# ---------------------------------------------------------------------------

class MyClient(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()
        print(f"Bot is ready. Logged in as {self.user}")

client = MyClient()

@client.tree.command(name="ask", description="Ask the commish bot about standings, projections, stats, or history.")
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

if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        print("Missing DISCORD_BOT_TOKEN!")
    else:
        client.run(DISCORD_BOT_TOKEN)
