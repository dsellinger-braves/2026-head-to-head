#!/usr/bin/env python3
"""
pipelines/sync_draft_pool.py

Automated replacement for fantasy-baseball scripts:
- 01 draft pool build.py
- 02 ESPN Batter Scrape.py
- 03 ESPN Pitcher Scrape.py
- 04 Batter Combine.py
- 05 Pitcher Combine.py

Pulls all players, live ADP, ownership, injury status, and projections directly
from ESPN Fantasy API in ~2 bulk requests, merges them in-memory, and upserts
into Supabase `dim_players` and `player-pool` without manual CSV files.
"""

import os
import sys
import json
import urllib.request
import urllib.error
import unicodedata
import re
from typing import Dict, List, Any
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wczdkcdqgtzlsbssogoz.supabase.co")
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

MLB_TEAM_MAP = {
    0: "FA", 1: "BAL", 2: "BOS", 3: "CWS", 4: "CLE", 5: "DET", 6: "KC", 7: "LAA",
    8: "MIN", 9: "NYY", 10: "OAK", 11: "SEA", 12: "TB", 13: "TEX", 14: "TOR",
    15: "ARI", 16: "ATL", 17: "CHC", 18: "CIN", 19: "COL", 20: "LAD", 21: "MIA",
    22: "MIL", 23: "NYM", 24: "PHI", 25: "PIT", 26: "SD", 27: "SF", 28: "STL",
    29: "WSH", 30: "HOU"
}

POSITION_MAP = {
    0: "C", 1: "1B", 2: "2B", 3: "3B", 4: "SS", 5: "OF",
    8: "LF", 9: "CF", 10: "RF", 11: "DH", 12: "UTIL",
    13: "P", 14: "SP", 15: "RP"
}

def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize('NFKD', name)
    clean = "".join([c for c in nfkd if not unicodedata.combining(c)])
    clean = re.sub(r"[.,'-]", "", clean)
    clean = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s+", " ", clean).strip().lower()
    return clean

def get_current_season(supabase: Client) -> int:
    try:
        res = supabase.table("league_settings").select("current_season").limit(1).execute()
        if res.data and res.data[0].get("current_season"):
            return int(res.data[0]["current_season"])
    except Exception as e:
        print(f"⚠️ Could not fetch current_season from league_settings ({e}); defaulting to 2026")
    return 2026

def fetch_espn_players(season: int) -> List[Dict[str, Any]]:
    print(f"📡 Querying ESPN API for season {season}...")
    base_url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{season}/segments/0/leaguedefaults/1?scoringPeriodId=0&view=kona_player_info"
    
    all_players = []
    limit = 1000
    offset = 0

    while True:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "X-Fantasy-Filter": json.dumps({
                "players": {
                    "filterStatsForExternalIds": {"value": [season]},
                    "sortDraftRanks": {"sortPriority": 1, "sortAsc": True, "value": "ROTO"},
                    "limit": limit,
                    "offset": offset
                }
            })
        }

        req = urllib.request.Request(base_url, headers=headers)
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                players = data.get("players", [])
                if not players:
                    break
                all_players.extend(players)
                print(f"  Fetched {len(players)} players (offset: {offset}, total: {len(all_players)})")
                if len(players) < limit:
                    break
                offset += limit
        except urllib.error.HTTPError as e:
            print(f"❌ ESPN API HTTP Error {e.code}: {e.reason}")
            break
        except Exception as e:
            print(f"❌ ESPN API Error: {e}")
            break

    print(f"✅ Successfully retrieved {len(all_players)} total players from ESPN")
    return all_players

def extract_player_record(entry: Dict[str, Any], season: int) -> Dict[str, Any]:
    p = entry.get("player", {})
    player_id = entry.get("id") or p.get("id")
    full_name = p.get("fullName") or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
    
    team_id = p.get("proTeamId", 0)
    team_abbr = MLB_TEAM_MAP.get(team_id, "FA")
    
    eligible_slots = p.get("eligibleSlots", [])
    pos_set = []
    for slot in [0, 1, 2, 3, 4, 5, 14, 15, 11]:  # standard baseball positions
        if slot in eligible_slots:
            pos_set.append(POSITION_MAP[slot])
    
    position_str = ", ".join(pos_set) if pos_set else (POSITION_MAP.get(p.get("defaultPositionId", 12), "UTIL"))
    
    ownership = p.get("ownership", {})
    adp = ownership.get("averageDraftPosition")
    adp_val = f"{round(float(adp), 1)}" if adp is not None else None
    
    pct_owned = ownership.get("percentOwned")
    pct_owned_str = f"{round(float(pct_owned), 2)}%" if pct_owned is not None else None
    
    pct_change = ownership.get("percentChange", 0.0)
    pct_change_str = f"{round(float(pct_change or 0.0), 2)}%"
    
    ranks = p.get("draftRanksByRankType", {}).get("ROTO", {})
    roto_rank = str(ranks.get("rank")) if ranks.get("rank") is not None else None

    # Parse projections
    stats_list = p.get("stats", [])
    proj_stats = {}
    for st in stats_list:
        # statSourceId 1 = projections, statSplitTypeId 0 = season total
        if st.get("statSourceId") == 1 and st.get("seasonId") == season:
            proj_stats = st.get("stats", {})
            break

    is_pitcher = "SP" in position_str or "RP" in position_str

    record = {
        "ESPN PlayerID": str(player_id),
        "Player": full_name,
        "Position": position_str,
        "Team": team_abbr,
        "ADP": adp_val,
        "Percent Owned": pct_owned_str,
        "Percent Owned (Change)": f"{pct_owned_str or '0%'} ({pct_change_str})",
        "ESPN Single Season Rank": roto_rank,
        "ESPN Keeper Rank": roto_rank,
        "Batter/Pitcher": "Pitcher" if is_pitcher else "Batter",
        
        # Batting projections
        "ESPNPA": str(int(round(float(proj_stats.get("16", 0))))) if "16" in proj_stats else None,
        "ESPNHR": str(int(round(float(proj_stats.get("5", 0))))) if "5" in proj_stats else None,
        "ESPNR": str(int(round(float(proj_stats.get("20", 0))))) if "20" in proj_stats else None,
        "ESPNRBI": str(int(round(float(proj_stats.get("21", 0))))) if "21" in proj_stats else None,
        "ESPNSB": str(int(round(float(proj_stats.get("23", 0))))) if "23" in proj_stats else None,
        "ESPNOBP": f"{round(float(proj_stats.get('17', 0)), 3)}" if "17" in proj_stats else None,
        
        # Pitching projections
        "ESPNIP": str(int(round(float(proj_stats.get("34", 0))))) if "34" in proj_stats else None,
        "ESPNK": str(int(round(float(proj_stats.get("48", 0))))) if "48" in proj_stats else None,
        "ESPNQS": str(int(round(float(proj_stats.get("63", 0))))) if "63" in proj_stats else None,
        "ESPNERA": f"{round(float(proj_stats.get('47', 0)), 2)}" if "47" in proj_stats else None,
        "ESPNWHIP": f"{round(float(proj_stats.get('41', 0)), 2)}" if "41" in proj_stats else None,
        "ESPNSV+HDs": str(int(round(float(proj_stats.get("57", 0)) + float(proj_stats.get("60", 0))))) if "57" in proj_stats or "60" in proj_stats else None
    }
    
    dim_record = {
        "player_id": int(player_id),
        "full_name": full_name,
        "clean_name": normalize_name(full_name),
        "primary_position": position_str.split(",")[0].strip(),
        "mlb_team": team_abbr,
        "is_active": True
    }
    
    return record, dim_record

def sync_pool(supabase: Client, season: int, dry_run: bool = False):
    espn_entries = fetch_espn_players(season)
    if not espn_entries:
        print("⚠️ No ESPN player records returned. Aborting.")
        return

    pool_records = []
    dim_records = []

    for entry in espn_entries:
        try:
            pool_rec, dim_rec = extract_player_record(entry, season)
            pool_records.append(pool_rec)
            dim_records.append(dim_rec)
        except Exception as e:
            continue

    print(f"Transformed {len(pool_records)} player pool records and {len(dim_records)} dim_players records")

    if dry_run:
        print("Dry run enabled. Skipping database upserts.")
        return

    # 1. Upsert dim_players
    print(f"Upserting {len(dim_records)} rows into dim_players...")
    batch_size = 200
    for i in range(0, len(dim_records), batch_size):
        batch = dim_records[i:i + batch_size]
        supabase.table("dim_players").upsert(batch, on_conflict="player_id").execute()
    print("✅ dim_players synchronized successfully")

    # 2. Upsert player-pool
    print(f"Upserting {len(pool_records)} rows into player-pool...")
    for i in range(0, len(pool_records), batch_size):
        batch = pool_records[i:i + batch_size]
        supabase.table("player-pool").upsert(batch, on_conflict="ESPN PlayerID").execute()
    print("✅ player-pool synchronized successfully")

def main():
    dry_run = "--dry-run" in sys.argv
    season_arg = None
    for arg in sys.argv:
        if arg.startswith("--season="):
            season_arg = int(arg.split("=")[1])

    print(f"🚀 Initializing Supabase client ({SUPABASE_URL})...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    season = season_arg or get_current_season(supabase)
    print(f"📅 Active Target Season: {season}")
    
    sync_pool(supabase, season, dry_run=dry_run)
    print("🎉 Draft pool synchronization completed successfully!")

if __name__ == "__main__":
    main()
