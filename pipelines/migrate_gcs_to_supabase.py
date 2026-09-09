#!/usr/bin/env python3
"""
pipelines/migrate_gcs_to_supabase.py

Migrates historical draft picks (draft-history.json) and season finishes
(historical-finish.json) from Google Cloud Storage into structured Supabase
warehouse tables: dim_players, draft_picks, and historical_finishes.
"""

import os
import sys
import json
import urllib.request
import unicodedata
import re
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wczdkcdqgtzlsbssogoz.supabase.co")
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

GCS_DRAFT_HISTORY_URL = "https://storage.googleapis.com/fantasy-draft-2026/draft-history.json"
GCS_HISTORICAL_FINISH_URL = "https://storage.googleapis.com/fantasy-draft-2026/historical-finish.json"

def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize('NFKD', name)
    clean = "".join([c for c in nfkd if not unicodedata.combining(c)])
    clean = re.sub(r"[.,'-]", "", clean)
    clean = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s+", " ", clean).strip().lower()
    return clean

def fetch_json(url: str):
    print(f"🌐 Fetching {url}...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def migrate_draft_history(supabase: Client, dry_run: bool = False):
    print("\n--- Migrating Draft History ---")
    data = fetch_json(GCS_DRAFT_HISTORY_URL)
    print(f"Loaded {len(data)} raw draft history records")

    players_map = {}
    draft_picks = []

    for r in data:
        raw_year = r.get("Year") or r.get("year")
        raw_overall = r.get("Pick_Overall") or r.get("overall_pick") or r.get("Overall Pick")
        raw_player_name = r.get("Player_Name") or r.get("Player") or r.get("Name") or "Unknown"
        raw_pid = r.get("player_id") or r.get("playerID") or r.get("ESPN PlayerID")
        
        try:
            year = int(raw_year)
            overall = int(raw_overall)
        except (ValueError, TypeError):
            continue

        player_id = int(raw_pid) if raw_pid and str(raw_pid).isdigit() else None
        round_num = int(r.get("Round")) if str(r.get("Round", "")).isdigit() else None
        
        owner = r.get("Team_ID") or r.get("Owner") or "Unknown"
        is_keeper = str(r.get("Keeper", "")).lower() == "true"

        if player_id and player_id not in players_map:
            players_map[player_id] = {
                "player_id": player_id,
                "full_name": raw_player_name,
                "clean_name": normalize_name(raw_player_name),
                "is_active": True
            }

        draft_picks.append({
            "season_year": year,
            "round": round_num,
            "overall_pick": overall,
            "team_owner": owner,
            "player_id": player_id,
            "player_name": raw_player_name,
            "is_keeper": is_keeper
        })

    print(f"Prepared {len(players_map)} unique players and {len(draft_picks)} draft picks")

    if dry_run:
        print("Dry run enabled. Skipping database writes.")
        return

    # 1. Upsert dim_players
    player_records = list(players_map.values())
    batch_size = 200
    print(f"Upserting {len(player_records)} players into dim_players...")
    for i in range(0, len(player_records), batch_size):
        batch = player_records[i:i + batch_size]
        supabase.table("dim_players").upsert(batch, on_conflict="player_id").execute()
    print("✅ dim_players populated successfully")

    # 2. Upsert draft_picks
    print(f"Upserting {len(draft_picks)} draft picks into draft_picks...")
    for i in range(0, len(draft_picks), batch_size):
        batch = draft_picks[i:i + batch_size]
        supabase.table("draft_picks").upsert(batch, on_conflict="season_year,overall_pick").execute()
    print("✅ draft_picks populated successfully")

def migrate_historical_finishes(supabase: Client, dry_run: bool = False):
    print("\n--- Migrating Historical Finishes ---")
    data = fetch_json(GCS_HISTORICAL_FINISH_URL)
    print(f"Loaded {len(data)} raw historical finish records")

    finishes = []
    for r in data:
        raw_year = r.get("Year") or r.get("year")
        owner = r.get("Owner") or r.get("team_owner") or "Unknown"
        if not raw_year or not owner:
            continue

        try:
            year = int(raw_year)
        except ValueError:
            continue

        final_place = int(r.get("Final Rank")) if str(r.get("Final Rank", "")).isdigit() else None
        points = float(r.get("Points")) if r.get("Points") not in (None, "") else None
        active_owner = str(r.get("Active Owner?", "Y")).upper() == "Y"

        category_ranks = {
            "R": r.get("R"),
            "HR": r.get("HR"),
            "RBI": r.get("RBI"),
            "SB": r.get("SB"),
            "OBP": r.get("OBP"),
            "K": r.get("K"),
            "QS": r.get("QS"),
            "SVHLD": r.get("SVHLD"),
            "ERA": r.get("ERA"),
            "WHIP": r.get("WHIP"),
            "hitting_points": r.get("Hitting Points"),
            "pitching_points": r.get("Pitching Points"),
            "team_name": r.get("Team Name")
        }

        finishes.append({
            "season_year": year,
            "team_owner": owner,
            "final_place": final_place,
            "total_roto_points": points,
            "is_active_owner": active_owner,
            "category_ranks": category_ranks,
            "raw_data": r
        })

    print(f"Prepared {len(finishes)} historical finishes")

    if dry_run:
        print("Dry run enabled. Skipping database writes.")
        return

    batch_size = 100
    print(f"Upserting {len(finishes)} historical finishes into historical_finishes...")
    for i in range(0, len(finishes), batch_size):
        batch = finishes[i:i + batch_size]
        supabase.table("historical_finishes").upsert(batch, on_conflict="season_year,team_owner").execute()
    print("✅ historical_finishes populated successfully")

def main():
    dry_run = "--dry-run" in sys.argv
    print(f"🚀 Initializing Supabase client ({SUPABASE_URL})...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    migrate_draft_history(supabase, dry_run=dry_run)
    migrate_historical_finishes(supabase, dry_run=dry_run)
    print("\n🎉 Migration completed successfully!")

if __name__ == "__main__":
    main()
