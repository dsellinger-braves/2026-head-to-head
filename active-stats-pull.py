import os
import json
import requests
import pandas as pd
from io import StringIO
from supabase import create_client, Client
from google.cloud import storage
from datetime import datetime

# --- CONFIGURATION ---
LEAGUE_ID = 130215
YEAR = 2026

# Load secrets from Environment Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") 

# --- STAT MAPPING ---
STAT_MAPPING = {
    '0': 'AB',
    '1': 'H',
    '2': 'AVG',
    '3': '2B',
    '4': '3B',
    '5': 'HR',
    '10': 'BB',
    '12': 'HBP',
    '16': 'PA',
    '17': 'OBP',
    '20': 'R',
    '21': 'RBI',
    '23': 'SB',
    '57': 'SV',
    '60': 'HD',
    '34': 'IP',
    '45': 'ER',
    '37': 'H_Allowed',
    '39': 'BB_Allowed',
    '63': 'QS',
    '48': 'K'
}

# Explicitly isolate batting vs pitching numeric IDs from ESPN
HITTING_STAT_IDS  = {'0', '1', '2', '3', '4', '5', '10', '12', '16', '17', '20', '21', '23'}
PITCHING_STAT_IDS = {'34', '45', '37', '39', '63', '48', '57', '60'}

def get_espn_data(league_id, team_ids, scoring_period_ids):
    all_data = []
    print(f"--- Starting Scrape for League {league_id} ---")

    for scoring_period_id in scoring_period_ids:
        print(f"Processing Scoring Period: {scoring_period_id}")
        
        for team_id in team_ids:
            url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{YEAR}/segments/0/leagues/{league_id}?forTeamId={team_id}&scoringPeriodId={scoring_period_id}&view=mRoster"

            try:
                r = requests.get(url)
                r.raise_for_status()
                data = r.json()

                if 'teams' not in data: continue
                
                team_data = data['teams'][0]
                roster_entries = team_data.get('roster', {}).get('entries', [])

                for entry in roster_entries:
                    lineup_slot_id = entry.get('lineupSlotId', -1)
                    player_data = entry.get('playerPoolEntry', {}).get('player', {})
                    player_id = player_data.get('id')
                    full_name = player_data.get('fullName', 'Unknown')
                    
                    stats_list = player_data.get('stats', [])
                    raw_stats = {}
                    
                    for stat_obj in stats_list:
                        # Look for actual daily stats for the targeted period
                        if stat_obj.get('scoringPeriodId') == scoring_period_id and stat_obj.get('statSplitTypeId') == 5:
                             game_stats = stat_obj.get('stats', {})
                             
                             for stat_id, value in game_stats.items():
                                 # --- ENFORCE SLOT RESTRICTIONS ---
                                 # 1. If in a hitting slot (0-12), discard pitching stats
                                 if lineup_slot_id in set(range(13)) and str(stat_id) in PITCHING_STAT_IDS:
                                     continue
                                 # 2. If in a pitching slot (13-15), discard batting stats
                                 if lineup_slot_id in {13, 14, 15} and str(stat_id) in HITTING_STAT_IDS:
                                     continue
                                 
                                 raw_stats[stat_id] = raw_stats.get(stat_id, 0) + value

                    # --- APPLY MAPPING HERE ---
                    mapped_stats = {}
                    for stat_id, value in raw_stats.items():
                        key_name = STAT_MAPPING.get(str(stat_id), str(stat_id))
                        mapped_stats[key_name] = value

                    record = {
                        "team_id": team_id,
                        "scoring_period_id": scoring_period_id,
                        "player_id": player_id,
                        "full_name": full_name,
                        "lineup_slot_id": lineup_slot_id,
                        "stats": mapped_stats,
                        "updated_at": datetime.now().isoformat()
                    }
                    all_data.append(record)

            except Exception as e:
                print(f"Error fetching Team {team_id} Period {scoring_period_id}: {e}")

    return all_data

def upload_to_supabase(records, active_periods: list):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Skipping Supabase: Credentials not found.")
        return

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print(f"Clearing existing database rows for periods: {list(active_periods)}...")
    try:
        supabase.table('player_daily_stats').delete().in_('scoring_period_id', list(active_periods)).execute()
    except Exception as e:
        print(f"Error clearing old data for active periods: {e}")
        print("Aborting insert to prevent duplicate key constraint failures.")
        return

    print(f"Inserting {len(records)} fresh records to Supabase...")
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table('player_daily_stats').insert(batch).execute()
        except Exception as e:
            print(f"Supabase Error on insert batch starting at index {i}: {e}")
            
    print("Supabase database insert complete.")

def upload_to_gcs(records, filename):
    if not GCS_BUCKET_NAME:
        print("Skipping GCS: Bucket name not found.")
        return

    print(f"Uploading {filename} to GCS...")
    
    df = pd.json_normalize(records) 
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    try:
        if GCS_CREDENTIALS_JSON:
            with open("gcs_key.json", "w") as f:
                f.write(GCS_CREDENTIALS_JSON)
            storage_client = storage.Client.from_service_account_json("gcs_key.json")
        else:
            storage_client = storage.Client()

        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(f"daily_stats/{filename}")
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print("GCS upload complete.")
        
    except Exception as e:
        print(f"GCS Upload Error: {e}")

if __name__ == "__main__":
    from datetime import date
    SEASON_START = date(2026, 3, 25)
    days_since_start = (date.today() - SEASON_START).days
    current_period = max(1, days_since_start)
    
    start_period = max(1, current_period - 2)
    end_period = current_period + 2
    PERIODS = range(start_period, end_period + 1)
    
    print(f"Calculated current season day as Period {current_period}")
    print(f"Targeting 5-day window: Periods {list(PERIODS)}")
    
    TEAMS = [1, 2, 3, 5, 6, 8, 12, 13, 14]
    
    data = get_espn_data(LEAGUE_ID, TEAMS, PERIODS)

    if data:
        upload_to_supabase(data, PERIODS)
        filename = f"stats_period_{PERIODS[0]}_to_{PERIODS[-1]}.csv"
        upload_to_gcs(data, filename)
    else:
        print("No data found to process.")
