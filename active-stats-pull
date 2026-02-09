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
YEAR = 2025

# Load secrets from Environment Variables (Best for GitHub Actions/Cloud Run)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY-If51A")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") 

# --- STAT MAPPING ---
# REPLACE THIS with your actual mapping. 
# Example format: { 'ESPN_ID': 'READABLE_NAME' }
STAT_MAPPING = {
    '0': 'AB',
    '1': 'H',
    '2': 'AVG',
    '3': '2B',
    '4': '3B',
    '5': 'HR',
    '10': 'BB',
    '16': 'PA',
    '20': 'R',
    '21': 'RBI',
    '23': 'SB',
    '47': 'WIN',
    '48': 'LOSS',
    '53': 'SV',
    '61': 'IP',
    '62': 'H_ALLOWED',
    '63': 'ER',
    '64': 'HR_ALLOWED',
    '65': 'BB_ALLOWED',
    '67': 'K',
    '79': 'QS'
    # Add the rest of your mapping here...
}

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
                    player_data = entry.get('playerPoolEntry', {}).get('player', {})
                    player_id = player_data.get('id')
                    full_name = player_data.get('fullName', 'Unknown')
                    
                    # Extract stats for this specific scoring period
                    raw_stats = {}
                    stats_list = player_data.get('stats', [])
                    for stat_obj in stats_list:
                        # filtered by Scoring Period and StatSplitType (3=Season, 5=Daily usually, check your needs)
                        if stat_obj.get('scoringPeriodId') == scoring_period_id:
                             # We prioritize actual stats (0) or projected (1) if needed, 
                             # but usually for active scoring we want type 5 or 0 depending on view.
                             # Based on your previous script, we look for the one matching the period.
                             raw_stats = stat_obj.get('stats', {})
                             break

                    # --- APPLY MAPPING HERE ---
                    mapped_stats = {}
                    for stat_id, value in raw_stats.items():
                        # If we have a name for it in the mapping, use it. Otherwise use the ID.
                        key_name = STAT_MAPPING.get(str(stat_id), str(stat_id))
                        mapped_stats[key_name] = value

                    record = {
                        "team_id": team_id,
                        "scoring_period_id": scoring_period_id,
                        "player_id": player_id,
                        "full_name": full_name,
                        "lineup_slot_id": entry.get('lineupSlotId'),
                        "stats": mapped_stats, # Saves: {"HR": 1, "RBI": 2} instead of {"5": 1, "21": 2}
                        "updated_at": datetime.now().isoformat()
                    }
                    all_data.append(record)

            except Exception as e:
                print(f"Error fetching Team {team_id} Period {scoring_period_id}: {e}")

    return all_data

def upload_to_supabase(records):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Skipping Supabase: Credentials not found.")
        return

    print(f"Upserting {len(records)} records to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Batch upsert to prevent timeouts
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            data, count = supabase.table('player_daily_stats').upsert(
                batch, on_conflict='player_id, scoring_period_id'
            ).execute()
        except Exception as e:
            print(f"Supabase Error on batch {i}: {e}")
            
    print("Supabase upload complete.")

def upload_to_gcs(records, filename):
    if not GCS_BUCKET_NAME:
        print("Skipping GCS: Bucket name not found.")
        return

    print(f"Uploading {filename} to GCS...")
    
    # Convert list of dicts to CSV string
    df = pd.json_normalize(records) # Flattens the nested 'stats' JSON for CSV columns
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Authenticate and Upload
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
    # Define the teams in your league
    TEAMS = [1, 2, 3, 5, 6, 8, 9, 11, 12, 13]
    
    # Define the period to scrape. 
    # For automation, you might want: current_period = get_current_espn_period()
    # For now, we use a range or a specific set.
    PERIODS = range(195, 196) 

    data = get_espn_data(LEAGUE_ID, TEAMS, PERIODS)

    if data:
        # 1. Upload to Supabase (Powering the Website)
        upload_to_supabase(data)

        # 2. Upload to GCS (Data Lake / Backup)
        filename = f"stats_period_{PERIODS[0]}_to_{PERIODS[-1]}.csv"
        upload_to_gcs(data, filename)
    else:
        print("No data found to process.")
