import os
from supabase import create_client, Client

OLD_URL = os.environ.get("OLD_SUPABASE_URL")
OLD_KEY = os.environ.get("OLD_SUPABASE_KEY")
NEW_URL = os.environ.get("NEW_SUPABASE_URL")
NEW_KEY = os.environ.get("NEW_SUPABASE_KEY")

LEAGUE_ID = 130215

if not all([OLD_URL, OLD_KEY, NEW_URL, NEW_KEY]):
    raise ValueError("Missing Supabase credentials in environment variables.")

old_client: Client = create_client(OLD_URL, OLD_KEY)
new_client: Client = create_client(NEW_URL, NEW_KEY)

def migrate_historical_data():
    print("--- Migrating table: historical_data ---")
    batch_size = 1000
    offset = 0
    total_migrated = 0
    
    while True:
        print(f"Fetching rows {offset} to {offset + batch_size - 1} from OLD DB...")
        try:
            res = old_client.table("historical_data").select("*").range(offset, offset + batch_size - 1).execute()
        except Exception as e:
            print(f"Error fetching from OLD DB: {e}")
            break
            
        rows = res.data
        if not rows:
            break
            
        print(f"  Fetched {len(rows)} rows.")
        
        # Transform data
        clean_rows = []
        for row in rows:
            clean_row = {"league_id": LEAGUE_ID}
            for k, v in row.items():
                if k not in ("_db_id", "fetched_at"):
                    clean_row[k] = v
            clean_rows.append(clean_row)
        
        # Upsert in batches
        print(f"Upserting {len(clean_rows)} rows to NEW DB...")
        try:
            # We don't specify on_conflict because ESPN historical data doesn't have a reliable PK, 
            # but we defined a composite UNIQUE constraint in the schema.
            # Supabase handles duplicate unique constraints on insert if we just use insert() or upsert() without on_conflict 
            # as long as we ignore errors or if the schema has unique constraint.
            new_client.table("historical_data").upsert(clean_rows, on_conflict="league_id,season_year,scoring_period_id,team_id,id").execute()
        except Exception as e:
            print(f"Error upserting batch starting at {offset}: {e}")
        
        total_migrated += len(rows)
        if len(rows) < batch_size:
            break
        offset += batch_size

    print(f"Finished migrating historical_data. Total rows migrated: {total_migrated}")

if __name__ == "__main__":
    migrate_historical_data()
