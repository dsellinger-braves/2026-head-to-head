import os
from supabase import create_client, Client

# --- CONFIGURATION ---
LEAGUE_ID = 130215

# Old environment credentials
OLD_SUPABASE_URL = os.environ.get("OLD_SUPABASE_URL")
OLD_SUPABASE_KEY = os.environ.get("OLD_SUPABASE_KEY")

# New environment credentials
NEW_SUPABASE_URL = os.environ.get("NEW_SUPABASE_URL")
NEW_SUPABASE_KEY = os.environ.get("NEW_SUPABASE_KEY")

def migrate_table(old_client: Client, new_client: Client, table_name: str, batch_size: int = 500, pk_column: str = None):
    print(f"\n--- Migrating table: {table_name} ---")
    
    # Supabase limits fetch to 1000 rows by default. We need to paginate.
    limit = 1000
    offset = 0
    total_migrated = 0
    
    while True:
        print(f"Fetching rows {offset} to {offset + limit - 1} from OLD DB...")
        response = old_client.table(table_name).select("*").range(offset, offset + limit - 1).execute()
        
        rows = response.data
        if not rows:
            print("No more rows to fetch.")
            break
            
        print(f"  Fetched {len(rows)} rows.")
        
        # Define allowed columns for each table
        allowed_transactions = {
            "espn_transaction_id", "league_id", "transaction_type", "transaction_date",
            "scoring_period_id", "to_team_id", "from_team_id", "player_id", "player_name", "raw_type"
        }
        allowed_stats = {
            "league_id", "team_id", "scoring_period_id", "player_id", 
            "full_name", "lineup_slot_id", "stats", "updated_at"
        }

        # Transform data to include league_id and filter columns
        clean_rows = []
        for row in rows:
            row["league_id"] = LEAGUE_ID
            clean_row = {}
            if table_name == "transactions":
                allowed = allowed_transactions
            else:
                allowed = allowed_stats
            
            for k, v in row.items():
                if k in allowed:
                    clean_row[k] = v
            clean_rows.append(clean_row)
        
        # Upsert in batches to the new DB
        print(f"Upserting {len(clean_rows)} rows to NEW DB...")
        for i in range(0, len(clean_rows), batch_size):
            batch = clean_rows[i:i + batch_size]
            try:
                if pk_column and table_name == "transactions":
                    new_client.table(table_name).upsert(batch, on_conflict=pk_column).execute()
                elif table_name == "player_daily_stats":
                    # For player_daily_stats, we added a unique constraint on (league_id, scoring_period_id, player_id)
                    new_client.table(table_name).upsert(batch, on_conflict="league_id,scoring_period_id,player_id").execute()
                else:
                    new_client.table(table_name).insert(batch).execute()
            except Exception as e:
                print(f"Error upserting batch starting at {i}: {e}")
                
        total_migrated += len(rows)
        
        if len(rows) < limit:
            break
            
        offset += limit

    print(f"Finished migrating {table_name}. Total rows migrated: {total_migrated}")

if __name__ == "__main__":
    if not OLD_SUPABASE_URL or not OLD_SUPABASE_KEY:
        print("Please set OLD_SUPABASE_URL and OLD_SUPABASE_KEY.")
        exit(1)
        
    if not NEW_SUPABASE_URL or not NEW_SUPABASE_KEY:
        print("Please set NEW_SUPABASE_URL and NEW_SUPABASE_KEY.")
        exit(1)

    print("Connecting to Supabase clients...")
    old_client: Client = create_client(OLD_SUPABASE_URL, OLD_SUPABASE_KEY)
    new_client: Client = create_client(NEW_SUPABASE_URL, NEW_SUPABASE_KEY)

    migrate_table(old_client, new_client, "transactions", pk_column="espn_transaction_id")
    migrate_table(old_client, new_client, "player_daily_stats")
    
    print("\nMigration complete!")
