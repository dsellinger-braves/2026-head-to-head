"""
transaction-scraper.py
Scrapes add/drop/trade transactions from the ESPN Fantasy API
and stores them in the Supabase `transactions` table.

Run on a schedule (e.g. every 2 hours) via GitHub Actions.

This version fetches all transactions from ESPN and can overwrite the
Supabase `transactions` table on each run so the DB matches the
current scrape (no stale rows remain). It also:
 - uses a PostgREST-friendly DELETE with a WHERE clause
 - deduplicates incoming rows by espn_transaction_id
 - retries delete up to 3 times
 - falls back to upsert for failed insert batches (to avoid unique constraint errors)
 - controlled by TRANSACTIONS_OVERWRITE env var (true/false)
"""

import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client
from typing import List, Dict

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

LEAGUE_ID  = 130215
YEAR       = 2026

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Optional: ESPN private league cookies (needed if the league is private)
ESPN_S2 = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")

# Toggle overwrite behavior via env var. Defaults to true.
TRANSACTIONS_OVERWRITE = os.environ.get("TRANSACTIONS_OVERWRITE", "true").lower() in (
    "1",
    "true",
    "yes",
)

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

# ESPN transaction type IDs in the activity log
MSG_TYPE_NAMES = {
    178: "TRADE_ACCEPT",
    180: "TRADE_PROPOSAL",
    181: "TRADE_DECLINE",
    244: "ADD",
    245: "DROP",
    239: "WAIVER_ADD",
    243: "WAIVER_DROP",
}

# ---------------------------------------------------------------------------
# ESPN API
# ---------------------------------------------------------------------------

def fetch_transactions() -> List[Dict]:
    """
    Fetch all transactions for the year.
    Since ESPN caps mTransactions2 to the current scoring period by default,
    we dynamically fetch the current period via mStatus and loop through all 
    periods up to today.
    """
    base_url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{YEAR}/segments/0/leagues/{LEAGUE_ID}"
    )
    
    cookies = {}
    if ESPN_S2 and ESPN_SWID:
        cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID}
        print("Using ESPN auth cookies.")
    else:
        print("No ESPN cookies set — attempting unauthenticated request.")

    # 1. Fetch current scoring period so we know how many days to loop through
    try:
        status_resp = requests.get(f"{base_url}?view=mStatus", cookies=cookies, timeout=10)
        status_resp.raise_for_status()
        current_period = status_resp.json().get("status", {}).get("latestScoringPeriod", 185)
        print(f"Current scoring period is {current_period}. Fetching all periods...")
    except Exception as e:
        print(f"Failed to fetch current scoring period: {e}. Defaulting to 185 (full season).")
        current_period = 185
        
    if current_period < 1:
        current_period = 1

    all_transactions = []

    # 2. Loop through all scoring periods using a Session for fast connection pooling
    with requests.Session() as session:
        session.cookies.update(cookies)
        
        # Start at 0 to catch preseason draft/trades, go up to current_period
        for period in range(0, current_period + 1):
            url = f"{base_url}?view=mTransactions2&scoringPeriodId={period}"
            resp = session.get(url, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                txns = data.get("transactions", [])
                if txns:
                    all_transactions.extend(txns)
            else:
                print(f"Warning: Failed to fetch period {period} (Status {resp.status_code})")

    return all_transactions


def fetch_player_name(player_id: int) -> str:
    """Look up a player name from ESPN. Fallback to player_id if not found."""
    url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{YEAR}/players/{player_id}?view=players_wl"
    )
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("fullName", f"Player {player_id}")
    except Exception:
        pass
    return f"Player {player_id}"


# ---------------------------------------------------------------------------
# PARSING
# ---------------------------------------------------------------------------

def parse_transactions(raw: List[Dict]) -> List[Dict]:
    """
    Parse ESPN transaction objects into flat rows for Supabase.
    Includes a debug catcher for trades to see how ESPN formats the players.
    """
    rows: List[Dict] = []
    for txn in raw:
        txn_id   = txn.get("id", "")
        status   = txn.get("status", "")
        
        if status != "EXECUTED":
            continue  # skip pending/declined transactions

        raw_type = txn.get("type", "UNKNOWN")

        # Skip daily lineup changes
        if raw_type in ("ROSTER", "FUTURE_ROSTER"):
            continue

        # --- DEBUG CATCHER FOR TRADES ---
        # If this is a trade, let's print the raw JSON so we can see its structure
        if "TRADE" in raw_type:
            print(f"\n--- FOUND A TRADE (ID: {txn_id}) ---")
            print(txn)
            print("------------------------------------\n")

        # ESPN stores dates in milliseconds
        executed_ms = txn.get("executedDate") or txn.get("proposedDate", 0)
        txn_date    = datetime.fromtimestamp(executed_ms / 1000, tz=timezone.utc)
        period_id   = txn.get("scoringPeriodId", 0)

        for item in txn.get("items", []):
            item_type    = item.get("type", raw_type)
            to_team_id   = item.get("toTeamId", -1)
            from_team_id = item.get("fromTeamId", -1)
            player_id    = item.get("playerId")

            if not player_id:
                continue

            # Classify the transaction
            if item_type in ("ADD", "WAIVER") or raw_type in ("ADD", "WAIVER"):
                txn_type = "WAIVER_ADD" if txn.get("executionType") == "WAIVER" else "ADD"
            elif item_type == "DROP" or raw_type == "DROP":
                txn_type = "DROP"
            elif item_type in ("TRADED",) or raw_type in ("TRADE", "TRADE_ACCEPT"):
                txn_type = "TRADE"
            else:
                txn_type = item_type or raw_type

            rows.append({
                "espn_transaction_id": f"{txn_id}_{player_id}_{to_team_id}",
                "transaction_type":    txn_type,
                "transaction_date":    txn_date.isoformat(),
                "scoring_period_id":   period_id,
                "to_team_id":          to_team_id,
                "from_team_id":        from_team_id, 
                "player_id":           player_id,
                "player_name":         item.get("playerNote") or f"Player {player_id}",
                "raw_type":            raw_type,
            })

    return rows


# ---------------------------------------------------------------------------
# PLAYER NAME ENRICHMENT
# ---------------------------------------------------------------------------

def enrich_player_names(rows: List[Dict]) -> List[Dict]:
    """
    Fill in player_name for rows that only have 'Player {id}'.
    Batches lookups to avoid hammering the ESPN API.
    """
    needs_lookup = {r["player_id"] for r in rows if r["player_name"].startswith("Player ")}
    name_map: Dict[int, str] = {}

    for pid in needs_lookup:
        name_map[pid] = fetch_player_name(pid)

    for row in rows:
        if row["player_name"].startswith("Player "):
            row["player_name"] = name_map.get(row["player_id"], row["player_name"])

    return rows


# ---------------------------------------------------------------------------
# SUPABASE UPSERT / OVERWRITE
# ---------------------------------------------------------------------------

def upsert_transactions(rows: List[Dict], overwrite: bool = TRANSACTIONS_OVERWRITE):
    """
    Upload transactions to Supabase.

    If overwrite is True (default controlled by TRANSACTIONS_OVERWRITE env var),
    delete all rows in the `transactions` table first using a WHERE clause
    acceptable to PostgREST, then insert the scraped rows in batches.

    The function deduplicates incoming rows by espn_transaction_id and will
    fall back to upsert for any batch that fails to insert due to unique
    constraint violations.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase credentials not set. Skipping upload.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"Uploading {len(rows)} transaction rows (overwrite={overwrite})...")

    # Deduplicate incoming rows by espn_transaction_id
    unique = {}
    for r in rows:
        key = r.get("espn_transaction_id")
        if key:
            unique[key] = r
    rows = list(unique.values())
    print(f"  After dedupe: {len(rows)} rows")

    # If requested, clear the entire table first so this run fully replaces it.
    if overwrite:
        delete_success = False
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempt {attempt}: Clearing existing transactions table (DELETE WHERE espn_transaction_id != '')...")
                # PostgREST requires a WHERE clause; remove all rows where espn_transaction_id is not empty.
                supabase.table("transactions").delete().neq("espn_transaction_id", "").execute()
                delete_success = True
                print("Existing transactions deleted.")
                break
            except Exception as e:
                print(f"Error clearing transactions table on attempt {attempt}: {e}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)

        if not delete_success:
            print("Warning: failed to clear transactions table after retries. Proceeding with inserts (some duplicates may remain).")

        # Insert fresh rows in batches. If an insert batch fails with a unique constraint,
        # fallback to upsert for that batch to avoid failing the whole run.
        batch_size = 200
        inserted = 0
        upserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                supabase.table("transactions").insert(batch).execute()
                inserted += len(batch)
            except Exception as e:
                print(f"Insert error for batch starting at {i}: {e}")
                # Fallback to upsert for this batch
                try:
                    supabase.table("transactions").upsert(batch, on_conflict="espn_transaction_id").execute()
                    upserted += len(batch)
                    print(f"Fallback upsert succeeded for batch starting at {i}.")
                except Exception as e2:
                    print(f"Fallback upsert failed for batch starting at {i}: {e2}")

        print(f"Transaction upload (overwrite) complete. Inserted: {inserted}, Upserted (fallback): {upserted}.")
    else:
        # Backwards-compatible behavior: upsert on espn_transaction_id
        batch_size = 200
        upserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                supabase.table("transactions").upsert(
                    batch, on_conflict="espn_transaction_id"
                ).execute()
                upserted += len(batch)
            except Exception as e:
                print(f"Error on upsert batch {i}: {e}")

        print(f"Transaction upload (upsert) complete. Upserted: {upserted}.")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Fetching transactions for league {LEAGUE_ID} ({YEAR})...")
    raw  = fetch_transactions()
    print(f"  Raw transactions returned: {len(raw)}")

    rows = parse_transactions(raw)
    print(f"  Parsed rows (executed moves): {len(rows)}")

    if rows:
        rows = enrich_player_names(rows)
        # Use environment toggle to control overwrite vs upsert
        upsert_transactions(rows, overwrite=TRANSACTIONS_OVERWRITE)
    else:
        print("  No executed transactions found.")
