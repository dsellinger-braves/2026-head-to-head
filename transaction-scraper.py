"""
transaction-scraper.py
Scrapes add/drop/trade transactions from the ESPN Fantasy API
and stores them in the Supabase `transactions` table.

Run on a schedule (e.g. every 2 hours) via GitHub Actions.

This version fetches all regular transactions via mTransactions2,
fetches all executed trades via the Activity Feed, and merges them.
"""

import os
import time
import json
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

ESPN_S2 = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")

TRANSACTIONS_OVERWRITE = os.environ.get("TRANSACTIONS_OVERWRITE", "true").lower() in ("1", "true", "yes")

# ---------------------------------------------------------------------------
# ESPN API: REGULAR ADDS & DROPS
# ---------------------------------------------------------------------------

def fetch_transactions() -> List[Dict]:
    """Fetch regular transactions (Adds/Drops) dynamically for the year."""
    base_url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{YEAR}/segments/0/leagues/{LEAGUE_ID}"
    )
    
    cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 else {}

    try:
        status_resp = requests.get(f"{base_url}?view=mStatus", cookies=cookies, timeout=10)
        status_resp.raise_for_status()
        current_period = status_resp.json().get("status", {}).get("latestScoringPeriod", 185)
    except Exception:
        current_period = 185
        
    if current_period < 1:
        current_period = 1

    all_transactions = []
    with requests.Session() as session:
        session.cookies.update(cookies)
        for period in range(0, current_period + 1):
            url = f"{base_url}?view=mTransactions2&scoringPeriodId={period}"
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                txns = resp.json().get("transactions", [])
                if txns:
                    all_transactions.extend(txns)

    return all_transactions

def parse_transactions(raw: List[Dict]) -> List[Dict]:
    """Parse standard adds/drops. (Trades are handled separately)"""
    rows: List[Dict] = []
    for txn in raw:
        status = txn.get("status", "")
        if status != "EXECUTED":
            continue

        txn_id   = txn.get("id", "")
        raw_type = txn.get("type", "UNKNOWN")

        # Skip roster lineup changes completely here
        if raw_type in ("ROSTER", "FUTURE_ROSTER"):
            continue
            
        # Skip trade objects here (we will grab them from the activity feed instead)
        if "TRADE" in raw_type:
            continue

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

            if item_type in ("ADD", "WAIVER") or raw_type in ("ADD", "WAIVER"):
                txn_type = "WAIVER_ADD" if txn.get("executionType") == "WAIVER" else "ADD"
            elif item_type == "DROP" or raw_type == "DROP":
                txn_type = "DROP"
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
# ESPN API: ACTIVITY FEED TRADES
# ---------------------------------------------------------------------------

def fetch_activity_trades() -> List[Dict]:
    """Fetch executed trades by safely paging through the Recent Activity feed."""
    url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{YEAR}"
        f"/segments/0/leagues/{LEAGUE_ID}/communication/?view=kona_league_communication"
    )
    
    cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 else {}
    all_topics = []
    offset = 0
    limit = 50
    
    print("  Paging through activity feed for trades...")
    
    with requests.Session() as session:
        session.cookies.update(cookies)
        
        while True:
            filters = {
                "topics": {
                    "filterType": {"value": ["ACTIVITY_TRANSACTIONS"]},
                    "limit": limit,
                    "limitPerMessageSet": {"value": 50},
                    "offset": offset,
                    "filterCommunicationTopic": {"value": ["TRADE"]}
                }
            }
            headers = {"x-fantasy-filter": json.dumps(filters)}
            
            try:
                resp = session.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                
                topics = resp.json().get("topics", [])
                if not topics:
                    break  # No more topics found, exit loop
                    
                all_topics.extend(topics)
                
                # If we got fewer topics than our limit, we are at the end of the history
                if len(topics) < limit:
                    break
                    
                offset += limit
                
            except Exception as e:
                print(f"Failed to fetch activity trades at offset {offset}: {e}")
                break

    return all_topics
    
def parse_activity_trades(topics: List[Dict]) -> List[Dict]:
    """Parse only the fully executed system trades from the activity feed."""
    rows = []
    for topic in topics:
        # We only want trades processed by the system (not the user acceptances)
        if topic.get("author") != "TradeTaskProcessor":
            continue
            
        topic_id = topic.get("id", "")
        
        for msg in topic.get("messages", []):
            player_id = msg.get("targetId")
            from_team_id = msg.get("from")
            to_team_id = msg.get("to")
            
            if not player_id or not from_team_id or not to_team_id:
                continue
                
            date_ms = msg.get("date", 0)
            txn_date = datetime.fromtimestamp(date_ms / 1000, tz=timezone.utc)
            
            rows.append({
                "espn_transaction_id": f"{topic_id}_{player_id}_{to_team_id}",
                "transaction_type":    "TRADE",
                "transaction_date":    txn_date.isoformat(),
                "scoring_period_id":   0, # Activity feed doesn't attach scoring periods
                "to_team_id":          to_team_id,
                "from_team_id":        from_team_id,
                "player_id":           player_id,
                "player_name":         f"Player {player_id}",
                "raw_type":            "ACTIVITY_TRADE"
            })
    return rows

# ---------------------------------------------------------------------------
# PLAYER NAME ENRICHMENT
# ---------------------------------------------------------------------------

def fetch_player_name(player_id: int) -> str:
    url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{YEAR}/players/{player_id}?view=players_wl"
    )
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("fullName", f"Player {player_id}")
    except Exception:
        pass
    return f"Player {player_id}"

def enrich_player_names(rows: List[Dict]) -> List[Dict]:
    needs_lookup = {r["player_id"] for r in rows if r["player_name"].startswith("Player ")}
    name_map: Dict[int, str] = {}

    for pid in needs_lookup:
        name_map[pid] = fetch_player_name(pid)

    for row in rows:
        if row["player_name"].startswith("Player "):
            row["player_name"] = name_map.get(row["player_id"], row["player_name"])

    return rows

# ---------------------------------------------------------------------------
# SUPABASE UPSERT
# ---------------------------------------------------------------------------

def upsert_transactions(rows: List[Dict], overwrite: bool = TRANSACTIONS_OVERWRITE):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase credentials not set. Skipping upload.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"Uploading {len(rows)} transaction rows (overwrite={overwrite})...")

    unique = {r.get("espn_transaction_id"): r for r in rows if r.get("espn_transaction_id")}
    rows = list(unique.values())
    print(f"  After dedupe: {len(rows)} rows")

    if overwrite:
        delete_success = False
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempt {attempt}: Clearing existing transactions table...")
                supabase.table("transactions").delete().neq("espn_transaction_id", "").execute()
                delete_success = True
                print("Existing transactions deleted.")
                break
            except Exception as e:
                print(f"Error clearing table on attempt {attempt}: {e}")
                if attempt < max_retries: time.sleep(1 * attempt)

        batch_size = 200
        inserted, upserted = 0, 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                supabase.table("transactions").insert(batch).execute()
                inserted += len(batch)
            except Exception as e:
                try:
                    supabase.table("transactions").upsert(batch, on_conflict="espn_transaction_id").execute()
                    upserted += len(batch)
                except Exception as e2:
                    print(f"Fallback upsert failed: {e2}")

        print(f"Upload complete. Inserted: {inserted}, Upserted (fallback): {upserted}.")
    else:
        batch_size = 200
        upserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                supabase.table("transactions").upsert(batch, on_conflict="espn_transaction_id").execute()
                upserted += len(batch)
            except Exception as e:
                print(f"Error on upsert batch {i}: {e}")

        print(f"Upload complete. Upserted: {upserted}.")

# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Fetching standard transactions for league {LEAGUE_ID}...")
    raw_txns = fetch_transactions()
    base_rows = parse_transactions(raw_txns)
    print(f"  Parsed standard rows: {len(base_rows)}")

    print(f"Fetching trade transactions from activity feed...")
    raw_trades = fetch_activity_trades()
    trade_rows = parse_activity_trades(raw_trades)
    print(f"  Parsed trade rows: {len(trade_rows)}")

    # Merge everything
    all_rows = base_rows + trade_rows

    if all_rows:
        all_rows = enrich_player_names(all_rows)
        upsert_transactions(all_rows, overwrite=TRANSACTIONS_OVERWRITE)
    else:
        print("  No transactions found.")
