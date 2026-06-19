"""
transaction-scraper.py
Scrapes add/drop/trade transactions from the ESPN Fantasy API
and stores them in the Supabase `transactions` table.

Run on a schedule (e.g. every 2 hours) via GitHub Actions.

This version fetches all transactions from ESPN and overwrites the
Supabase `transactions` table on each run so the DB matches the
current scrape (no stale rows remain).
"""

import os
import json
import requests
from datetime import datetime, timezone
from supabase import create_client

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

def fetch_transactions() -> list[dict]:
    """Fetch transactions from ESPN's mTransactions2 view.

    The mTransactions2 view returns all transactions for the league/season
    (not just today's). If you need a different view or pagination, update
    this function accordingly.
    """
    url = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{YEAR}/segments/0/leagues/{LEAGUE_ID}"
        f"?view=mTransactions2"
    )
    cookies = {}
    if ESPN_S2 and ESPN_SWID:
        cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID}
        print("Using ESPN auth cookies.")
    else:
        print("No ESPN cookies set — attempting unauthenticated request.")

    resp = requests.get(url, cookies=cookies, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("transactions", [])


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

def parse_transactions(raw: list[dict]) -> list[dict]:
    """
    Parse ESPN transaction objects into flat rows for Supabase.
    Each player movement in a transaction becomes its own row.
    Trades produce multiple rows (one per player moved).
    """
    rows = []
    for txn in raw:
        txn_id   = txn.get("id", "")
        status   = txn.get("status", "")
        if status != "EXECUTED":
            continue  # skip pending/declined transactions

        # ESPN stores dates in milliseconds
        executed_ms = txn.get("executedDate") or txn.get("proposedDate", 0)
        txn_date    = datetime.fromtimestamp(executed_ms / 1000, tz=timezone.utc)
        period_id   = txn.get("scoringPeriodId", 0)

        raw_type = txn.get("type", "UNKNOWN")

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
                "from_team_id":        from_team_id,  # -1 = free agent / waivers
                "player_id":           player_id,
                "player_name":         item.get("playerNote") or f"Player {player_id}",
                "raw_type":            raw_type,
            })

    return rows


# ---------------------------------------------------------------------------
# PLAYER NAME ENRICHMENT
# ---------------------------------------------------------------------------

def enrich_player_names(rows: list[dict]) -> list[dict]:
    """
    Fill in player_name for rows that only have 'Player {id}'.
    Batches lookups to avoid hammering the ESPN API.
    """
    needs_lookup = {r["player_id"] for r in rows if r["player_name"].startswith("Player ")}
    name_map = {}

    for pid in needs_lookup:
        name_map[pid] = fetch_player_name(pid)

    for row in rows:
        if row["player_name"].startswith("Player "):
            row["player_name"] = name_map.get(row["player_id"], row["player_name"])

    return rows


# ---------------------------------------------------------------------------
# SUPABASE UPSERT / OVERWRITE
# ---------------------------------------------------------------------------

def upsert_transactions(rows: list[dict], overwrite: bool = True):
    """
    Upload transactions to Supabase.

    If overwrite is True, delete all rows in the `transactions` table first,
    then insert the scraped rows in batches. This ensures the table is an
    exact mirror of the current scrape (no stale rows remain).

    If overwrite is False, fall back to upsert behavior on espn_transaction_id.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase credentials not set. Skipping upload.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"Uploading {len(rows)} transaction rows (overwrite={overwrite})...")

    # If requested, clear the entire table first so this run fully replaces it.
    if overwrite:
        try:
            print("Clearing existing transactions table...")
            # Delete all rows. The Python client allows delete without a filter to remove all rows.
            supabase.table("transactions").delete().execute()
            print("Existing transactions deleted.")
        except Exception as e:
            print(f"Error clearing transactions table: {e}")

        # Insert fresh rows in batches
        batch_size = 200
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                supabase.table("transactions").insert(batch).execute()
            except Exception as e:
                print(f"Error inserting batch {i}: {e}")

        print("Transaction upload (overwrite) complete.")
    else:
        # Backwards-compatible behavior: upsert on espn_transaction_id
        batch_size = 200
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                supabase.table("transactions").upsert(
                    batch, on_conflict="espn_transaction_id"
                ).execute()
            except Exception as e:
                print(f"Error on batch {i}: {e}")

        print("Transaction upload (upsert) complete.")


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
        # Overwrite the table every run so the DB is a complete snapshot of all transactions
        upsert_transactions(rows, overwrite=True)
    else:
        print("  No executed transactions found.")
