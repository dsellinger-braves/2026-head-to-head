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

def parse_standard_transactions(txns: List[Dict], yr: int = YEAR, include_trades: bool = False) -> List[Dict]:
    """Parse standard add, drop, waiver claim, and trade transactions from mTransactions2."""
    rows = []
    for txn in txns:
        txn_id = txn.get("id")
        if not txn_id:
            continue

        raw_type = txn.get("type", "UNKNOWN")

        # Skip roster lineup changes completely here
        if raw_type in ("ROSTER", "FUTURE_ROSTER"):
            continue

        executed_ms = txn.get("executedDate") or txn.get("proposedDate", 0)
        txn_date    = datetime.fromtimestamp(executed_ms / 1000, tz=timezone.utc)
        period_id   = txn.get("scoringPeriodId", 0)

        # Handle trade objects if requested
        if "TRADE" in raw_type:
            if not include_trades:
                continue
            if txn.get("status") != "EXECUTED":
                continue
            for item in txn.get("items", []):
                to_team_id   = item.get("toTeamId", -1)
                from_team_id = item.get("fromTeamId", -1)
                player_id    = item.get("playerId")
                if not player_id or to_team_id <= 0 or from_team_id <= 0:
                    continue
                rows.append({
                    "espn_transaction_id": f"{txn_id}_{player_id}_{to_team_id}",
                    "league_id":           LEAGUE_ID,
                    "season_year":         yr,
                    "transaction_type":    "TRADE",
                    "transaction_date":    txn_date.isoformat(),
                    "scoring_period_id":   period_id,
                    "to_team_id":          to_team_id,
                    "from_team_id":        from_team_id,
                    "player_id":           player_id,
                    "player_name":         item.get("playerNote") or f"Player {player_id}",
                    "raw_type":            raw_type,
                })
            continue

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
            elif "TRADE" in item_type or "TRADE" in raw_type:
                txn_type = "TRADE"
            else:
                txn_type = item_type or raw_type

            rows.append({
                "espn_transaction_id": f"{txn_id}_{player_id}_{to_team_id}",
                "league_id":           LEAGUE_ID,
                "season_year":         yr,
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
        f"/segments/0/leagues/{LEAGUE_ID}?view=kona_league_communication"
    )
    
    cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 else {}
    all_topics = []
    offset = 0
    limit = 200
    
    headers = {
        "x-fantasy-filter": json.dumps({
            "communication": {
                "topics": {
                    "filterType": {"value": ["ACTIVITY_TRANSACTIONS"]},
                    "limit": limit,
                    "limitPerMessageSet": {"value": 50},
                    "offset": offset,
                    "sortMessageDate": {"sortPriority": 1, "sortAsc": False}
                }
            }
        })
    }

    print("Paging through activity feed for trades...")
    while True:
        try:
            resp = requests.get(url, headers=headers, cookies=cookies, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            topics = data.get("communication", {}).get("topics", [])
            if not topics:
                break
                
            all_topics.extend(topics)
            offset += limit
            
            # Update filter for next page
            filters = json.loads(headers["x-fantasy-filter"])
            filters["communication"]["topics"]["offset"] = offset
            headers["x-fantasy-filter"] = json.dumps(filters)
            
        except requests.exceptions.HTTPError as e:
            print(f"Failed to fetch activity trades at offset {offset}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"ESPN API Response: {e.response.text[:200]}")
            break
        except Exception as e:
            print(f"Error fetching trades at offset {offset}: {e}")
            break

    return all_topics
    
def parse_activity_trades(topics: List[Dict]) -> List[Dict]:
    """Parse only the fully executed system trades from the activity feed."""
    rows = []
    for topic in topics:
        if topic.get("author") != "TradeTaskProcessor":
            continue
            
        topic_id = topic.get("id", "")
        
        for msg in topic.get("messages", []):
            player_id = msg.get("targetId")
            from_team_id = msg.get("from")
            to_team_id = msg.get("to")
            msg_type_id = msg.get("messageTypeId")
            
            if not player_id:
                continue
                
            date_ms = msg.get("date", 0) or topic.get("date", 0)
            txn_date = datetime.fromtimestamp(date_ms / 1000, tz=timezone.utc)
            
            if msg_type_id == 244 and to_team_id and to_team_id > 0 and from_team_id and from_team_id > 0:
                rows.append({
                    "espn_transaction_id": f"{topic_id}_{player_id}_{to_team_id}",
                    "league_id":           LEAGUE_ID,
                    "season_year":         YEAR,
                    "transaction_type":    "TRADE",
                    "transaction_date":    txn_date.isoformat(),
                    "scoring_period_id":   0,
                    "to_team_id":          to_team_id,
                    "from_team_id":        from_team_id,
                    "player_id":           player_id,
                    "player_name":         f"Player {player_id}",
                    "raw_type":            "TRADE"
                })
            elif msg_type_id == 245 or to_team_id == 0:
                rows.append({
                    "espn_transaction_id": f"{topic_id}_{player_id}_0",
                    "league_id":           LEAGUE_ID,
                    "season_year":         YEAR,
                    "transaction_type":    "DROP",
                    "transaction_date":    txn_date.isoformat(),
                    "scoring_period_id":   0,
                    "to_team_id":          0,
                    "from_team_id":        from_team_id,
                    "player_id":           player_id,
                    "player_name":         f"Player {player_id}",
                    "raw_type":            "TRADE_DROP"
                })
    return rows

# ---------------------------------------------------------------------------
# HISTORICAL ESPN TRANSACTIONS & TRADES SCRAPER (2018–2025)
# ---------------------------------------------------------------------------

def fetch_historical_transactions(years: List[int] = None) -> List[Dict]:
    """Fetch all historical transactions (adds, drops, waivers, trades) across seasons using parallel scoring periods."""
    from concurrent.futures import ThreadPoolExecutor
    if years is None:
        years = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 else {}
    session = requests.Session()
    session.cookies.update(cookies)

    all_historical_txns: List[Dict] = []

    def fetch_period(args):
        yr, sp = args
        url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{yr}/segments/0/leagues/{LEAGUE_ID}?view=mTransactions2&scoringPeriodId={sp}"
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                return r.json().get("transactions", [])
        except Exception:
            pass
        return []

    for yr in sorted(years):
        print(f"Fetching complete transaction history for Season {yr}...")
        tasks = [(yr, sp) for sp in range(0, 186)]
        with ThreadPoolExecutor(max_workers=25) as executor:
            results = list(executor.map(fetch_period, tasks))
        raw_txns = [t for sub in results for t in sub]
        parsed_rows = parse_standard_transactions(raw_txns, yr=yr, include_trades=True)

        # Deduplicate
        unique_yr = {}
        for r in parsed_rows:
            unique_yr[r["espn_transaction_id"]] = r
        yr_rows = list(unique_yr.values())
        trade_count = len([r for r in yr_rows if r.get("transaction_type") == "TRADE"])
        print(f"  Season {yr}: Parsed {len(yr_rows)} unique transactions (Trades: {trade_count})")
        all_historical_txns.extend(yr_rows)

    return all_historical_txns

fetch_historical_trades = fetch_historical_transactions

# ---------------------------------------------------------------------------
# PLAYER NAME ENRICHMENT
# ---------------------------------------------------------------------------

def fetch_all_player_names() -> Dict[int, str]:
    """Fetch MLB player names from ESPN and database fallback."""
    name_map: Dict[int, str] = {}
    url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{YEAR}/players?view=players_wl"
    headers = {"x-fantasy-filter": '{"filterActive":null}'}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            for p in resp.json():
                name_map[p["id"]] = p.get("fullName", f"Player {p['id']}")
    except Exception as e:
        print(f"Error bulk fetching player names from ESPN: {e}")

    # Fallback to Supabase dim_players table
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            res = supabase.table("dim_players").select("player_id, player_name").execute()
            for r in res.data or []:
                pid = r.get("player_id")
                pname = r.get("player_name")
                if pid and pname and int(pid) not in name_map:
                    name_map[int(pid)] = pname
        except Exception:
            pass

    return name_map

def enrich_player_names(rows: List[Dict]) -> List[Dict]:
    name_map = fetch_all_player_names()
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
    import sys

    run_historical = "--historical" in sys.argv or os.environ.get("SCRAPE_HISTORICAL", "").lower() in ("1", "true", "yes")

    if run_historical:
        print(f"🚀 Scraping HISTORICAL ESPN trade transactions (2018–2025)...")
        historical_trades = fetch_historical_trades()
        print(f"🎯 Total historical trades retrieved: {len(historical_trades)}")
        if historical_trades:
            historical_trades = enrich_player_names(historical_trades)
            upsert_transactions(historical_trades, overwrite=False)

            out_dir = os.path.join(os.path.dirname(__file__), "src", "data")
            os.makedirs(out_dir, exist_ok=True)
            hist_json = os.path.join(out_dir, "transactions_historical.json")
            with open(hist_json, "w", encoding="utf-8") as f:
                json.dump(historical_trades, f, indent=2)
            print(f"✅ Saved historical transactions to {hist_json}")

    print(f"Fetching standard transactions for league {LEAGUE_ID} (Season {YEAR})...")
    raw_txns = fetch_transactions()
    base_rows = parse_standard_transactions(raw_txns, yr=YEAR)
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

        # Also persist to bundled JSON if running in repository
        local_json = os.path.join(os.path.dirname(__file__), "src", "data", "transactions2026.json")
        if os.path.exists(os.path.dirname(local_json)):
            try:
                sorted_rows = sorted(all_rows, key=lambda x: x.get("transaction_date", ""), reverse=True)
                with open(local_json, "w") as f:
                    json.dump(sorted_rows, f, indent=2)
                print(f"Updated local bundled JSON: {local_json} with {len(sorted_rows)} transactions.")
            except Exception as e:
                print(f"Failed to update local JSON: {e}")
    else:
        print("  No 2026 transactions found.")
