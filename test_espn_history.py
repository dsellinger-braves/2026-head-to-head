"""
test_espn_history.py
Probes ESPN API for historical league data & transactions.
"""
import os
import json
import requests

LEAGUE_ID = 130215
ESPN_S2 = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")

cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 else {}
session = requests.Session()
session.cookies.update(cookies)

print(f"Testing with cookies: ESPN_S2 present? {bool(ESPN_S2)}, SWID present? {bool(ESPN_SWID)}")

# 1. Test leagueHistory without seasonId (all seasons)
print("\n--- 1. Testing leagueHistory endpoint (all historical seasons) ---")
url1 = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/leagueHistory/{LEAGUE_ID}"
for view in ["", "?view=mStatus", "?view=mSettings", "?view=mTransactions2", "?view=kona_league_communication"]:
    test_url = url1 + view
    try:
        r = session.get(test_url, timeout=10)
        print(f"URL: {test_url}")
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                seasons = [s.get("seasonId") for s in data if isinstance(s, dict)]
                print(f"  Returned array of {len(data)} items. Season IDs: {seasons}")
                for s in data:
                    yr = s.get("seasonId")
                    txns = s.get("transactions", [])
                    print(f"    Season {yr}: {len(txns)} transactions")
            elif isinstance(data, dict):
                print(f"  Keys in dict: {list(data.keys())}")
        else:
            print(f"  Error: {r.text[:200]}")
    except Exception as e:
        print(f"  Exception: {e}")

# 3. Test concurrent scoring periods fetch for 2024 and 2023
from concurrent.futures import ThreadPoolExecutor

print("\n--- 3. Testing concurrent scoring periods fetch for 2024 & 2023 ---")

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

for yr in [2024, 2023]:
    tasks = [(yr, sp) for sp in range(0, 186)]
    with ThreadPoolExecutor(max_workers=25) as executor:
        results = list(executor.map(fetch_period, tasks))
    all_txs = [t for sub in results for t in sub]
    trades = [t for t in all_txs if "TRADE" in t.get("type", "")]
    adds = [t for t in all_txs if "ADD" in t.get("type", "") or any(it.get("type") == "ADD" for it in t.get("items", []))]
    print(f"Season {yr} SUCCESS: Total transactions: {len(all_txs)}, Trades: {len(trades)}, Adds: {len(adds)}")
    if trades:
        print(f"  Sample Trade in {yr}: ID {trades[0].get('id')}, Date {trades[0].get('proposedDate') or trades[0].get('executionDate')}")
        for it in trades[0].get("items", []):
            print(f"    Item: player {it.get('playerId')}, from {it.get('fromTeamId')} -> to {it.get('toTeamId')}")
