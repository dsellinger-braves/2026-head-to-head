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

# 2. Test specific historical seasons with sort filter
print("\n--- 2. Testing seasons/{yr} with view=mTransactions2 and sortMessageDate ---")
headers = {
    "x-fantasy-filter": json.dumps({
        "transactions": {
            "sortMessageDate": {"sortPriority": 1, "sortAsc": False},
            "limit": 5000
        }
    })
}

for yr in [2025, 2024, 2023, 2022, 2021]:
    base = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{yr}/segments/0/leagues/{LEAGUE_ID}?view=mTransactions2"
    try:
        r = session.get(base, headers=headers, timeout=15)
        print(f"Season {yr} mTransactions2 (sorted filter): Status {r.status_code}")
        if r.status_code == 200:
            txs = r.json().get("transactions", [])
            print(f"  Found {len(txs)} total transactions for Season {yr}!")
            trades = [t for t in txs if "TRADE" in t.get("type", "")]
            print(f"  Trades in {yr}: {len(trades)}")
            if trades:
                sample = trades[0]
                print(f"  Sample trade ID: {sample.get('id')}, items: {len(sample.get('items', []))}")
        else:
            print(f"  Error: {r.text[:200]}")
    except Exception as e:
        print(f"  Exception: {e}")
