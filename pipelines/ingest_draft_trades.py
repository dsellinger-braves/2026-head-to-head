#!/usr/bin/env python3
"""
pipelines/ingest_draft_trades.py

Ingests traded draft assets from the Google Sheet Trade Log tab:
https://docs.google.com/spreadsheets/d/1_Nlv1mr8fBHSc0XBlV_u5GgfYhA9ttp7TRYC_ThqSW0/edit?gid=15650013#gid=15650013
(Tab 'Trade Log', gid=698281952)

Correlates future draft assets with ESPN player trade transactions,
upserts records into Supabase table `draft_asset_trades`, and saves
a static cache to `src/data/draftAssetTrades2026.json`.
"""

import urllib.request
import csv
import io
import json
import re
import os
import datetime

# Google Sheet Export URL
SHEET_ID = "1_Nlv1mr8fBHSc0XBlV_u5GgfYhA9ttp7TRYC_ThqSW0"
TRADE_LOG_GID = "698281952"

# Supabase REST Config
SUPABASE_URL = os.getenv("VITE_SUPABASE_URL", "https://wczdkcdqgtzlsbssogoz.supabase.co")
SUPABASE_ANON_KEY = os.getenv(
    "VITE_SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

OWNER_TO_TEAM_ID = {
    "Tim": 1,
    "Adrian": 2,
    "Garrett": 3,
    "Dan": 5,
    "Daniel": 5,
    "Anil": 6,
    "Alex": 8,
    "Will": 12,
    "Mark": 13,
    "Preston": 14
}

TEAM_ID_TO_OWNER = {v: k for k, v in OWNER_TO_TEAM_ID.items()}
TEAM_ID_TO_OWNER[5] = "Daniel"

# Known ESPN trade correlations for 2026 trades
KNOWN_TRADE_CORRELATIONS = {
    "1": {
        "espn_trade_id": "3296af0d-be6f-437d-9c25-2935555f789a",
        "description": "Bellinger to Garrett, Rocchio + 14th Rd pick to Mark"
    },
    "2": {
        "espn_trade_id": "14f5d2f5-cf22-4e23-afab-3936c259eb06",
        "description": "Keaschall + 13th Rd pick to Preston, Marte + Worst remaining pick to Daniel"
    },
    "3": {
        "espn_trade_id": "47e3fb97-97d5-4657-8e63-b1b4beb4d8d6",
        "description": "Yesavage, Naylor + 11th Rd pick to Garrett, Rasmussen, Olson + 22nd Rd pick to Daniel"
    },
    "4": {
        "espn_trade_id": "2bfc6d89-0637-4af2-9d3b-b72a4938cdcb",
        "description": "Crews, Jones + 8th Rd pick to Preston, Alcantara, Luzardo + 20th Rd pick to Adrian"
    },
    "5": {
        "espn_trade_id": "a816db90-3357-4853-9a06-80bb02f4e097",
        "description": "Larnach + 9th Rd pick to Garrett, Vargas + 25th Rd pick to Adrian"
    },
    "6": {
        "espn_trade_id": "4214c131-6173-4f43-b877-6f686134b0dd",
        "description": "Hader + 2nd worst remaining pick to Will, Guerrero + 22nd Rd pick to Preston"
    }
}


def parse_round_number(asset_name, sending_owner, trade_id):
    """
    Parses round number from asset name (e.g., '14th round' -> 14,
    'Worst remaining' -> 32, '2nd worst remaining' -> 31).
    """
    name_lower = asset_name.strip().lower()
    
    # Check for ordinal round numbers (e.g. 14th round, 8th round)
    match = re.search(r'(\d+)(?:st|nd|rd|th)?\s*round', name_lower)
    if match:
        return int(match.group(1))
    
    # Check for single integer if asset type is Overall Pick or Round
    if name_lower.isdigit():
        return int(name_lower)
    
    # Special conditions:
    if "worst" in name_lower:
        if "2nd" in name_lower or "second" in name_lower:
            return 31
        return 32

    return None


def fetch_sheet_csv():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={TRADE_LOG_GID}"
    print(f"🌐 Fetching Trade Log CSV from {url}...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        content = resp.read().decode("utf-8")
    return list(csv.reader(io.StringIO(content)))


def parse_trade_log(rows):
    """
    Finds the trade table starting at the header row containing 'Trade ID'.
    """
    header_idx = -1
    for idx, row in enumerate(rows):
        if len(row) > 3 and "trade id" in [c.strip().lower() for c in row]:
            header_idx = idx
            break
            
    if header_idx == -1:
        raise ValueError("Could not locate 'Trade ID' header row in Trade Log sheet.")

    header = [c.strip() for c in rows[header_idx]]
    print(f"📋 Found header at row {header_idx + 1}: {header[:7]}")

    date_col = 0
    trade_id_col = 1
    sending_col = 2
    receiving_col = 3
    asset_type_col = 4
    asset_name_col = 5
    orig_pick_col = 6 if len(header) > 6 else -1

    parsed_items = []
    
    for row_num, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        if not any(row):
            continue
        if len(row) <= max(date_col, trade_id_col, sending_col, receiving_col, asset_type_col, asset_name_col):
            continue
            
        date_str = row[date_col].strip()
        trade_id = row[trade_id_col].strip()
        sending_owner = row[sending_col].strip()
        receiving_owner = row[receiving_col].strip()
        asset_type = row[asset_type_col].strip()
        asset_name = row[asset_name_col].strip()
        orig_pick = row[orig_pick_col].strip() if orig_pick_col != -1 and len(row) > orig_pick_col else ""

        # Skip example, header repetitions, or empty rows
        if not trade_id or trade_id.upper().startswith("EXAMPLE") or trade_id.lower() == "trade id" or date_str.lower() == "date" or not date_str:
            continue

        # Format date as YYYY-MM-DD
        formatted_date = None
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
            try:
                dt = datetime.datetime.strptime(date_str, fmt)
                formatted_date = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                pass

        if not formatted_date:
            formatted_date = date_str

        from_team_id = OWNER_TO_TEAM_ID.get(sending_owner)
        to_team_id = OWNER_TO_TEAM_ID.get(receiving_owner)
        round_num = parse_round_number(asset_name, sending_owner, trade_id)

        correlation = KNOWN_TRADE_CORRELATIONS.get(trade_id, {})
        espn_txn_id = correlation.get("espn_trade_id")

        parsed_items.append({
            "trade_id": trade_id,
            "trade_date": formatted_date,
            "season_year": 2026,
            "target_draft_year": 2027,
            "sending_owner": sending_owner,
            "from_team_id": from_team_id,
            "receiving_owner": receiving_owner,
            "to_team_id": to_team_id,
            "asset_type": asset_type,
            "asset_name": asset_name,
            "round_num": round_num,
            "original_owner": orig_pick if orig_pick else sending_owner,
            "espn_transaction_id": espn_txn_id,
            "notes": correlation.get("description", "")
        })

    return parsed_items


def upsert_to_supabase(records):
    print(f"🚀 Upserting {len(records)} records into Supabase 'draft_asset_trades'...")
    url = f"{SUPABASE_URL}/rest/v1/draft_asset_trades"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    req = urllib.request.Request(url, data=json.dumps(records).encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"✅ Supabase upsert successful (HTTP {resp.status})")
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        print(f"⚠️ Supabase upsert error HTTP {e.code}: {err_msg}")


def save_local_json(all_items):
    out_dir = os.path.join(os.path.dirname(__file__), "..", "src", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "draftAssetTrades2026.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2)
    print(f"💾 Saved local JSON cache to {out_path} ({len(all_items)} records)")


def main():
    rows = fetch_sheet_csv()
    items = parse_trade_log(rows)
    print(f"🎯 Successfully parsed {len(items)} trade log items.")
    
    # Filter for draft assets (Overall Pick, Budget, etc.) and player items
    draft_assets = [item for item in items if item["asset_type"] in ("Overall Pick", "Budget", "Draft Pick")]
    print(f"📦 Future draft assets traded: {len(draft_assets)}")
    for da in draft_assets:
        print(f"  - Trade #{da['trade_id']} ({da['trade_date']}): {da['sending_owner']} -> {da['receiving_owner']}: {da['asset_name']} (Round {da['round_num']})")

    # Upsert all items into Supabase
    try:
        upsert_to_supabase(items)
    except Exception as e:
        print(f"Error during Supabase upsert: {e}")

    # Save to local JSON
    save_local_json(items)


if __name__ == "__main__":
    main()
