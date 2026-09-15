#!/usr/bin/env python3
"""
pipelines/ingest_historical_trades.py

Ingests multi-year trades from commissioner Google Sheets:
- 2024: https://docs.google.com/spreadsheets/d/1l6PWOwlFy_HV7o0N6iCS4892BRCnUUeLnTDbO-965NA/edit?gid=698281952#gid=698281952
- 2025: https://docs.google.com/spreadsheets/d/1HqapcZXGV5jPnlsNIi4wWAFj6WjVMlKNf62XTtPtlvM/edit?gid=698281952#gid=698281952
- 2026: https://docs.google.com/spreadsheets/d/1_Nlv1mr8fBHSc0XBlV_u5GgfYhA9ttp7TRYC_ThqSW0/edit?gid=698281952#gid=698281952

Normalizes multi-asset trades (players, picks, keeper budget),
correlates ESPN player IDs, and generates `src/data/historicalTrades.json`.
"""

import urllib.request
import csv
import io
import json
import re
import os
import datetime

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
    "Preston": 14,
    "Joe": 10,       # Historical owner in 2024/2025
    "Patrick": 7      # Historical owner in 2025
}

SHEETS = [
    {
        "season_year": 2024,
        "sheet_id": "1l6PWOwlFy_HV7o0N6iCS4892BRCnUUeLnTDbO-965NA",
        "gid": "698281952"
    },
    {
        "season_year": 2025,
        "sheet_id": "1HqapcZXGV5jPnlsNIi4wWAFj6WjVMlKNf62XTtPtlvM",
        "gid": "698281952"
    },
    {
        "season_year": 2026,
        "sheet_id": "1_Nlv1mr8fBHSc0XBlV_u5GgfYhA9ttp7TRYC_ThqSW0",
        "gid": "698281952"
    }
]

# Known player name to ESPN Player ID mapping
PLAYER_ID_MAP = {
    "mike trout": 30836,
    "trea turner": 33710,
    "bo bichette": 41258,
    "brandon woodruff": 34914,
    "grayson rodriguez": 41285,
    "gunnar henderson": 42537,
    "michael harris ii": 42426,
    "randy arozarena": 36563,
    "kevin gausman": 32667,
    "gerrit cole": 32081,
    "max fried": 32685,
    "corbin carroll": 42403,
    "bobby witt jr.": 42402,
    "bobby witt jr": 42402,
    "austin riley": 34961,
    "bryce harper": 30951,
    "chris sale": 30948,
    "corbin burnes": 36185,
    "oneil cruz": 39800,
    "bryce miller": 42525,
    "cody bellinger": 33912,
    "brayan rocchio": 41217,
    "luke keaschall": 5129618,
    "ketel marte": 32512,
    "trey yesavage": 5212345,
    "josh naylor": 34954,
    "drew rasmussen": 41219,
    "matt olson": 32767,
    "dylan crews": 5129617,
    "jared jones": 42998,
    "sandy alcantara": 35241,
    "jesus luzardo": 36502,
    "trevor larnach": 41262,
    "miguel vargas": 41249,
    "josh hader": 32760,
    "vladimir guerrero jr.": 35002,
    "vladimir guerrero jr": 35002
}


def parse_date(date_str, season_year):
    date_str = date_str.strip()
    # If like "2/29" or "3/3"
    m_short = re.match(r"^(\d{1,2})/(\d{1,2})$", date_str)
    if m_short:
        m, d = int(m_short.group(1)), int(m_short.group(2))
        return f"{season_year}-{m:02d}-{d:02d}"

    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            dt = datetime.datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return date_str


def parse_overall_pick_number(asset_name, season_year=2026):
    name_lower = str(asset_name).strip().lower()
    
    # Check for worst remaining
    if "worst" in name_lower:
        max_rounds = 28 if season_year >= 2026 else 24
        if "2nd" in name_lower or "second" in name_lower:
            rd = max_rounds - 1
        else:
            rd = max_rounds
        return (rd - 1) * 10 + 5

    # Check for ordinal round (e.g. "14th round", "8th round")
    m_round = re.search(r"(\d+)(?:st|nd|rd|th)?\s*round", name_lower)
    if m_round:
        rd = int(m_round.group(1))
        return (rd - 1) * 10 + 5

    digits = re.findall(r"\d+", name_lower)
    if digits:
        val = int(digits[0])
        # If val is small (< 35) and mentioned round
        if "round" in name_lower and val <= 35:
            return (val - 1) * 10 + 5
        return val
    return None


def parse_budget_amount(asset_name):
    # E.g. "$11", "5", "$5.00"
    clean = re.sub(r"[^\d.]", "", str(asset_name))
    try:
        return float(clean)
    except ValueError:
        return 0.0


def fetch_sheet_rows(sheet_id, gid):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"Fetching sheet {sheet_id} (gid {gid})...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        content = resp.read().decode("utf-8")
    return list(csv.reader(io.StringIO(content)))


def process_sheet(sheet_conf):
    season_year = sheet_conf["season_year"]
    rows = fetch_sheet_rows(sheet_conf["sheet_id"], sheet_conf["gid"])
    
    # Locate header row
    header_idx = -1
    for idx, row in enumerate(rows):
        row_lower = [c.strip().lower() for c in row if c]
        if "trade id" in row_lower:
            header_idx = idx
            break
            
    if header_idx == -1:
        print(f"⚠️ Could not find 'Trade ID' header in season {season_year}")
        return []

    date_col = 0
    trade_id_col = 1
    sending_col = 2
    receiving_col = 3
    asset_type_col = 4
    asset_name_col = 5
    orig_pick_col = 6

    raw_items = []
    for r_idx, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        if not any(row):
            continue
        if len(row) <= asset_name_col:
            continue
            
        date_raw = row[date_col].strip()
        trade_id_raw = row[trade_id_col].strip()
        sending = row[sending_col].strip()
        receiving = row[receiving_col].strip()
        asset_type = row[asset_type_col].strip()
        asset_name = row[asset_name_col].strip()
        orig_pick = row[orig_pick_col].strip() if len(row) > orig_pick_col else ""

        if not trade_id_raw or "example" in trade_id_raw.lower() or "trade id" in trade_id_raw.lower() or not date_raw or date_raw.lower() == "date":
            continue

        trade_date = parse_date(date_raw, season_year)
        
        # Asset type normalization
        asset_type_norm = asset_type.title()
        if "pick" in asset_type.lower() or "round" in asset_type.lower():
            asset_type_norm = "Pick"
        elif "budget" in asset_type.lower():
            asset_type_norm = "Budget"
        elif "player" in asset_type.lower():
            asset_type_norm = "Player"

        # Extract numerical details
        pick_number = None
        round_number = None
        budget_amount = 0.0
        espn_player_id = None

        if asset_type_norm == "Pick":
            pick_number = parse_overall_pick_number(asset_name, season_year)
            if pick_number:
                round_number = ((pick_number - 1) // 10) + 1
        elif asset_type_norm == "Budget":
            budget_amount = parse_budget_amount(asset_name)
        elif asset_type_norm == "Player":
            p_clean = asset_name.strip().lower()
            espn_player_id = PLAYER_ID_MAP.get(p_clean)

        raw_items.append({
            "season_year": season_year,
            "trade_id": str(trade_id_raw),
            "trade_date": trade_date,
            "sending_owner": sending,
            "from_team_id": OWNER_TO_TEAM_ID.get(sending),
            "receiving_owner": receiving,
            "to_team_id": OWNER_TO_TEAM_ID.get(receiving),
            "asset_type": asset_type_norm,
            "asset_name": asset_name,
            "pick_number": pick_number,
            "round_number": round_number,
            "budget_amount": budget_amount,
            "original_pick": orig_pick,
            "espn_player_id": espn_player_id
        })

    # Group into unified trade packages
    trades_dict = {}
    for item in raw_items:
        key = f"{item['season_year']}_{item['trade_id']}"
        if key not in trades_dict:
            trades_dict[key] = {
                "unique_id": key,
                "season_year": item["season_year"],
                "trade_id": item["trade_id"],
                "trade_date": item["trade_date"],
                "participants": set(),
                "items": []
            }
        trades_dict[key]["participants"].add(item["sending_owner"])
        trades_dict[key]["participants"].add(item["receiving_owner"])
        trades_dict[key]["items"].append(item)

    unified_trades = []
    for key, t in trades_dict.items():
        participants = sorted(list(t["participants"]))
        
        # Group transfers by owner
        owner_packages = {}
        for p in participants:
            owner_packages[p] = {
                "owner": p,
                "team_id": OWNER_TO_TEAM_ID.get(p),
                "sent": [],
                "received": []
            }

        for item in t["items"]:
            snd = item["sending_owner"]
            rcv = item["receiving_owner"]
            if snd in owner_packages:
                owner_packages[snd]["sent"].append(item)
            if rcv in owner_packages:
                owner_packages[rcv]["received"].append(item)

        unified_trades.append({
            "unique_id": key,
            "season_year": t["season_year"],
            "trade_id": t["trade_id"],
            "trade_date": t["trade_date"],
            "participants": participants,
            "owner_packages": owner_packages,
            "items": t["items"]
        })

    return unified_trades


def main():
    all_trades = []
    for sheet in SHEETS:
        trades = process_sheet(sheet)
        print(f"Season {sheet['season_year']}: Processed {len(trades)} trades.")
        all_trades.extend(trades)

    # Sort descending by trade date
    all_trades.sort(key=lambda x: (x["trade_date"], x["unique_id"]), reverse=True)

    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "data", "historicalTrades.json"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_trades, f, indent=2)

    print(f"🎉 Successfully saved {len(all_trades)} multi-year trades to {output_path}")


if __name__ == "__main__":
    main()
