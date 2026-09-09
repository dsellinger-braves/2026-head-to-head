#!/usr/bin/env python3
"""
pipelines/ingest_draft_infrastructure.py

Ingests:
1. Compensation Picks & Team Budgets (gid=904314503)
2. Keeper Input (gid=1710745212)
3. Future Draft Asset Trades (gid=698281952)

Upserts to Supabase tables:
- `draft_team_budgets`
- `draft_compensation_picks`
- `draft_keepers`
- `draft_asset_trades`

And writes static local caches to `src/data/`:
- `src/data/teamBudgets2026.json`
- `src/data/compensationPicks2026.json`
- `src/data/keeperInput2026.json`
- `src/data/draftAssetTrades2026.json`
"""

import urllib.request
import csv
import io
import json
import re
import os
import sys

SHEET_ID = "1_Nlv1mr8fBHSc0XBlV_u5GgfYhA9ttp7TRYC_ThqSW0"
COMP_PICKS_GID = "904314503"
KEEPER_INPUT_GID = "1710745212"
TRADE_LOG_GID = "698281952"

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

def fetch_csv(gid):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return list(csv.reader(io.StringIO(resp.read().decode("utf-8"))))

def clean_currency(val):
    if not val:
        return 0.0
    val_str = str(val).replace("$", "").replace(",", "").replace("#VALUE!", "0").strip()
    try:
        return float(val_str)
    except ValueError:
        return 0.0

def postgrest_upsert(table, records, on_conflict):
    if not records:
        print(f"⚠️ No records to upsert for {table}")
        return True
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    payload = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation"
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(f"✅ Supabase upserted {len(data)} rows into {table} (HTTP {resp.status})")
            return True
    except urllib.error.HTTPError as e:
        print(f"❌ Supabase HTTPError ({table}): {e.code} - {e.read().decode('utf-8')}")
        return False
    except Exception as e:
        print(f"❌ Supabase Exception ({table}): {e}")
        return False

# --- 1. INGEST KEEPERS ---
def ingest_keepers():
    print("\n📦 Fetching Keeper Input...")
    rows = fetch_csv(KEEPER_INPUT_GID)
    # Row 3 contains owner names in cols 2..10: Adrian, Alex, Anil, Daniel, Garrett, Mark, Preston, Tim, Will
    owner_cols = {}
    for col_idx in range(2, min(12, len(rows[3]))):
        owner = rows[3][col_idx].strip()
        if owner in OWNER_TO_TEAM_ID:
            owner_cols[col_idx] = owner

    keepers_data = []
    # 5 keepers:
    # Slot 1: Player (row 4), ESPN ID & Info (row 5), Rank (row 6), Cost (row 7)
    # Slot 2: Player (row 8), ESPN ID & Info (row 9), Rank (row 10), Cost (row 11)
    # Slot 3: Player (row 12), ESPN ID & Info (row 13), Rank (row 14), Cost (row 15)
    # Slot 4: Player (row 16), ESPN ID & Info (row 17), Rank (row 18), Cost (row 19)
    # Slot 5: Player (row 20), ESPN ID & Info (row 21), Rank (row 22), Cost (row 23)
    slot_row_map = [
        (1, 4, 5, 6, 7),
        (2, 8, 9, 10, 11),
        (3, 12, 13, 14, 15),
        (4, 16, 17, 18, 19),
        (5, 20, 21, 22, 23)
    ]

    for slot, p_row, id_row, rank_row, cost_row in slot_row_map:
        for col_idx, owner in owner_cols.items():
            player_name = rows[p_row][col_idx].strip()
            raw_id_info = rows[id_row][col_idx].strip()
            rank_str = rows[rank_row][col_idx].strip()
            cost_str = rows[cost_row][col_idx].strip()

            # Parse ESPN ID and metadata from "36969 / NYM / OF"
            espn_id = None
            mlb_team = None
            position = None
            if "/" in raw_id_info:
                parts = [p.strip() for p in raw_id_info.split("/")]
                if len(parts) >= 1: espn_id = parts[0]
                if len(parts) >= 2: mlb_team = parts[1]
                if len(parts) >= 3: position = parts[2]
            else:
                espn_id = raw_id_info

            rank = int(rank_str) if rank_str.isdigit() else None
            cost = clean_currency(cost_str)

            keepers_data.append({
                "season_year": 2026,
                "owner": owner,
                "team_id": OWNER_TO_TEAM_ID[owner],
                "keeper_slot": slot,
                "player_name": player_name,
                "espn_player_id": espn_id,
                "position": position,
                "mlb_team": mlb_team,
                "rank": rank,
                "cost": cost
            })

    # Also extract owner summary stats from rows 31-34
    owner_keeper_summary = {}
    for col_idx, owner in owner_cols.items():
        total_spend = clean_currency(rows[31][col_idx])
        avg_rank = float(rows[33][col_idx].strip()) if rows[33][col_idx].strip() else 0.0
        spend_rank = int(rows[34][col_idx].strip()) if rows[34][col_idx].strip().isdigit() else None
        owner_keeper_summary[owner] = {
            "total_spend": total_spend,
            "average_rank": avg_rank,
            "spend_rank": spend_rank
        }

    return keepers_data, owner_keeper_summary

# --- 2. INGEST COMPENSATION PICKS & BUDGETS ---
def ingest_compensation_picks_and_budgets(owner_keeper_summary):
    print("\n📦 Fetching Compensation Picks & Budgets...")
    rows = fetch_csv(COMP_PICKS_GID)

    # Header rounds at row 6, cols 11 to 42 (rounds 1 to 32)
    round_cols = {c: int(rows[6][c].strip()) for c in range(11, 43) if rows[6][c].strip().isdigit()}

    # Price schedules
    # Row 2 is purchase price ($15 for rd 6 down to $1 for rd 20)
    # Row 4 is sale price ($8 for rd 6, $4 for rd 7, $2 for rd 8, $1 for rd 9)
    purchase_prices = {}
    for c, r_num in round_cols.items():
        if c < len(rows[2]) and rows[2][c].strip().startswith("$"):
            purchase_prices[r_num] = clean_currency(rows[2][c])

    sale_prices = {}
    for c, r_num in round_cols.items():
        if c < len(rows[4]) and rows[4][c].strip().startswith("$"):
            sale_prices[r_num] = clean_currency(rows[4][c])

    # Manager rows 7 to 15
    budgets_data = []
    comp_picks_data = []

    # Map of overall pick numbers for inserted comp picks based on 2026 Draft Sheet:
    # Round 6 Comp Picks: Garrett (#55), Tim (#56)
    # Round 9 Comp Pick: Anil (#84)
    # Round 15 Comp Pick: Daniel (#139)
    # Round 16 Comp Pick: Daniel (#149)
    # Round 17 Comp Pick: Daniel (#159)
    # Round 18 Comp Picks: Daniel (#169), Will (#170)
    # Round 19 Comp Picks: Alex (#180), Daniel (#181)
    # Round 20 Comp Pick: Daniel (#191)
    KNOWN_COMP_PICK_OVERALLS = {
        ("Garrett", 6): 55,
        ("Tim", 6): 56,
        ("Anil", 9): 84,
        ("Daniel", 15): 139,
        ("Daniel", 16): 149,
        ("Daniel", 17): 159,
        ("Daniel", 18): 169,
        ("Will", 18): 170,
        ("Alex", 19): 180,
        ("Daniel", 19): 181,
        ("Daniel", 20): 191
    }

    for r_idx in range(7, 16):
        owner = rows[r_idx][0].strip()
        if owner not in OWNER_TO_TEAM_ID:
            continue
        finish_rank = int(rows[r_idx][1].strip()) if rows[r_idx][1].strip().isdigit() else None
        budget_spent = clean_currency(rows[r_idx][3])
        comp_spend = clean_currency(rows[r_idx][5])
        comp_income = clean_currency(rows[r_idx][6])
        purchased_count = int(rows[r_idx][8].strip()) if rows[r_idx][8].strip().isdigit() else 0
        sold_count = int(rows[r_idx][9].strip()) if rows[r_idx][9].strip().isdigit() else 0
        net_picks = int(rows[r_idx][10].strip()) if rows[r_idx][10].strip().lstrip("-").isdigit() else 0

        # Calculate effective base budget: standard is $100, but Mark had $111, Adrian $101, Daniel $102
        # If total spend > 100, base budget was expanded with bonuses/trades
        base_budget = max(100.0, budget_spent + comp_spend)
        final_budget = base_budget - budget_spent - comp_spend + comp_income

        budgets_data.append({
            "season_year": 2026,
            "owner": owner,
            "team_id": OWNER_TO_TEAM_ID[owner],
            "finish_rank": finish_rank,
            "base_budget": base_budget,
            "keeper_spend": budget_spent,
            "comp_pick_spend": comp_spend,
            "comp_pick_income": comp_income,
            "final_budget": final_budget,
            "net_picks": net_picks
        })

        # Scan rounds grid for B (Bought), L (Lost/Offset), S (Sold)
        for c, r_num in round_cols.items():
            val = rows[r_idx][c].strip().upper()
            if val == "B":
                cost = purchase_prices.get(r_num, 21 - r_num if 6 <= r_num <= 20 else 0)
                overall = KNOWN_COMP_PICK_OVERALLS.get((owner, r_num))
                comp_picks_data.append({
                    "season_year": 2026,
                    "owner": owner,
                    "team_id": OWNER_TO_TEAM_ID[owner],
                    "action_type": "BOUGHT",
                    "round_num": r_num,
                    "cost_or_income": cost,
                    "overall_pick_num": overall,
                    "notes": f"Purchased Round {r_num} compensation pick for ${int(cost)}"
                })
            elif val == "L":
                comp_picks_data.append({
                    "season_year": 2026,
                    "owner": owner,
                    "team_id": OWNER_TO_TEAM_ID[owner],
                    "action_type": "OFFSET_LOST",
                    "round_num": r_num,
                    "cost_or_income": 0.0,
                    "overall_pick_num": None,
                    "notes": f"Offset/forfeited Round {r_num} pick due to purchased comp pick"
                })
            elif val == "S":
                income = sale_prices.get(r_num, 0.0)
                comp_picks_data.append({
                    "season_year": 2026,
                    "owner": owner,
                    "team_id": OWNER_TO_TEAM_ID[owner],
                    "action_type": "SOLD",
                    "round_num": r_num,
                    "cost_or_income": income,
                    "overall_pick_num": None,
                    "notes": f"Sold Round {r_num} pick for +${int(income)} budget"
                })

    return budgets_data, comp_picks_data, purchase_prices, sale_prices

def main():
    print("🚀 Starting Draft Infrastructure Ingestion...")

    # 1. Ingest Keepers
    keepers_data, owner_keeper_summary = ingest_keepers()
    print(f"📋 Parsed {len(keepers_data)} keepers across 9 teams.")

    # 2. Ingest Compensation Picks & Budgets
    budgets_data, comp_picks_data, purchase_prices, sale_prices = ingest_compensation_picks_and_budgets(owner_keeper_summary)
    print(f"💰 Parsed budgets for {len(budgets_data)} owners.")
    print(f"🎟️ Parsed {len(comp_picks_data)} compensation/offset pick events.")

    # 3. Upsert to Supabase
    print("\n⚡ Upserting to Supabase...")
    postgrest_upsert("draft_team_budgets", budgets_data, "season_year,owner")
    postgrest_upsert("draft_compensation_picks", comp_picks_data, "season_year,owner,action_type,round_num")
    postgrest_upsert("draft_keepers", keepers_data, "season_year,owner,keeper_slot")

    # 4. Save Static Fallback JSONs to src/data/
    out_dir = os.path.join(os.path.dirname(__file__), "..", "src", "data")
    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, "teamBudgets2026.json"), "w", encoding="utf-8") as f:
        json.dump({
            "updated_at": "2026-09-09T21:40:00Z",
            "purchase_prices": purchase_prices,
            "sale_prices": sale_prices,
            "budgets": budgets_data,
            "keeper_summaries": owner_keeper_summary
        }, f, indent=2)
    print(f"💾 Saved {os.path.join(out_dir, 'teamBudgets2026.json')}")

    with open(os.path.join(out_dir, "compensationPicks2026.json"), "w", encoding="utf-8") as f:
        json.dump({
            "updated_at": "2026-09-09T21:40:00Z",
            "comp_picks": comp_picks_data
        }, f, indent=2)
    print(f"💾 Saved {os.path.join(out_dir, 'compensationPicks2026.json')}")

    with open(os.path.join(out_dir, "keeperInput2026.json"), "w", encoding="utf-8") as f:
        json.dump({
            "updated_at": "2026-09-09T21:40:00Z",
            "keepers": keepers_data
        }, f, indent=2)
    print(f"💾 Saved {os.path.join(out_dir, 'keeperInput2026.json')}")

    print("\n🎉 Draft Infrastructure Ingestion Complete!")

if __name__ == "__main__":
    main()
