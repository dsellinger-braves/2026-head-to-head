#!/usr/bin/env python3
"""
pipelines/generate_player_owner_stats.py

Flattens daily fantasy player records across all available seasons (2018–2026)
into pre-aggregated player-owner-season rows.

Outputs:
1. Local static bundle: src/data/playerOwnerSeasonStats.json
2. (Optional/Target) Supabase table: player_owner_season_stats
"""

import os
import json
import time
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# Active Supabase config
DEFAULT_SUPABASE_URL = "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"

raw_url = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
if not raw_url or "your-project" in raw_url:
    SUPABASE_URL = DEFAULT_SUPABASE_URL
else:
    SUPABASE_URL = raw_url

SUPABASE_URL = DEFAULT_SUPABASE_URL
SUPABASE_KEY = DEFAULT_SUPABASE_ANON_KEY

LEAGUE_ID = 130215

# Owner name mappings
TEAM_OWNERS = {
    1: "Tim",
    2: "Adrian",
    3: "Garrett",
    5: "Dan",
    6: "Anil",
    7: "Patrick",
    8: "Alex",
    9: "Owens",
    10: "Joe",
    11: "Michael",
    12: "Will",
    13: "Mark",
    14: "Preston",
    99: "Ghost"
}

# Lineup slot mappings
SLOT_NAMES = {
    0: 'C', 1: '1B', 2: '2B', 3: '3B', 4: 'SS',
    5: 'OF', 6: '2B/SS', 7: '1B/3B', 11: 'DH', 12: 'UTIL',
    13: 'P', 14: 'SP', 15: 'RP',
    16: 'Bench', 17: 'IL', 19: 'IF'
}

PRIMARY_POS_SLOTS = {
    0: 'C', 1: '1B', 2: '2B', 3: '3B', 4: 'SS',
    5: 'OF', 11: 'DH', 14: 'SP', 15: 'RP'
}


def get_supabase_client():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


HIST_COLS = 'id, team_id, fullName, lineupSlotID, "0", "1", "3", "4", "5", "8", "10", "12", "13", "16", "20", "21", "23", "24", "27", "33", "34", "37", "39", "44", "45", "48", "53", "54", "57", "60", "63"'
DAILY_COLS = 'player_id, team_id, full_name, lineup_slot_id, stats'

def fetch_all_daily_records_for_year(supabase, year):
    """Fetch all daily records for a given season from historical_data or player_daily_stats."""
    is_historical = (year < 2026)
    table_name = 'historical_data' if is_historical else 'player_daily_stats'
    select_cols = HIST_COLS if is_historical else DAILY_COLS
    
    print(f"[{year}] Querying {table_name}...")
    page_size = 1000
    all_rows = []
    
    # Get total count first
    query = supabase.table(table_name).select('id' if not is_historical else 'season_year', count='exact', head=True)
    if is_historical:
        query = query.eq('league_id', LEAGUE_ID).eq('season_year', year)
    else:
        query = query.eq('league_id', LEAGUE_ID)
    
    res = query.execute()
    total_count = res.count or 0
    print(f"[{year}] Total records to fetch: {total_count}")
    
    if total_count == 0:
        return []

    # Fetch in pages
    pages = (total_count + page_size - 1) // page_size
    for p in range(pages):
        start = p * page_size
        end = start + page_size - 1
        q = supabase.table(table_name).select(select_cols).range(start, end)
        if is_historical:
            q = q.eq('league_id', LEAGUE_ID).eq('season_year', year)
        else:
            q = q.eq('league_id', LEAGUE_ID)
        
        page_res = q.execute()
        if page_res.data:
            all_rows.extend(page_res.data)
        if (p + 1) % 5 == 0 or p == pages - 1:
            print(f"[{year}] Fetched {len(all_rows)} / {total_count} rows...")
            
    return all_rows


def aggregate_season_data(records, year):
    """Aggregate raw daily rows into player-owner rows for a single season."""
    is_historical = (year < 2026)
    groups = defaultdict(lambda: {
        'player_id': None,
        'player_name': '',
        'team_id': None,
        'owner_name': '',
        'season_year': year,
        'positions_set': set(),
        'pos_counts': defaultdict(int),
        'days_active': 0,
        'days_bench': 0,
        'days_il': 0,
        'days_total': 0,
        # Batting active
        'pa': 0, 'ab': 0, 'h': 0, 'r': 0, 'hr': 0, 'rbi': 0, 'sb': 0, 'bb': 0, 'so': 0,
        'd2': 0, 'd3': 0, 'tb': 0, 'sf': 0, 'hbp': 0, 'cs': 0,
        # Pitching active
        'gs': 0, 'ip_outs': 0, 'k': 0, 'qs': 0, 'w': 0, 'l': 0, 'sv': 0, 'hd': 0,
        'er': 0, 'h_allowed': 0, 'bb_allowed': 0, 'r_allowed': 0,
        # Bench totals
        'bench_pa': 0, 'bench_h': 0, 'bench_r': 0, 'bench_hr': 0, 'bench_rbi': 0, 'bench_sb': 0,
        'bench_ip_outs': 0, 'bench_k': 0, 'bench_qs': 0, 'bench_sv_hd': 0, 'bench_er': 0,
        'is_pitcher': False,
        'is_batter': False,
    })

    for row in records:
        if is_historical:
            player_id = row.get('id')
            player_name = row.get('fullName') or ''
            team_id = row.get('team_id')
            slot_id = row.get('lineupSlotID')
            try:
                slot_id = int(slot_id) if slot_id is not None else None
            except:
                slot_id = None
        else:
            player_id = row.get('player_id')
            player_name = row.get('full_name') or ''
            team_id = row.get('team_id')
            slot_id = row.get('lineup_slot_id')

        if not player_id or not team_id:
            continue

        key = (year, team_id, player_id)
        g = groups[key]
        g['player_id'] = player_id
        g['team_id'] = team_id
        g['owner_name'] = TEAM_OWNERS.get(team_id, f"Team {team_id}")
        if player_name and len(player_name) > len(g['player_name']):
            g['player_name'] = player_name

        is_bench = (slot_id == 16)
        is_il = (slot_id == 17)
        is_active = (slot_id is not None and not is_bench and not is_il)

        g['days_total'] += 1
        if is_active:
            g['days_active'] += 1
            if slot_id in PRIMARY_POS_SLOTS:
                g['pos_counts'][PRIMARY_POS_SLOTS[slot_id]] += 1
            elif slot_id in SLOT_NAMES:
                g['pos_counts'][SLOT_NAMES[slot_id]] += 1
        elif is_bench:
            g['days_bench'] += 1
        elif is_il:
            g['days_il'] += 1

        # Extract stats
        if is_historical:
            def get_hist_stat(col_id):
                v = row.get(str(col_id))
                try:
                    return float(v) if v is not None and v != '' else 0.0
                except:
                    return 0.0

            pa = get_hist_stat(16)
            ab = get_hist_stat(0)
            h = get_hist_stat(1)
            r = get_hist_stat(20)
            hr = get_hist_stat(5)
            rbi = get_hist_stat(21)
            sb = get_hist_stat(23)
            bb = get_hist_stat(10)
            so = get_hist_stat(27)
            d2 = get_hist_stat(3)
            d3 = get_hist_stat(4)
            tb = get_hist_stat(8)
            hbp = get_hist_stat(12)
            sf = get_hist_stat(13)
            cs = get_hist_stat(24)

            raw_ip = get_hist_stat(34) # outs
            gs = get_hist_stat(33)
            k = get_hist_stat(48)
            qs = get_hist_stat(63)
            w = get_hist_stat(53)
            l = get_hist_stat(54)
            sv = get_hist_stat(57)
            hd = get_hist_stat(60)
            er = get_hist_stat(45)
            h_all = get_hist_stat(37)
            bb_all = get_hist_stat(39)
            r_all = get_hist_stat(44)
        else:
            s = row.get('stats') or {}
            def get_daily_stat(name, col_id):
                v = s.get(name) or s.get(str(col_id))
                try:
                    return float(v) if v is not None and v != '' else 0.0
                except:
                    return 0.0

            pa = get_daily_stat('PA', 16)
            ab = get_daily_stat('AB', 0)
            h = get_daily_stat('H', 1)
            r = get_daily_stat('R', 20)
            hr = get_daily_stat('HR', 5)
            rbi = get_daily_stat('RBI', 21)
            sb = get_daily_stat('SB', 23)
            bb = get_daily_stat('BB', 10)
            so = get_daily_stat('SO', 27)
            d2 = get_daily_stat('2B', 3)
            d3 = get_daily_stat('3B', 4)
            tb = get_daily_stat('TB', 8)
            hbp = get_daily_stat('HBP', 12)
            sf = get_daily_stat('SF', 13)
            cs = get_daily_stat('CS', 24)

            raw_ip = get_daily_stat('IP_raw', 34)
            if raw_ip == 0.0:
                raw_ip = get_daily_stat('IP', 34) * 3 if get_daily_stat('IP', 34) < 50 else get_daily_stat('IP', 34)
            gs = get_daily_stat('GS', 33)
            k = get_daily_stat('K', 48)
            qs = get_daily_stat('QS', 63)
            w = get_daily_stat('W', 53)
            l = get_daily_stat('L', 54)
            sv = get_daily_stat('SV', 57)
            hd = get_daily_stat('HD', 60)
            er = get_daily_stat('ER', 45)
            h_all = get_daily_stat('H_Allowed', 37)
            bb_all = get_daily_stat('BB_Allowed', 39)
            r_all = get_daily_stat('R_Allowed', 44)

        if raw_ip > 0 or k > 0 or er > 0 or qs > 0 or sv > 0 or hd > 0 or gs > 0:
            g['is_pitcher'] = True
        if pa > 0 or ab > 0 or h > 0 or r > 0 or hr > 0 or rbi > 0 or sb > 0:
            g['is_batter'] = True

        if is_active:
            g['pa'] += int(pa)
            g['ab'] += int(ab)
            g['h'] += int(h)
            g['r'] += int(r)
            g['hr'] += int(hr)
            g['rbi'] += int(rbi)
            g['sb'] += int(sb)
            g['bb'] += int(bb)
            g['so'] += int(so)
            g['d2'] += int(d2)
            g['d3'] += int(d3)
            g['tb'] += int(tb) if tb > 0 else int(h + d2 + d3*2 + hr*3)
            g['hbp'] += int(hbp)
            g['sf'] += int(sf)
            g['cs'] += int(cs)

            g['gs'] += int(gs)
            g['ip_outs'] += int(round(raw_ip))
            g['k'] += int(k)
            g['qs'] += int(qs)
            g['w'] += int(w)
            g['l'] += int(l)
            g['sv'] += int(sv)
            g['hd'] += int(hd)
            g['er'] += int(er)
            g['h_allowed'] += int(h_all)
            g['bb_allowed'] += int(bb_all)
            g['r_allowed'] += int(r_all)
        elif is_bench:
            g['bench_pa'] += int(pa)
            g['bench_h'] += int(h)
            g['bench_r'] += int(r)
            g['bench_hr'] += int(hr)
            g['bench_rbi'] += int(rbi)
            g['bench_sb'] += int(sb)
            g['bench_ip_outs'] += int(round(raw_ip))
            g['bench_k'] += int(k)
            g['bench_qs'] += int(qs)
            g['bench_sv_hd'] += int(sv + hd)
            g['bench_er'] += int(er)

    # Format final list of records
    final_rows = []
    for g in groups.values():
        ab = g['ab']
        h = g['h']
        bb = g['bb']
        hbp = g['hbp']
        sf = g['sf']
        tb = g['tb']
        ip_outs = g['ip_outs']
        ip = round(ip_outs / 3.0, 2)
        er = g['er']
        h_all = g['h_allowed']
        bb_all = g['bb_allowed']
        k = g['k']
        gs = g['gs']
        qs = g['qs']

        # Batting rates
        avg = round(h / ab, 4) if ab > 0 else 0.0
        obp_denom = ab + bb + hbp + sf
        obp = round((h + bb + hbp) / obp_denom, 4) if obp_denom > 0 else (round((h + bb) / g['pa'], 4) if g['pa'] > 0 else 0.0)
        slg = round(tb / ab, 4) if ab > 0 else 0.0
        ops = round(obp + slg, 4)

        # Pitching rates
        era = round((er * 9.0) / (ip_outs / 3.0), 3) if ip_outs > 0 else 0.0
        whip = round((bb_all + h_all) / (ip_outs / 3.0), 3) if ip_outs > 0 else 0.0
        k_9 = round((k * 9.0) / (ip_outs / 3.0), 2) if ip_outs > 0 else 0.0
        bb_9 = round((bb_all * 9.0) / (ip_outs / 3.0), 2) if ip_outs > 0 else 0.0
        qs_pct = round((qs / gs) * 100.0, 1) if gs > 0 else 0.0

        # Position formatting: sort by frequency
        sorted_pos = sorted(g['pos_counts'].items(), key=lambda x: x[1], reverse=True)
        positions = ", ".join([p[0] for p in sorted_pos]) if sorted_pos else ("P" if g['is_pitcher'] else ("UTIL" if g['is_batter'] else "BN"))

        final_rows.append({
            'league_id': LEAGUE_ID,
            'season_year': g['season_year'],
            'team_id': g['team_id'],
            'owner_name': g['owner_name'],
            'player_id': g['player_id'],
            'player_name': g['player_name'],
            'positions': positions,
            'is_batter': g['is_batter'],
            'is_pitcher': g['is_pitcher'],
            'days_active': g['days_active'],
            'days_bench': g['days_bench'],
            'days_il': g['days_il'],
            'days_total': g['days_total'],
            # Batting
            'pa': g['pa'], 'ab': ab, 'h': h, 'r': g['r'], 'hr': g['hr'],
            'rbi': g['rbi'], 'sb': g['sb'], 'bb': bb, 'so': g['so'],
            'd2': g['d2'], 'd3': g['d3'], 'tb': tb, 'sf': sf, 'hbp': hbp, 'cs': g['cs'],
            'avg': avg, 'obp': obp, 'slg': slg, 'ops': ops,
            # Pitching
            'gs': gs, 'ip_outs': ip_outs, 'ip': ip, 'k': k, 'qs': qs,
            'w': g['w'], 'l': g['l'], 'sv': g['sv'], 'hd': g['hd'], 'sv_hd': g['sv'] + g['hd'],
            'er': er, 'h_allowed': h_all, 'bb_allowed': bb_all, 'r_allowed': g['r_allowed'],
            'era': era, 'whip': whip, 'k_9': k_9, 'bb_9': bb_9, 'qs_pct': qs_pct,
            # Bench stats
            'bench_pa': g['bench_pa'], 'bench_h': g['bench_h'], 'bench_r': g['bench_r'],
            'bench_hr': g['bench_hr'], 'bench_rbi': g['bench_rbi'], 'bench_sb': g['bench_sb'],
            'bench_ip_outs': g['bench_ip_outs'], 'bench_k': g['bench_k'],
            'bench_qs': g['bench_qs'], 'bench_sv_hd': g['bench_sv_hd'], 'bench_er': g['bench_er']
        })

    return final_rows


def main():
    start_time = time.time()
    print("=== Starting Player-Owner Flattening Pipeline ===")
    supabase = get_supabase_client()

    all_seasons_data = []

    # Available seasons: 2018 through 2026
    seasons = list(range(2018, 2027))
    for year in seasons:
        try:
            records = fetch_all_daily_records_for_year(supabase, year)
            if not records:
                print(f"[{year}] No records found. Skipping.")
                continue
            season_rows = aggregate_season_data(records, year)
            print(f"[{year}] Successfully aggregated {len(season_rows)} player-owner-season rows.")
            all_seasons_data.extend(season_rows)
        except Exception as e:
            print(f"[{year}] Error processing season: {e}")

    print(f"\nTotal aggregated records across all seasons: {len(all_seasons_data)}")

    # Save to local pre-bundled JSON
    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "data", "playerOwnerSeasonStats.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_seasons_data, f, indent=2)
    print(f"✅ Saved pre-bundled JSON to: {out_path} ({os.path.getsize(out_path) / 1024:.1f} KB)")

    # Attempt to upload to Supabase table player_owner_season_stats
    try:
        print("\nAttempting to upsert into Supabase table 'player_owner_season_stats'...")
        # Check if table exists
        test_q = supabase.table('player_owner_season_stats').select('id').limit(1).execute()
        print("Table 'player_owner_season_stats' exists. Upserting in batches...")
        batch_size = 500
        for i in range(0, len(all_seasons_data), batch_size):
            batch = all_seasons_data[i:i + batch_size]
            supabase.table('player_owner_season_stats').upsert(batch, on_conflict='league_id,season_year,team_id,player_id').execute()
            print(f"Upserted {min(i + batch_size, len(all_seasons_data))} / {len(all_seasons_data)} rows...")
        print("✅ Successfully updated Supabase table 'player_owner_season_stats'.")
    except Exception as e:
        print(f"ℹ️ Note: Supabase direct upsert skipped ({e}). Pre-bundled JSON is active and fully functional for frontend.")

    elapsed = time.time() - start_time
    print(f"\n🎉 Pipeline complete in {elapsed:.1f}s!")


if __name__ == "__main__":
    main()
