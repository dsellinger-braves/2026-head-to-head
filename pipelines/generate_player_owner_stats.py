#!/usr/bin/env python3
"""
pipelines/generate_player_owner_stats.py

Flattens daily fantasy player records across all available seasons (2018–2026)
into comprehensive, 100% complete player-owner-season rows:
- 2018, 2019, 2021, 2022, 2023, 2024, 2025: Scraped directly from ESPN API daily rosters
- 2020: ESPN 2020 official season totals from leagueHistory mRoster
- 2026: Active 2026 season stats from Supabase player_daily_stats

Outputs:
1. Local static bundle: src/data/playerOwnerSeasonStats.json
2. Cached daily raw records: src/data/espn_daily_cache/{year}.json
"""

import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dotenv import load_dotenv
import requests

load_dotenv()

LEAGUE_ID = 130215

# Active Supabase config
DEFAULT_SUPABASE_URL = "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"

SUPABASE_URL = DEFAULT_SUPABASE_URL
SUPABASE_KEY = DEFAULT_SUPABASE_ANON_KEY

# ESPN API Credentials
ESPN_S2 = os.environ.get("ESPN_S2") or "AEB3MyffcIwOXwNtqVObEhOa954aWtHmClqry8K2zUkWBQqqva0%2BudusV55Y%2BzlZlmzXa7GTyF55Rw1UGwVP0P%2FF1UXzCtYm8rhXig91IEYBSArPgPVcX680OkEfJ%2Bhmd5CcPGxhsMV2c27OT29gVY%2BX4ddWyyBpwxMSZztk%2BM9vltTUGlYx1G3oz5%2BFjTDTeywbm7ESpQ0ZBulFtRQI52G9uIILPNcnPiBTewHBLSeVhmbRMdgtzf4DyQI7ondV0Vry5ABZr6wmPu8KRS8HNjZ1O8Sqkn8mmZI4oMtkc4RbK3Hp%2BIcMsFM%2FY5m1wk2ngQE%3D"
ESPN_SWID = os.environ.get("ESPN_SWID") or "{81698BB0-C05B-433A-8364-E2D2278F134D}"

COOKIES = {
    'espn_s2': ESPN_S2,
    'SWID': ESPN_SWID
}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json'
}

# Final scoring periods for historical seasons
FINAL_SPS = {
    2018: 186,
    2019: 194,
    2021: 186,
    2022: 182,
    2023: 186,
    2024: 195,
    2025: 195
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

HITTING_SLOT_IDS  = {0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 19}
PITCHING_SLOT_IDS = {13, 14, 15}


def get_owner_name(year, team_id):
    """Accurate canonical owner name mapping by season."""
    if team_id == 1: return "Tim"
    if team_id == 2: return "Adrian"
    if team_id == 3: return "Garrett"
    if team_id == 5: return "Dan"
    if team_id == 6: return "Anil"
    if team_id == 7: return "Anurag"
    if team_id == 8: return "Alex"
    if team_id == 9: return "Joe"
    if team_id == 10: return "Andrew"
    if team_id == 11:
        if year >= 2025: return "Patrick"
        if year == 2024: return "Ghost"
        return "Michael"
    if team_id == 12: return "Will"
    if team_id == 13: return "Mark"
    if team_id == 14: return "Preston"
    if team_id == 99: return "Ghost"
    return f"Team {team_id}"


CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "data", "espn_daily_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def fetch_espn_sp(year, sp):
    """Fetch all rosters and daily stats for one scoring period."""
    url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{year}/segments/0/leagues/{LEAGUE_ID}?scoringPeriodId={sp}&view=mRoster"
    try:
        r = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=15)
        if r.status_code == 200:
            return sp, r.json()
    except Exception as e:
        print(f"[{year}] Error SP {sp}: {e}")
    return sp, None


def fetch_season_from_espn(year):
    """Fetch complete daily records for a season from ESPN API (or local cache)."""
    cache_file = os.path.join(CACHE_DIR, f"{year}.json")
    if os.path.exists(cache_file):
        print(f"[{year}] Loading from cache: {cache_file}")
        with open(cache_file, "r") as f:
            return json.load(f)

    final_sp = FINAL_SPS.get(year, 185)
    print(f"[{year}] Fetching {final_sp} scoring periods from ESPN API...")
    t0 = time.time()
    
    daily_rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_espn_sp, year, sp): sp for sp in range(1, final_sp + 1)}
        done_count = 0
        for future in as_completed(futures):
            sp, data = future.result()
            if data and 'teams' in data:
                for t in data['teams']:
                    team_id = t.get('id')
                    entries = (t.get('roster') or {}).get('entries') or []
                    for e in entries:
                        if not e:
                            continue
                        slot_id = e.get('lineupSlotId')
                        pool_entry = e.get('playerPoolEntry') or {}
                        p = pool_entry.get('player') or {}
                        pid = p.get('id')
                        if not pid:
                            continue
                        pname = p.get('fullName', 'Unknown')
                        
                        # Find stats for this day
                        day_stats = {}
                        for s in (p.get('stats') or []):
                            if s and s.get('scoringPeriodId') == sp and s.get('statSplitTypeId') == 5:
                                day_stats = s.get('stats') or {}
                                break
                                
                        daily_rows.append({
                            'season_year': year,
                            'scoring_period_id': sp,
                            'team_id': team_id,
                            'player_id': pid,
                            'full_name': pname,
                            'lineup_slot_id': slot_id,
                            'stats': day_stats
                        })
            done_count += 1
            if done_count % 30 == 0 or done_count == final_sp:
                print(f"[{year}] Progress: {done_count}/{final_sp} SPs ({time.time() - t0:.1f}s)")

    print(f"[{year}] Scraped {len(daily_rows)} player-day records in {time.time() - t0:.1f}s. Saving to cache...")
    with open(cache_file, "w") as f:
        json.dump(daily_rows, f)
        
    return daily_rows


def fetch_2020_season():
    """Fetch 2020 season totals from ESPN leagueHistory mRoster."""
    cache_file = os.path.join(CACHE_DIR, "2020.json")
    if os.path.exists(cache_file):
        print("[2020] Loading from cache...")
        with open(cache_file, "r") as f:
            return json.load(f)

    print("[2020] Fetching season totals from ESPN leagueHistory...")
    url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/leagueHistory/{LEAGUE_ID}?seasonId=2020&view=mRoster&view=mTeam"
    r = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=15)
    data = r.json()[0]
    
    rows = []
    for t in (data.get('teams') or []):
        team_id = t.get('id')
        entries = (t.get('roster') or {}).get('entries') or []
        for e in entries:
            if not e:
                continue
            slot_id = e.get('lineupSlotId')
            pool_entry = e.get('playerPoolEntry') or {}
            p = pool_entry.get('player') or {}
            pid = p.get('id')
            if not pid:
                continue
            pname = p.get('fullName', 'Unknown')
            
            season_stats = {}
            for s in (p.get('stats') or []):
                if s and s.get('statSplitTypeId') == 0 and s.get('seasonId') == 2020:
                    season_stats = s.get('stats') or {}
                    break
                    
            rows.append({
                'season_year': 2020,
                'team_id': team_id,
                'player_id': pid,
                'full_name': pname,
                'lineup_slot_id': slot_id if slot_id is not None else 12,
                'stats': season_stats
            })
            
    print(f"[2020] Captured {len(rows)} player season totals. Saving cache...")
    with open(cache_file, "w") as f:
        json.dump(rows, f)
        
    return rows


def fetch_2026_supabase():
    """Fetch complete 2026 daily records from Supabase player_daily_stats."""
    from supabase import create_client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("[2026] Fetching daily stats from Supabase...")
    all_rows = []
    page_size = 1000
    page = 0
    while True:
        res = sb.table('player_daily_stats').select('player_id, team_id, full_name, lineup_slot_id, stats').eq('league_id', LEAGUE_ID).range(page * page_size, (page + 1) * page_size - 1).execute()
        if not res.data:
            break
        all_rows.extend(res.data)
        page += 1
        if page % 10 == 0:
            print(f"[2026] Fetched {len(all_rows)} rows...")
            
    print(f"[2026] Fetched total {len(all_rows)} rows from Supabase.")
    return all_rows


def aggregate_season(records, year):
    """Aggregate daily records for a season into player-owner rows."""
    groups = defaultdict(lambda: {
        'player_id': None,
        'player_name': '',
        'team_id': None,
        'owner_name': '',
        'season_year': year,
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

    is_2020 = (year == 2020)

    for row in records:
        player_id = row.get('player_id')
        player_name = row.get('full_name') or row.get('fullName') or ''
        team_id = row.get('team_id')
        slot_id = row.get('lineup_slot_id') if 'lineup_slot_id' in row else row.get('lineupSlotID')

        if not player_id or not team_id:
            continue

        key = (year, team_id, player_id)
        g = groups[key]
        g['player_id'] = player_id
        g['team_id'] = team_id
        g['owner_name'] = get_owner_name(year, team_id)
        if player_name and len(player_name) > len(g['player_name']):
            g['player_name'] = player_name

        is_bench = (slot_id == 16)
        is_il = (slot_id == 17)
        is_active = (slot_id is not None and not is_bench and not is_il)

        # Extract stats
        s = row.get('stats') or {}
        def get_stat(name, col_id):
            v = s.get(name) if name in s else s.get(str(col_id))
            try:
                return float(v) if v is not None and v != '' else 0.0
            except:
                return 0.0

        pa = get_stat('PA', 16)
        ab = get_stat('AB', 0)
        h = get_stat('H', 1)
        r = get_stat('R', 20)
        hr = get_stat('HR', 5)
        rbi = get_stat('RBI', 21)
        sb = get_stat('SB', 23)
        bb = get_stat('BB', 10)
        so = get_stat('SO', 27)
        d2 = get_stat('2B', 3)
        d3 = get_stat('3B', 4)
        tb = get_stat('TB', 8)
        hbp = get_stat('HBP', 12)
        sf = get_stat('SF', 13)
        cs = get_stat('CS', 24)

        raw_ip = get_stat('IP_raw', 34)
        if raw_ip == 0.0:
            raw_ip = get_stat('IP', 34)
        gs = get_stat('GS', 33)
        k = get_stat('K', 48)
        qs = get_stat('QS', 63)
        w = get_stat('W', 53)
        l = get_stat('L', 54)
        sv = get_stat('SV', 57)
        hd = get_stat('HD', 60)
        er = get_stat('ER', 45)
        h_all = get_stat('H_Allowed', 37)
        bb_all = get_stat('BB_Allowed', 39)
        r_all = get_stat('R_Allowed', 44)

        if is_2020:
            # 2020 season total entry
            games_est = int(round(pa / 4.0)) if pa > 0 else (int(round(raw_ip / 3.0)) if raw_ip > 0 else 20)
            g['days_total'] += max(games_est, 1)
            g['days_active'] += max(games_est, 1)
            g['pos_counts']['P' if raw_ip > 0 else 'OF'] += 1
        else:
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

        if raw_ip > 0 or k > 0 or er > 0 or qs > 0 or sv > 0 or hd > 0 or gs > 0:
            g['is_pitcher'] = True
        if pa > 0 or ab > 0 or h > 0 or r > 0 or hr > 0 or rbi > 0 or sb > 0:
            g['is_batter'] = True

        if is_active or is_2020:
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


def calculate_fantasy_points(rows):
    """Compute league standard roto-composite points for valuation."""
    for r in rows:
        pts = 0.0
        if r['is_batter']:
            pts += r['r'] * 1.0
            pts += r['hr'] * 4.0
            pts += r['rbi'] * 1.0
            pts += r['sb'] * 2.0
            pts += r['bb'] * 0.5
            pts += (r['h'] - r['hr'] - r['d2'] - r['d3']) * 0.5
            pts += r['d2'] * 1.5
            pts += r['d3'] * 2.5
        if r['is_pitcher']:
            pts += (r['ip_outs'] / 3.0) * 1.0
            pts += r['k'] * 1.0
            pts += r['qs'] * 3.0
            pts += r['w'] * 2.0
            pts += r['sv'] * 4.0
            pts += r['hd'] * 2.5
            pts -= r['er'] * 1.5
            pts -= r['bb_allowed'] * 0.5
        r['fantasy_points'] = round(pts, 1)


def main():
    start_time = time.time()
    print("=== Starting Complete ESPN Historical Player-Owner Scrape (2018–2026) ===")
    all_season_rows = []

    # 1. Historical Scrapes (2018, 2019, 2021, 2022, 2023, 2024, 2025)
    for year in [2018, 2019, 2021, 2022, 2023, 2024, 2025]:
        daily_records = fetch_season_from_espn(year)
        season_rows = aggregate_season(daily_records, year)
        print(f"[{year}] Aggregated {len(season_rows)} player-owner records.")
        all_season_rows.extend(season_rows)

    # 2. 2020 Season (ESPN Season Totals)
    records_2020 = fetch_2020_season()
    rows_2020 = aggregate_season(records_2020, 2020)
    print(f"[2020] Aggregated {len(rows_2020)} player-owner records.")
    all_season_rows.extend(rows_2020)

    # 3. 2026 Season (Supabase player_daily_stats)
    records_2026 = fetch_2026_supabase()
    rows_2026 = aggregate_season(records_2026, 2026)
    print(f"[2026] Aggregated {len(rows_2026)} player-owner records.")
    all_season_rows.extend(rows_2026)

    # 4. Calculate Fantasy Points
    calculate_fantasy_points(all_season_rows)

    # 5. Save static JSON bundle
    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "data", "playerOwnerSeasonStats.json")
    with open(output_path, "w") as f:
        json.dump(all_season_rows, f, indent=2)
        
    print(f"\n🎉 Successfully wrote {len(all_season_rows)} total records to {output_path} in {time.time() - start_time:.1f}s!")


if __name__ == "__main__":
    main()
