#!/usr/bin/env python3
"""
pipelines/update_league_context.py

Automatically refreshes data/league_context.json and LEAGUE_STORYLINES.md
by querying player_daily_stats and historical_finishes in Supabase.
Can be executed standalone, via cron/GitHub Actions, or imported by build_recap_context.py.
"""

import os
import sys
import json
import urllib.request
from collections import defaultdict
from datetime import datetime

LEAGUE_ID = 130215
YEAR = 2026

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wczdkcdqgtzlsbssogoz.supabase.co")
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

SEASON_DATES = [
  { 'weekId': 1, 'startDate': '2026-03-25', 'endDate': '2026-04-05', 'phase': 1, 'name': 'Opening Week (Trios)' },
  { 'weekId': 2, 'startDate': '2026-04-06', 'endDate': '2026-04-12', 'phase': 1, 'name': 'Week 2 (Trios)' },
  { 'weekId': 3, 'startDate': '2026-04-13', 'endDate': '2026-04-19', 'phase': 1, 'name': 'Week 3 (Trios)' },
  { 'weekId': 4, 'startDate': '2026-04-20', 'endDate': '2026-04-26', 'phase': 1, 'name': 'Week 4 (Trios)' },
  { 'weekId': 5, 'startDate': '2026-04-27', 'endDate': '2026-05-03', 'phase': 1, 'name': 'Week 5 (Trios)' },
  { 'weekId': 6, 'startDate': '2026-05-04', 'endDate': '2026-05-10', 'phase': 1, 'name': 'Week 6 (Trios)' },
  { 'weekId': 7, 'startDate': '2026-05-11', 'endDate': '2026-05-17', 'phase': 1, 'name': 'Week 7 (Trios)' },
  { 'weekId': 8, 'startDate': '2026-05-18', 'endDate': '2026-05-24', 'phase': 1, 'name': 'Week 8 (Trios)' },
  { 'weekId': 9, 'startDate': '2026-05-25', 'endDate': '2026-05-31', 'phase': 1, 'name': 'Week 9 (Trios)' },
  { 'weekId': 10, 'startDate': '2026-06-01', 'endDate': '2026-06-07', 'phase': 1, 'name': 'Week 10 (Trios)' },
  { 'weekId': 11, 'startDate': '2026-06-08', 'endDate': '2026-06-14', 'phase': 1, 'name': 'Week 11 (Trios)' },
  { 'weekId': 12, 'startDate': '2026-06-15', 'endDate': '2026-06-21', 'phase': 1, 'name': 'Week 12 (Trios Finale)' },
  { 'weekId': 13, 'startDate': '2026-06-22', 'endDate': '2026-06-28', 'phase': 2, 'name': 'Mid-Season Round 1' },
  { 'weekId': 14, 'startDate': '2026-06-29', 'endDate': '2026-07-05', 'phase': 2, 'name': 'Mid-Season Round 2' },
  { 'weekId': 15, 'startDate': '2026-07-06', 'endDate': '2026-07-19', 'phase': 3, 'name': 'Week 15 (Split Start / ASG)' },
  { 'weekId': 16, 'startDate': '2026-07-20', 'endDate': '2026-07-26', 'phase': 3, 'name': 'Week 16 (Split)' },
  { 'weekId': 17, 'startDate': '2026-07-27', 'endDate': '2026-08-02', 'phase': 3, 'name': 'Week 17 (Split)' },
  { 'weekId': 18, 'startDate': '2026-08-03', 'endDate': '2026-08-09', 'phase': 3, 'name': 'Week 18 (Split)' },
  { 'weekId': 19, 'startDate': '2026-08-10', 'endDate': '2026-08-16', 'phase': 3, 'name': 'Week 19 (Split)' },
  { 'weekId': 20, 'startDate': '2026-08-17', 'endDate': '2026-08-23', 'phase': 3, 'name': 'Week 20 (Split)' },
  { 'weekId': 21, 'startDate': '2026-08-24', 'endDate': '2026-08-30', 'phase': 3, 'name': 'Week 21 (Split)' },
  { 'weekId': 22, 'startDate': '2026-08-31', 'endDate': '2026-09-06', 'phase': 3, 'name': 'Week 22 (Split)' },
  { 'weekId': 23, 'startDate': '2026-09-07', 'endDate': '2026-09-13', 'phase': 3, 'name': 'Week 23 (Regular Season Finale)' },
  { 'weekId': 24, 'startDate': '2026-09-14', 'endDate': '2026-09-20', 'phase': 4, 'name': 'Playoff Semi-Finals' },
  { 'weekId': 25, 'startDate': '2026-09-21', 'endDate': '2026-09-27', 'phase': 4, 'name': 'Championship Week' }
]

TEAMS = {
  1: {'name': 'Anti-lock Brake Systems', 'owner': 'Tim'},
  2: {'name': 'D0ng Demolishers', 'owner': 'Adrian'},
  3: {'name': "There's Always Next Year", 'owner': 'Garrett'},
  5: {'name': 'The Silver Bullets', 'owner': 'Dan'},
  6: {'name': 'Yeah Jeets', 'owner': 'Anil'},
  8: {'name': 'Raleigh Towels', 'owner': 'Alex'},
  12: {'name': 'José Can You See', 'owner': 'Will'},
  13: {'name': 'Basement Boy', 'owner': 'Mark'},
  14: {'name': 'Maikels Secret Stuff', 'owner': 'Preston'}
}

CATS = {
    'R': {'high': True, 'rate': False},
    'HR': {'high': True, 'rate': False},
    'RBI': {'high': True, 'rate': False},
    'OBP': {'high': True, 'rate': True},
    'SB': {'high': True, 'rate': False},
    'K': {'high': True, 'rate': False},
    'QS': {'high': True, 'rate': False},
    'SV_HD': {'high': True, 'rate': False},
    'ERA': {'high': False, 'rate': True},
    'WHIP': {'high': False, 'rate': True}
}

def fetch_supabase_table(table_name: str, select: str = "*", filters: str = "") -> list:
    records = []
    limit = 5000
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table_name}?select={select}{filters}&limit={limit}&offset={offset}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as resp:
            batch = json.loads(resp.read().decode('utf-8'))
            records.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
    return records

def get_period_map():
    season_start = datetime.strptime('2026-03-25', '%Y-%m-%d')
    period_to_week = {}
    for w in SEASON_DATES:
        s = datetime.strptime(w['startDate'], '%Y-%m-%d')
        e = datetime.strptime(w['endDate'], '%Y-%m-%d')
        start_id = max(1, (s - season_start).days + 1)
        duration = (e - s).days + 1
        end_id = start_id + duration - 1
        for p in range(start_id, end_id + 1):
            period_to_week[p] = w['weekId']
    return period_to_week

def get_stat(st, key, espn_id=None):
    if key in st: return float(st[key] or 0)
    if espn_id and espn_id in st: return float(st[espn_id] or 0)
    return 0.0

def compute_rates(totals):
    obp_denom = totals['AB'] + totals['BB'] + totals['HBP'] + totals['SF']
    obp = (totals['H'] + totals['BB'] + totals['HBP']) / obp_denom if obp_denom > 0 else 0.0
    era = (totals['ER'] * 9.0) / totals['IP'] if totals['IP'] > 0 else 99.0
    whip = (totals['BB_Allowed'] + totals['H_Allowed']) / totals['IP'] if totals['IP'] > 0 else 99.0
    return {
        'R': int(totals['R']), 'HR': int(totals['HR']), 'RBI': int(totals['RBI']),
        'OBP': round(obp, 4), 'SB': int(totals['SB']), 'K': int(totals['K']), 'QS': int(totals['QS']),
        'SV_HD': int(totals['SV_HD']), 'ERA': round(era, 2), 'WHIP': round(whip, 2),
        'raw_IP': totals['IP'], 'raw_AB': totals['AB'], 'raw_H': totals['H'],
        'raw_BB': totals['BB'], 'raw_ER': totals['ER']
    }

def calculate_roto_points(team_stats_map):
    team_ids = list(team_stats_map.keys())
    roto = {tid: {'total': 0.0, 'points': {}} for tid in team_ids}
    for cat, conf in CATS.items():
        vals = [{'id': tid, 'val': team_stats_map[tid][cat]} for tid in team_ids]
        vals.sort(key=lambda x: x['val'] if conf['high'] else -x['val'])
        i = 0
        while i < len(vals):
            j = i
            while j < len(vals) and abs(vals[j]['val'] - vals[i]['val']) < 0.000001:
                j += 1
            avg_pts = sum(range(i + 1, j + 1)) / (j - i)
            for k in range(i, j):
                roto[vals[k]['id']]['points'][cat] = avg_pts
                roto[vals[k]['id']]['total'] += avg_pts
            i = j
    return roto

def compute_playoff_matchups(week_stats_by_team, week_num=24):
    """
    Computes live head-to-head category scores for Playoff Semi-Finals (Week 24)
    and Championship (Week 25).
    """
    def compare_teams(tid_a, tid_b):
        sa = week_stats_by_team.get(tid_a, {})
        sb = week_stats_by_team.get(tid_b, {})
        score_a, score_b, ties = 0, 0, 0
        cat_results = {}
        for cat, conf in CATS.items():
            va = sa.get(cat, 0)
            vb = sb.get(cat, 0)
            if va == vb:
                ties += 1
                cat_results[cat] = {"winner": "TIE", "val_a": va, "val_b": vb}
            elif (va > vb if conf['high'] else va < vb):
                score_a += 1
                cat_results[cat] = {"winner": TEAMS[tid_a]['owner'], "val_a": va, "val_b": vb}
            else:
                score_b += 1
                cat_results[cat] = {"winner": TEAMS[tid_b]['owner'], "val_a": va, "val_b": vb}
        return {
            "score_a": score_a,
            "score_b": score_b,
            "ties": ties,
            "leader": TEAMS[tid_a]['owner'] if score_a > score_b else (TEAMS[tid_b]['owner'] if score_b > score_a else "TIED"),
            "categories": cat_results
        }

    sf_a = compare_teams(5, 2)
    sf_b = compare_teams(1, 12)

    consolation_tids = [3, 8, 6, 13, 14]
    con_stats = {tid: week_stats_by_team.get(tid, {}) for tid in consolation_tids}

    return {
        "week": week_num,
        "semi_final_a": {
            "home": {"team_id": 5, "owner": "Dan", "seed": 1, "score": sf_a["score_a"]},
            "away": {"team_id": 2, "owner": "Adrian", "seed": 4, "score": sf_a["score_b"]},
            "ties": sf_a["ties"],
            "leader": sf_a["leader"],
            "category_breakdown": sf_a["categories"]
        },
        "semi_final_b": {
            "home": {"team_id": 1, "owner": "Tim", "seed": 2, "score": sf_b["score_a"]},
            "away": {"team_id": 12, "owner": "Will", "seed": 3, "score": sf_b["score_b"]},
            "ties": sf_b["ties"],
            "leader": sf_b["leader"],
            "category_breakdown": sf_b["categories"]
        },
        "consolation_stats": con_stats
    }

def refresh_context_files(save_to_disk: bool = True) -> dict:
    """Pull current stats from Supabase and rebuild league_context.json and LEAGUE_STORYLINES.md."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    context_path = os.path.join(base_dir, "data", "league_context.json")
    storylines_path = os.path.join(base_dir, "LEAGUE_STORYLINES.md")

    # Load existing context if present to preserve custom text/personas/history
    existing_context = {}
    if os.path.exists(context_path):
        try:
            with open(context_path, "r", encoding="utf-8") as f:
                existing_context = json.load(f)
        except Exception as e:
            print(f"⚠️ Could not load existing context: {e}")

    print("🔄 Pulling latest 2026 records from Supabase...")
    records = fetch_supabase_table("player_daily_stats", filters=f"&league_id=eq.{LEAGUE_ID}")
    print(f"📊 Processed {len(records)} player daily records.")

    # Historical finishes from DB
    hist_records = fetch_supabase_table("historical_finishes", filters="&order=season_year.asc")
    print(f"🏛️ Processed {len(hist_records)} historical finish rows.")

    period_to_week = get_period_map()
    valid_periods = [r.get('scoring_period_id', 0) for r in records if r.get('stats') and any(float(v or 0) > 0 for v in r['stats'].values())]
    max_period = max(valid_periods or [1])
    current_week = period_to_week.get(max_period, 24)

    team_week_raw = defaultdict(lambda: {
        'R': 0, 'HR': 0, 'RBI': 0, 'SB': 0, 'K': 0, 'QS': 0, 'SV_HD': 0,
        'H': 0, 'BB': 0, 'HBP': 0, 'AB': 0, 'SF': 0,
        'ER': 0, 'IP': 0.0, 'BB_Allowed': 0, 'H_Allowed': 0
    })

    player_stats = defaultdict(lambda: {
        'team_id': None, 'name': None, 'days_started': 0,
        'R': 0, 'HR': 0, 'RBI': 0, 'SB': 0, 'K': 0, 'QS': 0, 'SV_HD': 0,
        'H': 0, 'BB': 0, 'HBP': 0, 'AB': 0, 'SF': 0,
        'ER': 0, 'IP': 0.0, 'BB_Allowed': 0, 'H_Allowed': 0
    })

    for r in records:
        tid = r.get('team_id')
        if tid not in TEAMS or r.get('lineup_slot_id') in (16, 17):
            continue
        p = r.get('scoring_period_id', 0)
        w = period_to_week.get(p)
        if not w:
            continue
        
        st = r.get('stats', {})
        tw = team_week_raw[(tid, w)]
        
        tw['R'] += get_stat(st, 'R', '20')
        tw['HR'] += get_stat(st, 'HR', '5')
        tw['RBI'] += get_stat(st, 'RBI', '21')
        tw['SB'] += get_stat(st, 'SB', '23')
        tw['K'] += get_stat(st, 'K', '48')
        tw['QS'] += get_stat(st, 'QS', '63')
        tw['SV_HD'] += (get_stat(st, 'SV', '57') + get_stat(st, 'HD', '60'))
        tw['ER'] += get_stat(st, 'ER', '45')
        raw_ip = get_stat(st, 'IP', '34')
        tw['IP'] += raw_ip / 3.0
        tw['BB_Allowed'] += get_stat(st, 'BB_Allowed', '39')
        tw['H_Allowed'] += get_stat(st, 'H_Allowed', '37')
        
        slot = r.get('lineup_slot_id')
        is_batter = ((0 <= slot <= 12) or slot == 19)
        if is_batter:
            tw['H'] += get_stat(st, 'H', '1')
            tw['BB'] += get_stat(st, 'BB', '10')
            tw['HBP'] += get_stat(st, 'HBP', '12')
            tw['AB'] += get_stat(st, 'AB', '0')
            tw['SF'] += get_stat(st, 'SF', '13')

        pid = r.get('player_id')
        pkey = (tid, pid)
        pts = player_stats[pkey]
        pts['team_id'] = tid
        pts['name'] = r.get('full_name', 'Unknown')
        pts['days_started'] += 1
        pts['R'] += get_stat(st, 'R', '20')
        pts['HR'] += get_stat(st, 'HR', '5')
        pts['RBI'] += get_stat(st, 'RBI', '21')
        pts['SB'] += get_stat(st, 'SB', '23')
        pts['K'] += get_stat(st, 'K', '48')
        pts['QS'] += get_stat(st, 'QS', '63')
        pts['SV_HD'] += (get_stat(st, 'SV', '57') + get_stat(st, 'HD', '60'))
        pts['ER'] += get_stat(st, 'ER', '45')
        pts['IP'] += raw_ip / 3.0
        pts['BB_Allowed'] += get_stat(st, 'BB_Allowed', '39')
        pts['H_Allowed'] += get_stat(st, 'H_Allowed', '37')
        if is_batter:
            pts['H'] += get_stat(st, 'H', '1')
            pts['BB'] += get_stat(st, 'BB', '10')
            pts['HBP'] += get_stat(st, 'HBP', '12')
            pts['AB'] += get_stat(st, 'AB', '0')
            pts['SF'] += get_stat(st, 'SF', '13')

    weekly_snapshots = []
    running_totals = {tid: defaultdict(float) for tid in TEAMS}

    # Simulate up to current week
    for w_idx in range(1, min(current_week + 1, 26)):
        w_info = SEASON_DATES[w_idx - 1]
        weekly_stats = {}
        for tid in TEAMS:
            raw_w = team_week_raw[(tid, w_idx)]
            weekly_stats[tid] = compute_rates(raw_w)
            for k, v in raw_w.items():
                running_totals[tid][k] += v

        cum_stats = {tid: compute_rates(running_totals[tid]) for tid in TEAMS}
        cum_roto = calculate_roto_points(cum_stats)
        
        standings = sorted(
            [{'team_id': tid, 'owner': TEAMS[tid]['owner'], 'team_name': TEAMS[tid]['name'],
              'roto_points': cum_roto[tid]['total'], 'category_points': cum_roto[tid]['points'],
              'stats': cum_stats[tid]} for tid in TEAMS],
            key=lambda x: -x['roto_points']
        )
        for r_idx, s in enumerate(standings):
            s['rank'] = r_idx + 1

        weekly_snapshots.append({
            'week': w_idx,
            'name': w_info['name'],
            'phase': w_info['phase'],
            'startDate': w_info['startDate'],
            'endDate': w_info['endDate'],
            'standings': standings,
            'weekly_stats': weekly_stats
        })

    # Playoff live matchup scores if current_week >= 24
    playoff_data = existing_context.get("playoffs_2026", {})
    if current_week >= 24 and len(weekly_snapshots) >= 24:
        w_current_stats = weekly_snapshots[-1]['weekly_stats']
        live_playoff = compute_playoff_matchups(w_current_stats, week_num=current_week)
        playoff_data[f"live_scores_week_{current_week}"] = live_playoff
        playoff_data["current_round_week"] = current_week

    # Build updated season timeline entries
    timeline_entries = []
    existing_timeline_map = {e['week']: e for e in existing_context.get("season_timeline_weeks", [])}
    for s in weekly_snapshots:
        w = s['week']
        st = s['standings']
        t1, t2, t4 = st[0], st[1], st[3]
        
        existing_narrative = existing_timeline_map.get(w, {}).get("narrative")
        if not existing_narrative:
            if w == 24:
                existing_narrative = "Playoff Semi-Finals: #1 Dan battles #4 Adrian while #2 Tim clashes with #3 Will in head-to-head playoff competition."
            elif w == 25:
                existing_narrative = "Championship Week: The final two remaining contenders battle for the 2026 Heftystrong title."
            else:
                existing_narrative = f"Week {w} competition across all 10 roto categories."

        timeline_entries.append({
            "week": w,
            "name": s['name'],
            "phase": s['phase'],
            "start_date": s['startDate'],
            "end_date": s['endDate'],
            "leader": {"owner": t1['owner'], "points": round(t1['roto_points'], 1)},
            "second_place": {"owner": t2['owner'], "points": round(t2['roto_points'], 1)},
            "fourth_place_cutline": {"owner": t4['owner'], "points": round(t4['roto_points'], 1)},
            "standings_summary": [
                {"rank": team['rank'], "owner": team['owner'], "team_name": team['team_name'], "points": round(team['roto_points'], 1)}
                for team in st
            ],
            "narrative": existing_narrative
        })

    # Build top player anchors per team
    team_anchors = defaultdict(lambda: {'batters': [], 'pitchers': []})
    for (tid, pid), p in player_stats.items():
        if p['days_started'] < 5:
            continue
        if p['AB'] >= 50 or p['HR'] >= 5:
            denom = p['AB'] + p['BB'] + p['HBP'] + p['SF']
            obp = (p['H'] + p['BB'] + p['HBP']) / denom if denom > 0 else 0
            team_anchors[tid]['batters'].append({
                'name': p['name'], 'hr': int(p['HR']), 'rbi': int(p['RBI']), 'r': int(p['R']),
                'sb': int(p['SB']), 'obp': round(obp, 3)
            })
        if p['IP'] >= 15.0 or p['K'] >= 25 or p['QS'] >= 3:
            era = (p['ER'] * 9.0) / p['IP'] if p['IP'] > 0 else 0
            team_anchors[tid]['pitchers'].append({
                'name': p['name'], 'k': int(p['K']), 'qs': int(p['QS']), 'sv_hd': int(p['SV_HD']),
                'era': round(era, 2), 'ip': round(p['IP'], 1)
            })

    for tid in team_anchors:
        team_anchors[tid]['batters'].sort(key=lambda x: -(x['hr'] * 3 + x['rbi'] + x['r'] + x['sb'] * 2))
        team_anchors[tid]['pitchers'].sort(key=lambda x: -(x['k'] + x['qs'] * 15 + x['sv_hd'] * 8))

    # Update team profiles
    team_profiles = existing_context.get("team_profiles_2026", {})
    for tid, info in TEAMS.items():
        owner = info['owner']
        if owner in team_profiles:
            team_profiles[owner]['top_batters'] = team_anchors[tid]['batters'][:5]
            team_profiles[owner]['top_pitchers'] = team_anchors[tid]['pitchers'][:5]

    # Construct complete refreshed context dictionary
    refreshed_context = {
        "league_id": LEAGUE_ID,
        "league_name": "2026 Head to Head Heftystrong",
        "season_year": YEAR,
        "scoring_format": "10-Category Head-to-Head Each Category / Points (R, HR, RBI, OBP, SB | K, QS, SV+HD, ERA, WHIP)",
        "last_updated_scoring_period": max_period,
        "last_updated_timestamp": datetime.now().isoformat() + "Z",
        "current_week": current_week,
        "season_structure": existing_context.get("season_structure", {}),
        "season_summary_2026": existing_context.get("season_summary_2026", {}),
        "team_profiles_2026": team_profiles,
        "category_battles_2026": existing_context.get("category_battles_2026", {}),
        "season_timeline_weeks": timeline_entries,
        "playoffs_2026": playoff_data,
        "manager_personas": existing_context.get("manager_personas", {}),
        "category_battlegrounds": existing_context.get("category_battlegrounds", []),
        "season_trends_and_patterns": existing_context.get("season_trends_and_patterns", []),
        "in_season_rivalries": existing_context.get("in_season_rivalries", []),
        "historical_finishes_summary": existing_context.get("historical_finishes_summary", {})
    }

    if save_to_disk:
        with open(context_path, "w", encoding="utf-8") as f:
            json.dump(refreshed_context, f, indent=2)
        print(f"💾 Updated {context_path} (scoring period {max_period}, current week {current_week}).")

        src_context_path = os.path.join(base_dir, "src", "data", "league_context.json")
        try:
            with open(src_context_path, "w", encoding="utf-8") as f:
                json.dump(refreshed_context, f, indent=2)
            print(f"💾 Also updated frontend {src_context_path}.")
        except Exception as e:
            print(f"⚠️ Could not write to {src_context_path}: {e}")

        # Update LEAGUE_STORYLINES.md with live playoff score callout if current_week >= 24
        if current_week >= 24 and os.path.exists(storylines_path):
            update_storylines_playoff_section(storylines_path, playoff_data, current_week)

    return refreshed_context

def update_storylines_playoff_section(filepath: str, playoff_data: dict, current_week: int):
    """Dynamically appends or updates the live playoff section in LEAGUE_STORYLINES.md."""
    live_key = f"live_scores_week_{current_week}"
    live = playoff_data.get(live_key)
    if not live:
        return

    sfa = live.get("semi_final_a", {})
    sfb = live.get("semi_final_b", {})
    
    live_block = f"""
> [!IMPORTANT]
> **⚡ LIVE Playoff Semi-Finals Status (Week {current_week})**  
> *Scored Head-to-Head across all 10 categories (First to 6 wins; ties award 0.5).*
> - **Semi-Final A**: **#1 Dan** ({sfa.get('home', {}).get('score', 0)}) vs **#4 Adrian** ({sfa.get('away', {}).get('score', 0)}) — **Leader: {sfa.get('leader', 'Tied')}** ({sfa.get('ties', 0)} tied categories)
> - **Semi-Final B**: **#2 Tim** ({sfb.get('home', {}).get('score', 0)}) vs **#3 Will** ({sfb.get('away', {}).get('score', 0)}) — **Leader: {sfb.get('leader', 'Tied')}** ({sfb.get('ties', 0)} tied categories)
"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        marker = "### Phase 4: The 2026 Playoffs (Weeks 24–25)"
        if marker in content:
            if "⚡ LIVE Playoff Semi-Finals Status" in content:
                parts = content.split("> [!IMPORTANT]\n> **⚡ LIVE Playoff Semi-Finals Status")
                sub_parts = parts[1].split("\n\n- **Semi-Final A**", 1)
                new_content = parts[0] + live_block.strip() + "\n\n- **Semi-Final A**" + sub_parts[1]
            else:
                parts = content.split(marker, 1)
                new_content = parts[0] + marker + "\n" + live_block + parts[1]
            
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(new_content)
            print(f"📝 Updated live playoff status in {filepath}")
    except Exception as e:
        print(f"⚠️ Could not update storylines markdown: {e}")

if __name__ == "__main__":
    refresh_context_files(save_to_disk=True)
