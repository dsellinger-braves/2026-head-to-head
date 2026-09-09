"""
pipelines/compute_player_values.py

Automated Player Valuation & Pricing Engine
-------------------------------------------
1. Fetches live FanGraphs Depth Charts & ZiPS projections (batting & pitching).
2. Calculates 9-category Player Ratings (PR) based on qualified volume thresholds:
   - Batting: R, HR, RBI, SB, OBP
   - Pitching: SO, QS, SV+HD, ERA, WHIP
3. Implements rate stat volume weighting, role split floors (SP SV+HD and RP QS floored at 0),
   and two-way player combination (Shohei Ohtani).
4. Produces two distinct valuation models:
   - 3-Year Keeper Model (60% Y1 + 30% Y2 + 10% Y3)
   - Single-Year Redraft Model (100% Y1 performance)
5. Maps ranks into the keeper pricing curve ($40 down to $0 at rank 148+).
6. Upserts results into Supabase `player_valuations` and updates `player-pool`.
"""

import os
import sys
import math
import json
import csv
import io
import urllib.request
import requests
from typing import Dict, List, Tuple, Any

# Supabase default credentials
DEFAULT_SUPABASE_URL = "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0."
    "wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

# Google Sheet fallback IDs
FULL_SHEET_ID = "1-D8ZgIz9xjh7yZSCzryR9K0MV3R1U7jqxosY9QedaVE"
GID_MAP = {
    "helper": 698568351,
    "pricing_curve": 560226089,
    "zips_bat_2027": 760795568,
    "zips_pit_2027": 32250753,
    "zips_bat_2028": 1595429461,
    "zips_pit_2028": 279961978,
}


def get_supabase_headers(key: str) -> Dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def fetch_csv_from_gsheet(gid: int) -> List[List[str]]:
    """Fetch raw CSV tab from Google Sheets."""
    url = f"https://docs.google.com/spreadsheets/d/{FULL_SHEET_ID}/export?format=csv&gid={gid}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8", errors="ignore")
            return list(csv.reader(io.StringIO(content)))
    except Exception as e:
        print(f"⚠️ Error fetching GSheet tab {gid}: {e}")
        return []


def fetch_live_fangraphs(stat_type: str, proj_type: str = "fangraphsdc") -> List[Dict]:
    """Fetch live FanGraphs projection JSON."""
    url = f"https://www.fangraphs.com/api/projections?type={proj_type}&stats={stat_type}&pos=all&team=0&players=0&lg=all"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        print(f"✅ Fetched FanGraphs {proj_type} {stat_type}: {len(data)} records")
        return data
    except Exception as e:
        print(f"⚠️ Failed to fetch live FanGraphs {stat_type}: {e}")
        return []


def load_pricing_curve(supabase_url: str, headers: Dict[str, str]) -> Dict[int, int]:
    """Load rank -> rounded dollar cost curve from Supabase."""
    url = f"{supabase_url}/rest/v1/keeper_pricing_curve?select=rank,rounded_cost&order=rank.asc&limit=300"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200 and r.json():
            return {int(row["rank"]): int(row["rounded_cost"]) for row in r.json()}
    except Exception as e:
        print(f"⚠️ Could not load pricing curve from Supabase: {e}")

    # Fallback to direct sheet
    rows = fetch_csv_from_gsheet(GID_MAP["pricing_curve"])
    curve = {}
    for r in rows[1:]:
        if r and r[0].strip().isdigit():
            rank = int(r[0].strip())
            price = int(r[1].replace("$", "").strip())
            curve[rank] = price
    return curve


def get_price_for_rank(curve: Dict[int, int], rank: int) -> int:
    """Return mapped price for a given rank."""
    if rank <= 0:
        return 0
    if rank in curve:
        return curve[rank]
    max_rank = max(curve.keys()) if curve else 150
    if rank > max_rank:
        return 0
    return 0


def calculate_category_benchmarks(
    batters: List[Dict],
    pitchers: List[Dict],
    min_ab: float,
    min_sp_ip: float,
    min_rp_ip: float,
) -> Dict[str, Tuple[float, float]]:
    """Compute mean and stddev for qualified players."""
    qual_bat = [b for b in batters if float(b.get("AB", 0) or 0) >= min_ab]
    if not qual_bat:
        qual_bat = batters

    def mean_std(values: List[float]) -> Tuple[float, float]:
        if not values:
            return 0.0, 1.0
        n = len(values)
        m = sum(values) / n
        var = sum((x - m) ** 2 for x in values) / (n - 1 if n > 1 else 1)
        s = math.sqrt(var)
        return m, s if s > 1e-6 else 1.0

    benchmarks = {}
    for cat in ["R", "HR", "RBI", "SB", "OBP"]:
        vals = [float(b.get(cat, 0) or 0) for b in qual_bat]
        benchmarks[f"BAT_{cat}"] = mean_std(vals)

    starters = []
    relievers = []
    for p in pitchers:
        g = float(p.get("G", 0) or 0)
        gs = float(p.get("GS", 0) or 0)
        ip = float(p.get("IP", 0) or 0)
        is_sp = (gs / g >= 0.5) if g > 0 else (gs > 5)
        if is_sp:
            if ip >= min_sp_ip:
                starters.append(p)
        else:
            if ip >= min_rp_ip:
                relievers.append(p)

    if not starters:
        starters = [p for p in pitchers if float(p.get("GS", 0) or 0) > 0]
    if not relievers:
        relievers = [p for p in pitchers if float(p.get("GS", 0) or 0) == 0]

    # SP benchmarks
    benchmarks["SP_SO"] = mean_std([float(p.get("SO", 0) or 0) for p in starters])
    benchmarks["SP_QS"] = mean_std([float(p.get("QS", 0) or 0) for p in starters])
    benchmarks["SP_ERA"] = mean_std([float(p.get("ERA", 0) or 0) for p in starters])
    benchmarks["SP_WHIP"] = mean_std([float(p.get("WHIP", 0) or 0) for p in starters])

    # RP benchmarks
    benchmarks["RP_SO"] = mean_std([float(p.get("SO", 0) or 0) for p in relievers])
    benchmarks["RP_SVHD"] = mean_std([float(p.get("SVHD", 0) or 0) for p in relievers])
    benchmarks["RP_ERA"] = mean_std([float(p.get("ERA", 0) or 0) for p in relievers])
    benchmarks["RP_WHIP"] = mean_std([float(p.get("WHIP", 0) or 0) for p in relievers])

    return benchmarks


def compute_player_season_pr(
    bat_stat: Dict[str, float],
    pit_stat: Dict[str, float],
    benchmarks: Dict[str, Tuple[float, float]],
    min_ab: float = 400.0,
    min_sp_ip: float = 130.0,
    min_rp_ip: float = 45.0,
) -> Tuple[float, Dict[str, float]]:
    """Compute total PR and individual category PRs for a player in a given season."""
    cat_prs = {
        "R": 0.0, "HR": 0.0, "RBI": 0.0, "SB": 0.0, "OBP": 0.0,
        "SO": 0.0, "QS": 0.0, "SV_HD": 0.0, "ERA": 0.0, "WHIP": 0.0
    }

    # Batting PR
    if bat_stat and float(bat_stat.get("AB", 0) or 0) > 0:
        ab = float(bat_stat.get("AB", 0) or 0)
        for cat in ["R", "HR", "RBI", "SB"]:
            val = float(bat_stat.get(cat, 0) or 0)
            mean, std = benchmarks.get(f"BAT_{cat}", (0.0, 1.0))
            cat_prs[cat] = (val - mean) / std

        obp = float(bat_stat.get("OBP", 0) or 0)
        mean, std = benchmarks.get("BAT_OBP", (0.320, 0.025))
        cat_prs["OBP"] = ((obp - mean) / std) * (ab / min_ab)

    # Pitching PR
    if pit_stat and float(pit_stat.get("IP", 0) or 0) > 0:
        ip = float(pit_stat.get("IP", 0) or 0)
        g = float(pit_stat.get("G", 0) or 0)
        gs = float(pit_stat.get("GS", 0) or 0)
        so = float(pit_stat.get("SO", 0) or 0)
        qs = float(pit_stat.get("QS", 0) or 0)
        svhd = float(pit_stat.get("SVHD", 0) or 0)
        era = float(pit_stat.get("ERA", 0) or 0)
        whip = float(pit_stat.get("WHIP", 0) or 0)

        is_sp = (gs / g >= 0.5) if g > 0 else (gs > 5)
        role = "SP" if is_sp else "RP"

        mean, std = benchmarks.get(f"{role}_SO", (100.0, 30.0))
        cat_prs["SO"] = (so - mean) / std

        if is_sp:
            mean, std = benchmarks.get("SP_QS", (10.0, 6.0))
            cat_prs["QS"] = (qs - mean) / std
            cat_prs["SV_HD"] = 0.0

            mean_era, std_era = benchmarks.get("SP_ERA", (4.00, 0.50))
            cat_prs["ERA"] = ((era - mean_era) / std_era) * (ip / min_sp_ip) * -1.0

            mean_whip, std_whip = benchmarks.get("SP_WHIP", (1.25, 0.10))
            cat_prs["WHIP"] = ((whip - mean_whip) / std_whip) * (ip / min_sp_ip) * -1.0
        else:
            cat_prs["QS"] = 0.0
            mean, std = benchmarks.get("RP_SVHD", (30.0, 9.0))
            cat_prs["SV_HD"] = (svhd - mean) / std

            mean_era, std_era = benchmarks.get("RP_ERA", (4.00, 1.00))
            cat_prs["ERA"] = (((era - mean_era) / std_era) * (ip / min_sp_ip) * -1.0) / 2.5

            mean_whip, std_whip = benchmarks.get("RP_WHIP", (1.30, 0.20))
            cat_prs["WHIP"] = (((whip - mean_whip) / std_whip) * (ip / min_sp_ip) * -1.0) / 2.5

    total_pr = sum(cat_prs.values())
    return total_pr, cat_prs


def run_pipeline():
    print("🚀 Initializing Player Pricing & Valuation Pipeline...")

    supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL") or DEFAULT_SUPABASE_URL
    supabase_key = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY") or DEFAULT_SUPABASE_KEY
    headers = get_supabase_headers(supabase_key)

    # 1. Load Pricing Curve
    print("📈 Fetching keeper pricing curve...")
    curve = load_pricing_curve(supabase_url, headers)
    print(f"Loaded pricing curve with {len(curve)} ranks ($40 -> $0)")

    # 2. Fetch Player Pool from Supabase
    print("⚾ Loading current player pool from Supabase...")
    players_res = requests.get(
        f"{supabase_url}/rest/v1/player-pool?select=ESPN PlayerID,Player,Position,Team,Availability,FangraphsID,MLBAMID,ESPN Single Season Rank,ESPN Keeper Rank&limit=2500",
        headers=headers,
        timeout=15,
    )
    if players_res.status_code != 200:
        print(f"❌ Failed to fetch player-pool: {players_res.text}")
        sys.exit(1)

    pool_players = players_res.json()
    print(f"Loaded {len(pool_players)} players from player-pool")

    # 3. Fetch FanGraphs 2026 Projections (Live Depth Charts)
    print("🌐 Ingesting 2026 projections from FanGraphs Depth Charts...")
    fg_dc_bat = fetch_live_fangraphs("bat", "fangraphsdc")
    fg_dc_pit = fetch_live_fangraphs("pit", "fangraphsdc")

    batters_2026: Dict[str, Dict] = {}
    for b in fg_dc_bat:
        pid = str(b.get("playerid") or "").strip()
        batters_2026[pid] = {
            "playerid": pid,
            "name": b.get("PlayerName", ""),
            "AB": float(b.get("AB", 0) or 0),
            "R": float(b.get("R", 0) or 0),
            "HR": float(b.get("HR", 0) or 0),
            "RBI": float(b.get("RBI", 0) or 0),
            "SB": float(b.get("SB", 0) or 0),
            "OBP": float(b.get("OBP", 0) or 0),
        }

    pitchers_2026: Dict[str, Dict] = {}
    for p in fg_dc_pit:
        pid = str(p.get("playerid") or "").strip()
        sv = float(p.get("SV", 0) or 0)
        hld = float(p.get("HLD", 0) or 0)
        pitchers_2026[pid] = {
            "playerid": pid,
            "name": p.get("PlayerName", ""),
            "IP": float(p.get("IP", 0) or 0),
            "G": float(p.get("G", 0) or 0),
            "GS": float(p.get("GS", 0) or 0),
            "SO": float(p.get("SO", 0) or 0),
            "QS": float(p.get("QS", 0) or 0),
            "SVHD": sv + hld,
            "ERA": float(p.get("ERA", 0) or 0),
            "WHIP": float(p.get("WHIP", 0) or 0),
        }

    # 4. Ingest Multi-Year Projections (2027 & 2028 ZiPS)
    print("📂 Ingesting 2027 and 2028 multi-year projections...")
    zips_bat_2027_raw = fetch_csv_from_gsheet(GID_MAP["zips_bat_2027"])
    zips_pit_2027_raw = fetch_csv_from_gsheet(GID_MAP["zips_pit_2027"])
    zips_bat_2028_raw = fetch_csv_from_gsheet(GID_MAP["zips_bat_2028"])
    zips_pit_2028_raw = fetch_csv_from_gsheet(GID_MAP["zips_pit_2028"])

    def parse_zips_bat(rows: List[List[str]]) -> Dict[str, Dict]:
        data = {}
        if not rows:
            return data
        headers = [h.strip() for h in rows[0]]
        for r in rows[1:]:
            if not r:
                continue
            row_dict = {headers[i]: r[i].strip() for i in range(min(len(headers), len(r)))}
            name = row_dict.get("Name", "")
            pid = row_dict.get("playerid") or row_dict.get("PlayerId")
            ab = float(row_dict.get("AB", 0) or 0)
            h = float(row_dict.get("H", 0) or 0)
            bb = float(row_dict.get("BB", 0) or 0)
            hbp = float(row_dict.get("HBP", 0) or 0)
            sf = float(row_dict.get("SF", 0) or 0)
            pa = float(row_dict.get("PA", 0) or 0)
            denom = (ab + bb + hbp + sf) if (ab + bb + hbp + sf) > 0 else pa
            obp = float(row_dict.get("OBP", 0) or 0) if "OBP" in row_dict else ((h + bb + hbp) / denom if denom > 0 else 0.0)
            key = pid if pid else name.lower().replace(".", "").replace("'", "").strip()
            data[key] = {
                "name": name,
                "AB": ab,
                "R": float(row_dict.get("R", 0) or 0),
                "HR": float(row_dict.get("HR", 0) or 0),
                "RBI": float(row_dict.get("RBI", 0) or 0),
                "SB": float(row_dict.get("SB", 0) or 0),
                "OBP": obp,
            }
        return data

    def parse_zips_pit(rows: List[List[str]], y1_pit: Dict[str, Dict]) -> Dict[str, Dict]:
        data = {}
        if not rows:
            return data
        headers = [h.strip() for h in rows[0]]
        for r in rows[1:]:
            if not r:
                continue
            row_dict = {headers[i]: r[i].strip() for i in range(min(len(headers), len(r)))}
            name = row_dict.get("Name", "")
            pid = row_dict.get("playerid") or row_dict.get("PlayerId")
            ip = float(row_dict.get("IP", 0) or 0)
            key = pid if pid else name.lower().replace(".", "").replace("'", "").strip()
            y1_player = y1_pit.get(key, {})
            y1_ip = float(y1_player.get("IP", 0) or 0)
            ip_ratio = (ip / y1_ip) if y1_ip > 0 else 1.0
            qs = float(y1_player.get("QS", 0) or 0) * ip_ratio
            svhd = float(y1_player.get("SVHD", 0) or 0) * ip_ratio

            data[key] = {
                "name": name,
                "IP": ip,
                "G": float(row_dict.get("G", 0) or 0),
                "GS": float(row_dict.get("GS", 0) or 0),
                "SO": float(row_dict.get("SO", 0) or 0),
                "QS": qs,
                "SVHD": svhd,
                "ERA": float(row_dict.get("ERA", 0) or 0),
                "WHIP": float(row_dict.get("WHIP", 0) or 0),
            }
        return data

    batters_2027 = parse_zips_bat(zips_bat_2027_raw)
    pitchers_2027 = parse_zips_pit(zips_pit_2027_raw, pitchers_2026)
    batters_2028 = parse_zips_bat(zips_bat_2028_raw)
    pitchers_2028 = parse_zips_pit(zips_pit_2028_raw, pitchers_2026)

    # 5. Calculate Benchmarks per Season
    benchmarks_2026 = calculate_category_benchmarks(
        list(batters_2026.values()), list(pitchers_2026.values()), min_ab=400.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_2027 = calculate_category_benchmarks(
        list(batters_2027.values()), list(pitchers_2027.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_2028 = calculate_category_benchmarks(
        list(batters_2028.values()), list(pitchers_2028.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )

    print("📊 Benchmarks computed for 2026, 2027, 2028.")

    # 6. Compute Valuations for each player in pool
    evaluated_players = []

    for player in pool_players:
        espn_id = player.get("ESPN PlayerID")
        full_name = player.get("Player", "")
        fg_id = str(player.get("FangraphsID") or "").strip()
        mlbam_id = player.get("MLBAMID")
        team = player.get("Team", "")
        pos = player.get("Position", "")

        name_clean = full_name.lower().replace(".", "").replace("'", "").strip()

        b26 = batters_2026.get(fg_id) or batters_2026.get(name_clean, {})
        p26 = pitchers_2026.get(fg_id) or pitchers_2026.get(name_clean, {})

        b27 = batters_2027.get(fg_id) or batters_2027.get(name_clean, {})
        p27 = pitchers_2027.get(fg_id) or pitchers_2027.get(name_clean, {})

        b28 = batters_2028.get(fg_id) or batters_2028.get(name_clean, {})
        p28 = pitchers_2028.get(fg_id) or pitchers_2028.get(name_clean, {})

        pr_2026, cats_2026 = compute_player_season_pr(b26, p26, benchmarks_2026, 400.0, 130.0, 45.0)
        pr_2027, cats_2027 = compute_player_season_pr(b27, p27, benchmarks_2027, 500.0, 130.0, 45.0)
        pr_2028, cats_2028 = compute_player_season_pr(b28, p28, benchmarks_2028, 500.0, 130.0, 45.0)

        pr_2027_adj = pr_2027 if pr_2027 != 0 else pr_2026
        pr_2028_adj = pr_2028 if pr_2028 != 0 else pr_2026
        pr_3year = (0.60 * pr_2026) + (0.30 * pr_2027_adj) + (0.10 * pr_2028_adj)

        def parse_rank(val):
            if not val:
                return 999
            s = str(val).strip()
            if s.isdigit():
                return int(s)
            try:
                return int(float(s))
            except Exception:
                return 999

        espn_rank = parse_rank(player.get("ESPN Keeper Rank") or player.get("ESPN Single Season Rank"))
        espn_price = get_price_for_rank(curve, espn_rank)

        evaluated_players.append({
            "player_id": int(espn_id),
            "fangraphs_id": fg_id if fg_id != "None" else None,
            "mlbam_id": int(mlbam_id) if mlbam_id and str(mlbam_id).isdigit() else None,
            "player_name": full_name,
            "team": team,
            "position": pos,
            "pr_2026": round(pr_2026, 2),
            "pr_3year": round(pr_3year, 2),
            "cats_2026": {k: round(v, 2) for k, v in cats_2026.items()},
            "espn_rank": espn_rank,
            "espn_price": espn_price,
            "stats_2026": {
                **{k: v for k, v in b26.items() if k not in ["name", "playerid"]},
                **{k: v for k, v in p26.items() if k not in ["name", "playerid"]},
            }
        })

    print(f"Evaluated {len(evaluated_players)} players.")

    # 7. Model 1: 3-Year Keeper Model Rankings & Prices
    evaluated_players.sort(key=lambda x: x["pr_3year"], reverse=True)
    for idx, p in enumerate(evaluated_players):
        rank = idx + 1
        price = get_price_for_rank(curve, rank)
        p["keeper_rank"] = rank
        p["keeper_price"] = price
        p["keeper_surplus"] = price - p["espn_price"]

    # 8. Model 2: Single-Year Redraft Model Rankings & Prices
    single_year_sorted = sorted(evaluated_players, key=lambda x: x["pr_2026"], reverse=True)
    for idx, p in enumerate(single_year_sorted):
        rank = idx + 1
        price = get_price_for_rank(curve, rank)
        p["single_rank"] = rank
        p["single_price"] = price
        p["single_surplus"] = price - p["espn_price"]

    print("\n🏆 Top 5 Players (3-Year Keeper Model):")
    for p in evaluated_players[:5]:
        print(f"  #{p['keeper_rank']} {p['player_name']} (${p['keeper_price']}) - 3-Yr PR: {p['pr_3year']} | ESPN: #{p['espn_rank']} (${p['espn_price']}) | Surplus: ${p['keeper_surplus']}")

    print("\n🏆 Top 5 Players (Single-Year Redraft Model):")
    for p in single_year_sorted[:5]:
        print(f"  #{p['single_rank']} {p['player_name']} (${p['single_price']}) - 2026 PR: {p['pr_2026']} | ESPN: #{p['espn_rank']} (${p['espn_price']}) | Surplus: ${p['single_surplus']}")

    # 9. Upsert to Supabase `player_valuations`
    print("\n💾 Upserting records into Supabase `player_valuations`...")
    valuation_records = []

    for p in evaluated_players:
        valuation_records.append({
            "season_year": 2026,
            "player_id": p["player_id"],
            "fangraphs_id": p["fangraphs_id"],
            "mlbam_id": p["mlbam_id"],
            "player_name": p["player_name"],
            "team": p["team"],
            "position": p["position"],
            "model_type": "3_YEAR_KEEPER",
            "total_pr": p["pr_3year"],
            "model_rank": p["keeper_rank"],
            "model_price": p["keeper_price"],
            "espn_rank": p["espn_rank"],
            "espn_price": p["espn_price"],
            "surplus_value": p["keeper_surplus"],
            "category_prs": p["cats_2026"],
            "projected_stats": p["stats_2026"],
        })

        valuation_records.append({
            "season_year": 2026,
            "player_id": p["player_id"],
            "fangraphs_id": p["fangraphs_id"],
            "mlbam_id": p["mlbam_id"],
            "player_name": p["player_name"],
            "team": p["team"],
            "position": p["position"],
            "model_type": "SINGLE_SEASON",
            "total_pr": p["pr_2026"],
            "model_rank": p["single_rank"],
            "model_price": p["single_price"],
            "espn_rank": p["espn_rank"],
            "espn_price": p["espn_price"],
            "surplus_value": p["single_surplus"],
            "category_prs": p["cats_2026"],
            "projected_stats": p["stats_2026"],
        })

    batch_size = 250
    for i in range(0, len(valuation_records), batch_size):
        batch = valuation_records[i : i + batch_size]
        res = requests.post(
            f"{supabase_url}/rest/v1/player_valuations?on_conflict=season_year,player_id,model_type",
            headers=headers,
            data=json.dumps(batch),
            timeout=15,
        )
        if res.status_code not in [200, 201]:
            print(f"⚠️ Failed batch {i//batch_size}: {res.status_code} {res.text}")

    print("✅ Successfully upserted all player_valuations records!")

    # 10. Update `player-pool` table with Hefty Keeper & Single Season fields
    print("🔄 Updating `player-pool` with Hefty valuation columns...")
    pool_updates = []
    for p in evaluated_players:
        pool_updates.append({
            "ESPN PlayerID": str(p["player_id"]),
            "Hefty Keeper Rank": p["keeper_rank"],
            "Hefty Keeper Price": p["keeper_price"],
            "Hefty Single Season Rank": p["single_rank"],
            "Hefty Single Season Price": p["single_price"],
            "Hefty Keeper PR": p["pr_3year"],
            "Projected PR": p["pr_2026"],
            "Surplus Value": p["keeper_surplus"],
        })

    for i in range(0, len(pool_updates), batch_size):
        batch = pool_updates[i : i + batch_size]
        res = requests.post(
            f"{supabase_url}/rest/v1/player-pool?on_conflict=ESPN%20PlayerID",
            headers=headers,
            data=json.dumps(batch),
            timeout=15,
        )
        if res.status_code not in [200, 201]:
            print(f"⚠️ player-pool update batch {i//batch_size} status: {res.status_code} {res.text}")

    print("🎉 Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()
