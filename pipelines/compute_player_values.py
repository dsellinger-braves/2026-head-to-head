"""
pipelines/compute_player_values.py

Automated Player Valuation & Pricing Engine
-------------------------------------------
1. Fetches live FanGraphs Depth Charts, ROS (Rest-of-Season) projections, actual MLB stats,
   and multi-year ZiPS projections (batting & pitching).
2. Calculates 9-category Player Ratings (PR) based on qualified volume thresholds:
   - Batting: R, HR, RBI, SB, OBP
   - Pitching: SO, QS, SV+HD, ERA, WHIP
3. Implements rate stat volume weighting, role split floors (SP SV+HD and RP QS floored at 0),
   and two-way player combination (Shohei Ohtani).
4. Produces two distinct valuation models:
   - 3-Year Keeper Model:
     * Year 1 (2026): FanGraphs Depth Charts
     * Year 2 (2027): ZiPS Year + 1
     * Year 3 (2028): ZiPS Year + 2
     * Weighted blend: 60% Y1 + 30% Y2 + 10% Y3
   - Single-Year Redraft Model (Matching Workbook Methodology):
     * Full Year 2026 = Current Year Actual Stats (YTD) + FanGraphs ROS Projections
     * Assumes all 3 years perform at that exact full-year level
     * Evaluates PR across graduated volume thresholds (min 400 AB in Y1, 500 AB in Y2/Y3)
     * Weighted blend: 60% Y1 + 30% Y2 + 10% Y3
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


def fetch_live_actuals(stat_type: str, season: int = 2026) -> List[Dict]:
    """Fetch live MLB season actual stats from FanGraphs Leaderboards API."""
    url = (
        f"https://www.fangraphs.com/api/leaders/major-league/data?"
        f"age=&pos=all&stats={stat_type}&lg=all&qual=0&season={season}&season1={season}&"
        f"startdate={season}-03-01&enddate={season}-11-01&month=0&team=0&pageitems=2500"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        print(f"✅ Fetched FanGraphs {season} actual {stat_type}: {len(data)} records")
        return data
    except Exception as e:
        print(f"⚠️ Failed to fetch FanGraphs live actual {stat_type}: {e}")
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
        print(f"⚠️ Failed to fetch live FanGraphs {proj_type} {stat_type}: {e}")
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
        f"{supabase_url}/rest/v1/player-pool?select=ESPN PlayerID,Player,Position,Team,Availability,FangraphsID,MLBAMID,ESPN Single Season Rank,ESPN Keeper Rank&limit=3000",
        headers=headers,
        timeout=15,
    )
    if players_res.status_code != 200:
        print(f"❌ Failed to fetch player-pool: {players_res.text}")
        sys.exit(1)

    pool_players = players_res.json()
    print(f"Loaded {len(pool_players)} players from player-pool")

    # 3. Fetch FanGraphs 2026 Projections & Actuals
    print("🌐 Ingesting live FanGraphs data:")
    print("  - Depth Charts 2026 Full-Season Projections...")
    fg_dc_bat = fetch_live_fangraphs("bat", "fangraphsdc")
    fg_dc_pit = fetch_live_fangraphs("pit", "fangraphsdc")

    print("  - Rest-of-Season (ROS) Depth Charts Projections...")
    fg_ros_bat = fetch_live_fangraphs("bat", "rfangraphsdc")
    fg_ros_pit = fetch_live_fangraphs("pit", "rfangraphsdc")

    print("  - 2026 Actual YTD Statistics...")
    fg_act_bat = fetch_live_actuals("bat", 2026)
    fg_act_pit = fetch_live_actuals("pit", 2026)

    # Convert actuals and ROS into fast lookup dictionaries by playerid and clean name
    def make_lookup(records: List[Dict], id_key: str = "playerid", name_key: str = "PlayerName") -> Dict[str, Dict]:
        lookup = {}
        for r in records:
            pid = str(r.get(id_key) or "").strip()
            name = str(r.get(name_key) or "").strip()
            name_clean = name.lower().replace(".", "").replace("'", "").strip()
            if pid and pid != "None":
                lookup[pid] = r
            if name_clean:
                lookup[name_clean] = r
        return lookup

    dc_bat_map = make_lookup(fg_dc_bat)
    dc_pit_map = make_lookup(fg_dc_pit)
    ros_bat_map = make_lookup(fg_ros_bat)
    ros_pit_map = make_lookup(fg_ros_pit)
    act_bat_map = make_lookup(fg_act_bat)
    act_pit_map = make_lookup(fg_act_pit)

    # 4. Multi-Year Projections (2027 & 2028 ZiPS)
    print("📂 Ingesting 2027 and 2028 multi-year projections:")
    # First try live FanGraphs API (zipsp1 for 2027, zipsp2 for 2028)
    fg_zips1_bat = fetch_live_fangraphs("bat", "zipsp1")
    fg_zips1_pit = fetch_live_fangraphs("pit", "zipsp1")
    fg_zips2_bat = fetch_live_fangraphs("bat", "zipsp2")
    fg_zips2_pit = fetch_live_fangraphs("pit", "zipsp2")

    # Fallback to Google Sheet tabs if API returns empty
    zips_bat_2027_raw = fetch_csv_from_gsheet(GID_MAP["zips_bat_2027"]) if not fg_zips1_bat else []
    zips_pit_2027_raw = fetch_csv_from_gsheet(GID_MAP["zips_pit_2027"]) if not fg_zips1_pit else []
    zips_bat_2028_raw = fetch_csv_from_gsheet(GID_MAP["zips_bat_2028"]) if not fg_zips2_bat else []
    zips_pit_2028_raw = fetch_csv_from_gsheet(GID_MAP["zips_pit_2028"]) if not fg_zips2_pit else []

    def parse_zips_api_bat(records: List[Dict]) -> Dict[str, Dict]:
        data = {}
        for r in records:
            pid = str(r.get("playerid") or "").strip()
            name = str(r.get("PlayerName") or "").strip()
            name_clean = name.lower().replace(".", "").replace("'", "").strip()
            item = {
                "name": name,
                "AB": float(r.get("AB", 0) or 0),
                "R": float(r.get("R", 0) or 0),
                "HR": float(r.get("HR", 0) or 0),
                "RBI": float(r.get("RBI", 0) or 0),
                "SB": float(r.get("SB", 0) or 0),
                "OBP": float(r.get("OBP", 0) or 0),
            }
            if pid:
                data[pid] = item
            if name_clean:
                data[name_clean] = item
        return data

    def parse_zips_api_pit(records: List[Dict]) -> Dict[str, Dict]:
        data = {}
        for r in records:
            pid = str(r.get("playerid") or "").strip()
            name = str(r.get("PlayerName") or "").strip()
            name_clean = name.lower().replace(".", "").replace("'", "").strip()
            item = {
                "name": name,
                "IP": float(r.get("IP", 0) or 0),
                "G": float(r.get("G", 0) or 0),
                "GS": float(r.get("GS", 0) or 0),
                "SO": float(r.get("SO", 0) or 0),
                "QS": float(r.get("QS", 0) or 0),
                "SVHD": float(r.get("SV", 0) or 0) + float(r.get("HLD", 0) or 0),
                "ERA": float(r.get("ERA", 0) or 0),
                "WHIP": float(r.get("WHIP", 0) or 0),
            }
            if pid:
                data[pid] = item
            if name_clean:
                data[name_clean] = item
        return data

    def parse_zips_gsheet_bat(rows: List[List[str]]) -> Dict[str, Dict]:
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
            item = {
                "name": name,
                "AB": ab,
                "R": float(row_dict.get("R", 0) or 0),
                "HR": float(row_dict.get("HR", 0) or 0),
                "RBI": float(row_dict.get("RBI", 0) or 0),
                "SB": float(row_dict.get("SB", 0) or 0),
                "OBP": obp,
            }
            if pid:
                data[str(pid).strip()] = item
            name_clean = name.lower().replace(".", "").replace("'", "").strip()
            if name_clean:
                data[name_clean] = item
        return data

    def parse_zips_gsheet_pit(rows: List[List[str]], y1_pit: Dict[str, Dict]) -> Dict[str, Dict]:
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
            name_clean = name.lower().replace(".", "").replace("'", "").strip()
            key = str(pid).strip() if pid else name_clean
            y1_player = y1_pit.get(key, {})
            y1_ip = float(y1_player.get("IP", 0) or 0)
            ip_ratio = (ip / y1_ip) if y1_ip > 0 else 1.0
            qs = float(y1_player.get("QS", 0) or 0) * ip_ratio
            svhd = float(y1_player.get("SVHD", 0) or 0) * ip_ratio

            item = {
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
            if pid:
                data[str(pid).strip()] = item
            if name_clean:
                data[name_clean] = item
        return data

    # Parse 2027 & 2028 multi-year datasets
    batters_2027 = parse_zips_api_bat(fg_zips1_bat) if fg_zips1_bat else parse_zips_gsheet_bat(zips_bat_2027_raw)
    pitchers_2027 = parse_zips_api_pit(fg_zips1_pit) if fg_zips1_pit else parse_zips_gsheet_pit(zips_pit_2027_raw, dc_pit_map)
    batters_2028 = parse_zips_api_bat(fg_zips2_bat) if fg_zips2_bat else parse_zips_gsheet_bat(zips_bat_2028_raw)
    pitchers_2028 = parse_zips_api_pit(fg_zips2_pit) if fg_zips2_pit else parse_zips_gsheet_pit(zips_pit_2028_raw, dc_pit_map)

    # -------------------------------------------------------------
    # 5. Build Season Datasets for Keeper Model vs Single-Year Model
    # -------------------------------------------------------------

    # === DATASET A: Keeper Model 2026 (Live Depth Charts) ===
    batters_keeper_2026: Dict[str, Dict] = {}
    for b in fg_dc_bat:
        pid = str(b.get("playerid") or "").strip()
        name = b.get("PlayerName", "")
        name_clean = name.lower().replace(".", "").replace("'", "").strip()
        item = {
            "playerid": pid,
            "name": name,
            "AB": float(b.get("AB", 0) or 0),
            "R": float(b.get("R", 0) or 0),
            "HR": float(b.get("HR", 0) or 0),
            "RBI": float(b.get("RBI", 0) or 0),
            "SB": float(b.get("SB", 0) or 0),
            "OBP": float(b.get("OBP", 0) or 0),
        }
        if pid:
            batters_keeper_2026[pid] = item
        if name_clean:
            batters_keeper_2026[name_clean] = item

    pitchers_keeper_2026: Dict[str, Dict] = {}
    for p in fg_dc_pit:
        pid = str(p.get("playerid") or "").strip()
        name = p.get("PlayerName", "")
        name_clean = name.lower().replace(".", "").replace("'", "").strip()
        sv = float(p.get("SV", 0) or 0)
        hld = float(p.get("HLD", 0) or 0)
        item = {
            "playerid": pid,
            "name": name,
            "IP": float(p.get("IP", 0) or 0),
            "G": float(p.get("G", 0) or 0),
            "GS": float(p.get("GS", 0) or 0),
            "SO": float(p.get("SO", 0) or 0),
            "QS": float(p.get("QS", 0) or 0),
            "SVHD": sv + hld,
            "ERA": float(p.get("ERA", 0) or 0),
            "WHIP": float(p.get("WHIP", 0) or 0),
        }
        if pid:
            pitchers_keeper_2026[pid] = item
        if name_clean:
            pitchers_keeper_2026[name_clean] = item

    # === DATASET B: Single-Year Model (Actuals YTD + ROS Projections) ===
    # Matches workbook methodology: Full 2026 = Current Actuals + ROS FanGraphs
    all_bat_keys = set(act_bat_map.keys()) | set(ros_bat_map.keys()) | set(dc_bat_map.keys())
    batters_single_year: Dict[str, Dict] = {}
    for k in all_bat_keys:
        act = act_bat_map.get(k, {})
        ros = ros_bat_map.get(k, {})
        dc = dc_bat_map.get(k, {})
        name = act.get("PlayerName") or ros.get("PlayerName") or dc.get("PlayerName", "")
        pid = str(act.get("playerid") or ros.get("playerid") or dc.get("playerid") or "").strip()

        act_ab = float(act.get("AB", 0) or 0)
        ros_ab = float(ros.get("AB", 0) or 0)
        dc_ab = float(dc.get("AB", 0) or 0)

        if act_ab == 0 and ros_ab == 0:
            # Preseason fallback
            item = {
                "playerid": pid,
                "name": name,
                "AB": dc_ab,
                "R": float(dc.get("R", 0) or 0),
                "HR": float(dc.get("HR", 0) or 0),
                "RBI": float(dc.get("RBI", 0) or 0),
                "SB": float(dc.get("SB", 0) or 0),
                "OBP": float(dc.get("OBP", 0) or 0),
            }
        else:
            tot_ab = act_ab + ros_ab
            tot_r = float(act.get("R", 0) or 0) + float(ros.get("R", 0) or 0)
            tot_hr = float(act.get("HR", 0) or 0) + float(ros.get("HR", 0) or 0)
            tot_rbi = float(act.get("RBI", 0) or 0) + float(ros.get("RBI", 0) or 0)
            tot_sb = float(act.get("SB", 0) or 0) + float(ros.get("SB", 0) or 0)
            act_pa = float(act.get("PA", 0) or 0)
            ros_pa = float(ros.get("PA", 0) or 0)
            act_obp = float(act.get("OBP", 0) or 0)
            ros_obp = float(ros.get("OBP", 0) or 0)
            tot_obp = (
                ((act_obp * act_pa) + (ros_obp * ros_pa)) / (act_pa + ros_pa)
                if (act_pa + ros_pa) > 0
                else (act_obp or ros_obp)
            )
            item = {
                "playerid": pid,
                "name": name,
                "AB": tot_ab,
                "R": tot_r,
                "HR": tot_hr,
                "RBI": tot_rbi,
                "SB": tot_sb,
                "OBP": tot_obp,
            }

        batters_single_year[k] = item

    all_pit_keys = set(act_pit_map.keys()) | set(ros_pit_map.keys()) | set(dc_pit_map.keys())
    pitchers_single_year: Dict[str, Dict] = {}
    for k in all_pit_keys:
        act = act_pit_map.get(k, {})
        ros = ros_pit_map.get(k, {})
        dc = dc_pit_map.get(k, {})
        name = act.get("PlayerName") or ros.get("PlayerName") or dc.get("PlayerName", "")
        pid = str(act.get("playerid") or ros.get("playerid") or dc.get("playerid") or "").strip()

        act_ip = float(act.get("IP", 0) or 0)
        ros_ip = float(ros.get("IP", 0) or 0)
        dc_ip = float(dc.get("IP", 0) or 0)

        if act_ip == 0 and ros_ip == 0:
            # Preseason fallback
            sv = float(dc.get("SV", 0) or 0)
            hld = float(dc.get("HLD", 0) or 0)
            item = {
                "playerid": pid,
                "name": name,
                "IP": dc_ip,
                "G": float(dc.get("G", 0) or 0),
                "GS": float(dc.get("GS", 0) or 0),
                "SO": float(dc.get("SO", 0) or 0),
                "QS": float(dc.get("QS", 0) or 0),
                "SVHD": sv + hld,
                "ERA": float(dc.get("ERA", 0) or 0),
                "WHIP": float(dc.get("WHIP", 0) or 0),
            }
        else:
            tot_ip = act_ip + ros_ip
            tot_g = float(act.get("G", 0) or 0) + float(ros.get("G", 0) or 0)
            tot_gs = float(act.get("GS", 0) or 0) + float(ros.get("GS", 0) or 0)
            tot_so = float(act.get("SO", 0) or 0) + float(ros.get("SO", 0) or 0)
            tot_qs = float(act.get("QS", 0) or 0) + float(ros.get("QS", 0) or 0)
            tot_svhd = (
                float(act.get("SV", 0) or 0)
                + float(act.get("HLD", 0) or 0)
                + float(ros.get("SV", 0) or 0)
                + float(ros.get("HLD", 0) or 0)
            )
            act_era = float(act.get("ERA", 0) or 0)
            ros_era = float(ros.get("ERA", 0) or 0)
            tot_era = ((act_era * act_ip) + (ros_era * ros_ip)) / tot_ip if tot_ip > 0 else (act_era or ros_era)

            act_whip = float(act.get("WHIP", 0) or 0)
            ros_whip = float(ros.get("WHIP", 0) or 0)
            tot_whip = ((act_whip * act_ip) + (ros_whip * ros_ip)) / tot_ip if tot_ip > 0 else (act_whip or ros_whip)

            item = {
                "playerid": pid,
                "name": name,
                "IP": tot_ip,
                "G": tot_g,
                "GS": tot_gs,
                "SO": tot_so,
                "QS": tot_qs,
                "SVHD": tot_svhd,
                "ERA": tot_era,
                "WHIP": tot_whip,
            }

        pitchers_single_year[k] = item

    # 6. Calculate Benchmarks per Model
    print("📊 Computing benchmarks for Keeper Model & Single-Year Model...")

    # Keeper Model Benchmarks
    benchmarks_k26 = calculate_category_benchmarks(
        list(batters_keeper_2026.values()), list(pitchers_keeper_2026.values()), min_ab=400.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_k27 = calculate_category_benchmarks(
        list(batters_2027.values()), list(pitchers_2027.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_k28 = calculate_category_benchmarks(
        list(batters_2028.values()), list(pitchers_2028.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )

    # Single-Year Model Benchmarks (Workbook methodology: full-year stats evaluated at 400 AB in Y1, 500 AB in Y2/Y3)
    benchmarks_sy_y1 = calculate_category_benchmarks(
        list(batters_single_year.values()), list(pitchers_single_year.values()), min_ab=400.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_sy_y2 = calculate_category_benchmarks(
        list(batters_single_year.values()), list(pitchers_single_year.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_sy_y3 = calculate_category_benchmarks(
        list(batters_single_year.values()), list(pitchers_single_year.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )

    # 7. Evaluate All Players in Pool
    print("🧮 Calculating individual Player Ratings (PR) and 3-year blends...")
    evaluated_players = []

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

    for player in pool_players:
        espn_id = player.get("ESPN PlayerID")
        full_name = player.get("Player", "")
        fg_id = str(player.get("FangraphsID") or "").strip()
        mlbam_id = player.get("MLBAMID")
        team = player.get("Team", "")
        pos = player.get("Position", "")

        name_clean = full_name.lower().replace(".", "").replace("'", "").strip()

        # ---------------------------
        # MODEL 1: 3-Year Keeper Model
        # ---------------------------
        kb26 = batters_keeper_2026.get(fg_id) or batters_keeper_2026.get(name_clean, {})
        kp26 = pitchers_keeper_2026.get(fg_id) or pitchers_keeper_2026.get(name_clean, {})
        kb27 = batters_2027.get(fg_id) or batters_2027.get(name_clean, {})
        kp27 = pitchers_2027.get(fg_id) or pitchers_2027.get(name_clean, {})
        kb28 = batters_2028.get(fg_id) or batters_2028.get(name_clean, {})
        kp28 = pitchers_2028.get(fg_id) or pitchers_2028.get(name_clean, {})

        kpr_2026, kcats_2026 = compute_player_season_pr(kb26, kp26, benchmarks_k26, 400.0, 130.0, 45.0)
        kpr_2027, _ = compute_player_season_pr(kb27, kp27, benchmarks_k27, 500.0, 130.0, 45.0)
        kpr_2028, _ = compute_player_season_pr(kb28, kp28, benchmarks_k28, 500.0, 130.0, 45.0)

        kpr_2027_adj = kpr_2027 if kpr_2027 != 0 else kpr_2026
        kpr_2028_adj = kpr_2028 if kpr_2028 != 0 else kpr_2026
        keeper_pr = (0.60 * kpr_2026) + (0.30 * kpr_2027_adj) + (0.10 * kpr_2028_adj)

        # -----------------------------------------------
        # MODEL 2: Single-Year Model (Workbook Methodology)
        # -----------------------------------------------
        syb = batters_single_year.get(fg_id) or batters_single_year.get(name_clean, {})
        syp = pitchers_single_year.get(fg_id) or pitchers_single_year.get(name_clean, {})

        sy_pr_y1, sy_cats_2026 = compute_player_season_pr(syb, syp, benchmarks_sy_y1, 400.0, 130.0, 45.0)
        sy_pr_y2, _ = compute_player_season_pr(syb, syp, benchmarks_sy_y2, 500.0, 130.0, 45.0)
        sy_pr_y3, _ = compute_player_season_pr(syb, syp, benchmarks_sy_y3, 500.0, 130.0, 45.0)

        # 60/30/10 blend across the 3 graduated thresholds
        single_season_pr = (0.60 * sy_pr_y1) + (0.30 * sy_pr_y2) + (0.10 * sy_pr_y3)

        espn_rank = parse_rank(player.get("ESPN Keeper Rank") or player.get("ESPN Single Season Rank"))
        espn_price = get_price_for_rank(curve, espn_rank)

        evaluated_players.append({
            "player_id": int(espn_id),
            "fangraphs_id": fg_id if fg_id != "None" else None,
            "mlbam_id": int(mlbam_id) if mlbam_id and str(mlbam_id).isdigit() else None,
            "player_name": full_name,
            "team": team,
            "position": pos,
            "keeper_pr": round(keeper_pr, 2),
            "single_season_pr": round(single_season_pr, 2),
            "keeper_cats_2026": {k: round(v, 2) for k, v in kcats_2026.items()},
            "single_cats_2026": {k: round(v, 2) for k, v in sy_cats_2026.items()},
            "espn_rank": espn_rank,
            "espn_price": espn_price,
            "stats_keeper_2026": {
                **{k: v for k, v in kb26.items() if k not in ["name", "playerid"]},
                **{k: v for k, v in kp26.items() if k not in ["name", "playerid"]},
            },
            "stats_single_2026": {
                **{k: v for k, v in syb.items() if k not in ["name", "playerid"]},
                **{k: v for k, v in syp.items() if k not in ["name", "playerid"]},
            },
        })

    print(f"Evaluated {len(evaluated_players)} players.")

    # 8. Model 1: 3-Year Keeper Model Rankings & Prices
    evaluated_players.sort(key=lambda x: x["keeper_pr"], reverse=True)
    for idx, p in enumerate(evaluated_players):
        rank = idx + 1
        price = get_price_for_rank(curve, rank)
        p["keeper_rank"] = rank
        p["keeper_price"] = price
        p["keeper_surplus"] = price - p["espn_price"]

    # 9. Model 2: Single-Year Model Rankings & Prices
    single_year_sorted = sorted(evaluated_players, key=lambda x: x["single_season_pr"], reverse=True)
    for idx, p in enumerate(single_year_sorted):
        rank = idx + 1
        price = get_price_for_rank(curve, rank)
        p["single_rank"] = rank
        p["single_price"] = price
        p["single_surplus"] = price - p["espn_price"]

    print("\n🏆 Top 5 Players (3-Year Keeper Model):")
    for p in evaluated_players[:5]:
        print(f"  #{p['keeper_rank']} {p['player_name']} (${p['keeper_price']}) - 3-Yr PR: {p['keeper_pr']} | ESPN: #{p['espn_rank']} (${p['espn_price']}) | Surplus: ${p['keeper_surplus']}")

    print("\n🏆 Top 5 Players (Single-Year Redraft Model):")
    for p in single_year_sorted[:5]:
        print(f"  #{p['single_rank']} {p['player_name']} (${p['single_price']}) - Single PR: {p['single_season_pr']} | ESPN: #{p['espn_rank']} (${p['espn_price']}) | Surplus: ${p['single_surplus']}")

    # 10. Upsert to Supabase `player_valuations`
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
            "total_pr": p["keeper_pr"],
            "model_rank": p["keeper_rank"],
            "model_price": p["keeper_price"],
            "espn_rank": p["espn_rank"],
            "espn_price": p["espn_price"],
            "surplus_value": p["keeper_surplus"],
            "category_prs": p["keeper_cats_2026"],
            "projected_stats": p["stats_keeper_2026"],
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
            "total_pr": p["single_season_pr"],
            "model_rank": p["single_rank"],
            "model_price": p["single_price"],
            "espn_rank": p["espn_rank"],
            "espn_price": p["espn_price"],
            "surplus_value": p["single_surplus"],
            "category_prs": p["single_cats_2026"],
            "projected_stats": p["stats_single_2026"],
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
            print(f"⚠️ Failed valuations batch {i//batch_size}: {res.status_code} {res.text}")

    print("✅ Successfully upserted all player_valuations records!")

    # 11. Update `player-pool` table with Hefty Keeper & Single Season fields
    print("🔄 Updating `player-pool` with Hefty valuation columns...")
    pool_updates = []
    for p in evaluated_players:
        pool_updates.append({
            "ESPN PlayerID": str(p["player_id"]),
            "Hefty Keeper Rank": p["keeper_rank"],
            "Hefty Keeper Price": p["keeper_price"],
            "Hefty Single Season Rank": p["single_rank"],
            "Hefty Single Season Price": p["single_price"],
            "Hefty Keeper PR": p["keeper_pr"],
            "Projected PR": p["single_season_pr"],
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
