"""
pipelines/generate_keeper_calculations.py

Generates detailed multi-year keeper calculations and PR math breakdowns:
1. Benchmarks (mean and standard deviation) for all 10 categories for 2026, 2027, and 2028.
2. Player Ratings (PR) projected for each year (2026, 2027, 2028).
3. Annual ranks for each year (Rank in 2026, Rank in 2027, Rank in 2028).
4. Overall multi-year weighted blend PR (60% Y1 + 30% Y2 + 10% Y3) and Overall Rank + Price.
5. Granular per-element calculation breakdown (player value, benchmark mean/std, formula, volume adjustment, and resulting category PR).
6. Saves static JSON artifact to `src/data/keeperCalculations.json`.
"""

import os
import sys
import math
import json
import requests
from typing import Dict, List, Tuple, Any

# Supabase default credentials
DEFAULT_SUPABASE_URL = "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0."
    "wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

# Import helper functions from compute_player_values
sys.path.append(os.path.dirname(__file__))
import compute_player_values as cpv


def compute_detailed_player_pr(
    bat_stat: Dict[str, float],
    pit_stat: Dict[str, float],
    benchmarks: Dict[str, Tuple[float, float]],
    min_ab: float = 400.0,
    min_sp_ip: float = 130.0,
    min_rp_ip: float = 45.0,
) -> Tuple[float, Dict[str, float], Dict[str, Any]]:
    """Compute PR with full element-by-element formula transparency."""
    cat_prs = {
        "R": 0.0, "HR": 0.0, "RBI": 0.0, "SB": 0.0, "OBP": 0.0,
        "SO": 0.0, "QS": 0.0, "SV_HD": 0.0, "ERA": 0.0, "WHIP": 0.0
    }
    calcs = {}

    is_batter = bat_stat and float(bat_stat.get("AB", 0) or 0) > 0
    is_pitcher = pit_stat and float(pit_stat.get("IP", 0) or 0) > 0

    if is_batter:
        ab = float(bat_stat.get("AB", 0) or 0)
        for cat in ["R", "HR", "RBI", "SB"]:
            val = float(bat_stat.get(cat, 0) or 0)
            mean, std = benchmarks.get(f"BAT_{cat}", (0.0, 1.0))
            z = (val - mean) / std if std > 0 else 0.0
            cat_prs[cat] = z
            calcs[cat] = {
                "stat": cat,
                "category_type": "Batting Counting",
                "val": round(val, 2),
                "mean": round(mean, 2),
                "std": round(std, 2),
                "z_score": round(z, 2),
                "weight": 1.0,
                "formula": f"({val:.1f} - {mean:.1f}) / {std:.2f}",
                "pr": round(z, 2)
            }

        obp = float(bat_stat.get("OBP", 0) or 0)
        mean, std = benchmarks.get("BAT_OBP", (0.320, 0.025))
        z_obp = (obp - mean) / std if std > 0 else 0.0
        weight_obp = ab / min_ab
        pr_obp = z_obp * weight_obp
        cat_prs["OBP"] = pr_obp
        calcs["OBP"] = {
            "stat": "OBP",
            "category_type": "Batting Rate (Volume Weighted)",
            "val": round(obp, 4),
            "mean": round(mean, 4),
            "std": round(std, 4),
            "z_score": round(z_obp, 2),
            "ab": round(ab, 1),
            "min_ab": min_ab,
            "weight": round(weight_obp, 2),
            "formula": f"(({obp:.3f} - {mean:.3f}) / {std:.3f}) * ({ab:.0f} / {min_ab:.0f})",
            "pr": round(pr_obp, 2)
        }

    if is_pitcher:
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

        if is_sp:
            # Strikeouts (SP)
            mean_so, std_so = benchmarks.get("SP_SO", (100.0, 30.0))
            z_so = (so - mean_so) / std_so if std_so > 0 else 0.0
            cat_prs["SO"] = z_so
            calcs["SO"] = {
                "stat": "SO",
                "category_type": "SP Counting",
                "val": round(so, 1),
                "mean": round(mean_so, 1),
                "std": round(std_so, 1),
                "z_score": round(z_so, 2),
                "weight": 1.0,
                "formula": f"({so:.1f} - {mean_so:.1f}) / {std_so:.2f}",
                "pr": round(z_so, 2)
            }

            # SP Quality Starts
            mean_qs, std_qs = benchmarks.get("SP_QS", (10.0, 6.0))
            z_qs = (qs - mean_qs) / std_qs if std_qs > 0 else 0.0
            cat_prs["QS"] = z_qs
            cat_prs["SV_HD"] = 0.0
            calcs["QS"] = {
                "stat": "QS",
                "category_type": "SP Counting",
                "val": round(qs, 1),
                "mean": round(mean_qs, 1),
                "std": round(std_qs, 1),
                "z_score": round(z_qs, 2),
                "weight": 1.0,
                "formula": f"({qs:.1f} - {mean_qs:.1f}) / {std_qs:.2f}",
                "pr": round(z_qs, 2)
            }
            calcs["SV_HD"] = {
                "stat": "SV_HD",
                "category_type": "SP Role Floor",
                "val": 0.0,
                "mean": 0.0,
                "std": 1.0,
                "z_score": 0.0,
                "weight": 0.0,
                "formula": "Floored at 0.0 for SP",
                "pr": 0.0
            }

            # SP ERA (inverted: lower ERA is better)
            mean_era, std_era = benchmarks.get("SP_ERA", (4.00, 0.50))
            z_era = ((era - mean_era) / std_era) * (ip / min_sp_ip) * -1.0 if std_era > 0 else 0.0
            cat_prs["ERA"] = z_era
            calcs["ERA"] = {
                "stat": "ERA",
                "category_type": "SP Rate (Inverted & Volume Weighted)",
                "val": round(era, 2),
                "mean": round(mean_era, 2),
                "std": round(std_era, 2),
                "ip": round(ip, 1),
                "min_ip": min_sp_ip,
                "weight": round(ip / min_sp_ip, 2),
                "formula": f"(({era:.2f} - {mean_era:.2f}) / {std_era:.2f}) * ({ip:.1f} / {min_sp_ip:.0f}) * -1",
                "pr": round(z_era, 2)
            }

            # SP WHIP (inverted)
            mean_whip, std_whip = benchmarks.get("SP_WHIP", (1.25, 0.10))
            z_whip = ((whip - mean_whip) / std_whip) * (ip / min_sp_ip) * -1.0 if std_whip > 0 else 0.0
            cat_prs["WHIP"] = z_whip
            calcs["WHIP"] = {
                "stat": "WHIP",
                "category_type": "SP Rate (Inverted & Volume Weighted)",
                "val": round(whip, 2),
                "mean": round(mean_whip, 2),
                "std": round(std_whip, 2),
                "ip": round(ip, 1),
                "min_ip": min_sp_ip,
                "weight": round(ip / min_sp_ip, 2),
                "formula": f"(({whip:.2f} - {mean_whip:.2f}) / {std_whip:.2f}) * ({ip:.1f} / {min_sp_ip:.0f}) * -1",
                "pr": round(z_whip, 2)
            }
        else:
            # Relief Pitchers (RP): Nerfing factor of 2.5x applied to std dev for all RP categories
            RP_NERF = 2.5

            # RP Strikeouts (std nerfed by 2.5x)
            mean_so, raw_std_so = benchmarks.get("RP_SO", (60.0, 20.0))
            std_so = raw_std_so * RP_NERF
            z_so = (so - mean_so) / std_so if std_so > 0 else 0.0
            cat_prs["SO"] = z_so
            calcs["SO"] = {
                "stat": "SO",
                "category_type": "RP Counting (2.5x Nerfed σ)",
                "val": round(so, 1),
                "mean": round(mean_so, 1),
                "std": round(raw_std_so, 1),
                "eff_std": round(std_so, 1),
                "z_score": round(z_so, 2),
                "weight": 1.0,
                "formula": f"({so:.1f} - {mean_so:.1f}) / ({raw_std_so:.1f} * 2.5)",
                "pr": round(z_so, 2)
            }

            # RP QS floored at 0
            cat_prs["QS"] = 0.0
            calcs["QS"] = {
                "stat": "QS",
                "category_type": "RP Role Floor",
                "val": 0.0,
                "mean": 0.0,
                "std": 1.0,
                "z_score": 0.0,
                "weight": 0.0,
                "formula": "Floored at 0.0 for RP",
                "pr": 0.0
            }

            # RP SV+HD (std nerfed by 2.5x)
            mean_svhd, raw_std_svhd = benchmarks.get("RP_SVHD", (30.0, 9.0))
            std_svhd = raw_std_svhd * RP_NERF
            z_svhd = (svhd - mean_svhd) / std_svhd if std_svhd > 0 else 0.0
            cat_prs["SV_HD"] = z_svhd
            calcs["SV_HD"] = {
                "stat": "SV_HD",
                "category_type": "RP Counting (2.5x Nerfed σ)",
                "val": round(svhd, 1),
                "mean": round(mean_svhd, 1),
                "std": round(raw_std_svhd, 1),
                "eff_std": round(std_svhd, 1),
                "z_score": round(z_svhd, 2),
                "weight": 1.0,
                "formula": f"({svhd:.1f} - {mean_svhd:.1f}) / ({raw_std_svhd:.1f} * 2.5)",
                "pr": round(z_svhd, 2)
            }

            # RP ERA (inverted, volume weighted, 2.5x nerfed std)
            mean_era, raw_std_era = benchmarks.get("RP_ERA", (4.00, 1.00))
            std_era = raw_std_era * RP_NERF
            z_era = ((era - mean_era) / std_era) * (ip / min_sp_ip) * -1.0 if std_era > 0 else 0.0
            cat_prs["ERA"] = z_era
            calcs["ERA"] = {
                "stat": "ERA",
                "category_type": "RP Rate (Inverted, Vol Weighted, 2.5x Nerfed σ)",
                "val": round(era, 2),
                "mean": round(mean_era, 2),
                "std": round(raw_std_era, 2),
                "eff_std": round(std_era, 2),
                "ip": round(ip, 1),
                "weight": round(ip / min_sp_ip, 2),
                "formula": f"(({era:.2f} - {mean_era:.2f}) / ({raw_std_era:.2f} * 2.5)) * ({ip:.1f} / {min_sp_ip:.0f}) * -1",
                "pr": round(z_era, 2)
            }

            # RP WHIP (inverted, volume weighted, 2.5x nerfed std)
            mean_whip, raw_std_whip = benchmarks.get("RP_WHIP", (1.30, 0.20))
            std_whip = raw_std_whip * RP_NERF
            z_whip = ((whip - mean_whip) / std_whip) * (ip / min_sp_ip) * -1.0 if std_whip > 0 else 0.0
            cat_prs["WHIP"] = z_whip
            calcs["WHIP"] = {
                "stat": "WHIP",
                "category_type": "RP Rate (Inverted, Vol Weighted, 2.5x Nerfed σ)",
                "val": round(whip, 2),
                "mean": round(mean_whip, 2),
                "std": round(raw_std_whip, 2),
                "eff_std": round(std_whip, 2),
                "ip": round(ip, 1),
                "weight": round(ip / min_sp_ip, 2),
                "formula": f"(({whip:.2f} - {mean_whip:.2f}) / ({raw_std_whip:.2f} * 2.5)) * ({ip:.1f} / {min_sp_ip:.0f}) * -1",
                "pr": round(z_whip, 2)
            }

    total_pr = sum(cat_prs.values())
    return total_pr, cat_prs, calcs


def main():
    print("🚀 Generating detailed keeper calculations JSON...")

    supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL") or DEFAULT_SUPABASE_URL
    supabase_key = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY") or DEFAULT_SUPABASE_KEY
    headers = cpv.get_supabase_headers(supabase_key)

    # 1. Load Pricing Curve
    curve = cpv.load_pricing_curve(supabase_url, headers)
    print(f"Loaded pricing curve ({len(curve)} ranks)")

    # 2. Fetch Player Pool
    players_res = requests.get(
        f"{supabase_url}/rest/v1/player-pool?select=ESPN PlayerID,Player,Position,Team,Availability,FangraphsID,MLBAMID,ESPN Single Season Rank,ESPN Keeper Rank&limit=3000",
        headers=headers,
        timeout=15,
    )
    pool_players = players_res.json() if players_res.status_code == 200 else []
    print(f"Loaded {len(pool_players)} players from pool")

    # 3. Fetch FanGraphs data
    fg_dc_bat = cpv.fetch_live_fangraphs("bat", "fangraphsdc")
    fg_dc_pit = cpv.fetch_live_fangraphs("pit", "fangraphsdc")
    fg_zips1_bat = cpv.fetch_live_fangraphs("bat", "zipsp1")
    fg_zips1_pit = cpv.fetch_live_fangraphs("pit", "zipsp1")
    fg_zips2_bat = cpv.fetch_live_fangraphs("bat", "zipsp2")
    fg_zips2_pit = cpv.fetch_live_fangraphs("pit", "zipsp2")

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

    dc_pit_map = make_lookup(fg_dc_pit)

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

    # 1. Base 2026 Batters & Pitchers (FanGraphs Depth Charts)
    batters_keeper_2026 = {}
    for b in fg_dc_bat:
        pid = str(b.get("playerid") or "").strip()
        name = b.get("PlayerName", "")
        name_clean = name.lower().replace(".", "").replace("'", "").strip()
        item = {
            "playerid": pid, "name": name,
            "AB": float(b.get("AB", 0) or 0),
            "R": float(b.get("R", 0) or 0),
            "HR": float(b.get("HR", 0) or 0),
            "RBI": float(b.get("RBI", 0) or 0),
            "SB": float(b.get("SB", 0) or 0),
            "OBP": float(b.get("OBP", 0) or 0),
        }
        if pid: batters_keeper_2026[pid] = item
        if name_clean: batters_keeper_2026[name_clean] = item

    pitchers_keeper_2026 = {}
    for p in fg_dc_pit:
        pid = str(p.get("playerid") or "").strip()
        name = p.get("PlayerName", "")
        name_clean = name.lower().replace(".", "").replace("'", "").strip()
        sv = float(p.get("SV", 0) or 0)
        hld = float(p.get("HLD", 0) or 0)
        item = {
            "playerid": pid, "name": name,
            "IP": float(p.get("IP", 0) or 0),
            "G": float(p.get("G", 0) or 0),
            "GS": float(p.get("GS", 0) or 0),
            "SO": float(p.get("SO", 0) or 0),
            "QS": float(p.get("QS", 0) or 0),
            "SVHD": sv + hld,
            "ERA": float(p.get("ERA", 0) or 0),
            "WHIP": float(p.get("WHIP", 0) or 0),
        }
        if pid: pitchers_keeper_2026[pid] = item
        if name_clean: pitchers_keeper_2026[name_clean] = item

    # 2. Parse 2027 with proportional IP scaling for QS and SVHD from 2026
    batters_2027 = parse_zips_api_bat(fg_zips1_bat)
    pitchers_2027 = {}
    for r in fg_zips1_pit:
        pid = str(r.get("playerid") or "").strip()
        name = str(r.get("PlayerName") or "").strip()
        name_clean = name.lower().replace(".", "").replace("'", "").strip()
        ip_27 = float(r.get("IP", 0) or 0)
        gs_27 = float(r.get("GS", 0) or 0)

        p26 = pitchers_keeper_2026.get(pid) or pitchers_keeper_2026.get(name_clean, {})
        ip_26 = float(p26.get("IP", 0) or 0)
        qs_26 = float(p26.get("QS", 0) or 0)
        svhd_26 = float(p26.get("SVHD", 0) or 0)

        # Proportional IP scaling from 2026
        if ip_26 > 0:
            ip_ratio = ip_27 / ip_26
            qs_27 = round(qs_26 * ip_ratio, 1)
            svhd_27 = round(svhd_26 * ip_ratio, 1)
        elif gs_27 >= 5:
            qs_27 = round(gs_27 * 0.45, 1)
            svhd_27 = 0.0
        else:
            qs_27 = 0.0
            svhd_27 = 0.0

        item = {
            "name": name,
            "playerid": pid,
            "IP": ip_27,
            "G": float(r.get("G", 0) or 0),
            "GS": gs_27,
            "SO": float(r.get("SO", 0) or 0),
            "QS": qs_27,
            "SVHD": svhd_27,
            "ERA": float(r.get("ERA", 0) or 0),
            "WHIP": float(r.get("WHIP", 0) or 0),
        }
        if pid: pitchers_2027[pid] = item
        if name_clean: pitchers_2027[name_clean] = item

    # 3. Parse 2028 with proportional IP scaling for QS and SVHD from 2027
    batters_2028 = parse_zips_api_bat(fg_zips2_bat)
    pitchers_2028 = {}
    for r in fg_zips2_pit:
        pid = str(r.get("playerid") or "").strip()
        name = str(r.get("PlayerName") or "").strip()
        name_clean = name.lower().replace(".", "").replace("'", "").strip()
        ip_28 = float(r.get("IP", 0) or 0)
        gs_28 = float(r.get("GS", 0) or 0)

        p27 = pitchers_2027.get(pid) or pitchers_2027.get(name_clean, {})
        ip_27 = float(p27.get("IP", 0) or 0)
        p26 = pitchers_keeper_2026.get(pid) or pitchers_keeper_2026.get(name_clean, {})
        ip_26 = float(p26.get("IP", 0) or 0)

        # Proportional IP scaling: from 2027 to 2028
        if ip_27 > 0:
            ip_ratio = ip_28 / ip_27
            qs_28 = round(float(p27.get("QS", 0) or 0) * ip_ratio, 1)
            svhd_28 = round(float(p27.get("SVHD", 0) or 0) * ip_ratio, 1)
        elif ip_26 > 0:
            ip_ratio = ip_28 / ip_26
            qs_28 = round(float(p26.get("QS", 0) or 0) * ip_ratio, 1)
            svhd_28 = round(float(p26.get("SVHD", 0) or 0) * ip_ratio, 1)
        elif gs_28 >= 5:
            qs_28 = round(gs_28 * 0.45, 1)
            svhd_28 = 0.0
        else:
            qs_28 = 0.0
            svhd_28 = 0.0

        item = {
            "name": name,
            "playerid": pid,
            "IP": ip_28,
            "G": float(r.get("G", 0) or 0),
            "GS": gs_28,
            "SO": float(r.get("SO", 0) or 0),
            "QS": qs_28,
            "SVHD": svhd_28,
            "ERA": float(r.get("ERA", 0) or 0),
            "WHIP": float(r.get("WHIP", 0) or 0),
        }
        if pid: pitchers_2028[pid] = item
        if name_clean: pitchers_2028[name_clean] = item

    # Benchmarks computed with scaled stats
    benchmarks_k26 = cpv.calculate_category_benchmarks(
        list(batters_keeper_2026.values()), list(pitchers_keeper_2026.values()), min_ab=400.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_k27 = cpv.calculate_category_benchmarks(
        list(batters_2027.values()), list(pitchers_2027.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )
    benchmarks_k28 = cpv.calculate_category_benchmarks(
        list(batters_2028.values()), list(pitchers_2028.values()), min_ab=500.0, min_sp_ip=130.0, min_rp_ip=45.0
    )

    def serialize_benchmarks(b_dict, min_ab, min_sp_ip, min_rp_ip):
        return {
            "min_ab": min_ab,
            "min_sp_ip": min_sp_ip,
            "min_rp_ip": min_rp_ip,
            "rp_nerf_factor": 2.5,
            "batting": {
                "R": {"mean": round(b_dict.get("BAT_R", (0, 1))[0], 2), "std": round(b_dict.get("BAT_R", (0, 1))[1], 2)},
                "HR": {"mean": round(b_dict.get("BAT_HR", (0, 1))[0], 2), "std": round(b_dict.get("BAT_HR", (0, 1))[1], 2)},
                "RBI": {"mean": round(b_dict.get("BAT_RBI", (0, 1))[0], 2), "std": round(b_dict.get("BAT_RBI", (0, 1))[1], 2)},
                "SB": {"mean": round(b_dict.get("BAT_SB", (0, 1))[0], 2), "std": round(b_dict.get("BAT_SB", (0, 1))[1], 2)},
                "OBP": {"mean": round(b_dict.get("BAT_OBP", (0.320, 0.025))[0], 4), "std": round(b_dict.get("BAT_OBP", (0.320, 0.025))[1], 4)},
            },
            "sp": {
                "SO": {"mean": round(b_dict.get("SP_SO", (100, 30))[0], 2), "std": round(b_dict.get("SP_SO", (100, 30))[1], 2)},
                "QS": {"mean": round(b_dict.get("SP_QS", (10, 6))[0], 2), "std": round(b_dict.get("SP_QS", (10, 6))[1], 2)},
                "ERA": {"mean": round(b_dict.get("SP_ERA", (4.0, 0.5))[0], 2), "std": round(b_dict.get("SP_ERA", (4.0, 0.5))[1], 2)},
                "WHIP": {"mean": round(b_dict.get("SP_WHIP", (1.25, 0.10))[0], 2), "std": round(b_dict.get("SP_WHIP", (1.25, 0.10))[1], 2)},
            },
            "rp": {
                "SO": {"mean": round(b_dict.get("RP_SO", (60, 20))[0], 2), "std": round(b_dict.get("RP_SO", (60, 20))[1], 2), "eff_std": round(b_dict.get("RP_SO", (60, 20))[1] * 2.5, 2)},
                "SV_HD": {"mean": round(b_dict.get("RP_SVHD", (30, 9))[0], 2), "std": round(b_dict.get("RP_SVHD", (30, 9))[1], 2), "eff_std": round(b_dict.get("RP_SVHD", (30, 9))[1] * 2.5, 2)},
                "ERA": {"mean": round(b_dict.get("RP_ERA", (4.0, 1.0))[0], 2), "std": round(b_dict.get("RP_ERA", (4.0, 1.0))[1], 2), "eff_std": round(b_dict.get("RP_ERA", (4.0, 1.0))[1] * 2.5, 2)},
                "WHIP": {"mean": round(b_dict.get("RP_WHIP", (1.30, 0.20))[0], 2), "std": round(b_dict.get("RP_WHIP", (1.30, 0.20))[1], 2), "eff_std": round(b_dict.get("RP_WHIP", (1.30, 0.20))[1] * 2.5, 2)},
            }
        }

    benchmarks_data = {
        "2026": serialize_benchmarks(benchmarks_k26, 400.0, 130.0, 45.0),
        "2027": serialize_benchmarks(benchmarks_k27, 500.0, 130.0, 45.0),
        "2028": serialize_benchmarks(benchmarks_k28, 500.0, 130.0, 45.0),
    }

    # Evaluate all players
    evaluated = []
    for player in pool_players:
        espn_id = player.get("ESPN PlayerID")
        full_name = player.get("Player", "")
        fg_id = str(player.get("FangraphsID") or "").strip()
        mlbam_id = player.get("MLBAMID")
        team = str(player.get("Team") or "")
        pos = str(player.get("Position") or "")
        name_clean = full_name.lower().replace(".", "").replace("'", "").strip()

        kb26 = batters_keeper_2026.get(fg_id) or batters_keeper_2026.get(name_clean, {})
        kp26 = pitchers_keeper_2026.get(fg_id) or pitchers_keeper_2026.get(name_clean, {})
        kb27 = batters_2027.get(fg_id) or batters_2027.get(name_clean, {})
        kp27 = pitchers_2027.get(fg_id) or pitchers_2027.get(name_clean, {})
        kb28 = batters_2028.get(fg_id) or batters_2028.get(name_clean, {})
        kp28 = pitchers_2028.get(fg_id) or pitchers_2028.get(name_clean, {})

        pr_y1, cats_y1, calcs_y1 = compute_detailed_player_pr(kb26, kp26, benchmarks_k26, 400.0, 130.0, 45.0)
        pr_y2, cats_y2, calcs_y2 = compute_detailed_player_pr(kb27, kp27, benchmarks_k27, 500.0, 130.0, 45.0)
        pr_y3, cats_y3, calcs_y3 = compute_detailed_player_pr(kb28, kp28, benchmarks_k28, 500.0, 130.0, 45.0)

        # Fallbacks if future years missing
        pr_y2_eff = pr_y2 if pr_y2 != 0 else pr_y1
        pr_y3_eff = pr_y3 if pr_y3 != 0 else pr_y1
        overall_pr = (0.60 * pr_y1) + (0.30 * pr_y2_eff) + (0.10 * pr_y3_eff)

        espn_rank = parse_rank(player.get("ESPN Keeper Rank") or player.get("ESPN Single Season Rank"))
        espn_price = cpv.get_price_for_rank(curve, espn_rank)

        is_sp = False
        is_pitcher = False
        if kp26:
            is_pitcher = True
            is_sp = (float(kp26.get("GS", 0) or 0) / float(kp26.get("G", 1) or 1)) >= 0.5
        elif "SP" in pos:
            is_pitcher = True
            is_sp = True
        elif "RP" in pos:
            is_pitcher = True
            is_sp = False

        avail = player.get("Availability")
        fantasy_owner = avail.strip() if avail and avail.strip() != "Available" else "Available"
        mlbam_id_val = int(mlbam_id) if mlbam_id and str(mlbam_id).isdigit() else None
        espn_id_val = int(espn_id) if espn_id and str(espn_id).isdigit() else espn_id

        evaluated.append({
            "player_id": espn_id_val,
            "player_name": full_name,
            "team": team,
            "position": pos,
            "fantasy_owner": fantasy_owner,
            "fangraphs_id": fg_id if fg_id != "None" else None,
            "mlbam_id": mlbam_id_val,
            "MLBAMID": str(mlbam_id_val) if mlbam_id_val else None,
            "ESPN PlayerID": str(espn_id_val) if espn_id_val else None,
            "espn_player_id": espn_id_val,
            "is_pitcher": is_pitcher,
            "pitcher_role": "SP" if is_sp else ("RP" if is_pitcher else None),
            "overall_pr": round(overall_pr, 2),
            "espn_rank": espn_rank,
            "espn_price": espn_price,
            "y1": {
                "year": 2026,
                "label": "2026 (FanGraphs Depth Charts)",
                "weight": 0.60,
                "pr": round(pr_y1, 2),
                "stats": {k: round(v, 4) if isinstance(v, float) else v for k, v in {**kb26, **kp26}.items() if k not in ["name", "playerid"]},
                "category_prs": {k: round(v, 2) for k, v in cats_y1.items()},
                "calcs": calcs_y1,
            },
            "y2": {
                "year": 2027,
                "label": "2027 (ZiPS +1)",
                "weight": 0.30,
                "pr": round(pr_y2, 2),
                "effective_pr": round(pr_y2_eff, 2),
                "is_fallback": pr_y2 == 0 and pr_y1 != 0,
                "stats": {k: round(v, 4) if isinstance(v, float) else v for k, v in {**kb27, **kp27}.items() if k not in ["name", "playerid"]},
                "category_prs": {k: round(v, 2) for k, v in cats_y2.items()},
                "calcs": calcs_y2,
            },
            "y3": {
                "year": 2028,
                "label": "2028 (ZiPS +2)",
                "weight": 0.10,
                "pr": round(pr_y3, 2),
                "effective_pr": round(pr_y3_eff, 2),
                "is_fallback": pr_y3 == 0 and pr_y1 != 0,
                "stats": {k: round(v, 4) if isinstance(v, float) else v for k, v in {**kb28, **kp28}.items() if k not in ["name", "playerid"]},
                "category_prs": {k: round(v, 2) for k, v in cats_y3.items()},
                "calcs": calcs_y3,
            }
        })

    # Sort & compute Annual Ranks and Overall Rank
    # 1. Year 1 Rank
    evaluated.sort(key=lambda x: x["y1"]["pr"], reverse=True)
    for idx, p in enumerate(evaluated):
        p["y1"]["rank"] = idx + 1

    # 2. Year 2 Rank
    evaluated.sort(key=lambda x: x["y2"]["pr"], reverse=True)
    for idx, p in enumerate(evaluated):
        p["y2"]["rank"] = idx + 1

    # 3. Year 3 Rank
    evaluated.sort(key=lambda x: x["y3"]["pr"], reverse=True)
    for idx, p in enumerate(evaluated):
        p["y3"]["rank"] = idx + 1

    # 4. Overall Rank
    evaluated.sort(key=lambda x: x["overall_pr"], reverse=True)
    for idx, p in enumerate(evaluated):
        rank = idx + 1
        price = cpv.get_price_for_rank(curve, rank)
        p["overall_rank"] = rank
        p["overall_price"] = price
        p["surplus_value"] = price - p["espn_price"]

    # Filter to top 1,000 players for high performance and lightweight payload
    evaluated = evaluated[:1000]

    print(f"\n🏆 Top 5 Players by Overall Keeper PR:")
    for p in evaluated[:5]:
        print(f"  #{p['overall_rank']} {p['player_name']} (${p['overall_price']}): Overall PR={p['overall_pr']:.2f} | Y1 (#{p['y1']['rank']})={p['y1']['pr']:.2f} | Y2 (#{p['y2']['rank']})={p['y2']['pr']:.2f} | Y3 (#{p['y3']['rank']})={p['y3']['pr']:.2f}")

    output_payload = {
        "updated_at": "2026-09-30T20:20:00Z",
        "description": "Comprehensive multi-year keeper valuation calculations, benchmarks, and category PR formulas",
        "blend_weights": {
            "y1": 0.60,
            "y2": 0.30,
            "y3": 0.10,
            "formula": "0.60 * PR_2026 + 0.30 * PR_2027 + 0.10 * PR_2028"
        },
        "benchmarks": benchmarks_data,
        "players": evaluated
    }

    out_file = os.path.join(os.path.dirname(__file__), "..", "src", "data", "keeperCalculations.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2)

    print(f"💾 Successfully saved keeper calculations to {out_file} ({len(evaluated)} players, {os.path.getsize(out_file):,} bytes)")


if __name__ == "__main__":
    main()
