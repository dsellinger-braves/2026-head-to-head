#!/usr/bin/env python3
"""
pipelines/ingest_historical_trades.py

Ingests multi-year trades from commissioner Google Sheets:
- 2024: https://docs.google.com/spreadsheets/d/1l6PWOwlFy_HV7o0N6iCS4892BRCnUUeLnTDbO-965NA/edit?gid=698281952#gid=698281952
- 2025: https://docs.google.com/spreadsheets/d/1HqapcZXGV5jPnlsNIi4wWAFj6WjVMlKNf62XTtPtlvM/edit?gid=698281952#gid=698281952
- 2026: https://docs.google.com/spreadsheets/d/1_Nlv1mr8fBHSc0XBlV_u5GgfYhA9ttp7TRYC_ThqSW0/edit?gid=698281952#gid=698281952

Normalizes multi-asset trades (players, picks, keeper budget),
correlates ESPN player IDs, enriches traded picks with actual historical drafted players,
attaches in-season statistics to all assets, and generates `src/data/historicalTrades.json`.
"""

import urllib.request
import csv
import io
import json
import re
import os
import datetime
import unicodedata
from collections import defaultdict

GCS_DRAFT_HISTORY_URL = "https://storage.googleapis.com/fantasy-draft-2026/draft-history.json"
SUPABASE_URL = os.getenv("VITE_SUPABASE_URL", "https://wczdkcdqgtzlsbssogoz.supabase.co")
SUPABASE_KEY = os.getenv(
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
    "Preston": 14,
    "Joe": 10,       # Historical owner in 2024/2025
    "Joseph": 10,
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
    "brandon woodruff": 37515,
    "grayson rodriguez": 41285,
    "gunnar henderson": 42507,
    "michael harris ii": 42426,
    "randy arozarena": 36563,
    "kevin gausman": 32667,
    "gerrit cole": 32081,
    "max fried": 32685,
    "corbin carroll": 42404,
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
    "bryan rocchio": 41217,
    "luke keaschall": 5129618,
    "ketel marte": 32512,
    "josh naylor": 35066,
    "drew rasmussen": 42584,
    "matt olson": 32767,
    "dylan crews": 4719511,
    "jared jones": 4918156,
    "sandy alcantara": 35241,
    "jesus luzardo": 39667,
    "trevor larnach": 41205,
    "miguel vargas": 42453,
    "josh hader": 32760,
    "tyron guerrero": 33816,
    "trey yesavage": 4949041,
    "vladimir guerrero jr.": 35002,
    "vladimir guerrero jr": 35002,
    "chase delauter": 4619649,
    "aroldis chapman": 30442,
    "ceddanne rafaela": 4987382,
    "sonny gray": 32082,
    "keider montero": 5182933,
    "max clark": 5148964,
    "munetaka murakami": 4872595,
    "ivan herrera": 41889,
    "salvador perez": 31127,
    "joe ryan": 42450,
    "riley o'brien": 41509,
    "riley obrien": 41509,
    "mookie betts": 33039,
    "aaron judge": 33192,
    "shane mcclanahan": 41199,
    "andres munoz": 40939,
    "jacob misiorowski": 5080761,
    "travis bazzana": 5007707,
    "seiya suzuki": 4142424,
    "jordan walker": 4684778,
    "bryan reynolds": 38980,
    "xavier edwards": 41326,
    "jonathan india": 41171,
    "cristopher sanchez": 42359,
    "heliot ramos": 39642,
    "corey seager": 32691,
    "konnor griffin": 5218285,
    "tommy edman": 39907,
    "manny machado": 31097,
    "pablo lopez": 39671,
    "cole ragans": 41054,
    "logan gilbert": 41221,
    "logan webb": 41216,
    "bryan woo": 4629089,
    "yordan alvarez": 36018
}


def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    clean = "".join([c for c in nfkd if not unicodedata.combining(c)])
    clean = re.sub(r"[.,'-]", "", clean)
    clean = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s+", " ", clean).strip().lower()
    return clean


def parse_date(date_str, season_year):
    date_str = date_str.strip()
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
    
    if "worst" in name_lower:
        max_rounds = 28 if season_year >= 2026 else 24
        if "2nd" in name_lower or "second" in name_lower:
            rd = max_rounds - 1
        else:
            rd = max_rounds
        return (rd - 1) * 10 + 5

    m_round = re.search(r"(\d+)(?:st|nd|rd|th)?\s*round", name_lower)
    if m_round:
        rd = int(m_round.group(1))
        return (rd - 1) * 10 + 5

    digits = re.findall(r"\d+", name_lower)
    if digits:
        val = int(digits[0])
        if "round" in name_lower and val <= 35:
            return (val - 1) * 10 + 5
        return val
    return None


def parse_budget_amount(asset_name):
    clean = re.sub(r"[^\d.]", "", str(asset_name))
    try:
        return float(clean)
    except ValueError:
        return 0.0


def fetch_sheet_rows(sheet_id, gid):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"Fetching sheet {sheet_id} (gid {gid})...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        content = resp.read().decode("utf-8")
    return list(csv.reader(io.StringIO(content)))


def extract_stats_from_draft_record(d: dict) -> dict:
    """Extract and normalize counting stats from draft-history.json record."""
    ip_outs = float(d.get("IP") or 0)
    k = float(d.get("K") or 0)
    qs = float(d.get("QS") or 0)
    sv = float(d.get("SV") or 0)
    hd = float(d.get("HD") or 0)
    er = float(d.get("ER") or 0)

    ab = float(d.get("AB") or 0)
    h = float(d.get("H") or 0)
    bb = float(d.get("BB") or 0)
    r = float(d.get("R") or 0)
    hr = float(d.get("HR") or 0)
    rbi = float(d.get("RBI") or 0)
    sb = float(d.get("SB") or 0)
    pa = float(d.get("PA") or (ab + bb))
    obp_val = float(d.get("OBP") or ((h + bb) / (ab + bb) if (ab + bb) > 0 else 0.0))

    is_pitcher = (ip_outs > 0 or k > 0 or sv > 0)
    ip_dec = round(ip_outs / 3.0, 1)

    return {
        "is_pitcher": is_pitcher,
        "PA": pa,
        "AB": ab,
        "H": h,
        "R": r,
        "HR": hr,
        "RBI": rbi,
        "SB": sb,
        "BB": bb,
        "OBP": round(obp_val, 3),
        "IP": ip_dec,
        "ER": er,
        "K": k,
        "QS": qs,
        "SV": sv,
        "HD": hd
    }


def load_draft_history():
    print(f"🌐 Fetching draft history from {GCS_DRAFT_HISTORY_URL}...")
    req = urllib.request.Request(GCS_DRAFT_HISTORY_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    picks_by_year_pick = {}
    players_by_year_name = {}

    for r in data:
        try:
            yr = int(r.get("Year") or r.get("year"))
            pk = int(r.get("Pick_Overall") or r.get("overall_pick") or 0)
            pname = r.get("Player_Name") or r.get("Player") or ""
            cname = normalize_name(pname)
            if yr and pk:
                picks_by_year_pick[(yr, pk)] = r
            if yr and cname:
                players_by_year_name[(yr, cname)] = r
        except (ValueError, TypeError):
            continue

    print(f"✅ Indexed {len(picks_by_year_pick)} draft picks and {len(players_by_year_name)} player seasons")
    return picks_by_year_pick, players_by_year_name


SEASON_START_2026 = datetime.date(2026, 3, 25)


def load_2026_daily_records():
    """Fetch 2026 daily player statistics from Supabase player_daily_stats for all traded players."""
    print("🌐 Fetching 2026 player daily records from Supabase...")
    records_by_player_team = defaultdict(list)
    try:
        target_pids = sorted(list(set(PLAYER_ID_MAP.values())))
        chunk_size = 25
        all_data = []

        for i in range(0, len(target_pids), chunk_size):
            chunk = target_pids[i:i + chunk_size]
            pids_param = ",".join(str(p) for p in chunk)
            offset, page_size = 0, 1000

            while True:
                url = f"{SUPABASE_URL}/rest/v1/player_daily_stats?select=player_id,full_name,team_id,scoring_period_id,stats&player_id=in.({pids_param})&offset={offset}&limit={page_size}"
                req = urllib.request.Request(url, headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    batch = json.loads(resp.read().decode("utf-8"))
                all_data.extend(batch)
                if len(batch) < page_size:
                    break
                offset += page_size

        for r in all_data:
            pid = r.get("player_id")
            tid = r.get("team_id")
            if pid and tid is not None:
                records_by_player_team[(pid, tid)].append(r)

        print(f"✅ Loaded {len(all_data)} daily records across {len(records_by_player_team)} player-team combinations")
    except Exception as e:
        print(f"⚠️ Could not load 2026 daily records from Supabase: {e}")

    return records_by_player_team


def compute_post_trade_stats(pid, to_team_id, trade_date_str, records_by_player_team):
    """Compute player statistics accumulated strictly on the receiving team AFTER the trade took place."""
    try:
        t_dt = datetime.datetime.strptime(trade_date_str, "%Y-%m-%d").date()
    except Exception:
        t_dt = SEASON_START_2026

    # If traded pre-season or on opening day, count from period 1
    if t_dt <= SEASON_START_2026:
        min_sp = 1
    else:
        min_sp = max(1, (t_dt - SEASON_START_2026).days + 1)

    team_recs = records_by_player_team.get((pid, to_team_id), [])
    post_recs = [r for r in team_recs if r.get("scoring_period_id", 0) >= min_sp]

    ab, h, r, hr, rbi, sb, bb = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    ip, k, er, qs, sv, hd = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    is_pitcher = False

    for rec in post_recs:
        st = rec.get("stats") or {}
        if isinstance(st, str):
            try:
                st = json.loads(st)
            except Exception:
                st = {}

        ip_raw = float(st.get("34", st.get("IP_raw", st.get("IP", 0))))
        k_val = float(st.get("48", st.get("K", 0)))
        sv_val = float(st.get("57", st.get("SV", 0)))
        hd_val = float(st.get("60", st.get("HD", 0)))
        er_val = float(st.get("45", st.get("ER", 0)))
        qs_val = float(st.get("63", st.get("QS", 0)))

        ab_val = float(st.get("0", st.get("AB", 0)))
        h_val = float(st.get("1", st.get("H", 0)))
        hr_val = float(st.get("5", st.get("HR", 0)))
        r_val = float(st.get("20", st.get("R", 0)))
        rbi_val = float(st.get("21", st.get("RBI", 0)))
        sb_val = float(st.get("23", st.get("SB", 0)))
        bb_val = float(st.get("10", st.get("BB", 0)))

        # ESPN '34' encodes total outs pitched
        cur_ip = ip_raw / 3.0
        if cur_ip > 0 or k_val > 0 or sv_val > 0:
            is_pitcher = True

        ab += ab_val
        h += h_val
        r += r_val
        hr += hr_val
        rbi += rbi_val
        sb += sb_val
        bb += bb_val
        ip += cur_ip
        k += k_val
        er += er_val
        qs += qs_val
        sv += sv_val
        hd += hd_val

    pa = ab + bb
    obp = round((h + bb) / pa, 3) if pa > 0 else 0.0

    return {
        "is_pitcher": is_pitcher,
        "games_post_trade": len(post_recs),
        "PA": round(pa, 0),
        "AB": round(ab, 0),
        "H": round(h, 0),
        "R": round(r, 0),
        "HR": round(hr, 0),
        "RBI": round(rbi, 0),
        "SB": round(sb, 0),
        "BB": round(bb, 0),
        "OBP": obp,
        "IP": round(ip, 1),
        "ER": round(er, 0),
        "K": round(k, 0),
        "QS": round(qs, 0),
        "SV": round(sv, 0),
        "HD": round(hd, 0)
    }


def process_sheet(sheet_conf, picks_by_year_pick, players_by_year_name, stats_2026_map):
    season_year = sheet_conf["season_year"]
    rows = fetch_sheet_rows(sheet_conf["sheet_id"], sheet_conf["gid"])
    
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
        
        asset_type_norm = asset_type.title()
        if "pick" in asset_type.lower() or "round" in asset_type.lower():
            asset_type_norm = "Pick"
        elif "budget" in asset_type.lower():
            asset_type_norm = "Budget"
        elif "player" in asset_type.lower():
            asset_type_norm = "Player"

        pick_number = None
        round_number = None
        budget_amount = 0.0
        espn_player_id = None
        drafted_player = None
        stats = None

        if asset_type_norm == "Pick":
            pick_number = parse_overall_pick_number(asset_name, season_year)
            if pick_number:
                round_number = ((pick_number - 1) // 10) + 1
                # Cross-reference with draft history
                pick_match = picks_by_year_pick.get((season_year, pick_number))
                if pick_match:
                    drafted_player = {
                        "player_name": pick_match.get("Player_Name") or pick_match.get("Player"),
                        "player_id": int(pick_match["player_id"]) if pick_match.get("player_id") else None,
                        "team_owner": pick_match.get("Team_ID") or pick_match.get("Owner"),
                        "adp": float(pick_match["ADP"]) if pick_match.get("ADP") else None,
                        "is_keeper": str(pick_match.get("Keeper", "")).lower() == "true",
                        "stats": extract_stats_from_draft_record(pick_match)
                    }

        elif asset_type_norm == "Budget":
            budget_amount = parse_budget_amount(asset_name)

        elif asset_type_norm == "Player":
            p_clean = asset_name.strip().lower()
            espn_player_id = PLAYER_ID_MAP.get(p_clean)
            cname = normalize_name(asset_name)

            to_tid = OWNER_TO_TEAM_ID.get(receiving)

            if season_year in (2024, 2025):
                player_match = players_by_year_name.get((season_year, cname))
                if player_match:
                    stats = extract_stats_from_draft_record(player_match)
                else:
                    # Player was injured/DNP all season (e.g. Brandon Woodruff in 2024)
                    stats = {
                        "is_pitcher": True if "woodruff" in cname else False,
                        "PA": 0.0, "AB": 0.0, "H": 0.0, "R": 0.0, "HR": 0.0, "RBI": 0.0, "SB": 0.0, "BB": 0.0, "OBP": 0.0,
                        "IP": 0.0, "ER": 0.0, "K": 0.0, "QS": 0.0, "SV": 0.0, "HD": 0.0,
                        "games_post_trade": 0
                    }
            elif season_year == 2026 and espn_player_id:
                stats = compute_post_trade_stats(espn_player_id, to_tid, trade_date, stats_2026_map)

        item_dict = {
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
        }

        if drafted_player:
            item_dict["drafted_player"] = drafted_player
        if stats:
            item_dict["stats"] = stats

        raw_items.append(item_dict)

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


def compute_historical_post_trade_stats(player_id, to_team_id, trade_date, season_year):
    """Compute post-trade stats for a player from Supabase historical_data."""
    if not player_id or not season_year:
        return None

    try:
        dt = datetime.datetime.strptime(trade_date, "%Y-%m-%d")
        season_start = datetime.datetime(season_year, 3, 28)
        min_sp = max(1, (dt - season_start).days)
    except Exception:
        min_sp = 1

    headers = {
        "apikey": DEFAULT_SUPABASE_KEY,
        "Authorization": f"Bearer {DEFAULT_SUPABASE_KEY}"
    }
    url = (
        f"{DEFAULT_SUPABASE_URL}/rest/v1/historical_data?"
        f"season_year=eq.{season_year}&id=eq.{player_id}&team_id=eq.{to_team_id}&"
        f"scoring_period_id=gte.{min_sp}&select=*"
    )
    rows = []
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as resp:
            rows = json.loads(resp.read().decode("utf-8"))
    except Exception:
        pass

    if not rows:
        url2 = (
            f"{DEFAULT_SUPABASE_URL}/rest/v1/historical_data?"
            f"season_year=eq.{season_year}&id=eq.{player_id}&"
            f"scoring_period_id=gte.{min_sp}&select=*"
        )
        try:
            req2 = urllib.request.Request(url2, headers=headers)
            with urllib.request.urlopen(req2, timeout=5) as resp:
                rows = json.loads(resp.read().decode("utf-8"))
        except Exception:
            pass

    if not rows:
        return None

    c_r, c_hr, c_rbi, c_sb = 0, 0, 0, 0
    c_h, c_bb, c_pa = 0, 0, 0
    c_k, c_qs, c_sv, c_hd = 0, 0, 0, 0
    c_ip_outs, c_er = 0, 0

    for r in rows:
        c_r += float(r.get("20") or 0)
        c_hr += float(r.get("5") or 0)
        c_rbi += float(r.get("21") or 0)
        c_sb += float(r.get("23") or 0)
        c_h += float(r.get("1") or 0)
        c_bb += float(r.get("10") or 0)
        c_pa += float(r.get("16") or (r.get("0") or 0))

        c_k += float(r.get("48") or 0)
        c_qs += float(r.get("63") or 0)
        c_sv += float(r.get("57") or 0)
        c_hd += float(r.get("60") or 0)
        c_ip_outs += float(r.get("34") or 0)
        c_er += float(r.get("45") or 0)

    is_pitcher = c_ip_outs > 0 or c_k > 0 or c_sv > 0
    if is_pitcher:
        ip = round(c_ip_outs / 3.0, 1)
        era = round((c_er * 9.0) / max(0.1, ip), 2) if ip > 0 else 0.0
        return {
            "IP": ip,
            "K": int(c_k),
            "QS": int(c_qs),
            "SV": int(c_sv),
            "HD": int(c_hd),
            "ERA": era
        }
    else:
        obp = round((c_h + c_bb) / max(1, c_pa), 3) if c_pa > 0 else 0.0
        return {
            "R": int(c_r),
            "HR": int(c_hr),
            "RBI": int(c_rbi),
            "SB": int(c_sb),
            "OBP": obp
        }


def process_espn_trades(existing_trades, stats_2026_map):
    txs_2026_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "data", "transactions2026.json"
    )
    txs_hist_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "data", "transactions_historical.json"
    )

    all_txs = []
    if os.path.exists(txs_2026_path):
        try:
            with open(txs_2026_path, "r", encoding="utf-8") as f:
                all_txs.extend(json.load(f))
        except Exception:
            pass

    if os.path.exists(txs_hist_path):
        try:
            with open(txs_hist_path, "r", encoding="utf-8") as f:
                all_txs.extend(json.load(f))
        except Exception:
            pass

    # Deduplicate by espn_transaction_id
    seen_tx_ids = set()
    trade_txs = []
    for t in all_txs:
        if t.get("transaction_type") != "TRADE":
            continue
        tid = t.get("espn_transaction_id")
        if tid and tid not in seen_tx_ids:
            seen_tx_ids.add(tid)
            trade_txs.append(t)

    deals = defaultdict(lambda: {"items": []})
    for t in trade_txs:
        root = t["espn_transaction_id"].split("_")[0]
        date_val = t.get("transaction_date", "").split("T")[0]
        yr = t.get("season_year")
        if not yr and date_val:
            yr = int(date_val[:4])
        key = f"{yr}_{root}"
        deals[key]["id"] = root
        deals[key]["year"] = yr or 2026
        deals[key]["date"] = date_val
        deals[key]["items"].append(t)

    TEAM_ID_TO_OWNER = {
        1: "Tim",
        2: "Adrian",
        3: "Garrett",
        5: "Dan",
        6: "Anil",
        8: "Alex",
        12: "Will",
        13: "Mark",
        14: "Preston"
    }

    new_espn_trades = []
    # Group by season year
    deals_by_year = defaultdict(list)
    for d in deals.values():
        deals_by_year[d["year"]].append(d)

    for yr in sorted(deals_by_year.keys(), reverse=True):
        sorted_deals = sorted(deals_by_year[yr], key=lambda d: d["date"])
        deal_counter = 1

        for deal in sorted_deals:
            deal_date = deal["date"]
            try:
                d_dt = datetime.datetime.strptime(deal_date, "%Y-%m-%d")
            except Exception:
                d_dt = datetime.datetime(yr, 6, 1)

            deal_player_ids = set(i["player_id"] for i in deal["items"])
            deal_teams = set()
            for i in deal["items"]:
                deal_teams.add(i["from_team_id"])
                deal_teams.add(i["to_team_id"])
            deal_owners = set(TEAM_ID_TO_OWNER.get(tid) for tid in deal_teams if tid in TEAM_ID_TO_OWNER)

            # Check overlap against existing Google Sheet trades for this year
            matched = None
            for ext in existing_trades:
                if ext.get("season_year") != yr:
                    continue
                ext_date = ext.get("trade_date")
                try:
                    ext_dt = datetime.datetime.strptime(ext_date, "%Y-%m-%d")
                except Exception:
                    continue
                if abs((d_dt - ext_dt).days) > 5:
                    continue
                ext_pids = set(it.get("espn_player_id") for it in ext.get("items", []) if it.get("asset_type") == "Player")
                if ext_pids.intersection(deal_player_ids):
                    matched = ext
                    break

            if matched:
                print(f"ℹ️ ESPN deal {deal['id'][:8]} ({deal_date}) overlaps with existing {matched['unique_id']} ({matched['trade_date']}) - keeping primary sheet record")
                continue

            if len(deal_owners) < 2:
                continue

            participants = sorted(list(deal_owners))
            trade_unique_id = f"{yr}_espn_{deal_counter}"
            trade_id_label = f"ESPN-{deal_counter}"

            trade_items = []
            for i in deal["items"]:
                snd = TEAM_ID_TO_OWNER.get(i["from_team_id"], f"Team {i['from_team_id']}")
                rcv = TEAM_ID_TO_OWNER.get(i["to_team_id"], f"Team {i['to_team_id']}")
                pid = i.get("player_id")
                pname = i.get("player_name")
                to_tid = i["to_team_id"]

                if yr == 2026:
                    p_stats = compute_post_trade_stats(pid, to_tid, deal_date, stats_2026_map)
                else:
                    p_stats = compute_historical_post_trade_stats(pid, to_tid, deal_date, yr)

                trade_item = {
                    "season_year": yr,
                    "trade_id": trade_id_label,
                    "trade_date": deal_date,
                    "sending_owner": snd,
                    "from_team_id": i["from_team_id"],
                    "receiving_owner": rcv,
                    "to_team_id": i["to_team_id"],
                    "asset_type": "Player",
                    "asset_name": pname,
                    "pick_number": None,
                    "round_number": None,
                    "budget_amount": 0.0,
                    "original_pick": "",
                    "espn_player_id": pid
                }
                if p_stats:
                    trade_item["stats"] = p_stats

                trade_items.append(trade_item)

            owner_packages = {}
            for p in participants:
                owner_packages[p] = {
                    "owner": p,
                    "team_id": OWNER_TO_TEAM_ID.get(p),
                    "sent": [],
                    "received": []
                }

            for item in trade_items:
                snd = item["sending_owner"]
                rcv = item["receiving_owner"]
                if snd in owner_packages:
                    owner_packages[snd]["sent"].append(item)
                if rcv in owner_packages:
                    owner_packages[rcv]["received"].append(item)

            new_espn_trades.append({
                "unique_id": trade_unique_id,
                "season_year": yr,
                "trade_id": trade_id_label,
                "trade_date": deal_date,
                "espn_root_id": deal["id"],
                "is_espn_player_only": True,
                "participants": participants,
                "owner_packages": owner_packages,
                "items": trade_items
            })
            deal_counter += 1

        print(f"✅ Ingested {deal_counter - 1} ESPN trades for season {yr}")

    return new_espn_trades


def main():
    picks_by_year_pick, players_by_year_name = load_draft_history()
    daily_records_2026 = load_2026_daily_records()

    all_trades = []
    for sheet in SHEETS:
        trades = process_sheet(sheet, picks_by_year_pick, players_by_year_name, daily_records_2026)
        print(f"Season {sheet['season_year']}: Processed {len(trades)} trades.")
        all_trades.extend(trades)

    espn_trades = process_espn_trades(all_trades, daily_records_2026)
    all_trades.extend(espn_trades)

    all_trades.sort(key=lambda x: (x["trade_date"], x["unique_id"]), reverse=True)

    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "data", "historicalTrades.json"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_trades, f, indent=2)

    print(f"🎉 Successfully saved {len(all_trades)} multi-year trades with drafted player, stats, & ESPN trades to {output_path}")


if __name__ == "__main__":
    main()

