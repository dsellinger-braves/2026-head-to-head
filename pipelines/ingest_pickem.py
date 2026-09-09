"""
pipelines/ingest_pickem.py

Annual MLB Pick'em Ingestion & Management Pipeline
---------------------------------------------------
1. Scrapes historical and active Pick'em sheets from Google Sheets:
   - 2024-2026: 23-question format (3 Wild Cards per league)
   - 2019-2021: 21-question format (2 Wild Cards per league)
   - 2016-2018: Summary score format with category totals
2. Normalizes questions, categories, point rules, and answers.
3. Maps owner names to league team IDs.
4. Calculates/validates scores, rankings, and draft budget prizes.
5. Populates Supabase tables:
   - `pickem_seasons`
   - `pickem_questions`
   - `pickem_picks`
   - `pickem_scores`
6. Provides turnkey CLI bootstrap function for creating future seasons (e.g. 2027).
"""

import os
import sys
import json
import csv
import io
import urllib.request
import urllib.parse
import requests
from typing import Dict, List, Tuple, Any, Optional

# Supabase default credentials
DEFAULT_SUPABASE_URL = "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0."
    "wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

SHEET_ID = "1ANAT7XUhA8preuNTowuSySZGJD4GBo_ghfej6h2v5WI"
AVAILABLE_TABS = ["2026", "2025", "2024", "2021", "2020", "2019", "2018", "2017", "2016"]

# Standard 23 Questions for Modern Era (2022+)
STANDARD_QUESTIONS_MODERN = [
    {"question_key": "nl_east", "question_label": "NL East", "category": "division", "options_type": "mlb_team", "display_order": 1, "points_exact": 3, "points_partial": 2},
    {"question_key": "nl_central", "question_label": "NL Central", "category": "division", "options_type": "mlb_team", "display_order": 2, "points_exact": 3, "points_partial": 2},
    {"question_key": "nl_west", "question_label": "NL West", "category": "division", "options_type": "mlb_team", "display_order": 3, "points_exact": 3, "points_partial": 2},
    {"question_key": "nl_wc_1", "question_label": "NL Wild Card 1", "category": "wild_card", "options_type": "mlb_team", "display_order": 4, "points_exact": 2, "points_partial": 2},
    {"question_key": "nl_wc_2", "question_label": "NL Wild Card 2", "category": "wild_card", "options_type": "mlb_team", "display_order": 5, "points_exact": 2, "points_partial": 2},
    {"question_key": "nl_wc_3", "question_label": "NL Wild Card 3", "category": "wild_card", "options_type": "mlb_team", "display_order": 6, "points_exact": 2, "points_partial": 2},
    {"question_key": "al_east", "question_label": "AL East", "category": "division", "options_type": "mlb_team", "display_order": 7, "points_exact": 3, "points_partial": 2},
    {"question_key": "al_central", "question_label": "AL Central", "category": "division", "options_type": "mlb_team", "display_order": 8, "points_exact": 3, "points_partial": 2},
    {"question_key": "al_west", "question_label": "AL West", "category": "division", "options_type": "mlb_team", "display_order": 9, "points_exact": 3, "points_partial": 2},
    {"question_key": "al_wc_1", "question_label": "AL Wild Card 1", "category": "wild_card", "options_type": "mlb_team", "display_order": 10, "points_exact": 2, "points_partial": 2},
    {"question_key": "al_wc_2", "question_label": "AL Wild Card 2", "category": "wild_card", "options_type": "mlb_team", "display_order": 11, "points_exact": 2, "points_partial": 2},
    {"question_key": "al_wc_3", "question_label": "AL Wild Card 3", "category": "wild_card", "options_type": "mlb_team", "display_order": 12, "points_exact": 2, "points_partial": 2},
    {"question_key": "nl_pennant", "question_label": "NL Pennant", "category": "playoff_result", "options_type": "mlb_team", "display_order": 13, "points_exact": 5, "points_partial": 0},
    {"question_key": "al_pennant", "question_label": "AL Pennant", "category": "playoff_result", "options_type": "mlb_team", "display_order": 14, "points_exact": 5, "points_partial": 0},
    {"question_key": "world_series", "question_label": "World Series Winner", "category": "playoff_result", "options_type": "mlb_team", "display_order": 15, "points_exact": 7, "points_partial": 0},
    {"question_key": "nl_mvp", "question_label": "NL MVP", "category": "award", "options_type": "player_text", "display_order": 16, "points_exact": 4, "points_partial": 0},
    {"question_key": "nl_cy_young", "question_label": "NL Cy Young", "category": "award", "options_type": "player_text", "display_order": 17, "points_exact": 4, "points_partial": 0},
    {"question_key": "nl_roy", "question_label": "NL Rookie of the Year", "category": "award", "options_type": "player_text", "display_order": 18, "points_exact": 4, "points_partial": 0},
    {"question_key": "al_mvp", "question_label": "AL MVP", "category": "award", "options_type": "player_text", "display_order": 19, "points_exact": 4, "points_partial": 0},
    {"question_key": "al_cy_young", "question_label": "AL Cy Young", "category": "award", "options_type": "player_text", "display_order": 20, "points_exact": 4, "points_partial": 0},
    {"question_key": "al_roy", "question_label": "AL Rookie of the Year", "category": "award", "options_type": "player_text", "display_order": 21, "points_exact": 4, "points_partial": 0},
    {"question_key": "most_wins", "question_label": "Most Team Wins (reg. season)", "category": "extremes", "options_type": "mlb_team", "display_order": 22, "points_exact": 3, "points_partial": 0},
    {"question_key": "most_losses", "question_label": "Most Team Losses (reg. season)", "category": "extremes", "options_type": "mlb_team", "display_order": 23, "points_exact": 3, "points_partial": 0},
]

# Standard 21 Questions for Pre-2022 Era (2 Wild Cards per league)
STANDARD_QUESTIONS_CLASSIC = [
    {"question_key": "nl_east", "question_label": "NL East", "category": "division", "options_type": "mlb_team", "display_order": 1, "points_exact": 3, "points_partial": 2},
    {"question_key": "nl_central", "question_label": "NL Central", "category": "division", "options_type": "mlb_team", "display_order": 2, "points_exact": 3, "points_partial": 2},
    {"question_key": "nl_west", "question_label": "NL West", "category": "division", "options_type": "mlb_team", "display_order": 3, "points_exact": 3, "points_partial": 2},
    {"question_key": "nl_wc_1", "question_label": "NL Wild Card 1", "category": "wild_card", "options_type": "mlb_team", "display_order": 4, "points_exact": 2, "points_partial": 2},
    {"question_key": "nl_wc_2", "question_label": "NL Wild Card 2", "category": "wild_card", "options_type": "mlb_team", "display_order": 5, "points_exact": 2, "points_partial": 2},
    {"question_key": "al_east", "question_label": "AL East", "category": "division", "options_type": "mlb_team", "display_order": 6, "points_exact": 3, "points_partial": 2},
    {"question_key": "al_central", "question_label": "AL Central", "category": "division", "options_type": "mlb_team", "display_order": 7, "points_exact": 3, "points_partial": 2},
    {"question_key": "al_west", "question_label": "AL West", "category": "division", "options_type": "mlb_team", "display_order": 8, "points_exact": 3, "points_partial": 2},
    {"question_key": "al_wc_1", "question_label": "AL Wild Card 1", "category": "wild_card", "options_type": "mlb_team", "display_order": 9, "points_exact": 2, "points_partial": 2},
    {"question_key": "al_wc_2", "question_label": "AL Wild Card 2", "category": "wild_card", "options_type": "mlb_team", "display_order": 10, "points_exact": 2, "points_partial": 2},
    {"question_key": "nl_pennant", "question_label": "NL Pennant", "category": "playoff_result", "options_type": "mlb_team", "display_order": 11, "points_exact": 5, "points_partial": 0},
    {"question_key": "al_pennant", "question_label": "AL Pennant", "category": "playoff_result", "options_type": "mlb_team", "display_order": 12, "points_exact": 5, "points_partial": 0},
    {"question_key": "world_series", "question_label": "World Series Winner", "category": "playoff_result", "options_type": "mlb_team", "display_order": 13, "points_exact": 7, "points_partial": 0},
    {"question_key": "nl_mvp", "question_label": "NL MVP", "category": "award", "options_type": "player_text", "display_order": 14, "points_exact": 4, "points_partial": 0},
    {"question_key": "nl_cy_young", "question_label": "NL Cy Young", "category": "award", "options_type": "player_text", "display_order": 15, "points_exact": 4, "points_partial": 0},
    {"question_key": "nl_roy", "question_label": "NL Rookie of the Year", "category": "award", "options_type": "player_text", "display_order": 16, "points_exact": 4, "points_partial": 0},
    {"question_key": "al_mvp", "question_label": "AL MVP", "category": "award", "options_type": "player_text", "display_order": 17, "points_exact": 4, "points_partial": 0},
    {"question_key": "al_cy_young", "question_label": "AL Cy Young", "category": "award", "options_type": "player_text", "display_order": 18, "points_exact": 4, "points_partial": 0},
    {"question_key": "al_roy", "question_label": "AL Rookie of the Year", "category": "award", "options_type": "player_text", "display_order": 19, "points_exact": 4, "points_partial": 0},
    {"question_key": "most_wins", "question_label": "Most Team Wins (reg. season)", "category": "extremes", "options_type": "mlb_team", "display_order": 20, "points_exact": 3, "points_partial": 0},
    {"question_key": "most_losses", "question_label": "Most Team Losses (reg. season)", "category": "extremes", "options_type": "mlb_team", "display_order": 21, "points_exact": 3, "points_partial": 0},
]

# Owner Name Normalization & Team ID Mapping
OWNER_TEAM_MAP = {
    "tim": 1,
    "adrian": 2,
    "garrett": 3,
    "daniel": 5,
    "dan": 5,
    "anil": 6,
    "alex": 8,
    "will": 12,
    "william": 12,
    "mark": 13,
    "preston": 14,
    "patrick": 14,
    "owens": 14,
}

CANONICAL_OWNER_NAMES = {
    "tim": "Tim",
    "adrian": "Adrian",
    "garrett": "Garrett",
    "daniel": "Daniel",
    "dan": "Daniel",
    "anil": "Anil",
    "alex": "Alex",
    "will": "Will",
    "william": "Will",
    "mark": "Mark",
    "preston": "Preston",
    "patrick": "Preston",
    "owens": "Preston",
    "joe": "Joe",
    "joseph": "Joseph",
    "anurag": "Anurag",
    "andrew": "Andrew",
    "jordie": "Jordie",
}


def get_supabase_headers(key: str) -> Dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def fetch_tab_rows(tab_name: str) -> List[List[str]]:
    """Fetch raw CSV rows for a tab from Google Sheets."""
    enc = urllib.parse.quote(tab_name)
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={enc}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8", errors="ignore")
            return list(csv.reader(io.StringIO(content)))
    except Exception as e:
        print(f"⚠️ Error fetching tab {tab_name}: {e}")
        return []


def normalize_owner(raw_name: str) -> str:
    clean = raw_name.strip().lower()
    return CANONICAL_OWNER_NAMES.get(clean, raw_name.strip())


def get_team_id(owner_name: str) -> Optional[int]:
    return OWNER_TEAM_MAP.get(owner_name.strip().lower())


def ingest_season(season_year: int, rows: List[List[str]], supabase_url: str, headers: Dict[str, str]):
    print(f"\n📂 Processing Pick'em Season {season_year} ({len(rows)} rows)...")

    is_modern = season_year >= 2022
    questions_template = STANDARD_QUESTIONS_MODERN if is_modern else STANDARD_QUESTIONS_CLASSIC

    status = "in_progress" if season_year == 2026 else ("open" if season_year > 2026 else "completed")

    # 1. Upsert Season Record
    season_payload = {
        "season_year": season_year,
        "status": status,
        "picks_deadline": f"{season_year}-03-26T17:00:00Z",
        "scoring_rules": {
            "division_winner": 3,
            "playoff_team": 2,
            "pennant": 5,
            "world_series": 7,
            "award": 4,
            "wins_losses": 3,
        },
        "budget_rules": {
            "1": 4,
            "2": 3,
            "3": 3,
            "4": 3,
            "5": 1,
        },
    }

    res = requests.post(
        f"{supabase_url}/rest/v1/pickem_seasons?on_conflict=season_year",
        headers=headers,
        data=json.dumps(season_payload),
        timeout=15,
    )
    if res.status_code not in [200, 201]:
        print(f"⚠️ Failed to upsert season {season_year}: {res.status_code} {res.text}")
        return

    print(f"  ✅ Season {season_year} registered (status: {status})")

    # 2. Extract Questions & Answers
    questions_to_insert = []
    question_answer_map = {}

    if season_year in [2016, 2017, 2018]:
        # Summary era
        for q in questions_template:
            questions_to_insert.append({
                "season_year": season_year,
                "question_key": q["question_key"],
                "question_label": q["question_label"],
                "category": q["category"],
                "options_type": q["options_type"],
                "display_order": q["display_order"],
                "correct_answer": None,
                "points_exact": q["points_exact"],
                "points_partial": q["points_partial"],
            })
    else:
        # Matrix era (2019+)
        header = rows[0]
        ans_col_idx = -1
        for idx, col in enumerate(header):
            if col.strip().lower() == "answer":
                ans_col_idx = idx
                break

        num_q = len(questions_template)
        for r_idx in range(1, num_q + 1):
            if r_idx >= len(rows):
                break
            row = rows[r_idx]
            q_label = row[0].strip()
            answer = row[ans_col_idx].strip() if ans_col_idx != -1 and ans_col_idx < len(row) else None
            if answer in ["-", "", "none"]:
                answer = None

            tmpl = questions_template[r_idx - 1] if (r_idx - 1) < len(questions_template) else {
                "question_key": f"q_{r_idx}", "question_label": q_label, "category": "custom", "options_type": "player_text", "display_order": r_idx, "points_exact": 3, "points_partial": 0
            }

            q_record = {
                "season_year": season_year,
                "question_key": tmpl["question_key"],
                "question_label": tmpl["question_label"],
                "category": tmpl["category"],
                "options_type": tmpl["options_type"],
                "display_order": tmpl["display_order"],
                "correct_answer": answer,
                "points_exact": tmpl["points_exact"],
                "points_partial": tmpl["points_partial"],
            }
            questions_to_insert.append(q_record)
            question_answer_map[tmpl["question_key"]] = answer

    # Upsert questions
    res = requests.post(
        f"{supabase_url}/rest/v1/pickem_questions?on_conflict=season_year,question_key",
        headers=headers,
        data=json.dumps(questions_to_insert),
        timeout=15,
    )
    if res.status_code not in [200, 201]:
        print(f"⚠️ Failed to upsert questions for {season_year}: {res.status_code} {res.text}")
        return

    # Fetch inserted questions with their UUIDs
    res = requests.get(
        f"{supabase_url}/rest/v1/pickem_questions?season_year=eq.{season_year}&select=id,question_key,display_order,correct_answer",
        headers=headers,
        timeout=15,
    )
    db_questions = {q["question_key"]: q["id"] for q in res.json()}
    print(f"  ✅ {len(db_questions)} questions configured in Supabase")

    # 3. Extract Individual Picks
    picks_to_insert = []
    seen_pick_keys = set()
    owner_scores_map = {}

    if season_year in [2016, 2017, 2018]:
        # Parse summary era scores from rows 1-7
        row0 = rows[0]
        owners = []
        for col_idx in range(1, len(row0)):
            val = row0[col_idx].strip()
            if not val:
                continue
            parts = val.split()
            if not parts:
                continue
            owner_raw = parts[0]
            if owner_raw.lower() in ["answer", "total", "-"]:
                continue
            canon_name = normalize_owner(owner_raw)
            owners.append((col_idx, canon_name))

        for col_idx, owner_name in owners:
            cat_scores = {}
            for r_idx in range(1, min(len(rows), 8)):
                r = rows[r_idx]
                if not r or not r[0].strip():
                    continue
                label = r[0].strip()
                val_str = r[col_idx].strip() if col_idx < len(r) else "0"
                pts = int(val_str) if val_str.isdigit() else 0
                cat_scores[label.lower().replace(" ", "_")] = pts

            tot = cat_scores.get("total", sum(cat_scores.values()))
            owner_scores_map[owner_name] = {
                "season_year": season_year,
                "owner_name": owner_name,
                "team_id": get_team_id(owner_name),
                "total_points": tot,
                "category_scores": cat_scores,
                "place": None,
                "budget_awarded": None,
            }

    else:
        # Matrix era (2019+)
        header = rows[0]
        owner_cols = []
        ans_col_idx = -1
        for idx, col in enumerate(header):
            c_clean = col.strip()
            if c_clean.lower() == "answer":
                ans_col_idx = idx
                break
            if c_clean and c_clean != "-" and idx > 0:
                owner_cols.append((idx, normalize_owner(c_clean)))

        # Find points columns on the right (columns with matching owner names)
        points_cols = {}
        for idx in range(ans_col_idx + 1, len(header)):
            c_clean = header[idx].strip()
            if c_clean and c_clean != "-":
                c_norm = normalize_owner(c_clean)
                points_cols[c_norm] = idx

        num_q = len(questions_template)
        for r_idx in range(1, num_q + 1):
            if r_idx >= len(rows):
                break
            row = rows[r_idx]
            tmpl = questions_template[r_idx - 1]
            q_id = db_questions.get(tmpl["question_key"])
            if not q_id:
                continue

            for col_idx, owner_name in owner_cols:
                pick_val = row[col_idx].strip() if col_idx < len(row) else ""
                if not pick_val:
                    continue

                dedup_key = (season_year, q_id, owner_name)
                if dedup_key in seen_pick_keys:
                    continue
                seen_pick_keys.add(dedup_key)

                # Points awarded from sheet if available
                pts_col = points_cols.get(owner_name)
                pts = 0
                if pts_col and pts_col < len(row):
                    pts_str = row[pts_col].strip()
                    if pts_str.isdigit():
                        pts = int(pts_str)

                picks_to_insert.append({
                    "season_year": season_year,
                    "question_id": q_id,
                    "owner_name": owner_name,
                    "team_id": get_team_id(owner_name),
                    "pick_value": pick_val,
                    "points_awarded": pts,
                    "is_correct": pts > 0,
                })

        # Parse category subtotals and leaderboard
        for r_idx in range(num_q + 1, len(rows)):
            row = rows[r_idx]
            if not row or not row[0].strip():
                continue
            cat_name = row[0].strip().lower().replace(" ", "_")
            if cat_name in ["nl_playoffs", "al_playoffs", "playoff_result", "nl_award", "al_award", "most_wins/losses", "total"]:
                for col_idx, owner_name in owner_cols:
                    val_str = row[col_idx].strip() if col_idx < len(row) else "0"
                    pts = int(val_str) if val_str.isdigit() else 0
                    if owner_name not in owner_scores_map:
                        owner_scores_map[owner_name] = {
                            "season_year": season_year,
                            "owner_name": owner_name,
                            "team_id": get_team_id(owner_name),
                            "total_points": 0,
                            "category_scores": {},
                            "place": None,
                            "budget_awarded": None,
                        }
                    if cat_name == "total":
                        owner_scores_map[owner_name]["total_points"] = pts
                    else:
                        owner_scores_map[owner_name]["category_scores"][cat_name] = pts

        # Parse Prize table (Place, Owner, Points, Budget) if present
        for r_idx in range(num_q + 1, len(rows)):
            row = rows[r_idx]
            for c_idx in range(len(row) - 3):
                cell = row[c_idx].strip().lower()
                if cell == "place":
                    for p_row_idx in range(r_idx + 1, len(rows)):
                        p_row = rows[p_row_idx]
                        if c_idx < len(p_row) and p_row[c_idx].strip().isdigit():
                            plc = int(p_row[c_idx].strip())
                            own = normalize_owner(p_row[c_idx + 1].strip())
                            bdg = p_row[c_idx + 3].strip() if (c_idx + 3) < len(p_row) else None
                            if own in owner_scores_map:
                                owner_scores_map[own]["place"] = plc
                                owner_scores_map[own]["budget_awarded"] = bdg
                    break

    # Upsert picks in batches
    if picks_to_insert:
        batch_size = 150
        for i in range(0, len(picks_to_insert), batch_size):
            batch = picks_to_insert[i : i + batch_size]
            res = requests.post(
                f"{supabase_url}/rest/v1/pickem_picks?on_conflict=season_year,question_id,owner_name",
                headers=headers,
                data=json.dumps(batch),
                timeout=20,
            )
            if res.status_code not in [200, 201]:
                print(f"⚠️ Failed picks batch for {season_year}: {res.status_code} {res.text}")
        print(f"  ✅ {len(picks_to_insert)} individual picks saved")

    # Compute places if missing
    sorted_scores = sorted(owner_scores_map.values(), key=lambda x: x["total_points"], reverse=True)
    current_rank = 1
    for idx, s in enumerate(sorted_scores):
        if idx > 0 and s["total_points"] < sorted_scores[idx - 1]["total_points"]:
            current_rank = idx + 1
        if s["place"] is None:
            s["place"] = current_rank

    scores_to_insert = list(owner_scores_map.values())
    if scores_to_insert:
        res = requests.post(
            f"{supabase_url}/rest/v1/pickem_scores?on_conflict=season_year,owner_name",
            headers=headers,
            data=json.dumps(scores_to_insert),
            timeout=20,
        )
        if res.status_code not in [200, 201]:
            print(f"⚠️ Failed scores for {season_year}: {res.status_code} {res.text}")
        else:
            print(f"  ✅ {len(scores_to_insert)} owner scores & rankings recorded")

    print(f"🎉 Season {season_year} fully ingested.")


def bootstrap_future_season(season_year: int, supabase_url: str, headers: Dict[str, str]):
    """Seed questions for a brand new season and open it for owner submissions."""
    print(f"\n🚀 Bootstrapping Pick'em for future season {season_year}...")

    season_payload = {
        "season_year": season_year,
        "status": "open",
        "picks_deadline": f"{season_year}-03-25T17:00:00Z",
        "scoring_rules": {
            "division_winner": 3,
            "playoff_team": 2,
            "pennant": 5,
            "world_series": 7,
            "award": 4,
            "wins_losses": 3,
        },
        "budget_rules": {
            "1": 4,
            "2": 3,
            "3": 3,
            "4": 3,
            "5": 1,
        },
    }

    res = requests.post(
        f"{supabase_url}/rest/v1/pickem_seasons?on_conflict=season_year",
        headers=headers,
        data=json.dumps(season_payload),
        timeout=15,
    )
    if res.status_code not in [200, 201]:
        print(f"❌ Failed to create season {season_year}: {res.text}")
        return

    questions = []
    for q in STANDARD_QUESTIONS_MODERN:
        questions.append({
            "season_year": season_year,
            "question_key": q["question_key"],
            "question_label": q["question_label"],
            "category": q["category"],
            "options_type": q["options_type"],
            "display_order": q["display_order"],
            "correct_answer": None,
            "points_exact": q["points_exact"],
            "points_partial": q["points_partial"],
        })

    res = requests.post(
        f"{supabase_url}/rest/v1/pickem_questions?on_conflict=season_year,question_key",
        headers=headers,
        data=json.dumps(questions),
        timeout=15,
    )
    if res.status_code in [200, 201]:
        print(f"✅ Successfully created season {season_year} with all 23 standard questions! Open for owner picks.")
    else:
        print(f"❌ Failed to seed questions: {res.text}")


def run_pipeline():
    print("⚾ Starting Annual Pick'em Ingestion Pipeline...")

    supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL") or DEFAULT_SUPABASE_URL
    supabase_key = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY") or DEFAULT_SUPABASE_KEY
    headers = get_supabase_headers(supabase_key)

    # Ingest all historical tabs
    for tab in AVAILABLE_TABS:
        year = int(tab)
        rows = fetch_tab_rows(tab)
        if rows:
            ingest_season(year, rows, supabase_url, headers)

    # Bootstrap 2027 as an upcoming open season
    bootstrap_future_season(2027, supabase_url, headers)

    print("\n🏁 All Pick'em data successfully warehoused in Supabase!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("--bootstrap="):
        yr = int(sys.argv[1].split("=")[1])
        s_url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL") or DEFAULT_SUPABASE_URL
        s_key = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY") or DEFAULT_SUPABASE_KEY
        bootstrap_future_season(yr, s_url, get_supabase_headers(s_key))
    else:
        run_pipeline()
