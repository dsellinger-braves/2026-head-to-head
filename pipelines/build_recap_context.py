#!/usr/bin/env python3
"""
pipelines/build_recap_context.py

Fetches real-time MLB news from ESPN and MLB.com RSS feeds,
matches mentioned players against active fantasy rosters in league 130215,
synthesizes manager personas and historical league lore from data/league_context.json,
and generates narrative context blocks for Google Gemini in discord-daily-recap.py.
"""

import os
import json
import re
import requests
import xml.etree.ElementTree as ET
from datetime import date
from typing import Optional, List, Dict

RSS_FEEDS = [
    "https://www.espn.com/espn/rss/mlb/news",
    "https://www.mlb.com/feeds/news/rss.xml"
]

TEAM_NAMES = {
    1:  "Tim",
    2:  "Adrian",
    3:  "Garrett",
    5:  "Dan",
    6:  "Anil",
    8:  "Alex",
    12: "Will",
    13: "Mark",
    14: "Preston"
}

_context_cache = None


def load_league_context() -> dict:
    """Load the persistent league context document."""
    global _context_cache
    if _context_cache is not None:
        return _context_cache

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    context_path = os.path.join(base_dir, "data", "league_context.json")
    if os.path.exists(context_path):
        try:
            with open(context_path, "r", encoding="utf-8") as f:
                _context_cache = json.load(f)
                return _context_cache
        except Exception as e:
            print(f"⚠️ Could not read league_context.json: {e}")

    return {}


def fetch_mlb_news(limit: int = 15) -> List[Dict[str, str]]:
    """Fetch top breaking MLB news headlines and summaries from RSS feeds."""
    articles = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for feed_url in RSS_FEEDS:
        try:
            resp = requests.get(feed_url, headers=headers, timeout=6)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                channel = root.find("channel")
                if channel is not None:
                    for item in channel.findall("item"):
                        title = (item.findtext("title") or "").strip()
                        desc = (item.findtext("description") or "").strip()
                        # Clean HTML tags from description if any
                        desc = re.sub(r"<[^>]+>", "", desc).strip()
                        if title and not any(a["title"] == title for a in articles):
                            articles.append({"title": title, "description": desc})
                            if len(articles) >= limit:
                                break
        except Exception as e:
            print(f"⚠️ Failed fetching MLB news from {feed_url}: {e}")

        if len(articles) >= limit:
            break

    return articles


def build_roster_player_map(today_records: Optional[List[dict]] = None) -> Dict[str, dict]:
    """
    Build a mapping of clean player name -> { team_id, owner_name, full_name }
    from today_records or player_daily_stats.
    """
    player_map = {}
    if not today_records:
        return player_map

    for r in today_records:
        full_name = r.get("full_name") or r.get("fullName") or ""
        tid = r.get("team_id")
        if full_name and tid:
            clean = full_name.lower().strip()
            player_map[clean] = {
                "team_id": tid,
                "owner_name": TEAM_NAMES.get(tid, f"Team {tid}"),
                "full_name": full_name
            }
            # Also map without suffixes like Jr., Sr., III
            no_suffix = re.sub(r"\s+(jr\.?|sr\.?|ii|iii|iv)$", "", clean)
            if no_suffix != clean:
                player_map[no_suffix] = player_map[clean]

    return player_map


def find_news_roster_overlap(news_articles: List[Dict[str, str]], player_map: Dict[str, dict]) -> List[dict]:
    """Find overlap between real MLB news articles and players rostered in our league."""
    overlaps = []
    if not player_map or not news_articles:
        return overlaps

    for art in news_articles:
        text = f"{art['title']} {art['description']}".lower()
        matched_players = []

        for p_clean, p_info in player_map.items():
            # Check for word boundary match on player name (require length > 5 to avoid false positives)
            if len(p_clean) > 5 and re.search(r"\b" + re.escape(p_clean) + r"\b", text):
                matched_players.append(p_info)

        if matched_players:
            overlaps.append({
                "article": art,
                "players": matched_players
            })

    return overlaps


CAT_HIGHER_IS_BETTER = {
    "R": True, "HR": True, "RBI": True, "OBP": True, "SB": True,
    "QS": True, "ERA": False, "WHIP": False, "K": True, "SV_HD": True,
}

CAT_THRESHOLDS = {
    "OBP": 0.0035,
    "ERA": 0.15,
    "WHIP": 0.030,
    "SB": 3,
    "HR": 3,
    "SV_HD": 3,
    "QS": 2,
    "R": 6,
    "RBI": 6,
    "K": 8,
}


def fmt_cat_val(cat: str, val: float) -> str:
    """Format a category stat value cleanly."""
    if cat == "OBP":
        return f"{val:.4f}".replace("0.", ".")
    if cat in ("ERA", "WHIP"):
        return f"{val:.2f}"
    return f"{int(round(val))}"


def detect_active_roto_battles(
    standings: Optional[dict] = None,
    delta: Optional[dict] = None,
    limit: int = 4
) -> List[str]:
    """
    Dynamically identify active category dogfights and standings tug-of-wars
    where managers are separated by razor-thin margins and actively trading roto points.
    """
    if not standings or not isinstance(standings, dict):
        return []

    battles = []

    # 1. Category Point Battles (adjacent teams within striking distance of flipping a point)
    for cat, higher in CAT_HIGHER_IS_BETTER.items():
        thresh = CAT_THRESHOLDS.get(cat, 3)
        teams_with_cat = []
        for tid, data in standings.items():
            if cat in data and isinstance(data[cat], (int, float)):
                pts = data.get("cat_points", {}).get(cat, 0.0)
                teams_with_cat.append((tid, float(data[cat]), float(pts)))

        # Sort teams best to worst in this category
        teams_with_cat.sort(key=lambda x: x[1], reverse=higher)

        for i in range(len(teams_with_cat) - 1):
            t1_id, t1_val, t1_pts = teams_with_cat[i]
            t2_id, t2_val, t2_pts = teams_with_cat[i + 1]

            diff = abs(t1_val - t2_val)
            if diff <= thresh:
                m1 = TEAM_NAMES.get(t1_id, f"Team {t1_id}")
                m2 = TEAM_NAMES.get(t2_id, f"Team {t2_id}")

                # Check if delta reflects an active flip or movement
                flipped = False
                if delta:
                    d1_pt = delta.get(t1_id, {}).get("cat_delta", {}).get(cat, 0)
                    d2_pt = delta.get(t2_id, {}).get("cat_delta", {}).get(cat, 0)
                    if (d1_pt > 0 and d2_pt < 0) or (d1_pt < 0 and d2_pt > 0):
                        flipped = True

                t1_str = f"{m1} ({fmt_cat_val(cat, t1_val)}, {t1_pts:.1f} pts)"
                t2_str = f"{m2} ({fmt_cat_val(cat, t2_val)}, {t2_pts:.1f} pts)"
                diff_str = f"{diff:.4f}".replace("0.", ".") if cat == "OBP" else f"{diff:.2f}" if cat in ("ERA", "WHIP") else f"{int(round(diff))}"

                if flipped:
                    battles.append(
                        f"  - ⚡ ACTIVE POINT FLIP in {cat}: {m1} and {m2} just traded roto points! Separated by only {diff_str} in {cat} ({t1_str} vs {t2_str})."
                    )
                else:
                    battles.append(
                        f"  - ⚔️ {cat} TUG-OF-WAR (Margin: {diff_str}): {t1_str} vs {t2_str} — neck-and-neck for this roto point, actively trading it back and forth!"
                    )

    # 2. Overall Standings Logjams (teams within 1.5 total roto points)
    ranked_teams = sorted(
        [(tid, d.get("roto_points", 0.0), d.get("standing", 0)) for tid, d in standings.items()],
        key=lambda x: x[1],
        reverse=True
    )
    for i in range(len(ranked_teams) - 1):
        t1_id, t1_pts, t1_rank = ranked_teams[i]
        t2_id, t2_pts, t2_rank = ranked_teams[i + 1]
        pt_gap = round(t1_pts - t2_pts, 1)
        if pt_gap <= 1.5:
            m1 = TEAM_NAMES.get(t1_id, f"Team {t1_id}")
            m2 = TEAM_NAMES.get(t2_id, f"Team {t2_id}")
            battles.append(
                f"  - 🏆 STANDINGS DEADLOCK: #{t1_rank} {m1} ({t1_pts:.1f} pts) vs #{t2_rank} {m2} ({t2_pts:.1f} pts) — separated by just {pt_gap} total point; one category swing flips their podium rank!"
            )

    return battles[:limit]


def format_recap_context(
    today_records: Optional[List[dict]] = None,
    involved_team_ids: Optional[List[int]] = None,
    standings: Optional[dict] = None,
    delta: Optional[dict] = None,
) -> str:
    """
    Generate comprehensive context injection text prioritizing:
    1. Real MLB headlines matched to fantasy rosters
    2. Dynamic active category battlegrounds & roto point tug-of-wars
    3. Season-long category battlegrounds and volatility patterns
    4. Manager personas, in-season pacing tendencies, and banter triggers
    """
    context_data = load_league_context()
    personas = context_data.get("manager_personas", {})
    battlegrounds = context_data.get("category_battlegrounds", [])
    season_patterns = context_data.get("season_trends_and_patterns", [])
    rivalries = context_data.get("in_season_rivalries", []) or context_data.get("rivalries", [])

    # 1. Real MLB News Overlap
    news_articles = fetch_mlb_news(limit=15)
    player_map = build_roster_player_map(today_records)
    overlaps = find_news_roster_overlap(news_articles, player_map)

    news_lines = []
    if overlaps:
        for o in overlaps[:4]:
            art = o["article"]
            for p in o["players"]:
                news_lines.append(f"  - ⚾ {p['full_name']} (Rostered by {p['owner_name']}): \"{art['title']}\" — {art['description'][:140]}...")
    elif news_articles:
        for a in news_articles[:3]:
            news_lines.append(f"  - ⚾ MLB Breaking: \"{a['title']}\"")

    news_block = "\n".join(news_lines) if news_lines else "  (No breaking MLB news matches today)"

    # 2. Dynamic Live Category Battles & Point Tug-of-Wars
    active_battles = detect_active_roto_battles(standings, delta, limit=5)
    active_battles_block = "\n".join(active_battles) if active_battles else "  (Standings margins evenly distributed today)"

    # 3. Persistent Season-Long Category Battlegrounds & Trends
    bg_lines = []
    if battlegrounds:
        for bg in battlegrounds[:3]:
            bg_lines.append(f"  - {bg['title']} ({bg['category']}): {bg['dynamic']}")

    if season_patterns:
        for sp in season_patterns[:2]:
            bg_lines.append(f"  - Pattern: {sp['pattern']} — {sp['description']}")

    patterns_block = "\n".join(bg_lines) if bg_lines else ""

    # 4. Manager Profiles & 2026 Season Trajectories
    team_profiles = context_data.get("team_profiles_2026", {})
    playoff_info = context_data.get("playoffs_2026", {})
    persona_lines = []
    target_teams = list(dict.fromkeys(involved_team_ids or list(TEAM_NAMES.keys())))
    for tid in target_teams[:6]:
        m_name = TEAM_NAMES.get(tid)
        prof = team_profiles.get(m_name)
        p = personas.get(m_name, {})
        if prof:
            seed = prof.get("regular_season_seed")
            pts = prof.get("regular_season_points")
            traj = prof.get("trajectory", {})
            net = traj.get("net_points_change", 0.0)
            net_str = f"+{net}" if net > 0 else f"{net}"
            top_b = prof.get("top_batters", [{}])[0].get("name", "N/A")
            top_p = prof.get("top_pitchers", [{}])[0].get("name", "N/A")
            strengths = ", ".join(prof.get("category_strengths", [])[:2])
            persona_lines.append(
                f"  - {m_name} ({prof.get('team_name', '')}): Regular Season Seed #{seed} ({pts} pts, net {net_str} pts, peak: {traj.get('peak_points')} pts in W{traj.get('peak_week')}). Top Anchors: {top_b} & {top_p}. Key Strengths: {strengths}."
            )
        elif p:
            arch = p.get("archetype", "")
            tendencies = p.get("in_season_tendencies", p.get("tendencies", [""]))
            t_str = tendencies[0] if tendencies else ""
            persona_lines.append(f"  - {m_name} ({p.get('team_name', '')}): {arch}. Tendency: {t_str}.")

    personas_block = "\n".join(persona_lines) if persona_lines else ""

    # 5. In-Season Rivalries & Playoff Context
    rivalry_lines = []
    if playoff_info:
        for match in playoff_info.get("championship_bracket", []):
            h_owner, a_owner = match["home"]["owner"], match["away"]["owner"]
            if not involved_team_ids or (match["home"]["seed"] in involved_team_ids or match["away"]["seed"] in involved_team_ids):
                rivalry_lines.append(f"  - 🏆 {match['name']}: #{match['home']['seed']} {h_owner} vs #{match['away']['seed']} {a_owner} — {match['storyline']}")
    if rivalries:
        for r in rivalries[:2]:
            rivalry_lines.append(f"  - Clash: {r['name']} ({', '.join(r['managers'])}) — {r['narrative']}")

    rivalries_block = "\n".join(rivalry_lines) if rivalry_lines else ""

    output = f"""
REAL MLB NEWS & FANTASY ROSTER OVERLAP:
{news_block}

ACTIVE CATEGORY BATTLEGROUNDS & ROTO POINT TUG-OF-WARS (LIVE):
{active_battles_block}

SEASON-LONG CATEGORY BATTLEGROUNDS & PATTERNS:
{patterns_block}

LEAGUE MANAGER PROFILES & 2026 SEASON TRAJECTORIES:
{personas_block}

SEASON RIVALRIES & PLAYOFF BRACKET STAKES:
{rivalries_block}
"""
    return output.strip()


if __name__ == "__main__":
    print("Testing recap context generator with mock standings...")
    mock_records = [
        {"full_name": "James Wood", "team_id": 5},
        {"full_name": "Yordan Alvarez", "team_id": 1},
        {"full_name": "Kyle Schwarber", "team_id": 12},
        {"full_name": "Chris Sale", "team_id": 2},
        {"full_name": "Pete Crow-Armstrong", "team_id": 8}
    ]

    mock_standings = {
        5:  {"R": 1274, "HR": 338, "RBI": 1086, "OBP": 0.3424, "SB": 185, "QS": 135, "ERA": 3.80, "WHIP": 1.200, "K": 1986, "SV_HD": 194, "roto_points": 73.0, "standing": 1, "cat_points": {"OBP": 9.0, "K": 9.0, "QS": 9.0, "SV_HD": 9.0}},
        1:  {"R": 1297, "HR": 385, "RBI": 1278, "OBP": 0.3397, "SB": 257, "QS": 121, "ERA": 3.86, "WHIP": 1.250, "K": 1927, "SV_HD": 191, "roto_points": 70.5, "standing": 2, "cat_points": {"R": 9.0, "HR": 9.0, "RBI": 9.0, "SB": 9.0}},
        12: {"R": 1262, "HR": 323, "RBI": 1067, "OBP": 0.3311, "SB": 233, "QS": 118, "ERA": 3.53, "WHIP": 1.140, "K": 1957, "SV_HD": 179, "roto_points": 66.0, "standing": 3, "cat_points": {"WHIP": 9.0, "ERA": 8.0, "K": 8.0}},
        2:  {"R": 1220, "HR": 331, "RBI": 1139, "OBP": 0.3306, "SB": 177, "QS": 113, "ERA": 3.46, "WHIP": 1.170, "K": 1811, "SV_HD": 177, "roto_points": 60.0, "standing": 4, "cat_points": {"ERA": 9.0, "WHIP": 8.0}},
    }

    mock_delta = {
        5: {"points_change": 0.5, "rank_change": 0, "cat_delta": {"OBP": 1.0}},
        1: {"points_change": -0.5, "rank_change": 0, "cat_delta": {"OBP": -1.0}},
    }

    ctx = format_recap_context(mock_records, [5, 1, 12, 2], standings=mock_standings, delta=mock_delta)
    print("\n--- GENERATED CONTEXT INJECTION BLOCK ---")
    print(ctx)

