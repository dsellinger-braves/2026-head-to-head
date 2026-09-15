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


def format_recap_context(
    today_records: Optional[List[dict]] = None,
    involved_team_ids: Optional[List[int]] = None
) -> str:
    """
    Generate comprehensive context injection text including:
    1. Real MLB headlines matched to fantasy owners
    2. Manager personas & banter triggers
    3. Active rivalries & historical trade lore
    """
    context_data = load_league_context()
    personas = context_data.get("manager_personas", {})
    rivalries = context_data.get("rivalries", [])
    trade_lore = context_data.get("trade_lore", [])

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

    # 2. Manager Lore & Personas
    persona_lines = []
    target_teams = list(dict.fromkeys(involved_team_ids or list(TEAM_NAMES.keys())))
    for tid in target_teams[:5]:
        m_name = TEAM_NAMES.get(tid)
        p = personas.get(m_name)
        if p:
            arch = p.get("archetype", "")
            banter = p.get("banter_triggers", [""])[0]
            persona_lines.append(f"  - {m_name} ({p.get('team_name', '')}): {arch}. Banter: {banter}")

    personas_block = "\n".join(persona_lines) if persona_lines else ""

    # 3. Notable Rivalries & Trade Lore
    lore_lines = []
    if rivalries:
        for r in rivalries[:2]:
            lore_lines.append(f"  - Rivalry: {r['name']} ({', '.join(r['managers'])}) — {r['narrative']}")
    if trade_lore:
        for t in trade_lore[:2]:
            lore_lines.append(f"  - Trade Lore ({t['deal']}): {t['summary']} -> Result: {t['outcome']}")

    lore_block = "\n".join(lore_lines) if lore_lines else ""

    output = f"""
REAL MLB NEWS & FANTASY ROSTER OVERLAP:
{news_block}

LEAGUE MANAGER PERSONAS & BANTER LORE:
{personas_block}

HISTORICAL RIVALRIES & TRADE LORE:
{lore_block}
"""
    return output.strip()


if __name__ == "__main__":
    print("Testing recap context generator...")
    # Mock records for testing
    mock_records = [
        {"full_name": "Aaron Judge", "team_id": 14},
        {"full_name": "Pete Alonso", "team_id": 1},
        {"full_name": "Bobby Witt Jr.", "team_id": 6},
        {"full_name": "Corbin Carroll", "team_id": 13},
        {"full_name": "Gunnar Henderson", "team_id": 3}
    ]
    ctx = format_recap_context(mock_records, [1, 14, 6])
    print("\n--- GENERATED CONTEXT INJECTION BLOCK ---")
    print(ctx)
