#!/usr/bin/env python3
"""
pipelines/sync_player_news.py

Syncs live player news directly from the ESPN Fantasy Baseball API
(syndicated upstream from RotoWire) for all drafted/pooled players.

Outputs a consolidated player-news.json dataset compatible with the
draft room modal and fantasy analytics dashboard.
"""

import os
import sys
import json
import time
import argparse
import urllib.request
import urllib.error
from typing import Dict, List, Any

DEFAULT_POOL_GCS = "https://storage.googleapis.com/fantasy-draft-2026/player-pool.json"
DEFAULT_OUTPUT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "player-news.json")
ESPN_NEWS_API_TEMPLATE = "https://site.api.espn.com/apis/fantasy/v2/games/flb/news/players?playerId={player_id}"

HEADERS = {
    "User-Agent": "curl/7.68.0",
    "Accept": "application/json"
}

def load_player_pool(pool_path: str = None) -> List[Dict[str, Any]]:
    """Loads player pool from local file, Supabase, or GCS."""
    if pool_path and os.path.exists(pool_path):
        print(f"📖 Reading player pool from local file: {pool_path}")
        with open(pool_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # Check data/player-pool.json
    local_data_pool = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "player-pool.json")
    if os.path.exists(local_data_pool):
        print(f"📖 Reading player pool from {local_data_pool}")
        with open(local_data_pool, "r", encoding="utf-8") as f:
            return json.load(f)

    # Fallback: Download from GCS
    print(f"🌐 Fetching player pool from {DEFAULT_POOL_GCS}...")
    req = urllib.request.Request(DEFAULT_POOL_GCS, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))

def fetch_player_news(player_id: str, timeout: int = 6) -> List[Dict[str, Any]]:
    """Fetches news feed for an individual ESPN player ID."""
    url = ESPN_NEWS_API_TEMPLATE.format(player_id=player_id)
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            feed = data.get("feed", [])
            items = []
            for item in feed:
                headline = item.get("headline") or ""
                story = item.get("story") or item.get("description") or ""
                last_modified = item.get("lastModified") or item.get("published") or item.get("categorized")
                news_type = item.get("type") or "Rotowire"
                if headline or story:
                    items.append({
                        "player_id": str(player_id),
                        "headline": headline,
                        "story": story,
                        "lastModified": last_modified,
                        "type": news_type
                    })
            return items
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        print(f"⚠️ HTTP {e.code} error for player {player_id}: {e.reason}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"⚠️ Error fetching news for player {player_id}: {e}", file=sys.stderr)
        return []

def main():
    parser = argparse.ArgumentParser(description="Sync live ESPN/RotoWire player news.")
    parser.add_argument("--pool", type=str, default=None, help="Path to player-pool.json")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_PATH, help="Output JSON path")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of players for testing")
    parser.add_argument("--delay", type=float, default=0.04, help="Delay between requests in seconds")
    args = parser.parse_args()

    players = load_player_pool(args.pool)
    print(f"Loaded {len(players)} players from pool.")

    # Deduplicate player IDs
    seen_ids = set()
    player_id_list = []
    for p in players:
        pid = str(p.get("ESPN PlayerID") or p.get("player_id") or p.get("id") or "").strip()
        name = p.get("Player") or p.get("full_name") or p.get("name") or "Unknown"
        if pid and pid != "0" and pid not in seen_ids:
            seen_ids.add(pid)
            player_id_list.append((pid, name))

    if args.limit:
        player_id_list = player_id_list[:args.limit]
        print(f"Limiting to first {len(player_id_list)} players.")

    print(f"Starting news scrape for {len(player_id_list)} unique players...")
    all_news = []
    success_count = 0

    for idx, (pid, name) in enumerate(player_id_list, start=1):
        items = fetch_player_news(pid)
        if items:
            all_news.extend(items)
            success_count += 1
        if idx % 25 == 0 or idx == len(player_id_list):
            print(f"[{idx}/{len(player_id_list)}] Fetched {len(all_news)} total news articles ({success_count} players with news)")
        if args.delay > 0:
            time.sleep(args.delay)

    # Sort descending by timestamp
    all_news.sort(key=lambda x: str(x.get("lastModified") or ""), reverse=True)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(all_news, f, indent=2, ensure_ascii=False)

    print(f"\n🎉 Successfully synced {len(all_news)} news articles across {success_count} players.")
    print(f"💾 Written to {args.output}")

if __name__ == "__main__":
    main()
