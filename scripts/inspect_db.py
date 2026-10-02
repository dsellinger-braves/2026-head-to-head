#!/usr/bin/env python3
"""
scripts/inspect_db.py

Safe, structured CLI inspector for Supabase database tables in the
2026 Head to Head Heftystrong project. Eliminates ad-hoc bash one-liners.

Usage:
    python scripts/inspect_db.py keepers [--season 2027] [--owner Daniel] [--team 5]
    python scripts/inspect_db.py budgets [--season 2027]
    python scripts/inspect_db.py roster [--period 195] [--owner Will] [--team 12]
    python scripts/inspect_db.py pool --search "Ohtani"
"""

import os
import sys
import argparse
import requests
from typing import Dict, Any, List

DEFAULT_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_KEY = (
    os.environ.get("VITE_SUPABASE_ANON_KEY")
    or os.environ.get("SUPABASE_KEY")
    or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

TEAM_OWNERS = {
    1: "Tim", 2: "Adrian", 3: "Garrett", 5: "Daniel",
    6: "Anil", 8: "Alex", 12: "Will", 13: "Mark", 14: "Preston"
}
OWNER_TO_TEAM = {v.lower(): k for k, v in TEAM_OWNERS.items()}
OWNER_TO_TEAM["dan"] = 5


def get_headers() -> Dict[str, str]:
    return {
        "apikey": DEFAULT_KEY,
        "Authorization": f"Bearer {DEFAULT_KEY}",
        "Content-Type": "application/json"
    }


def cmd_keepers(args: argparse.Namespace) -> None:
    season = args.season
    url = f"{DEFAULT_URL}/rest/v1/draft_keepers?season_year=eq.{season}&order=team_id.asc,keeper_slot.asc"
    
    if args.team:
        url += f"&team_id=eq.{args.team}"
    elif args.owner:
        owner_name = args.owner.strip().capitalize()
        url += f"&owner=ilike.{owner_name}"

    resp = requests.get(url, headers=get_headers(), timeout=15)
    if resp.status_code != 200:
        print(f"Error ({resp.status_code}): {resp.text}")
        sys.exit(1)

    keepers: List[Dict[str, Any]] = resp.json()
    if not keepers:
        print(f"No keepers found for season {season}.")
        return

    print(f"\n🏷️  Official Keepers ({season}) — Total: {len(keepers)}")
    print(f"{'Slot':<5} {'Owner':<10} {'Player':<24} {'Pos':<6} {'MLB':<5} {'Rank':<6} {'Cost':<6} {'Token':<6} {'Prior':<6}")
    print("-" * 78)

    current_owner = None
    for k in keepers:
        owner = k.get("owner", "Unknown")
        if owner != current_owner:
            if current_owner is not None:
                print("-" * 78)
            current_owner = owner

        slot = k.get("keeper_slot", 0)
        pname = k.get("player_name", "---")
        pos = k.get("position", "---")
        team = k.get("mlb_team", "---")
        rank = k.get("rank") or "NR"
        cost = k.get("cost", 0)
        token = "YES" if k.get("token_applied") else "NO"
        prior = k.get("prior_cost") if k.get("prior_cost") is not None else "-"

        print(f"#{slot:<4} {owner:<10} {pname:<24} {pos:<6} {team:<5} #{rank:<5} ${cost:<5} {token:<6} ${prior}")
    print()


def cmd_budgets(args: argparse.Namespace) -> None:
    season = args.season
    url = f"{DEFAULT_URL}/rest/v1/draft_team_budgets?season_year=eq.{season}&order=team_id.asc"
    resp = requests.get(url, headers=get_headers(), timeout=15)
    if resp.status_code != 200:
        print(f"Error ({resp.status_code}): {resp.text}")
        sys.exit(1)

    budgets: List[Dict[str, Any]] = resp.json()
    if not budgets:
        print(f"No budgets found for season {season}.")
        return

    print(f"\n💰 Team Budgets ({season})")
    print(f"{'Team':<6} {'Owner':<12} {'Base':<8} {'Keepers':<10} {'Comp Spend':<12} {'Final':<8}")
    print("-" * 60)

    for b in budgets:
        tid = b.get("team_id", 0)
        owner = b.get("owner", "Unknown")
        base = b.get("base_budget", 100)
        k_spend = b.get("keeper_spend", 0)
        comp = b.get("comp_pick_spend", 0)
        final = b.get("final_budget", base - k_spend - comp)
        print(f"#{tid:<5} {owner:<12} ${base:<7} ${k_spend:<9} ${comp:<11} ${final:<7}")
    print()


def cmd_roster(args: argparse.Namespace) -> None:
    period = args.period
    if not period:
        sp_res = requests.get(
            f"{DEFAULT_URL}/rest/v1/player_daily_stats?select=scoring_period_id&order=scoring_period_id.desc&limit=1",
            headers=get_headers(),
            timeout=15
        )
        period = sp_res.json()[0]["scoring_period_id"] if sp_res.status_code == 200 and sp_res.json() else 195

    url = f"{DEFAULT_URL}/rest/v1/player_daily_stats?scoring_period_id=eq.{period}&select=team_id,player_id,full_name,lineup_slot_id&order=team_id.asc,full_name.asc"
    
    target_team = args.team
    if not target_team and args.owner:
        target_team = OWNER_TO_TEAM.get(args.owner.strip().lower())

    if target_team:
        url += f"&team_id=eq.{target_team}"

    resp = requests.get(url, headers=get_headers(), timeout=15)
    if resp.status_code != 200:
        print(f"Error ({resp.status_code}): {resp.text}")
        sys.exit(1)

    rows: List[Dict[str, Any]] = resp.json()
    print(f"\n📋 Active Rosters (Scoring Period {period}) — Total: {len(rows)} players")
    print(f"{'Team ID':<8} {'Owner':<12} {'ESPN ID':<10} {'Player Name':<28} {'Slot':<6}")
    print("-" * 68)

    for r in rows:
        tid = r.get("team_id", 0)
        owner = TEAM_OWNERS.get(tid, f"Team {tid}")
        pid = r.get("player_id", "")
        pname = r.get("full_name", "")
        slot = r.get("lineup_slot_id", "")
        print(f"#{tid:<7} {owner:<12} {pid:<10} {pname:<28} {slot:<6}")
    print()


def cmd_pool(args: argparse.Namespace) -> None:
    q = args.search.strip()
    url = f"{DEFAULT_URL}/rest/v1/player-pool?Player=ilike.*{q}*&limit={args.limit}"
    resp = requests.get(url, headers=get_headers(), timeout=15)
    if resp.status_code != 200:
        print(f"Error ({resp.status_code}): {resp.text}")
        sys.exit(1)

    rows: List[Dict[str, Any]] = resp.json()
    print(f"\n🔍 Player Pool Search ('{q}') — Total Found: {len(rows)}")
    print(f"{'ESPN ID':<10} {'Player Name':<28} {'Pos':<8} {'Team':<6} {'Availability':<14} {'Price':<6}")
    print("-" * 76)

    for p in rows:
        pid = p.get("ESPN PlayerID", "")
        name = p.get("Player", "")
        pos = p.get("Position", "")
        team = p.get("Team", "")
        avail = p.get("Availability", "Available")
        price = p.get("Hefty Keeper Price", 0)
        print(f"{pid:<10} {name:<28} {pos:<8} {team:<6} {avail:<14} ${price}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Supabase fantasy baseball data tables safely.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Keepers
    p_k = subparsers.add_parser("keepers", help="Inspect official keepers")
    p_k.add_argument("--season", type=int, default=2027, help="Season year (default 2027)")
    p_k.add_argument("--owner", type=str, help="Filter by owner name")
    p_k.add_argument("--team", type=int, help="Filter by team ID")
    p_k.set_defaults(func=cmd_keepers)

    # Budgets
    p_b = subparsers.add_parser("budgets", help="Inspect team budgets")
    p_b.add_argument("--season", type=int, default=2027, help="Season year (default 2027)")
    p_b.set_defaults(func=cmd_budgets)

    # Roster
    p_r = subparsers.add_parser("roster", help="Inspect active rosters")
    p_r.add_argument("--period", type=int, help="Scoring period ID (defaults to latest)")
    p_r.add_argument("--owner", type=str, help="Filter by owner name")
    p_r.add_argument("--team", type=int, help="Filter by team ID")
    p_r.set_defaults(func=cmd_roster)

    # Pool
    p_p = subparsers.add_parser("pool", help="Search player pool")
    p_p.add_argument("--search", type=str, required=True, help="Player name query")
    p_p.add_argument("--limit", type=int, default=10, help="Max results (default 10)")
    p_p.set_defaults(func=cmd_pool)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
