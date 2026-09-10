"""
pipelines/update_live_pickem_standings.py

Live YTD MLB Standings & WAR Projections Tracker for Annual Pick'em
-------------------------------------------------------------------
1. Fetches real-time MLB standings from MLB Stats API.
2. Fetches 2026 YTD actual stats and Rest-of-Season (ROS) projections from FanGraphs.
3. Computes projected full-season fWAR (Actual YTD WAR + ROS WAR) to determine
   interim leaders and top contenders for AL/NL MVP, Cy Young, and Rookie of the Year.
4. Evaluates all 2026 owner predictions against current live leaders, playoff spots,
   and crossover qualification rules.
5. Calculates interim projected point totals, ranks, and draft budget prizes.
6. Upserts the live projections snapshot into Supabase `pickem_seasons.live_projections`.
"""

import os
import sys
import json
import datetime
import requests
from typing import Dict, List, Any, Optional

DEFAULT_SUPABASE_URL = "https://wczdkcdqgtzlsbssogoz.supabase.co"
DEFAULT_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0."
    "wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44"
)

AL_TEAMS = {'BAL', 'BOS', 'NYY', 'TBR', 'TB', 'TOR', 'CHW', 'CWS', 'CLE', 'DET', 'KCR', 'KC', 'MIN', 'HOU', 'LAA', 'OAK', 'ATH', 'SEA', 'TEX'}
NL_TEAMS = {'ATL', 'MIA', 'NYM', 'PHI', 'WSN', 'WSH', 'CHC', 'CIN', 'MIL', 'PIT', 'STL', 'ARI', 'AZ', 'COL', 'LAD', 'SDP', 'SD', 'SFG', 'SF'}


def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.lower().replace(".", "").replace("'", "").replace("-", " ").strip()


MLB_TEAM_ALIASES = {
    'bal': 'BAL', 'baltimore': 'BAL', 'orioles': 'BAL', 'baltimore orioles': 'BAL',
    'bos': 'BOS', 'boston': 'BOS', 'red sox': 'BOS', 'boston red sox': 'BOS',
    'nyy': 'NYY', 'yankees': 'NYY', 'new york yankees': 'NYY',
    'tb': 'TB', 'tbr': 'TB', 'rays': 'TB', 'tampa bay rays': 'TB', 'tampa bay': 'TB',
    'tor': 'TOR', 'blue jays': 'TOR', 'toronto blue jays': 'TOR', 'toronto': 'TOR', 'jays': 'TOR',
    'cws': 'CWS', 'chw': 'CWS', 'white sox': 'CWS', 'chicago white sox': 'CWS',
    'cle': 'CLE', 'guardians': 'CLE', 'cleveland guardians': 'CLE', 'cleveland': 'CLE',
    'det': 'DET', 'tigers': 'DET', 'detroit tigers': 'DET', 'detroit': 'DET',
    'kc': 'KC', 'kcr': 'KC', 'royals': 'KC', 'kansas city royals': 'KC', 'kansas city': 'KC',
    'min': 'MIN', 'twins': 'MIN', 'minnesota twins': 'MIN', 'minnesota': 'MIN',
    'hou': 'HOU', 'astros': 'HOU', 'houston astros': 'HOU', 'houston': 'HOU',
    'laa': 'LAA', 'angels': 'LAA', 'los angeles angels': 'LAA',
    'ath': 'ATH', 'oak': 'ATH', 'athletics': 'ATH', 'oakland athletics': 'ATH', 'as': 'ATH',
    'sea': 'SEA', 'mariners': 'SEA', 'seattle mariners': 'SEA', 'seattle': 'SEA',
    'tex': 'TEX', 'rangers': 'TEX', 'texas rangers': 'TEX', 'texas': 'TEX',
    'atl': 'ATL', 'braves': 'ATL', 'atlanta braves': 'ATL', 'atlanta': 'ATL',
    'mia': 'MIA', 'marlins': 'MIA', 'miami marlins': 'MIA', 'miami': 'MIA',
    'nym': 'NYM', 'mets': 'NYM', 'new york mets': 'NYM',
    'phi': 'PHI', 'phillies': 'PHI', 'philadelphia phillies': 'PHI', 'philadelphia': 'PHI',
    'wsh': 'WSH', 'wsn': 'WSH', 'nationals': 'WSH', 'washington nationals': 'WSH', 'washington': 'WSH', 'nats': 'WSH',
    'chc': 'CHC', 'cubs': 'CHC', 'chicago cubs': 'CHC',
    'cin': 'CIN', 'reds': 'CIN', 'cincinnati reds': 'CIN', 'cincinnati': 'CIN',
    'mil': 'MIL', 'brewers': 'MIL', 'milwaukee brewers': 'MIL', 'milwaukee': 'MIL',
    'pit': 'PIT', 'pirates': 'PIT', 'pittsburgh pirates': 'PIT', 'pittsburgh': 'PIT',
    'stl': 'STL', 'cardinals': 'STL', 'st louis cardinals': 'STL', 'cards': 'STL',
    'az': 'AZ', 'ari': 'AZ', 'diamondbacks': 'AZ', 'arizona diamondbacks': 'AZ', 'arizona': 'AZ', 'dbacks': 'AZ',
    'col': 'COL', 'rockies': 'COL', 'colorado rockies': 'COL', 'colorado': 'COL',
    'lad': 'LAD', 'dodgers': 'LAD', 'los angeles dodgers': 'LAD',
    'sd': 'SD', 'sdp': 'SD', 'padres': 'SD', 'san diego padres': 'SD', 'san diego': 'SD',
    'sf': 'SF', 'sfg': 'SF', 'giants': 'SF', 'san francisco giants': 'SF', 'san francisco': 'SF'
}


def get_team_code(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    norm = normalize_text(s)
    clean = norm.replace(" ", "")
    if norm in MLB_TEAM_ALIASES:
        return MLB_TEAM_ALIASES[norm]
    if clean in MLB_TEAM_ALIASES:
        return MLB_TEAM_ALIASES[clean]
    return None


def matches_team_or_val(s1: Optional[str], s2: Optional[str]) -> bool:
    if not s1 or not s2:
        return False
    t1 = get_team_code(s1)
    t2 = get_team_code(s2)
    if t1 and t2:
        return t1 == t2
    n1 = normalize_text(s1)
    n2 = normalize_text(s2)
    return n1 == n2 or n1 in n2 or n2 in n1


def fetch_mlb_standings() -> Dict[str, Any]:
    """Fetch live MLB team standings from MLB Stats API."""
    print("⚾ Fetching live MLB Standings from statsapi.mlb.com...")
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&hydrate=team"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    div_map = {
        201: "al_east",
        202: "al_central",
        200: "al_west",
        204: "nl_east",
        205: "nl_central",
        203: "nl_west",
    }

    division_leaders = {}
    division_standings = {}
    all_teams = []
    wild_cards_al = []
    wild_cards_nl = []

    for rec in data.get("records", []):
        div_id = rec.get("division", {}).get("id")
        div_key = div_map.get(div_id)
        if not div_key:
            continue

        division_standings[div_key] = []
        for tr in rec.get("teamRecords", []):
            tname = tr["team"]["name"]
            w = int(tr.get("wins", 0))
            l = int(tr.get("losses", 0))
            pct = tr.get("winningPercentage", ".000")
            div_rank = tr.get("divisionRank", "99")
            wc_rank = tr.get("wildCardRank")
            is_al = div_id in (200, 201, 202)
            gb = tr.get("gamesBack", "-")

            entry = {
                "name": tname,
                "wins": w,
                "losses": l,
                "pct": pct,
                "div_rank": div_rank,
                "wc_rank": wc_rank,
                "is_al": is_al,
                "div_key": div_key,
                "gb": gb,
            }
            division_standings[div_key].append(entry)
            all_teams.append(entry)

            if div_rank == "1":
                division_leaders[div_key] = entry
            elif wc_rank and wc_rank in ("1", "2", "3"):
                if is_al:
                    wild_cards_al.append(entry)
                else:
                    wild_cards_nl.append(entry)

    wild_cards_al.sort(key=lambda x: int(x.get("wc_rank", 99)))
    wild_cards_nl.sort(key=lambda x: int(x.get("wc_rank", 99)))

    all_teams_sorted_wins = sorted(all_teams, key=lambda x: x["wins"], reverse=True)
    all_teams_sorted_losses = sorted(all_teams, key=lambda x: x["losses"], reverse=True)

    al_teams = [t for t in all_teams if t["is_al"]]
    nl_teams = [t for t in all_teams if not t["is_al"]]
    al_teams.sort(key=lambda x: x["wins"], reverse=True)
    nl_teams.sort(key=lambda x: x["wins"], reverse=True)

    return {
        "division_leaders": division_leaders,
        "division_standings": division_standings,
        "wild_cards_al": wild_cards_al,
        "wild_cards_nl": wild_cards_nl,
        "al_best": al_teams[0] if al_teams else None,
        "nl_best": nl_teams[0] if nl_teams else None,
        "most_wins": all_teams_sorted_wins[0] if all_teams_sorted_wins else None,
        "most_losses": all_teams_sorted_losses[0] if all_teams_sorted_losses else None,
        "all_playoff_al": [division_leaders.get("al_east", {}).get("name"),
                           division_leaders.get("al_central", {}).get("name"),
                           division_leaders.get("al_west", {}).get("name")] + [t["name"] for t in wild_cards_al[:3]],
        "all_playoff_nl": [division_leaders.get("nl_east", {}).get("name"),
                           division_leaders.get("nl_central", {}).get("name"),
                           division_leaders.get("nl_west", {}).get("name")] + [t["name"] for t in wild_cards_nl[:3]],
    }


def fetch_projected_war_leaders() -> Dict[str, Any]:
    """Fetch live FanGraphs YTD actuals and ROS projections to compute projected fWAR."""
    print("📊 Ingesting live FanGraphs actuals + ROS projections for awards...")

    # 1. Batting Actuals
    url_bat = "https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=bat&lg=all&qual=0&season=2026&season1=2026&startdate=2026-03-01&enddate=2026-11-01&month=0&team=0&pageitems=2500"
    r_bat = requests.get(url_bat, timeout=15).json().get("data", [])

    # 2. Pitching Actuals
    url_pit = "https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=pit&lg=all&qual=0&season=2026&season1=2026&startdate=2026-03-01&enddate=2026-11-01&month=0&team=0&pageitems=2500"
    r_pit = requests.get(url_pit, timeout=15).json().get("data", [])

    # 3. Rest of Season Projections
    url_ros_bat = "https://www.fangraphs.com/api/projections?type=rfangraphsdc&stats=bat&pos=all&team=0&players=0&lg=all"
    r_ros_bat = requests.get(url_ros_bat, timeout=15).json()

    url_ros_pit = "https://www.fangraphs.com/api/projections?type=rfangraphsdc&stats=pit&pos=all&team=0&players=0&lg=all"
    r_ros_pit = requests.get(url_ros_pit, timeout=15).json()

    ros_bat_war = {str(b.get("playerid")): float(b.get("WAR", 0) or 0) for b in r_ros_bat}
    ros_pit_war = {str(p.get("playerid")): float(p.get("WAR", 0) or 0) for p in r_ros_pit}

    al_batters, nl_batters = [], []
    for b in r_bat:
        pid = str(b.get("playerid") or "")
        name = b.get("PlayerName", "")
        team = b.get("TeamNameAbb", "")
        act_war = float(b.get("WAR", 0) or 0)
        ros_war = ros_bat_war.get(pid, 0.0)
        entry = {
            "name": name,
            "team": team,
            "act_war": round(act_war, 2),
            "ros_war": round(ros_war, 2),
            "proj_war": round(act_war + ros_war, 2),
            "hr": int(float(b.get("HR", 0) or 0)),
            "is_rookie": b.get("minormasterid") is not None or b.get("Age", 30) <= 24
        }
        if team in AL_TEAMS:
            al_batters.append(entry)
        elif team in NL_TEAMS:
            nl_batters.append(entry)

    al_pitchers, nl_pitchers = [], []
    for p in r_pit:
        pid = str(p.get("playerid") or "")
        name = p.get("PlayerName", "")
        team = p.get("TeamNameAbb", "")
        act_war = float(p.get("WAR", 0) or 0)
        ros_war = ros_pit_war.get(pid, 0.0)
        entry = {
            "name": name,
            "team": team,
            "act_war": round(act_war, 2),
            "ros_war": round(ros_war, 2),
            "proj_war": round(act_war + ros_war, 2),
            "so": int(float(p.get("SO", 0) or 0)),
            "era": round(float(p.get("ERA", 0) or 0), 2),
        }
        if team in AL_TEAMS:
            al_pitchers.append(entry)
        elif team in NL_TEAMS:
            nl_pitchers.append(entry)

    al_batters.sort(key=lambda x: x["proj_war"], reverse=True)
    nl_batters.sort(key=lambda x: x["proj_war"], reverse=True)
    al_pitchers.sort(key=lambda x: x["proj_war"], reverse=True)
    nl_pitchers.sort(key=lambda x: x["proj_war"], reverse=True)

    # Rookies
    # Known 2026 rookie contenders based on picks & minors status
    al_rookie_names = {"kevin mcgonigle", "kazuma okamoto", "munetaka murakami", "trey yesavage", "colton cowser", "wyatt langford", "luis gil"}
    nl_rookie_names = {"sal stewart", "nolan mclean", "mclean", "bubba chandler", "jj wetherholt", "konnor griffin", "paul skenes", "jackson merrill"}

    al_rookies = [b for b in al_batters if normalize_text(b["name"]) in al_rookie_names]
    if not al_rookies:
        al_rookies = al_batters[:3]

    nl_rookies = [b for b in nl_batters if normalize_text(b["name"]) in nl_rookie_names]
    if not nl_rookies:
        nl_rookies = nl_batters[:3]

    return {
        "al_mvp": al_batters,
        "nl_mvp": nl_batters,
        "al_cy_young": al_pitchers,
        "nl_cy_young": nl_pitchers,
        "al_roy": al_rookies,
        "nl_roy": nl_rookies,
    }


def build_live_in_progress_snapshot() -> Dict[str, Any]:
    """Combine MLB standings and FanGraphs WAR into a complete 23-category live snapshot."""
    standings = fetch_mlb_standings()
    awards = fetch_projected_war_leaders()

    categories = {}

    # 1. Divisions
    for div in ["al_east", "al_central", "al_west", "nl_east", "nl_central", "nl_west"]:
        leader = standings["division_leaders"].get(div)
        st = standings["division_standings"].get(div, [])
        runner_up = st[1] if len(st) > 1 else None
        categories[div] = {
            "leader": leader["name"] if leader else "TBD",
            "stat": f"{leader['wins']}-{leader['losses']} ({leader['pct']})" if leader else "",
            "runner_up": f"{runner_up['name']} ({runner_up['gb']} GB)" if runner_up else "",
            "type": "division",
        }

    # 2. Wild Cards
    for idx, wc in enumerate(standings["wild_cards_al"][:3]):
        key = f"al_wc_{idx+1}"
        categories[key] = {
            "leader": wc["name"],
            "stat": f"{wc['wins']}-{wc['losses']} (AL WC #{idx+1})",
            "type": "wild_card",
        }

    for idx, wc in enumerate(standings["wild_cards_nl"][:3]):
        key = f"nl_wc_{idx+1}"
        categories[key] = {
            "leader": wc["name"],
            "stat": f"{wc['wins']}-{wc['losses']} (NL WC #{idx+1})",
            "type": "wild_card",
        }

    # 3. Pennants & World Series
    al_best = standings["al_best"]
    nl_best = standings["nl_best"]
    ws_fav = standings["most_wins"]

    categories["al_pennant"] = {
        "leader": al_best["name"] if al_best else "TBD",
        "stat": f"{al_best['wins']}-{al_best['losses']} (Best AL Record)",
        "type": "playoff_result",
    }
    categories["nl_pennant"] = {
        "leader": nl_best["name"] if nl_best else "TBD",
        "stat": f"{nl_best['wins']}-{nl_best['losses']} (Best NL Record)",
        "type": "playoff_result",
    }
    categories["world_series"] = {
        "leader": ws_fav["name"] if ws_fav else "TBD",
        "stat": f"{ws_fav['wins']}-{ws_fav['losses']} (Best MLB Record)",
        "type": "playoff_result",
    }

    # 4. Individual Awards (WAR leaders)
    for award_key, label in [
        ("al_mvp", "AL MVP"),
        ("nl_mvp", "NL MVP"),
        ("al_cy_young", "AL Cy Young"),
        ("nl_cy_young", "NL Cy Young"),
        ("al_roy", "AL ROY"),
        ("nl_roy", "NL ROY"),
    ]:
        candidates = awards.get(award_key, [])
        top = candidates[0] if candidates else {"name": "TBD", "proj_war": 0}
        contenders = [f"{c['name']} ({c['proj_war']} WAR)" for c in candidates[1:4]]
        categories[award_key] = {
            "leader": top["name"],
            "stat": f"{top['proj_war']} Proj. fWAR (Leading {label})",
            "contenders": contenders,
            "type": "award",
        }

    # 5. Extreme Wins / Losses
    mw = standings["most_wins"]
    ml = standings["most_losses"]
    categories["most_wins"] = {
        "leader": mw["name"] if mw else "TBD",
        "stat": f"{mw['wins']} Wins ({mw['wins']}-{mw['losses']})",
        "type": "extremes",
    }
    categories["most_losses"] = {
        "leader": ml["name"] if ml else "TBD",
        "stat": f"{ml['losses']} Losses ({ml['losses']}-{ml['wins']})",
        "type": "extremes",
    }

    snapshot = {
        "as_of": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "season_year": 2026,
        "categories": categories,
        "playoff_al": standings["all_playoff_al"],
        "playoff_nl": standings["all_playoff_nl"],
    }

    return snapshot


def evaluate_owner_projected_scores(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Evaluate current 2026 picks against the live in-progress snapshot."""
    url = os.environ.get("VITE_SUPABASE_URL") or DEFAULT_SUPABASE_URL
    key = os.environ.get("VITE_SUPABASE_ANON_KEY") or DEFAULT_SUPABASE_KEY
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}

    picks = requests.get(f"{url}/rest/v1/pickem_picks?season_year=eq.2026", headers=headers).json()
    questions = {q["id"]: q for q in requests.get(f"{url}/rest/v1/pickem_questions?season_year=eq.2026", headers=headers).json()}

    categories = snapshot["categories"]
    playoff_al = [normalize_text(t) for t in snapshot["playoff_al"]]
    playoff_nl = [normalize_text(t) for t in snapshot["playoff_nl"]]

    owner_scores = {}
    owner_hits = {}
    owner_team_ids = {}

    for p in picks:
        q = questions.get(p["question_id"])
        if not q:
            continue
        k = q["question_key"]
        owner = p["owner_name"]
        val = p["pick_value"]
        team_id = p.get("team_id")
        owner_team_ids[owner] = team_id

        if owner not in owner_scores:
            owner_scores[owner] = 0
            owner_hits[owner] = []

        target_info = categories.get(k, {})
        target = target_info.get("leader", "")

        n_val = normalize_text(val)
        n_target = normalize_text(target)

        pts = 0
        hit_label = ""

        # Check Exact Match
        if matches_team_or_val(val, target):
            pts = q.get("points_exact", 3)
            hit_label = "EXACT"
        elif q["category"] in ("division", "wild_card"):
            # Playoff Crossover: 2 points for correct team in wrong playoff spot
            in_al = any(matches_team_or_val(val, t) for t in snapshot["playoff_al"])
            in_nl = any(matches_team_or_val(val, t) for t in snapshot["playoff_nl"])
            if (k.startswith("al_") and in_al) or (k.startswith("nl_") and in_nl):
                pts = 2
                hit_label = "PLAYOFF_CROSSOVER"

        if pts > 0:
            owner_scores[owner] += pts
            owner_hits[owner].append({
                "question_key": k,
                "question_label": q["question_label"],
                "pick_value": val,
                "hit_type": hit_label,
                "points": pts,
            })

    sorted_owners = sorted(owner_scores.items(), key=lambda x: x[1], reverse=True)
    results = []
    current_rank = 1
    for idx, (owner, total) in enumerate(sorted_owners):
        if idx > 0 and total < sorted_owners[idx - 1][1]:
            current_rank = idx + 1
        place = current_rank
        prize = None
        if place == 1:
            prize = 4
        elif place == 2:
            prize = 3
        elif place == 3:
            prize = 2
        elif place == 4:
            prize = 1

        results.append({
            "place": place,
            "owner_name": owner,
            "team_id": owner_team_ids.get(owner),
            "projected_points": total,
            "projected_budget": prize,
            "hits": owner_hits.get(owner, []),
            "total_hits": len(owner_hits.get(owner, [])),
        })

    return results


def run():
    print("🚀 Starting Live MLB Standings & WAR Projections Tracker...")
    snapshot = build_live_in_progress_snapshot()
    projected_standings = evaluate_owner_projected_scores(snapshot)
    snapshot["projected_standings"] = projected_standings

    print("\n🏆 Projected 2026 Pick'em Leaderboard (Based on Live MLB Data):")
    for s in projected_standings:
        p_badge = f"+${s['projected_budget']} Budget" if s['projected_budget'] else "$0"
        print(f"  #{s['place']} {s['owner_name']}: {s['projected_points']} pts ({s['total_hits']} active hits, {p_badge})")

    # Upsert to Supabase
    url = os.environ.get("VITE_SUPABASE_URL") or DEFAULT_SUPABASE_URL
    key = os.environ.get("VITE_SUPABASE_ANON_KEY") or DEFAULT_SUPABASE_KEY
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    print("\n💾 Updating Supabase `pickem_seasons.live_projections` for 2026...")
    res = requests.patch(
        f"{url}/rest/v1/pickem_seasons?season_year=eq.2026",
        headers=headers,
        data=json.dumps({"live_projections": snapshot}),
        timeout=15,
    )
    if res.status_code in (200, 204):
        print("✅ Successfully updated live projections in Supabase!")
    else:
        print(f"⚠️ Supabase update failed: {res.status_code} {res.text}")


if __name__ == "__main__":
    run()
