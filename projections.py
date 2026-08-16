"""
projections.py — FanGraphs ZiPS Rest-of-Season Projections
Fetches batting and pitching ROS projections, caches them in memory,
and provides helpers to forecast final roto standings and look up
individual player projections.

Used by discord-bot.py to enrich /ask answers with projection data.
"""

import re
import time
import json
import requests
from typing import Optional

# ---------------------------------------------------------------------------
# FANGRAPHS API ENDPOINTS
# ---------------------------------------------------------------------------

FANGRAPHS_BAT_URL = (
    "https://www.fangraphs.com/api/projections"
    "?type=rzips&stats=bat&pos=all&team=0&players=0&lg=all&download=1"
)
FANGRAPHS_PIT_URL = (
    "https://www.fangraphs.com/api/projections"
    "?type=rzips&stats=pit&pos=all&team=0&players=0&lg=all&download=1"
)

# Cache TTL: 6 hours (projections update infrequently)
_CACHE_TTL = 6 * 60 * 60
_cache: dict[str, tuple[float, object]] = {}

# ---------------------------------------------------------------------------
# TEAM / SLOT CONSTANTS  (must stay in sync with discord-bot.py)
# ---------------------------------------------------------------------------

TEAM_NAMES = {
    1:  "Tim",
    2:  "Adrian",
    3:  "Garrett",
    5:  "Dan",
    6:  "Anil",
    8:  "Alex",
    12: "Will",
    13: "Mark",
    14: "Preston",
}

ROTO_CATS = ["R", "HR", "RBI", "OBP", "SB", "QS", "ERA", "WHIP", "K", "SV_HD"]
CAT_HIGHER_IS_BETTER = {
    "R": True, "HR": True, "RBI": True, "OBP": True, "SB": True,
    "QS": True, "ERA": False, "WHIP": False, "K": True, "SV_HD": True,
}
CAT_DISPLAY = {
    "R": "R", "HR": "HR", "RBI": "RBI", "OBP": "OBP", "SB": "SB",
    "QS": "QS", "ERA": "ERA", "WHIP": "WHIP", "K": "K", "SV_HD": "SV+H",
}

BENCH_IL_SLOTS    = {16, 17, 20, 21, 22}
HITTING_SLOT_IDS  = {0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 19}
PITCHING_SLOT_IDS = {13, 14, 15}

# ---------------------------------------------------------------------------
# FETCHING + CACHING
# ---------------------------------------------------------------------------

def _fg_get(url: str) -> list[dict]:
    """Fetch a FanGraphs projection endpoint with caching."""
    if url in _cache:
        ts, data = _cache[url]
        if time.time() - ts < _CACHE_TTL:
            return data
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        _cache[url] = (time.time(), data)
        print(f"  FanGraphs: fetched {len(data)} rows from {'bat' if 'bat' in url else 'pit'} projections")
        return data
    except Exception as e:
        print(f"  FanGraphs fetch failed ({url[:60]}...): {e}")
        # Return stale cache if available
        if url in _cache:
            return _cache[url][1]
        return []


def fetch_projections() -> dict[str, list[dict]]:
    """Fetch both batting and pitching ROS projections."""
    return {
        "batting":  _fg_get(FANGRAPHS_BAT_URL),
        "pitching": _fg_get(FANGRAPHS_PIT_URL),
    }

# ---------------------------------------------------------------------------
# PROJECTION MAPS  (keyed by MLBAM ID for fast roster matching)
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation and suffixes for fuzzy matching."""
    name = name.lower().strip()
    name = re.sub(r"\'s\b", "", name)  # strip possessive 's
    name = re.sub(r"[.\'\-]", "", name)
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", name).strip()
    return name


def build_projection_map(raw: list[dict], stat_type: str) -> dict[int, dict]:
    """
    Transform raw FanGraphs JSON into {mlbam_id: projection_dict}.

    stat_type: 'batting' or 'pitching'
    Maps FanGraphs field names to our internal stat names.
    """
    proj_map: dict[int, dict] = {}

    for row in raw:
        mlbam_id = row.get("xMLBAMID")
        if not mlbam_id:
            continue

        if stat_type == "batting":
            proj_map[mlbam_id] = {
                "full_name":    row.get("PlayerName", ""),
                "team":         row.get("Team", ""),
                "stat_type":    "batting",
                "G":            row.get("G", 0),
                "AB":           row.get("AB", 0),
                "PA":           row.get("PA", 0),
                "H":            row.get("H", 0),
                "HR":           row.get("HR", 0),
                "R":            row.get("R", 0),
                "RBI":          row.get("RBI", 0),
                "BB":           row.get("BB", 0),
                "HBP":          row.get("HBP", 0),
                "SB":           row.get("SB", 0),
                "CS":           row.get("CS", 0),
                "SF":           row.get("SF", 0),
                "SO":           row.get("SO", 0),
                "AVG":          row.get("AVG", 0),
                "OBP":          row.get("OBP", 0),
                "SLG":          row.get("SLG", 0),
                "WAR":          row.get("WAR", 0),
            }
        else:  # pitching
            # FanGraphs uses IP as decimal innings, SO for strikeouts
            # SV and HLD may be null for starters
            ip = row.get("IP", 0) or 0
            proj_map[mlbam_id] = {
                "full_name":    row.get("PlayerName", ""),
                "team":         row.get("Team", ""),
                "stat_type":    "pitching",
                "G":            row.get("G", 0),
                "GS":           row.get("GS", 0),
                "IP":           ip,
                "K":            row.get("SO", 0),
                "ER":           row.get("ER", 0),
                "H_Allowed":    row.get("H", 0),
                "BB_Allowed":   row.get("BB", 0),
                "HR_Allowed":   row.get("HR", 0),
                "SV":           row.get("SV", 0) or 0,
                "HD":           row.get("HLD", 0) or 0,
                "ERA":          row.get("ERA", 0),
                "WHIP":         row.get("WHIP", 0),
                "W":            row.get("W", 0),
                "L":            row.get("L", 0),
                "WAR":          row.get("WAR", 0),
                # Estimate QS: ~60% of starts for pitchers with ERA < 4.50 and GS > 0
                # This is a common fantasy heuristic when QS isn't directly projected
                "QS":           _estimate_qs(row),
            }

    return proj_map


def _estimate_qs(row: dict) -> int:
    """
    Estimate quality starts from FanGraphs projection data.
    Heuristic: QS rate ≈ based on ERA and IP/GS ratio.
    - ERA < 3.00 → ~70% QS rate
    - ERA 3.00-3.75 → ~55% QS rate
    - ERA 3.75-4.50 → ~40% QS rate
    - ERA > 4.50 → ~25% QS rate
    """
    gs = row.get("GS", 0) or 0
    era = row.get("ERA", 0) or 0
    if gs == 0:
        return 0
    if era < 3.00:
        rate = 0.70
    elif era < 3.75:
        rate = 0.55
    elif era < 4.50:
        rate = 0.40
    else:
        rate = 0.25
    return round(gs * rate)

# ---------------------------------------------------------------------------
# TEAM ROS PROJECTION
# ---------------------------------------------------------------------------

def _get_stat_val(row: dict, stat_name: str) -> float:
    stats = row.get("stats", {})
    if isinstance(stats, str):
        try:
            stats = json.loads(stats)
        except Exception:
            stats = {}
    return float(stats.get(stat_name, 0))


def project_team_ros(
    team_id: int,
    roster_records: list[dict],
    proj_bat: dict[int, dict],
    proj_pit: dict[int, dict],
    ytd_records: list[dict],
    current_period: int,
) -> dict:
    """
    Sum ROS projections for all rostered players on a team (including bench and IL).

    roster_records: current roster from ESPN or Supabase (including BE and IL slots)
    ytd_records:    all active YTD stats records (for player-level QS/SV/HD calculations)
    current_period: current scoring period ID (day number)
    Returns a dict of projected ROS counting stats + rate stat components.
    """
    TOTAL_SCORING_PERIODS = 186
    # Get unique player IDs on this team (including active, BE, and IL slots)
    team_players: dict[int, int] = {}  # player_id -> lineup_slot_id
    for row in roster_records:
        if row["team_id"] != team_id:
            continue
        slot = row.get("lineup_slot_id")
        pid = row["player_id"]
        if pid not in team_players:
            team_players[pid] = slot

    # Accumulate projected stats
    totals = {
        # Hitting
        "R": 0, "HR": 0, "RBI": 0, "SB": 0,
        "H": 0, "BB": 0, "HBP": 0, "PA": 0, "AB": 0, "SF": 0,
        # Pitching
        "K": 0, "QS": 0, "IP": 0, "ER": 0,
        "H_Allowed": 0, "BB_Allowed": 0, "SV": 0, "HD": 0,
    }
    matched = 0

    for pid, slot in team_players.items():
        is_bench = (slot == 16)
        is_il = (slot in {17, 20, 21, 22})
        is_inactive = is_bench or is_il
        is_hitter_slot = (slot in HITTING_SLOT_IDS or is_inactive)
        is_pitcher_slot = (slot in PITCHING_SLOT_IDS or is_inactive)

        if pid in proj_bat and is_hitter_slot:
            p = proj_bat[pid]
            # Batters on the bench count for 50%, while batters on active/IL count for 100%
            factor = 0.5 if is_bench else 1.0
            for stat in ["R", "HR", "RBI", "SB", "H", "BB", "HBP", "PA", "AB", "SF"]:
                totals[stat] += p.get(stat, 0) * factor
            matched += 1

        # Note: If two-way player on the bench, they can match both blocks
        if pid in proj_pit and is_pitcher_slot:
            p = proj_pit[pid]
            # Pitchers on the bench count for 100%
            factor = 1.0
            # Accumulate standard pitching stats (ignoring SV/HD since we project them via player YTD rates)
            for stat in ["K", "ER", "H_Allowed", "BB_Allowed"]:
                totals[stat] += p.get(stat, 0) * factor
            # Convert FanGraphs decimal IP to ESPN outs format (×3)
            totals["IP"] += p.get("IP", 0) * 3 * factor
            matched += 1

            # Quality Starts: Player YTD actual starts rate applied to remaining projected starts
            gs_proj = p.get("GS", 0) or 0
            if gs_proj > 0:
                # Count actual games started in active slots (SP=13 or P=15) where pitcher threw a pitch
                gs_actual = sum(
                    1 for r in ytd_records
                    if r["player_id"] == pid
                    and r.get("lineup_slot_id") in (13, 15)
                    and _get_stat_val(r, "IP") > 0
                )
                qs_actual = sum(
                    _get_stat_val(r, "QS") for r in ytd_records
                    if r["player_id"] == pid
                )
                if gs_actual > 0:
                    qs_rate = min(1.0, max(0.0, qs_actual / gs_actual))
                else:
                    # Fallback to ERA-based estimation if no YTD starts yet
                    qs_rate = _estimate_qs(p) / gs_proj if gs_proj > 0 else 0
                
                totals["QS"] += gs_proj * qs_rate * factor

            # Save + Holds: player-level YTD actual SV+HD rate per period extrapolated to remaining periods
            ytd_sv = sum(_get_stat_val(r, "SV") for r in ytd_records if r["player_id"] == pid)
            ytd_hd = sum(_get_stat_val(r, "HD") for r in ytd_records if r["player_id"] == pid)
            extrap_ratio = (TOTAL_SCORING_PERIODS - current_period) / max(1, current_period)
            totals["SV"] += ytd_sv * extrap_ratio * factor
            totals["HD"] += ytd_hd * extrap_ratio * factor

    totals["_matched"] = matched
    totals["_roster_size"] = len(team_players)
    return totals

# ---------------------------------------------------------------------------
# COMBINED YTD + ROS STANDINGS FORECAST
# ---------------------------------------------------------------------------

def _espn_ip_to_innings(ip_val: float) -> float:
    """ESPN stores IP as total outs. Divide by 3 for decimal innings."""
    return ip_val / 3.0


def _compute_roto_points(team_cats: dict) -> dict:
    """Assign roto points per category across teams. Same logic as discord-bot.py."""
    n = len(team_cats)
    cat_points: dict[int, dict] = {tid: {} for tid in team_cats}

    for cat, higher in CAT_HIGHER_IS_BETTER.items():
        sorted_teams = sorted(team_cats.items(), key=lambda x: x[1][cat], reverse=higher)
        i = 0
        while i < len(sorted_teams):
            j = i
            while j < len(sorted_teams) - 1 and sorted_teams[j][1][cat] == sorted_teams[j+1][1][cat]:
                j += 1
            avg = sum(n - k for k in range(i, j + 1)) / (j - i + 1)
            for k in range(i, j + 1):
                cat_points[sorted_teams[k][0]][cat] = round(avg, 1)
            i = j + 1

    for tid in team_cats:
        team_cats[tid]["cat_points"]  = cat_points[tid]
        team_cats[tid]["roto_points"] = round(sum(cat_points[tid].values()), 1)

    for rank, (tid, _) in enumerate(
        sorted(team_cats.items(), key=lambda x: x[1]["roto_points"], reverse=True), 1
    ):
        team_cats[tid]["standing"] = rank

    return team_cats


def forecast_final_standings(
    ytd_records: list[dict],
    roster_records: list[dict],
    proj_bat: dict[int, dict],
    proj_pit: dict[int, dict],
    current_period: int,
) -> dict:
    """
    Combine actual YTD stats with projected ROS stats to forecast final standings.

    ytd_records:     all active records through today (already filtered for active slots)
    roster_records:  current roster (for matching players to projections)
    proj_bat/pit:    projection maps from build_projection_map()
    current_period:  current scoring period ID (day number)

    Returns a dict keyed by team_id with projected final category values and roto points.
    """
    TOTAL_SCORING_PERIODS = 186
    HITTING_STATS  = {'R', 'HR', 'RBI', 'SB', 'H', 'BB', 'HBP', 'PA', 'AB', 'SF'}
    PITCHING_STATS = {'K', 'QS', 'IP', 'ER', 'H_Allowed', 'BB_Allowed', 'SV', 'HD'}

    # Step 1: Aggregate YTD actuals by team (same logic as discord-bot.py)
    ytd_totals: dict[int, dict] = {}
    for row in ytd_records:
        if row.get("scoring_period_id") == 0:
            continue
        tid  = row["team_id"]
        slot = row.get("lineup_slot_id")
        stats = row.get("stats", {})
        if isinstance(stats, str):
            stats = json.loads(stats)

        if slot in HITTING_SLOT_IDS:
            allowed = HITTING_STATS
        elif slot in PITCHING_SLOT_IDS:
            allowed = PITCHING_STATS
        else:
            continue

        if tid not in ytd_totals:
            ytd_totals[tid] = {}
        for stat, val in stats.items():
            if stat in allowed and isinstance(val, (int, float)):
                ytd_totals[tid][stat] = ytd_totals[tid].get(stat, 0) + val

    # Step 2: Add ROS projections per team
    combined: dict[int, dict] = {}
    match_info: dict[int, dict] = {}

    for tid in TEAM_NAMES:
        ytd = ytd_totals.get(tid, {})
        ros = project_team_ros(tid, roster_records, proj_bat, proj_pit, ytd_records, current_period)

        match_info[tid] = {
            "matched": ros.pop("_matched", 0),
            "roster_size": ros.pop("_roster_size", 0),
        }

        merged = {}
        for stat in list(HITTING_STATS | PITCHING_STATS):
            merged[stat] = ytd.get(stat, 0) + ros.get(stat, 0)
        combined[tid] = merged

    # Step 3: Compute projected final category values
    team_cats: dict[int, dict] = {}
    for tid, stats in combined.items():
        pa     = stats.get("PA", 0)
        obp    = round((stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
        ip_dec = _espn_ip_to_innings(stats.get("IP", 0))
        era    = round((stats.get("ER", 0) / ip_dec) * 9, 2) if ip_dec > 0 else 0.0
        whip   = round((stats.get("H_Allowed", 0) + stats.get("BB_Allowed", 0)) / ip_dec, 3) if ip_dec > 0 else 0.0

        team_cats[tid] = {
            "R":     int(stats.get("R", 0)),
            "HR":    int(stats.get("HR", 0)),
            "RBI":   int(stats.get("RBI", 0)),
            "OBP":   obp,
            "SB":    int(stats.get("SB", 0)),
            "QS":    int(stats.get("QS", 0)),
            "ERA":   era,
            "WHIP":  whip,
            "K":     int(stats.get("K", 0)),
            "SV_HD": int(round(stats.get("SV", 0) + stats.get("HD", 0))),
        }

    # Step 4: Assign roto points and rankings
    team_cats = _compute_roto_points(team_cats)

    # Attach match info
    for tid in team_cats:
        team_cats[tid]["proj_matched"]    = match_info.get(tid, {}).get("matched", 0)
        team_cats[tid]["proj_roster_size"] = match_info.get(tid, {}).get("roster_size", 0)

    return team_cats

# ---------------------------------------------------------------------------
# PLAYER PROJECTION LOOKUP
# ---------------------------------------------------------------------------

def get_player_projection(
    player_name: str,
    proj_bat: dict[int, dict],
    proj_pit: dict[int, dict],
    ytd_records: list[dict] = None,
    current_period: int = 1,
) -> list[dict]:
    """
    Fuzzy-match a player name against projections.
    Returns a list of matching projections (may be 2 for two-way players like Ohtani).
    """
    target = normalize_name(player_name)
    results = []
    target_parts = target.split()

    for proj_map in [proj_bat, proj_pit]:
        for mlbam_id, data in proj_map.items():
            candidate = normalize_name(data.get("full_name", ""))
            cand_parts = candidate.split()
            matched_flag = False

            if candidate == target:
                matched_flag = True
            elif target in candidate or candidate in target:
                matched_flag = True
            elif (len(target_parts) >= 2 and len(cand_parts) >= 2
                    and target_parts[-1] == cand_parts[-1]
                    and target_parts[0][0] == cand_parts[0][0]):
                matched_flag = True

            if matched_flag:
                p_data = {**data, "mlbam_id": mlbam_id}
                if data.get("stat_type") == "pitching" and ytd_records is not None:
                    TOTAL_SCORING_PERIODS = 186
                    gs_proj = data.get("GS", 0) or 0
                    gs_actual = sum(
                        1 for r in ytd_records
                        if r["player_id"] == mlbam_id
                        and r.get("lineup_slot_id") in (13, 15)
                        and _get_stat_val(r, "IP") > 0
                    )
                    qs_actual = sum(
                        _get_stat_val(r, "QS") for r in ytd_records
                        if r["player_id"] == mlbam_id
                    )
                    if gs_proj > 0:
                        if gs_actual > 0:
                            qs_rate = min(1.0, max(0.0, qs_actual / gs_actual))
                        else:
                            qs_rate = _estimate_qs(data) / gs_proj if gs_proj > 0 else 0
                        proj_qs = gs_proj * qs_rate
                    else:
                        proj_qs = 0

                    ytd_sv = sum(_get_stat_val(r, "SV") for r in ytd_records if r["player_id"] == mlbam_id)
                    ytd_hd = sum(_get_stat_val(r, "HD") for r in ytd_records if r["player_id"] == mlbam_id)
                    extrap_ratio = TOTAL_SCORING_PERIODS / max(1, current_period)
                    proj_sv = ytd_sv * extrap_ratio
                    proj_hd = ytd_hd * extrap_ratio

                    p_data["QS"] = proj_qs
                    p_data["SV"] = proj_sv
                    p_data["HD"] = proj_hd
                    p_data["YTD_QS"] = qs_actual
                    p_data["YTD_GS"] = gs_actual
                    p_data["YTD_SV"] = ytd_sv
                    p_data["YTD_HD"] = ytd_hd

                results.append(p_data)

    return results

# ---------------------------------------------------------------------------
# FORMATTING FOR CONTEXT / DISPLAY
# ---------------------------------------------------------------------------

def format_projected_standings_block(projected: dict, current: dict | None = None) -> str:
    """Format projected final standings as a text block for Gemini context."""
    sorted_t = sorted(projected.items(), key=lambda x: x[1]["standing"])

    header = (
        f"{'#':<3} {'Team':<14} {'Pts':>5}  "
        + "  ".join(f"{CAT_DISPLAY[c]:>5}" for c in ROTO_CATS)
    )
    lines = ["PROJECTED FINAL ROTO STANDINGS (YTD actual + FanGraphs ZiPS ROS):", header]

    for tid, data in sorted_t:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        cats = "  ".join(f"{data['cat_points'].get(c, 0):>5.1f}" for c in ROTO_CATS)
        proj_note = f"  ({data.get('proj_matched', '?')}/{data.get('proj_roster_size', '?')} matched)"
        lines.append(f"{data['standing']:<3} {name:<14} {data['roto_points']:>5.1f}  {cats}{proj_note}")

    # Show projected category values too
    fmt = {"R":"d","HR":"d","RBI":"d","OBP":".3f","SB":"d","QS":"d","ERA":".2f","WHIP":".3f","K":"d","SV_HD":"d"}
    val_header = f"\n{'#':<3} {'Team':<14}  " + "  ".join(f"{CAT_DISPLAY[c]:>7}" for c in ROTO_CATS)
    lines.append("\nPROJECTED FINAL CATEGORY VALUES:" + val_header)
    for tid, data in sorted_t:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        vals = "  ".join(f"{data[c]:{fmt[c]}}".rjust(7) for c in ROTO_CATS)
        lines.append(f"{data['standing']:<3} {name:<14}  {vals}")

    # Delta from current standings if provided
    if current:
        movers = []
        for tid, pdata in projected.items():
            if tid in current:
                curr_rank = current[tid].get("standing", 0)
                proj_rank = pdata["standing"]
                change = curr_rank - proj_rank
                if change != 0:
                    name = TEAM_NAMES.get(tid, f"Team {tid}")
                    arrow = "▲" if change > 0 else "▼"
                    movers.append((abs(change), f"  {arrow} {name}: #{curr_rank} now → #{proj_rank} projected"))
        if movers:
            movers.sort(reverse=True)
            lines.append("\nPROJECTED RANK CHANGES VS CURRENT:")
            lines.extend(m[1] for m in movers)

    lines.append(
        "\nNote: Projections use FanGraphs ZiPS ROS. QS is estimated from ERA/GS. "
        "Only rostered active-lineup players with matching MLBAM IDs are included."
    )
    return "\n".join(lines)


def format_player_projection_block(projections: list[dict]) -> str:
    """Format one or more player projections into a readable context block."""
    if not projections:
        return ""

    lines = []
    for p in projections:
        name = p.get("full_name", "Unknown")
        team = p.get("team", "?")

        if p.get("stat_type") == "batting":
            lines.append(
                f"ROS PROJECTION — {name} ({team}) [Batting]:\n"
                f"  G:{p.get('G',0)} PA:{p.get('PA',0)} AB:{p.get('AB',0)}\n"
                f"  AVG:{p.get('AVG',0):.3f} OBP:{p.get('OBP',0):.3f} SLG:{p.get('SLG',0):.3f}\n"
                f"  HR:{p.get('HR',0)} R:{p.get('R',0)} RBI:{p.get('RBI',0)} "
                f"SB:{p.get('SB',0)} BB:{p.get('BB',0)} SO:{p.get('SO',0)}\n"
                f"  WAR:{p.get('WAR',0):.1f}"
            )
        else:
            ip = p.get("IP", 0)
            # Check if we have YTD actuals to display custom extrapolated format
            if "YTD_QS" in p:
                qs_line = f"  QS (extrapolated YTD): {p.get('QS',0):.1f}  (YTD actual: {int(p.get('YTD_QS',0))}/{int(p.get('YTD_GS',0))} starts)"
                sv_hd_line = f"  SV+HD (extrapolated YTD): {p.get('SV',0)+p.get('HD',0):.1f}  (YTD actual: {int(p.get('YTD_SV',0))} SV, {int(p.get('YTD_HD',0))} HD)"
            else:
                qs_line = f"  QS (estimated from ERA): {p.get('QS',0):.1f}"
                sv_hd_line = f"  SV+HD (FanGraphs ROS): {p.get('SV',0)+p.get('HD',0):.1f}"

            lines.append(
                f"ROS PROJECTION — {name} ({team}) [Pitching]:\n"
                f"  G:{p.get('G',0)} GS:{p.get('GS',0)} IP:{ip:.1f}\n"
                f"  ERA:{p.get('ERA',0):.2f} WHIP:{p.get('WHIP',0):.3f}\n"
                f"  K:{p.get('K',0)} W:{p.get('W',0)} L:{p.get('L',0)}\n"
                + qs_line + "\n"
                + sv_hd_line + "\n"
                f"  WAR:{p.get('WAR',0):.1f}"
            )

    return "\n\n".join(lines)


def forecast_ros_only_standings(
    roster_records: list[dict],
    proj_bat: dict[int, dict],
    proj_pit: dict[int, dict],
    ytd_records: list[dict],
    current_period: int,
) -> dict:
    """
    Compute standings and category values based PURELY on the rest-of-season (ROS) projections,
    without including YTD actuals.
    """
    # 1. Project ROS totals for each team
    ros_totals: dict[int, dict] = {}
    match_info: dict[int, dict] = {}
    for tid in TEAM_NAMES:
        ros = project_team_ros(tid, roster_records, proj_bat, proj_pit, ytd_records, current_period)
        match_info[tid] = {
            "matched": ros.pop("_matched", 0),
            "roster_size": ros.pop("_roster_size", 0),
        }
        ros_totals[tid] = ros

    # 2. Compute category values for ROS portion
    team_cats: dict[int, dict] = {}
    for tid, stats in ros_totals.items():
        pa     = stats.get("PA", 0)
        obp    = round((stats.get("H", 0) + stats.get("BB", 0) + stats.get("HBP", 0)) / pa, 3) if pa > 0 else 0.0
        ip_dec = _espn_ip_to_innings(stats.get("IP", 0))
        era    = round((stats.get("ER", 0) / ip_dec) * 9, 2) if ip_dec > 0 else 0.0
        whip   = round((stats.get("H_Allowed", 0) + stats.get("BB_Allowed", 0)) / ip_dec, 3) if ip_dec > 0 else 0.0

        team_cats[tid] = {
            "R":     int(stats.get("R", 0)),
            "HR":    int(stats.get("HR", 0)),
            "RBI":   int(stats.get("RBI", 0)),
            "OBP":   obp,
            "SB":    int(stats.get("SB", 0)),
            "QS":    int(stats.get("QS", 0)),
            "ERA":   era,
            "WHIP":  whip,
            "K":     int(stats.get("K", 0)),
            "SV_HD": int(round(stats.get("SV", 0) + stats.get("HD", 0))),
        }

    # 3. Assign roto points and rankings
    team_cats = _compute_roto_points(team_cats)

    # Attach match info
    for tid in team_cats:
        team_cats[tid]["proj_matched"]    = match_info.get(tid, {}).get("matched", 0)
        team_cats[tid]["proj_roster_size"] = match_info.get(tid, {}).get("roster_size", 0)

    return team_cats


def format_ros_only_standings_block(ros_projected: dict) -> str:
    """Format pure rest-of-season projected standings as a text block."""
    sorted_t = sorted(ros_projected.items(), key=lambda x: x[1]["standing"])

    header = (
        f"{'#':<3} {'Team':<14} {'Pts':>5}  "
        + "  ".join(f"{CAT_DISPLAY[c]:>5}" for c in ROTO_CATS)
    )
    lines = ["PURE REST-OF-SEASON PROJECTED ROTO STANDINGS (ROS only, no YTD actuals):", header]

    for tid, data in sorted_t:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        cats = "  ".join(f"{data['cat_points'].get(c, 0):>5.1f}" for c in ROTO_CATS)
        proj_note = f"  ({data.get('proj_matched', '?')}/{data.get('proj_roster_size', '?')} matched)"
        lines.append(f"{data['standing']:<3} {name:<14} {data['roto_points']:>5.1f}  {cats}{proj_note}")

    # Show projected category values too
    fmt = {"R":"d","HR":"d","RBI":"d","OBP":".3f","SB":"d","QS":"d","ERA":".2f","WHIP":".3f","K":"d","SV_HD":"d"}
    val_header = f"\n{'#':<3} {'Team':<14}  " + "  ".join(f"{CAT_DISPLAY[c]:>7}" for c in ROTO_CATS)
    lines.append("\nPURE REST-OF-SEASON PROJECTED CATEGORY VALUES:" + val_header)
    for tid, data in sorted_t:
        name = TEAM_NAMES.get(tid, f"Team {tid}")
        vals = "  ".join(f"{data[c]:{fmt[c]}}".rjust(7) for c in ROTO_CATS)
        lines.append(f"{data['standing']:<3} {name:<14}  {vals}")

    return "\n".join(lines)
