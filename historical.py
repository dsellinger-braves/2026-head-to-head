"""
historical.py — shared historical context module
Import this in discord-bot.py and discord-daily-recap.py
"""

import requests

HISTORICAL_DATA_URL = "https://storage.googleapis.com/fantasy-draft-2026/historical-finish.json"

# Maps all known Owner name variants in the JSON → canonical name in TEAM_NAMES
HISTORICAL_OWNER_MAP = {
    "tim":                    "Tim",
    "adrian":                 "Adrian",
    "garrett":                "Garrett",
    "daniel":                 "Dan",
    "dan":                    "Dan",
    "anil":                   "Anil",
    "alex":                   "Alex",
    "will":                   "Will",
    "mark":                   "Mark",
    "preston":                "Preston",
}

_historical_cache: list[dict] | None = None


def load_historical_data() -> list[dict]:
    """Fetch and cache the historical finish JSON from GCS. Active owners only."""
    global _historical_cache
    if _historical_cache is None:
        resp = requests.get(HISTORICAL_DATA_URL, timeout=10)
        resp.raise_for_status()
        _historical_cache = resp.json()
    return _historical_cache


def _canonical(raw_owner: str) -> str:
    """Normalize a raw Owner string to the canonical TEAM_NAMES key."""
    return HISTORICAL_OWNER_MAP.get(raw_owner.lower(), raw_owner)


def get_owner_seasons(owner_canonical: str, active_only: bool = True) -> list[dict]:
    """Return all historical seasons for one owner (canonical name)."""
    data = load_historical_data()
    rows = [r for r in data if _canonical(r["Owner"]) == owner_canonical]
    if active_only:
        rows = [r for r in rows if r.get("Active Owner?") == "Y"]
    return sorted(rows, key=lambda r: int(r["Year"]), reverse=True)


# ---------------------------------------------------------------------------
# Formatted context blocks
# ---------------------------------------------------------------------------

def format_owner_history(owner_canonical: str) -> str:
    """Full season-by-season record for one owner, suitable for AI context."""
    seasons = get_owner_seasons(owner_canonical)
    if not seasons:
        return f"{owner_canonical}: no historical data found."

    finishes      = [int(s["Final Rank"]) for s in seasons]
    champ_years   = [s["Year"] for s in seasons if s["Final Rank"] == "1"]
    top3_count    = sum(1 for f in finishes if f <= 3)
    avg_finish    = sum(finishes) / len(finishes)
    best_pts      = max(seasons, key=lambda s: float(s["Points"]))
    worst_pts     = min(seasons, key=lambda s: float(s["Points"]))

    # Category averages (hitting vs pitching split)
    hit_cats  = ["R", "HR", "RBI", "SB", "OBP"]
    pitch_cats = ["K", "QS", "SVHLD", "ERA", "WHIP"]

    def avg_cat(cat):
        vals = [float(s[cat]) for s in seasons if cat in s]
        return round(sum(vals) / len(vals), 1) if vals else 0

    cat_avgs = {c: avg_cat(c) for c in hit_cats + pitch_cats}

    # Find strengths (avg >= 7) and weaknesses (avg <= 4)
    strengths  = [c for c, v in cat_avgs.items() if v >= 7.0]
    weaknesses = [c for c, v in cat_avgs.items() if v <= 4.0]

    lines = [
        f"{owner_canonical.upper()} ALL-TIME RECORD ({len(seasons)} seasons, "
        f"{min(s['Year'] for s in seasons)}–{max(s['Year'] for s in seasons)}):",
        f"  Championships: {len(champ_years)}"
        + (f" ({', '.join(sorted(champ_years))})" if champ_years else ""),
        f"  Top-3 finishes: {top3_count} of {len(seasons)}",
        f"  Average finish: #{avg_finish:.1f}",
        f"  Best season:  {best_pts['Year']} — #{best_pts['Final Rank']} ({best_pts['Points']} pts)",
        f"  Worst season: {worst_pts['Year']} — #{worst_pts['Final Rank']} ({worst_pts['Points']} pts)",
    ]

    if strengths:
        lines.append(f"  Historical strengths (avg ≥7 pts): {', '.join(strengths)}")
    if weaknesses:
        lines.append(f"  Historical weaknesses (avg ≤4 pts): {', '.join(weaknesses)}")

    lines.append("  Season-by-season:")
    for s in seasons:
        hit_pts   = s.get("Hitting Points", "?")
        pitch_pts = s.get("Pitching Points", "?")
        lines.append(
            f"    {s['Year']}: #{s['Final Rank']} — {s['Points']} pts "
            f"(H:{hit_pts} / P:{pitch_pts})  "
            f"R:{s['R']} HR:{s['HR']} RBI:{s['RBI']} SB:{s['SB']} OBP:{s['OBP']} | "
            f"K:{s['K']} QS:{s['QS']} SV+H:{s['SVHLD']} ERA:{s['ERA']} WHIP:{s['WHIP']}"
        )
    return "\n".join(lines)


def format_league_champions() -> str:
    """Championship counts for all active owners."""
    data    = load_historical_data()
    active  = [r for r in data if r.get("Active Owner?") == "Y"]
    champs: dict[str, list[str]] = {}
    for r in active:
        if r["Final Rank"] == "1":
            owner = _canonical(r["Owner"])
            champs.setdefault(owner, []).append(r["Year"])

    lines = ["HEFTYSTRONG CHAMPIONSHIP HISTORY (active owners):"]
    for owner, years in sorted(champs.items(), key=lambda x: -len(x[1])):
        lines.append(f"  {owner}: {len(years)}x  ({', '.join(sorted(years))})")

    # Owners without a title
    all_active = {_canonical(r["Owner"]) for r in active}
    titleless  = sorted(all_active - set(champs.keys()))
    if titleless:
        lines.append(f"  Still waiting: {', '.join(titleless)}")

    return "\n".join(lines)


def format_all_active_owner_summaries() -> str:
    """One-liner summary for every active owner — always included in context."""
    data        = load_historical_data()
    active_rows = [r for r in data if r.get("Active Owner?") == "Y"]
    owners      = sorted({_canonical(r["Owner"]) for r in active_rows})

    lines = ["OWNER HISTORICAL SUMMARIES:"]
    for owner in owners:
        seasons = [r for r in active_rows if _canonical(r["Owner"]) == owner]
        if not seasons:
            continue
        finishes   = [int(s["Final Rank"]) for s in seasons]
        champs     = sum(1 for f in finishes if f == 1)
        top3       = sum(1 for f in finishes if f <= 3)
        avg_finish = sum(finishes) / len(finishes)
        best_year  = min(seasons, key=lambda s: int(s["Final Rank"]))
        lines.append(
            f"  {owner}: {len(seasons)} seasons, {champs} titles, "
            f"{top3} top-3s, avg finish #{avg_finish:.1f} "
            f"(best: #{best_year['Final Rank']} in {best_year['Year']})"
        )
    return "\n".join(lines)
