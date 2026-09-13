// src/utils/eraAdjustments.js

/**
 * Roster capacities across league history.
 * 2018-2025: 13 active batters, 9 active pitchers (22 total active starters).
 * 2026: 16 active batters, 12 active pitchers (28 total active starters).
 */
export const ROSTER_CAPACITIES = {
  // Baseline modern era: 2026
  baseline: {
    batters: 16,
    pitchers: 12,
    total: 28,
  },
  // Season specifics
  seasons: {
    2012: { batters: 13, pitchers: 9, total: 22 },
    2013: { batters: 13, pitchers: 9, total: 22 },
    2014: { batters: 13, pitchers: 9, total: 22 },
    2015: { batters: 13, pitchers: 9, total: 22 },
    2016: { batters: 13, pitchers: 9, total: 22 },
    2017: { batters: 13, pitchers: 9, total: 22 },
    2018: { batters: 13, pitchers: 9, total: 22 },
    2019: { batters: 13, pitchers: 9, total: 22 },
    2020: { batters: 13, pitchers: 9, total: 22 },
    2021: { batters: 13, pitchers: 9, total: 22 },
    2022: { batters: 13, pitchers: 9, total: 22 },
    2023: { batters: 13, pitchers: 9, total: 22 },
    2024: { batters: 13, pitchers: 9, total: 22 },
    2025: { batters: 13, pitchers: 9, total: 22 },
    2026: { batters: 16, pitchers: 12, total: 28 },
  }
};

/**
 * Official MLB league-wide averages per team-game across seasons.
 * Sourced from official Major League Baseball statistics / Baseball-Reference.
 */
export const MLB_LEAGUE_AVERAGES = {
  baselineYear: 2026,
  seasons: {
    2012: { R: 4.32, HR: 1.02, RBI: 4.11, SB: 0.66, K: 7.50, QS: 0.54, 'SV+HDs': 0.46 },
    2013: { R: 4.17, HR: 0.96, RBI: 3.96, SB: 0.55, K: 7.55, QS: 0.53, 'SV+HDs': 0.46 },
    2014: { R: 4.07, HR: 0.86, RBI: 3.86, SB: 0.57, K: 7.70, QS: 0.53, 'SV+HDs': 0.48 },
    2015: { R: 4.25, HR: 1.01, RBI: 4.04, SB: 0.52, K: 7.71, QS: 0.50, 'SV+HDs': 0.49 },
    2016: { R: 4.48, HR: 1.16, RBI: 4.27, SB: 0.52, K: 8.03, QS: 0.48, 'SV+HDs': 0.50 },
    2017: { R: 4.65, HR: 1.26, RBI: 4.44, SB: 0.52, K: 8.25, QS: 0.46, 'SV+HDs': 0.51 },
    2018: { R: 4.45, HR: 1.15, RBI: 4.24, SB: 0.51, K: 8.48, QS: 0.42, 'SV+HDs': 0.52 },
    2019: { R: 4.83, HR: 1.39, RBI: 4.63, SB: 0.47, K: 8.81, QS: 0.39, 'SV+HDs': 0.53 }, // Juiced ball peak
    2020: { R: 4.65, HR: 1.28, RBI: 4.44, SB: 0.49, K: 8.68, QS: 0.36, 'SV+HDs': 0.54 }, // Shortened 60-game season
    2021: { R: 4.53, HR: 1.22, RBI: 4.32, SB: 0.46, K: 8.68, QS: 0.37, 'SV+HDs': 0.58 }, // SB scarcity low
    2022: { R: 4.28, HR: 1.07, RBI: 4.09, SB: 0.51, K: 8.40, QS: 0.39, 'SV+HDs': 0.57 }, // Deadened ball
    2023: { R: 4.62, HR: 1.21, RBI: 4.43, SB: 0.72, K: 8.61, QS: 0.35, 'SV+HDs': 0.59 }, // Pitch clock & base size rules
    2024: { R: 4.39, HR: 1.12, RBI: 4.19, SB: 0.74, K: 8.48, QS: 0.34, 'SV+HDs': 0.60 },
    2025: { R: 4.45, HR: 1.16, RBI: 4.27, SB: 0.71, K: 8.36, QS: 0.34, 'SV+HDs': 0.60 },
    2026: { R: 4.49, HR: 1.15, RBI: 4.29, SB: 0.68, K: 8.36, QS: 0.33, 'SV+HDs': 0.60 }, // Current baseline
  }
};

export const BAT_COUNTING_STATS = new Set(['R', 'HR', 'RBI', 'SB']);
export const PITCH_COUNTING_STATS = new Set(['K', 'QS', 'SV+HDs']);
export const ALL_COUNTING_STATS = new Set(['R', 'HR', 'RBI', 'SB', 'K', 'QS', 'SV+HDs']);
export const RATE_STATS = new Set(['ERA', 'WHIP', 'OBP']);

/**
 * Returns the roster capacity multiplier for a given stat category and season.
 * Normalizes to the 2026 baseline (16 active batters, 12 active pitchers).
 */
export function getRosterMultiplier(statKey, seasonYear) {
  if (!ALL_COUNTING_STATS.has(statKey)) return 1.0;

  const seasonConfig = ROSTER_CAPACITIES.seasons[seasonYear] || ROSTER_CAPACITIES.seasons[2026];
  const baseline = ROSTER_CAPACITIES.baseline;

  if (BAT_COUNTING_STATS.has(statKey)) {
    const seasonBatters = seasonConfig.batters || 13;
    return baseline.batters / seasonBatters;
  }

  if (PITCH_COUNTING_STATS.has(statKey)) {
    const seasonPitchers = seasonConfig.pitchers || 9;
    return baseline.pitchers / seasonPitchers;
  }

  return 1.0;
}

/**
 * Returns the MLB environment multiplier for a given stat category and season.
 * Multiplier = MLB Baseline (2026) / MLB Season.
 * - If stat was inflated in that season (e.g. 2019 HRs), multiplier < 1.0 (deflates).
 * - If stat was scarce in that season (e.g. 2021 SBs), multiplier > 1.0 (boosts).
 */
export function getMlbMultiplier(statKey, seasonYear) {
  if (!ALL_COUNTING_STATS.has(statKey)) return 1.0;

  const baselineYear = MLB_LEAGUE_AVERAGES.baselineYear;
  const baselineStats = MLB_LEAGUE_AVERAGES.seasons[baselineYear];
  const seasonStats = MLB_LEAGUE_AVERAGES.seasons[seasonYear] || baselineStats;

  const baselineVal = baselineStats[statKey];
  const seasonVal = seasonStats[statKey];

  if (!baselineVal || !seasonVal || seasonVal <= 0) return 1.0;

  return baselineVal / seasonVal;
}

/**
 * Computes the era-adjusted value for a stat given active adjustment toggles.
 * @param {string} statKey - e.g. 'HR', 'SB', 'K', 'QS', 'R', 'RBI', 'SV+HDs'
 * @param {number} rawVal - Raw single-day count
 * @param {number} seasonYear - Season year of the performance
 * @param {object} options - { adjustRoster: boolean, adjustMlb: boolean }
 * @returns {object} { adjustedVal, rawVal, rosterMultiplier, mlbMultiplier, totalMultiplier }
 */
export function calculateEraAdjustedStat(statKey, rawVal, seasonYear, { adjustRoster = false, adjustMlb = false } = {}) {
  const n = parseFloat(rawVal) || 0;

  if (RATE_STATS.has(statKey) || (!adjustRoster && !adjustMlb)) {
    return {
      adjustedVal: n,
      rawVal: n,
      rosterMultiplier: 1.0,
      mlbMultiplier: 1.0,
      totalMultiplier: 1.0,
      isAdjusted: false,
    };
  }

  const rosterMultiplier = adjustRoster ? getRosterMultiplier(statKey, seasonYear) : 1.0;
  const mlbMultiplier = adjustMlb ? getMlbMultiplier(statKey, seasonYear) : 1.0;
  const totalMultiplier = rosterMultiplier * mlbMultiplier;

  const adjustedVal = n * totalMultiplier;

  return {
    adjustedVal,
    rawVal: n,
    rosterMultiplier,
    mlbMultiplier,
    totalMultiplier,
    isAdjusted: totalMultiplier !== 1.0,
  };
}

/**
 * Formats a stat value for display, applying appropriate decimal places for adjusted stats.
 */
export function formatHighlightVal(val, statKey, isAdjusted = false) {
  const n = parseFloat(val);
  if (isNaN(n) || val === undefined || val === null) return '-';

  if (statKey === 'ERA') return n.toFixed(2);
  if (statKey === 'WHIP') return n.toFixed(2);
  if (statKey === 'OBP') return n.toFixed(4).replace(/^0/, '');
  if (statKey === 'IP') return `${Math.floor(n)}.${Math.round((n % 1) * 3)}`;

  // Counting stats: if adjusted, display with 1 decimal place (or 2 if needed for small values like QS)
  if (isAdjusted) {
    if (statKey === 'QS' || statKey === 'SV+HDs') {
      return n.toFixed(2);
    }
    return n.toFixed(1);
  }

  return Math.round(n);
}
