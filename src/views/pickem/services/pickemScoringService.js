/**
 * @file pickemScoringService.js
 * @description Pure scoring evaluation, playoff crossover checking, category labeling,
 * and Hall of Fame dynasty aggregation for the Annual MLB Pick'em Championship.
 */

import { teamsMatch } from '../../../utils/mlbTeams';

/**
 * Returns a human-friendly display label for question categories.
 */
export function getCategoryLabel(cat) {
  switch (cat) {
    case 'division': return 'Division';
    case 'wild_card': return 'Wild Card';
    case 'playoff_result': return 'Playoffs / WS';
    case 'award': return 'Awards';
    case 'extremes': return 'Win / Loss Extremes';
    default: return cat;
  }
}

/**
 * Evaluates an owner's prediction against live in-progress MLB standings and projected WAR.
 *
 * @param {string} qKey - Question identifier (e.g., 'al_east', 'nl_mvp')
 * @param {string} pickVal - The owner's pick string
 * @param {object} liveProjectionsData - Live MLB standings & projections from Supabase/API
 * @returns {object|null} { status, label, pts, leaderText }
 */
export function evaluateLivePick(qKey, pickVal, liveProjectionsData) {
  if (!liveProjectionsData?.categories) return null;
  const cat = liveProjectionsData.categories[qKey];
  if (!cat) return null;

  const leaderName = cat.leader || '';
  const pVal = pickVal || '';

  if (!pVal) return null;

  // Check exact match (using teamsMatch for team/player matching)
  if (leaderName && teamsMatch(pVal, leaderName)) {
    let exactPts = 3;
    if (cat.type === 'wild_card') exactPts = 2;
    else if (cat.type === 'division') exactPts = 3;
    else if (cat.type === 'award') exactPts = 4;
    else if (cat.type === 'playoff_result') exactPts = qKey.includes('world_series') ? 7 : 5;
    else if (cat.type === 'extremes') exactPts = 3;

    const label = cat.type === 'award' ? '✓ #1 in WAR (+4 pts)' : `✓ Leading (+${exactPts} pts)`;
    return { status: 'EXACT', label, pts: exactPts, leaderText: cat.leader };
  }

  // Check award contenders
  if (cat.type === 'award') {
    const isContender = (cat.contenders || []).some(c => teamsMatch(pVal, c));
    if (isContender) {
      return { status: 'CONTENDER', label: '⚡ Top Contender', pts: 0, leaderText: cat.leader };
    }
  }

  // Check Playoff Crossover for division and wild card questions
  const isPlayoffQ = cat.type === 'division' || cat.type === 'wild_card' ||
    qKey.includes('east') || qKey.includes('central') || qKey.includes('west') || qKey.includes('wc');

  if (isPlayoffQ) {
    const alPlayoffs = liveProjectionsData.playoff_al || [];
    const nlPlayoffs = liveProjectionsData.playoff_nl || [];

    if (qKey.startsWith('al_') && alPlayoffs.some(t => teamsMatch(pVal, t))) {
      return { status: 'CROSSOVER', label: '✦ In Playoff Spot (+2 pts)', pts: 2, leaderText: cat.leader };
    }
    if (qKey.startsWith('nl_') && nlPlayoffs.some(t => teamsMatch(pVal, t))) {
      return { status: 'CROSSOVER', label: '✦ In Playoff Spot (+2 pts)', pts: 2, leaderText: cat.leader };
    }
  }

  return { status: 'OFF_PACE', label: 'Off Pace', pts: 0, leaderText: cat.leader };
}

/**
 * Computes All-Time Hall of Fame aggregates from all seasons' scores.
 *
 * @param {Array} allTimeScores - Records from pickem_scores table
 * @returns {Array} Sorted list of owner career records
 */
export function computeHallOfFame(allTimeScores = []) {
  const records = {};
  allTimeScores.forEach(s => {
    const name = s.owner_name;
    if (!records[name]) {
      records[name] = {
        name,
        titles: 0,
        top3: 0,
        totalPrizes: 0,
        totalPoints: 0,
        seasonsCount: 0,
        bestScore: 0,
        bestYear: null,
      };
    }
    const r = records[name];
    const place = s.place || 99;
    const budget = s.budget_awarded || 0;
    r.seasonsCount += 1;
    r.totalPoints += (s.total_points || 0);
    r.totalPrizes += budget;
    if (place === 1) r.titles += 1;
    if (place <= 3) r.top3 += 1;
    if ((s.total_points || 0) > r.bestScore) {
      r.bestScore = s.total_points;
      r.bestYear = s.season_year;
    }
  });

  return Object.values(records).sort((a, b) => {
    if (b.titles !== a.titles) return b.titles - a.titles;
    if (b.totalPrizes !== a.totalPrizes) return b.totalPrizes - a.totalPrizes;
    return b.totalPoints - a.totalPoints;
  });
}

/**
 * The 23 standard categories initialized when bootstrapping a new season.
 */
export const STANDARD_PICKEM_QUESTIONS = [
  { key: 'nl_east', label: 'NL East', cat: 'division', exact: 3, partial: 2, ord: 1 },
  { key: 'nl_central', label: 'NL Central', cat: 'division', exact: 3, partial: 2, ord: 2 },
  { key: 'nl_west', label: 'NL West', cat: 'division', exact: 3, partial: 2, ord: 3 },
  { key: 'nl_wc_1', label: 'NL Wild Card 1', cat: 'wild_card', exact: 2, partial: 2, ord: 4 },
  { key: 'nl_wc_2', label: 'NL Wild Card 2', cat: 'wild_card', exact: 2, partial: 2, ord: 5 },
  { key: 'nl_wc_3', label: 'NL Wild Card 3', cat: 'wild_card', exact: 2, partial: 2, ord: 6 },
  { key: 'al_east', label: 'AL East', cat: 'division', exact: 3, partial: 2, ord: 7 },
  { key: 'al_central', label: 'AL Central', cat: 'division', exact: 3, partial: 2, ord: 8 },
  { key: 'al_west', label: 'AL West', cat: 'division', exact: 3, partial: 2, ord: 9 },
  { key: 'al_wc_1', label: 'AL Wild Card 1', cat: 'wild_card', exact: 2, partial: 2, ord: 10 },
  { key: 'al_wc_2', label: 'AL Wild Card 2', cat: 'wild_card', exact: 2, partial: 2, ord: 11 },
  { key: 'al_wc_3', label: 'AL Wild Card 3', cat: 'wild_card', exact: 2, partial: 2, ord: 12 },
  { key: 'nl_pennant', label: 'NL Pennant', cat: 'playoff_result', exact: 5, partial: 0, ord: 13 },
  { key: 'al_pennant', label: 'AL Pennant', cat: 'playoff_result', exact: 5, partial: 0, ord: 14 },
  { key: 'world_series', label: 'World Series Winner', cat: 'playoff_result', exact: 7, partial: 0, ord: 15 },
  { key: 'nl_mvp', label: 'NL MVP', cat: 'award', exact: 4, partial: 0, ord: 16 },
  { key: 'nl_cy_young', label: 'NL Cy Young', cat: 'award', exact: 4, partial: 0, ord: 17 },
  { key: 'nl_roy', label: 'NL Rookie of the Year', cat: 'award', exact: 4, partial: 0, ord: 18 },
  { key: 'al_mvp', label: 'AL MVP', cat: 'award', exact: 4, partial: 0, ord: 19 },
  { key: 'al_cy_young', label: 'AL Cy Young', cat: 'award', exact: 4, partial: 0, ord: 20 },
  { key: 'al_roy', label: 'AL Rookie of the Year', cat: 'award', exact: 4, partial: 0, ord: 21 },
  { key: 'most_wins', label: 'Most Team Wins (reg. season)', cat: 'extremes', exact: 3, partial: 0, ord: 22 },
  { key: 'most_losses', label: 'Most Team Losses (reg. season)', cat: 'extremes', exact: 3, partial: 0, ord: 23 },
];
