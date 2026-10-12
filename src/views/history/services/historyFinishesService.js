// src/views/history/services/historyFinishesService.js
import { supabase } from '../../../supabaseClient';
import { get, set } from 'idb-keyval';
import {
  ROSTER_CAPACITIES,
  MLB_LEAGUE_AVERAGES,
  calculateEraAdjustedStat,
} from '../../../utils/eraAdjustments';
import leagueContextData from '../../../data/league_context.json';
import historicalRawStats from '../../../data/historicalRawStats.json';

export const CANONICAL_OWNERS = {
  tim: 'Tim',
  adrian: 'Adrian',
  garrett: 'Garrett',
  daniel: 'Dan',
  dan: 'Dan',
  anil: 'Anil',
  alex: 'Alex',
  will: 'Will',
  'will alexander': 'Will',
  mark: 'Mark',
  preston: 'Preston',
};

export const normalizeOwner = (raw) => {
  if (!raw) return 'Unknown';
  const k = raw.trim().toLowerCase();
  return CANONICAL_OWNERS[k] || raw.trim();
};

export const formatRawStat = (key, val) => {
  if (val === null || val === undefined || val === '') return '—';
  const n = parseFloat(val);
  if (isNaN(n)) return '—';
  if (key === 'OBP' || key === 'AVG') {
    return n < 1 ? n.toFixed(3).replace(/^0/, '') : n.toFixed(3);
  }
  if (key === 'ERA' || key === 'WHIP') {
    return n.toFixed(2);
  }
  return Math.round(n).toLocaleString();
};

export const formatDiffStat = (key, diff) => {
  if (diff === null || diff === undefined || isNaN(diff)) return '—';
  const sign = diff > 0 ? '+' : diff < 0 ? '-' : '';
  const absVal = Math.abs(diff);
  if (key === 'OBP' || key === 'AVG') {
    const s = absVal < 1 ? absVal.toFixed(3).replace(/^0/, '') : absVal.toFixed(3);
    return `${sign}${s}`;
  }
  if (key === 'ERA' || key === 'WHIP') {
    return `${sign}${absVal.toFixed(2)}`;
  }
  return `${sign}${Math.round(absVal).toLocaleString()}`;
};

export const formatPctDiff = (pct) => {
  if (pct === null || pct === undefined || isNaN(pct)) return '—';
  const sign = pct > 0 ? '+' : '';
  return `${sign}${pct.toFixed(1)}%`;
};

export const formatMeanStat = (key, val) => {
  if (val === null || val === undefined || isNaN(val)) return '—';
  if (key === 'OBP' || key === 'AVG') {
    return val < 1 ? val.toFixed(3).replace(/^0/, '') : val.toFixed(3);
  }
  if (key === 'ERA' || key === 'WHIP') {
    return val.toFixed(2);
  }
  return val.toFixed(1);
};

export const getDisplayRawStat = (catKey, row) => {
  if (!row.rawStats) return { text: '—', badge: null, title: '' };

  if (catKey === 'OBP') {
    if (row.year === 2012) {
      return {
        text: formatRawStat('AVG', row.rawStats.AVG),
        badge: 'BA',
        title: '2012 scored Batting Average instead of OBP',
      };
    }
    return {
      text: formatRawStat('OBP', row.rawStats.OBP),
      badge: null,
      title: '',
    };
  }

  if (catKey === 'QS') {
    if (row.year <= 2013) {
      return {
        text: formatRawStat('W', row.rawStats.W),
        badge: 'W',
        title: `${row.year} scored Wins instead of Quality Starts`,
      };
    }
    return {
      text: formatRawStat('QS', row.rawStats.QS),
      badge: null,
      title: '',
    };
  }

  if (catKey === 'SVHLD') {
    if (row.year <= 2018) {
      return {
        text: formatRawStat('SV', row.rawStats.SV),
        badge: 'SV',
        title: `${row.year} scored Saves instead of Saves+Holds`,
      };
    }
    return {
      text: formatRawStat('SVHLD', row.rawStats.SVHLD),
      badge: null,
      title: '',
    };
  }

  return {
    text: formatRawStat(catKey, row.rawStats[catKey]),
    badge: null,
    title: '',
  };
};

export const AVAILABLE_SEASONS = [
  'ALL',
  2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012
];

export const CATEGORIES = [
  { key: 'R', label: 'R', name: 'Runs', type: 'bat', higherIsBetter: true, activeYears: [2012, 2026] },
  { key: 'HR', label: 'HR', name: 'Home Runs', type: 'bat', higherIsBetter: true, activeYears: [2012, 2026] },
  { key: 'RBI', label: 'RBI', name: 'Runs Batted In', type: 'bat', higherIsBetter: true, activeYears: [2012, 2026] },
  { key: 'OBP', label: 'OBP', name: 'On-Base %', type: 'bat', higherIsBetter: true, activeYears: [2013, 2026], legacyNote: '2012 used Batting Average' },
  { key: 'SB', label: 'SB', name: 'Stolen Bases', type: 'bat', higherIsBetter: true, activeYears: [2012, 2026] },
  { key: 'K', label: 'K', name: 'Strikeouts', type: 'pitch', higherIsBetter: true, activeYears: [2012, 2026] },
  { key: 'QS', label: 'QS', name: 'Quality Starts', type: 'pitch', higherIsBetter: true, activeYears: [2014, 2026], legacyNote: '2012–2013 used Wins' },
  { key: 'SVHLD', label: 'SV+H', name: 'Saves + Holds', type: 'pitch', higherIsBetter: true, activeYears: [2019, 2026], legacyNote: '2012–2018 used Saves' },
  { key: 'ERA', label: 'ERA', name: 'Earned Run Avg', type: 'pitch', higherIsBetter: false, activeYears: [2012, 2026] },
  { key: 'WHIP', label: 'WHIP', name: 'WHIP', type: 'pitch', higherIsBetter: false, activeYears: [2012, 2026] },
];

export const LEGACY_CATEGORIES = [
  { key: 'AVG', label: 'BA', name: 'Batting Average', type: 'bat', higherIsBetter: true, activeYears: [2012, 2012], statField: 'AVG', rankField: 'OBP', legacyNote: 'Used in 2012 instead of OBP' },
  { key: 'W', label: 'W', name: 'Wins', type: 'pitch', higherIsBetter: true, activeYears: [2012, 2013], statField: 'W', rankField: 'QS', legacyNote: 'Used in 2012–2013 instead of QS' },
  { key: 'SV', label: 'SV', name: 'Saves', type: 'pitch', higherIsBetter: true, activeYears: [2012, 2018], statField: 'SV', rankField: 'SVHLD', legacyNote: 'Used in 2012–2018 instead of Saves+Holds' },
];

export const CAT_ICONS = {
  R: '🏃',
  HR: '💣',
  RBI: '💥',
  OBP: '🎯',
  SB: '⚡',
  K: '💨',
  QS: '💎',
  SVHLD: '🛡️',
  ERA: '📉',
  WHIP: '🔒',
  AVG: '🎯',
  W: '🏆',
  SV: '🛡️',
};

export const GCS_HISTORICAL_FINISHES = 'https://storage.googleapis.com/fantasy-draft-2026/historical-finish.json';

export async function fetchHistoricalFinishes() {
  let cachedRecords = null;
  try {
    const cached = await get('league_history_finishes_v3');
    if (cached && cached.length > 0) {
      cachedRecords = cached.map(r => ({ ...r, owner: normalizeOwner(r.owner) }));
    }
  } catch (cacheErr) {
    console.warn('IndexedDB read notice:', cacheErr);
  }

  let dbRecords = [];
  try {
    const { data, error } = await supabase
      .from('historical_finishes')
      .select('*')
      .order('season_year', { ascending: false })
      .order('final_place', { ascending: true });

    if (!error && data && data.length > 0) {
      dbRecords = data.map(r => ({
        year: r.season_year,
        place: r.final_place,
        owner: normalizeOwner(r.team_owner),
        points: parseFloat(r.total_roto_points) || 0,
        isActive: Boolean(r.is_active_owner),
        teamName: r.category_ranks?.team_name || r.raw_data?.['Team Name'] || `Team ${normalizeOwner(r.team_owner)}`,
        hittingPoints: parseFloat(r.category_ranks?.hitting_points || r.raw_data?.['Hitting Points']) || 0,
        pitchingPoints: parseFloat(r.category_ranks?.pitching_points || r.raw_data?.['Pitching Points']) || 0,
        categoryRanks: {
          R: parseFloat(r.category_ranks?.R) || 0,
          HR: parseFloat(r.category_ranks?.HR) || 0,
          RBI: parseFloat(r.category_ranks?.RBI) || 0,
          OBP: parseFloat(r.category_ranks?.OBP) || 0,
          SB: parseFloat(r.category_ranks?.SB) || 0,
          K: parseFloat(r.category_ranks?.K) || 0,
          QS: parseFloat(r.category_ranks?.QS) || 0,
          SVHLD: parseFloat(r.category_ranks?.SVHLD || r.category_ranks?.['SV+HDs']) || 0,
          ERA: parseFloat(r.category_ranks?.ERA) || 0,
          WHIP: parseFloat(r.category_ranks?.WHIP) || 0,
        }
      }));
    }
  } catch (err) {
    console.warn('Could not query Supabase historical_finishes:', err);
  }

  if (!dbRecords.length) {
    try {
      const res = await fetch(GCS_HISTORICAL_FINISHES);
      if (res.ok) {
        const gcsData = await res.json();
        dbRecords = gcsData.map(r => ({
          year: parseInt(r.Year, 10),
          place: parseInt(r['Final Rank'], 10),
          owner: normalizeOwner(r.Owner),
          points: parseFloat(r.Points) || 0,
          isActive: r['Active Owner?'] === 'Y',
          teamName: r['Team Name'] || `Team ${normalizeOwner(r.Owner)}`,
          hittingPoints: parseFloat(r['Hitting Points']) || 0,
          pitchingPoints: parseFloat(r['Pitching Points']) || 0,
          categoryRanks: {
            R: parseFloat(r.R) || 0,
            HR: parseFloat(r.HR) || 0,
            RBI: parseFloat(r.RBI) || 0,
            OBP: parseFloat(r.OBP) || 0,
            SB: parseFloat(r.SB) || 0,
            K: parseFloat(r.K) || 0,
            QS: parseFloat(r.QS) || 0,
            SVHLD: parseFloat(r.SVHLD) || 0,
            ERA: parseFloat(r.ERA) || 0,
            WHIP: parseFloat(r.WHIP) || 0,
          }
        }));
      }
    } catch (gcsErr) {
      console.warn('GCS historical-finish fallback notice:', gcsErr);
    }
  }

  const w23Standings = leagueContextData?.season_summary_2026?.standings_week_23 || [];
  const records2026 = w23Standings.map(s => {
    const catPts = s.category_points || {};
    const hitPts = (catPts.R || 0) + (catPts.HR || 0) + (catPts.RBI || 0) + (catPts.OBP || 0) + (catPts.SB || 0);
    const pitchPts = (catPts.K || 0) + (catPts.QS || 0) + (catPts.SV_HD || 0) + (catPts.ERA || 0) + (catPts.WHIP || 0);
    return {
      year: 2026,
      place: s.seed || s.rank,
      owner: normalizeOwner(s.owner),
      points: s.points,
      isActive: true,
      teamName: s.team_name,
      hittingPoints: hitPts,
      pitchingPoints: pitchPts,
      categoryRanks: {
        R: catPts.R || 0,
        HR: catPts.HR || 0,
        RBI: catPts.RBI || 0,
        OBP: catPts.OBP || 0,
        SB: catPts.SB || 0,
        K: catPts.K || 0,
        QS: catPts.QS || 0,
        SVHLD: catPts.SV_HD || 0,
        ERA: catPts.ERA || 0,
        WHIP: catPts.WHIP || 0,
      }
    };
  });

  const combined = [...records2026, ...dbRecords.filter(r => r.year !== 2026)];

  if (combined.length > 0) {
    try {
      await set('league_history_finishes_v3', combined);
    } catch (writeErr) {
      console.warn('Could not update IndexedDB cache:', writeErr);
    }
  }

  return {
    cachedRecords,
    records: combined.length ? combined : (cachedRecords || []),
    source: dbRecords.length ? 'Supabase Warehouse + 2026 Standings' : (cachedRecords ? 'IndexedDB Cache' : 'GCS Archive + 2026 Standings')
  };
}

export function computeTeamsPerSeason(rawFinishes) {
  const map = {};
  rawFinishes.forEach(r => {
    map[r.year] = (map[r.year] || 0) + 1;
  });
  return map;
}

export function computeAdjustedRecord(record, teamsPerSeason, adjustRoster, adjustMlb) {
  const year = record.year;
  const totalTeams = teamsPerSeason[year] || 10;
  const maxPossiblePoints = totalTeams * 10;

  const dominanceIndex = maxPossiblePoints > 0
    ? (record.points / maxPossiblePoints) * 100
    : record.points;

  const rosterMultiplier = adjustRoster
    ? (year === 2026 ? 1.0 : ROSTER_CAPACITIES.baseline.total / (ROSTER_CAPACITIES.seasons[year]?.total || 22))
    : 1.0;

  let mlbMultiplier = 1.0;
  if (adjustMlb) {
    const baseYear = MLB_LEAGUE_AVERAGES.baselineYear;
    const baseHR = MLB_LEAGUE_AVERAGES.seasons[baseYear]?.HR || 1.15;
    const seasonHR = MLB_LEAGUE_AVERAGES.seasons[year]?.HR || baseHR;
    mlbMultiplier = baseHR / seasonHR;
  }

  const totalMultiplier = rosterMultiplier * mlbMultiplier;
  const adjustedPoints = dominanceIndex * (adjustRoster || adjustMlb ? totalMultiplier : 1.0);

  return {
    ...record,
    totalTeams,
    maxPossiblePoints,
    dominanceIndex,
    rosterMultiplier,
    mlbMultiplier,
    adjustedPoints,
    isAdjusted: adjustRoster || adjustMlb,
  };
}

export function computeAugmentedFinishes(rawFinishes, teamsPerSeason, adjustRoster, adjustMlb) {
  return rawFinishes.map(r => {
    const normOwner = normalizeOwner(r.owner);
    const baseAdj = computeAdjustedRecord({ ...r, owner: normOwner }, teamsPerSeason, adjustRoster, adjustMlb);
    const rawSeason = historicalRawStats[String(r.year)];
    const rawTeam = rawSeason
      ? (rawSeason[normOwner] || rawSeason[r.owner] || null)
      : null;
    return {
      ...baseAdj,
      owner: normOwner,
      rawStats: rawTeam,
    };
  });
}

export function computeHighlightsData(augmentedFinishes, adjustRoster, adjustMlb, exclude2020Lowlights) {
  const list = [...augmentedFinishes];

  const topScoring = [...list].sort((a, b) => {
    const pA = a.isAdjusted ? a.adjustedPoints : a.points;
    const pB = b.isAdjusted ? b.adjustedPoints : b.points;
    return pB - pA;
  }).slice(0, 10);

  const lowestScoringList = exclude2020Lowlights
    ? list.filter(r => r.year !== 2020)
    : list;
  const lowestScoring = [...lowestScoringList].sort((a, b) => {
    const pA = a.isAdjusted ? a.adjustedPoints : a.points;
    const pB = b.isAdjusted ? b.adjustedPoints : b.points;
    return pA - pB;
  }).slice(0, 10);

  const topHitting = [...list].sort((a, b) => b.hittingPoints - a.hittingPoints).slice(0, 8);
  const topPitching = [...list].sort((a, b) => b.pitchingPoints - a.pitchingPoints).slice(0, 8);

  const blowouts = [];
  const completedSeasonsList = Array.from(new Set(list.map(r => r.year))).filter(y => y !== 2026).sort((a, b) => b - a);
  completedSeasonsList.forEach(yr => {
    const rows = list.filter(r => r.year === yr).sort((a, b) => a.place - b.place);
    if (rows.length >= 2) {
      const diff = rows[0].points - rows[1].points;
      blowouts.push({
        year: yr,
        champ: rows[0].owner,
        champTeam: rows[0].teamName,
        champPoints: rows[0].points,
        runnerUp: rows[1].owner,
        runnerUpPoints: rows[1].points,
        margin: diff,
      });
    }
  });
  blowouts.sort((a, b) => b.margin - a.margin);

  const hangovers = [];
  completedSeasonsList.forEach(yr => {
    const champ = list.find(r => r.year === yr && r.place === 1);
    if (champ) {
      const nextYearRow = list.find(r => r.year === yr + 1 && normalizeOwner(r.owner).toLowerCase() === normalizeOwner(champ.owner).toLowerCase());
      if (nextYearRow) {
        const rankDrop = nextYearRow.place - 1;
        const pointsDrop = champ.points - nextYearRow.points;
        hangovers.push({
          champYear: yr,
          nextYear: yr + 1,
          owner: champ.owner,
          champTeam: champ.teamName,
          champPoints: champ.points,
          nextPlace: nextYearRow.place,
          nextTeam: nextYearRow.teamName,
          nextPoints: nextYearRow.points,
          rankDrop,
          pointsDrop,
        });
      }
    }
  });
  hangovers.sort((a, b) => b.rankDrop - a.rankDrop);

  const allCategoriesList = [...CATEGORIES, ...LEGACY_CATEGORIES];
  const seasonCategoryMetrics = {};

  const seasonRowsMap = {};
  list.forEach(r => {
    if (!seasonRowsMap[r.year]) seasonRowsMap[r.year] = [];
    seasonRowsMap[r.year].push(r);
  });

  AVAILABLE_SEASONS.filter(y => y !== 'ALL').forEach(yr => {
    const rows = seasonRowsMap[yr] || [];
    allCategoriesList.forEach(cat => {
      const statField = cat.statField || cat.key;
      const rankField = cat.rankField || cat.key;
      const minYear = cat.activeYears ? cat.activeYears[0] : 2012;
      const maxYear = cat.activeYears ? cat.activeYears[1] : 2026;
      if (yr < minYear || yr > maxYear) return;

      const validSeasonRecords = [];
      rows.forEach(r => {
        const rawVal = r.rawStats?.[statField];
        if (rawVal !== undefined && rawVal !== null && rawVal > 0) {
          validSeasonRecords.push({
            year: yr,
            owner: r.owner,
            teamName: r.teamName,
            place: r.place,
            rotoPoints: r.categoryRanks?.[rankField] || 0,
            rawVal,
          });
        }
      });

      if (validSeasonRecords.length >= 2) {
        const vals = validSeasonRecords.map(x => x.rawVal);
        const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
        const variance = vals.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / vals.length;
        const stdDev = Math.sqrt(variance);

        validSeasonRecords.sort((a, b) => {
          return cat.higherIsBetter ? b.rawVal - a.rawVal : a.rawVal - b.rawVal;
        });

        const first = validSeasonRecords[0];
        const second = validSeasonRecords[1];
        const secondLast = validSeasonRecords[validSeasonRecords.length - 2];
        const last = validSeasonRecords[validSeasonRecords.length - 1];

        const gap12 = cat.higherIsBetter
          ? first.rawVal - second.rawVal
          : second.rawVal - first.rawVal;
        const pctGap12 = second.rawVal > 0 ? (gap12 / second.rawVal) * 100 : 0;

        const gapLast = cat.higherIsBetter
          ? secondLast.rawVal - last.rawVal
          : last.rawVal - secondLast.rawVal;
        const pctGapLast = secondLast.rawVal > 0 ? (gapLast / secondLast.rawVal) * 100 : 0;

        seasonCategoryMetrics[`${yr}_${cat.key}`] = {
          year: yr,
          catKey: cat.key,
          cat,
          mean,
          stdDev,
          count: validSeasonRecords.length,
          first,
          second,
          secondLast,
          last,
          gap12,
          pctGap12,
          gapLast,
          pctGapLast,
        };
      }
    });
  });

  const categoryStatRecords = {};
  const relativeDominanceRecords = {};
  const categoryGapsRecords = {};

  CATEGORIES.forEach(cat => {
    const higherIsBetter = cat.higherIsBetter;
    const statKey = cat.key;
    const eraKey = statKey === 'SVHLD' ? 'SV+HDs' : statKey;
    const minYear = cat.activeYears ? cat.activeYears[0] : 2012;
    const maxYear = cat.activeYears ? cat.activeYears[1] : 2026;

    const validRecords = [];
    list.forEach(r => {
      if (r.year < minYear || r.year > maxYear) return;

      const rawVal = r.rawStats?.[statKey];
      if (rawVal !== undefined && rawVal !== null && rawVal > 0) {
        const adj = calculateEraAdjustedStat(eraKey, rawVal, r.year, { adjustRoster, adjustMlb });
        const sortVal = (adjustRoster || adjustMlb) ? adj.adjustedVal : rawVal;

        const sMetrics = seasonCategoryMetrics[`${r.year}_${statKey}`];
        const seasonMean = sMetrics ? sMetrics.mean : null;
        let diffVsAvg = null;
        let pctVsAvg = null;
        if (seasonMean !== null && seasonMean > 0) {
          diffVsAvg = higherIsBetter ? rawVal - seasonMean : seasonMean - rawVal;
          pctVsAvg = ((higherIsBetter ? rawVal - seasonMean : seasonMean - rawVal) / seasonMean) * 100;
        }

        validRecords.push({
          year: r.year,
          owner: r.owner,
          teamName: r.teamName,
          place: r.place,
          rotoPoints: r.categoryRanks?.[statKey] || 0,
          rawVal,
          adjustedVal: adj.adjustedVal,
          isAdjusted: adj.isAdjusted,
          sortVal,
          seasonMean,
          diffVsAvg,
          pctVsAvg,
        });
      }
    });

    const highs = [...validRecords].sort((a, b) => {
      return higherIsBetter ? b.sortVal - a.sortVal : a.sortVal - b.sortVal;
    }).slice(0, 5);

    const candidateLows = exclude2020Lowlights
      ? validRecords.filter(r => r.year !== 2020)
      : validRecords;

    const lows = [...candidateLows].sort((a, b) => {
      return higherIsBetter ? a.sortVal - b.sortVal : b.sortVal - a.sortVal;
    }).slice(0, 5);

    categoryStatRecords[statKey] = {
      cat,
      highs,
      lows,
      totalTracked: validRecords.length,
    };

    const relHighs = [...validRecords]
      .filter(r => r.pctVsAvg !== null)
      .sort((a, b) => b.pctVsAvg - a.pctVsAvg)
      .slice(0, 5);

    const candidateRelLows = exclude2020Lowlights
      ? validRecords.filter(r => r.year !== 2020 && r.pctVsAvg !== null)
      : validRecords.filter(r => r.pctVsAvg !== null);

    const relLows = [...candidateRelLows]
      .sort((a, b) => a.pctVsAvg - b.pctVsAvg)
      .slice(0, 5);

    relativeDominanceRecords[statKey] = {
      cat,
      highs: relHighs,
      lows: relLows,
      totalTracked: validRecords.length,
    };

    const seasonMetricsForCat = [];
    AVAILABLE_SEASONS.filter(y => y !== 'ALL').forEach(yr => {
      const m = seasonCategoryMetrics[`${yr}_${statKey}`];
      if (m) seasonMetricsForCat.push(m);
    });

    const gaps12 = [...seasonMetricsForCat]
      .sort((a, b) => b.pctGap12 - a.pctGap12)
      .slice(0, 5);

    const candidateGapsLast = exclude2020Lowlights
      ? seasonMetricsForCat.filter(m => m.year !== 2020)
      : seasonMetricsForCat;

    const gapsLast = [...candidateGapsLast]
      .sort((a, b) => b.pctGapLast - a.pctGapLast)
      .slice(0, 5);

    categoryGapsRecords[statKey] = {
      cat,
      gaps12,
      gapsLast,
      totalSeasons: seasonMetricsForCat.length,
    };
  });

  const legacyStatRecords = {};
  LEGACY_CATEGORIES.forEach(cat => {
    const higherIsBetter = cat.higherIsBetter;
    const statField = cat.statField;
    const rankField = cat.rankField;
    const minYear = cat.activeYears[0];
    const maxYear = cat.activeYears[1];

    const validRecords = [];
    list.forEach(r => {
      if (r.year < minYear || r.year > maxYear) return;
      const rawVal = r.rawStats?.[statField];
      if (rawVal !== undefined && rawVal !== null && rawVal > 0) {
        const sMetrics = seasonCategoryMetrics[`${r.year}_${cat.key}`];
        const seasonMean = sMetrics ? sMetrics.mean : null;
        let diffVsAvg = null;
        let pctVsAvg = null;
        if (seasonMean !== null && seasonMean > 0) {
          diffVsAvg = higherIsBetter ? rawVal - seasonMean : seasonMean - rawVal;
          pctVsAvg = ((higherIsBetter ? rawVal - seasonMean : seasonMean - rawVal) / seasonMean) * 100;
        }

        validRecords.push({
          year: r.year,
          owner: r.owner,
          teamName: r.teamName,
          place: r.place,
          rotoPoints: r.categoryRanks?.[rankField] || 0,
          rawVal,
          adjustedVal: rawVal,
          isAdjusted: false,
          sortVal: rawVal,
          seasonMean,
          diffVsAvg,
          pctVsAvg,
        });
      }
    });

    const highs = [...validRecords].sort((a, b) => {
      return higherIsBetter ? b.sortVal - a.sortVal : a.sortVal - b.sortVal;
    }).slice(0, 5);

    const candidateLows = exclude2020Lowlights
      ? validRecords.filter(r => r.year !== 2020)
      : validRecords;

    const lows = [...candidateLows].sort((a, b) => {
      return higherIsBetter ? a.sortVal - b.sortVal : b.sortVal - a.sortVal;
    }).slice(0, 5);

    legacyStatRecords[cat.key] = {
      cat,
      highs,
      lows,
      totalTracked: validRecords.length,
    };

    const relHighs = [...validRecords]
      .filter(r => r.pctVsAvg !== null)
      .sort((a, b) => b.pctVsAvg - a.pctVsAvg)
      .slice(0, 5);

    const candidateRelLows = exclude2020Lowlights
      ? validRecords.filter(r => r.year !== 2020 && r.pctVsAvg !== null)
      : validRecords.filter(r => r.pctVsAvg !== null);

    const relLows = [...candidateRelLows]
      .sort((a, b) => a.pctVsAvg - b.pctVsAvg)
      .slice(0, 5);

    relativeDominanceRecords[cat.key] = {
      cat,
      highs: relHighs,
      lows: relLows,
      totalTracked: validRecords.length,
    };

    const seasonMetricsForCat = [];
    AVAILABLE_SEASONS.filter(y => y !== 'ALL').forEach(yr => {
      const m = seasonCategoryMetrics[`${yr}_${cat.key}`];
      if (m) seasonMetricsForCat.push(m);
    });

    const gaps12 = [...seasonMetricsForCat]
      .sort((a, b) => b.pctGap12 - a.pctGap12)
      .slice(0, 5);

    const candidateGapsLast = exclude2020Lowlights
      ? seasonMetricsForCat.filter(m => m.year !== 2020)
      : seasonMetricsForCat;

    const gapsLast = [...candidateGapsLast]
      .sort((a, b) => b.pctGapLast - a.pctGapLast)
      .slice(0, 5);

    categoryGapsRecords[cat.key] = {
      cat,
      gaps12,
      gapsLast,
      totalSeasons: seasonMetricsForCat.length,
    };
  });

  const allSeasonCategoryMetricsList = Object.values(seasonCategoryMetrics);

  const topOverall12Gaps = [...allSeasonCategoryMetricsList]
    .filter(m => m.gap12 > 0)
    .sort((a, b) => b.pctGap12 - a.pctGap12)
    .slice(0, 10);

  const candidateOverallLastGaps = exclude2020Lowlights
    ? allSeasonCategoryMetricsList.filter(m => m.year !== 2020 && m.gapLast > 0)
    : allSeasonCategoryMetricsList.filter(m => m.gapLast > 0);

  const topOverallLastGaps = [...candidateOverallLastGaps]
    .sort((a, b) => b.pctGapLast - a.pctGapLast)
    .slice(0, 10);

  return {
    topScoring,
    lowestScoring,
    topHitting,
    topPitching,
    blowouts,
    hangovers,
    categoryStatRecords,
    legacyStatRecords,
    relativeDominanceRecords,
    categoryGapsRecords,
    topOverall12Gaps,
    topOverallLastGaps,
    seasonCategoryMetrics,
  };
}

export function computeFranchiseRecords(augmentedFinishes) {
  const summaryByOwner = {};

  augmentedFinishes.forEach(r => {
    const o = normalizeOwner(r.owner);
    if (!summaryByOwner[o]) {
      summaryByOwner[o] = {
        owner: o,
        seasons: 0,
        titles: 0,
        titleYears: [],
        podiums: 0,
        podiumYears: [],
        totalPoints: 0,
        bestPlace: 99,
        worstPlace: 0,
        places: [],
        finishesByYear: {},
      };
    }
    const s = summaryByOwner[o];
    s.seasons += 1;
    s.totalPoints += r.points;
    s.places.push(r.place);
    s.finishesByYear[r.year] = {
      place: r.place,
      points: r.points,
      teamName: r.teamName,
    };

    if (r.year !== 2026) {
      if (r.place === 1) {
        s.titles += 1;
        s.titleYears.push(r.year);
      }
      if (r.place <= 3) {
        s.podiums += 1;
        s.podiumYears.push(r.year);
      }
    }
    if (r.place < s.bestPlace) s.bestPlace = r.place;
    if (r.place > s.worstPlace) s.worstPlace = r.place;
  });

  const list = Object.values(summaryByOwner).map(s => {
    const avgNum = s.places.length ? (s.places.reduce((a, b) => a + b, 0) / s.places.length) : 99;
    const ptsPerSeasonNum = s.seasons ? (s.totalPoints / s.seasons) : 0;
    return {
      ...s,
      avgPlace: s.places.length ? avgNum.toFixed(2) : '-',
      avgPlaceNum: avgNum,
      ptsPerSeason: s.seasons ? ptsPerSeasonNum.toFixed(1) : '-',
      ptsPerSeasonNum: ptsPerSeasonNum,
    };
  });

  list.sort((a, b) => {
    if (b.titles !== a.titles) return b.titles - a.titles;
    if (b.podiums !== a.podiums) return b.podiums - a.podiums;
    return parseFloat(b.totalPoints) - parseFloat(a.totalPoints);
  });

  return list;
}
