// src/views/LeagueHistoryView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import { get, set } from 'idb-keyval';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';
import {
  ROSTER_CAPACITIES,
  MLB_LEAGUE_AVERAGES,
  RATE_STATS,
  calculateEraAdjustedStat,
} from '../utils/eraAdjustments';
import leagueContextData from '../data/league_context.json';
import historicalRawStats from '../data/historicalRawStats.json';

const CANONICAL_OWNERS = {
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
  preston: 'Preston'
};

const normalizeOwner = (raw) => {
  if (!raw) return 'Unknown';
  const k = raw.trim().toLowerCase();
  return CANONICAL_OWNERS[k] || raw.trim();
};

const formatRawStat = (key, val) => {
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

const formatDiffStat = (key, diff) => {
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

const formatPctDiff = (pct) => {
  if (pct === null || pct === undefined || isNaN(pct)) return '—';
  const sign = pct > 0 ? '+' : '';
  return `${sign}${pct.toFixed(1)}%`;
};

const formatMeanStat = (key, val) => {
  if (val === null || val === undefined || isNaN(val)) return '—';
  if (key === 'OBP' || key === 'AVG') {
    return val < 1 ? val.toFixed(3).replace(/^0/, '') : val.toFixed(3);
  }
  if (key === 'ERA' || key === 'WHIP') {
    return val.toFixed(2);
  }
  return val.toFixed(1);
};

const getDisplayRawStat = (catKey, row) => {
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

const AVAILABLE_SEASONS = [
  'ALL',
  2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012
];

const CATEGORIES = [
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

const LEGACY_CATEGORIES = [
  { key: 'AVG', label: 'BA', name: 'Batting Average', type: 'bat', higherIsBetter: true, activeYears: [2012, 2012], statField: 'AVG', rankField: 'OBP', legacyNote: 'Used in 2012 instead of OBP' },
  { key: 'W', label: 'W', name: 'Wins', type: 'pitch', higherIsBetter: true, activeYears: [2012, 2013], statField: 'W', rankField: 'QS', legacyNote: 'Used in 2012–2013 instead of QS' },
  { key: 'SV', label: 'SV', name: 'Saves', type: 'pitch', higherIsBetter: true, activeYears: [2012, 2018], statField: 'SV', rankField: 'SVHLD', legacyNote: 'Used in 2012–2018 instead of Saves+Holds' },
];

const CAT_ICONS = {
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

const GCS_HISTORICAL_FINISHES = 'https://storage.googleapis.com/fantasy-draft-2026/historical-finish.json';

export default function LeagueHistoryView({
  selectedSeason = 2026,
  onOwnerClick,
}) {
  const [activeSubTab, setActiveSubTab] = useState('standings'); // 'standings' | 'highlights' | 'records'
  const [loading, setLoading] = useState(true);
  const [rawFinishes, setRawFinishes] = useState([]);
  const [selectedYear, setSelectedYear] = useState(selectedSeason ? String(selectedSeason) : 'ALL');
  const [selectedOwner, setSelectedOwner] = useState('ALL');
  const [dataSource, setDataSource] = useState('');

  // Era adjustment states (similar to HighlightsView)
  const [adjustRoster, setAdjustRoster] = useState(false);
  const [adjustMlb, setAdjustMlb] = useState(false);
  const [showMethodologyModal, setShowMethodologyModal] = useState(false);
  const [highlightFilter, setHighlightFilter] = useState('all'); // 'all' | 'highlights' | 'lowlights'
  const [exclude2020Lowlights, setExclude2020Lowlights] = useState(true);
  const [highlightViewMode, setHighlightViewMode] = useState('totals'); // 'totals' | 'relative' | 'margins'
  const [marginsGapType, setMarginsGapType] = useState('all'); // 'all' | '1st_2nd' | 'last_2nd_last'

  // Standings display controls
  const [showRawStats, setShowRawStats] = useState(selectedYear === 'ALL');
  const [currentCategoriesOnly, setCurrentCategoriesOnly] = useState(false);

  // Highlights stat records filter
  const [selectedStatCategory, setSelectedStatCategory] = useState('ALL'); // 'ALL' | 'R' | 'HR' | ...

  // Sorting state for standings table
  const [sortField, setSortField] = useState('points');
  const [sortDirection, setSortDirection] = useState('desc');

  // Sorting state for Franchise Hall leaderboard table
  const [hallSortField, setHallSortField] = useState('titles');
  const [hallSortDirection, setHallSortDirection] = useState('desc');

  // Close methodology modal on Escape key
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') setShowMethodologyModal(false);
    };
    if (showMethodologyModal) {
      window.addEventListener('keydown', handleKeyDown);
      return () => window.removeEventListener('keydown', handleKeyDown);
    }
  }, [showMethodologyModal]);

  // Load historical finishes from Supabase, IndexedDB, or fallback context
  useEffect(() => {
    let isMounted = true;

    async function fetchHistoricalFinishes() {
      setLoading(true);

      // 1. Check IndexedDB Cache first
      try {
        const cached = await get('league_history_finishes_v3');
        if (cached && cached.length > 0 && isMounted) {
          const normalized = cached.map(r => ({ ...r, owner: normalizeOwner(r.owner) }));
          setRawFinishes(normalized);
          setDataSource('IndexedDB Cache');
          setLoading(false);
        }
      } catch (cacheErr) {
        console.warn('IndexedDB read notice:', cacheErr);
      }

      // 2. Fetch from Supabase historical_finishes
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

      // 3. Fallback to GCS or league_context.json if Supabase was empty
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

      // 4. Merge 2026 Regular Season Final Standings from league_context.json
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

      // Filter out 2026 if already present, then prepend
      const combined = [...records2026, ...dbRecords.filter(r => r.year !== 2026)];

      if (isMounted && combined.length > 0) {
        setRawFinishes(combined);
        setDataSource(dbRecords.length ? 'Supabase Warehouse + 2026 Standings' : 'GCS Archive + 2026 Standings');
        setLoading(false);
        try {
          await set('league_history_finishes_v3', combined);
        } catch (writeErr) {
          console.warn('Could not update IndexedDB cache:', writeErr);
        }
      }
    }

    fetchHistoricalFinishes();
    return () => {
      isMounted = false;
    };
  }, []);

  // Compute total teams per season
  const teamsPerSeason = useMemo(() => {
    const map = {};
    rawFinishes.forEach(r => {
      map[r.year] = (map[r.year] || 0) + 1;
    });
    return map;
  }, [rawFinishes]);

  // Compute Era-Adjusted Points for a record
  const getAdjustedRecord = useMemo(() => {
    return (record) => {
      const year = record.year;
      const totalTeams = teamsPerSeason[year] || 10;
      const maxPossiblePoints = totalTeams * 10;

      // 1. Dominance Index: Scale to universal 100-point index
      const dominanceIndex = maxPossiblePoints > 0
        ? (record.points / maxPossiblePoints) * 100
        : record.points;

      // 2. Roster Capacity Multiplier (13/9 vs 16/12)
      // 2012-2025 total active = 22; 2026 total active = 28.
      const rosterMultiplier = adjustRoster
        ? (year === 2026 ? 1.0 : ROSTER_CAPACITIES.baseline.total / (ROSTER_CAPACITIES.seasons[year]?.total || 22))
        : 1.0;

      // 3. MLB Environment Multiplier (using composite counting factor)
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
    };
  }, [teamsPerSeason, adjustRoster, adjustMlb]);

  // Augmented finishes with adjustment metrics and raw stats
  const augmentedFinishes = useMemo(() => {
    return rawFinishes.map(r => {
      const normOwner = normalizeOwner(r.owner);
      const baseAdj = getAdjustedRecord({ ...r, owner: normOwner });
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
  }, [rawFinishes, getAdjustedRecord]);

  // Filtered finishes based on user selection
  const filteredFinishes = useMemo(() => {
    return augmentedFinishes.filter(r => {
      if (currentCategoriesOnly && r.year < 2019) return false;
      if (selectedYear !== 'ALL' && r.year !== parseInt(selectedYear, 10)) return false;
      if (selectedOwner !== 'ALL' && normalizeOwner(r.owner).toLowerCase() !== normalizeOwner(selectedOwner).toLowerCase()) return false;
      return true;
    });
  }, [augmentedFinishes, selectedYear, selectedOwner, currentCategoriesOnly]);

  // Sorted finishes for Year-by-Year table
  const sortedFinishes = useMemo(() => {
    const list = [...filteredFinishes];
    list.sort((a, b) => {
      let valA, valB;
      if (sortField === 'place') {
        valA = a.place;
        valB = b.place;
        return sortDirection === 'asc' ? valA - valB : valB - valA;
      }
      if (sortField === 'points') {
        valA = a.isAdjusted ? a.adjustedPoints : a.points;
        valB = b.isAdjusted ? b.adjustedPoints : b.points;
      } else if (sortField === 'hitting') {
        valA = a.hittingPoints;
        valB = b.hittingPoints;
      } else if (sortField === 'pitching') {
        valA = a.pitchingPoints;
        valB = b.pitchingPoints;
      } else if (CATEGORIES.some(c => c.key === sortField)) {
        const isRawSort = showRawStats || selectedYear === 'ALL';
        if (isRawSort) {
          let rawA = a.rawStats?.[sortField];
          let rawB = b.rawStats?.[sortField];
          if (rawA === undefined || rawA === null) {
            if (sortField === 'OBP' && a.year === 2012) rawA = a.rawStats?.AVG;
            else if (sortField === 'QS' && a.year <= 2013) rawA = a.rawStats?.W;
            else if (sortField === 'SVHLD' && a.year <= 2018) rawA = a.rawStats?.SV;
          }
          if (rawB === undefined || rawB === null) {
            if (sortField === 'OBP' && b.year === 2012) rawB = b.rawStats?.AVG;
            else if (sortField === 'QS' && b.year <= 2013) rawB = b.rawStats?.W;
            else if (sortField === 'SVHLD' && b.year <= 2018) rawB = b.rawStats?.SV;
          }

          if (rawA !== undefined && rawA !== null && rawB !== undefined && rawB !== null) {
            valA = parseFloat(rawA) || 0;
            valB = parseFloat(rawB) || 0;
          } else {
            valA = a.categoryRanks[sortField] || 0;
            valB = b.categoryRanks[sortField] || 0;
          }
        } else {
          valA = a.categoryRanks[sortField] || 0;
          valB = b.categoryRanks[sortField] || 0;
        }
      } else {
        valA = a[sortField];
        valB = b[sortField];
      }

      if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
      if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
      return a.place - b.place;
    });
    return list;
  }, [filteredFinishes, sortField, sortDirection, showRawStats, selectedYear]);

  // Unique list of owners across history
  const allOwners = useMemo(() => {
    const set = new Set();
    rawFinishes.forEach(r => {
      const o = normalizeOwner(r.owner);
      if (o) set.add(o);
    });
    return Array.from(set).sort();
  }, [rawFinishes]);

  // Handle table header sorting
  const handleSort = (field) => {
    if (sortField === field) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      const cat = CATEGORIES.find(c => c.key === field);
      const isRawSort = showRawStats || selectedYear === 'ALL';
      const preferAsc = field === 'place' || (isRawSort && cat && !cat.higherIsBetter);
      setSortDirection(preferAsc ? 'asc' : 'desc');
    }
  };

  // Champion for selected single season (hero card) - only for completed seasons
  const selectedSeasonChampion = useMemo(() => {
    if (selectedYear === 'ALL' || selectedYear === '2026') return null;
    const yr = parseInt(selectedYear, 10);
    const seasonRows = augmentedFinishes.filter(r => r.year === yr).sort((a, b) => a.place - b.place);
    if (!seasonRows.length) return null;
    const champ = seasonRows[0];
    const runnerUp = seasonRows[1] || null;
    const thirdPlace = seasonRows[2] || null;
    const margin = runnerUp ? (champ.points - runnerUp.points).toFixed(1) : null;
    return {
      year: yr,
      champ,
      runnerUp,
      thirdPlace,
      margin,
      totalTeams: seasonRows.length
    };
  }, [selectedYear, augmentedFinishes]);

  // ---------------------------------------------------------------------------
  // ALL-TIME HIGHLIGHTS & LOWLIGHTS CALCULATIONS
  // ---------------------------------------------------------------------------

  const highlightsData = useMemo(() => {
    const list = [...augmentedFinishes];

    // 1. Highest scoring finishes ever (Raw vs Adjusted)
    const topScoring = [...list].sort((a, b) => {
      const pA = a.isAdjusted ? a.adjustedPoints : a.points;
      const pB = b.isAdjusted ? b.adjustedPoints : b.points;
      return pB - pA;
    }).slice(0, 10);

    // 2. Lowest scoring finishes ever (Lowlights)
    const lowestScoringList = exclude2020Lowlights
      ? list.filter(r => r.year !== 2020)
      : list;
    const lowestScoring = [...lowestScoringList].sort((a, b) => {
      const pA = a.isAdjusted ? a.adjustedPoints : a.points;
      const pB = b.isAdjusted ? b.adjustedPoints : b.points;
      return pA - pB;
    }).slice(0, 10);

    // 3. Highest Hitting Points in a season
    const topHitting = [...list].sort((a, b) => b.hittingPoints - a.hittingPoints).slice(0, 8);

    // 4. Highest Pitching Points in a season
    const topPitching = [...list].sort((a, b) => b.pitchingPoints - a.pitchingPoints).slice(0, 8);

    // 5. Largest Championship Blowouts (completed seasons only, exclude 2026)
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

    // 6. Championship Hangovers (completed seasons only, exclude 2026)
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

    // 7. Precompute season-level category metrics (mean, stdDev, ranks, gaps) for all categories
    const allCategoriesList = [...CATEGORIES, ...LEGACY_CATEGORIES];
    const seasonCategoryMetrics = {}; // key: `${yr}_${catKey}`

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

          // Sort best to worst
          validSeasonRecords.sort((a, b) => {
            return cat.higherIsBetter ? b.rawVal - a.rawVal : a.rawVal - b.rawVal;
          });

          const first = validSeasonRecords[0];
          const second = validSeasonRecords[1];
          const secondLast = validSeasonRecords[validSeasonRecords.length - 2];
          const last = validSeasonRecords[validSeasonRecords.length - 1];

          // 1st vs 2nd gap
          const gap12 = cat.higherIsBetter
            ? first.rawVal - second.rawVal
            : second.rawVal - first.rawVal;
          const pctGap12 = second.rawVal > 0 ? (gap12 / second.rawVal) * 100 : 0;

          // Last vs 2nd-to-last gap (deficit)
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

    // 8. All-Time Highs and Lows in each individual stat
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
        // Enforce active era boundaries for the category
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

      // Highs (Best): For counting & OBP, highest sortVal. For ERA & WHIP, lowest sortVal.
      const highs = [...validRecords].sort((a, b) => {
        return higherIsBetter ? b.sortVal - a.sortVal : a.sortVal - b.sortVal;
      }).slice(0, 5);

      // Lows (Worst): For counting & OBP, lowest sortVal. For ERA & WHIP, highest sortVal.
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

      // Relative Dominance (% vs Season Average)
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

      // Category single-season margins & gaps
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

    // 9. Legacy Stat Records (BA in 2012, Wins in 2012-13, Saves in 2012-18)
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

      // Relative dominance for legacy category
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

      // Gaps for legacy category
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

    // 10. Top Overall Gaps Across ALL Categories (All-Time Blowouts & Chasm Deficits)
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
  }, [augmentedFinishes, adjustRoster, adjustMlb, exclude2020Lowlights]);

  // ---------------------------------------------------------------------------
  // ALL-TIME FRANCHISE LEADERBOARDS & MATRIX
  // ---------------------------------------------------------------------------

  const franchiseRecords = useMemo(() => {
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

      // Exclude 2026 from titles and podiums since season is still in progress
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

    // Default baseline sort: championships desc, then podiums desc, then total points desc
    list.sort((a, b) => {
      if (b.titles !== a.titles) return b.titles - a.titles;
      if (b.podiums !== a.podiums) return b.podiums - a.podiums;
      return parseFloat(b.totalPoints) - parseFloat(a.totalPoints);
    });

    return list;
  }, [augmentedFinishes]);

  // Sorting handler for Franchise Hall leaderboard table
  const handleHallSort = (field) => {
    if (hallSortField === field) {
      setHallSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setHallSortField(field);
      const preferAsc = field === 'owner' || field === 'avgPlace' || field === 'bestPlace';
      setHallSortDirection(preferAsc ? 'asc' : 'desc');
    }
  };

  // Sorted list for Franchise Career Leaderboard
  const sortedFranchiseRecords = useMemo(() => {
    const list = [...franchiseRecords];
    list.sort((a, b) => {
      let valA, valB;
      if (hallSortField === 'owner') {
        valA = a.owner.toLowerCase();
        valB = b.owner.toLowerCase();
        return hallSortDirection === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      }
      if (hallSortField === 'titles') {
        valA = a.titles;
        valB = b.titles;
        if (valA === valB) {
          if (b.podiums !== a.podiums) return b.podiums - a.podiums;
          return b.totalPoints - a.totalPoints;
        }
      } else if (hallSortField === 'podiums') {
        valA = a.podiums;
        valB = b.podiums;
        if (valA === valB) return b.titles - a.titles;
      } else if (hallSortField === 'seasons') {
        valA = a.seasons;
        valB = b.seasons;
      } else if (hallSortField === 'avgPlace') {
        valA = a.avgPlaceNum;
        valB = b.avgPlaceNum;
        if (valA === valB) return b.titles - a.titles;
      } else if (hallSortField === 'bestPlace') {
        valA = a.bestPlace;
        valB = b.bestPlace;
        if (valA === valB) return a.avgPlaceNum - b.avgPlaceNum;
      } else if (hallSortField === 'worstPlace') {
        valA = a.worstPlace;
        valB = b.worstPlace;
        if (valA === valB) return a.avgPlaceNum - b.avgPlaceNum;
      } else if (hallSortField === 'totalPoints') {
        valA = a.totalPoints;
        valB = b.totalPoints;
      } else if (hallSortField === 'ptsPerSeason') {
        valA = a.ptsPerSeasonNum;
        valB = b.ptsPerSeasonNum;
      } else {
        valA = a[hallSortField];
        valB = b[hallSortField];
      }

      if (valA < valB) return hallSortDirection === 'asc' ? -1 : 1;
      if (valA > valB) return hallSortDirection === 'asc' ? 1 : -1;
      return 0;
    });
    return list;
  }, [franchiseRecords, hallSortField, hallSortDirection]);

  // Label for current Franchise Hall sort
  const hallSortLabel = useMemo(() => {
    const labels = {
      titles: 'Championships',
      podiums: 'Podiums',
      seasons: 'Seasons',
      avgPlace: 'Average Finish',
      bestPlace: 'Best Finish',
      worstPlace: 'Worst Finish',
      totalPoints: 'Total Points',
      ptsPerSeason: 'Points / Season',
      owner: 'Owner Name',
    };
    return labels[hallSortField] || hallSortField;
  }, [hallSortField]);

  // Find team object for onOwnerClick
  const getOwnerTeamObj = (ownerName) => {
    const norm = normalizeOwner(ownerName).toLowerCase();
    return Object.values(TEAMS).find(t => {
      const tNorm = normalizeOwner(t.owner).toLowerCase();
      return tNorm === norm;
    }) || { owner: normalizeOwner(ownerName), name: normalizeOwner(ownerName) };
  };

  // Filtered categories for stat records
  const displayedStatCategories = useMemo(() => {
    if (selectedStatCategory === 'ALL') return CATEGORIES;
    const cat = CATEGORIES.find(c => c.key === selectedStatCategory);
    if (cat) return [cat];
    const leg = LEGACY_CATEGORIES.find(c => c.key === selectedStatCategory);
    if (leg) return [leg];
    return CATEGORIES;
  }, [selectedStatCategory]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8 animate-fade-in">
      {/* HEADER & BRANDING */}
      <div className="bg-gradient-to-r from-slate-900 via-indigo-950 to-slate-900 border border-slate-800 rounded-3xl p-6 sm:p-8 shadow-2xl relative overflow-hidden">
        <div className="absolute top-0 right-0 w-96 h-96 bg-blue-500/10 rounded-full blur-3xl pointer-events-none -mr-20 -mt-20"></div>
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 relative z-10">
          <div>
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-500/10 border border-blue-500/30 text-blue-400 text-xs font-black uppercase tracking-wider mb-3">
              <span>🏛️</span>
              <span>All-Time Archives (2012–2026)</span>
            </div>
            <h1 className="text-3xl sm:text-4xl font-black text-white tracking-tight">
              League History & Finishes Database
            </h1>
            <p className="mt-2 text-sm sm:text-base text-slate-300 max-w-2xl">
              Official archive of annual standings, roto point records, championship lineage, and era-adjusted highlights across 14+ seasons of Heftystrong competition.
            </p>
          </div>

          {/* Sub-Tab Navigation Bar */}
          <div className="flex flex-wrap gap-2 p-1.5 bg-slate-950/80 rounded-2xl border border-slate-800/90 shadow-inner">
            <button
              onClick={() => setActiveSubTab('standings')}
              className={`px-4 py-2 rounded-xl text-xs font-black transition-all cursor-pointer flex items-center gap-2 ${
                activeSubTab === 'standings'
                  ? 'bg-blue-600 text-white shadow-lg ring-1 ring-blue-400'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
              }`}
            >
              <span>🏆</span>
              <span>Annual Standings</span>
            </button>
            <button
              onClick={() => setActiveSubTab('highlights')}
              className={`px-4 py-2 rounded-xl text-xs font-black transition-all cursor-pointer flex items-center gap-2 ${
                activeSubTab === 'highlights'
                  ? 'bg-amber-600 text-white shadow-lg ring-1 ring-amber-400'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
              }`}
            >
              <span>⭐</span>
              <span>Highlights & Lowlights</span>
            </button>
            <button
              onClick={() => setActiveSubTab('records')}
              className={`px-4 py-2 rounded-xl text-xs font-black transition-all cursor-pointer flex items-center gap-2 ${
                activeSubTab === 'records'
                  ? 'bg-emerald-600 text-white shadow-lg ring-1 ring-emerald-400'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
              }`}
            >
              <span>🏛️</span>
              <span>Franchise Hall</span>
            </button>
          </div>
        </div>

        {/* Status bar */}
        <div className="mt-6 pt-4 border-t border-slate-800/80 flex flex-wrap items-center justify-between gap-4 text-xs text-slate-400">
          <div className="flex items-center gap-4">
            <span>📦 Total Finished Seasons: <strong className="text-white">14</strong></span>
            <span>👥 Tracked Team-Seasons: <strong className="text-white">{augmentedFinishes.length}</strong></span>
            <span className="flex items-center gap-1.5">
              ⚡ Source: <span className="text-blue-400 font-mono">{dataSource || (loading ? 'Loading...' : 'Supabase')}</span>
              {loading && <span className="inline-block animate-spin h-3 w-3 border-2 border-blue-400 border-t-transparent rounded-full"></span>}
            </span>
          </div>
          <button
            onClick={() => setShowMethodologyModal(true)}
            className="text-xs text-amber-400 hover:text-amber-300 font-bold flex items-center gap-1.5 transition cursor-pointer underline underline-offset-4"
          >
            <span>ℹ️</span>
            <span>How Era Adjustments Work</span>
          </button>
        </div>
      </div>

      {loading && augmentedFinishes.length === 0 && (
        <div className="bg-slate-900 border border-slate-800 rounded-3xl p-12 text-center shadow-xl space-y-4">
          <div className="inline-block animate-spin rounded-full h-10 w-10 border-4 border-blue-500 border-t-transparent mb-2"></div>
          <div className="font-extrabold text-white text-lg">Loading Historical Finishes...</div>
          <p className="text-slate-400 text-xs">
            Querying Supabase database archive and compiling multi-era records...
          </p>
        </div>
      )}

      {/* -------------------------------------------------------------------- */}
      {/* SUB-TAB 1: ANNUAL STANDINGS & YEAR-BY-YEAR DATABASE                   */}
      {/* -------------------------------------------------------------------- */}
      {activeSubTab === 'standings' && (
        <div className="space-y-6">
          {/* Filter Bar */}
          <div className="bg-white border border-gray-200 rounded-2xl p-4 shadow-sm flex flex-wrap items-center justify-between gap-4">
            <div className="flex flex-wrap items-center gap-3">
              {/* Year Selector */}
              <div className="flex items-center gap-2">
                <span className="text-xs font-black text-gray-500 uppercase">Season:</span>
                <select
                  value={selectedYear}
                  onChange={e => {
                    const yr = e.target.value;
                    setSelectedYear(yr);
                    if (yr === 'ALL') {
                      setShowRawStats(true);
                    }
                    if (yr !== 'ALL' && parseInt(yr, 10) < 2019 && currentCategoriesOnly) {
                      setCurrentCategoriesOnly(false);
                    }
                  }}
                  className="bg-gray-50 border border-gray-300 text-gray-800 text-xs font-black rounded-xl px-3 py-2 focus:ring-2 focus:ring-blue-500 cursor-pointer shadow-xs"
                >
                  {AVAILABLE_SEASONS.filter(y => !currentCategoriesOnly || y === 'ALL' || y >= 2019).map(y => (
                    <option key={y} value={y}>
                      {y === 'ALL' 
                        ? (currentCategoriesOnly ? '🌟 Modern Era (2019–2026)' : '🌟 All Seasons (2012–2026)')
                        : y === 2026 ? '⚾ 2026 (Regular Season)' : `🏛️ ${y} Season`}
                    </option>
                  ))}
                </select>
              </div>

              {/* Owner Filter */}
              <div className="flex items-center gap-2">
                <span className="text-xs font-black text-gray-500 uppercase">Owner:</span>
                <select
                  value={selectedOwner}
                  onChange={e => setSelectedOwner(e.target.value)}
                  className="bg-gray-50 border border-gray-300 text-gray-800 text-xs font-black rounded-xl px-3 py-2 focus:ring-2 focus:ring-blue-500 cursor-pointer shadow-xs"
                >
                  <option value="ALL">👥 All Owners</option>
                  {allOwners.map(o => (
                    <option key={o} value={o}>{o}</option>
                  ))}
                </select>
              </div>
            </div>

            {/* Standings Controls & Toggles */}
            <div className="flex flex-wrap items-center gap-2.5">
              {/* Current Categories Only Toggle */}
              <button
                type="button"
                onClick={() => {
                  setCurrentCategoriesOnly(prev => {
                    const next = !prev;
                    if (next && selectedYear !== 'ALL' && parseInt(selectedYear, 10) < 2019) {
                      setSelectedYear('ALL');
                    }
                    return next;
                  });
                }}
                className={`px-3 py-1.5 rounded-xl text-xs font-black flex items-center gap-1.5 transition cursor-pointer shadow-xs ${
                  currentCategoriesOnly
                    ? 'bg-emerald-600 text-white shadow-md ring-1 ring-emerald-400'
                    : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'
                }`}
                title="Only include seasons (2019–2026) using the current 10 stat categories (OBP, QS, SV+H)"
              >
                <span>🎯</span>
                <span>{currentCategoriesOnly ? 'Current Categories Only (2019–2026)' : 'Current Categories (2019+)'}</span>
              </button>

              <button
                type="button"
                onClick={() => setShowRawStats(prev => !prev)}
                className={`px-3 py-1.5 rounded-xl text-xs font-black flex items-center gap-1.5 transition cursor-pointer shadow-xs ${
                  showRawStats
                    ? 'bg-blue-600 text-white shadow-md ring-1 ring-blue-400'
                    : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'
                }`}
                title="Toggle displaying the underlying raw statistics underneath the roto point ranks"
              >
                <span>🔢</span>
                <span>{showRawStats ? 'Hide Raw Stats' : 'Show Raw Stats'}</span>
              </button>

              <label className="flex items-center gap-1.5 text-xs font-bold text-gray-600 cursor-pointer bg-gray-50 px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-100 transition">
                <input
                  type="checkbox"
                  checked={adjustRoster}
                  onChange={e => setAdjustRoster(e.target.checked)}
                  className="rounded text-blue-600 focus:ring-blue-500 cursor-pointer"
                />
                <span>Roster Adjusted (22➔28)</span>
              </label>
              <label className="flex items-center gap-1.5 text-xs font-bold text-gray-600 cursor-pointer bg-gray-50 px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-100 transition">
                <input
                  type="checkbox"
                  checked={adjustMlb}
                  onChange={e => setAdjustMlb(e.target.checked)}
                  className="rounded text-blue-600 focus:ring-blue-500 cursor-pointer"
                />
                <span>MLB Era Adjusted</span>
              </label>
            </div>
          </div>

          {/* Season Hero Spotlight (when a single season is selected) */}
          {selectedSeasonChampion && (
            <div className="bg-gradient-to-br from-amber-500/10 via-amber-500/5 to-transparent border border-amber-400/40 rounded-3xl p-6 shadow-md flex flex-col md:flex-row items-center justify-between gap-6">
              <div className="flex items-center gap-4">
                <div className="w-16 h-16 rounded-2xl bg-amber-500/20 border-2 border-amber-400 flex items-center justify-center text-3xl shadow-inner">
                  🏆
                </div>
                <div>
                  <div className="text-xs font-black text-amber-700 uppercase tracking-wider">
                    {selectedSeasonChampion.year} League Champion
                  </div>
                  <div className="text-2xl font-black text-gray-900 flex items-center gap-2">
                    <span>{selectedSeasonChampion.champ.owner}</span>
                    <span className="text-base text-gray-500 font-normal">({selectedSeasonChampion.champ.teamName})</span>
                  </div>
                  <div className="text-xs text-gray-600 mt-1">
                    Recorded <strong className="text-amber-800 font-black">{selectedSeasonChampion.champ.points} roto points</strong> across {selectedSeasonChampion.totalTeams} teams
                    {selectedSeasonChampion.margin && (
                      <span> • Won by <strong className="text-emerald-700 font-bold">+{selectedSeasonChampion.margin} pts</strong></span>
                    )}
                  </div>
                </div>
              </div>

              {/* Podium summary */}
              <div className="flex items-center gap-4 text-xs font-bold">
                {selectedSeasonChampion.runnerUp && (
                  <div className="bg-white/80 border border-gray-200 rounded-xl px-3 py-2 shadow-xs">
                    <span className="text-gray-400 uppercase text-[10px] block font-black">🥈 Runner-Up</span>
                    <span className="text-gray-800 font-black">{selectedSeasonChampion.runnerUp.owner}</span>
                    <span className="text-gray-500 ml-1">({selectedSeasonChampion.runnerUp.points} pts)</span>
                  </div>
                )}
                {selectedSeasonChampion.thirdPlace && (
                  <div className="bg-white/80 border border-gray-200 rounded-xl px-3 py-2 shadow-xs">
                    <span className="text-gray-400 uppercase text-[10px] block font-black">🥉 3rd Place</span>
                    <span className="text-gray-800 font-black">{selectedSeasonChampion.thirdPlace.owner}</span>
                    <span className="text-gray-500 ml-1">({selectedSeasonChampion.thirdPlace.points} pts)</span>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* 2026 In-Progress Notice */}
          {selectedYear === '2026' && (
            <div className="bg-gradient-to-br from-blue-500/10 via-indigo-500/5 to-transparent border border-blue-400/30 rounded-3xl p-6 shadow-sm flex items-center gap-4">
              <div className="w-14 h-14 rounded-2xl bg-blue-500/20 border-2 border-blue-400 flex items-center justify-center text-3xl shadow-inner shrink-0">
                ⚾
              </div>
              <div>
                <div className="text-xs font-black text-blue-700 uppercase tracking-wider flex items-center gap-2">
                  <span>2026 Regular Season In Progress</span>
                  <span className="inline-block px-2 py-0.5 rounded-full bg-blue-100 text-blue-800 text-[10px] font-bold">Active Season</span>
                </div>
                <div className="text-lg font-black text-gray-900 mt-0.5">
                  Standings reflect Week 23 regular season table.
                </div>
                <div className="text-xs text-gray-600 mt-1">
                  The 2026 League Champion will be crowned upon completion of the postseason playoffs.
                </div>
              </div>
            </div>
          )}

          {/* Standings Table */}
          <div className="bg-white border border-gray-200 rounded-3xl shadow-xl overflow-hidden">
            <div className="p-4 sm:p-5 border-b border-gray-100 flex items-center justify-between bg-gray-50/70">
              <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
                <span>📋</span>
                <span>Standings & Category Breakdown</span>
                <span className="text-xs text-gray-500 font-normal">
                  ({sortedFinishes.length} finishes found {currentCategoriesOnly && '• 2019–2026 current format'})
                </span>
              </h2>
              <div className="text-xs text-gray-500 hidden sm:block">
                Click headers to sort • Click owner to view profile
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-xs text-left">
                <thead className="bg-gray-100/90 text-gray-700 font-black uppercase text-[10px] tracking-wider border-b border-gray-200 select-none">
                  <tr>
                    <th onClick={() => handleSort('year')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition">Year</th>
                    <th onClick={() => handleSort('place')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition">Place</th>
                    <th onClick={() => handleSort('owner')} className="py-3 px-4 cursor-pointer hover:bg-gray-200 transition">Owner & Team</th>
                    <th onClick={() => handleSort('points')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition text-right bg-blue-50/60 text-blue-900">
                      {adjustRoster || adjustMlb ? 'Adj Pts' : 'Roto Pts'}
                    </th>
                    <th onClick={() => handleSort('hitting')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition text-center">Hit Pts</th>
                    <th onClick={() => handleSort('pitching')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition text-center">Pitch Pts</th>
                    {CATEGORIES.map(cat => {
                      let legacySub = null;
                      if (!currentCategoriesOnly) {
                        if (cat.key === 'OBP') legacySub = "BA in '12";
                        else if (cat.key === 'QS') legacySub = "W in '12–'13";
                        else if (cat.key === 'SVHLD') legacySub = "SV in '12–'18";
                      }

                      return (
                        <th
                          key={cat.key}
                          onClick={() => handleSort(cat.key)}
                          className={`py-3 px-2 text-center cursor-pointer hover:bg-gray-200 transition ${
                            cat.type === 'bat' ? 'bg-amber-50/40 text-amber-900' : 'bg-indigo-50/40 text-indigo-900'
                          }`}
                          title={cat.name + (cat.legacyNote ? ` • ${cat.legacyNote}` : '')}
                        >
                          <div>{cat.label}</div>
                          {showRawStats && (
                            <div className="text-[8px] font-normal text-gray-500 font-sans normal-case tracking-normal mt-0.5">
                              {legacySub ? (
                                <span className="text-amber-700 font-bold" title={cat.legacyNote}>{legacySub}</span>
                              ) : (
                                'pts / raw'
                              )}
                            </div>
                          )}
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {sortedFinishes.map((row, idx) => {
                    const isChamp = row.place === 1 && row.year !== 2026;
                    const isPodium = row.place <= 3 && row.year !== 2026;
                    const dispPoints = row.isAdjusted ? row.adjustedPoints.toFixed(1) : row.points.toFixed(1);

                    // Split bar percentage
                    const totalCatPts = (row.hittingPoints + row.pitchingPoints) || 1;
                    const hitPct = Math.round((row.hittingPoints / totalCatPts) * 100);

                    return (
                      <tr
                        key={`${row.year}-${row.owner}-${idx}`}
                        className={`transition-colors hover:bg-blue-50/40 ${
                          isChamp ? 'bg-amber-50/25 font-bold' : isPodium ? 'bg-slate-50/50' : ''
                        }`}
                      >
                        {/* Year */}
                        <td className="py-3 px-3 font-mono font-bold text-gray-500">
                          {row.year}
                        </td>

                        {/* Place Badge */}
                        <td className="py-3 px-3 font-black">
                          <span className={`inline-flex items-center justify-center w-7 h-7 rounded-xl text-xs font-black shadow-xs ${
                            isChamp ? 'bg-amber-400 text-amber-950 ring-2 ring-amber-300' :
                            (isPodium && row.place === 2) ? 'bg-slate-300 text-slate-900' :
                            (isPodium && row.place === 3) ? 'bg-amber-700 text-amber-100' :
                            'bg-gray-100 text-gray-600'
                          }`}>
                            {isChamp ? '🥇' : (isPodium && row.place === 2) ? '🥈' : (isPodium && row.place === 3) ? '🥉' : `#${row.place}`}
                          </span>
                        </td>

                        {/* Owner & Team */}
                        <td className="py-3 px-4">
                          <div
                            onClick={() => onOwnerClick && onOwnerClick(getOwnerTeamObj(row.owner))}
                            className="flex items-center gap-2.5 cursor-pointer group"
                          >
                            <TeamAvatar team={{ owner: row.owner }} size="sm" className="group-hover:scale-105 transition-transform" />
                            <div>
                              <div className="font-black text-gray-900 group-hover:text-blue-600 transition flex items-center gap-1.5">
                                <span>{row.owner}</span>
                                {isChamp && <span className="text-[10px] text-amber-500">🏆</span>}
                              </div>
                              <div className="text-[11px] text-gray-500 truncate max-w-[180px] sm:max-w-[240px]">
                                {row.teamName}
                              </div>
                            </div>
                          </div>
                        </td>

                        {/* Points (Total Roto) */}
                        <td className="py-3 px-3 text-right font-mono font-black text-sm text-blue-700 bg-blue-50/40">
                          {dispPoints}
                          {row.isAdjusted && (
                            <span className="block text-[10px] text-gray-400 font-normal">raw: {row.points.toFixed(1)}</span>
                          )}
                        </td>

                        {/* Hitting Points */}
                        <td className="py-3 px-3 text-center font-mono font-bold text-amber-800">
                          {row.hittingPoints}
                        </td>

                        {/* Pitching Points */}
                        <td className="py-3 px-3 text-center font-mono font-bold text-indigo-800">
                          {row.pitchingPoints}
                          {/* Mini visual split bar */}
                          <div className="w-12 mx-auto mt-1 h-1 rounded-full bg-gray-200 overflow-hidden flex" title={`${hitPct}% Hit / ${100-hitPct}% Pitch`}>
                            <div className="bg-amber-500 h-full" style={{ width: `${hitPct}%` }}></div>
                            <div className="bg-indigo-500 h-full" style={{ width: `${100-hitPct}%` }}></div>
                          </div>
                        </td>

                        {/* 10 Category Breakdown */}
                        {CATEGORIES.map(cat => {
                          const val = row.categoryRanks[cat.key];
                          const maxCatVal = Math.max(...sortedFinishes.filter(f => f.year === row.year).map(f => f.categoryRanks[cat.key] || 0));
                          const isLeader = val && val === maxCatVal && val > 0;
                          const { text: rawFormatted, badge, title } = getDisplayRawStat(cat.key, row);
                          return (
                            <td
                              key={cat.key}
                              className={`py-2 px-2 text-center font-mono ${
                                isLeader ? 'bg-emerald-100/70 text-emerald-950 font-black' : 'text-gray-700'
                              }`}
                            >
                              <div className="font-bold text-xs">{val !== undefined ? val : '-'}</div>
                              {showRawStats && (
                                <div
                                  className="text-[10px] font-mono font-medium text-slate-500 mt-0.5 whitespace-nowrap flex items-center justify-center gap-1"
                                  title={title || undefined}
                                >
                                  <span>{rawFormatted}</span>
                                  {badge && (
                                    <span className="text-[9px] px-1 py-0.2 rounded bg-amber-100 text-amber-900 font-sans font-bold border border-amber-300">
                                      {badge}
                                    </span>
                                  )}
                                </div>
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* -------------------------------------------------------------------- */}
      {/* SUB-TAB 2: HIGHLIGHTS & LOWLIGHTS OF FINISHES (ERA ADJUSTED)          */}
      {/* -------------------------------------------------------------------- */}
      {activeSubTab === 'highlights' && (
        <div className="space-y-8">
          {/* Era & Roster Controls Header */}
          <div className="bg-gradient-to-r from-amber-500/10 via-amber-500/5 to-transparent border border-amber-300/60 rounded-3xl p-6 shadow-sm flex flex-col md:flex-row items-center justify-between gap-6">
            <div>
              <h2 className="text-xl font-black text-gray-900 flex items-center gap-2">
                <span>⭐</span>
                <span>Era & Roster Adjusted Finishes Engine</span>
              </h2>
              <p className="text-xs text-gray-600 mt-1 max-w-2xl">
                Compare historical dominance across different league sizes (8, 9, 10 teams), roster expansions (22 active starters to 28), and varying MLB ball environments.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <label className="flex items-center gap-2 bg-white px-3.5 py-2 rounded-2xl border border-gray-300 shadow-xs cursor-pointer hover:border-amber-400 transition text-xs font-black text-gray-800">
                <input
                  type="checkbox"
                  checked={adjustRoster}
                  onChange={e => setAdjustRoster(e.target.checked)}
                  className="rounded text-amber-600 focus:ring-amber-500 cursor-pointer"
                />
                <span>Roster Size Adjusted (22➔28)</span>
              </label>

              <label className="flex items-center gap-2 bg-white px-3.5 py-2 rounded-2xl border border-gray-300 shadow-xs cursor-pointer hover:border-amber-400 transition text-xs font-black text-gray-800">
                <input
                  type="checkbox"
                  checked={adjustMlb}
                  onChange={e => setAdjustMlb(e.target.checked)}
                  className="rounded text-amber-600 focus:ring-amber-500 cursor-pointer"
                />
                <span>MLB Era Adjusted</span>
              </label>

              <label className="flex items-center gap-2 bg-white px-3.5 py-2 rounded-2xl border border-gray-300 shadow-xs cursor-pointer hover:border-rose-400 transition text-xs font-black text-gray-800" title="Exclude the 60-game COVID-shortened 2020 season from all-time lowlights & counting lows">
                <input
                  type="checkbox"
                  checked={exclude2020Lowlights}
                  onChange={e => setExclude2020Lowlights(e.target.checked)}
                  className="rounded text-rose-600 focus:ring-rose-500 cursor-pointer"
                />
                <span className="flex items-center gap-1.5">
                  <span>🦠</span>
                  <span>Exclude 2020 (COVID)</span>
                </span>
              </label>

              {/* View filter */}
              <div className="flex rounded-xl bg-gray-100 p-1 border border-gray-200 text-xs font-black">
                <button
                  onClick={() => setHighlightFilter('all')}
                  className={`px-3 py-1 rounded-lg transition ${highlightFilter === 'all' ? 'bg-white text-gray-900 shadow-xs' : 'text-gray-500'}`}
                >
                  All
                </button>
                <button
                  onClick={() => setHighlightFilter('highlights')}
                  className={`px-3 py-1 rounded-lg transition ${highlightFilter === 'highlights' ? 'bg-amber-500 text-white shadow-xs' : 'text-gray-500'}`}
                >
                  ⭐ Highlights
                </button>
                <button
                  onClick={() => setHighlightFilter('lowlights')}
                  className={`px-3 py-1 rounded-lg transition ${highlightFilter === 'lowlights' ? 'bg-rose-500 text-white shadow-xs' : 'text-gray-500'}`}
                >
                  ⚠️ Lowlights
                </button>
              </div>
            </div>
          </div>

          {/* ALL-TIME STAT RECORDS (HIGHS, LOWS, RELATIVE DOMINANCE & CATEGORY MARGINS) */}
          <div className="space-y-6">
            <div className="flex flex-col gap-4 border-b border-gray-200 pb-4">
              <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                  <div className="flex items-center gap-2 text-sm font-black text-gray-900 uppercase tracking-wider">
                    <span>{highlightViewMode === 'totals' ? '📊' : highlightViewMode === 'relative' ? '📈' : '⚡'}</span>
                    <span>
                      {highlightViewMode === 'totals' && 'All-Time Category Stat Records (Highs & Lows)'}
                      {highlightViewMode === 'relative' && 'Relative Dominance & Deficits (% vs Season League Average)'}
                      {highlightViewMode === 'margins' && 'Historical Category Margins & Gaps (Blowouts & Punts)'}
                    </span>
                    <span className="text-xs text-gray-500 font-normal lowercase">
                      {highlightViewMode === 'totals' && (adjustRoster || adjustMlb ? '(Era & roster adjusted)' : '(Raw single-season totals)')}
                      {highlightViewMode === 'relative' && '(Standardized across eras)'}
                      {highlightViewMode === 'margins' && '(1st vs 2nd Leads & Last vs 2nd-Last Deficits)'}
                    </span>
                  </div>
                  <p className="text-xs text-gray-500 mt-1 max-w-3xl">
                    {highlightViewMode === 'totals' && (
                      <>All-time single-season benchmarks across all 10 roto categories. Evaluates active seasons for each category (OBP: 2013+, QS: 2014+, SV+H: 2019+). Each record displays its comparative margin against that season's league mean.</>
                    )}
                    {highlightViewMode === 'relative' && (
                      <>Ranks single-season performances by percentage above (or below) that season's league mean. Standardizes performance across high and low offensive eras to highlight historically unprecedented dominance or basement collapses.</>
                    )}
                    {highlightViewMode === 'margins' && (
                      <>Measures single-season competitive chasms: runaway category titles (largest margins between 1st and 2nd place) and the basement abyss (largest deficits between last and second-to-last place).</>
                    )}
                  </p>
                </div>

                {/* Sub-view Switcher */}
                <div className="flex flex-wrap items-center gap-1.5 p-1 bg-gray-100 rounded-2xl border border-gray-200 text-xs font-black self-start md:self-auto shrink-0">
                  <button
                    type="button"
                    onClick={() => setHighlightViewMode('totals')}
                    className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${
                      highlightViewMode === 'totals'
                        ? 'bg-white text-gray-900 shadow-xs ring-1 ring-black/5'
                        : 'text-gray-500 hover:text-gray-900'
                    }`}
                  >
                    <span>🏆</span>
                    <span>All-Time Totals</span>
                  </button>
                  <button
                    type="button"
                    onClick={() => setHighlightViewMode('relative')}
                    className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${
                      highlightViewMode === 'relative'
                        ? 'bg-white text-indigo-950 shadow-xs ring-1 ring-black/5'
                        : 'text-gray-500 hover:text-gray-900'
                    }`}
                  >
                    <span>📈</span>
                    <span>Relative (% vs Avg)</span>
                  </button>
                  <button
                    type="button"
                    onClick={() => setHighlightViewMode('margins')}
                    className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${
                      highlightViewMode === 'margins'
                        ? 'bg-white text-amber-950 shadow-xs ring-1 ring-black/5'
                        : 'text-gray-500 hover:text-gray-900'
                    }`}
                  >
                    <span>⚡</span>
                    <span>Margins & Gaps</span>
                  </button>
                </div>
              </div>

              {/* Second row: Margins Gap Type Switcher & Category filter pills */}
              <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-3 pt-1">
                {highlightViewMode === 'margins' ? (
                  <div className="flex items-center gap-1.5 p-1 bg-amber-50 rounded-xl border border-amber-200 text-xs font-bold self-start shrink-0">
                    <button
                      type="button"
                      onClick={() => setMarginsGapType('all')}
                      className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                        marginsGapType === 'all'
                          ? 'bg-amber-600 text-white shadow-xs'
                          : 'text-amber-900 hover:bg-amber-100'
                      }`}
                    >
                      All Margins
                    </button>
                    <button
                      type="button"
                      onClick={() => setMarginsGapType('1st_2nd')}
                      className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                        marginsGapType === '1st_2nd'
                          ? 'bg-amber-600 text-white shadow-xs'
                          : 'text-amber-900 hover:bg-amber-100'
                      }`}
                    >
                      🥇 1st vs 2nd Leads
                    </button>
                    <button
                      type="button"
                      onClick={() => setMarginsGapType('last_2nd_last')}
                      className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                        marginsGapType === 'last_2nd_last'
                          ? 'bg-rose-600 text-white shadow-xs'
                          : 'text-rose-900 hover:bg-rose-100'
                      }`}
                    >
                      ⚠️ Last vs 2nd-Last Deficits
                    </button>
                  </div>
                ) : (
                  <div className="text-xs text-gray-400 font-medium">
                    Filter by stat category:
                  </div>
                )}

                {/* Category filter pills */}
                <div className="flex flex-wrap items-center gap-1.5 overflow-x-auto py-1">
                  <button
                    type="button"
                    onClick={() => setSelectedStatCategory('ALL')}
                    className={`px-2.5 py-1 rounded-lg text-xs font-black transition cursor-pointer ${
                      selectedStatCategory === 'ALL'
                        ? 'bg-blue-600 text-white shadow-xs'
                        : 'bg-white text-gray-700 border border-gray-200 hover:bg-gray-100'
                    }`}
                  >
                    All Stats
                  </button>
                  {CATEGORIES.map(cat => (
                    <button
                      key={cat.key}
                      type="button"
                      onClick={() => setSelectedStatCategory(cat.key)}
                      className={`px-2 py-1 rounded-lg text-xs font-bold transition cursor-pointer flex items-center gap-1 ${
                        selectedStatCategory === cat.key
                          ? (cat.type === 'bat' ? 'bg-amber-600 text-white shadow-xs' : 'bg-indigo-600 text-white shadow-xs')
                          : 'bg-white text-gray-700 border border-gray-200 hover:bg-gray-100'
                      }`}
                    >
                      <span>{CAT_ICONS[cat.key]}</span>
                      <span>{cat.label}</span>
                    </button>
                  ))}

                  {/* Legacy Category Pills */}
                  <div className="hidden sm:inline-block w-px h-5 bg-gray-300 mx-1"></div>
                  {LEGACY_CATEGORIES.map(cat => (
                    <button
                      key={cat.key}
                      type="button"
                      onClick={() => setSelectedStatCategory(cat.key)}
                      className={`px-2 py-1 rounded-lg text-xs font-bold transition cursor-pointer flex items-center gap-1 ${
                        selectedStatCategory === cat.key
                          ? 'bg-purple-700 text-white shadow-xs'
                          : 'bg-purple-50 text-purple-800 border border-purple-200 hover:bg-purple-100'
                      }`}
                      title={`Legacy Category: ${cat.name} (${cat.activeYears[0]}${cat.activeYears[0] !== cat.activeYears[1] ? '–' + cat.activeYears[1] : ''})`}
                    >
                      <span>{CAT_ICONS[cat.key]}</span>
                      <span>{cat.label} ({cat.activeYears[0] === cat.activeYears[1] ? `'${String(cat.activeYears[0]).slice(2)}` : `'${String(cat.activeYears[0]).slice(2)}–'${String(cat.activeYears[1]).slice(2)}`})</span>
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {/* SUB-VIEW 1: ALL-TIME TOTALS (WITH SEASON AVERAGE COMPARISON CHIPS) */}
            {highlightViewMode === 'totals' && (() => {
              const renderCategoryCard = (cat, isLegacy = false) => {
                const record = isLegacy
                  ? highlightsData.legacyStatRecords?.[cat.key]
                  : highlightsData.categoryStatRecords?.[cat.key];
                if (!record) return null;
                const { highs, lows, totalTracked } = record;
                const icon = CAT_ICONS[cat.key] || '⚾';
                const isBat = cat.type === 'bat';
                const showHighs = highlightFilter === 'all' || highlightFilter === 'highlights';
                const showLows = highlightFilter === 'all' || highlightFilter === 'lowlights';
                const twoCols = showHighs && showLows;

                return (
                  <div
                    key={`stat-rec-${cat.key}`}
                    className="bg-white border border-gray-200 rounded-3xl p-5 sm:p-6 shadow-sm space-y-4 hover:shadow-md transition"
                  >
                    {/* Card Header */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
                      <div className="flex items-center gap-2.5">
                        <span className="text-2xl">{icon}</span>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                            <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                              isLegacy
                                ? 'bg-purple-100 text-purple-800'
                                : isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                            }`}>
                              {isLegacy ? 'Legacy' : isBat ? 'Batting' : 'Pitching'}
                            </span>
                          </div>
                          <div className="text-[11px] text-gray-400 mt-0.5 flex flex-wrap items-center gap-2">
                            <span>{cat.higherIsBetter ? '▲ Higher is better' : '▼ Lower is better'}</span>
                            <span>•</span>
                            <span>{totalTracked} team-seasons tracked</span>
                            <span>•</span>
                            <span className="font-mono text-gray-500">
                              {cat.activeYears[0] === cat.activeYears[1] ? `Season: ${cat.activeYears[0]}` : `Active: ${cat.activeYears[0]}–${cat.activeYears[1]}`}
                            </span>
                            {cat.legacyNote && (
                              <>
                                <span>•</span>
                                <span className="text-amber-700 font-semibold bg-amber-50 px-1.5 py-0.5 rounded border border-amber-200">
                                  ℹ️ {cat.legacyNote}
                                </span>
                              </>
                            )}
                          </div>
                        </div>
                      </div>

                      {isLegacy ? (
                        <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-purple-100 text-purple-800 border border-purple-300 self-start sm:self-auto">
                          <span>🏛️</span>
                          <span>Discontinued</span>
                        </span>
                      ) : (
                        (adjustRoster || adjustMlb) && !RATE_STATS.has(cat.key) && (
                          <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-amber-500/10 text-amber-800 border border-amber-300/60 self-start sm:self-auto">
                            <span>⚡</span>
                            <span>Era Adjusted</span>
                          </span>
                        )
                      )}
                    </div>

                    {/* Highs & Lows Columns */}
                    <div className={`grid grid-cols-1 ${twoCols ? 'lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100' : ''} gap-6`}>
                      {/* HIGHS / BEST COLUMN */}
                      {showHighs && (
                        <div className="space-y-2">
                          <div className="flex items-center justify-between px-1">
                            <span className="text-xs font-black text-emerald-800 flex items-center gap-1.5">
                              <span>🥇</span>
                              <span>All-Time Highs (Best)</span>
                            </span>
                            <span className="text-[10px] font-mono font-bold text-emerald-600">Top 5</span>
                          </div>

                          <div className="divide-y divide-gray-50 text-xs">
                            {highs.map((rec, i) => {
                              const isGold = i === 0;
                              const isSilver = i === 1;
                              const isBronze = i === 2;
                              let dispVal = '';
                              let subRaw = null;
                              if (!isLegacy && rec.isAdjusted && !RATE_STATS.has(cat.key)) {
                                dispVal = cat.key === 'QS' || cat.key === 'SVHLD'
                                  ? rec.adjustedVal.toFixed(1)
                                  : Math.round(rec.adjustedVal).toLocaleString();
                                subRaw = `raw: ${formatRawStat(cat.key, rec.rawVal)}`;
                              } else {
                                dispVal = formatRawStat(cat.key, rec.rawVal);
                              }

                              return (
                                <div
                                  key={`high-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                                  className="py-2.5 px-2 flex items-center justify-between hover:bg-emerald-50/50 rounded-xl transition gap-3"
                                >
                                  <div className="flex items-center gap-2.5 min-w-0">
                                    <span className={`w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 ${
                                      isGold ? 'bg-amber-400 text-amber-950 ring-1 ring-amber-300' :
                                      isSilver ? 'bg-slate-300 text-slate-900' :
                                      isBronze ? 'bg-amber-700 text-amber-100' :
                                      'bg-gray-100 text-gray-600'
                                    }`}>
                                      {isGold ? '🥇' : isSilver ? '🥈' : isBronze ? '🥉' : i + 1}
                                    </span>
                                    <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                                      <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                                        <span>{rec.owner}</span>
                                        <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                                        {rec.place === 1 && <span className="text-amber-500 text-[11px]">👑</span>}
                                      </div>
                                      <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                                        • {rec.teamName}
                                      </div>
                                    </div>
                                  </div>

                                  <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                    <div className="text-right font-mono">
                                      <div className="font-black text-emerald-700 text-xs sm:text-sm">
                                        {dispVal}
                                      </div>
                                      {subRaw && (
                                        <div className="text-[9px] text-gray-400">
                                          {subRaw}
                                        </div>
                                      )}
                                    </div>
                                    {rec.pctVsAvg !== null && (
                                      <span
                                        className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-emerald-50 text-emerald-700 border border-emerald-200/80 shrink-0 font-mono"
                                        title={`Season average in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)} (${formatDiffStat(cat.key, rec.diffVsAvg)} vs avg)`}
                                      >
                                        <span>▲</span>
                                        <span>{formatPctDiff(rec.pctVsAvg)}</span>
                                      </span>
                                    )}
                                    <div className="text-[11px] font-mono text-gray-400 font-semibold w-11 text-right shrink-0">
                                      {rec.rotoPoints} pts
                                    </div>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {/* LOWS / WORST COLUMN */}
                      {showLows && (
                        <div className={`space-y-2 ${twoCols ? 'pt-4 lg:pt-0 lg:pl-6' : ''}`}>
                          <div className="flex items-center justify-between px-1">
                            <span className="text-xs font-black text-rose-800 flex items-center gap-1.5">
                              <span>⚠️</span>
                              <span>All-Time Lows (Worst)</span>
                            </span>
                            <span className="text-[10px] font-mono font-bold text-rose-600">Bottom 5</span>
                          </div>

                          <div className="divide-y divide-gray-50 text-xs">
                            {lows.map((rec, i) => {
                              let dispVal = '';
                              let subRaw = null;
                              if (!isLegacy && rec.isAdjusted && !RATE_STATS.has(cat.key)) {
                                dispVal = cat.key === 'QS' || cat.key === 'SVHLD'
                                  ? rec.adjustedVal.toFixed(1)
                                  : Math.round(rec.adjustedVal).toLocaleString();
                                subRaw = `raw: ${formatRawStat(cat.key, rec.rawVal)}`;
                              } else {
                                dispVal = formatRawStat(cat.key, rec.rawVal);
                              }

                              return (
                                <div
                                  key={`low-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                                  className="py-2.5 px-2 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition gap-3"
                                >
                                  <div className="flex items-center gap-2.5 min-w-0">
                                    <span className="w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 bg-rose-100 text-rose-900 font-bold">
                                      {i + 1}
                                    </span>
                                    <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                                      <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                                        <span>{rec.owner}</span>
                                        <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                                      </div>
                                      <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                                        • {rec.teamName}
                                      </div>
                                    </div>
                                  </div>

                                  <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                    <div className="text-right font-mono">
                                      <div className="font-black text-rose-700 text-xs sm:text-sm">
                                        {dispVal}
                                      </div>
                                      {subRaw && (
                                        <div className="text-[9px] text-gray-400">
                                          {subRaw}
                                        </div>
                                      )}
                                    </div>
                                    {rec.pctVsAvg !== null && (
                                      <span
                                        className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-rose-50 text-rose-700 border border-rose-200/80 shrink-0 font-mono"
                                        title={`Season average in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)} (${formatDiffStat(cat.key, rec.diffVsAvg)} vs avg)`}
                                      >
                                        <span>▼</span>
                                        <span>{formatPctDiff(rec.pctVsAvg)}</span>
                                      </span>
                                    )}
                                    <div className="text-[11px] font-mono text-gray-400 font-semibold w-11 text-right shrink-0">
                                      {rec.rotoPoints} pts
                                    </div>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                );
              };

              return (
                <>
                  <div className="grid grid-cols-1 gap-6">
                    {displayedStatCategories.map(cat => renderCategoryCard(cat, LEGACY_CATEGORIES.some(l => l.key === cat.key)))}
                  </div>

                  {/* Discontinued / Legacy categories spotlight when viewing All Stats */}
                  {selectedStatCategory === 'ALL' && (
                    <div className="mt-8 pt-6 border-t border-gray-200 space-y-4">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <div className="flex items-center gap-2 text-sm font-black text-purple-950 uppercase tracking-wider">
                            <span>🏛️</span>
                            <span>Discontinued & Legacy Category Records</span>
                            <span className="text-xs text-purple-700 font-bold px-2 py-0.5 rounded-full bg-purple-100 border border-purple-200">
                              Historical Formats
                            </span>
                          </div>
                          <p className="text-xs text-gray-500 mt-0.5">
                            All-time single-season benchmarks for categories contested in earlier league seasons prior to standardizing on OBP, QS, and SV+H.
                          </p>
                        </div>
                      </div>

                      <div className="grid grid-cols-1 gap-6">
                        {LEGACY_CATEGORIES.map(cat => renderCategoryCard(cat, true))}
                      </div>
                    </div>
                  )}
                </>
              );
            })()}

            {/* SUB-VIEW 2: RELATIVE DOMINANCE (% VS SEASON LEAGUE AVERAGE) */}
            {highlightViewMode === 'relative' && (() => {
              const renderRelativeDominanceCard = (cat, isLegacy = false) => {
                const record = highlightsData.relativeDominanceRecords?.[cat.key];
                if (!record) return null;
                const { highs, lows, totalTracked } = record;
                const icon = CAT_ICONS[cat.key] || '⚾';
                const isBat = cat.type === 'bat';
                const showHighs = highlightFilter === 'all' || highlightFilter === 'highlights';
                const showLows = highlightFilter === 'all' || highlightFilter === 'lowlights';
                const twoCols = showHighs && showLows;

                return (
                  <div
                    key={`rel-rec-${cat.key}`}
                    className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4 hover:shadow-md transition"
                  >
                    {/* Card Header */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
                      <div className="flex items-center gap-2.5">
                        <span className="text-2xl">{icon}</span>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                            <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                              isLegacy
                                ? 'bg-purple-100 text-purple-800'
                                : isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                            }`}>
                              {isLegacy ? 'Legacy' : isBat ? 'Batting' : 'Pitching'}
                            </span>
                          </div>
                          <div className="text-[11px] text-gray-400 mt-0.5 flex flex-wrap items-center gap-2">
                            <span className="text-indigo-600 font-bold">Relative Dominance</span>
                            <span>•</span>
                            <span>{totalTracked} team-seasons</span>
                            <span>•</span>
                            <span>{cat.higherIsBetter ? '▲ Higher % is better' : '▼ Lower stat = higher % lead'}</span>
                          </div>
                        </div>
                      </div>

                      <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-indigo-50 text-indigo-800 border border-indigo-200 self-start sm:self-auto">
                        <span>📈</span>
                        <span>% vs Season Mean</span>
                      </span>
                    </div>

                    {/* Columns: Most Dominant vs Largest Deficits */}
                    <div className={`grid grid-cols-1 ${twoCols ? 'lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100' : ''} gap-4`}>
                      {/* HIGHEST % ABOVE AVG */}
                      {showHighs && (
                        <div className="space-y-2">
                          <div className="flex items-center justify-between px-1">
                            <span className="text-xs font-black text-emerald-800 flex items-center gap-1.5">
                              <span>🥇</span>
                              <span>Most Dominant (% Above Avg)</span>
                            </span>
                            <span className="text-[10px] font-mono font-bold text-emerald-600">Top 5</span>
                          </div>

                          <div className="divide-y divide-gray-50 text-xs">
                            {highs.map((rec, i) => {
                              const isGold = i === 0;
                              const isSilver = i === 1;
                              const isBronze = i === 2;

                              return (
                                <div
                                  key={`rel-high-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                                  className="py-2.5 px-2 flex items-center justify-between hover:bg-emerald-50/50 rounded-xl transition gap-3"
                                >
                                  <div className="flex items-center gap-2.5 min-w-0">
                                    <span className={`w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 ${
                                      isGold ? 'bg-amber-400 text-amber-950 ring-1 ring-amber-300' :
                                      isSilver ? 'bg-slate-300 text-slate-900' :
                                      isBronze ? 'bg-amber-700 text-amber-100' :
                                      'bg-gray-100 text-gray-600'
                                    }`}>
                                      {isGold ? '🥇' : isSilver ? '🥈' : isBronze ? '🥉' : i + 1}
                                    </span>
                                    <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                                      <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                                        <span>{rec.owner}</span>
                                        <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                                        {rec.place === 1 && <span className="text-amber-500 text-xs">👑</span>}
                                      </div>
                                      <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                                        • {rec.teamName}
                                      </div>
                                    </div>
                                  </div>

                                  <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                    <div className="text-right">
                                      <div className="text-[11px] text-gray-700 font-mono font-bold">
                                        {formatRawStat(cat.key, rec.rawVal)}
                                        <span className="text-gray-400 font-normal font-sans ml-1">(avg: {formatMeanStat(cat.key, rec.seasonMean)})</span>
                                      </div>
                                      <div className="text-[10px] font-mono text-gray-400">
                                        {rec.rotoPoints} pts
                                      </div>
                                    </div>
                                    <span
                                      className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-emerald-50 text-emerald-700 border border-emerald-200/80 shrink-0 font-mono"
                                      title={`Season average in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)}`}
                                    >
                                      <span>▲</span>
                                      <span>+{rec.pctVsAvg.toFixed(1)}%</span>
                                    </span>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {/* WORST % BELOW AVG */}
                      {showLows && (
                        <div className={`space-y-2 ${twoCols ? 'pt-4 lg:pt-0 lg:pl-6' : ''}`}>
                          <div className="flex items-center justify-between px-1">
                            <span className="text-xs font-black text-rose-800 flex items-center gap-1.5">
                              <span>⚠️</span>
                              <span>Largest Deficits (% Below Avg)</span>
                            </span>
                            <span className="text-[10px] font-mono font-bold text-rose-600">Bottom 5</span>
                          </div>

                          <div className="divide-y divide-gray-50 text-xs">
                            {lows.map((rec, i) => (
                              <div
                                key={`rel-low-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                                className="py-2.5 px-2 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition gap-3"
                              >
                                <div className="flex items-center gap-2.5 min-w-0">
                                  <span className="w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 bg-rose-100 text-rose-900 font-bold">
                                    {i + 1}
                                  </span>
                                  <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                                    <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                                      <span>{rec.owner}</span>
                                      <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                                    </div>
                                    <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                                      • {rec.teamName}
                                    </div>
                                  </div>
                                </div>

                                <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                  <div className="text-right">
                                    <div className="text-[11px] text-gray-700 font-mono font-bold">
                                      {formatRawStat(cat.key, rec.rawVal)}
                                      <span className="text-gray-400 font-normal font-sans ml-1">(avg: {formatMeanStat(cat.key, rec.seasonMean)})</span>
                                    </div>
                                    <div className="text-[10px] font-mono text-gray-400">
                                      {rec.rotoPoints} pts
                                    </div>
                                  </div>
                                  <span
                                    className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-rose-50 text-rose-700 border border-rose-200/80 shrink-0 font-mono"
                                    title={`Season average in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)}`}
                                  >
                                    <span>▼</span>
                                    <span>{rec.pctVsAvg.toFixed(1)}%</span>
                                  </span>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                );
              };

              return (
                <>
                  <div className="grid grid-cols-1 gap-6">
                    {displayedStatCategories.map(cat => renderRelativeDominanceCard(cat, LEGACY_CATEGORIES.some(l => l.key === cat.key)))}
                  </div>

                  {/* Discontinued / Legacy categories spotlight when viewing All Stats */}
                  {selectedStatCategory === 'ALL' && (
                    <div className="mt-8 pt-6 border-t border-gray-200 space-y-4">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <div className="flex items-center gap-2 text-sm font-black text-purple-950 uppercase tracking-wider">
                            <span>🏛️</span>
                            <span>Discontinued & Legacy Relative Dominance</span>
                          </div>
                          <p className="text-xs text-gray-500 mt-0.5">
                            Relative dominance metrics for discontinued formats (Batting Average in 2012, Wins in 2012–13, Saves in 2012–18).
                          </p>
                        </div>
                      </div>

                      <div className="grid grid-cols-1 gap-6">
                        {LEGACY_CATEGORIES.map(cat => renderRelativeDominanceCard(cat, true))}
                      </div>
                    </div>
                  )}
                </>
              );
            })()}

            {/* SUB-VIEW 3: CATEGORY MARGINS & GAPS (1ST VS 2ND AND LAST VS 2ND-TO-LAST) */}
            {highlightViewMode === 'margins' && (() => {
              const showLeads = marginsGapType === 'all' || marginsGapType === '1st_2nd';
              const showDeficits = marginsGapType === 'all' || marginsGapType === 'last_2nd_last';

              const renderCategoryMarginsCard = (cat, isLegacy = false) => {
                const record = highlightsData.categoryGapsRecords?.[cat.key];
                if (!record) return null;
                const { gaps12, gapsLast, totalSeasons } = record;
                const icon = CAT_ICONS[cat.key] || '⚾';
                const isBat = cat.type === 'bat';
                const twoCols = showLeads && showDeficits;

                return (
                  <div
                    key={`margin-rec-${cat.key}`}
                    className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4 hover:shadow-md transition"
                  >
                    {/* Header */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
                      <div className="flex items-center gap-2.5">
                        <span className="text-2xl">{icon}</span>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                            <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                              isLegacy
                                ? 'bg-purple-100 text-purple-800'
                                : isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                            }`}>
                              {isLegacy ? 'Legacy' : isBat ? 'Batting' : 'Pitching'}
                            </span>
                          </div>
                          <div className="text-[11px] text-gray-400 mt-0.5 flex flex-wrap items-center gap-2">
                            <span>Competitive chasms</span>
                            <span>•</span>
                            <span>{totalSeasons} seasons</span>
                            <span>•</span>
                            <span className="font-mono text-gray-500">
                              {cat.activeYears[0] === cat.activeYears[1] ? `Season: ${cat.activeYears[0]}` : `Active: ${cat.activeYears[0]}–${cat.activeYears[1]}`}
                            </span>
                          </div>
                        </div>
                      </div>

                      <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-amber-50 text-amber-800 border border-amber-200 self-start sm:self-auto">
                        <span>⚡</span>
                        <span>Margins & Gaps</span>
                      </span>
                    </div>

                    {/* Columns: 1st vs 2nd Leads and Last vs 2nd-Last Deficits */}
                    <div className={`grid grid-cols-1 ${twoCols ? 'lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100' : ''} gap-4`}>
                      {/* 1st vs 2nd Leads */}
                      {showLeads && (
                        <div className="space-y-2">
                          <div className="flex items-center justify-between px-1">
                            <span className="text-xs font-black text-amber-800 flex items-center gap-1.5">
                              <span>🥇</span>
                              <span>Biggest 1st vs 2nd Leads (Runaway Titles)</span>
                            </span>
                            <span className="text-[10px] font-mono font-bold text-amber-600">Top 5</span>
                          </div>

                          <div className="divide-y divide-gray-50 text-xs">
                            {gaps12.map((m, i) => (
                              <div
                                key={`gap12-${cat.key}-${m.year}-${i}`}
                                className="py-2.5 px-2 flex items-center justify-between hover:bg-amber-50/50 rounded-xl transition gap-3"
                              >
                                <div className="min-w-0 flex items-center gap-2 flex-wrap sm:flex-nowrap">
                                  <span className="font-mono font-black text-xs text-gray-900 bg-gray-100 px-1.5 py-0.5 rounded shrink-0">
                                    {m.year}
                                  </span>
                                  <div className="min-w-0">
                                    <div className="font-black text-emerald-800 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                                      <span>1st: {m.first.owner}</span>
                                      <span className="font-mono font-bold text-emerald-700 text-xs">({formatRawStat(cat.key, m.first.rawVal)})</span>
                                    </div>
                                    <div className="text-[11px] text-gray-500 whitespace-nowrap flex items-center gap-1 mt-0.5">
                                      <span>vs 2nd:</span>
                                      <span className="font-bold text-gray-700">{m.second.owner}</span>
                                      <span className="font-mono text-gray-500">({formatRawStat(cat.key, m.second.rawVal)})</span>
                                    </div>
                                  </div>
                                </div>

                                <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                  <div className="text-right font-mono">
                                    <div className="font-black text-amber-800 text-xs sm:text-sm">
                                      +{formatDiffStat(cat.key, m.gap12)}
                                    </div>
                                  </div>
                                  <span className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-amber-50 text-amber-800 border border-amber-300 shrink-0 font-mono">
                                    +{m.pctGap12.toFixed(1)}% lead
                                  </span>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Last vs 2nd-Last Deficits */}
                      {showDeficits && (
                        <div className={`space-y-2 ${twoCols ? 'pt-4 lg:pt-0 lg:pl-6' : ''}`}>
                          <div className="flex items-center justify-between px-1">
                            <span className="text-xs font-black text-rose-800 flex items-center gap-1.5">
                              <span>⚠️</span>
                              <span>Biggest Last vs 2nd-Last Deficits (Abyss)</span>
                            </span>
                            <span className="text-[10px] font-mono font-bold text-rose-600">Bottom 5</span>
                          </div>

                          <div className="divide-y divide-gray-50 text-xs">
                            {gapsLast.map((m, i) => (
                              <div
                                key={`gaplast-${cat.key}-${m.year}-${i}`}
                                className="py-2.5 px-2 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition gap-3"
                              >
                                <div className="min-w-0 flex items-center gap-2 flex-wrap sm:flex-nowrap">
                                  <span className="font-mono font-black text-xs text-gray-900 bg-gray-100 px-1.5 py-0.5 rounded shrink-0">
                                    {m.year}
                                  </span>
                                  <div className="min-w-0">
                                    <div className="font-black text-rose-800 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                                      <span>Last: {m.last.owner}</span>
                                      <span className="font-mono font-bold text-rose-700 text-xs">({formatRawStat(cat.key, m.last.rawVal)})</span>
                                    </div>
                                    <div className="text-[11px] text-gray-500 whitespace-nowrap flex items-center gap-1 mt-0.5">
                                      <span>behind 2nd-last:</span>
                                      <span className="font-bold text-gray-700">{m.secondLast.owner}</span>
                                      <span className="font-mono text-gray-500">({formatRawStat(cat.key, m.secondLast.rawVal)})</span>
                                    </div>
                                  </div>
                                </div>

                                <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                  <div className="text-right font-mono">
                                    <div className="font-black text-rose-800 text-xs sm:text-sm">
                                      -{formatDiffStat(cat.key, m.gapLast)}
                                    </div>
                                  </div>
                                  <span className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-rose-50 text-rose-800 border border-rose-300 shrink-0 font-mono">
                                    -{m.pctGapLast.toFixed(1)}% deficit
                                  </span>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                );
              };

              return (
                <div className="space-y-8">
                  {/* OVERALL ALL-TIME GAP LEADERBOARDS (SHOWN WHEN ALL STATS IS SELECTED) */}
                  {selectedStatCategory === 'ALL' && (
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                      {/* TOP 10 ALL-TIME RUNAWAY CATEGORY TITLES (1ST VS 2ND LEADS) */}
                      {showLeads && (
                        <div className="bg-gradient-to-br from-amber-500/10 via-amber-50/40 to-white border border-amber-200 rounded-3xl p-6 shadow-sm space-y-4">
                          <div className="flex items-center justify-between border-b border-amber-200/60 pb-3">
                            <div className="flex items-center gap-2">
                              <span className="text-2xl">🥇</span>
                              <div>
                                <h3 className="text-base font-black text-amber-950">Top 10 Runaway Category Titles</h3>
                                <p className="text-xs text-amber-800/80">All-time largest single-season leads over 2nd place (% lead)</p>
                              </div>
                            </div>
                            <span className="text-xs font-mono font-black text-amber-700 bg-amber-100 px-2 py-0.5 rounded-full border border-amber-300">
                              Blowouts
                            </span>
                          </div>

                          <div className="divide-y divide-amber-100/70 text-xs">
                            {highlightsData.topOverall12Gaps?.map((m, idx) => {
                              const isGold = idx === 0;
                              const isSilver = idx === 1;
                              const isBronze = idx === 2;

                              return (
                                <div
                                  key={`overall-gap12-${m.year}-${m.catKey}-${idx}`}
                                  className="py-2.5 flex items-center justify-between hover:bg-amber-100/40 rounded-xl px-2 transition gap-3"
                                >
                                  <div className="flex items-center gap-2.5 min-w-0 pr-2">
                                    <span className={`w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 ${
                                      isGold ? 'bg-amber-400 text-amber-950 ring-1 ring-amber-300' :
                                      isSilver ? 'bg-slate-300 text-slate-900' :
                                      isBronze ? 'bg-amber-700 text-amber-100' :
                                      'bg-amber-100 text-amber-900'
                                    }`}>
                                      {isGold ? '🥇' : isSilver ? '🥈' : isBronze ? '🥉' : idx + 1}
                                    </span>
                                    <div className="min-w-0">
                                      <div className="flex items-center gap-1.5 flex-wrap">
                                        <span className="font-black text-gray-900 text-[11px] flex items-center gap-1 whitespace-nowrap">
                                          <span>{CAT_ICONS[m.catKey]}</span>
                                          <span>{m.cat.name}</span>
                                          <span className="text-gray-500 font-mono font-normal">('{String(m.year).slice(2)})</span>
                                        </span>
                                        <span className="text-gray-300">•</span>
                                        <span className="font-bold text-emerald-800 text-[11px] whitespace-nowrap">
                                          1st: {m.first.owner} <span className="font-mono text-emerald-700">({formatRawStat(m.catKey, m.first.rawVal)})</span>
                                        </span>
                                      </div>
                                      <div className="text-[10px] text-gray-500 flex items-center gap-1 mt-0.5 whitespace-nowrap">
                                        <span>vs 2nd:</span>
                                        <span className="font-semibold text-gray-700">{m.second.owner}</span>
                                        <span className="font-mono">({formatRawStat(m.catKey, m.second.rawVal)})</span>
                                      </div>
                                    </div>
                                  </div>

                                  <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                    <div className="text-right font-mono">
                                      <div className="font-black text-amber-800 text-xs sm:text-sm">
                                        +{formatDiffStat(m.catKey, m.gap12)}
                                      </div>
                                    </div>
                                    <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-100 text-amber-900 border border-amber-300 font-mono shrink-0">
                                      +{m.pctGap12.toFixed(1)}% lead
                                    </span>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {/* TOP 10 ALL-TIME THE BASEMENT ABYSS (LAST VS 2ND-TO-LAST DEFICITS) */}
                      {showDeficits && (
                        <div className="bg-gradient-to-br from-rose-500/10 via-rose-50/40 to-white border border-rose-200 rounded-3xl p-6 shadow-sm space-y-4">
                          <div className="flex items-center justify-between border-b border-rose-200/60 pb-3">
                            <div className="flex items-center gap-2">
                              <span className="text-2xl">⚠️</span>
                              <div>
                                <h3 className="text-base font-black text-rose-950">Top 10 The Basement Abyss</h3>
                                <p className="text-xs text-rose-800/80">All-time largest deficits between last & 2nd-to-last (% deficit)</p>
                              </div>
                            </div>
                            <span className="text-xs font-mono font-black text-rose-700 bg-rose-100 px-2 py-0.5 rounded-full border border-rose-300">
                              Punts & Collapses
                            </span>
                          </div>

                          <div className="divide-y divide-rose-100/70 text-xs">
                            {highlightsData.topOverallLastGaps?.map((m, idx) => (
                              <div
                                key={`overall-gaplast-${m.year}-${m.catKey}-${idx}`}
                                className="py-2.5 flex items-center justify-between hover:bg-rose-100/40 rounded-xl px-2 transition gap-3"
                              >
                                <div className="flex items-center gap-2.5 min-w-0 pr-2">
                                  <span className="w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 bg-rose-100 text-rose-900 font-bold">
                                    {idx + 1}
                                  </span>
                                  <div className="min-w-0">
                                    <div className="flex items-center gap-1.5 flex-wrap">
                                      <span className="font-black text-gray-900 text-[11px] flex items-center gap-1 whitespace-nowrap">
                                        <span>{CAT_ICONS[m.catKey]}</span>
                                        <span>{m.cat.name}</span>
                                        <span className="text-gray-500 font-mono font-normal">('{String(m.year).slice(2)})</span>
                                      </span>
                                      <span className="text-gray-300">•</span>
                                      <span className="font-bold text-rose-800 text-[11px] whitespace-nowrap">
                                        Last: {m.last.owner} <span className="font-mono text-rose-700">({formatRawStat(m.catKey, m.last.rawVal)})</span>
                                      </span>
                                    </div>
                                    <div className="text-[10px] text-gray-500 flex items-center gap-1 mt-0.5 whitespace-nowrap">
                                      <span>behind 2nd-last:</span>
                                      <span className="font-semibold text-gray-700">{m.secondLast.owner}</span>
                                      <span className="font-mono">({formatRawStat(m.catKey, m.secondLast.rawVal)})</span>
                                    </div>
                                  </div>
                                </div>

                                <div className="flex items-center gap-2.5 shrink-0 pl-2">
                                  <div className="text-right font-mono">
                                    <div className="font-black text-rose-800 text-xs sm:text-sm">
                                      -{formatDiffStat(m.catKey, m.gapLast)}
                                    </div>
                                  </div>
                                  <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-rose-100 text-rose-900 border border-rose-300 font-mono shrink-0">
                                    -{m.pctGapLast.toFixed(1)}% deficit
                                  </span>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {/* PER-CATEGORY MARGIN BREAKDOWN CARDS */}
                  <div className="space-y-4">
                    {selectedStatCategory === 'ALL' && (
                      <div className="text-xs font-black text-gray-700 uppercase tracking-wider pt-2">
                        Per-Category Margin Breakdowns
                      </div>
                    )}

                    <div className="grid grid-cols-1 gap-6">
                      {displayedStatCategories.map(cat => renderCategoryMarginsCard(cat, LEGACY_CATEGORIES.some(l => l.key === cat.key)))}
                    </div>

                    {/* Discontinued / Legacy categories spotlight when viewing All Stats */}
                    {selectedStatCategory === 'ALL' && (
                      <div className="mt-8 pt-6 border-t border-gray-200 space-y-4">
                        <div className="flex items-center justify-between gap-3">
                          <div>
                            <div className="flex items-center gap-2 text-sm font-black text-purple-950 uppercase tracking-wider">
                              <span>🏛️</span>
                              <span>Discontinued & Legacy Margins</span>
                            </div>
                            <p className="text-xs text-gray-500 mt-0.5">
                              Historical 1st vs 2nd leads and last place deficits for discontinued categories (Batting Average in 2012, Wins in 2012–13, Saves in 2012–18).
                            </p>
                          </div>
                        </div>

                        <div className="grid grid-cols-1 gap-6">
                          {LEGACY_CATEGORIES.map(cat => renderCategoryMarginsCard(cat, true))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              );
            })()}
          </div>

          {/* HIGHLIGHTS SECTION */}
          {(highlightFilter === 'all' || highlightFilter === 'highlights') && (
            <div className="space-y-6">
              <div className="flex items-center gap-2 text-sm font-black text-amber-900 uppercase tracking-wider">
                <span>🏆</span>
                <span>All-Time Highlights (Greatest Historical Finishes)</span>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* 1. All-Time Highest Scoring Finishes */}
                <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
                  <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                    <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
                      <span>🥇</span>
                      <span>Highest Roto Point Finishes Ever</span>
                    </h3>
                    <span className="text-[11px] text-gray-400 font-mono">Top 10 Campaigns</span>
                  </div>
                  <div className="divide-y divide-gray-100 text-xs">
                    {highlightsData.topScoring.map((rec, i) => (
                      <div key={`high-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-gray-50/80 px-2 rounded-xl transition">
                        <div className="flex items-center gap-3">
                          <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                          <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                          <div>
                            <div className="font-black text-gray-900 flex items-center gap-1.5">
                              <span>{rec.owner}</span>
                              <span className="text-[11px] text-gray-400 font-normal">({rec.year})</span>
                              {rec.place === 1 && <span className="text-amber-500 text-[10px]">👑</span>}
                            </div>
                            <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                          </div>
                        </div>
                        <div className="text-right font-mono">
                          <div className="font-black text-blue-700 text-sm">
                            {rec.isAdjusted ? rec.adjustedPoints.toFixed(1) : rec.points.toFixed(1)} pts
                          </div>
                          <div className="text-[10px] text-gray-400">
                            {rec.isAdjusted ? `raw: ${rec.points} • ` : ''}{rec.dominanceIndex.toFixed(1)}% dominance
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* 2. Largest Championship Blowouts */}
                <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
                  <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                    <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
                      <span>💥</span>
                      <span>Largest Championship Victory Margins</span>
                    </h3>
                    <span className="text-[11px] text-gray-400 font-mono">Margin of Victory</span>
                  </div>
                  <div className="divide-y divide-gray-100 text-xs">
                    {highlightsData.blowouts.slice(0, 8).map((b, i) => (
                      <div key={`blow-${b.year}`} className="py-2.5 flex items-center justify-between hover:bg-gray-50/80 px-2 rounded-xl transition">
                        <div className="flex items-center gap-3">
                          <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                          <div>
                            <div className="font-black text-gray-900">
                              {b.year}: <strong className="text-amber-600 font-black">{b.champ}</strong> over {b.runnerUp}
                            </div>
                            <div className="text-[10px] text-gray-500 truncate max-w-[200px]">{b.champTeam}</div>
                          </div>
                        </div>
                        <div className="text-right font-mono">
                          <div className="font-black text-emerald-600 text-sm">+{b.margin.toFixed(1)} pts</div>
                          <div className="text-[10px] text-gray-400">{b.champPoints} vs {b.runnerUpPoints}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* 3. Offensive Masterclasses (Hitting Points) */}
                <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
                  <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                    <h3 className="text-sm font-black text-amber-900 flex items-center gap-2">
                      <span>🏏</span>
                      <span>Offensive Masterclasses (Top Hitting Pts)</span>
                    </h3>
                    <span className="text-[11px] text-gray-400 font-mono">50 Pt Max</span>
                  </div>
                  <div className="divide-y divide-gray-100 text-xs">
                    {highlightsData.topHitting.map((rec, i) => (
                      <div key={`hit-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-amber-50/40 px-2 rounded-xl transition">
                        <div className="flex items-center gap-3">
                          <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                          <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                          <div>
                            <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                            <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                          </div>
                        </div>
                        <div className="text-right font-mono font-black text-amber-700 text-sm">
                          {rec.hittingPoints} pts
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* 4. Pitching Masterclasses (Pitching Points) */}
                <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
                  <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                    <h3 className="text-sm font-black text-indigo-900 flex items-center gap-2">
                      <span>🎯</span>
                      <span>Pitching Masterclasses (Top Pitching Pts)</span>
                    </h3>
                    <span className="text-[11px] text-gray-400 font-mono">50 Pt Max</span>
                  </div>
                  <div className="divide-y divide-gray-100 text-xs">
                    {highlightsData.topPitching.map((rec, i) => (
                      <div key={`pitch-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-indigo-50/40 px-2 rounded-xl transition">
                        <div className="flex items-center gap-3">
                          <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                          <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                          <div>
                            <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                            <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                          </div>
                        </div>
                        <div className="text-right font-mono font-black text-indigo-700 text-sm">
                          {rec.pitchingPoints} pts
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* LOWLIGHTS SECTION */}
          {(highlightFilter === 'all' || highlightFilter === 'lowlights') && (
            <div className="space-y-6 pt-4">
              <div className="flex items-center gap-2 text-sm font-black text-rose-900 uppercase tracking-wider">
                <span>⚠️</span>
                <span>All-Time Lowlights (Toughest Historical Campaigns)</span>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* 1. Lowest Scoring Finishes */}
                <div className="bg-white border border-rose-200 rounded-3xl p-5 shadow-sm space-y-3">
                  <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                    <h3 className="text-sm font-black text-rose-900 flex items-center gap-2">
                      <span>📉</span>
                      <span>Lowest Roto Point Finishes in History</span>
                    </h3>
                    <span 
                      onClick={() => setExclude2020Lowlights(!exclude2020Lowlights)}
                      className="text-[11px] text-gray-400 font-mono cursor-pointer hover:text-gray-600 transition"
                    >
                      {exclude2020Lowlights ? 'Excl. 2020 (COVID)' : 'All Seasons'}
                    </span>
                  </div>
                  <div className="divide-y divide-gray-100 text-xs">
                    {highlightsData.lowestScoring.map((rec, i) => (
                      <div key={`low-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-rose-50/40 px-2 rounded-xl transition">
                        <div className="flex items-center gap-3">
                          <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                          <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                          <div>
                            <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                            <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                          </div>
                        </div>
                        <div className="text-right font-mono font-black text-rose-600 text-sm">
                          {rec.points.toFixed(1)} pts
                          <span className="block text-[10px] text-gray-400 font-normal">#{rec.place} finish</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* 2. Championship Hangovers */}
                <div className="bg-white border border-rose-200 rounded-3xl p-5 shadow-sm space-y-3">
                  <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                    <h3 className="text-sm font-black text-rose-900 flex items-center gap-2">
                      <span>🛌</span>
                      <span>Championship Hangovers (Title Defenses)</span>
                    </h3>
                    <span className="text-[11px] text-gray-400 font-mono">Rank Drop Following Championship</span>
                  </div>
                  <div className="divide-y divide-gray-100 text-xs">
                    {highlightsData.hangovers.map((h, i) => (
                      <div key={`hang-${h.champYear}-${h.owner}`} className="py-2.5 flex items-center justify-between hover:bg-rose-50/40 px-2 rounded-xl transition">
                        <div className="flex items-center gap-3">
                          <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                          <div>
                            <div className="font-black text-gray-900">
                              {h.owner}: {h.champYear} Champ ➔ {h.nextYear} (#{h.nextPlace})
                            </div>
                            <div className="text-[10px] text-gray-500 truncate max-w-[200px]">{h.nextTeam}</div>
                          </div>
                        </div>
                        <div className="text-right font-mono font-black text-rose-600 text-sm">
                          ▼ {h.rankDrop} spots
                          <span className="block text-[10px] text-gray-400 font-normal">{h.champPoints} ➔ {h.nextPoints} pts</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* -------------------------------------------------------------------- */}
      {/* SUB-TAB 3: ALL-TIME FRANCHISE HALL & PLACEMENT MATRIX                */}
      {/* -------------------------------------------------------------------- */}
      {activeSubTab === 'records' && (
        <div className="space-y-8">
          {/* 1. All-Time Leaderboard Table */}
          <div className="bg-white border border-gray-200 rounded-3xl shadow-xl overflow-hidden">
            <div className="p-5 border-b border-gray-100 bg-gray-50/70 flex items-center justify-between">
              <div>
                <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
                  <span>🏛️</span>
                  <span>All-Time Franchise Career Leaderboard</span>
                </h2>
                <p className="text-xs text-gray-500 mt-0.5">Cumulative statistics across all 14 seasons (2012–2025)</p>
              </div>
              <span className="text-xs font-mono font-bold text-gray-500">
                Sorted by <span className="text-blue-600 font-black">{hallSortLabel}</span> ({hallSortDirection === 'desc' ? 'High to Low' : 'Low to High'}) • Click headers to sort
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-xs text-left select-none">
                <thead className="bg-gray-100/90 text-gray-700 font-black uppercase text-[10px] tracking-wider border-b border-gray-200">
                  <tr>
                    <th className="py-3 px-3">#</th>
                    <th
                      onClick={() => handleHallSort('owner')}
                      className={`py-3 px-4 cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'owner' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center gap-1">
                        <span>Owner</span>
                        {hallSortField === 'owner' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('titles')}
                      className={`py-3 px-3 text-center cursor-pointer hover:bg-amber-100 transition ${
                        hallSortField === 'titles' ? 'bg-amber-100/90 text-amber-950 font-black' : 'bg-amber-50/80 text-amber-950'
                      }`}
                    >
                      <div className="flex items-center justify-center gap-1">
                        <span>🏆 Titles</span>
                        {hallSortField === 'titles' && (
                          <span className="text-amber-900 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('podiums')}
                      className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'podiums' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-center gap-1">
                        <span>Podiums (1-3)</span>
                        {hallSortField === 'podiums' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('seasons')}
                      className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'seasons' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-center gap-1">
                        <span>Seasons</span>
                        {hallSortField === 'seasons' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('avgPlace')}
                      className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'avgPlace' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-center gap-1">
                        <span>Avg Finish</span>
                        {hallSortField === 'avgPlace' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('bestPlace')}
                      className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'bestPlace' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-center gap-1">
                        <span>Best</span>
                        {hallSortField === 'bestPlace' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('worstPlace')}
                      className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'worstPlace' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-center gap-1">
                        <span>Worst</span>
                        {hallSortField === 'worstPlace' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('totalPoints')}
                      className={`py-3 px-4 text-right cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'totalPoints' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-end gap-1">
                        <span>All-Time Pts</span>
                        {hallSortField === 'totalPoints' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                    <th
                      onClick={() => handleHallSort('ptsPerSeason')}
                      className={`py-3 px-3 text-right cursor-pointer hover:bg-gray-200 transition ${
                        hallSortField === 'ptsPerSeason' ? 'bg-blue-50 text-blue-900' : ''
                      }`}
                    >
                      <div className="flex items-center justify-end gap-1">
                        <span>Pts/Season</span>
                        {hallSortField === 'ptsPerSeason' && (
                          <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                        )}
                      </div>
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {sortedFranchiseRecords.map((f, i) => (
                    <tr
                      key={f.owner}
                      className={`hover:bg-blue-50/40 transition-colors ${
                        f.titles > 0 ? 'font-bold' : ''
                      }`}
                    >
                      <td className="py-3 px-3 font-mono font-black text-gray-400">{i + 1}</td>
                      <td className="py-3 px-4">
                        <div
                          onClick={() => onOwnerClick && onOwnerClick(getOwnerTeamObj(f.owner))}
                          className="flex items-center gap-2.5 cursor-pointer group"
                        >
                          <TeamAvatar team={{ owner: f.owner }} size="sm" className="group-hover:scale-105 transition-transform" />
                          <div>
                            <div className="font-black text-gray-900 group-hover:text-blue-600 transition flex items-center gap-1.5">
                              <span>{f.owner}</span>
                              {f.titles >= 2 && <span className="text-xs">👑</span>}
                            </div>
                            {f.titleYears.length > 0 && (
                              <div className="text-[10px] text-amber-600 font-mono">
                                Champs: {f.titleYears.join(', ')}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>

                      {/* Championships */}
                      <td className="py-3 px-3 text-center font-black text-amber-900 bg-amber-50/40 text-sm">
                        {f.titles > 0 ? `${f.titles} 🏆` : '-'}
                      </td>

                      {/* Podiums */}
                      <td className="py-3 px-3 text-center font-black text-gray-800">
                        {f.podiums}
                      </td>

                      {/* Seasons */}
                      <td className="py-3 px-3 text-center font-mono text-gray-600">
                        {f.seasons}
                      </td>

                      {/* Average Finish */}
                      <td className="py-3 px-3 text-center font-mono font-black text-blue-700">
                        #{f.avgPlace}
                      </td>

                      {/* Best / Worst */}
                      <td className="py-3 px-3 text-center font-mono text-emerald-700">#{f.bestPlace}</td>
                      <td className="py-3 px-3 text-center font-mono text-gray-500">#{f.worstPlace}</td>

                      {/* All-Time Points */}
                      <td className="py-3 px-4 text-right font-mono font-black text-gray-900">
                        {f.totalPoints.toFixed(1)}
                      </td>

                      {/* Pts per season */}
                      <td className="py-3 px-3 text-right font-mono text-gray-600">
                        {f.ptsPerSeason}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* 2. 14-Year Placement Matrix Grid */}
          <div className="bg-white border border-gray-200 rounded-3xl shadow-xl overflow-hidden">
            <div className="p-5 border-b border-gray-100 bg-gray-50/70 flex items-center justify-between">
              <div>
                <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
                  <span>📅</span>
                  <span>14-Year Placement Finish Matrix (2012–2026)</span>
                </h2>
                <p className="text-xs text-gray-500 mt-0.5">Complete historical track record by owner and year</p>
              </div>
              <div className="flex items-center gap-2 text-[10px] font-bold">
                <span className="inline-block w-3 h-3 rounded bg-amber-400"></span> 1st
                <span className="inline-block w-3 h-3 rounded bg-slate-300"></span> 2nd
                <span className="inline-block w-3 h-3 rounded bg-amber-700"></span> 3rd
                <span className="inline-block w-3 h-3 rounded bg-blue-100"></span> 4-5
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-xs text-center border-collapse">
                <thead className="bg-gray-100/90 text-gray-700 font-black uppercase text-[10px] tracking-wider border-b border-gray-200">
                  <tr>
                    <th className="py-3 px-3 text-left">Owner</th>
                    {[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026].map(yr => (
                      <th key={yr} className="py-3 px-2 font-mono">{String(yr).slice(2)}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {franchiseRecords.map(f => (
                    <tr key={`matrix-${f.owner}`} className="hover:bg-gray-50/80 transition-colors">
                      <td className="py-2.5 px-3 text-left font-black text-gray-900 flex items-center gap-2">
                        <TeamAvatar team={{ owner: f.owner }} size="xs" />
                        <span>{f.owner}</span>
                      </td>
                      {[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026].map(yr => {
                        const item = f.finishesByYear[yr];
                        if (!item) {
                          return <td key={yr} className="py-2.5 px-2 text-gray-300 font-mono">-</td>;
                        }
                        const p = item.place;
                        const badgeColor =
                          p === 1 ? 'bg-amber-400 text-amber-950 font-black ring-1 ring-amber-300' :
                          p === 2 ? 'bg-slate-300 text-slate-900 font-bold' :
                          p === 3 ? 'bg-amber-700 text-amber-100 font-bold' :
                          p <= 5 ? 'bg-blue-100 text-blue-900 font-semibold' :
                          'bg-gray-100 text-gray-600';

                        return (
                          <td key={yr} className="py-2 px-1">
                            <span
                              className={`inline-flex items-center justify-center w-6 h-6 rounded-lg text-[11px] font-mono shadow-2xs ${badgeColor}`}
                              title={`${yr}: #${p} place (${item.points} pts) - ${item.teamName}`}
                            >
                              {p}
                            </span>
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* 3. Championship Roll of Honor Timeline */}
          <div className="bg-white border border-gray-200 rounded-3xl p-6 shadow-xl space-y-4">
            <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>👑</span>
              <span>Championship Roll of Honor</span>
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
              {leagueContextData?.historical_finishes_summary?.championship_roll_of_honor?.map(c => (
                <div
                  key={c.year}
                  className="bg-gradient-to-br from-amber-500/10 via-slate-50 to-white border border-amber-300/50 rounded-2xl p-4 shadow-xs hover:shadow-md transition space-y-2"
                >
                  <div className="flex items-center justify-between text-xs font-black">
                    <span className="text-amber-800">{c.year} Champion</span>
                    <span className="text-xs">🏆</span>
                  </div>
                  <div className="text-base font-black text-gray-900">{c.champion}</div>
                  <div className="text-[11px] text-gray-500 italic truncate">{c.champion_team_name}</div>
                  <div className="pt-2 border-t border-gray-100 text-[11px] text-gray-600 flex justify-between">
                    <span>{c.champion_points} pts</span>
                    <span className="text-gray-400">2nd: {c.runner_up} ({c.runner_up_points})</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* -------------------------------------------------------------------- */}
      {/* METHODOLOGY EXPLAINER MODAL                                          */}
      {/* -------------------------------------------------------------------- */}
      {showMethodologyModal && (
        <div className="fixed inset-0 z-50 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4 animate-fade-in">
          <div className="bg-white rounded-3xl max-w-2xl w-full p-6 sm:p-8 shadow-2xl border border-gray-200 space-y-6 relative max-h-[90vh] overflow-y-auto">
            <button
              onClick={() => setShowMethodologyModal(false)}
              className="absolute top-5 right-5 w-8 h-8 rounded-full bg-gray-100 hover:bg-gray-200 text-gray-600 font-bold flex items-center justify-center cursor-pointer transition"
            >
              ✕
            </button>

            <div className="flex items-center gap-3">
              <div className="w-12 h-12 rounded-2xl bg-amber-500/10 border border-amber-400/30 flex items-center justify-center text-2xl">
                📐
              </div>
              <div>
                <h3 className="text-xl font-black text-gray-900">Historical Era & Roster Adjustments</h3>
                <p className="text-xs text-gray-500">Mathematical methodology and normalization principles</p>
              </div>
            </div>

            <div className="space-y-4 text-xs sm:text-sm text-gray-700 leading-relaxed">
              <div className="bg-amber-50/60 border border-amber-200/80 rounded-2xl p-4 space-y-2">
                <h4 className="font-black text-amber-900 flex items-center gap-2">
                  <span>1.</span>
                  <span>Roster Capacity Expansion (22 starters vs 28 starters)</span>
                </h4>
                <p>
                  From <strong>2012 through 2025</strong>, the league used a 22-man active starter lineup (13 active batters and 9 active pitchers). In <strong>2026</strong>, active lineups expanded to 28 starters (16 batters and 12 pitchers).
                </p>
                <p className="font-mono text-xs text-amber-800">
                  Batting Multiplier: 16 / 13 = 1.231× | Pitching Multiplier: 12 / 9 = 1.333×
                </p>
              </div>

              <div className="bg-blue-50/60 border border-blue-200/80 rounded-2xl p-4 space-y-2">
                <h4 className="font-black text-blue-900 flex items-center gap-2">
                  <span>2.</span>
                  <span>League Size & Roto Point Normalization</span>
                </h4>
                <p>
                  League size has varied: 8 teams in 2013 (max 80 pts), 9 teams in 2012 and 2026 (max 90 pts), and 10 teams from 2014 to 2025 (max 100 pts). To compare championships equitably, we calculate a <strong>Dominance Index (% of Max Points Available)</strong> scaled to a 100-point universal standard.
                </p>
              </div>

              <div className="bg-emerald-50/60 border border-emerald-200/80 rounded-2xl p-4 space-y-2">
                <h4 className="font-black text-emerald-900 flex items-center gap-2">
                  <span>3.</span>
                  <span>MLB League-Wide Run Environment Normalization</span>
                </h4>
                <p>
                  MLB run environments have shifted dramatically over the past 14 years: the 2019 juiced-ball spike (1.39 HR/game), the 2022 deadened-ball slump (1.07 HR/game), and the 2023–2026 pitch clock and enlarged bases. The MLB toggle scales counts using official MLB league averages relative to the 2026 baseline.
                </p>
              </div>
            </div>

            <div className="pt-2 flex justify-end">
              <button
                onClick={() => setShowMethodologyModal(false)}
                className="px-6 py-2.5 rounded-xl bg-blue-600 text-white font-bold text-xs hover:bg-blue-700 transition cursor-pointer shadow-md"
              >
                Got It
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
