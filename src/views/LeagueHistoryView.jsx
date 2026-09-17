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
  if (key === 'OBP') {
    return n < 1 ? n.toFixed(3).replace(/^0/, '') : n.toFixed(3);
  }
  if (key === 'ERA' || key === 'WHIP') {
    return n.toFixed(2);
  }
  return Math.round(n).toLocaleString();
};

const AVAILABLE_SEASONS = [
  'ALL',
  2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012
];

const CATEGORIES = [
  { key: 'R', label: 'R', name: 'Runs', type: 'bat', higherIsBetter: true },
  { key: 'HR', label: 'HR', name: 'Home Runs', type: 'bat', higherIsBetter: true },
  { key: 'RBI', label: 'RBI', name: 'Runs Batted In', type: 'bat', higherIsBetter: true },
  { key: 'OBP', label: 'OBP', name: 'On-Base %', type: 'bat', higherIsBetter: true },
  { key: 'SB', label: 'SB', name: 'Stolen Bases', type: 'bat', higherIsBetter: true },
  { key: 'K', label: 'K', name: 'Strikeouts', type: 'pitch', higherIsBetter: true },
  { key: 'QS', label: 'QS', name: 'Quality Starts', type: 'pitch', higherIsBetter: true },
  { key: 'SVHLD', label: 'SV+H', name: 'Saves + Holds', type: 'pitch', higherIsBetter: true },
  { key: 'ERA', label: 'ERA', name: 'Earned Run Avg', type: 'pitch', higherIsBetter: false },
  { key: 'WHIP', label: 'WHIP', name: 'WHIP', type: 'pitch', higherIsBetter: false },
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

  // Standings display controls
  const [showRawStats, setShowRawStats] = useState(selectedYear === 'ALL');

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
      if (selectedYear !== 'ALL' && r.year !== parseInt(selectedYear, 10)) return false;
      if (selectedOwner !== 'ALL' && normalizeOwner(r.owner).toLowerCase() !== normalizeOwner(selectedOwner).toLowerCase()) return false;
      return true;
    });
  }, [augmentedFinishes, selectedYear, selectedOwner]);

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
          const rawA = a.rawStats?.[sortField];
          const rawB = b.rawStats?.[sortField];
          if (rawA !== undefined && rawB !== undefined) {
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
    const lowestScoring = [...list].sort((a, b) => {
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

    // 7. All-Time Highs and Lows in each individual stat
    const categoryStatRecords = {};
    CATEGORIES.forEach(cat => {
      const higherIsBetter = cat.higherIsBetter;
      const statKey = cat.key;
      const eraKey = statKey === 'SVHLD' ? 'SV+HDs' : statKey;

      const validRecords = [];
      list.forEach(r => {
        const rawVal = r.rawStats?.[statKey];
        if (rawVal !== undefined && rawVal !== null && rawVal > 0) {
          const adj = calculateEraAdjustedStat(eraKey, rawVal, r.year, { adjustRoster, adjustMlb });
          const sortVal = (adjustRoster || adjustMlb) ? adj.adjustedVal : rawVal;
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
          });
        }
      });

      // Highs (Best): For counting & OBP, highest sortVal. For ERA & WHIP, lowest sortVal.
      const highs = [...validRecords].sort((a, b) => {
        return higherIsBetter ? b.sortVal - a.sortVal : a.sortVal - b.sortVal;
      }).slice(0, 5);

      // Lows (Worst): For counting & OBP, lowest sortVal. For ERA & WHIP, highest sortVal.
      const lows = [...validRecords].sort((a, b) => {
        return higherIsBetter ? a.sortVal - b.sortVal : b.sortVal - a.sortVal;
      }).slice(0, 5);

      categoryStatRecords[statKey] = {
        cat,
        highs,
        lows,
        totalTracked: validRecords.length,
      };
    });

    return {
      topScoring,
      lowestScoring,
      topHitting,
      topPitching,
      blowouts,
      hangovers,
      categoryStatRecords,
    };
  }, [augmentedFinishes, adjustRoster, adjustMlb]);

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
    return selectedStatCategory === 'ALL'
      ? CATEGORIES
      : CATEGORIES.filter(c => c.key === selectedStatCategory);
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
                  }}
                  className="bg-gray-50 border border-gray-300 text-gray-800 text-xs font-black rounded-xl px-3 py-2 focus:ring-2 focus:ring-blue-500 cursor-pointer shadow-xs"
                >
                  {AVAILABLE_SEASONS.map(y => (
                    <option key={y} value={y}>
                      {y === 'ALL' ? '🌟 All Seasons (2012–2026)' : y === 2026 ? '⚾ 2026 (Regular Season)' : `🏛️ ${y} Season`}
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
                <span className="text-xs text-gray-500 font-normal">({sortedFinishes.length} finishes found)</span>
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
                    {CATEGORIES.map(cat => (
                      <th
                        key={cat.key}
                        onClick={() => handleSort(cat.key)}
                        className={`py-3 px-2 text-center cursor-pointer hover:bg-gray-200 transition ${
                          cat.type === 'bat' ? 'bg-amber-50/40 text-amber-900' : 'bg-indigo-50/40 text-indigo-900'
                        }`}
                        title={cat.name}
                      >
                        <div>{cat.label}</div>
                        {showRawStats && (
                          <div className="text-[8px] font-normal text-gray-500 font-sans normal-case tracking-normal mt-0.5">
                            pts / raw
                          </div>
                        )}
                      </th>
                    ))}
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
                          const rawFormatted = formatRawStat(cat.key, row.rawStats?.[cat.key]);
                          return (
                            <td
                              key={cat.key}
                              className={`py-2 px-2 text-center font-mono ${
                                isLeader ? 'bg-emerald-100/70 text-emerald-950 font-black' : 'text-gray-700'
                              }`}
                            >
                              <div className="font-bold text-xs">{val !== undefined ? val : '-'}</div>
                              {showRawStats && (
                                <div className="text-[10px] font-mono font-medium text-slate-500 mt-0.5 whitespace-nowrap">
                                  {rawFormatted}
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

          {/* ALL-TIME STAT RECORDS (HIGHS & LOWS BY STAT CATEGORY) */}
          <div className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-gray-200 pb-3">
              <div>
                <div className="flex items-center gap-2 text-sm font-black text-gray-900 uppercase tracking-wider">
                  <span>📊</span>
                  <span>All-Time Category Stat Records (Highs & Lows)</span>
                  <span className="text-xs text-gray-500 font-normal lowercase">(2018–2026 data tracked)</span>
                </div>
                <p className="text-xs text-gray-500 mt-0.5">
                  All-time single-season benchmarks across all 10 roto categories. {adjustRoster || adjustMlb ? 'Era & roster adjustments applied.' : 'Raw seasonal totals shown.'}
                </p>
              </div>

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
              </div>
            </div>

            {/* Category Records Cards Grid */}
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              {displayedStatCategories.map(cat => {
                const record = highlightsData.categoryStatRecords[cat.key];
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
                    className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4 hover:shadow-md transition"
                  >
                    {/* Card Header */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
                      <div className="flex items-center gap-2.5">
                        <span className="text-2xl">{icon}</span>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                            <span className="font-mono text-xs font-bold text-gray-500">({cat.label})</span>
                            <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                              isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                            }`}>
                              {isBat ? 'Batting' : 'Pitching'}
                            </span>
                          </div>
                          <div className="text-[11px] text-gray-400 mt-0.5 flex items-center gap-2">
                            <span>{cat.higherIsBetter ? '▲ Higher is better' : '▼ Lower is better'}</span>
                            <span>•</span>
                            <span>{totalTracked} team-seasons tracked</span>
                          </div>
                        </div>
                      </div>

                      {(adjustRoster || adjustMlb) && !RATE_STATS.has(cat.key) && (
                        <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-amber-500/10 text-amber-800 border border-amber-300/60 self-start sm:self-auto">
                          <span>⚡</span>
                          <span>Era Adjusted</span>
                        </span>
                      )}
                    </div>

                    {/* Highs & Lows Columns */}
                    <div className={`grid grid-cols-1 ${twoCols ? 'md:grid-cols-2 divide-y md:divide-y-0 md:divide-x divide-gray-100' : ''} gap-4`}>
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
                              if (rec.isAdjusted && !RATE_STATS.has(cat.key)) {
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
                                  className="py-2 px-1.5 flex items-center justify-between hover:bg-emerald-50/50 rounded-xl transition"
                                >
                                  <div className="flex items-center gap-2 min-w-0">
                                    <span className={`w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 ${
                                      isGold ? 'bg-amber-400 text-amber-950 ring-1 ring-amber-300' :
                                      isSilver ? 'bg-slate-300 text-slate-900' :
                                      isBronze ? 'bg-amber-700 text-amber-100' :
                                      'bg-gray-100 text-gray-600'
                                    }`}>
                                      {isGold ? '🥇' : isSilver ? '🥈' : isBronze ? '🥉' : i + 1}
                                    </span>
                                    <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                                    <div className="min-w-0">
                                      <div className="font-black text-gray-900 flex items-center gap-1 text-[11px] truncate">
                                        <span className="truncate">{rec.owner}</span>
                                        <span className="text-gray-400 font-normal shrink-0">({rec.year})</span>
                                        {rec.place === 1 && <span className="text-amber-500 text-[10px] shrink-0">👑</span>}
                                      </div>
                                      <div className="text-[10px] text-gray-500 truncate max-w-[120px] sm:max-w-[150px]">
                                        {rec.teamName}
                                      </div>
                                    </div>
                                  </div>

                                  <div className="text-right font-mono shrink-0 pl-2">
                                    <div className="font-black text-emerald-700 text-xs sm:text-sm">
                                      {dispVal}
                                    </div>
                                    {subRaw && (
                                      <div className="text-[9px] text-gray-400">
                                        {subRaw}
                                      </div>
                                    )}
                                    <div className="text-[9px] text-gray-400">
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
                        <div className={`space-y-2 ${twoCols ? 'pt-3 md:pt-0 md:pl-4' : ''}`}>
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
                              if (rec.isAdjusted && !RATE_STATS.has(cat.key)) {
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
                                  className="py-2 px-1.5 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition"
                                >
                                  <div className="flex items-center gap-2 min-w-0">
                                    <span className="w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 bg-rose-100 text-rose-900 font-bold">
                                      {i + 1}
                                    </span>
                                    <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                                    <div className="min-w-0">
                                      <div className="font-black text-gray-900 flex items-center gap-1 text-[11px] truncate">
                                        <span className="truncate">{rec.owner}</span>
                                        <span className="text-gray-400 font-normal shrink-0">({rec.year})</span>
                                      </div>
                                      <div className="text-[10px] text-gray-500 truncate max-w-[120px] sm:max-w-[150px]">
                                        {rec.teamName}
                                      </div>
                                    </div>
                                  </div>

                                  <div className="text-right font-mono shrink-0 pl-2">
                                    <div className="font-black text-rose-700 text-xs sm:text-sm">
                                      {dispVal}
                                    </div>
                                    {subRaw && (
                                      <div className="text-[9px] text-gray-400">
                                        {subRaw}
                                      </div>
                                    )}
                                    <div className="text-[9px] text-gray-400">
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
              })}
            </div>
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
                    <span className="text-[11px] text-gray-400 font-mono">Sub-35 Campaigns</span>
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
