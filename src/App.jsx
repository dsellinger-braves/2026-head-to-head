import { useEffect, useState, useMemo } from 'react';
import { supabase } from './supabaseClient';
import { get, set, del } from 'idb-keyval';

import { generateSchedule, TEAMS, getPeriodRangeForWeek } from './schedule';
import { calculateStandings } from './utils/standings';
// NOTE: Make sure to export calculateTrioMatchupResult from scoring.js!
import { aggregateStats, calculateMatchupResult, calculateTrioMatchupResult } from './utils/scoring';

import WeeklyView from './views/WeeklyView';
import SummaryView from './views/SummaryView';
import TeamsView from './views/TeamsView';
import PlayersView from './views/PlayersView';
import ProgressionView from './views/ProgressionView';
import HighlightsView from './views/HighlightsView';
import OwnerDisparitiesView from './views/OwnerDisparitiesView';
import OwnerDetailModal from './components/OwnerDetailModal';
import PlayerHistoryModal from './components/PlayerHistoryModal';
import LiveScoreboardView from './views/LiveScoreboardView';
import TransactionsView from './views/TransactionsView';
import DraftRoomView from './views/DraftRoomView';
import PlayerValuationsView from './views/PlayerValuationsView';
import PickemView from './views/PickemView';
import DraftCapitalView from './views/DraftCapitalView';
import KeepersBudgetsView from './views/KeepersBudgetsView';

const AVAILABLE_SEASONS = Array.from({ length: 2026 - 2012 + 1 }, (_, i) => 2026 - i);

const VIEW_TO_HASH = {
  weekly: 'matchups',
  summary: 'standings',
  teams: 'teams',
  transactions: 'transactions',
  players: 'players',
  disparities: 'disparities',
  progression: 'progression',
  highlights: 'highlights',
  fantasycast: 'fantasycast',
  valuations: 'keeper-prices',
  draft: 'draft',
  pickem: 'pickem',
  capital: 'draft-capital',
  keepers: 'keepers-budgets',
};

const HASH_TO_VIEW = {
  matchups: 'weekly',
  weekly: 'weekly',
  standings: 'summary',
  summary: 'summary',
  teams: 'teams',
  transactions: 'transactions',
  players: 'players',
  disparities: 'disparities',
  progression: 'progression',
  highlights: 'highlights',
  fantasycast: 'fantasycast',
  'keeper-prices': 'valuations',
  'keeper-pricing': 'valuations',
  valuations: 'valuations',
  pricing: 'valuations',
  draft: 'draft',
  pickem: 'pickem',
  'draft-capital': 'capital',
  capital: 'capital',
  'keepers-budgets': 'keepers',
  keepers: 'keepers',
  budgets: 'keepers',
};

const OFFSEASON_VIEWS = new Set(['valuations', 'draft', 'pickem', 'capital', 'keepers']);

function getViewFromHash() {
  if (typeof window === 'undefined') return 'weekly';
  const hash = window.location.hash.replace(/^#\/?/, '').trim().toLowerCase();
  const route = hash.split('?')[0].split('/')[0];
  return HASH_TO_VIEW[route] || 'weekly';
}

function App() {
  const [loading, setLoading] = useState(true);
  const [currentView, setCurrentView] = useState(getViewFromHash);
  const [rawData, setRawData] = useState([]);
  const [loadStatus, setLoadStatus] = useState("Initializing...");
  const [selectedSeason, setSelectedSeason] = useState(2026);

  const [selectedOwner, setSelectedOwner] = useState(null);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [allSeasonData, setAllSeasonData] = useState({});

  const [activeGroup, setActiveGroup] = useState(() => {
    const initialView = getViewFromHash();
    return OFFSEASON_VIEWS.has(initialView) ? 'offseason' : 'season';
  });

  // Synchronize browser URL hash with currentView
  useEffect(() => {
    const slug = VIEW_TO_HASH[currentView] || currentView;
    const targetHash = `#/${slug}`;
    if (window.location.hash !== targetHash) {
      window.location.hash = targetHash;
    }
    setActiveGroup(OFFSEASON_VIEWS.has(currentView) ? 'offseason' : 'season');
  }, [currentView]);

  const handleGroupChange = (group) => {
    setActiveGroup(group);
    if (group === 'season' && OFFSEASON_VIEWS.has(currentView)) {
      setCurrentView('weekly');
    } else if (group === 'offseason' && !OFFSEASON_VIEWS.has(currentView)) {
      setCurrentView('valuations');
    }
  };

  // Listen to browser Back/Forward and direct hash navigation
  useEffect(() => {
    const handleHashChange = () => {
      const newView = getViewFromHash();
      setCurrentView((prev) => (prev !== newView ? newView : prev));
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  const baseSchedule = useMemo(() => generateSchedule(), []);

  // historical_data rows are flat CSVs from the Python scraper.
  // Reshape them to match the player_daily_stats schema the rest of the app expects.
  const normalizeHistoricalRecord = (row) => {
    const stats = {};
    for (const [k, v] of Object.entries(row)) {
      // ESPN stat IDs are always purely numeric column names ('5', '20', '34', …).
      // Everything else is a metadata column we handle explicitly below.
      if (/^\d+$/.test(k) && v !== null && v !== '' && v !== undefined) {
        stats[k] = v;
      }
    }
    return {
      season_year: row.season_year,
      team_id: row.team_id,
      scoring_period_id: row.scoring_period_id,
      player_id: row.id,
      full_name: row.fullName,
      lineup_slot_id: row.lineupSlotID != null ? parseInt(row.lineupSlotID) : null,
      stats,
    };
  };

  // Core fetch: check cache → Supabase. Returns normalized records, touches no state.
  const fetchRawSeason = async (season, onProgress) => {
    const cacheKey = `fantasy_data_${season}`;
    const cached = await get(cacheKey);
    if (cached?.length > 0) return cached;

    const isHistorical = season < 2026;
    const tableName = isHistorical ? 'historical_data' : 'player_daily_stats';
    let allRecords = [];
    let page = 0;
    const pageSize = 1000;
    let hasMore = true;

    while (hasMore) {
      let query = supabase
        .from(tableName)
        .select('*')
        .range(page * pageSize, (page + 1) * pageSize - 1);

      if (!isHistorical) {
        query = query.eq('league_id', 130215).order('id', { ascending: true });
      } else {
        query = query.eq('league_id', 130215).eq('season_year', season);
      }

      const { data, error } = await query;
      if (error) throw error;
      if (data?.length > 0) {
        allRecords = [...allRecords, ...data];
        onProgress?.(`Downloading ${season}… (${allRecords.length} records)`);
      }
      if (!data || data.length < pageSize) hasMore = false;
      else page++;
    }

    const normalized = isHistorical
      ? allRecords.map(normalizeHistoricalRecord)
      : allRecords;

    // Remove duplicate rows (e.g. from a CSV uploaded to Supabase more than once).
    // Key: season + team + period + player — all four must match to be a duplicate.
    const seen = new Set();
    const deduped = normalized.filter(r => {
      const k = `${r.season_year}__${r.team_id}__${r.scoring_period_id}__${r.player_id}__${r.full_name}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });

    await set(cacheKey, deduped);
    return deduped;
  };

  const fetchAllData = async (season) => {
    try {
      setLoading(true);
      setLoadStatus("Checking local cache...");
      const records = await fetchRawSeason(season, setLoadStatus);
      setRawData(records);
    } catch (err) {
      console.error("App Error:", err);
      try {
        const cached = await get(`fantasy_data_${season}`);
        if (cached?.length > 0) {
          console.warn(`Recovered ${cached.length} records from local cache following server error`);
          setRawData(cached);
          return;
        }
      } catch (cacheErr) {
        console.error("Cache recovery failed:", cacheErr);
      }
      setLoadStatus("Error loading data from server.");
    } finally {
      setLoading(false);
    }
  };

  // Download every season into allSeasonData without switching the active view.
  const [downloadAllProgress, setDownloadAllProgress] = useState(null);

  const downloadAllSeasons = async () => {
    const missing = AVAILABLE_SEASONS.filter(y => !allSeasonData[y]?.length);
    if (!missing.length) return;
    setDownloadAllProgress({ done: 0, total: missing.length, current: null });
    for (let i = 0; i < missing.length; i++) {
      const year = missing[i];
      setDownloadAllProgress({ done: i, total: missing.length, current: year });
      try {
        const data = await fetchRawSeason(year);
        if (data?.length > 0) {
          setAllSeasonData(prev => ({ ...prev, [year]: data }));
        }
      } catch (e) {
        console.error(`Failed to load ${year}:`, e);
      }
    }
    setDownloadAllProgress(null);
  };

  const todaysRecords = useMemo(() => {
    if (!rawData.length) return [];
    const maxPeriodId = Math.max(...rawData.map(r => r.scoring_period_id));
    return rawData.filter(r => r.scoring_period_id === maxPeriodId);
  }, [rawData]);

  useEffect(() => {
    fetchAllData(selectedSeason);
  }, [selectedSeason]); // eslint-disable-line react-hooks/exhaustive-deps

  // Accumulate every loaded season so HighlightsView can span multiple years.
  useEffect(() => {
    if (rawData.length > 0) {
      setAllSeasonData(prev => ({ ...prev, [selectedSeason]: rawData }));
    }
  }, [rawData]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleRefresh = async () => {
    // Clear every season's cache so deduplication runs fresh on the next fetch.
    await Promise.all(AVAILABLE_SEASONS.map(y => del(`fantasy_data_${y}`)));
    setAllSeasonData({});
    fetchAllData(selectedSeason);
  };

  const handleSeasonChange = (year) => {
    setRawData([]);
    setSelectedSeason(year);
  };


  // --- CORE LOGIC: PROCESS SCHEDULE & STATS ---
  const processedWeeks = useMemo(() => {
    if (!rawData.length) return [];

    const resolveMatchupStats = (matchup, week) => {

      // ---------------------------------------------------
      // 1. TRIO MATCHUPS (Phases 1 & 2)
      // ---------------------------------------------------
      if (matchup.teamIds) {
        // Handle Placeholders (e.g. SEED_1)
        if (matchup.teamIds.some(id => typeof id === 'string')) {
          return {
            ...matchup,
            isPlaceholder: true,
            type: 'trio',
            teams: matchup.teamIds.map(id => ({ name: id, owner: 'TBD', id: null })),
            teamStats: {},
            teamRecords: {},
            result: {}
          };
        }

        const { startId, endId } = getPeriodRangeForWeek(week);
        const teamRecords = {};
        matchup.teamIds.forEach(id => teamRecords[id] = []);

        for (const r of rawData) {
          if (r.scoring_period_id >= startId && r.scoring_period_id <= endId) {
            if (teamRecords[r.team_id]) {
              teamRecords[r.team_id].push(r);
            }
          }
        }

        const teamStats = {};
        matchup.teamIds.forEach(id => {
          teamStats[id] = aggregateStats(teamRecords[id]);
        });

        // Fail gracefully if trio scoring isn't built yet
        let result = {};
        if (typeof calculateTrioMatchupResult === 'function') {
          result = calculateTrioMatchupResult(teamStats, matchup.teamIds);
        }

        const teams = matchup.teamIds.map(id => ({ ...(TEAMS[id] || { name: 'Unknown', owner: '' }), id }));

        return {
          matchupId: matchup.id,
          type: 'trio',
          label: matchup.label,
          teams,
          teamStats,
          teamRecords,
          result,
          isPlaceholder: false
        };
      }

      // ---------------------------------------------------
      // 2. HEAD-TO-HEAD MATCHUPS (Phases 3 & 4)
      // ---------------------------------------------------
      if (typeof matchup.homeTeamId === 'string' || typeof matchup.awayTeamId === 'string') {
        return {
          ...matchup,
          isPlaceholder: true,
          type: 'h2h',
          homeTeam: { name: matchup.homeTeamId, owner: 'TBD', id: null },
          awayTeam: { name: matchup.awayTeamId, owner: 'TBD', id: null },
          homeStats: {}, awayStats: {},
          result: { homeScore: 0, awayScore: 0, ties: 0 },
          homeRecords: [], awayRecords: []
        };
      }

      const { startId, endId } = getPeriodRangeForWeek(week);
      const homeRecords = [];
      const awayRecords = [];
      const allHumanRecords = [];

      for (const r of rawData) {
        if (r.scoring_period_id >= startId && r.scoring_period_id <= endId) {
          if (r.team_id == matchup.homeTeamId) homeRecords.push(r);
          else if (r.team_id == matchup.awayTeamId) awayRecords.push(r);

          if (r.team_id != 99) allHumanRecords.push(r);
        }
      }

      const computeAverageTeamStats = (records) => {
        const totalStats = aggregateStats(records);
        const avgStats = { ...totalStats };
        const numTeams = 9; // 9 human teams in the league

        // Divide counting components by 9 to get the average
        const fieldsToDivide = ['R', 'HR', 'RBI', 'SB', 'K', 'QS', 'SV+HDs', 'ER', 'IP', 'BB_Allowed', 'H_Allowed', 'OBP_num', 'PA'];
        fieldsToDivide.forEach(key => {
          if (avgStats[key]) avgStats[key] = avgStats[key] / numTeams;
        });

        // Recalculate rate stats from the divided components
        avgStats.OBP = avgStats.PA > 0 ? (avgStats.OBP_num / avgStats.PA).toFixed(3) : ".000";
        avgStats.ERA = avgStats.IP > 0 ? ((avgStats.ER * 9) / avgStats.IP).toFixed(2) : "0.00";
        avgStats.WHIP = avgStats.IP > 0 ? ((avgStats.BB_Allowed + avgStats.H_Allowed) / avgStats.IP).toFixed(2) : "0.00";

        return avgStats;
      };

      const homeStats = matchup.homeTeamId == 99 ? computeAverageTeamStats(allHumanRecords) : aggregateStats(homeRecords);
      const awayStats = matchup.awayTeamId == 99 ? computeAverageTeamStats(allHumanRecords) : aggregateStats(awayRecords);
      const result = calculateMatchupResult(homeStats, awayStats);

      const homeTeamInfo = TEAMS[matchup.homeTeamId] || { name: 'Unknown', owner: '' };
      const awayTeamInfo = TEAMS[matchup.awayTeamId] || { name: 'Unknown', owner: '' };

      return {
        matchupId: matchup.id,
        type: 'h2h',
        label: matchup.label,
        homeTeam: { ...homeTeamInfo, id: matchup.homeTeamId },
        awayTeam: { ...awayTeamInfo, id: matchup.awayTeamId },
        homeStats, awayStats, result, homeRecords, awayRecords,
        isPlaceholder: false
      };
    };

    let resolvedSchedule = [];

    // --- STEP A: PROCESS PHASE 1 (Weeks 1-12) ---
    for (let i = 0; i < 12; i++) {
      const week = baseSchedule[i];
      if (!week) continue;
      const resolvedMatchups = week.matchups.map(m => resolveMatchupStats(m, week));
      resolvedSchedule.push({ ...week, matchups: resolvedMatchups });
    }

    // --- STEP B: PROCESS PHASE 2 (Weeks 13-14) MID-SEASON CHAMPIONSHIP ---
    const standingsAfter12 = calculateStandings(resolvedSchedule, 12);
    const midSeeds = {};
    standingsAfter12.forEach((team, index) => {
      midSeeds[`SEED_${index + 1}`] = team.id;
    });

    for (let i = 12; i < 14; i++) {
      const week = baseSchedule[i];
      if (!week) continue;
      const dynamicMatchups = week.matchups.map(m => {
        if (m.teamIds) {
          const realIds = m.teamIds.map(id => typeof id === 'string' ? midSeeds[id] : id);
          return { ...m, teamIds: realIds };
        }
        return m;
      });
      const resolvedMatchups = dynamicMatchups.map(m => resolveMatchupStats(m, week));
      resolvedSchedule.push({ ...week, matchups: resolvedMatchups });
    }

    // --- STEP C: PROCESS PHASE 3 (Weeks 15-23) SPLIT LEAGUES ---
    const standingsAfter14 = calculateStandings(resolvedSchedule, 14);
    const splitSeeds = {};
    standingsAfter14.forEach((team, index) => {
      splitSeeds[`SEED_${index + 1}`] = team.id;
    });

    for (let i = 14; i < 23; i++) {
      const week = baseSchedule[i];
      if (!week) continue;
      const dynamicMatchups = week.matchups.map(m => {
        if (m.homeTeamId !== undefined && m.awayTeamId !== undefined) {
          const realHome = typeof m.homeTeamId === 'string' ? splitSeeds[m.homeTeamId] : m.homeTeamId;
          const realAway = typeof m.awayTeamId === 'string' ? splitSeeds[m.awayTeamId] : m.awayTeamId;
          return { ...m, homeTeamId: realHome, awayTeamId: realAway };
        }
        return m;
      });

      const resolvedMatchups = dynamicMatchups.map(m => resolveMatchupStats(m, week));
      resolvedSchedule.push({ ...week, matchups: resolvedMatchups });
    }

    // --- STEP D: PROCESS PHASE 4 (Weeks 24-25) PLAYOFFS ---
    // Option 1: Seeding is determined by Phase 3 (Weeks 15-23) head-to-head records
    // within each respective tier (Winners League Seeds 1-4, Consolation League Seeds 5-9).
    const phase3Standings = calculateStandings(resolvedSchedule, { startWeek: 15, upToWeek: 23, phase: 3 });
    const standingsAfter23 = calculateStandings(resolvedSchedule, 23);

    const getFullSeasonRank = (teamId) => {
      const idx = standingsAfter23.findIndex(t => t.id === teamId);
      return idx >= 0 ? idx : 999;
    };

    const sortTier = (teamA, teamB) => {
      const totalA = teamA.wins + teamA.losses + teamA.ties;
      const totalB = teamB.wins + teamB.losses + teamB.ties;
      const pctA = totalA > 0 ? (teamA.wins + teamA.ties * 0.5) / totalA : 0;
      const pctB = totalB > 0 ? (teamB.wins + teamB.ties * 0.5) / totalB : 0;
      if (Math.abs(pctB - pctA) > 0.0001) return pctB - pctA;
      if (teamB.score !== teamA.score) return teamB.score - teamA.score;
      return getFullSeasonRank(teamA.id) - getFullSeasonRank(teamB.id);
    };

    const winnersTeamIds = [
      splitSeeds.SEED_1,
      splitSeeds.SEED_2,
      splitSeeds.SEED_3,
      splitSeeds.SEED_4
    ].filter(Boolean);

    const champSeeds = winnersTeamIds
      .map(id => phase3Standings.find(t => t.id === id) || { id, wins: 0, losses: 0, ties: 0, score: 0 })
      .sort(sortTier);

    const consolTeamIds = [
      splitSeeds.SEED_5,
      splitSeeds.SEED_6,
      splitSeeds.SEED_7,
      splitSeeds.SEED_8,
      splitSeeds.SEED_9
    ].filter(Boolean);

    const consolSeeds = consolTeamIds
      .map(id => phase3Standings.find(t => t.id === id) || { id, wins: 0, losses: 0, ties: 0, score: 0 })
      .sort(sortTier);

    if (baseSchedule[23]) {
      const week24 = baseSchedule[23];
      const sfMatchups = [
        { id: 'sf1', homeTeamId: champSeeds[0]?.id, awayTeamId: champSeeds[3]?.id, label: "Semi-Final A" },
        { id: 'sf2', homeTeamId: champSeeds[1]?.id, awayTeamId: champSeeds[2]?.id, label: "Semi-Final B" },
        { id: 'c1', homeTeamId: consolSeeds[0]?.id, awayTeamId: consolSeeds[3]?.id, label: "Consolation A" },
        { id: 'c2', homeTeamId: consolSeeds[1]?.id, awayTeamId: consolSeeds[2]?.id, label: "Consolation B" }
      ];

      const resolvedSF = sfMatchups.map(m => resolveMatchupStats(m, week24));
      resolvedSchedule.push({ ...week24, matchups: resolvedSF });
    }

    if (baseSchedule[24]) {
      const week25 = baseSchedule[24];
      const prevWeek = resolvedSchedule[23];

      const isUnplayed = (m) => {
        if (!m || !m.result) return true;
        return m.result.homeScore === 0 && m.result.awayScore === 0 && (m.result.ties === 0 || m.result.ties === undefined);
      };

      const getWinner = (matchId) => {
        const m = prevWeek?.matchups.find(pm => pm.id === matchId || pm.matchupId === matchId);
        if (isUnplayed(m)) return "TBD";
        return m.result.homeScore >= m.result.awayScore ? m.homeTeam?.id : m.awayTeam?.id;
      };

      const getLoser = (matchId) => {
        const m = prevWeek?.matchups.find(pm => pm.id === matchId || pm.matchupId === matchId);
        if (isUnplayed(m)) return "TBD";
        return m.result.homeScore >= m.result.awayScore ? m.awayTeam?.id : m.homeTeam?.id;
      };

      const finalMatchups = [
        { id: 'final', homeTeamId: getWinner('sf1'), awayTeamId: getWinner('sf2'), label: "🏆 Championship" },
        { id: '3rd', homeTeamId: getLoser('sf1'), awayTeamId: getLoser('sf2'), label: "3rd Place Match" },
        { id: 'c_final', homeTeamId: getWinner('c1'), awayTeamId: getWinner('c2'), label: "Consolation Final (5th Place)" },
        { id: 'c_3rd', homeTeamId: getLoser('c1'), awayTeamId: getLoser('c2'), label: "7th Place Match" }
      ];

      const resolvedFinals = finalMatchups.map(m => resolveMatchupStats(m, week25));
      resolvedSchedule.push({ ...week25, matchups: resolvedFinals });
    }

    return resolvedSchedule;

  }, [rawData, baseSchedule]);

  // --- RENDER ---
  // Do not block Draft Room on season data loading
  if (loading && currentView !== 'draft') {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-gray-50 text-gray-500 gap-4">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-900"></div>
        <div className="font-bold animate-pulse">{loadStatus}</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100 font-sans text-gray-800">
      {/* League site header - hidden in Draft Room for clean, full-screen war room layout */}
      {currentView !== 'draft' && (
        <nav className="bg-blue-900 text-white shadow-lg sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            {/* Top Bar: Brand, Group Selector, Controls */}
            <div className="flex items-center justify-between h-14 border-b border-blue-800/80">
              <div className="flex items-center gap-4 sm:gap-6">
                <a
                  href="#/matchups"
                  onClick={(e) => { e.preventDefault(); setCurrentView('weekly'); }}
                  className="font-black text-lg sm:text-xl tracking-wider text-white hover:text-blue-200 transition-colors cursor-pointer flex items-center gap-2"
                >
                  <span className="text-xl">⚾</span>
                  <span>FANTASY LEAGUE</span>
                </a>

                {/* Season vs. Off-Season Group Switcher */}
                <div className="flex items-center bg-blue-950/80 p-1 rounded-xl border border-blue-800 shadow-inner">
                  <button
                    onClick={() => handleGroupChange('season')}
                    className={`px-3 py-1 rounded-lg text-xs font-black uppercase tracking-wider flex items-center gap-1.5 transition-all cursor-pointer ${
                      activeGroup === 'season'
                        ? 'bg-blue-600 text-white shadow-sm ring-1 ring-blue-400'
                        : 'text-blue-300 hover:text-white hover:bg-blue-800/50'
                    }`}
                  >
                    <span>⚾</span>
                    <span>Season</span>
                  </button>
                  <button
                    onClick={() => handleGroupChange('offseason')}
                    className={`px-3 py-1 rounded-lg text-xs font-black uppercase tracking-wider flex items-center gap-1.5 transition-all cursor-pointer ${
                      activeGroup === 'offseason'
                        ? 'bg-amber-600 text-white shadow-sm ring-1 ring-amber-400'
                        : 'text-amber-200 hover:text-white hover:bg-blue-800/50'
                    }`}
                  >
                    <span>🌴</span>
                    <span>Off-Season</span>
                  </button>
                </div>
              </div>

              {/* Right Side: Season Selector & Refresh */}
              <div className="flex items-center space-x-2">
                <select
                  value={selectedSeason}
                  onChange={e => handleSeasonChange(parseInt(e.target.value))}
                  className="bg-blue-800 text-white text-xs font-bold rounded-lg px-2.5 py-1.5 border border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-400 cursor-pointer"
                >
                  {AVAILABLE_SEASONS.map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
                <button
                  onClick={handleRefresh}
                  className="p-1.5 text-blue-200 hover:text-white cursor-pointer rounded-lg hover:bg-blue-800 transition-colors"
                  title="Clear Cache & Reload"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
                </button>
              </div>
            </div>

            {/* Bottom Bar: Sub-Navigation for Active Group */}
            <div className="flex items-center space-x-1.5 py-2 overflow-x-auto scrollbar-none text-xs">
              {activeGroup === 'season' ? (
                <>
                  <a
                    href="#/matchups"
                    onClick={(e) => { e.preventDefault(); setCurrentView('weekly'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'weekly' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Matchups
                  </a>
                  <a
                    href="#/standings"
                    onClick={(e) => { e.preventDefault(); setCurrentView('summary'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'summary' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Standings
                  </a>
                  <a
                    href="#/teams"
                    onClick={(e) => { e.preventDefault(); setCurrentView('teams'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'teams' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Teams
                  </a>
                  <a
                    href="#/transactions"
                    onClick={(e) => { e.preventDefault(); setCurrentView('transactions'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'transactions' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Transactions
                  </a>
                  <a
                    href="#/players"
                    onClick={(e) => { e.preventDefault(); setCurrentView('players'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'players' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Players
                  </a>
                  <a
                    href="#/disparities"
                    onClick={(e) => { e.preventDefault(); setCurrentView('disparities'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'disparities' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Disparities
                  </a>
                  <a
                    href="#/progression"
                    onClick={(e) => { e.preventDefault(); setCurrentView('progression'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'progression' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Progression
                  </a>
                  <a
                    href="#/highlights"
                    onClick={(e) => { e.preventDefault(); setCurrentView('highlights'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold transition-colors ${currentView === 'highlights' ? 'bg-blue-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    Highlights
                  </a>
                  <a
                    href="#/fantasycast"
                    onClick={(e) => { e.preventDefault(); setCurrentView('fantasycast'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold flex items-center gap-1.5 transition-colors ${currentView === 'fantasycast' ? 'bg-red-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'}`}
                  >
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-300 opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
                    </span>
                    FantasyCast
                  </a>
                </>
              ) : (
                <>
                  <a
                    href="#/keeper-prices"
                    onClick={(e) => { e.preventDefault(); setCurrentView('valuations'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold flex items-center gap-1.5 transition-colors ${
                      currentView === 'valuations' ? 'bg-indigo-600 text-white ring-1 ring-indigo-400 shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="Keeper Prices & Player Valuations"
                  >
                    <span>💰</span>
                    <span>Keeper Prices</span>
                  </a>

                  <a
                    href="#/draft"
                    onClick={(e) => { e.preventDefault(); setCurrentView('draft'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold flex items-center gap-1.5 transition-colors ${
                      currentView === 'draft' ? 'bg-amber-600 text-white ring-1 ring-amber-400 shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="Draft War Room"
                  >
                    <span>🎯</span>
                    <span>Draft Room</span>
                  </a>

                  <a
                    href="#/pickem"
                    onClick={(e) => { e.preventDefault(); setCurrentView('pickem'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold flex items-center gap-1.5 transition-colors ${
                      currentView === 'pickem' ? 'bg-purple-700 text-white ring-1 ring-purple-400 shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="Annual MLB Pick'em"
                  >
                    <span>🔮</span>
                    <span>Pick&apos;em</span>
                  </a>

                  <a
                    href="#/draft-capital"
                    onClick={(e) => { e.preventDefault(); setCurrentView('capital'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold flex items-center gap-1.5 transition-colors ${
                      currentView === 'capital' ? 'bg-teal-600 text-white ring-1 ring-teal-400 shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="2027 Draft Capital & Future Pick Ledgers"
                  >
                    <span>🎟️</span>
                    <span>Draft Capital</span>
                  </a>

                  <a
                    href="#/keepers-budgets"
                    onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); }}
                    className={`px-3 py-1.5 rounded-lg font-bold flex items-center gap-1.5 transition-colors ${
                      currentView === 'keepers' ? 'bg-emerald-600 text-white ring-1 ring-emerald-400 shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="Keepers, Budget Matrix & Compensation Simulator"
                  >
                    <span>💎</span>
                    <span>Keepers & Budget</span>
                  </a>
                </>
              )}
            </div>
          </div>
        </nav>
      )}

      {currentView === 'draft' ? (
        <DraftRoomView
          onOpenPlayerModal={(id, name) => setSelectedPlayer({ id, name })}
          onSwitchView={(view) => setCurrentView(view)}
        />
      ) : (
        <main className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
          {currentView === 'weekly' && (
            <WeeklyView
              processedWeeks={processedWeeks}
              allStats={rawData}
              onOwnerClick={(team) => setSelectedOwner(team)}
            />
          )}
          {currentView === 'summary' && (
            <SummaryView
              processedWeeks={processedWeeks}
              allStats={rawData}
              onOwnerClick={(team) => setSelectedOwner(team)}
            />
          )}
          {currentView === 'teams' && (
            <TeamsView
              allStats={rawData}
              selectedSeason={selectedSeason}
              onOwnerClick={(team) => setSelectedOwner(team)}
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
            />
          )}
          {currentView === 'transactions' && (
            <TransactionsView
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
              onOwnerClick={(team) => setSelectedOwner(team)}
            />
          )}
          {currentView === 'players' && (
            <PlayersView
              allStats={rawData}
              selectedSeason={selectedSeason}
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
            />
          )}
          {currentView === 'disparities' && (
            <OwnerDisparitiesView
              allStats={rawData}
              selectedSeason={selectedSeason}
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
              onOwnerClick={(team) => setSelectedOwner(team)}
            />
          )}
          {currentView === 'progression' && (
            <ProgressionView allStats={rawData} selectedSeason={selectedSeason} processedWeeks={processedWeeks} />
          )}
          {currentView === 'highlights' && (
            <HighlightsView
              allStats={rawData}
              allSeasonData={allSeasonData}
              selectedSeason={selectedSeason}
              onDownloadAll={downloadAllSeasons}
              downloadAllProgress={downloadAllProgress}
            />
          )}
          {currentView === 'fantasycast' && (
            <LiveScoreboardView
              todaysRecords={todaysRecords}
            />
          )}
          {currentView === 'valuations' && (
            <PlayerValuationsView
              allStats={rawData}
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
              onOwnerClick={(team) => setSelectedOwner(team)}
            />
          )}
          {currentView === 'pickem' && (
            <PickemView />
          )}
          {currentView === 'capital' && (
            <DraftCapitalView currentUser="Daniel" />
          )}
          {currentView === 'keepers' && (
            <KeepersBudgetsView
              currentUser="Daniel"
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
            />
          )}
        </main>
      )}

      {/* --- MODAL LAYER --- */}
      {selectedOwner && (
        <OwnerDetailModal
          team={selectedOwner}
          allStats={rawData}
          onClose={() => setSelectedOwner(null)}
          onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
        />
      )}

      {selectedPlayer && (
        <div style={{ zIndex: 90, position: 'relative' }}>
          <PlayerHistoryModal
            playerId={selectedPlayer.id}
            playerName={selectedPlayer.name}
            allStats={rawData}
            selectedSeason={selectedSeason}
            onClose={() => setSelectedPlayer(null)}
          />
        </div>
      )}
    </div>
  );
}

export default App;