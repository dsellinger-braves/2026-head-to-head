import { useEffect, useState, useMemo, useRef } from 'react';
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
import UserNavWidget from './components/UserNavWidget';
import CommishActiveBanner from './components/CommishActiveBanner';
import { useAuth } from './context/useAuth';

const AVAILABLE_SEASONS = [2027, ...Array.from({ length: 2026 - 2012 + 1 }, (_, i) => 2026 - i)];

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
  const rawHash = window.location.hash || '';
  // If the URL contains OAuth return tokens (access_token=, refresh_token=, error=),
  // don't treat the token string as a view slug. Restore saved pre-login route if available.
  if (rawHash.includes('access_token=') || rawHash.includes('refresh_token=') || rawHash.includes('error=')) {
    try {
      const saved = sessionStorage.getItem('oauth_pre_login_hash');
      if (saved) {
        const cleanSaved = saved.replace(/^#\/?/, '').trim().toLowerCase();
        const route = cleanSaved.split('?')[0].split('/')[0];
        if (HASH_TO_VIEW[route]) return HASH_TO_VIEW[route];
      }
    } catch {
      // ignore
    }
    return 'keepers';
  }
  const hash = rawHash.replace(/^#\/?/, '').trim().toLowerCase();
  const route = hash.split('?')[0].split('/')[0];
  return HASH_TO_VIEW[route] || 'weekly';
}

function App() {
  const { user, signOut, effectiveOwner, isCommissioner } = useAuth();
  const [loading, setLoading] = useState(true);
  const [currentView, setCurrentView] = useState(getViewFromHash);
  const [rawData, setRawData] = useState([]);
  const [loadStatus, setLoadStatus] = useState("Initializing...");
  const [selectedSeason, setSelectedSeason] = useState(2026);
  const [offseasonYear, setOffseasonYear] = useState(2027);
  const fetchingRef = useRef(false);

  const [selectedOwner, setSelectedOwner] = useState(null);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [allSeasonData, setAllSeasonData] = useState({});

  const [activeGroup, setActiveGroup] = useState(() => {
    const initialView = getViewFromHash();
    return OFFSEASON_VIEWS.has(initialView) ? 'offseason' : 'season';
  });

  // Synchronize browser URL hash with currentView
  useEffect(() => {
    // If the hash contains OAuth tokens, do not overwrite it immediately; wait for Supabase to parse it
    if (window.location.hash && (window.location.hash.includes('access_token=') || window.location.hash.includes('error='))) {
      return;
    }
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
    if (season > 2026) {
      return [];
    }
    const cacheKey = `fantasy_data_${season}`;
    try {
      const cachePromise = get(cacheKey);
      const cacheTimeout = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('IndexedDB read timeout')), 2500)
      );
      const cached = await Promise.race([cachePromise, cacheTimeout]);
      if (cached?.length > 0) return cached;
    } catch (cacheErr) {
      console.warn('Local cache read notice or timeout:', cacheErr);
    }

    const isHistorical = season < 2026;
    const tableName = isHistorical ? 'historical_data' : 'player_daily_stats';
    const pageSize = 1000;

    let countQuery = supabase
      .from(tableName)
      .select('*', { count: 'exact', head: true });

    if (!isHistorical) {
      countQuery = countQuery.eq('league_id', 130215);
    } else {
      countQuery = countQuery.eq('league_id', 130215).eq('season_year', season);
    }

    const { count, error: countErr } = await countQuery;
    if (countErr) throw countErr;

    const totalPages = Math.max(1, Math.ceil((count || 0) / pageSize));
    let allRecords = [];
    const batchSize = 6;

    for (let b = 0; b < totalPages; b += batchSize) {
      const promises = [];
      for (let p = b; p < Math.min(b + batchSize, totalPages); p++) {
        let q = supabase
          .from(tableName)
          .select('*')
          .range(p * pageSize, (p + 1) * pageSize - 1);

        if (!isHistorical) {
          q = q.eq('league_id', 130215).order('id', { ascending: true });
        } else {
          q = q.eq('league_id', 130215).eq('season_year', season);
        }
        promises.push(q);
      }

      const results = await Promise.all(promises);
      for (const res of results) {
        if (res.error) throw res.error;
        if (res.data?.length > 0) {
          allRecords.push(...res.data);
        }
      }
      onProgress?.(`Downloading ${season}… (${Math.min(allRecords.length, count || allRecords.length)} of ${count || allRecords.length} records)`);
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

    try {
      await set(cacheKey, deduped);
    } catch (cacheWriteErr) {
      console.warn('Local cache write notice:', cacheWriteErr);
    }

    return deduped;
  };

  const fetchAllData = async (season) => {
    if (fetchingRef.current) return;
    fetchingRef.current = true;
    try {
      setLoading(true);
      setLoadStatus("Checking local cache...");
      const records = await fetchRawSeason(season, setLoadStatus);
      setRawData(records || []);
    } catch (err) {
      console.error("App Error:", err);
      try {
        const cachePromise = get(`fantasy_data_${season}`);
        const cacheTimeout = new Promise((_, reject) =>
          setTimeout(() => reject(new Error('IndexedDB recovery timeout')), 2000)
        );
        const cached = await Promise.race([cachePromise, cacheTimeout]);
        if (cached?.length > 0) {
          console.warn(`Recovered ${cached.length} records from local cache following server error`);
          setRawData(cached);
          return;
        }
      } catch (cacheErr) {
        console.error("Cache recovery failed:", cacheErr);
      }
      setLoadStatus("Notice: In-season statistics could not be loaded.");
    } finally {
      fetchingRef.current = false;
      setLoading(false);
    }
  };

  // Download every season into allSeasonData without switching the active view.
  const [downloadAllProgress, setDownloadAllProgress] = useState(null);

  const downloadAllSeasons = async () => {
    const missing = AVAILABLE_SEASONS.filter(y => y <= 2026 && !allSeasonData[y]?.length);
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
    let maxPeriodId = -1;
    for (let i = 0; i < rawData.length; i++) {
      if (rawData[i].scoring_period_id > maxPeriodId) {
        maxPeriodId = rawData[i].scoring_period_id;
      }
    }
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
    setOffseasonYear(year);
  };

  const handleOffseasonYearChange = (year) => {
    setOffseasonYear(year);
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
  return (
    <div className="min-h-screen bg-gray-100 font-sans text-gray-800">
      {/* Commissioner Break-Glass Active Banner */}
      <CommishActiveBanner />

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

              {/* Right Side: Universal Season Dropdown & Controls */}
              <div className="flex items-center space-x-2">
                <div className="flex items-center gap-1.5 bg-blue-950/90 px-2.5 py-1 rounded-xl border border-blue-700/80 shadow-inner">
                  <span className="text-[11px] font-black text-blue-300 uppercase tracking-wider hidden sm:inline">Season</span>
                  <select
                    id="universal-season-dropdown"
                    value={activeGroup === 'offseason' ? offseasonYear : selectedSeason}
                    onChange={e => handleSeasonChange(parseInt(e.target.value, 10))}
                    className="bg-blue-900 hover:bg-blue-800 text-white text-xs font-bold rounded-lg px-2.5 py-1 border border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-400 cursor-pointer shadow-xs"
                    title="Select League Season"
                  >
                    {AVAILABLE_SEASONS.map(y => (
                      <option key={y} value={y}>
                        {y === 2027 ? '🚀 2027 (Upcoming)' : y === 2026 ? '⚾ 2026 (Active)' : `🏛️ ${y}`}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  onClick={handleRefresh}
                  className="p-1.5 text-blue-200 hover:text-white cursor-pointer rounded-lg hover:bg-blue-800 transition-colors"
                  title="Clear Cache & Reload"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
                </button>
                <div className="h-6 w-px bg-blue-700/60 mx-1 hidden sm:block"></div>
                <UserNavWidget />
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
          seasonYear={offseasonYear}
          onSeasonYearChange={handleOffseasonYearChange}
        />
      ) : (
        <main className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
          {loading && !OFFSEASON_VIEWS.has(currentView) ? (
            <div className="bg-white border border-gray-200 rounded-2xl p-8 text-center shadow-md max-w-lg mx-auto space-y-4 my-12">
              <div className="inline-block animate-spin rounded-full h-10 w-10 border-b-2 border-blue-900 mb-2"></div>
              <div className="font-extrabold text-gray-800 text-lg">{loadStatus}</div>
              <p className="text-gray-500 text-xs">
                Synchronizing in-season player statistics and schedule records...
              </p>
              
              {/* Recovery Action Controls */}
              <div className="pt-4 border-t border-gray-100 flex flex-col sm:flex-row gap-2 justify-center text-xs font-bold">
                <button
                  type="button"
                  onClick={handleRefresh}
                  className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-xl transition cursor-pointer shadow-xs"
                >
                  🧹 Clear Cache & Reload
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setActiveGroup('offseason');
                    setCurrentView('keepers');
                  }}
                  className="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-xl transition cursor-pointer shadow-xs"
                >
                  🚀 2027 Offseason Prep
                </button>
                {user && (
                  <button
                    type="button"
                    onClick={signOut}
                    className="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-xl transition cursor-pointer"
                  >
                    🚪 Sign Out
                  </button>
                )}
              </div>
            </div>
          ) : selectedSeason > 2026 && activeGroup === 'season' ? (
            <div className="bg-white border border-blue-200 rounded-3xl p-8 sm:p-12 text-center shadow-lg max-w-2xl mx-auto space-y-6 my-8">
              <div className="w-20 h-20 mx-auto rounded-3xl bg-gradient-to-tr from-blue-600 to-indigo-600 text-white flex items-center justify-center text-4xl shadow-md ring-8 ring-blue-50">
                🚀
              </div>
              <div>
                <span className="inline-block px-3 py-1 rounded-full bg-blue-100 text-blue-800 text-xs font-black uppercase tracking-wider mb-2">
                  Upcoming Season
                </span>
                <h2 className="text-3xl font-black text-slate-900 tracking-tight">2027 Season In Progress</h2>
                <p className="text-sm text-slate-600 mt-2 max-w-md mx-auto leading-relaxed">
                  Regular season matchups, standings, and live scoring will activate with the 2027 MLB Opening Day. 
                  Off-season draft preparation, trade negotiations, keeper selections, and draft capital ledgers are currently live!
                </p>
              </div>
              
              <div className="pt-2 flex flex-wrap items-center justify-center gap-3 text-xs font-black">
                <button
                  type="button"
                  onClick={() => {
                    setActiveGroup('offseason');
                    setCurrentView('capital');
                  }}
                  className="px-5 py-2.5 bg-teal-600 hover:bg-teal-700 text-white rounded-xl shadow-md transition cursor-pointer flex items-center gap-1.5"
                >
                  <span>🎟️</span>
                  <span>2027 Draft Capital & Trades</span>
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setActiveGroup('offseason');
                    setCurrentView('keepers');
                  }}
                  className="px-5 py-2.5 bg-emerald-600 hover:bg-emerald-700 text-white rounded-xl shadow-md transition cursor-pointer flex items-center gap-1.5"
                >
                  <span>💎</span>
                  <span>Keepers & Budget Simulator</span>
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setActiveGroup('offseason');
                    setCurrentView('draft');
                  }}
                  className="px-5 py-2.5 bg-amber-600 hover:bg-amber-700 text-white rounded-xl shadow-md transition cursor-pointer flex items-center gap-1.5"
                >
                  <span>🎯</span>
                  <span>Draft War Room</span>
                </button>
              </div>
            </div>
          ) : (
            <>
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
              seasonYear={offseasonYear}
              onSeasonYearChange={handleOffseasonYearChange}
              onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
              onOwnerClick={(team) => setSelectedOwner(team)}
            />
          )}
          {currentView === 'pickem' && (
            <PickemView
              initialSeason={offseasonYear}
              onSeasonChange={handleOffseasonYearChange}
            />
          )}
          {currentView === 'capital' && (
            <DraftCapitalView
              currentUser={effectiveOwner || 'Daniel'}
              isCommissioner={isCommissioner}
              draftYear={offseasonYear}
              onDraftYearChange={handleOffseasonYearChange}
            />
          )}
              {currentView === 'keepers' && (
                <KeepersBudgetsView
                  currentUser={effectiveOwner || 'Daniel'}
                  isCommissioner={isCommissioner}
                  seasonYear={offseasonYear}
                  onSeasonYearChange={handleOffseasonYearChange}
                  onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
                />
              )}
            </>
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