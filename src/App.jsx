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

const AVAILABLE_SEASONS = Array.from({ length: 2026 - 2012 + 1 }, (_, i) => 2026 - i);

function App() {
  const [loading, setLoading] = useState(true);
  const [currentView, setCurrentView] = useState('weekly');
  const [rawData, setRawData] = useState([]);
  const [loadStatus, setLoadStatus] = useState("Initializing...");
  const [selectedSeason, setSelectedSeason] = useState(2026);

  const [selectedOwner, setSelectedOwner] = useState(null);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [allSeasonData, setAllSeasonData] = useState({});

  const baseSchedule = useMemo(() => generateSchedule(), []);

  const cacheKey = `fantasy_data_${selectedSeason}`;

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
      season_year:       row.season_year,
      team_id:           row.team_id,
      scoring_period_id: row.scoring_period_id,
      player_id:         row.id,
      full_name:         row.fullName,
      lineup_slot_id:    row.lineupSlotID != null ? parseInt(row.lineupSlotID) : null,
      stats,
    };
  };

  // Core fetch: check cache → Supabase. Returns normalized records, touches no state.
  const fetchRawSeason = async (season, onProgress) => {
    const cacheKey = `fantasy_data_${season}`;
    const cached = await get(cacheKey);
    if (cached?.length > 0) return cached;

    const isHistorical = season < 2026;
    const tableName    = isHistorical ? 'historical_data' : 'player_daily_stats';
    let allRecords = [];
    let page = 0;
    const pageSize = 1000;
    let hasMore = true;

    while (hasMore) {
      let query = supabase
        .from(tableName)
        .select('*')
        .eq('season_year', season)
        .range(page * pageSize, (page + 1) * pageSize - 1);
      if (!isHistorical) query = query.order('id', { ascending: true });

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
      setLoadStatus("Error loading data.");
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
    const standingsAfter23 = calculateStandings(resolvedSchedule, 23);
    const playoffSeeds = standingsAfter23.slice(0, 8); 

    if (baseSchedule[23]) {
      const week24 = baseSchedule[23]; 
      const sfMatchups = [
        { id: 'sf1', homeTeamId: playoffSeeds[0]?.id, awayTeamId: playoffSeeds[3]?.id, label: "Semi-Final A" },
        { id: 'sf2', homeTeamId: playoffSeeds[1]?.id, awayTeamId: playoffSeeds[2]?.id, label: "Semi-Final B" },
        { id: 'c1', homeTeamId: playoffSeeds[4]?.id, awayTeamId: playoffSeeds[7]?.id, label: "Consolation A" },
        { id: 'c2', homeTeamId: playoffSeeds[5]?.id, awayTeamId: playoffSeeds[6]?.id, label: "Consolation B" }
      ];

      const resolvedSF = sfMatchups.map(m => resolveMatchupStats(m, week24));
      resolvedSchedule.push({ ...week24, matchups: resolvedSF });
    }

    if (baseSchedule[24]) {
      const week25 = baseSchedule[24];
      const prevWeek = resolvedSchedule[23]; 
      
      const getWinner = (matchId) => {
        const m = prevWeek?.matchups.find(pm => pm.id === matchId || pm.matchupId === matchId);
        if (!m || !m.result) return "TBD";
        return m.result.homeScore > m.result.awayScore ? m.homeTeam.id : m.awayTeam.id;
      };

      const getLoser = (matchId) => {
        const m = prevWeek?.matchups.find(pm => pm.id === matchId || pm.matchupId === matchId);
        if (!m || !m.result) return "TBD";
        return m.result.homeScore > m.result.awayScore ? m.awayTeam.id : m.homeTeam.id;
      };

      const finalMatchups = [
        { id: 'final', homeTeamId: getWinner('sf1'), awayTeamId: getWinner('sf2'), label: "🏆 Championship" },
        { id: '3rd', homeTeamId: getLoser('sf1'), awayTeamId: getLoser('sf2'), label: "3rd Place Match" }
      ];

      const resolvedFinals = finalMatchups.map(m => resolveMatchupStats(m, week25));
      resolvedSchedule.push({ ...week25, matchups: resolvedFinals });
    }

    return resolvedSchedule;

  }, [rawData, baseSchedule]);


  // --- RENDER ---
  if (loading) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-gray-50 text-gray-500 gap-4">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-900"></div>
        <div className="font-bold animate-pulse">{loadStatus}</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100 font-sans text-gray-800">
      <nav className="bg-blue-900 text-white shadow-lg sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="font-black text-xl tracking-wider">FANTASY LEAGUE</div>
            <div className="flex items-center space-x-4">
              <button onClick={() => setCurrentView('weekly')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'weekly' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Matchups</button>
              <button onClick={() => setCurrentView('summary')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'summary' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Standings</button>
              <button onClick={() => setCurrentView('teams')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'teams' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Teams</button>
              <button onClick={() => setCurrentView('players')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'players' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Players</button>
              <button onClick={() => setCurrentView('disparities')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'disparities' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Disparities</button>
              <button onClick={() => setCurrentView('progression')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'progression' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Progression</button>
              <button onClick={() => setCurrentView('highlights')} className={`px-3 py-2 rounded text-sm font-bold ${currentView === 'highlights' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}>Highlights</button>
              <button onClick={() => setCurrentView('fantasycast')} className={`px-3 py-2 rounded text-sm font-bold flex items-center gap-2 ${currentView === 'fantasycast' ? 'bg-red-700' : 'hover:bg-blue-800'}`}>
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-300 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
                </span>
                FantasyCast
              </button>
             
              <select
                value={selectedSeason}
                onChange={e => handleSeasonChange(parseInt(e.target.value))}
                className="ml-4 bg-blue-800 text-white text-sm font-bold rounded px-2 py-1 border border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-400 cursor-pointer"
              >
                {AVAILABLE_SEASONS.map(y => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
              <button onClick={handleRefresh} className="ml-2 p-2 text-blue-200 hover:text-white" title="Clear Cache & Reload">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
              </button>
            </div>
          </div>
        </div>
      </nav>

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
      </main>

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
             onClose={() => setSelectedPlayer(null)}
           />
         </div>
      )}
    </div>
  );
}

export default App;