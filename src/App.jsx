import { useEffect, useState, useMemo, lazy, Suspense } from 'react';

import { generateSchedule } from './schedule';
import {
  AVAILABLE_SEASONS,
  OFFSEASON_VIEWS,
  getViewFromHash,
  getSubTabsFromHash,
  getHashForRoute,
} from './routes';
import { useSeasonData } from './hooks/useSeasonData';
import { useProcessedSchedule } from './hooks/useProcessedSchedule';

import UserNavWidget from './components/UserNavWidget';
import CommishActiveBanner from './components/CommishActiveBanner';
import { useAuth } from './context/useAuth';

// Lazy-loaded Views for code splitting and lightweight initial bundle
const WeeklyView = lazy(() => import('./views/WeeklyView'));
const SummaryView = lazy(() => import('./views/SummaryView'));
const TeamsView = lazy(() => import('./views/TeamsView'));
const PlayersView = lazy(() => import('./views/PlayersView'));
const ProgressionView = lazy(() => import('./views/ProgressionView'));
const HighlightsView = lazy(() => import('./views/HighlightsView'));
const OwnerDisparitiesView = lazy(() => import('./views/OwnerDisparitiesView'));
const OwnerDetailModal = lazy(() => import('./components/OwnerDetailModal'));
const PlayerHistoryModal = lazy(() => import('./components/PlayerHistoryModal'));
const LiveScoreboardView = lazy(() => import('./views/LiveScoreboardView'));
const TransactionsView = lazy(() => import('./views/TransactionsView'));
const TradeRepositoryView = lazy(() => import('./views/TradeRepositoryView'));
const DraftRoomView = lazy(() => import('./views/DraftRoomView'));
const PlayerValuationsView = lazy(() => import('./views/PlayerValuationsView'));
const PickemView = lazy(() => import('./views/PickemView'));
const DraftCapitalView = lazy(() => import('./views/DraftCapitalView'));
const KeepersBudgetsView = lazy(() => import('./views/KeepersBudgetsView'));
const OwnerLandingView = lazy(() => import('./views/OwnerLandingView'));
const DraftHistoryView = lazy(() => import('./views/DraftHistoryView'));
const FullRosterView = lazy(() => import('./views/FullRosterView'));
const LeagueHistoryView = lazy(() => import('./views/LeagueHistoryView'));
const PlayerOwnerStatsView = lazy(() => import('./views/PlayerOwnerStatsView'));

function ViewLoadingFallback() {
  return (
    <div className="flex flex-col items-center justify-center min-h-[380px] py-16 space-y-4">
      <div className="w-10 h-10 border-4 border-slate-700 border-t-blue-500 rounded-full animate-spin"></div>
      <p className="text-xs font-semibold text-slate-400 uppercase tracking-widest animate-pulse">Loading View...</p>
    </div>
  );
}



function App() {
  const { user, signOut, effectiveOwner, isCommissioner } = useAuth();
  const [currentView, setCurrentView] = useState(getViewFromHash);
  const [keepersSubTab, setKeepersSubTab] = useState(() => getSubTabsFromHash().keepersSubTab);
  const [draftSubTab, setDraftSubTab] = useState(() => getSubTabsFromHash().draftSubTab);
  const [teamsSubTab, setTeamsSubTab] = useState(() => getSubTabsFromHash().teamsSubTab);
  const [playerOwnerMode, setPlayerOwnerMode] = useState(() => getSubTabsFromHash().playerOwnerMode);
  const [historySubTab, setHistorySubTab] = useState(() => getSubTabsFromHash().historySubTab);
  const [openDropdown, setOpenDropdown] = useState(null); // 'teams' | 'players' | 'keepers' | 'draft' | null

  const [selectedSeason, setSelectedSeason] = useState(2026);
  const [offseasonYear, setOffseasonYear] = useState(2027);

  const [selectedOwner, setSelectedOwner] = useState(null);
  const [selectedPlayer, setSelectedPlayer] = useState(null);

  // Group is cleanly derived from the active view (avoids cascading state effects)
  const activeGroup = OFFSEASON_VIEWS.has(currentView) ? 'offseason' : 'season';

  // Season data fetching & caching hook
  const {
    rawData,
    setRawData,
    loading,
    loadStatus,
    allSeasonData,
    downloadAllProgress,
    todaysRecords,
    loadSeason,
    downloadAllSeasons,
    refreshSeasonData,
  } = useSeasonData(selectedSeason);

  // Close dropdown on outside click
  useEffect(() => {
    const handlePointerDown = (e) => {
      if (!e.target.closest('[data-dropdown]')) {
        setOpenDropdown(null);
      }
    };
    document.addEventListener('pointerdown', handlePointerDown);
    return () => document.removeEventListener('pointerdown', handlePointerDown);
  }, []);

  // Synchronize browser URL hash with currentView and active subtabs
  useEffect(() => {
    if (window.location.hash && (window.location.hash.includes('access_token=') || window.location.hash.includes('error='))) {
      return;
    }
    const targetHash = getHashForRoute(currentView, {
      keepersSubTab,
      draftSubTab,
      teamsSubTab,
      historySubTab,
    });
    if (window.location.hash !== targetHash) {
      window.location.hash = targetHash;
    }
  }, [currentView, keepersSubTab, draftSubTab, teamsSubTab, historySubTab]);

  const handleGroupChange = (group) => {
    setOpenDropdown(null);
    if (group === 'season' && OFFSEASON_VIEWS.has(currentView)) {
      setCurrentView('dashboard');
    } else if (group === 'offseason' && !OFFSEASON_VIEWS.has(currentView)) {
      setCurrentView('keepers');
      setKeepersSubTab('matrix');
    }
  };

  // Listen to browser Back/Forward and direct hash navigation
  useEffect(() => {
    const handleHashChange = () => {
      const newView = getViewFromHash();
      const { keepersSubTab: kTab, draftSubTab: dTab, teamsSubTab: tTab, playerOwnerMode: poMode, historySubTab: hTab } = getSubTabsFromHash();
      setCurrentView((prev) => (prev !== newView ? newView : prev));
      setKeepersSubTab(kTab);
      setDraftSubTab(dTab);
      setTeamsSubTab(tTab);
      setPlayerOwnerMode(poMode);
      setHistorySubTab(hTab);
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  const baseSchedule = useMemo(() => generateSchedule(), []);

  const handleRefresh = async () => {
    await refreshSeasonData();
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


  const processedWeeks = useProcessedSchedule(rawData, baseSchedule);

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

            {/* Bottom Bar: Categorized Navigation for Active Group */}
            <div className="flex items-center space-x-2 py-2 overflow-x-visible text-xs">
              {activeGroup === 'season' ? (
                <>
                  {/* DASHBOARD (HOME) */}
                  <a
                    href="#/dashboard"
                    onClick={(e) => { e.preventDefault(); setCurrentView('dashboard'); setOpenDropdown(null); }}
                    className={`px-3.5 py-1.5 rounded-xl font-black flex items-center gap-1.5 transition-all cursor-pointer ${
                      currentView === 'dashboard'
                        ? 'bg-blue-600 text-white shadow-md ring-1 ring-blue-400'
                        : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="Personalized Owner Landing Page"
                  >
                    <span>🏠</span>
                    <span>Dashboard</span>
                  </a>

                  {/* TEAMS DROPDOWN */}
                  <div className="relative" data-dropdown="teams">
                    <button
                      type="button"
                      onClick={() => setOpenDropdown(prev => prev === 'teams' ? null : 'teams')}
                      className={`px-3.5 py-1.5 rounded-xl font-black flex items-center gap-1.5 transition-all cursor-pointer ${
                        ['teams', 'rosters', 'summary', 'weekly', 'progression', 'highlights', 'history'].includes(currentView)
                          ? 'bg-blue-600 text-white shadow-md ring-1 ring-blue-400'
                          : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                      }`}
                      title="Teams Category"
                    >
                      <span>👥</span>
                      <span>Teams</span>
                      <span className={`text-[9px] transition-transform duration-200 ${openDropdown === 'teams' ? 'rotate-180' : ''}`}>▼</span>
                    </button>

                    {openDropdown === 'teams' && (
                      <div className="absolute top-full left-0 mt-1.5 min-w-[220px] bg-slate-900/95 border border-slate-700/80 rounded-2xl shadow-2xl backdrop-blur-xl p-1.5 z-50 animate-fade-in flex flex-col gap-0.5">
                        <a
                          href="#/rosters"
                          onClick={(e) => { e.preventDefault(); setCurrentView('rosters'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'rosters' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📋</span><span>Lineups & Rosters</span></span>
                          {currentView === 'rosters' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/teams"
                          onClick={(e) => { e.preventDefault(); setCurrentView('teams'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'teams' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📊</span><span>Roto</span></span>
                          {currentView === 'teams' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/standings"
                          onClick={(e) => { e.preventDefault(); setCurrentView('summary'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'summary' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🏆</span><span>H2H Standings</span></span>
                          {currentView === 'summary' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/matchups"
                          onClick={(e) => { e.preventDefault(); setCurrentView('weekly'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'weekly' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>⚔️</span><span>Matchups</span></span>
                          {currentView === 'weekly' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/progression"
                          onClick={(e) => { e.preventDefault(); setCurrentView('progression'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'progression' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📈</span><span>Progression</span></span>
                          {currentView === 'progression' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/highlights"
                          onClick={(e) => { e.preventDefault(); setCurrentView('highlights'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'highlights' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>⭐</span><span>Highlights</span></span>
                          {currentView === 'highlights' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/history/standings"
                          onClick={(e) => { e.preventDefault(); setCurrentView('history'); setHistorySubTab('standings'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'history' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🏛️</span><span>League History</span></span>
                          {currentView === 'history' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                      </div>
                    )}
                  </div>

                  {/* PLAYERS DROPDOWN */}
                  <div className="relative" data-dropdown="players">
                    <button
                      type="button"
                      onClick={() => setOpenDropdown(prev => prev === 'players' ? null : 'players')}
                      className={`px-3.5 py-1.5 rounded-xl font-black flex items-center gap-1.5 transition-all cursor-pointer ${
                        ['players', 'transactions', 'trades', 'disparities', 'player-owners'].includes(currentView)
                          ? 'bg-blue-600 text-white shadow-md ring-1 ring-blue-400'
                          : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                      }`}
                      title="Players Category"
                    >
                      <span>⚾</span>
                      <span>Players</span>
                      <span className={`text-[9px] transition-transform duration-200 ${openDropdown === 'players' ? 'rotate-180' : ''}`}>▼</span>
                    </button>

                    {openDropdown === 'players' && (
                      <div className="absolute top-full left-0 mt-1.5 min-w-[210px] bg-slate-900/95 border border-slate-700/80 rounded-2xl shadow-2xl backdrop-blur-xl p-1.5 z-50 animate-fade-in flex flex-col gap-0.5">
                        <a
                          href="#/players"
                          onClick={(e) => { e.preventDefault(); setCurrentView('players'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'players' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📊</span><span>Stats</span></span>
                          {currentView === 'players' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/player-owners"
                          onClick={(e) => { e.preventDefault(); setCurrentView('player-owners'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'player-owners' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>👤</span><span>Player-Owner Stats</span></span>
                          {currentView === 'player-owners' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/transactions"
                          onClick={(e) => { e.preventDefault(); setCurrentView('transactions'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'transactions' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📋</span><span>Transactions</span></span>
                          {currentView === 'transactions' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/trades"
                          onClick={(e) => { e.preventDefault(); setCurrentView('trades'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'trades' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🤝</span><span>Trade Repository</span></span>
                          {currentView === 'trades' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                        <a
                          href="#/disparities"
                          onClick={(e) => { e.preventDefault(); setCurrentView('disparities'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'disparities' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>⚖️</span><span>Disparities</span></span>
                          {currentView === 'disparities' && <span className="text-[10px] text-blue-200">●</span>}
                        </a>
                      </div>
                    )}
                  </div>

                  {/* FANTASYCAST LIVE */}
                  <a
                    href="#/fantasycast"
                    onClick={(e) => { e.preventDefault(); setCurrentView('fantasycast'); setOpenDropdown(null); }}
                    className={`px-3 py-1.5 rounded-xl font-bold flex items-center gap-1.5 transition-colors ${
                      currentView === 'fantasycast' ? 'bg-red-700 text-white shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                  >
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-300 opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
                    </span>
                    <span>FantasyCast</span>
                  </a>
                </>
              ) : (
                <>
                  {/* KEEPERS DROPDOWN */}
                  <div className="relative" data-dropdown="keepers">
                    <button
                      type="button"
                      onClick={() => setOpenDropdown(prev => prev === 'keepers' ? null : 'keepers')}
                      className={`px-3.5 py-1.5 rounded-xl font-black flex items-center gap-1.5 transition-all cursor-pointer ${
                        currentView === 'keepers' || currentView === 'valuations'
                          ? 'bg-emerald-600 text-white shadow-md ring-1 ring-emerald-400'
                          : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                      }`}
                      title="Keepers Category"
                    >
                      <span>💎</span>
                      <span>Keepers</span>
                      <span className={`text-[9px] transition-transform duration-200 ${openDropdown === 'keepers' ? 'rotate-180' : ''}`}>▼</span>
                    </button>

                    {openDropdown === 'keepers' && (
                      <div className="absolute top-full left-0 mt-1.5 min-w-[210px] bg-slate-900/95 border border-slate-700/80 rounded-2xl shadow-2xl backdrop-blur-xl p-1.5 z-50 animate-fade-in flex flex-col gap-0.5">
                        <a
                          href="#/keepers/summary"
                          onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); setKeepersSubTab('matrix'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'keepers' && keepersSubTab === 'matrix' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📊</span><span>Keeper Summary</span></span>
                          {currentView === 'keepers' && keepersSubTab === 'matrix' && <span className="text-[10px] text-emerald-200">●</span>}
                        </a>
                        <a
                          href="#/keepers/rosters"
                          onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); setKeepersSubTab('rosters'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'keepers' && keepersSubTab === 'rosters' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📋</span><span>Rosters</span></span>
                          {currentView === 'keepers' && keepersSubTab === 'rosters' && <span className="text-[10px] text-emerald-200">●</span>}
                        </a>
                        <a
                          href="#/keepers/comp-picks"
                          onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); setKeepersSubTab('simulator'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'keepers' && keepersSubTab === 'simulator' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🎲</span><span>Comp Pick Submission</span></span>
                          {currentView === 'keepers' && keepersSubTab === 'simulator' && <span className="text-[10px] text-emerald-200">●</span>}
                        </a>
                        <a
                          href="#/keepers/planner"
                          onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); setKeepersSubTab('planner'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'keepers' && keepersSubTab === 'planner' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📋</span><span>Keeper Submission</span></span>
                          {currentView === 'keepers' && keepersSubTab === 'planner' && <span className="text-[10px] text-emerald-200">●</span>}
                        </a>
                        <a
                          href="#/keeper-calculations"
                          onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); setKeepersSubTab('calculations'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'keepers' && keepersSubTab === 'calculations' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🏷️</span><span>Keeper Prices</span></span>
                          {currentView === 'keepers' && keepersSubTab === 'calculations' && <span className="text-[10px] text-emerald-200">●</span>}
                        </a>
                        <a
                          href="#/keepers/settings"
                          onClick={(e) => { e.preventDefault(); setCurrentView('keepers'); setKeepersSubTab('settings'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'keepers' && keepersSubTab === 'settings' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>⚙️</span><span>League Settings</span></span>
                          {currentView === 'keepers' && keepersSubTab === 'settings' && <span className="text-[10px] text-emerald-200">●</span>}
                        </a>
                      </div>
                    )}
                  </div>

                  {/* DRAFT & TRADES DROPDOWN */}
                  <div className="relative" data-dropdown="draft">
                    <button
                      type="button"
                      onClick={() => setOpenDropdown(prev => prev === 'draft' ? null : 'draft')}
                      className={`px-3.5 py-1.5 rounded-xl font-black flex items-center gap-1.5 transition-all cursor-pointer ${
                        ['capital', 'drafthistory', 'draft', 'history'].includes(currentView)
                          ? 'bg-amber-600 text-white shadow-md ring-1 ring-amber-400'
                          : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                      }`}
                      title="Draft and Trades"
                    >
                      <span>🎯</span>
                      <span>Draft and Trades</span>
                      <span className={`text-[9px] transition-transform duration-200 ${openDropdown === 'draft' ? 'rotate-180' : ''}`}>▼</span>
                    </button>

                    {openDropdown === 'draft' && (
                      <div className="absolute top-full left-0 mt-1.5 min-w-[210px] bg-slate-900/95 border border-slate-700/80 rounded-2xl shadow-2xl backdrop-blur-xl p-1.5 z-50 animate-fade-in flex flex-col gap-0.5">
                        <a
                          href="#/draft/traded-board"
                          onClick={(e) => { e.preventDefault(); setCurrentView('capital'); setDraftSubTab('board'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'capital' && draftSubTab === 'board' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🎟️</span><span>Traded Board</span></span>
                          {currentView === 'capital' && draftSubTab === 'board' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/draft/ledgers"
                          onClick={(e) => { e.preventDefault(); setCurrentView('capital'); setDraftSubTab('ledgers'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'capital' && draftSubTab === 'ledgers' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📒</span><span>Owner Ledgers</span></span>
                          {currentView === 'capital' && draftSubTab === 'ledgers' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/draft/trade-proposals"
                          onClick={(e) => { e.preventDefault(); setCurrentView('capital'); setDraftSubTab('proposals'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'capital' && draftSubTab === 'proposals' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🤝</span><span>Trade Proposals</span></span>
                          {currentView === 'capital' && draftSubTab === 'proposals' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/draft/trade-history"
                          onClick={(e) => { e.preventDefault(); setCurrentView('capital'); setDraftSubTab('history'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'capital' && draftSubTab === 'history' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📜</span><span>Traded Assets Log</span></span>
                          {currentView === 'capital' && draftSubTab === 'history' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/draft/2026-board"
                          onClick={(e) => { e.preventDefault(); setCurrentView('capital'); setDraftSubTab('2026board'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'capital' && draftSubTab === '2026board' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🏛️</span><span>2026 Ground Truth</span></span>
                          {currentView === 'capital' && draftSubTab === '2026board' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/draft/history"
                          onClick={(e) => { e.preventDefault(); setCurrentView('drafthistory'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'drafthistory' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>📋</span><span>Draft History Recap</span></span>
                          {currentView === 'drafthistory' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/history/standings"
                          onClick={(e) => { e.preventDefault(); setCurrentView('history'); setHistorySubTab('standings'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'history' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🏛️</span><span>Historical Finishes</span></span>
                          {currentView === 'history' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                        <a
                          href="#/draft/lobby"
                          onClick={(e) => { e.preventDefault(); setCurrentView('draft'); setOpenDropdown(null); }}
                          className={`px-3 py-2 rounded-xl text-xs font-black flex items-center justify-between transition cursor-pointer ${
                            currentView === 'draft' ? 'bg-amber-600 text-white shadow-xs' : 'text-slate-300 hover:bg-slate-800 hover:text-white'
                          }`}
                        >
                          <span className="flex items-center gap-2"><span>🎯</span><span>Draft Lobby</span></span>
                          {currentView === 'draft' && <span className="text-[10px] text-amber-200">●</span>}
                        </a>
                      </div>
                    )}
                  </div>

                  {/* PICK'EM */}
                  <a
                    href="#/pickem"
                    onClick={(e) => { e.preventDefault(); setCurrentView('pickem'); setOpenDropdown(null); }}
                    className={`px-3.5 py-1.5 rounded-xl font-bold flex items-center gap-1.5 transition-colors cursor-pointer ${
                      currentView === 'pickem' ? 'bg-purple-700 text-white ring-1 ring-purple-400 shadow-xs' : 'text-blue-200 hover:bg-blue-800/80 hover:text-white'
                    }`}
                    title="Annual MLB Pick'em"
                  >
                    <span>🔮</span>
                    <span>Pick&apos;em</span>
                  </a>
                </>
              )}
            </div>
          </div>
        </nav>
      )}

      {currentView === 'draft' ? (
        <Suspense fallback={<ViewLoadingFallback />}>
          <DraftRoomView
            onOpenPlayerModal={(id, name) => setSelectedPlayer({ id, name })}
            onSwitchView={(view) => setCurrentView(view)}
            seasonYear={offseasonYear}
            onSeasonYearChange={handleOffseasonYearChange}
          />
        </Suspense>
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
            <Suspense fallback={<ViewLoadingFallback />}>
              {currentView === 'dashboard' && (
                <OwnerLandingView
                  allStats={rawData}
                  processedWeeks={processedWeeks}
                  selectedSeason={selectedSeason}
                  onOwnerClick={(team) => setSelectedOwner(team)}
                  onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
                  onNavigate={(view, subTab) => {
                    if (subTab) {
                      if (view === 'keepers') setKeepersSubTab(subTab);
                      if (view === 'capital') setDraftSubTab(subTab);
                      if (view === 'teams') setTeamsSubTab(subTab);
                    }
                    setCurrentView(view);
                  }}
                />
              )}
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
                  initialViewMode={teamsSubTab}
                  initialTeamId={selectedOwner?.id || 2}
                  onOwnerClick={(team) => setSelectedOwner(team)}
                  onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
                />
              )}
              {currentView === 'rosters' && (
                <FullRosterView
                  allStats={rawData}
                  selectedSeason={selectedSeason}
                  onOwnerClick={(team) => setSelectedOwner(team)}
                  onPlayerClick={(id, name, teamId) => setSelectedPlayer({ id, name, teamId })}
                />
              )}
              {currentView === 'transactions' && (
                <TransactionsView
                  onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
                  onOwnerClick={(team) => setSelectedOwner(team)}
                />
              )}
              {currentView === 'trades' && (
                <TradeRepositoryView
                  allStats={rawData}
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
              {currentView === 'player-owners' && (
                <PlayerOwnerStatsView
                  initialMode={playerOwnerMode || 'season'}
                  onPlayerClick={(p) => setSelectedPlayer(p)}
                  onOwnerClick={(team) => setSelectedOwner(team)}
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
                  onLoadSeason={loadSeason}
                />
              )}
              {currentView === 'history' && (
                <LeagueHistoryView
                  selectedSeason={selectedSeason}
                  onOwnerClick={(team) => setSelectedOwner(team)}
                  subTab={historySubTab}
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
                  subTab={draftSubTab}
                />
              )}
              {currentView === 'drafthistory' && (
                <DraftHistoryView
                  onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
                  onOwnerClick={(team) => setSelectedOwner(team)}
                />
              )}
              {currentView === 'keepers' && (
                <KeepersBudgetsView
                  currentUser={effectiveOwner || 'Daniel'}
                  isCommissioner={isCommissioner}
                  seasonYear={offseasonYear}
                  onSeasonYearChange={handleOffseasonYearChange}
                  onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
                  subTab={keepersSubTab}
                />
              )}
            </Suspense>
          )}
        </main>
      )}

      {/* --- MODAL LAYER --- */}
      {selectedOwner && (
        <Suspense fallback={null}>
          <OwnerDetailModal
            team={selectedOwner}
            allStats={rawData}
            onClose={() => setSelectedOwner(null)}
            onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
          />
        </Suspense>
      )}

      {selectedPlayer && (
        <div style={{ zIndex: 90, position: 'relative' }}>
          <Suspense fallback={null}>
            <PlayerHistoryModal
              playerId={selectedPlayer.id}
              playerName={selectedPlayer.name}
              teamId={selectedPlayer.teamId}
              allStats={rawData}
              selectedSeason={selectedSeason}
              onClose={() => setSelectedPlayer(null)}
            />
          </Suspense>
        </div>
      )}
    </div>
  );
}

export default App;