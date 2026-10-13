/**
 * @file PickemHeader.jsx
 * @description Top hero banner, season switcher, live projections toggle, alerts,
 * and tab navigation controls for PickemView.
 */

import React from 'react';

export default function PickemHeader({
  selectedSeason,
  onSeasonChange,
  seasons = [],
  isUpcoming,
  isInProgress,
  questionsCount = 23,
  showLiveProjections,
  setShowLiveProjections,
  liveProjectionsData,
  refreshingLive,
  onRefreshLiveStandings,
  onOpenMethodology,
  saveSuccess,
  errorMessage,
  activeTab,
  setActiveTab,
  totalPicks = 0,
  totalOwners = 0,
}) {
  return (
    <div className="space-y-6">
      {/* HEADER BANNER */}
      <div className="bg-gradient-to-r from-slate-900 via-indigo-950 to-slate-900 border border-indigo-800/40 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div className="absolute -right-12 -bottom-12 w-64 h-64 bg-indigo-500/10 rounded-full blur-3xl pointer-events-none"></div>
        <div className="absolute -left-12 -top-12 w-64 h-64 bg-purple-500/10 rounded-full blur-3xl pointer-events-none"></div>

        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <span className="text-3xl">🔮</span>
              <h1 className="text-2xl sm:text-3xl font-black tracking-tight text-white">
                Annual MLB Pick'em Championship
              </h1>
            </div>
            <p className="text-indigo-200/80 text-sm mt-1 max-w-2xl">
              League forecasting war room. Predict division champions, wild cards, pennants, World Series, award winners, and extreme win totals to earn auction draft budget cash!
            </p>
          </div>

          {/* Quick Season Switcher & Live Tracker Controls */}
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2 bg-slate-950/70 p-2 rounded-xl border border-indigo-900/60 shadow-inner">
              <span className="text-xs font-bold uppercase tracking-wider text-indigo-300 pl-2">Season:</span>
              <select
                value={selectedSeason}
                onChange={(e) => onSeasonChange?.(parseInt(e.target.value, 10))}
                className="bg-indigo-950 text-white font-bold text-sm px-3 py-1.5 rounded-lg border border-indigo-700/60 focus:outline-none focus:ring-2 focus:ring-indigo-400 cursor-pointer"
              >
                {seasons.map(s => (
                  <option key={s.season_year} value={s.season_year}>
                    {s.season_year} {s.season_year === 2027 ? '🚀 (Open Entry)' : s.season_year === 2026 ? '⚡ (Live Tracker)' : '🏆 (Final)'}
                  </option>
                ))}
              </select>
            </div>

            {/* In-Progress Live Tracker Mode Switch */}
            {isInProgress && liveProjectionsData && (
              <button
                onClick={() => setShowLiveProjections(!showLiveProjections)}
                className={`px-3.5 py-2 rounded-xl text-xs font-bold flex items-center gap-2 transition-all cursor-pointer shadow-md ${
                  showLiveProjections
                    ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 ring-2 ring-amber-300 font-black animate-pulse'
                    : 'bg-slate-800 text-slate-300 hover:bg-slate-700 border border-slate-700'
                }`}
                title="Toggle between Live In-Progress Standings and Settled Results"
              >
                <span>⚡</span>
                <span>{showLiveProjections ? 'Live In-Progress Standings: ON' : 'Show Live Projections'}</span>
              </button>
            )}
          </div>
        </div>

        {/* Season Badges & Rule Summary */}
        <div className="relative z-10 mt-5 pt-4 border-t border-indigo-800/30 flex flex-wrap items-center justify-between gap-3 text-xs">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-300">Status:</span>
            {isUpcoming ? (
              <span className="px-2.5 py-1 rounded-full bg-emerald-950/80 border border-emerald-500/40 text-emerald-300 font-bold flex items-center gap-1.5 animate-pulse">
                <span className="w-2 h-2 rounded-full bg-emerald-400"></span> Open for Owner Entries
              </span>
            ) : isInProgress ? (
              <span className="px-2.5 py-1 rounded-full bg-amber-950/80 border border-amber-500/40 text-amber-300 font-bold flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-amber-400 animate-ping"></span> Live 2026 In-Progress (YTD MLB Standings + Projected WAR)
              </span>
            ) : (
              <span className="px-2.5 py-1 rounded-full bg-indigo-950/80 border border-indigo-500/40 text-indigo-300 font-bold">
                ✓ Season Completed & Settled
              </span>
            )}
            <span className="text-slate-500 mx-1">•</span>
            <span className="text-slate-400">Questions:</span>
            <span className="text-slate-200 font-medium">
              {questionsCount} Categories
            </span>
          </div>

          {/* Quick Refresh & Methodology Links */}
          <div className="flex items-center gap-2">
            {isInProgress && (
              <>
                <button
                  onClick={onRefreshLiveStandings}
                  disabled={refreshingLive}
                  className="px-2.5 py-1 rounded bg-indigo-900/60 hover:bg-indigo-800 border border-indigo-700/60 text-indigo-200 text-xs font-semibold flex items-center gap-1.5 transition-colors cursor-pointer disabled:opacity-50"
                  title="Fetch live MLB standings directly from MLB Stats API"
                >
                  <span className={refreshingLive ? 'animate-spin' : ''}>🔄</span>
                  <span>{refreshingLive ? 'Refreshing...' : 'Refresh MLB Standings'}</span>
                </button>
                <button
                  onClick={onOpenMethodology}
                  className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold transition-colors cursor-pointer"
                >
                  ℹ️ Projections Methodology
                </button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* TOAST / ALERTS */}
      {saveSuccess && (
        <div className="bg-emerald-900/60 border border-emerald-500/50 text-emerald-200 p-4 rounded-xl flex items-center gap-3 shadow-lg animate-fade-in">
          <span className="text-2xl">🎉</span>
          <div className="font-semibold text-sm">{saveSuccess}</div>
        </div>
      )}
      {errorMessage && (
        <div className="bg-rose-950/60 border border-rose-500/50 text-rose-200 p-4 rounded-xl flex items-center gap-3 shadow-lg animate-fade-in">
          <span className="text-2xl">⚠️</span>
          <div className="font-semibold text-sm">{errorMessage}</div>
        </div>
      )}

      {/* NAVIGATION TABS */}
      <div className="flex items-center justify-between border-b border-slate-700/60 pb-3">
        <div className="flex items-center gap-2 overflow-x-auto pb-1">
          <button
            onClick={() => setActiveTab('board')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'board'
                ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30 ring-2 ring-indigo-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>📊</span>
            <span>Leaderboard & Picks</span>
          </button>

          <button
            onClick={() => setActiveTab('entry')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'entry'
                ? 'bg-emerald-600 text-white shadow-md shadow-emerald-600/30 ring-2 ring-emerald-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>✍️</span>
            <span>Submit / Edit Picks</span>
            {selectedSeason === 2027 && <span className="w-2 h-2 rounded-full bg-emerald-300 animate-ping"></span>}
          </button>

          <button
            onClick={() => setActiveTab('history')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'history'
                ? 'bg-amber-600 text-white shadow-md shadow-amber-600/30 ring-2 ring-amber-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>🏛️</span>
            <span>Hall of Fame (2016-2026)</span>
          </button>

          <button
            onClick={() => setActiveTab('admin')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'admin'
                ? 'bg-purple-600 text-white shadow-md shadow-purple-600/30 ring-2 ring-purple-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>⚙️</span>
            <span>Commissioner Tools</span>
          </button>
        </div>

        {/* Total picks indicator */}
        <div className="hidden sm:flex items-center gap-2 text-xs font-medium text-slate-400">
          <span>{totalPicks} picks</span>
          <span>•</span>
          <span>{totalOwners} owners</span>
        </div>
      </div>
    </div>
  );
}
