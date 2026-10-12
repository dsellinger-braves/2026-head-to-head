// src/views/history/components/HistoryHeaderNav.jsx
import React from 'react';

export default function HistoryHeaderNav({
  activeSubTab,
  setActiveSubTab,
  augmentedFinishesCount = 0,
  dataSource = '',
  loading = false,
  onOpenMethodologyModal,
}) {
  return (
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
          <span>👥 Tracked Team-Seasons: <strong className="text-white">{augmentedFinishesCount}</strong></span>
          <span className="flex items-center gap-1.5">
            ⚡ Source: <span className="text-blue-400 font-mono">{dataSource || (loading ? 'Loading...' : 'Supabase')}</span>
            {loading && <span className="inline-block animate-spin h-3 w-3 border-2 border-blue-400 border-t-transparent rounded-full"></span>}
          </span>
        </div>
        <button
          onClick={onOpenMethodologyModal}
          className="text-xs text-amber-400 hover:text-amber-300 font-bold flex items-center gap-1.5 transition cursor-pointer underline underline-offset-4"
        >
          <span>ℹ️</span>
          <span>How Era Adjustments Work</span>
        </button>
      </div>
    </div>
  );
}
