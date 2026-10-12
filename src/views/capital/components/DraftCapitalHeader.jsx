import React from 'react';

export default function DraftCapitalHeader({
  activeSubTab,
  onSubTabChange,
  onDraftYearChange,
  assetTradesList = [],
  incomingProposals = [],
  pendingCount = 0,
  DRAFT_OWNERS = [],
  ownerStats = {},
  currentUser,
  selectedOwner,
  setSelectedOwner
}) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <span className="text-3xl">🎟️</span>
          <div>
            <h1 className="text-2xl font-black text-white tracking-tight">Draft Capital & Pick Board</h1>
            <p className="text-slate-400 text-xs mt-0.5">
              Future 2027 draft assets, trade ledgers, compensation offsets, and historical 2026 draft structure
            </p>
          </div>
        </div>

        {/* Subtabs */}
        <div className="flex flex-wrap items-center bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-bold">
          <button
            onClick={() => {
              onSubTabChange('board');
              if (onDraftYearChange) onDraftYearChange(2027);
            }}
            className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
              activeSubTab === 'board'
                ? 'bg-teal-500 text-slate-950 shadow-md font-black'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            🚀 2027 Traded Board
          </button>
          <button
            onClick={() => onSubTabChange('ledgers')}
            className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
              activeSubTab === 'ledgers'
                ? 'bg-amber-500 text-slate-950 shadow-md font-black'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            📑 Owner Ledgers
          </button>
          <button
            onClick={() => onSubTabChange('history')}
            className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
              activeSubTab === 'history'
                ? 'bg-purple-500 text-white shadow-md font-black'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            📜 Trade History ({assetTradesList.length})
          </button>
          <button
            onClick={() => {
              onSubTabChange('2026board');
              if (onDraftYearChange) onDraftYearChange(2026);
            }}
            className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
              activeSubTab === '2026board'
                ? 'bg-blue-600 text-white shadow-md font-black'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            🏛️ 2026 Ground Truth
          </button>
          <button
            onClick={() => onSubTabChange('proposals')}
            className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer flex items-center gap-1.5 ${
              activeSubTab === 'proposals'
                ? 'bg-rose-600 text-white shadow-md font-black ring-1 ring-rose-400'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            <span>🤝 Trade Proposals</span>
            {incomingProposals.length > 0 ? (
              <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-emerald-400 text-slate-950 animate-pulse">
                📥 {incomingProposals.length}
              </span>
            ) : pendingCount > 0 ? (
              <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-amber-400 text-slate-950">
                {pendingCount}
              </span>
            ) : null}
          </button>
          <a
            href="#/trades"
            className="px-3.5 py-2 rounded-lg transition-colors cursor-pointer flex items-center gap-1.5 text-indigo-300 hover:text-white hover:bg-indigo-950/60 border border-indigo-500/40 text-xs font-black"
            title="View Graded Multi-Year Trade Archive"
          >
            <span>🏛️ Graded Trade Archive</span>
          </a>
        </div>
      </div>

      {/* Quick Manager Capital Summary Cards */}
      <div className="grid grid-cols-3 sm:grid-cols-5 md:grid-cols-9 gap-2 mt-6 pt-4 border-t border-slate-800">
        {DRAFT_OWNERS.map(owner => {
          const stat = ownerStats[owner];
          const diff = stat?.diff || 0;
          const isMe = owner === currentUser;
          return (
            <button
              key={owner}
              onClick={() => {
                setSelectedOwner(owner);
                onSubTabChange('ledgers');
              }}
              className={`p-2 rounded-xl text-center transition-all cursor-pointer border ${
                selectedOwner === owner && activeSubTab === 'ledgers'
                  ? 'bg-amber-500/20 border-amber-500/60 shadow-lg'
                  : isMe
                  ? 'bg-slate-800/80 border-slate-700 hover:border-slate-600'
                  : 'bg-slate-950/60 border-slate-800/60 hover:bg-slate-800/40'
              }`}
            >
              <div className="text-[11px] font-bold text-slate-300 truncate">{owner}</div>
              <div className="text-sm font-black text-white mt-0.5">{stat?.total ?? 26}</div>
              <div className={`text-[10px] font-extrabold ${diff > 0 ? 'text-emerald-400' : diff < 0 ? 'text-rose-400' : 'text-slate-500'}`}>
                {diff > 0 ? `+${diff}` : diff === 0 ? '±0' : `${diff}`}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
