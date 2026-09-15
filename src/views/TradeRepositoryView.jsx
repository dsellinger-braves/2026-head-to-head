// src/views/TradeRepositoryView.jsx
import React, { useState, useMemo } from 'react';
import historicalTrades from '../data/historicalTrades.json' with { type: 'json' };
import { gradeTrade } from '../utils/tradeGrading';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';

const ALL_MANAGERS = [
  'All Managers',
  'Daniel',
  'Adrian',
  'Garrett',
  'Mark',
  'Anil',
  'Tim',
  'Will',
  'Preston',
  'Alex',
  'Joe',
  'Patrick'
];

export default function TradeRepositoryView({ onPlayerClick, onOwnerClick }) {
  const [selectedSeason, setSelectedSeason] = useState('all');
  const [selectedManager, setSelectedManager] = useState('All Managers');
  const [sortBy, setSortBy] = useState('newest');
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedTradeId, setExpandedTradeId] = useState(null);

  // Grade all historical trades using the trade grading engine
  const gradedTrades = useMemo(() => {
    return (historicalTrades || []).map(t => gradeTrade(t));
  }, []);

  // Filter & sort trades
  const filteredTrades = useMemo(() => {
    let list = [...gradedTrades];

    // Season Filter
    if (selectedSeason !== 'all') {
      const year = parseInt(selectedSeason, 10);
      list = list.filter(t => t.season_year === year);
    }

    // Manager Filter
    if (selectedManager !== 'All Managers') {
      list = list.filter(t => t.participants?.includes(selectedManager));
    }

    // Search Query Filter
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase().trim();
      list = list.filter(t => {
        const inParticipants = t.participants?.some(p => p.toLowerCase().includes(q));
        const inItems = t.items?.some(i => 
          (i.asset_name || '').toLowerCase().includes(q) ||
          (i.sending_owner || '').toLowerCase().includes(q) ||
          (i.receiving_owner || '').toLowerCase().includes(q)
        );
        return inParticipants || inItems;
      });
    }

    // Sorting
    if (sortBy === 'newest') {
      list.sort((a, b) => (b.trade_date || '').localeCompare(a.trade_date || ''));
    } else if (sortBy === 'oldest') {
      list.sort((a, b) => (a.trade_date || '').localeCompare(b.trade_date || ''));
    } else if (sortBy === 'margin') {
      list.sort((a, b) => {
        const maxMarginA = Math.max(...Object.values(a.gradedPackages || {}).map(p => Math.abs(p.netMargin || 0)));
        const maxMarginB = Math.max(...Object.values(b.gradedPackages || {}).map(p => Math.abs(p.netMargin || 0)));
        return maxMarginB - maxMarginA;
      });
    } else if (sortBy === 'assets') {
      list.sort((a, b) => (b.items?.length || 0) - (a.items?.length || 0));
    }

    return list;
  }, [gradedTrades, selectedSeason, selectedManager, sortBy, searchQuery]);

  // Aggregate stats KPIs
  const kpis = useMemo(() => {
    const totalTrades = gradedTrades.length;
    let totalItems = 0;
    let totalPicks = 0;
    let totalCash = 0;
    const managerCounts = {};

    gradedTrades.forEach(t => {
      totalItems += t.items?.length || 0;
      t.items?.forEach(i => {
        if (i.asset_type === 'Pick') totalPicks++;
        if (i.asset_type === 'Budget') totalCash += (i.budget_amount || 0);
      });
      t.participants?.forEach(p => {
        managerCounts[p] = (managerCounts[p] || 0) + 1;
      });
    });

    const topTrader = Object.entries(managerCounts).sort((a, b) => b[1] - a[1])[0] || ['None', 0];

    return {
      totalTrades,
      totalItems,
      totalPicks,
      totalCash,
      topTraderName: topTrader[0],
      topTraderCount: topTrader[1]
    };
  }, [gradedTrades]);

  const toggleExpand = (uniqueId) => {
    setExpandedTradeId(prev => (prev === uniqueId ? null : uniqueId));
  };

  return (
    <div className="space-y-8 animate-fadeIn pb-16">
      {/* 1. HERO HEADER & KPI CARDS */}
      <div className="relative overflow-hidden rounded-3xl bg-gradient-to-br from-slate-900 via-indigo-950/40 to-slate-950 p-6 md:p-8 border border-indigo-500/20 shadow-2xl">
        <div className="absolute top-0 right-0 -mt-8 -mr-8 w-72 h-72 bg-indigo-500/10 rounded-full blur-3xl pointer-events-none" />
        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div>
            <div className="flex items-center gap-2 text-indigo-400 text-xs font-black uppercase tracking-widest mb-2">
              <span>🏛️ Multi-Year Trade Archive & Retrospective</span>
              <span className="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse" />
            </div>
            <h1 className="text-3xl md:text-4xl font-black text-white tracking-tight">
              Historical Trade Repository
            </h1>
            <p className="text-sm text-slate-400 max-w-2xl mt-1.5 leading-relaxed font-medium">
              Every blockbuster deal, off-season draft pick swap, and keeper budget transfer across league history—graded objectively by post-trade in-season production, long-term franchise keeper equity, and empirical draft pick curves.
            </p>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="p-3.5 rounded-2xl bg-slate-950/60 border border-indigo-500/20 backdrop-blur-md">
              <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Total Trades</div>
              <div className="text-2xl font-black text-white mt-0.5">{kpis.totalTrades}</div>
              <div className="text-[10px] text-indigo-400 font-semibold mt-0.5">2024 – 2026</div>
            </div>
            <div className="p-3.5 rounded-2xl bg-slate-950/60 border border-indigo-500/20 backdrop-blur-md">
              <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Draft Picks</div>
              <div className="text-2xl font-black text-cyan-300 mt-0.5">{kpis.totalPicks}</div>
              <div className="text-[10px] text-slate-400 font-semibold mt-0.5">Swapped in Deals</div>
            </div>
            <div className="p-3.5 rounded-2xl bg-slate-950/60 border border-indigo-500/20 backdrop-blur-md">
              <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Budget Cash</div>
              <div className="text-2xl font-black text-emerald-400 mt-0.5">${kpis.totalCash}</div>
              <div className="text-[10px] text-slate-400 font-semibold mt-0.5">Keeper Currency</div>
            </div>
            <div className="p-3.5 rounded-2xl bg-slate-950/60 border border-indigo-500/20 backdrop-blur-md">
              <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Most Active</div>
              <div className="text-xl font-black text-amber-300 mt-0.5 truncate">{kpis.topTraderName}</div>
              <div className="text-[10px] text-slate-400 font-semibold mt-0.5">{kpis.topTraderCount} Deals Involved</div>
            </div>
          </div>
        </div>
      </div>

      {/* 2. FILTER CONTROLS BAR */}
      <div className="flex flex-wrap items-center justify-between gap-4 p-4 rounded-2xl bg-slate-900/60 border border-slate-800 backdrop-blur-md">
        {/* Season Filter Pills */}
        <div className="flex items-center gap-1.5 bg-slate-950/60 p-1 rounded-xl border border-slate-800">
          {[
            { id: 'all', label: 'All Seasons' },
            { id: '2026', label: '2026' },
            { id: '2025', label: '2025' },
            { id: '2024', label: '2024' }
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setSelectedSeason(tab.id)}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold transition ${
                selectedSeason === tab.id
                  ? 'bg-indigo-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="flex flex-wrap items-center gap-3">
          {/* Manager Filter */}
          <div className="flex items-center gap-2">
            <span className="text-xs font-semibold text-slate-400">Manager:</span>
            <select
              value={selectedManager}
              onChange={(e) => setSelectedManager(e.target.value)}
              className="bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs font-semibold text-white focus:outline-none focus:border-indigo-500 transition"
            >
              {ALL_MANAGERS.map(m => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </div>

          {/* Sort By */}
          <div className="flex items-center gap-2">
            <span className="text-xs font-semibold text-slate-400">Sort:</span>
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
              className="bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs font-semibold text-white focus:outline-none focus:border-indigo-500 transition"
            >
              <option value="newest">Newest First</option>
              <option value="oldest">Oldest First</option>
              <option value="margin">Biggest Margin (A+ / F)</option>
              <option value="assets">Most Assets Traded</option>
            </select>
          </div>

          {/* Search Input */}
          <div className="relative">
            <input
              type="text"
              placeholder="Search player, owner, or pick..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="bg-slate-950 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs font-medium text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 w-52 transition"
            />
            <span className="absolute left-2.5 top-2 text-slate-500 text-xs">🔍</span>
          </div>
        </div>
      </div>

      {/* 3. TRADE CARDS LIST */}
      {filteredTrades.length === 0 ? (
        <div className="p-12 text-center rounded-3xl bg-slate-900/30 border border-slate-800 text-slate-400">
          <div className="text-3xl mb-2">📜</div>
          <div className="font-bold text-white text-base">No trades match your search filters</div>
          <div className="text-xs mt-1">Try resetting manager or season filters.</div>
        </div>
      ) : (
        <div className="space-y-6">
          {filteredTrades.map(trade => {
            const isExpanded = expandedTradeId === trade.unique_id;
            const participantsList = trade.participants || [];
            const packages = trade.gradedPackages || {};

            return (
              <div
                key={trade.unique_id}
                className="overflow-hidden rounded-3xl bg-slate-900/60 border border-slate-800 hover:border-indigo-500/40 transition shadow-xl"
              >
                {/* Header Strip */}
                <div className="flex flex-wrap items-center justify-between gap-4 px-6 py-4 bg-slate-950/80 border-b border-slate-800/80">
                  <div className="flex items-center gap-3">
                    <span className="px-2.5 py-1 rounded-lg bg-indigo-500/20 text-indigo-300 font-black text-xs border border-indigo-500/30">
                      {trade.season_year} Deal #{trade.trade_id}
                    </span>
                    <span className="text-xs font-bold text-slate-400">
                      📅 {trade.trade_date}
                    </span>
                    <div className="flex items-center gap-1.5 text-xs font-black text-white">
                      {participantsList.map((pName, idx) => (
                        <React.Fragment key={pName}>
                          <span className="text-indigo-200">{pName}</span>
                          {idx < participantsList.length - 1 && (
                            <span className="text-slate-600">⇄</span>
                          )}
                        </React.Fragment>
                      ))}
                    </div>
                  </div>

                  {/* Verdict / Outcome Banner */}
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-black text-indigo-300 bg-indigo-950/50 px-3 py-1 rounded-xl border border-indigo-500/30">
                      {trade.outcomeSummary}
                    </span>
                    <button
                      onClick={() => toggleExpand(trade.unique_id)}
                      className="px-3 py-1 rounded-xl bg-slate-800 hover:bg-slate-700 text-xs font-bold text-white transition flex items-center gap-1.5"
                    >
                      <span>{isExpanded ? 'Hide Details' : 'Retrospective'}</span>
                      <span>{isExpanded ? '▲' : '▼'}</span>
                    </button>
                  </div>
                </div>

                {/* Side-by-Side Packages */}
                <div className="p-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                  {participantsList.map(owner => {
                    const pkg = packages[owner] || { sent: [], received: [], netMargin: 0, grade: 'C', gradeColor: 'slate' };
                    const teamObj = TEAMS[pkg.team_id] || { name: `${owner}'s Team` };

                    const isPositive = pkg.netMargin >= 0;

                    return (
                      <div
                        key={owner}
                        className="flex flex-col justify-between rounded-2xl bg-slate-950/40 p-5 border border-slate-800/80 space-y-4"
                      >
                        {/* Owner Header & Grade Pill */}
                        <div className="flex items-center justify-between gap-3 border-b border-slate-800/60 pb-3">
                          <div
                            onClick={() => onOwnerClick && onOwnerClick(teamObj)}
                            className="flex items-center gap-3 cursor-pointer hover:opacity-80 transition"
                          >
                            <TeamAvatar teamId={pkg.team_id} className="w-10 h-10 rounded-full border border-slate-700" />
                            <div>
                              <div className="text-sm font-black text-white flex items-center gap-1.5">
                                <span>{owner}</span>
                                <span className="text-[10px] text-slate-400 font-semibold truncate max-w-[120px]">
                                  ({teamObj.name})
                                </span>
                              </div>
                              <div className="text-[11px] font-bold mt-0.5 text-slate-400">
                                Total Return: <span className="text-white font-black">{pkg.totalReceivedVal} pts</span>
                              </div>
                            </div>
                          </div>

                          {/* Grade Badge */}
                          <div className="text-right">
                            <div className={`px-2.5 py-1 rounded-xl font-black text-xs border ${
                              pkg.gradeColor === 'emerald'
                                ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                                : pkg.gradeColor === 'teal' || pkg.gradeColor === 'cyan'
                                ? 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40'
                                : pkg.gradeColor === 'amber'
                                ? 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                                : pkg.gradeColor === 'rose'
                                ? 'bg-rose-500/20 text-rose-300 border-rose-500/40'
                                : 'bg-slate-800 text-slate-300 border-slate-700'
                            }`}>
                              Grade {pkg.grade}
                            </div>
                            <div className={`text-[10px] font-bold mt-0.5 ${isPositive ? 'text-emerald-400' : 'text-rose-400'}`}>
                              {isPositive ? `+${pkg.netMargin}` : pkg.netMargin} pts margin
                            </div>
                          </div>
                        </div>

                        {/* Received Assets Section */}
                        <div className="space-y-2">
                          <div className="text-[10px] font-black uppercase tracking-wider text-emerald-400 flex items-center gap-1">
                            <span>📥</span>
                            <span>Acquired / Received ({pkg.received?.length || 0})</span>
                          </div>
                          {pkg.received?.length === 0 ? (
                            <div className="text-xs text-slate-500 italic">No incoming assets</div>
                          ) : (
                            <div className="space-y-1.5">
                              {pkg.received.map((item, idx) => (
                                <div
                                  key={idx}
                                  className="flex items-center justify-between p-2.5 rounded-xl bg-slate-900/80 border border-slate-800 text-xs"
                                >
                                  <div className="flex items-center gap-2 min-w-0">
                                    <span className="text-sm shrink-0">
                                      {item.asset_type === 'Player' ? '⚾' : item.asset_type === 'Pick' ? '🎟️' : '💰'}
                                    </span>
                                    <div className="min-w-0">
                                      <div className="font-black text-white truncate flex items-center gap-1.5 flex-wrap">
                                        <span
                                          onClick={() => item.espn_player_id && onPlayerClick && onPlayerClick(item.espn_player_id, item.asset_name)}
                                          className={item.asset_type === 'Player' ? 'hover:text-cyan-300 cursor-pointer transition' : ''}
                                        >
                                          {item.asset_name}
                                        </span>
                                        {item.isKept && (
                                          <span className="text-[9px] bg-purple-500/20 text-purple-300 px-1.5 py-0.5 rounded border border-purple-500/30 font-bold">
                                            🔒 {item.keeperLabel}
                                          </span>
                                        )}
                                      </div>
                                      <div className="text-[10px] text-slate-400 font-medium truncate">
                                        {item.detail}
                                      </div>
                                    </div>
                                  </div>
                                  <div className="text-right shrink-0 ml-2">
                                    <span className="text-xs font-black text-emerald-400">
                                      +{item.value}
                                    </span>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>

                        {/* Sent Assets Section */}
                        <div className="space-y-2 pt-2 border-t border-slate-800/40">
                          <div className="text-[10px] font-black uppercase tracking-wider text-rose-400 flex items-center gap-1">
                            <span>📤</span>
                            <span>Traded Away / Sent ({pkg.sent?.length || 0})</span>
                          </div>
                          {pkg.sent?.length === 0 ? (
                            <div className="text-xs text-slate-500 italic">No outgoing assets</div>
                          ) : (
                            <div className="space-y-1.5">
                              {pkg.sent.map((item, idx) => (
                                <div
                                  key={idx}
                                  className="flex items-center justify-between p-2.5 rounded-xl bg-slate-900/40 border border-slate-800/60 text-xs"
                                >
                                  <div className="flex items-center gap-2 min-w-0">
                                    <span className="text-sm shrink-0">
                                      {item.asset_type === 'Player' ? '⚾' : item.asset_type === 'Pick' ? '🎟️' : '💰'}
                                    </span>
                                    <div className="min-w-0">
                                      <div className="font-bold text-slate-300 truncate">
                                        {item.asset_name}
                                      </div>
                                      <div className="text-[10px] text-slate-500 font-medium truncate">
                                        {item.detail}
                                      </div>
                                    </div>
                                  </div>
                                  <div className="text-right shrink-0 ml-2">
                                    <span className="text-xs font-black text-slate-400">
                                      -{item.value}
                                    </span>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>

                {/* Expandable Retrospective Breakdown */}
                {isExpanded && (
                  <div className="px-6 py-5 bg-slate-950/90 border-t border-slate-800 animate-fadeIn space-y-4">
                    <div className="flex items-center gap-2 text-xs font-black text-indigo-400 uppercase tracking-wider">
                      <span>🔍</span>
                      <span>Trade Retrospective & Mathematical Analysis</span>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                      <div className="p-4 rounded-2xl bg-slate-900/60 border border-slate-800 space-y-2">
                        <div className="font-black text-white text-sm">Franchise & Keeper Impact</div>
                        <p className="text-slate-300 leading-relaxed font-medium">
                          Trades in our league must be evaluated beyond simple short-term box scores. When young cornerstones (like Bobby Witt Jr., Gunnar Henderson, or Corbin Carroll) are acquired, the receiving manager captures high-leverage keeper surplus value ($+28 pts bonus) that echoes into future seasons.
                        </p>
                      </div>

                      <div className="p-4 rounded-2xl bg-slate-900/60 border border-slate-800 space-y-2">
                        <div className="font-black text-white text-sm">Draft Capital & Budget Evaluation</div>
                        <p className="text-slate-300 leading-relaxed font-medium">
                          Draft picks are evaluated using an exponential draft curve calibrated to historic player production (<span className="text-indigo-300 font-mono">100 · e^(-0.015·Pick)</span>), while keeper budget cash carries a calibrated exchange rate of <span className="text-emerald-300 font-mono">$1 = 3.5 pts</span> of purchasing leverage.
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
