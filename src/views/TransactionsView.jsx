import { useState, useMemo, useEffect } from 'react';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';
import { supabase } from '../supabaseClient';
import defaultTransactions2026 from '../data/transactions2026.json';

const TYPE_CONFIG = {
  ADD_DROP: {
    label: 'Add / Drop',
    bg: 'bg-blue-50 text-blue-700 border-blue-200',
    icon: '🔄'
  },
  WAIVER_ADD_DROP: {
    label: 'Waiver / Drop',
    bg: 'bg-amber-50 text-amber-700 border-amber-200',
    icon: '📋'
  },
  ADD: {
    label: 'Add',
    bg: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    icon: '➕'
  },
  WAIVER_ADD: {
    label: 'Waiver Claim',
    bg: 'bg-amber-50 text-amber-700 border-amber-200',
    icon: '📋'
  },
  DROP: {
    label: 'Drop',
    bg: 'bg-rose-50 text-rose-700 border-rose-200',
    icon: '➖'
  },
  TRADE: {
    label: 'Trade',
    bg: 'bg-purple-50 text-purple-700 border-purple-200',
    icon: '🤝'
  },
  DRAFT: {
    label: 'Draft',
    bg: 'bg-gray-100 text-gray-700 border-gray-200',
    icon: '🏷️'
  }
};

const ITEMS_PER_PAGE = 50;

/**
 * Groups raw transaction records by base ESPN transaction ID.
 * Collapses:
 *  - Multi-item trades into 1 summarized trade row
 *  - Simultaneous add/drops into 1 summarized add/drop row
 */
function groupTransactions(rawTransactions) {
  if (!Array.isArray(rawTransactions)) return [];

  const groups = new Map();

  for (const t of rawTransactions) {
    if (!t) continue;
    const baseId = (t.espn_transaction_id || '').split('_')[0] || t.espn_transaction_id || Math.random().toString();
    if (!groups.has(baseId)) {
      groups.set(baseId, []);
    }
    groups.get(baseId).push(t);
  }

  const flattened = [];

  for (const [baseId, items] of groups.entries()) {
    const first = items[0];
    const date = first.transaction_date;
    const year = first.season_year || 2026;
    const period = items.find(i => i.scoring_period_id > 0)?.scoring_period_id || first.scoring_period_id || 0;

    const types = new Set(items.map(i => i.transaction_type));

    if (types.has('TRADE')) {
      const teamsInvolved = new Set();
      const receivedByTeam = {};
      const tradeDrops = [];

      for (const i of items) {
        if (i.transaction_type === 'TRADE' && i.to_team_id > 0) {
          teamsInvolved.add(i.to_team_id);
          if (i.from_team_id > 0) teamsInvolved.add(i.from_team_id);
          if (!receivedByTeam[i.to_team_id]) receivedByTeam[i.to_team_id] = [];
          receivedByTeam[i.to_team_id].push({
            player_id: i.player_id,
            player_name: i.player_name,
            from_team_id: i.from_team_id
          });
        } else if (i.transaction_type === 'DROP' || i.to_team_id === 0) {
          tradeDrops.push({
            player_id: i.player_id,
            player_name: i.player_name,
            from_team_id: i.from_team_id
          });
        }
      }

      flattened.push({
        id: baseId,
        type: 'TRADE',
        date,
        year,
        period,
        teams: Array.from(teamsInvolved),
        received: receivedByTeam,
        drops: tradeDrops,
        raw_items_count: items.length
      });
    } else if ((types.has('ADD') || types.has('WAIVER_ADD')) && types.has('DROP')) {
      const teamId = items.find(i => i.to_team_id > 0)?.to_team_id || first.from_team_id;
      const isWaiver = items.some(i => i.transaction_type === 'WAIVER_ADD' || i.raw_type === 'WAIVER');
      const adds = items
        .filter(i => i.transaction_type === 'ADD' || i.transaction_type === 'WAIVER_ADD')
        .map(i => ({ player_id: i.player_id, player_name: i.player_name }));
      const drops = items
        .filter(i => i.transaction_type === 'DROP')
        .map(i => ({ player_id: i.player_id, player_name: i.player_name }));

      flattened.push({
        id: baseId,
        type: isWaiver ? 'WAIVER_ADD_DROP' : 'ADD_DROP',
        team_id: teamId,
        is_waiver: isWaiver,
        date,
        year,
        period,
        adds,
        drops,
        raw_items_count: items.length
      });
    } else if (types.has('ADD') || types.has('WAIVER_ADD')) {
      const teamId = items.find(i => i.to_team_id > 0)?.to_team_id || first.to_team_id;
      const isWaiver = items.some(i => i.transaction_type === 'WAIVER_ADD' || i.raw_type === 'WAIVER');
      const adds = items.map(i => ({ player_id: i.player_id, player_name: i.player_name }));

      flattened.push({
        id: baseId,
        type: isWaiver ? 'WAIVER_ADD' : 'ADD',
        team_id: teamId,
        is_waiver: isWaiver,
        date,
        year,
        period,
        adds,
        drops: [],
        raw_items_count: items.length
      });
    } else if (types.has('DROP')) {
      const teamId = items.find(i => i.from_team_id > 0)?.from_team_id || first.from_team_id;
      const drops = items.map(i => ({ player_id: i.player_id, player_name: i.player_name }));

      flattened.push({
        id: baseId,
        type: 'DROP',
        team_id: teamId,
        date,
        year,
        period,
        adds: [],
        drops,
        raw_items_count: items.length
      });
    } else if (types.has('DRAFT')) {
      flattened.push({
        id: baseId,
        type: 'DRAFT',
        date,
        year,
        period,
        players: items.map(i => ({ player_id: i.player_id, player_name: i.player_name })),
        raw_items_count: items.length
      });
    }
  }

  return flattened.sort((a, b) => new Date(b.date) - new Date(a.date));
}

export default function TransactionsView({ onPlayerClick, onOwnerClick }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedType, setSelectedType] = useState('ALL');
  const [selectedTeam, setSelectedTeam] = useState('ALL');
  const [selectedYear, setSelectedYear] = useState('ALL');
  const [currentPage, setCurrentPage] = useState(1);
  const [rawTransactions, setRawTransactions] = useState(defaultTransactions2026 || []);
  const [isLoadingSupabase, setIsLoadingSupabase] = useState(false);

  // Optional Supabase sync (disabled by default because project wczdkcdqgtzlsbssogoz is currently paused)
  useEffect(() => {
    const syncEnabled = import.meta.env.VITE_SYNC_SUPABASE_TRANSACTIONS === 'true';
    if (!syncEnabled) return;

    let isMounted = true;
    async function loadSupabaseTxns() {
      try {
        setIsLoadingSupabase(true);
        const { data, error } = await supabase
          .from('transactions')
          .select('*')
          .order('transaction_date', { ascending: false })
          .limit(2000);

        if (!error && data && data.length > 0 && isMounted) {
          const map = new Map();
          (defaultTransactions2026 || []).forEach(t => map.set(t.espn_transaction_id, t));
          data.forEach(t => map.set(t.espn_transaction_id, t));
          const merged = Array.from(map.values()).sort(
            (a, b) => new Date(b.transaction_date) - new Date(a.transaction_date)
          );
          setRawTransactions(merged);
        }
      } catch (err) {
        console.warn('Transactions Supabase fetch skipped or errored, using bundled transactions:', err);
      } finally {
        if (isMounted) setIsLoadingSupabase(false);
      }
    }
    loadSupabaseTxns();
    return () => { isMounted = false; };
  }, []);

  // Group raw transactions into flattened single-row trades and add/drops
  const flattenedTransactions = useMemo(() => {
    return groupTransactions(rawTransactions);
  }, [rawTransactions]);

  const availableYears = useMemo(() => {
    const years = new Set(flattenedTransactions.map(t => t.year || 2026));
    return Array.from(years).sort((a, b) => b - a);
  }, [flattenedTransactions]);

  const filteredTransactions = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();

    return flattenedTransactions.filter(t => {
      // Type filter
      if (selectedType !== 'ALL') {
        if (selectedType === 'TRADE' && t.type !== 'TRADE') return false;
        if (selectedType === 'ADD_DROP' && t.type !== 'ADD_DROP' && t.type !== 'WAIVER_ADD_DROP') return false;
        if (selectedType === 'ADD' && t.type !== 'ADD' && t.type !== 'ADD_DROP' && t.type !== 'WAIVER_ADD' && t.type !== 'WAIVER_ADD_DROP') return false;
        if (selectedType === 'WAIVER_ADD' && !t.is_waiver && t.type !== 'WAIVER_ADD' && t.type !== 'WAIVER_ADD_DROP') return false;
        if (selectedType === 'DROP' && t.type !== 'DROP' && t.type !== 'ADD_DROP' && t.type !== 'WAIVER_ADD_DROP') return false;
      }

      // Year filter
      if (selectedYear !== 'ALL' && String(t.year || 2026) !== String(selectedYear)) {
        return false;
      }

      // Team filter
      if (selectedTeam !== 'ALL') {
        const teamIdNum = parseInt(selectedTeam, 10);
        if (t.type === 'TRADE') {
          if (!t.teams?.includes(teamIdNum)) return false;
        } else {
          if (t.team_id !== teamIdNum) return false;
        }
      }

      // Search term filter
      if (term) {
        // Player names check
        if (t.adds?.some(p => p.player_name?.toLowerCase().includes(term))) return true;
        if (t.drops?.some(p => p.player_name?.toLowerCase().includes(term))) return true;
        if (t.players?.some(p => p.player_name?.toLowerCase().includes(term))) return true;
        if (t.received) {
          for (const pList of Object.values(t.received)) {
            if (pList.some(p => p.player_name?.toLowerCase().includes(term))) return true;
          }
        }

        // Teams and owners check
        if (t.team_id) {
          const team = TEAMS[t.team_id];
          if (team?.name?.toLowerCase().includes(term) || team?.owner?.toLowerCase().includes(term)) return true;
        }
        if (t.teams) {
          for (const tid of t.teams) {
            const team = TEAMS[tid];
            if (team?.name?.toLowerCase().includes(term) || team?.owner?.toLowerCase().includes(term)) return true;
          }
        }

        // Type label check
        const typeConf = TYPE_CONFIG[t.type];
        if (typeConf?.label?.toLowerCase().includes(term)) return true;

        return false;
      }

      return true;
    });
  }, [flattenedTransactions, searchTerm, selectedType, selectedTeam, selectedYear]);

  // Pagination
  const totalPages = Math.ceil(filteredTransactions.length / ITEMS_PER_PAGE) || 1;
  const paginatedTransactions = useMemo(() => {
    const start = (currentPage - 1) * ITEMS_PER_PAGE;
    return filteredTransactions.slice(start, start + ITEMS_PER_PAGE);
  }, [filteredTransactions, currentPage]);

  const handlePageChange = (newPage) => {
    setCurrentPage(newPage);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const humanTeams = useMemo(() => {
    return Object.keys(TEAMS).filter(id => parseInt(id) !== 99);
  }, []);

  const formatDate = (isoStr) => {
    if (!isoStr) return '-';
    try {
      const d = new Date(isoStr);
      return d.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
      });
    } catch {
      return isoStr;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header & Controls */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-5 border-b border-gray-100">
          <div>
            <h2 className="text-2xl font-black text-gray-900 flex items-center gap-3">
              <span>📋 League Transaction Log</span>
              {isLoadingSupabase && (
                <span className="text-xs font-normal text-blue-600 animate-pulse">Syncing...</span>
              )}
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              Search all adds, drops, waiver claims, and trades across the league history. Simultaneous moves and trades are summarized into single entries.
            </p>
          </div>

          <div className="text-right">
            <span className="text-xs font-bold uppercase text-gray-400 block tracking-wider">Total Transactions</span>
            <span className="text-lg font-black font-mono text-blue-800">
              {filteredTransactions.length.toLocaleString()} <span className="text-xs font-normal text-gray-500">moves ({flattenedTransactions.length.toLocaleString()} total)</span>
            </span>
          </div>
        </div>

        {/* Filter Toolbar */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 mt-4">
          {/* Search Bar */}
          <div>
            <label className="text-[11px] font-bold uppercase text-gray-500 block mb-1">
              Search Player or Team:
            </label>
            <div className="relative">
              <input
                type="text"
                value={searchTerm}
                onChange={(e) => { setSearchTerm(e.target.value); setCurrentPage(1); }}
                placeholder="e.g. Marte, Keaschall, Dan, Mark..."
                className="w-full bg-gray-50 border border-gray-300 rounded-lg px-3 py-2 text-xs font-medium text-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:bg-white transition-all pl-8"
              />
              <span className="absolute left-2.5 top-2 text-gray-400 text-xs">🔍</span>
              {searchTerm && (
                <button
                  onClick={() => setSearchTerm('')}
                  className="absolute right-2.5 top-2 text-gray-400 hover:text-gray-600 text-xs"
                >
                  ✕
                </button>
              )}
            </div>
          </div>

          {/* Type Filter */}
          <div>
            <label className="text-[11px] font-bold uppercase text-gray-500 block mb-1">
              Transaction Type:
            </label>
            <select
              value={selectedType}
              onChange={(e) => { setSelectedType(e.target.value); setCurrentPage(1); }}
              className="w-full bg-gray-50 border border-gray-300 rounded-lg px-3 py-2 text-xs font-bold text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="ALL">All Move Types</option>
              <option value="ADD_DROP">Add / Drops (Simultaneous)</option>
              <option value="TRADE">Trades</option>
              <option value="ADD">Free Agent Adds</option>
              <option value="WAIVER_ADD">Waiver Claims</option>
              <option value="DROP">Pure Drops</option>
            </select>
          </div>

          {/* Team Filter */}
          <div>
            <label className="text-[11px] font-bold uppercase text-gray-500 block mb-1">
              Filter by Team:
            </label>
            <select
              value={selectedTeam}
              onChange={(e) => { setSelectedTeam(e.target.value); setCurrentPage(1); }}
              className="w-full bg-gray-50 border border-gray-300 rounded-lg px-3 py-2 text-xs font-bold text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="ALL">All Teams & Owners</option>
              {humanTeams.map(id => (
                <option key={id} value={id}>
                  {TEAMS[id].name} ({TEAMS[id].owner})
                </option>
              ))}
            </select>
          </div>

          {/* Year Filter */}
          <div>
            <label className="text-[11px] font-bold uppercase text-gray-500 block mb-1">
              Season / Year:
            </label>
            <select
              value={selectedYear}
              onChange={(e) => { setSelectedYear(e.target.value); setCurrentPage(1); }}
              className="w-full bg-gray-50 border border-gray-300 rounded-lg px-3 py-2 text-xs font-bold text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="ALL">All Seasons</option>
              {availableYears.map(year => (
                <option key={year} value={year}>{year} Season</option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Transactions Feed / Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-36">
                  Date & Time
                </th>
                <th className="px-3 py-3 text-center text-xs font-bold text-gray-500 uppercase tracking-wider w-32">
                  Type
                </th>
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-56">
                  Team(s)
                </th>
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">
                  Summary of Move
                </th>
                <th className="px-3 py-3 text-center text-xs font-bold text-gray-500 uppercase tracking-wider w-20">
                  Period
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {paginatedTransactions.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-6 py-12 text-center text-gray-400">
                    No transactions match your search or filters.
                  </td>
                </tr>
              ) : (
                paginatedTransactions.map((t) => {
                  const typeConf = TYPE_CONFIG[t.type] || {
                    label: t.type || 'Move',
                    bg: 'bg-gray-100 text-gray-700 border-gray-200',
                    icon: '•'
                  };

                  return (
                    <tr key={t.id} className="hover:bg-blue-50/40 transition-colors">
                      {/* Date */}
                      <td className="px-4 py-3 whitespace-nowrap text-xs font-mono text-gray-500">
                        {formatDate(t.date)}
                      </td>

                      {/* Type Badge */}
                      <td className="px-3 py-3 text-center whitespace-nowrap">
                        <span className={`inline-flex items-center gap-1 text-[11px] font-bold px-2.5 py-1 rounded-full border ${typeConf.bg}`}>
                          <span>{typeConf.icon}</span>
                          <span>{typeConf.label}</span>
                        </span>
                      </td>

                      {/* Team(s) */}
                      <td className="px-4 py-3">
                        {t.type === 'TRADE' ? (
                          <div className="flex items-center gap-1.5 flex-wrap text-xs">
                            {t.teams.map((tid, idx) => {
                              const team = TEAMS[tid];
                              return (
                                <span key={tid} className="flex items-center gap-1">
                                  {idx > 0 && <span className="text-purple-400 font-bold">⇄</span>}
                                  {team ? (
                                    <button
                                      onClick={() => onOwnerClick?.(team)}
                                      className="flex items-center gap-1 font-bold text-gray-900 hover:text-blue-700 hover:underline cursor-pointer"
                                    >
                                      <TeamAvatar team={team} size="xs" />
                                      <span>{team.name}</span>
                                    </button>
                                  ) : (
                                    <span className="font-semibold text-gray-600">Team {tid}</span>
                                  )}
                                </span>
                              );
                            })}
                          </div>
                        ) : (
                          (() => {
                            const team = TEAMS[t.team_id];
                            return team ? (
                              <button
                                onClick={() => onOwnerClick?.(team)}
                                className="flex items-center gap-2 font-bold text-gray-900 hover:text-blue-700 hover:underline text-left text-xs cursor-pointer"
                              >
                                <TeamAvatar team={team} size="sm" />
                                <div>
                                  <div className="leading-tight">{team.name}</div>
                                  <div className="text-[10px] text-gray-400 font-normal">{team.owner}</div>
                                </div>
                              </button>
                            ) : (
                              <span className="text-xs text-gray-500 font-semibold">
                                {t.team_id ? `Team ${t.team_id}` : '-'}
                              </span>
                            );
                          })()
                        )}
                      </td>

                      {/* Summary of Move */}
                      <td className="px-4 py-3">
                        {t.type === 'TRADE' ? (
                          <div className="space-y-2 py-0.5">
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                              {Object.entries(t.received || {}).map(([teamIdStr, players]) => {
                                const toTeam = TEAMS[teamIdStr];
                                return (
                                  <div
                                    key={teamIdStr}
                                    className="bg-purple-50/70 border border-purple-200/70 rounded-lg p-2.5 text-xs shadow-xs"
                                  >
                                    <div className="flex items-center gap-1.5 font-bold text-purple-900 mb-1.5">
                                      <TeamAvatar team={toTeam} size="xs" />
                                      <span>{toTeam?.name || `Team ${teamIdStr}`} receives:</span>
                                    </div>
                                    <div className="flex flex-wrap gap-x-2 gap-y-1 pl-4">
                                      {players.map((p) => (
                                        <button
                                          key={p.player_id}
                                          onClick={() => onPlayerClick?.(p.player_id, p.player_name)}
                                          className="font-bold text-gray-900 hover:text-purple-700 hover:underline cursor-pointer inline-flex items-center gap-1"
                                        >
                                          <span className="text-purple-600 font-bold">•</span>
                                          <span>{p.player_name}</span>
                                        </button>
                                      ))}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>

                            {t.drops && t.drops.length > 0 && (
                              <div className="text-[11px] text-gray-500 flex items-center gap-1.5 pl-1">
                                <span className="text-rose-500 font-bold">Cut to roster:</span>
                                {t.drops.map((d) => (
                                  <button
                                    key={d.player_id}
                                    onClick={() => onPlayerClick?.(d.player_id, d.player_name)}
                                    className="text-rose-700 hover:underline font-semibold cursor-pointer"
                                  >
                                    {d.player_name} ({TEAMS[d.from_team_id]?.name || 'Team'})
                                  </button>
                                ))}
                              </div>
                            )}
                          </div>
                        ) : t.type === 'ADD_DROP' || t.type === 'WAIVER_ADD_DROP' ? (
                          <div className="flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-4 py-1 text-xs">
                            {/* Added */}
                            <div className="flex items-center gap-1.5 flex-wrap">
                              <span className="inline-flex items-center justify-center px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-800 font-bold text-[10px] uppercase tracking-wide border border-emerald-200">
                                + Added
                              </span>
                              {t.adds.map((p) => (
                                <button
                                  key={p.player_id}
                                  onClick={() => onPlayerClick?.(p.player_id, p.player_name)}
                                  className="font-bold text-emerald-950 hover:text-blue-700 hover:underline cursor-pointer"
                                >
                                  {p.player_name}
                                </button>
                              ))}
                            </div>

                            <span className="text-gray-300 hidden sm:inline">|</span>

                            {/* Dropped */}
                            <div className="flex items-center gap-1.5 flex-wrap">
                              <span className="inline-flex items-center justify-center px-1.5 py-0.5 rounded bg-rose-100 text-rose-800 font-bold text-[10px] uppercase tracking-wide border border-rose-200">
                                - Dropped
                              </span>
                              {t.drops.map((p) => (
                                <button
                                  key={p.player_id}
                                  onClick={() => onPlayerClick?.(p.player_id, p.player_name)}
                                  className="font-medium text-rose-900 hover:text-blue-700 hover:underline cursor-pointer"
                                >
                                  {p.player_name}
                                </button>
                              ))}
                            </div>
                          </div>
                        ) : t.type === 'ADD' || t.type === 'WAIVER_ADD' ? (
                          <div className="flex items-center gap-1.5 flex-wrap py-1 text-xs">
                            <span className="inline-flex items-center justify-center px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-800 font-bold text-[10px] uppercase tracking-wide border border-emerald-200">
                              + Added
                            </span>
                            {t.adds.map((p) => (
                              <button
                                key={p.player_id}
                                onClick={() => onPlayerClick?.(p.player_id, p.player_name)}
                                className="font-bold text-emerald-950 hover:text-blue-700 hover:underline cursor-pointer"
                              >
                                {p.player_name}
                              </button>
                            ))}
                          </div>
                        ) : t.type === 'DROP' ? (
                          <div className="flex items-center gap-1.5 flex-wrap py-1 text-xs">
                            <span className="inline-flex items-center justify-center px-1.5 py-0.5 rounded bg-rose-100 text-rose-800 font-bold text-[10px] uppercase tracking-wide border border-rose-200">
                              - Dropped
                            </span>
                            {t.drops.map((p) => (
                              <button
                                key={p.player_id}
                                onClick={() => onPlayerClick?.(p.player_id, p.player_name)}
                                className="font-medium text-rose-900 hover:text-blue-700 hover:underline cursor-pointer"
                              >
                                {p.player_name}
                              </button>
                            ))}
                          </div>
                        ) : (
                          <div className="flex items-center gap-1.5 flex-wrap py-1 text-xs text-gray-700">
                            <span className="font-semibold text-gray-500">Drafted:</span>
                            {t.players?.map((p) => (
                              <button
                                key={p.player_id}
                                onClick={() => onPlayerClick?.(p.player_id, p.player_name)}
                                className="font-bold text-gray-900 hover:text-blue-700 hover:underline cursor-pointer"
                              >
                                {p.player_name}
                              </button>
                            ))}
                          </div>
                        )}
                      </td>

                      {/* Scoring Period */}
                      <td className="px-3 py-3 text-center whitespace-nowrap text-xs font-mono text-gray-400">
                        {t.period ? `P${t.period}` : '-'}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination Bar */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-6 py-4 bg-gray-50 border-t border-gray-200 flex-wrap gap-3">
            <span className="text-xs text-gray-500">
              Page <strong>{currentPage}</strong> of <strong>{totalPages}</strong> ({filteredTransactions.length.toLocaleString()} moves)
            </span>
            <div className="flex items-center gap-1">
              <button
                onClick={() => handlePageChange(1)}
                disabled={currentPage === 1}
                className="px-2.5 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
              >
                « First
              </button>
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
                className="px-3 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
              >
                ‹ Prev
              </button>
              <span className="px-3 py-1 text-xs font-mono font-bold text-blue-700 bg-blue-50 border border-blue-200 rounded">
                {currentPage}
              </span>
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
                className="px-3 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
              >
                Next ›
              </button>
              <button
                onClick={() => handlePageChange(totalPages)}
                disabled={currentPage === totalPages}
                className="px-2.5 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
              >
                Last »
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
