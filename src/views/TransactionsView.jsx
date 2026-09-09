import { useState, useMemo, useEffect } from 'react';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';
import { supabase } from '../supabaseClient';
import defaultTransactions2026 from '../data/transactions2026.json';

const TYPE_CONFIG = {
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
    icon: '🔄'
  }
};

const ITEMS_PER_PAGE = 50;

export default function TransactionsView({ onPlayerClick, onOwnerClick }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedType, setSelectedType] = useState('ALL');
  const [selectedTeam, setSelectedTeam] = useState('ALL');
  const [selectedYear, setSelectedYear] = useState('ALL');
  const [currentPage, setCurrentPage] = useState(1);
  const [allTransactions, setAllTransactions] = useState(defaultTransactions2026 || []);
  const [isLoadingSupabase, setIsLoadingSupabase] = useState(false);

  // Attempt to supplement or update from Supabase transactions table
  useEffect(() => {
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
          // Merge Supabase transactions with local ones, preferring Supabase
          const map = new Map();
          // Seed with bundled
          (defaultTransactions2026 || []).forEach(t => map.set(t.espn_transaction_id, t));
          // Overlay Supabase
          data.forEach(t => map.set(t.espn_transaction_id, t));
          const merged = Array.from(map.values()).sort(
            (a, b) => new Date(b.transaction_date) - new Date(a.transaction_date)
          );
          setAllTransactions(merged);
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

  const availableYears = useMemo(() => {
    const years = new Set(allTransactions.map(t => t.season_year || 2026));
    return Array.from(years).sort((a, b) => b - a);
  }, [allTransactions]);

  const filteredTransactions = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();

    return allTransactions.filter(t => {
      // Type filter
      if (selectedType !== 'ALL' && t.transaction_type !== selectedType) {
        return false;
      }

      // Year filter
      if (selectedYear !== 'ALL' && String(t.season_year || 2026) !== String(selectedYear)) {
        return false;
      }

      // Team filter
      if (selectedTeam !== 'ALL') {
        const teamIdNum = parseInt(selectedTeam, 10);
        const matchesTo = t.to_team_id === teamIdNum;
        const matchesFrom = t.from_team_id === teamIdNum;
        if (!matchesTo && !matchesFrom) return false;
      }

      // Search term filter
      if (term) {
        const pName = (t.player_name || '').toLowerCase();
        const toTeamName = (TEAMS[t.to_team_id]?.name || '').toLowerCase();
        const toOwner = (TEAMS[t.to_team_id]?.owner || '').toLowerCase();
        const fromTeamName = (TEAMS[t.from_team_id]?.name || '').toLowerCase();
        const fromOwner = (TEAMS[t.from_team_id]?.owner || '').toLowerCase();
        const typeStr = (t.transaction_type || '').toLowerCase();

        return (
          pName.includes(term) ||
          toTeamName.includes(term) ||
          toOwner.includes(term) ||
          fromTeamName.includes(term) ||
          fromOwner.includes(term) ||
          typeStr.includes(term)
        );
      }

      return true;
    });
  }, [allTransactions, searchTerm, selectedType, selectedTeam, selectedYear]);

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
              Search all adds, waiver claims, drops, and trades across the league history.
            </p>
          </div>

          <div className="text-right">
            <span className="text-xs font-bold uppercase text-gray-400 block tracking-wider">Total Records</span>
            <span className="text-lg font-black font-mono text-blue-800">
              {filteredTransactions.length.toLocaleString()} <span className="text-xs font-normal text-gray-500">of {allTransactions.length.toLocaleString()}</span>
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
                placeholder="e.g. Skenes, Ohtani, Adrian..."
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
              <option value="ALL">All Types</option>
              <option value="ADD">Free Agent Adds</option>
              <option value="WAIVER_ADD">Waiver Claims</option>
              <option value="DROP">Drops</option>
              <option value="TRADE">Trades</option>
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
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider w-40">
                  Date & Time
                </th>
                <th className="px-3 py-3 text-center text-xs font-bold text-gray-500 uppercase tracking-wider w-28">
                  Type
                </th>
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">
                  Player
                </th>
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">
                  Roster Movement
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
                  const typeConf = TYPE_CONFIG[t.transaction_type] || {
                    label: t.transaction_type || 'Move',
                    bg: 'bg-gray-100 text-gray-700 border-gray-200',
                    icon: '•'
                  };

                  const toTeam = TEAMS[t.to_team_id];
                  const fromTeam = TEAMS[t.from_team_id];

                  return (
                    <tr key={t.espn_transaction_id} className="hover:bg-blue-50/50 transition-colors">
                      {/* Date */}
                      <td className="px-4 py-3 whitespace-nowrap text-xs font-mono text-gray-500">
                        {formatDate(t.transaction_date)}
                      </td>

                      {/* Type Badge */}
                      <td className="px-3 py-3 text-center whitespace-nowrap">
                        <span className={`inline-flex items-center gap-1 text-[11px] font-bold px-2.5 py-1 rounded-full border ${typeConf.bg}`}>
                          <span>{typeConf.icon}</span>
                          <span>{typeConf.label}</span>
                        </span>
                      </td>

                      {/* Player Name */}
                      <td className="px-4 py-3">
                        <button
                          onClick={() => onPlayerClick?.(t.player_id, t.player_name)}
                          className="font-bold text-gray-900 hover:text-blue-700 hover:underline text-left cursor-pointer transition-colors"
                        >
                          {t.player_name}
                        </button>
                      </td>

                      {/* Movement */}
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2 flex-wrap text-xs">
                          {t.transaction_type === 'ADD' || t.transaction_type === 'WAIVER_ADD' ? (
                            <div className="flex items-center gap-1.5">
                              <span className="text-gray-400">Added to</span>
                              {toTeam ? (
                                <button
                                  onClick={() => onOwnerClick?.(toTeam)}
                                  className="flex items-center gap-1.5 font-bold text-gray-900 hover:text-blue-700 hover:underline"
                                >
                                  <TeamAvatar team={toTeam} size="sm" />
                                  <span>{toTeam.name}</span>
                                </button>
                              ) : (
                                <span className="font-semibold text-gray-600">Roster</span>
                              )}
                            </div>
                          ) : t.transaction_type === 'DROP' ? (
                            <div className="flex items-center gap-1.5">
                              <span className="text-gray-400">Dropped by</span>
                              {fromTeam ? (
                                <button
                                  onClick={() => onOwnerClick?.(fromTeam)}
                                  className="flex items-center gap-1.5 font-bold text-gray-900 hover:text-blue-700 hover:underline"
                                >
                                  <TeamAvatar team={fromTeam} size="sm" />
                                  <span>{fromTeam.name}</span>
                                </button>
                              ) : (
                                <span className="font-semibold text-gray-600">Roster</span>
                              )}
                            </div>
                          ) : t.transaction_type === 'TRADE' ? (
                            <div className="flex items-center gap-2">
                              {fromTeam && (
                                <button
                                  onClick={() => onOwnerClick?.(fromTeam)}
                                  className="flex items-center gap-1.5 font-bold text-gray-900 hover:text-blue-700"
                                >
                                  <TeamAvatar team={fromTeam} size="sm" />
                                  <span>{fromTeam.name}</span>
                                </button>
                              )}
                              <span className="text-purple-600 font-bold">➔</span>
                              {toTeam && (
                                <button
                                  onClick={() => onOwnerClick?.(toTeam)}
                                  className="flex items-center gap-1.5 font-bold text-gray-900 hover:text-blue-700"
                                >
                                  <TeamAvatar team={toTeam} size="sm" />
                                  <span>{toTeam.name}</span>
                                </button>
                              )}
                            </div>
                          ) : (
                            <span className="text-gray-600">{t.raw_type || '-'}</span>
                          )}
                        </div>
                      </td>

                      {/* Scoring Period */}
                      <td className="px-3 py-3 text-center whitespace-nowrap text-xs font-mono text-gray-400">
                        {t.scoring_period_id ? `P${t.scoring_period_id}` : '-'}
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
              Page <strong>{currentPage}</strong> of <strong>{totalPages}</strong> ({filteredTransactions.length.toLocaleString()} total moves)
            </span>
            <div className="flex items-center gap-1">
              <button
                onClick={() => handlePageChange(1)}
                disabled={currentPage === 1}
                className="px-2.5 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                « First
              </button>
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
                className="px-3 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                ‹ Prev
              </button>
              <span className="px-3 py-1 text-xs font-mono font-bold text-blue-700 bg-blue-50 border border-blue-200 rounded">
                {currentPage}
              </span>
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
                className="px-3 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                Next ›
              </button>
              <button
                onClick={() => handlePageChange(totalPages)}
                disabled={currentPage === totalPages}
                className="px-2.5 py-1 text-xs font-bold rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed"
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
