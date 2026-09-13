// src/views/DraftHistoryView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';

const GCS_DRAFT_HISTORY = 'https://storage.googleapis.com/fantasy-draft-2026/draft-history.json';

const AVAILABLE_YEARS = [
  'ALL',
  2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012
];

export default function DraftHistoryView({ onPlayerClick, onOwnerClick }) {
  const [loading, setLoading] = useState(true);
  const [allPicks, setAllPicks] = useState([]);
  const [selectedYear, setSelectedYear] = useState('ALL');
  const [selectedOwner, setSelectedOwner] = useState('ALL');
  const [selectedRound, setSelectedRound] = useState('ALL');
  const [keeperFilter, setKeeperFilter] = useState('ALL'); // 'ALL' | 'KEEPERS' | 'DRAFTED'
  const [searchQuery, setSearchQuery] = useState('');
  const [dataSource, setDataSource] = useState('');

  // Fetch draft history from Supabase, falling back to GCS
  useEffect(() => {
    let isMounted = true;

    async function loadDraftHistory() {
      setLoading(true);
      try {
        // 1. Try Supabase draft_picks table
        const { data, error } = await supabase
          .from('draft_picks')
          .select('*')
          .order('season_year', { ascending: false })
          .order('overall_pick', { ascending: true });

        if (!error && data && data.length > 0) {
          if (isMounted) {
            setAllPicks(data.map(d => ({
              id: d.id,
              year: d.season_year,
              round: d.round,
              pick: d.pick,
              overallPick: d.overall_pick,
              owner: d.team_owner,
              playerId: d.player_id,
              playerName: d.player_name,
              position: d.player_position || '-',
              team: d.player_team || '-',
              isKeeper: Boolean(d.is_keeper),
              pickedAt: d.picked_at
            })));
            setDataSource('Supabase Warehouse');
            setLoading(false);
            return;
          }
        }
      } catch (err) {
        console.warn('Could not load draft_picks from Supabase, trying GCS archive:', err);
      }

      // 2. Fallback to GCS draft-history.json
      try {
        const res = await fetch(GCS_DRAFT_HISTORY);
        if (res.ok) {
          const gcsData = await res.json();
          if (isMounted && Array.isArray(gcsData)) {
            const mapped = gcsData.map((d, index) => ({
              id: `gcs-${d.Year}-${d.Pick_Overall || index}`,
              year: parseInt(d.Year, 10),
              round: parseInt(d.Round, 10) || 1,
              pick: null,
              overallPick: parseInt(d.Pick_Overall, 10) || (index + 1),
              owner: d.Team_ID || 'Unknown',
              playerId: d.player_id ? parseInt(d.player_id, 10) : null,
              playerName: d.Player_Name || 'Unknown Player',
              position: d.Position || '-',
              team: d.Team || '-',
              isKeeper: String(d.Keeper).toLowerCase() === 'true',
              pickedAt: null
            }));
            setAllPicks(mapped);
            setDataSource('GCS Archive');
          }
        }
      } catch (err) {
        console.error('Error fetching fallback draft-history.json:', err);
      } finally {
        if (isMounted) setLoading(false);
      }
    }

    loadDraftHistory();

    return () => {
      isMounted = false;
    };
  }, []);

  // Distinct Owners
  const ownersList = useMemo(() => {
    const set = new Set();
    allPicks.forEach(p => {
      if (p.owner && p.owner.trim()) set.add(p.owner.trim());
    });
    return Array.from(set).sort();
  }, [allPicks]);

  // Filtered Picks
  const filteredPicks = useMemo(() => {
    return allPicks.filter(p => {
      if (selectedYear !== 'ALL' && p.year !== parseInt(selectedYear, 10)) {
        return false;
      }
      if (selectedOwner !== 'ALL' && p.owner !== selectedOwner) {
        return false;
      }
      if (selectedRound !== 'ALL') {
        const r = parseInt(selectedRound, 10);
        if (p.round !== r) return false;
      }
      if (keeperFilter === 'KEEPERS' && !p.isKeeper) return false;
      if (keeperFilter === 'DRAFTED' && p.isKeeper) return false;

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        const nameMatch = p.playerName && p.playerName.toLowerCase().includes(q);
        const ownerMatch = p.owner && p.owner.toLowerCase().includes(q);
        const teamMatch = p.team && p.team.toLowerCase().includes(q);
        if (!nameMatch && !ownerMatch && !teamMatch) return false;
      }

      return true;
    });
  }, [allPicks, selectedYear, selectedOwner, selectedRound, keeperFilter, searchQuery]);

  // Summary Metrics
  const summaryMetrics = useMemo(() => {
    const total = filteredPicks.length;
    const keepersCount = filteredPicks.filter(p => p.isKeeper).length;
    const uniquePlayers = new Set(filteredPicks.map(p => p.playerName)).size;
    const uniqueYears = new Set(filteredPicks.map(p => p.year)).size;
    return { total, keepersCount, uniquePlayers, uniqueYears };
  }, [filteredPicks]);

  return (
    <div className="space-y-6 animate-fade-in pb-12">
      {/* Header Banner */}
      <div className="relative overflow-hidden bg-gradient-to-br from-slate-900 via-slate-900/95 to-slate-950 border border-slate-800 rounded-3xl p-6 sm:p-8 shadow-2xl backdrop-blur-xl">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-2.5 mb-2">
              <span className="px-2.5 py-0.5 rounded-full text-xs font-black uppercase tracking-wider bg-amber-500/20 text-amber-400 border border-amber-500/30">
                Multi-Year Archive (2012–2025)
              </span>
              {dataSource && (
                <span className="text-[11px] font-bold text-slate-500">
                  Source: {dataSource}
                </span>
              )}
            </div>
            <h1 className="text-3xl sm:text-4xl font-black text-white tracking-tight flex items-center gap-3">
              <span>🎯</span>
              <span>Draft History Archive</span>
            </h1>
            <p className="text-slate-400 text-sm mt-1 max-w-2xl">
              Inspect historical draft results across 14 seasons. Search any player to trace their selection history, round values, and keeper statuses.
            </p>
          </div>

          {/* Quick Stats Chips */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-3 text-center">
              <div className="text-[11px] font-bold uppercase tracking-wider text-slate-400">Picks</div>
              <div className="text-xl font-black text-white">{summaryMetrics.total}</div>
            </div>
            <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-3 text-center">
              <div className="text-[11px] font-bold uppercase tracking-wider text-amber-400">Keepers</div>
              <div className="text-xl font-black text-amber-400">{summaryMetrics.keepersCount}</div>
            </div>
            <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-3 text-center">
              <div className="text-[11px] font-bold uppercase tracking-wider text-blue-400">Players</div>
              <div className="text-xl font-black text-blue-400">{summaryMetrics.uniquePlayers}</div>
            </div>
            <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-3 text-center">
              <div className="text-[11px] font-bold uppercase tracking-wider text-emerald-400">Seasons</div>
              <div className="text-xl font-black text-emerald-400">{summaryMetrics.uniqueYears}</div>
            </div>
          </div>
        </div>

        {/* Year Selector Pills */}
        <div className="mt-6 pt-5 border-t border-slate-800/80 flex items-center gap-1.5 overflow-x-auto pb-1 no-scrollbar">
          {AVAILABLE_YEARS.map(yr => {
            const isSelected = String(selectedYear) === String(yr);
            return (
              <button
                key={yr}
                type="button"
                onClick={() => setSelectedYear(yr)}
                className={`px-3 py-1.5 rounded-xl text-xs font-black transition cursor-pointer whitespace-nowrap ${
                  isSelected
                    ? 'bg-amber-500 text-slate-950 shadow-md font-extrabold scale-105'
                    : 'bg-slate-800/60 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-700/50'
                }`}
              >
                {yr === 'ALL' ? 'All Seasons' : yr}
              </button>
            );
          })}
        </div>
      </div>

      {/* Filter Controls Bar */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 shadow-xl backdrop-blur-md flex flex-col md:flex-row items-stretch md:items-center justify-between gap-4">
        {/* Search Box */}
        <div className="relative flex-1">
          <input
            type="text"
            placeholder="Search by player name, owner, or MLB team..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-amber-500/50"
          />
          {searchQuery && (
            <button
              type="button"
              onClick={() => setSearchQuery('')}
              className="absolute right-3 top-2.5 text-xs text-slate-400 hover:text-white cursor-pointer"
            >
              ✕
            </button>
          )}
        </div>

        {/* Dropdowns */}
        <div className="flex items-center gap-3 flex-wrap">
          {/* Owner Filter */}
          <div className="flex items-center gap-1.5 text-xs">
            <span className="text-slate-400 font-bold uppercase text-[10px]">Owner:</span>
            <select
              value={selectedOwner}
              onChange={(e) => setSelectedOwner(e.target.value)}
              className="bg-slate-950 border border-slate-800 text-slate-200 text-xs rounded-xl px-3 py-2 font-bold focus:outline-none focus:border-amber-500/50"
            >
              <option value="ALL">All Owners ({ownersList.length})</option>
              {ownersList.map(o => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>

          {/* Round Filter */}
          <div className="flex items-center gap-1.5 text-xs">
            <span className="text-slate-400 font-bold uppercase text-[10px]">Round:</span>
            <select
              value={selectedRound}
              onChange={(e) => setSelectedRound(e.target.value)}
              className="bg-slate-950 border border-slate-800 text-slate-200 text-xs rounded-xl px-3 py-2 font-bold focus:outline-none focus:border-amber-500/50"
            >
              <option value="ALL">All Rounds</option>
              {Array.from({ length: 25 }, (_, i) => i + 1).map(r => (
                <option key={r} value={r}>Round {r}</option>
              ))}
            </select>
          </div>

          {/* Keeper Filter */}
          <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-bold">
            <button
              type="button"
              onClick={() => setKeeperFilter('ALL')}
              className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                keeperFilter === 'ALL' ? 'bg-slate-800 text-white shadow-xs' : 'text-slate-400 hover:text-white'
              }`}
            >
              All
            </button>
            <button
              type="button"
              onClick={() => setKeeperFilter('KEEPERS')}
              className={`px-2.5 py-1 rounded-lg transition cursor-pointer flex items-center gap-1 ${
                keeperFilter === 'KEEPERS' ? 'bg-amber-500 text-slate-950 font-black shadow-xs' : 'text-slate-400 hover:text-white'
              }`}
            >
              <span>💎</span>
              <span>Keepers</span>
            </button>
            <button
              type="button"
              onClick={() => setKeeperFilter('DRAFTED')}
              className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                keeperFilter === 'DRAFTED' ? 'bg-blue-600 text-white shadow-xs' : 'text-slate-400 hover:text-white'
              }`}
            >
              Drafted
            </button>
          </div>
        </div>
      </div>

      {/* Draft Table */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl shadow-xl overflow-hidden backdrop-blur-md">
        {loading ? (
          <div className="py-24 text-center">
            <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-amber-500 mb-3"></div>
            <div className="text-slate-400 text-sm font-semibold">Loading historical draft picks...</div>
          </div>
        ) : filteredPicks.length === 0 ? (
          <div className="py-16 text-center text-slate-400 text-sm">
            No draft picks found matching the active filters.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950/80 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-4 text-center w-16">Year</th>
                  <th className="py-3 px-3 text-center w-16">Ovr</th>
                  <th className="py-3 px-3 text-center w-16">Rnd</th>
                  <th className="py-3 px-4">Player Name</th>
                  <th className="py-3 px-4">Owner</th>
                  <th className="py-3 px-3 text-center w-24">Type</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {filteredPicks.map((pick) => (
                  <tr key={pick.id} className="hover:bg-slate-800/40 transition">
                    <td className="py-2.5 px-4 text-center font-bold text-slate-400">
                      {pick.year}
                    </td>
                    <td className="py-2.5 px-3 text-center font-mono font-bold text-slate-400">
                      #{pick.overallPick}
                    </td>
                    <td className="py-2.5 px-3 text-center font-mono text-slate-400">
                      R{pick.round}
                    </td>
                    <td className="py-2.5 px-4">
                      <button
                        type="button"
                        onClick={() => onPlayerClick && onPlayerClick(pick.playerId, pick.playerName)}
                        className="text-left font-black text-white hover:text-amber-400 transition cursor-pointer flex items-center gap-1.5"
                      >
                        <span>{pick.playerName}</span>
                        {pick.team && pick.team !== '-' && (
                          <span className="text-[10px] text-slate-500 font-medium">({pick.team})</span>
                        )}
                      </button>
                    </td>
                    <td className="py-2.5 px-4">
                      <button
                        type="button"
                        onClick={() => onOwnerClick && onOwnerClick({ name: pick.owner })}
                        className="font-bold text-slate-300 hover:text-blue-400 transition cursor-pointer"
                      >
                        {pick.owner}
                      </button>
                    </td>
                    <td className="py-2.5 px-3 text-center">
                      {pick.isKeeper ? (
                        <span className="px-2 py-0.5 rounded-full text-[10px] font-black uppercase tracking-wider bg-amber-500/20 text-amber-400 border border-amber-500/30">
                          💎 Keeper
                        </span>
                      ) : (
                        <span className="text-slate-500 text-[11px]">Pick</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
