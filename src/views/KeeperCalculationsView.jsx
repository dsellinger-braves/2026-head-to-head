// src/views/KeeperCalculationsView.jsx
import React, { useState, useMemo, useEffect } from 'react';
import defaultCalculations from '../data/keeperCalculations.json';
import { getPlayerHeadshotUrl, handleHeadshotError, updateGlobalPlayerLookup } from '../utils/headshotUtils';

const POSITIONS = ['ALL', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', 'DH'];
const LEAGUE_MANAGERS = ['Adrian', 'Alex', 'Anil', 'Daniel', 'Garrett', 'Mark', 'Preston', 'Tim', 'Will'];
const PRICE_TIERS = [
  { id: 'ALL', label: 'All Tiers' },
  { id: 'TIER_TOP', label: 'Elite ($25+)' },
  { id: 'TIER_MID', label: 'Mid-Tier ($10 - $24)' },
  { id: 'TIER_LOW', label: 'Value ($1 - $9)' },
  { id: 'TIER_FREE', label: 'End of Bench ($0)' }
];

export default function KeeperCalculationsView({
  onPlayerClick,
  seasonYear = 2027,
  onSeasonYearChange
}) {
  const [data] = useState(defaultCalculations);
  const [selectedBenchmarkYear, setSelectedBenchmarkYear] = useState('2026');
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [modalYear, setModalYear] = useState('y1'); // 'y1', 'y2', 'y3'

  // Filter states
  const [search, setSearch] = useState('');
  const [posFilter, setPosFilter] = useState('ALL');
  const [ownerFilter, setOwnerFilter] = useState('ALL');
  const [priceTier, setPriceTier] = useState('ALL');
  const [sortConfig, setSortConfig] = useState({ key: 'overall_rank', direction: 'asc' });

  const benchmarks = data?.benchmarks?.[selectedBenchmarkYear] || data?.benchmarks?.['2026'] || {};
  const playersList = useMemo(() => data?.players || [], [data]);

  useEffect(() => {
    if (playersList.length > 0) {
      updateGlobalPlayerLookup(playersList);
    }
  }, [playersList]);

  // Filter players
  const filteredPlayers = useMemo(() => {
    return playersList.filter(p => {
      if (search) {
        const q = search.toLowerCase();
        const matchesName = (p.player_name || '').toLowerCase().includes(q);
        const matchesTeam = (p.team || '').toLowerCase().includes(q);
        const matchesPos = (p.position || '').toLowerCase().includes(q);
        if (!matchesName && !matchesTeam && !matchesPos) return false;
      }

      if (posFilter !== 'ALL') {
        const pos = p.position || '';
        if (posFilter === 'OF') {
          if (!pos.includes('OF') && !pos.includes('LF') && !pos.includes('CF') && !pos.includes('RF')) return false;
        } else if (posFilter === 'SP') {
          if (!pos.includes('SP') && p.pitcher_role !== 'SP') return false;
        } else if (posFilter === 'RP') {
          if (!pos.includes('RP') && p.pitcher_role !== 'RP') return false;
        } else {
          if (!pos.includes(posFilter)) return false;
        }
      }

      if (ownerFilter !== 'ALL') {
        const o = p.fantasy_owner || 'Available';
        if (ownerFilter === 'ROSTERED' && o === 'Available') return false;
        if (ownerFilter === 'AVAILABLE' && o !== 'Available') return false;
        if (ownerFilter !== 'ROSTERED' && ownerFilter !== 'AVAILABLE' && o !== ownerFilter) return false;
      }

      if (priceTier !== 'ALL') {
        const price = p.overall_price || 0;
        if (priceTier === 'TIER_TOP' && price < 25) return false;
        if (priceTier === 'TIER_MID' && (price < 10 || price >= 25)) return false;
        if (priceTier === 'TIER_LOW' && (price < 1 || price >= 10)) return false;
        if (priceTier === 'TIER_FREE' && price > 0) return false;
      }

      return true;
    });
  }, [playersList, search, posFilter, ownerFilter, priceTier]);

  // Sort players
  const sortedPlayers = useMemo(() => {
    const list = [...filteredPlayers];
    list.sort((a, b) => {
      let aVal = a[sortConfig.key];
      let bVal = b[sortConfig.key];

      // Handle nested year fields
      if (sortConfig.key === 'y1_pr') aVal = a.y1?.pr;
      if (sortConfig.key === 'y1_pr') bVal = b.y1?.pr;
      if (sortConfig.key === 'y1_rank') aVal = a.y1?.rank;
      if (sortConfig.key === 'y1_rank') bVal = b.y1?.rank;

      if (sortConfig.key === 'y2_pr') aVal = a.y2?.pr;
      if (sortConfig.key === 'y2_pr') bVal = b.y2?.pr;
      if (sortConfig.key === 'y2_rank') aVal = a.y2?.rank;
      if (sortConfig.key === 'y2_rank') bVal = b.y2?.rank;

      if (sortConfig.key === 'y3_pr') aVal = a.y3?.pr;
      if (sortConfig.key === 'y3_pr') bVal = b.y3?.pr;
      if (sortConfig.key === 'y3_rank') aVal = a.y3?.rank;
      if (sortConfig.key === 'y3_rank') bVal = b.y3?.rank;

      if (sortConfig.key === 'player_name' || sortConfig.key === 'position' || sortConfig.key === 'team') {
        return sortConfig.direction === 'asc'
          ? (aVal || '').localeCompare(bVal || '')
          : (bVal || '').localeCompare(aVal || '');
      }

      if (sortConfig.key === 'fantasy_owner') {
        const oA = a.fantasy_owner === 'Available' ? 'ZZZ' : (a.fantasy_owner || 'ZZZ');
        const oB = b.fantasy_owner === 'Available' ? 'ZZZ' : (b.fantasy_owner || 'ZZZ');
        return sortConfig.direction === 'asc'
          ? oA.localeCompare(oB)
          : oB.localeCompare(oA);
      }

      const numA = parseFloat(aVal) || 0;
      const numB = parseFloat(bVal) || 0;

      // Primary numerical sort
      if (numA !== numB) {
        return sortConfig.direction === 'asc' ? numA - numB : numB - numA;
      }

      // Tie-breaker: For all players tied (and specifically when both are $0):
      // continue to sort by projected PR descending across all 3 years!
      const oprA = parseFloat(a.overall_pr) || 0;
      const oprB = parseFloat(b.overall_pr) || 0;
      if (Math.abs(oprB - oprA) > 0.0001) return oprB - oprA;

      const y1A = parseFloat(a.y1?.pr) || 0;
      const y1B = parseFloat(b.y1?.pr) || 0;
      if (Math.abs(y1B - y1A) > 0.0001) return y1B - y1A;

      const y2A = parseFloat(a.y2?.pr) || 0;
      const y2B = parseFloat(b.y2?.pr) || 0;
      if (Math.abs(y2B - y2A) > 0.0001) return y2B - y2A;

      const y3A = parseFloat(a.y3?.pr) || 0;
      const y3B = parseFloat(b.y3?.pr) || 0;
      if (Math.abs(y3B - y3A) > 0.0001) return y3B - y3A;

      return (a.overall_rank || 999) - (b.overall_rank || 999);
    });
    return list;
  }, [filteredPlayers, sortConfig]);

  const requestSort = (key) => {
    setSortConfig(prev => {
      if (prev.key === key) {
        return { key, direction: prev.direction === 'asc' ? 'desc' : 'asc' };
      }
      // Numerical metrics, prices, and PRs default to descending on first click; ranks and names default to ascending
      const defaultDir = (key === 'overall_rank' || key === 'player_name' || key === 'y1_rank' || key === 'y2_rank' || key === 'y3_rank') ? 'asc' : 'desc';
      return { key, direction: defaultDir };
    });
  };

  const getSortIcon = (key) => {
    if (sortConfig.key !== key) return '↕';
    return sortConfig.direction === 'asc' ? '▲' : '▼';
  };

  return (
    <div className="space-y-6 animate-fadeIn pb-12">
      {/* Top Header Card */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div className="absolute top-0 right-0 w-96 h-96 bg-emerald-500/5 rounded-full blur-3xl pointer-events-none" />

        <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4 relative z-10">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-2xl">🏷️</span>
              <h1 className="text-2xl font-black tracking-tight text-white">
                Keeper Prices & Valuation Math
              </h1>
            </div>
            <p className="text-slate-400 text-sm mt-1 max-w-3xl leading-relaxed">
              Transparent multi-year Player Rating (PR) breakdown with qualified category averages, standard deviations,
              and year-by-year valuation models.
            </p>
          </div>

          <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
            {onSeasonYearChange && (
              <div className="inline-flex bg-slate-950 p-1.5 rounded-xl border border-slate-800 shadow-inner gap-1">
                <button
                  type="button"
                  onClick={() => onSeasonYearChange(2027)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                    seasonYear === 2027
                      ? 'bg-emerald-500 text-slate-950 shadow-md font-black'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  🚀 2027 Projections
                </button>
                <button
                  type="button"
                  onClick={() => onSeasonYearChange(2026)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                    seasonYear === 2026
                      ? 'bg-purple-600 text-white shadow-md font-black'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  🏛️ 2026 Archive
                </button>
              </div>
            )}

            {/* Model Formula Banner */}
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-xs shadow-inner shrink-0">
              <div className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-1">
                📐 Multi-Year Blend Formula
              </div>
              <div className="font-mono text-emerald-400 font-bold">
                Total PR = 0.60 × Y1 (2026) + 0.30 × Y2 (2027) + 0.10 × Y3 (2028)
              </div>
              <div className="text-[10px] text-slate-500 mt-1">
                Overall Rank mapped to pricing curve ($40 down to $0). 6 Keepers starting 2027.
              </div>
            </div>
          </div>
        </div>

        {/* Category Benchmarks (Averages & Standard Deviations) */}
        <div className="mt-6 pt-5 border-t border-slate-800/80">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
            <div className="flex items-center gap-2">
              <span className="text-base">📊</span>
              <h3 className="text-sm font-black text-white uppercase tracking-wider">
                Category Benchmarks (Means & Standard Deviations)
              </h3>
            </div>

            {/* Benchmark Year Selector */}
            <div className="inline-flex bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold gap-1">
              {['2026', '2027', '2028'].map(yr => (
                <button
                  key={yr}
                  type="button"
                  onClick={() => setSelectedBenchmarkYear(yr)}
                  className={`px-3 py-1 rounded-md transition-all cursor-pointer ${
                    selectedBenchmarkYear === yr
                      ? 'bg-emerald-500 text-slate-950 font-black shadow-xs'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  {yr} {yr === '2026' ? '(Depth Charts)' : yr === '2027' ? '(ZiPS +1)' : '(ZiPS +2)'}
                </button>
              ))}
            </div>
          </div>

          {/* Benchmarks Grid */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            {/* Batting Benchmarks */}
            <div className="bg-slate-950/80 border border-slate-800/90 rounded-xl p-3.5">
              <div className="flex items-center justify-between pb-2 mb-2 border-b border-slate-800/80">
                <span className="text-xs font-black text-amber-400 uppercase tracking-wider flex items-center gap-1.5">
                  <span>🏏</span> Batting (Qual ≥ {benchmarks.min_ab || 400} AB)
                </span>
                <span className="text-[10px] text-slate-500">5 Categories</span>
              </div>
              <div className="grid grid-cols-5 gap-1.5 text-center">
                {Object.entries(benchmarks.batting || {}).map(([cat, b]) => (
                  <div key={cat} className="bg-slate-900/90 border border-slate-800/80 rounded p-1.5">
                    <div className="text-[10px] font-black text-slate-400">{cat}</div>
                    <div className="text-xs font-bold text-white mt-0.5">μ {b.mean}</div>
                    <div className="text-[10px] text-slate-400">σ {b.std}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Starting Pitching Benchmarks */}
            <div className="bg-slate-950/80 border border-slate-800/90 rounded-xl p-3.5">
              <div className="flex items-center justify-between pb-2 mb-2 border-b border-slate-800/80">
                <span className="text-xs font-black text-blue-400 uppercase tracking-wider flex items-center gap-1.5">
                  <span>⚾</span> Starting Pitching (Qual ≥ {benchmarks.min_sp_ip || 130} IP)
                </span>
                <span className="text-[10px] text-slate-500">SP Roles</span>
              </div>
              <div className="grid grid-cols-4 gap-1.5 text-center">
                {Object.entries(benchmarks.sp || {}).map(([cat, b]) => (
                  <div key={cat} className="bg-slate-900/90 border border-slate-800/80 rounded p-1.5">
                    <div className="text-[10px] font-black text-slate-400">{cat}</div>
                    <div className="text-xs font-bold text-white mt-0.5">μ {b.mean}</div>
                    <div className="text-[10px] text-slate-400">σ {b.std}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Relief Pitching Benchmarks */}
            <div className="bg-slate-950/80 border border-slate-800/90 rounded-xl p-3.5">
              <div className="flex items-center justify-between pb-2 mb-2 border-b border-slate-800/80">
                <span className="text-xs font-black text-purple-400 uppercase tracking-wider flex items-center gap-1.5">
                  <span>🔥</span> Relief Pitching (Qual ≥ {benchmarks.min_rp_ip || 45} IP)
                </span>
                <div className="flex items-center gap-1">
                  <span className="text-[10px] bg-amber-500/20 text-amber-300 border border-amber-500/40 px-1.5 py-0.5 rounded font-bold" title="SV+HD benchmark only includes relievers projected for ≥25 SV+HD in Year 1">
                    SV+HD ≥25 Y1
                  </span>
                  <span className="text-[10px] bg-purple-500/20 text-purple-300 border border-purple-500/40 px-1.5 py-0.5 rounded font-bold" title="2.5x standard deviation nerfing factor applied to RP stats">
                    2.5x σ Nerf
                  </span>
                </div>
              </div>
              <div className="grid grid-cols-4 gap-1.5 text-center">
                {Object.entries(benchmarks.rp || {}).map(([cat, b]) => (
                  <div key={cat} className="bg-slate-900/90 border border-slate-800/80 rounded p-1.5">
                    <div className="text-[10px] font-black text-slate-400 flex items-center justify-center gap-1">
                      <span>{cat}</span>
                      {cat === 'SV_HD' && (
                        <span className="text-[8px] text-amber-400 font-bold" title="Only relievers projected for ≥25 SV+HD in Year 1 are included in μ and σ">≥25</span>
                      )}
                    </div>
                    <div className="text-xs font-bold text-white mt-0.5">μ {b.mean}</div>
                    <div className="text-[10px] text-purple-300 font-semibold" title={`Effective nerfed σ = ${b.eff_std || b.std} (raw: ${b.std})`}>
                      σ {b.eff_std || b.std}
                    </div>
                    {b.eff_std && (
                      <div className="text-[8px] text-slate-500 font-mono">raw {b.std}</div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Proportional Scaling & Nerf Factor Explanatory Sub-banner */}
          <div className="mt-3.5 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-400 bg-slate-950/60 border border-slate-800/80 rounded-xl px-3.5 py-2">
            <div className="flex items-center gap-2">
              <span className="text-teal-400 font-bold flex items-center gap-1">
                <span>📈</span> Proportional IP Scaling:
              </span>
              <span>FanGraphs 2027 &amp; 2028 omit QS and SV+HD; values are scaled proportionally to projected IP changes.</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-amber-400 font-bold flex items-center gap-1">
                <span>🛡️</span> SV+HD High-Leverage Benchmark:
              </span>
              <span>Average and std dev for SV+HD only include relievers projected for ≥25 SV+HD in Year 1.</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-purple-400 font-bold flex items-center gap-1">
                <span>⚖️</span> RP 2.5x σ Nerf:
              </span>
              <span>Std dev multiplied by 2.5x for RP across SO, SV+HD, ERA, and WHIP to balance reliever valuations.</span>
            </div>
          </div>
        </div>
      </div>

      {/* Filter Toolbar */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-md flex flex-wrap items-center justify-between gap-3">
        {/* Positional Tabs */}
        <div className="flex flex-wrap items-center gap-1">
          {POSITIONS.map(pos => (
            <button
              key={pos}
              type="button"
              onClick={() => setPosFilter(pos)}
              className={`px-2.5 py-1 rounded-md text-xs font-bold transition-colors cursor-pointer ${
                posFilter === pos
                  ? 'bg-emerald-600 text-white shadow-xs'
                  : 'bg-slate-950 text-slate-400 hover:text-white hover:bg-slate-800'
              }`}
            >
              {pos}
            </button>
          ))}
        </div>

        {/* Search & Price Filter */}
        <div className="flex flex-wrap items-center gap-2 w-full md:w-auto">
          <input
            type="text"
            placeholder="Search player, team, pos..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500 focus:ring-1 focus:ring-emerald-500 w-full sm:w-56"
          />

          {/* Owner Filter */}
          <select
            value={ownerFilter}
            onChange={(e) => setOwnerFilter(e.target.value)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-emerald-500 cursor-pointer"
          >
            <option value="ALL">All Owners</option>
            <option value="ROSTERED">Rostered Only</option>
            <option value="AVAILABLE">Free Agents (FA)</option>
            <optgroup label="Fantasy Managers">
              {LEAGUE_MANAGERS.map(owner => (
                <option key={owner} value={owner}>{owner}</option>
              ))}
            </optgroup>
          </select>

          <select
            value={priceTier}
            onChange={(e) => setPriceTier(e.target.value)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-emerald-500 cursor-pointer"
          >
            {PRICE_TIERS.map(t => (
              <option key={t.id} value={t.id}>{t.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Interactive Table Showing Player Name, Annual PRs, Annual Ranks, and Overall Rank */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl shadow-xl overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black select-none">
                <th
                  onClick={() => requestSort('overall_rank')}
                  className="py-3 px-3 text-center cursor-pointer hover:text-white transition-colors w-16"
                  title="Sorted by 60/30/10 weighted blend PR"
                >
                  Overall {getSortIcon('overall_rank')}
                </th>
                <th
                  onClick={() => requestSort('player_name')}
                  className="py-3 px-4 cursor-pointer hover:text-white transition-colors"
                >
                  Player {getSortIcon('player_name')}
                </th>
                <th className="py-3 px-3 text-center">Pos</th>
                <th className="py-3 px-3 text-center">MLB</th>
                <th
                  onClick={() => requestSort('fantasy_owner')}
                  className="py-3 px-3 text-center cursor-pointer hover:text-white transition-colors"
                  title="Owning Fantasy Manager"
                >
                  Owner {getSortIcon('fantasy_owner')}
                </th>
                <th
                  onClick={() => requestSort('overall_price')}
                  className="py-3 px-3 text-right cursor-pointer hover:text-white transition-colors"
                >
                  Price {getSortIcon('overall_price')}
                </th>
                <th
                  onClick={() => requestSort('overall_pr')}
                  className="py-3 px-3 text-right cursor-pointer hover:text-white transition-colors"
                  title="Multi-year weighted PR (60% Y1 + 30% Y2 + 10% Y3)"
                >
                  Overall PR {getSortIcon('overall_pr')}
                </th>

                {/* Annual PR & Ranks */}
                <th
                  onClick={() => requestSort('y1_pr')}
                  className="py-3 px-3 text-right cursor-pointer hover:text-white transition-colors bg-slate-900/50"
                  title="Year 1: 2026 FanGraphs Depth Charts PR (60% weight)"
                >
                  2026 PR {getSortIcon('y1_pr')}
                </th>
                <th
                  onClick={() => requestSort('y1_rank')}
                  className="py-3 px-2 text-center cursor-pointer hover:text-white transition-colors bg-slate-900/50"
                  title="Annual Rank in 2026"
                >
                  '26 Rank {getSortIcon('y1_rank')}
                </th>

                <th
                  onClick={() => requestSort('y2_pr')}
                  className="py-3 px-3 text-right cursor-pointer hover:text-white transition-colors bg-slate-950/60"
                  title="Year 2: 2027 ZiPS Year + 1 PR (30% weight)"
                >
                  2027 PR {getSortIcon('y2_pr')}
                </th>
                <th
                  onClick={() => requestSort('y2_rank')}
                  className="py-3 px-2 text-center cursor-pointer hover:text-white transition-colors bg-slate-950/60"
                  title="Annual Rank in 2027"
                >
                  '27 Rank {getSortIcon('y2_rank')}
                </th>

                <th
                  onClick={() => requestSort('y3_pr')}
                  className="py-3 px-3 text-right cursor-pointer hover:text-white transition-colors bg-slate-900/50"
                  title="Year 3: 2028 ZiPS Year + 2 PR (10% weight)"
                >
                  2028 PR {getSortIcon('y3_pr')}
                </th>
                <th
                  onClick={() => requestSort('y3_rank')}
                  className="py-3 px-2 text-center cursor-pointer hover:text-white transition-colors bg-slate-900/50"
                  title="Annual Rank in 2028"
                >
                  '28 Rank {getSortIcon('y3_rank')}
                </th>

                <th className="py-3 px-4 text-center">Inspect Math</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
              {sortedPlayers.slice(0, 200).map(player => {
                const headshotUrl = getPlayerHeadshotUrl(player);
                const isTop10 = player.overall_rank <= 10;
                const isTop25 = player.overall_rank <= 25;

                return (
                  <tr
                    key={player.player_id}
                    onClick={() => {
                      setSelectedPlayer(player);
                      setModalYear('y1');
                    }}
                    className="hover:bg-slate-800/50 transition-colors cursor-pointer group"
                  >
                    {/* Overall Rank */}
                    <td className="py-3 px-3 text-center">
                      <span className={`inline-block px-2 py-0.5 rounded font-black text-xs ${
                        isTop10
                          ? 'bg-amber-400 text-slate-950 shadow-xs'
                          : isTop25
                          ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                          : 'text-slate-400 group-hover:text-white'
                      }`}>
                        #{player.overall_rank}
                      </span>
                    </td>

                    {/* Player Info with Headshot */}
                    <td className="py-3 px-4">
                      <div className="flex items-center gap-2.5">
                        <img
                          src={headshotUrl}
                          alt={player.player_name}
                          onError={(e) => handleHeadshotError(e, player)}
                          className="w-7 h-7 rounded-full object-cover bg-slate-800 border border-slate-700/60 shrink-0"
                        />
                        <div>
                          <span
                            onClick={(e) => {
                              if (onPlayerClick) {
                                e.stopPropagation();
                                onPlayerClick(player.player_id, player.player_name);
                              }
                            }}
                            className="font-bold text-white group-hover:text-emerald-400 hover:underline transition-colors cursor-pointer"
                          >
                            {player.player_name}
                          </span>
                        </div>
                      </div>
                    </td>

                    {/* Pos */}
                    <td className="py-3 px-3 text-center">
                      <span className="px-2 py-0.5 rounded text-[11px] font-bold bg-slate-800 text-slate-300 border border-slate-700/60">
                        {player.position || 'UTIL'}
                      </span>
                    </td>

                    {/* MLB Team */}
                    <td className="py-3 px-3 text-center text-slate-400 font-semibold">
                      {player.team || 'FA'}
                    </td>

                    {/* Fantasy Team Owner */}
                    <td className="py-3 px-3 text-center">
                      {player.fantasy_owner && player.fantasy_owner !== 'Available' ? (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-bold bg-sky-500/15 text-sky-300 border border-sky-500/30">
                          <span className="text-[9px]">👤</span> {player.fantasy_owner}
                        </span>
                      ) : (
                        <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-slate-950 text-slate-500 border border-slate-800">
                          FA
                        </span>
                      )}
                    </td>

                    {/* Overall Price */}
                    <td className="py-3 px-3 text-right font-black text-amber-300">
                      ${player.overall_price}
                    </td>

                    {/* Overall PR */}
                    <td className="py-3 px-3 text-right font-black">
                      <span className={player.overall_pr >= 10 ? 'text-amber-400' : player.overall_pr >= 5 ? 'text-emerald-400' : player.overall_pr >= 0 ? 'text-teal-300' : 'text-slate-500'}>
                        {player.overall_pr > 0 ? `+${player.overall_pr.toFixed(2)}` : player.overall_pr.toFixed(2)}
                      </span>
                    </td>

                    {/* 2026 PR */}
                    <td className="py-3 px-3 text-right font-bold text-slate-300 bg-slate-900/40">
                      {player.y1?.pr > 0 ? `+${player.y1.pr.toFixed(2)}` : player.y1?.pr.toFixed(2)}
                    </td>
                    {/* 2026 Rank */}
                    <td className="py-3 px-2 text-center text-slate-400 font-semibold bg-slate-900/40 text-[11px]">
                      #{player.y1?.rank}
                    </td>

                    {/* 2027 PR */}
                    <td className="py-3 px-3 text-right font-bold text-slate-300 bg-slate-950/40">
                      {player.y2?.pr > 0 ? `+${player.y2.pr.toFixed(2)}` : player.y2?.pr.toFixed(2)}
                    </td>
                    {/* 2027 Rank */}
                    <td className="py-3 px-2 text-center text-slate-400 font-semibold bg-slate-950/40 text-[11px]">
                      #{player.y2?.rank}
                    </td>

                    {/* 2028 PR */}
                    <td className="py-3 px-3 text-right font-bold text-slate-300 bg-slate-900/40">
                      {player.y3?.pr > 0 ? `+${player.y3.pr.toFixed(2)}` : player.y3?.pr.toFixed(2)}
                    </td>
                    {/* 2028 Rank */}
                    <td className="py-3 px-2 text-center text-slate-400 font-semibold bg-slate-900/40 text-[11px]">
                      #{player.y3?.rank}
                    </td>

                    {/* Action Button */}
                    <td className="py-3 px-4 text-center">
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedPlayer(player);
                          setModalYear('y1');
                        }}
                        className="px-2.5 py-1 bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-400 hover:text-emerald-300 border border-emerald-500/30 rounded-lg text-[11px] font-bold transition-all cursor-pointer inline-flex items-center gap-1"
                      >
                        <span>🔬</span>
                        <span>Details</span>
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {sortedPlayers.length > 200 && (
          <div className="p-4 text-center text-xs text-slate-500 border-t border-slate-800 bg-slate-950">
            Showing top 200 of {sortedPlayers.length} players. Use search or position filters to inspect others.
          </div>
        )}
      </div>

      {/* Drill-down Modal: Detailed Player Calculation by Year & Element */}
      {selectedPlayer && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-fadeIn">
          <div
            className="bg-slate-900 border border-slate-800 rounded-2xl max-w-4xl w-full max-h-[90vh] overflow-y-auto shadow-2xl flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Modal Header */}
            <div className="p-6 border-b border-slate-800 flex items-start justify-between gap-4 sticky top-0 bg-slate-900/95 backdrop-blur-md z-10">
              <div className="flex items-center gap-4">
                <img
                  src={getPlayerHeadshotUrl(selectedPlayer)}
                  alt={selectedPlayer.player_name}
                  onError={(e) => handleHeadshotError(e, selectedPlayer)}
                  className="w-14 h-14 rounded-2xl object-cover bg-slate-950 border border-slate-700 shadow-md shrink-0"
                />
                <div>
                  <div className="flex items-center gap-2">
                    <h2 className="text-xl font-black text-white">{selectedPlayer.player_name}</h2>
                    <span className="px-2 py-0.5 rounded text-xs font-bold bg-slate-800 text-slate-300 border border-slate-700">
                      {selectedPlayer.position}
                    </span>
                    <span className="text-xs text-slate-400 font-bold">{selectedPlayer.team}</span>
                    {selectedPlayer.fantasy_owner && selectedPlayer.fantasy_owner !== 'Available' ? (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-bold bg-sky-500/20 text-sky-300 border border-sky-500/40">
                        👤 {selectedPlayer.fantasy_owner}
                      </span>
                    ) : (
                      <span className="px-2 py-0.5 rounded text-xs font-medium bg-slate-800 text-slate-400 border border-slate-700">
                        Free Agent
                      </span>
                    )}
                  </div>
                  <div className="flex flex-wrap items-center gap-3 mt-1 text-xs">
                    <span className="text-amber-400 font-black">
                      Overall Rank: #{selectedPlayer.overall_rank}
                    </span>
                    <span className="text-slate-400">•</span>
                    <span className="text-emerald-400 font-black">
                      Keeper Price: ${selectedPlayer.overall_price}
                    </span>
                    <span className="text-slate-400">•</span>
                    <span className="text-teal-300 font-black">
                      Overall PR: {selectedPlayer.overall_pr > 0 ? `+${selectedPlayer.overall_pr.toFixed(2)}` : selectedPlayer.overall_pr.toFixed(2)}
                    </span>
                  </div>
                </div>
              </div>

              <button
                type="button"
                onClick={() => setSelectedPlayer(null)}
                className="w-8 h-8 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white flex items-center justify-center font-bold text-sm cursor-pointer transition-colors"
              >
                ✕
              </button>
            </div>

            {/* Modal Body */}
            <div className="p-6 space-y-6">
              {/* RP Nerf & Scaling Info Badges */}
              {selectedPlayer.pitcher_role === 'RP' && (
                <div className="bg-purple-950/40 border border-purple-500/40 rounded-xl p-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2 text-xs">
                  <div className="flex items-center gap-2">
                    <span className="text-base">🔥</span>
                    <span className="text-purple-200 font-bold">
                      Relief Pitcher Rules Active (2.5x σ Nerf &amp; ≥25 Y1 SV+HD Benchmark)
                    </span>
                  </div>
                  <span className="text-purple-300 text-[11px]">
                    Std dev multiplied by 2.5x across all RP stats. SV+HD benchmark specifically isolates high-leverage relievers (≥25 SV+HD in Year 1).
                  </span>
                </div>
              )}

              {modalYear !== 'y1' && selectedPlayer.is_pitcher && (
                <div className="bg-teal-950/30 border border-teal-500/30 rounded-xl p-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2 text-xs">
                  <div className="flex items-center gap-2">
                    <span className="text-base">📈</span>
                    <span className="text-teal-200 font-bold">
                      Proportional IP Scaling ({selectedPlayer[modalYear]?.year})
                    </span>
                  </div>
                  <span className="text-teal-300 text-[11px]">
                    QS and SV+HD are scaled proportionally based on projected IP ({selectedPlayer[modalYear]?.stats?.IP || 0} IP) from baseline.
                  </span>
                </div>
              )}

              {/* Multi-Year Blend Breakdown */}
              <div className="bg-slate-950 border border-slate-800 rounded-xl p-4">
                <div className="text-xs font-black uppercase tracking-wider text-slate-400 mb-2">
                  Multi-Year Blend Calculation
                </div>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-center">
                  <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
                    <div className="text-[11px] text-slate-400 font-bold">Year 1 (2026) • 60%</div>
                    <div className="text-base font-black text-emerald-400 mt-1">
                      {selectedPlayer.y1?.pr > 0 ? `+${selectedPlayer.y1.pr.toFixed(2)}` : selectedPlayer.y1?.pr.toFixed(2)}
                    </div>
                    <div className="text-[10px] text-slate-500 mt-0.5">
                      Rank #{selectedPlayer.y1?.rank} • FanGraphs DC
                    </div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
                    <div className="text-[11px] text-slate-400 font-bold">Year 2 (2027) • 30%</div>
                    <div className="text-base font-black text-teal-400 mt-1">
                      {selectedPlayer.y2?.pr > 0 ? `+${selectedPlayer.y2.pr.toFixed(2)}` : selectedPlayer.y2?.pr.toFixed(2)}
                    </div>
                    <div className="text-[10px] text-slate-500 mt-0.5">
                      Rank #{selectedPlayer.y2?.rank} • ZiPS +1
                    </div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
                    <div className="text-[11px] text-slate-400 font-bold">Year 3 (2028) • 10%</div>
                    <div className="text-base font-black text-blue-400 mt-1">
                      {selectedPlayer.y3?.pr > 0 ? `+${selectedPlayer.y3.pr.toFixed(2)}` : selectedPlayer.y3?.pr.toFixed(2)}
                    </div>
                    <div className="text-[10px] text-slate-500 mt-0.5">
                      Rank #{selectedPlayer.y3?.rank} • ZiPS +2
                    </div>
                  </div>

                  <div className="bg-emerald-950/30 border border-emerald-500/40 rounded-lg p-3">
                    <div className="text-[11px] text-emerald-300 font-bold">Total Weighted PR</div>
                    <div className="text-base font-black text-amber-400 mt-1">
                      {selectedPlayer.overall_pr > 0 ? `+${selectedPlayer.overall_pr.toFixed(2)}` : selectedPlayer.overall_pr.toFixed(2)}
                    </div>
                    <div className="text-[10px] text-emerald-400 mt-0.5">
                      Overall #{selectedPlayer.overall_rank} (${selectedPlayer.overall_price})
                    </div>
                  </div>
                </div>
              </div>

              {/* Year Selector for Inspecting Underlying Stats & Calculations */}
              <div>
                <div className="flex items-center justify-between gap-3 mb-3">
                  <div className="text-sm font-black text-white uppercase tracking-wider flex items-center gap-2">
                    <span>🔬</span>
                    <span>Detailed Calculations for {selectedPlayer[modalYear]?.label}</span>
                  </div>

                  <div className="inline-flex bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold gap-1">
                    <button
                      type="button"
                      onClick={() => setModalYear('y1')}
                      className={`px-3 py-1 rounded-md transition-all cursor-pointer ${
                        modalYear === 'y1' ? 'bg-emerald-500 text-slate-950 font-black' : 'text-slate-400 hover:text-white'
                      }`}
                    >
                      2026 (Y1)
                    </button>
                    <button
                      type="button"
                      onClick={() => setModalYear('y2')}
                      className={`px-3 py-1 rounded-md transition-all cursor-pointer ${
                        modalYear === 'y2' ? 'bg-teal-500 text-slate-950 font-black' : 'text-slate-400 hover:text-white'
                      }`}
                    >
                      2027 (Y2)
                    </button>
                    <button
                      type="button"
                      onClick={() => setModalYear('y3')}
                      className={`px-3 py-1 rounded-md transition-all cursor-pointer ${
                        modalYear === 'y3' ? 'bg-blue-500 text-white font-black' : 'text-slate-400 hover:text-white'
                      }`}
                    >
                      2028 (Y3)
                    </button>
                  </div>
                </div>

                {/* Projected Stats Grid */}
                <div className="bg-slate-950 border border-slate-800 rounded-xl p-4 mb-4">
                  <div className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-2">
                    Projected Box Score & Volume Metrics ({selectedPlayer[modalYear]?.year})
                  </div>
                  <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 gap-2 text-center text-xs">
                    {Object.entries(selectedPlayer[modalYear]?.stats || {}).map(([st, val]) => (
                      <div key={st} className="bg-slate-900 border border-slate-800 rounded p-2">
                        <div className="text-[10px] text-slate-500 font-semibold">{st}</div>
                        <div className="text-xs font-bold text-white mt-0.5">
                          {typeof val === 'number' && st === 'OBP' ? val.toFixed(3) : val}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Per-Element Category PR Calculations Table */}
                <div className="bg-slate-950 border border-slate-800 rounded-xl overflow-hidden shadow-inner">
                  <div className="p-3 bg-slate-900/60 border-b border-slate-800 text-xs font-black uppercase tracking-wider text-slate-300 flex items-center justify-between">
                    <span>Category PR Calculations</span>
                    <span className="text-emerald-400 font-bold">
                      Sum of Category PRs: {selectedPlayer[modalYear]?.pr > 0 ? `+${selectedPlayer[modalYear]?.pr.toFixed(2)}` : selectedPlayer[modalYear]?.pr?.toFixed(2)}
                    </span>
                  </div>

                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs border-collapse">
                      <thead>
                        <tr className="bg-slate-900 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-bold">
                          <th className="py-2.5 px-3">Stat</th>
                          <th className="py-2.5 px-3">Category Type</th>
                          <th className="py-2.5 px-3 text-right">Proj Value</th>
                          <th className="py-2.5 px-3 text-right">Benchmark Mean (μ)</th>
                          <th className="py-2.5 px-3 text-right">Std Dev (σ)</th>
                          <th className="py-2.5 px-4 font-mono text-[11px]">Applied Formula</th>
                          <th className="py-2.5 px-3 text-right">Category PR</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/60 text-slate-300">
                        {Object.entries(selectedPlayer[modalYear]?.calcs || {}).map(([cat, c]) => {
                          const isPositive = (c.pr || 0) > 0;
                          const isZero = (c.pr || 0) === 0;

                          return (
                            <tr key={cat} className="hover:bg-slate-900/40">
                              <td className="py-2.5 px-3 font-black text-white">{cat}</td>
                              <td className="py-2.5 px-3 text-[11px] text-slate-400">{c.category_type}</td>
                              <td className="py-2.5 px-3 text-right font-bold text-white">{c.val}</td>
                              <td className="py-2.5 px-3 text-right text-slate-400">{c.mean}</td>
                              <td className="py-2.5 px-3 text-right text-slate-400">
                                {c.eff_std ? (
                                  <span title={`Effective σ: ${c.eff_std} (nerfed 2.5x from raw ${c.std})`}>
                                    <span className="text-purple-300 font-bold">{c.eff_std}</span>{' '}
                                    <span className="text-[10px] text-slate-500 font-mono">({c.std})</span>
                                  </span>
                                ) : (
                                  c.std
                                )}
                              </td>
                              <td className="py-2.5 px-4 font-mono text-[11px] text-slate-400">
                                {c.formula}
                              </td>
                              <td className="py-2.5 px-3 text-right font-black">
                                <span className={`inline-block px-2 py-0.5 rounded text-[11px] font-bold ${
                                  isPositive
                                    ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                                    : isZero
                                    ? 'bg-slate-800 text-slate-400'
                                    : 'bg-rose-500/20 text-rose-400 border border-rose-500/30'
                                }`}>
                                  {c.pr > 0 ? `+${c.pr.toFixed(2)}` : c.pr.toFixed(2)}
                                </span>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>

            {/* Modal Footer */}
            <div className="p-4 border-t border-slate-800 bg-slate-950 flex items-center justify-between text-xs text-slate-500">
              <span>Hefty PR Valuation Engine • Qualified 10-Category Fantasy Scoring</span>
              <button
                type="button"
                onClick={() => setSelectedPlayer(null)}
                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-xl font-bold cursor-pointer transition-colors"
              >
                Close Breakdown
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
