// src/views/PlayerOwnerStatsView.jsx
import React, { useState, useMemo } from 'react';
import preloadedData from '../data/playerOwnerSeasonStats.json';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';

const SEASONS = [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018];
const POSITIONS = ['ALL', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', 'UTIL'];
const RAW_DATA = preloadedData || [];

function round(val, decimals) {
  if (val === null || val === undefined || isNaN(val)) return 0;
  const factor = Math.pow(10, decimals);
  return Math.round(val * factor) / factor;
}

export default function PlayerOwnerStatsView({
  initialMode = 'season', // 'season' | 'career'
  onOwnerClick,
  onPlayerClick
}) {
  const [viewMode, setViewMode] = useState(initialMode);
  const [selectedSeason, setSelectedSeason] = useState('ALL');
  const [selectedOwner, setSelectedOwner] = useState('ALL');
  const [typeFilter, setTypeFilter] = useState('ALL'); // 'ALL' | 'BATTER' | 'PITCHER'
  const [posFilter, setPosFilter] = useState('ALL');
  const [searchQuery, setSearchQuery] = useState('');
  const [minPA, setMinPA] = useState(0);
  const [minIP, setMinIP] = useState(0);

  // Sorting
  const [sortKey, setSortKey] = useState('pa');
  const [sortDir, setSortDir] = useState('desc');

  // Pagination
  const [pageSize, setPageSize] = useState(50);
  const [page, setPage] = useState(1);

  const handleTypeFilterChange = (newType) => {
    setTypeFilter(newType);
    setPage(1);
    if (newType === 'PITCHER') {
      if (['pa', 'r', 'hr', 'rbi', 'sb', 'avg', 'obp', 'ops'].includes(sortKey)) {
        setSortKey('ip');
        setSortDir('desc');
      }
    } else if (newType === 'BATTER') {
      if (['ip', 'k', 'qs', 'sv_hd', 'era', 'whip', 'w'].includes(sortKey)) {
        setSortKey('pa');
        setSortDir('desc');
      }
    }
  };

  const handlePosFilterChange = (newPos) => {
    setPosFilter(newPos);
    setPage(1);
    if (newPos === 'SP' || newPos === 'RP') {
      if (['pa', 'r', 'hr', 'rbi', 'sb', 'avg', 'obp', 'ops'].includes(sortKey)) {
        setSortKey('ip');
        setSortDir('desc');
      }
    } else if (newPos !== 'ALL') {
      if (['ip', 'k', 'qs', 'sv_hd', 'era', 'whip', 'w'].includes(sortKey)) {
        setSortKey('pa');
        setSortDir('desc');
      }
    }
  };

  const hasActiveFilters = selectedSeason !== 'ALL' ||
    selectedOwner !== 'ALL' ||
    typeFilter !== 'ALL' ||
    posFilter !== 'ALL' ||
    minPA > 0 ||
    minIP > 0 ||
    searchQuery.trim() !== '';

  const resetFilters = () => {
    setSelectedSeason('ALL');
    setSelectedOwner('ALL');
    setTypeFilter('ALL');
    setPosFilter('ALL');
    setMinPA(0);
    setMinIP(0);
    setSearchQuery('');
    setPage(1);
  };

  // 1. Season-Grain Filtered Records
  const seasonRecords = useMemo(() => {
    return RAW_DATA.filter(r => {
      if (selectedSeason !== 'ALL' && r.season_year !== Number(selectedSeason)) return false;
      if (selectedOwner !== 'ALL' && String(r.team_id) !== String(selectedOwner) && r.owner_name !== selectedOwner) return false;
      if (typeFilter === 'BATTER' && !r.is_batter) return false;
      if (typeFilter === 'PITCHER' && !r.is_pitcher) return false;
      if (posFilter !== 'ALL') {
        const pList = (r.positions || '').split(',').map(s => s.trim().toUpperCase());
        if (!pList.includes(posFilter)) return false;
      }
      
      // Threshold filters (PA for Batters, IP for Pitchers)
      if (typeFilter === 'BATTER') {
        if (minPA > 0 && (r.pa || 0) < minPA) return false;
      } else if (typeFilter === 'PITCHER') {
        if (minIP > 0 && (r.ip || 0) < minIP) return false;
      } else {
        if (minPA > 0 && minIP > 0) {
          const qualifiesBatter = r.is_batter && (r.pa || 0) >= minPA;
          const qualifiesPitcher = r.is_pitcher && (r.ip || 0) >= minIP;
          if (!qualifiesBatter && !qualifiesPitcher) return false;
        } else if (minPA > 0) {
          if ((r.pa || 0) < minPA) return false;
        } else if (minIP > 0) {
          if ((r.ip || 0) < minIP) return false;
        }
      }

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        const pName = (r.player_name || '').toLowerCase();
        const oName = (r.owner_name || '').toLowerCase();
        if (!pName.includes(q) && !oName.includes(q)) return false;
      }
      return true;
    });
  }, [selectedSeason, selectedOwner, typeFilter, posFilter, minPA, minIP, searchQuery]);

  // 2. Career-Grain (All Seasons Combined) Rollup
  const careerRecords = useMemo(() => {
    const map = {};

    RAW_DATA.forEach(r => {
      const key = `${r.player_id}_${r.team_id}`;
      if (!map[key]) {
        map[key] = {
          player_id: r.player_id,
          player_name: r.player_name,
          team_id: r.team_id,
          owner_name: r.owner_name,
          is_batter: false,
          is_pitcher: false,
          seasons: new Set(),
          positionsSet: new Set(),
          days_active: 0,
          days_bench: 0,
          days_il: 0,
          days_total: 0,
          pa: 0, ab: 0, h: 0, r: 0, hr: 0, rbi: 0, sb: 0, bb: 0, so: 0,
          d2: 0, d3: 0, tb: 0, sf: 0, hbp: 0, cs: 0,
          gs: 0, ip_outs: 0, k: 0, qs: 0, w: 0, l: 0, sv: 0, hd: 0, sv_hd: 0,
          er: 0, h_allowed: 0, bb_allowed: 0
        };
      }

      const c = map[key];
      c.seasons.add(r.season_year);
      if (r.player_name && r.player_name.length > c.player_name.length) {
        c.player_name = r.player_name;
      }
      if (r.is_batter) c.is_batter = true;
      if (r.is_pitcher) c.is_pitcher = true;

      (r.positions || '').split(',').forEach(p => {
        const trimmed = p.trim();
        if (trimmed) c.positionsSet.add(trimmed);
      });

      c.days_active += r.days_active || 0;
      c.days_bench += r.days_bench || 0;
      c.days_il += r.days_il || 0;
      c.days_total += r.days_total || 0;

      // Batting
      c.pa += r.pa || 0;
      c.ab += r.ab || 0;
      c.h += r.h || 0;
      c.r += r.r || 0;
      c.hr += r.hr || 0;
      c.rbi += r.rbi || 0;
      c.sb += r.sb || 0;
      c.bb += r.bb || 0;
      c.so += r.so || 0;
      c.d2 += r.d2 || 0;
      c.d3 += r.d3 || 0;
      c.tb += r.tb || 0;
      c.sf += r.sf || 0;
      c.hbp += r.hbp || 0;
      c.cs += r.cs || 0;

      // Pitching
      c.gs += r.gs || 0;
      c.ip_outs += r.ip_outs || 0;
      c.k += r.k || 0;
      c.qs += r.qs || 0;
      c.w += r.w || 0;
      c.l += r.l || 0;
      c.sv += r.sv || 0;
      c.hd += r.hd || 0;
      c.sv_hd += (r.sv_hd || (r.sv || 0) + (r.hd || 0));
      c.er += r.er || 0;
      c.h_allowed += r.h_allowed || 0;
      c.bb_allowed += r.bb_allowed || 0;
    });

    const rolledUp = Object.values(map).map(c => {
      const sortedSeasons = Array.from(c.seasons).sort((a, b) => a - b);
      const seasonSpan = sortedSeasons.length === 1
        ? `${sortedSeasons[0]}`
        : `${sortedSeasons[0]}–${sortedSeasons[sortedSeasons.length - 1]} (${sortedSeasons.length}y)`;

      const ab = c.ab;
      const h = c.h;
      const bb = c.bb;
      const hbp = c.hbp;
      const sf = c.sf;
      const tb = c.tb;
      const ip_outs = c.ip_outs;
      const ip = round(ip_outs / 3.0, 2);
      const er = c.er;
      const gs = c.gs;
      const qs = c.qs;
      const k = c.k;
      const bb_all = c.bb_allowed;
      const h_all = c.h_allowed;

      const avg = ab > 0 ? h / ab : 0;
      const obp_denom = ab + bb + hbp + sf;
      const obp = obp_denom > 0 ? (h + bb + hbp) / obp_denom : (c.pa > 0 ? (h + bb) / c.pa : 0);
      const slg = ab > 0 ? tb / ab : 0;
      const ops = obp + slg;

      const era = ip_outs > 0 ? (er * 9.0) / (ip_outs / 3.0) : 0;
      const whip = ip_outs > 0 ? (bb_all + h_all) / (ip_outs / 3.0) : 0;
      const k_9 = ip_outs > 0 ? (k * 9.0) / (ip_outs / 3.0) : 0;
      const bb_9 = ip_outs > 0 ? (bb_all * 9.0) / (ip_outs / 3.0) : 0;
      const qs_pct = gs > 0 ? (qs / gs) * 100.0 : 0;

      return {
        ...c,
        season_year: seasonSpan,
        seasonSpan,
        seasonsCount: sortedSeasons.length,
        positions: Array.from(c.positionsSet).join(', ') || (c.is_pitcher ? 'P' : 'UTIL'),
        ip,
        avg: round(avg, 3),
        obp: round(obp, 4),
        slg: round(slg, 3),
        ops: round(ops, 3),
        era: round(era, 2),
        whip: round(whip, 2),
        k_9: round(k_9, 2),
        bb_9: round(bb_9, 2),
        qs_pct: round(qs_pct, 1)
      };
    });

    // Filter career records
    return rolledUp.filter(r => {
      if (selectedOwner !== 'ALL' && String(r.team_id) !== String(selectedOwner) && r.owner_name !== selectedOwner) return false;
      if (typeFilter === 'BATTER' && !r.is_batter) return false;
      if (typeFilter === 'PITCHER' && !r.is_pitcher) return false;
      if (posFilter !== 'ALL') {
        const pList = (r.positions || '').split(',').map(s => s.trim().toUpperCase());
        if (!pList.includes(posFilter)) return false;
      }
      
      // Threshold filters (PA for Batters, IP for Pitchers)
      if (typeFilter === 'BATTER') {
        if (minPA > 0 && (r.pa || 0) < minPA) return false;
      } else if (typeFilter === 'PITCHER') {
        if (minIP > 0 && (r.ip || 0) < minIP) return false;
      } else {
        if (minPA > 0 && minIP > 0) {
          const qualifiesBatter = r.is_batter && (r.pa || 0) >= minPA;
          const qualifiesPitcher = r.is_pitcher && (r.ip || 0) >= minIP;
          if (!qualifiesBatter && !qualifiesPitcher) return false;
        } else if (minPA > 0) {
          if ((r.pa || 0) < minPA) return false;
        } else if (minIP > 0) {
          if ((r.ip || 0) < minIP) return false;
        }
      }

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        const pName = (r.player_name || '').toLowerCase();
        const oName = (r.owner_name || '').toLowerCase();
        if (!pName.includes(q) && !oName.includes(q)) return false;
      }
      return true;
    });
  }, [selectedOwner, typeFilter, posFilter, minPA, minIP, searchQuery]);

  // Active records to display
  const activeRecords = viewMode === 'season' ? seasonRecords : careerRecords;

  // Sorting
  const sortedRecords = useMemo(() => {
    return [...activeRecords].sort((a, b) => {
      let va = a[sortKey];
      let vb = b[sortKey];

      if (va === undefined || va === null) va = 0;
      if (vb === undefined || vb === null) vb = 0;

      if (typeof va === 'string') {
        return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
      }

      const isLowBetter = sortKey === 'era' || sortKey === 'whip' || sortKey === 'so' || sortKey === 'bb_9';
      if (isLowBetter) {
        return sortDir === 'asc' ? vb - va : va - vb;
      }
      return sortDir === 'asc' ? va - vb : vb - va;
    });
  }, [activeRecords, sortKey, sortDir]);

  // Pagination
  const totalPages = pageSize === 'ALL' ? 1 : Math.max(1, Math.ceil(sortedRecords.length / pageSize));
  const pagedRecords = useMemo(() => {
    if (pageSize === 'ALL') return sortedRecords;
    const start = (page - 1) * pageSize;
    return sortedRecords.slice(start, start + pageSize);
  }, [sortedRecords, page, pageSize]);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(prev => prev === 'desc' ? 'asc' : 'desc');
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  };

  // CSV Export
  const exportToCSV = () => {
    const headers = [
      'Player Name', 'Owner', 'Season', 'Positions', 'Days Active', 'Days Bench', 'Days IL',
      'PA', 'AB', 'H', 'R', 'HR', 'RBI', 'SB', 'BB', 'SO', 'AVG', 'OBP', 'SLG', 'OPS',
      'GS', 'IP', 'K', 'QS', 'SV+HD', 'ERA', 'WHIP', 'W', 'L'
    ];

    const rows = sortedRecords.map(r => [
      `"${r.player_name || ''}"`,
      `"${r.owner_name || ''}"`,
      `"${r.season_year || ''}"`,
      `"${r.positions || ''}"`,
      r.days_active || 0,
      r.days_bench || 0,
      r.days_il || 0,
      r.pa || 0,
      r.ab || 0,
      r.h || 0,
      r.r || 0,
      r.hr || 0,
      r.rbi || 0,
      r.sb || 0,
      r.bb || 0,
      r.so || 0,
      r.avg || 0,
      r.obp || 0,
      r.slg || 0,
      r.ops || 0,
      r.gs || 0,
      r.ip || 0,
      r.k || 0,
      r.qs || 0,
      r.sv_hd || 0,
      r.era || 0,
      r.whip || 0,
      r.w || 0,
      r.l || 0
    ]);

    const csvContent = 'data:text/csv;charset=utf-8,' + [headers.join(','), ...rows.map(e => e.join(','))].join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', `player_owner_${viewMode}_stats.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="space-y-6">
      {/* Header Banner */}
      <div className="bg-gradient-to-br from-slate-900 via-blue-950 to-indigo-950 rounded-3xl p-6 md:p-8 text-white shadow-xl border border-blue-900/40 relative overflow-hidden">
        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div>
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-500/20 border border-blue-400/30 text-blue-300 text-xs font-bold uppercase tracking-wider mb-2">
              <span>⚡ Flattened High-Speed Dataset</span>
              <span>•</span>
              <span>2018–2026 Archive</span>
            </div>
            <h2 className="text-2xl md:text-3xl font-black tracking-tight text-white flex items-center gap-3">
              <span>Player-Owner Analytics</span>
            </h2>
            <p className="text-sm text-blue-200/80 mt-1 max-w-2xl">
              Inspect historical player production at the owner level. Explore single-season splits or aggregate across all seasons to discover each manager's franchise cornerstones.
            </p>
          </div>

          {/* View Mode Toggle Pill */}
          <div className="flex bg-slate-800/90 p-1.5 rounded-2xl border border-slate-700/80 shadow-lg">
            <button
              onClick={() => { setViewMode('season'); setPage(1); }}
              className={`px-4 py-2 rounded-xl text-xs font-black transition-all flex items-center gap-2 ${
                viewMode === 'season'
                  ? 'bg-blue-600 text-white shadow-md shadow-blue-500/30'
                  : 'text-slate-300 hover:text-white hover:bg-slate-700/50'
              }`}
            >
              <span>📅</span>
              <span>Player-Owner-Season</span>
            </button>
            <button
              onClick={() => { setViewMode('career'); setPage(1); }}
              className={`px-4 py-2 rounded-xl text-xs font-black transition-all flex items-center gap-2 ${
                viewMode === 'career'
                  ? 'bg-blue-600 text-white shadow-md shadow-blue-500/30'
                  : 'text-slate-300 hover:text-white hover:bg-slate-700/50'
              }`}
            >
              <span>👑</span>
              <span>Player-Owner Career (All-Time)</span>
            </button>
          </div>
        </div>
      </div>

      {/* Filter and Control Toolbar */}
      <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-4">
          {/* Quick Filters */}
          <div className="flex flex-wrap items-center gap-3">
            {/* Season Filter (Only in season mode) */}
            {viewMode === 'season' && (
              <div>
                <label className="block text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Season</label>
                <select
                  value={selectedSeason}
                  onChange={e => { setSelectedSeason(e.target.value); setPage(1); }}
                  className="bg-gray-50 border border-gray-300 rounded-xl px-3 py-1.5 text-xs font-bold text-gray-800 focus:ring-2 focus:ring-blue-500 outline-none"
                >
                  <option value="ALL">All Seasons</option>
                  {SEASONS.map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
              </div>
            )}

            {/* Owner Filter */}
            <div>
              <label className="block text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Owner</label>
              <select
                value={selectedOwner}
                onChange={e => { setSelectedOwner(e.target.value); setPage(1); }}
                className="bg-gray-50 border border-gray-300 rounded-xl px-3 py-1.5 text-xs font-bold text-gray-800 focus:ring-2 focus:ring-blue-500 outline-none"
              >
                <option value="ALL">All Owners</option>
                {Object.values(TEAMS).filter(t => t.id !== 99).map(t => (
                  <option key={t.id} value={t.id}>{t.name} ({t.owner})</option>
                ))}
              </select>
            </div>

            {/* Type Filter */}
            <div>
              <label className="block text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Player Type</label>
              <div className="flex bg-gray-100 p-0.5 rounded-xl">
                <button
                  onClick={() => handleTypeFilterChange('ALL')}
                  className={`px-3 py-1 text-xs font-bold rounded-lg transition ${typeFilter === 'ALL' ? 'bg-white shadow-xs text-blue-600' : 'text-gray-600'}`}
                >
                  All
                </button>
                <button
                  onClick={() => handleTypeFilterChange('BATTER')}
                  className={`px-3 py-1 text-xs font-bold rounded-lg transition ${typeFilter === 'BATTER' ? 'bg-white shadow-xs text-blue-600' : 'text-gray-600'}`}
                >
                  Batters
                </button>
                <button
                  onClick={() => handleTypeFilterChange('PITCHER')}
                  className={`px-3 py-1 text-xs font-bold rounded-lg transition ${typeFilter === 'PITCHER' ? 'bg-white shadow-xs text-blue-600' : 'text-gray-600'}`}
                >
                  Pitchers
                </button>
              </div>
            </div>

            {/* Position Filter */}
            <div>
              <label className="block text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Position</label>
              <select
                value={posFilter}
                onChange={e => handlePosFilterChange(e.target.value)}
                className="bg-gray-50 border border-gray-300 rounded-xl px-3 py-1.5 text-xs font-bold text-gray-800 focus:ring-2 focus:ring-blue-500 outline-none"
              >
                {POSITIONS.map(pos => (
                  <option key={pos} value={pos}>{pos}</option>
                ))}
              </select>
            </div>

            {/* Min PA Filter for Batters */}
            {(typeFilter === 'BATTER' || (typeFilter === 'ALL' && (posFilter === 'ALL' || !['SP', 'RP'].includes(posFilter)))) && (
              <div>
                <label className="block text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">
                  {typeFilter === 'ALL' ? 'Min PA (Batters)' : 'Min PA'}
                </label>
                <select
                  value={minPA}
                  onChange={e => {
                    setMinPA(Number(e.target.value));
                    setPage(1);
                  }}
                  className="bg-gray-50 border border-gray-300 rounded-xl px-3 py-1.5 text-xs font-bold text-gray-800 focus:ring-2 focus:ring-blue-500 outline-none"
                >
                  <option value={0}>All PA (0+)</option>
                  {viewMode === 'season' ? (
                    <>
                      <option value={50}>50+ PA</option>
                      <option value={100}>100+ PA</option>
                      <option value={250}>250+ PA</option>
                      <option value={400}>400+ PA</option>
                      <option value={550}>550+ PA</option>
                    </>
                  ) : (
                    <>
                      <option value={100}>100+ PA</option>
                      <option value={250}>250+ PA</option>
                      <option value={500}>500+ PA</option>
                      <option value={1000}>1,000+ PA</option>
                      <option value={2000}>2,000+ PA</option>
                    </>
                  )}
                </select>
              </div>
            )}

            {/* Min IP Filter for Pitchers */}
            {(typeFilter === 'PITCHER' || (typeFilter === 'ALL' && (posFilter === 'ALL' || ['SP', 'RP'].includes(posFilter)))) && (
              <div>
                <label className="block text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">
                  {typeFilter === 'ALL' ? 'Min IP (Pitchers)' : 'Min IP'}
                </label>
                <select
                  value={minIP}
                  onChange={e => {
                    setMinIP(Number(e.target.value));
                    setPage(1);
                  }}
                  className="bg-gray-50 border border-gray-300 rounded-xl px-3 py-1.5 text-xs font-bold text-gray-800 focus:ring-2 focus:ring-blue-500 outline-none"
                >
                  <option value={0}>All IP (0+)</option>
                  {viewMode === 'season' ? (
                    <>
                      <option value={10}>10+ IP</option>
                      <option value={25}>25+ IP</option>
                      <option value={50}>50+ IP</option>
                      <option value={75}>75+ IP</option>
                      <option value={100}>100+ IP</option>
                      <option value={150}>150+ IP</option>
                      <option value={180}>180+ IP</option>
                    </>
                  ) : (
                    <>
                      <option value={25}>25+ IP</option>
                      <option value={50}>50+ IP</option>
                      <option value={100}>100+ IP</option>
                      <option value={250}>250+ IP</option>
                      <option value={500}>500+ IP</option>
                      <option value={1000}>1,000+ IP</option>
                    </>
                  )}
                </select>
              </div>
            )}
          </div>

          {/* Search & Export */}
          <div className="flex items-center gap-3">
            <input
              type="text"
              placeholder="Search player or owner..."
              value={searchQuery}
              onChange={e => { setSearchQuery(e.target.value); setPage(1); }}
              className="px-3.5 py-1.5 bg-gray-50 border border-gray-300 rounded-xl text-xs focus:ring-2 focus:ring-blue-500 outline-none w-56 font-medium"
            />
            <button
              onClick={exportToCSV}
              className="px-3.5 py-1.5 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-xl text-xs font-bold transition flex items-center gap-1.5"
              title="Download CSV of current table"
            >
              <span>📥</span>
              <span>Export CSV</span>
            </button>
          </div>
        </div>

        {/* Results Counter & Page Size */}
        <div className="flex items-center justify-between text-xs text-gray-500 pt-2 border-t border-gray-100">
          <div className="flex items-center gap-3">
            <span>
              Showing <span className="font-bold text-gray-900">{sortedRecords.length}</span> {viewMode === 'season' ? 'player-owner-season' : 'player-owner career'} records
            </span>
            {hasActiveFilters && (
              <button
                onClick={resetFilters}
                className="text-xs text-blue-600 hover:text-blue-800 font-bold underline transition"
              >
                Reset filters
              </button>
            )}
          </div>
          <div className="flex items-center gap-2">
            <span>Per Page:</span>
            {[50, 100, 250, 'ALL'].map(sz => (
              <button
                key={sz}
                onClick={() => { setPageSize(sz); setPage(1); }}
                className={`px-2 py-0.5 rounded text-xs font-bold ${
                  pageSize === sz ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {sz}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main Data Table */}
      <div className="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-gray-50 border-b border-gray-200 text-gray-500 uppercase tracking-wider font-bold">
              <tr>
                <th className="px-3 py-3 text-left sticky left-0 bg-gray-50 z-10">Player</th>
                <th className="px-3 py-3 text-left">Owner</th>
                <th className="px-3 py-3 text-center">{viewMode === 'season' ? 'Season' : 'Tenure'}</th>
                <th className="px-3 py-3 text-center">Pos</th>
                <th className="px-3 py-3 text-center">Active Days</th>

                {/* Hitting Columns */}
                {(typeFilter === 'ALL' || typeFilter === 'BATTER') && (
                  <>
                    <th onClick={() => handleSort('pa')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      PA {sortKey === 'pa' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('r')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      R {sortKey === 'r' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('hr')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      HR {sortKey === 'hr' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('rbi')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      RBI {sortKey === 'rbi' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('sb')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      SB {sortKey === 'sb' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('avg')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      AVG {sortKey === 'avg' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('obp')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50 text-blue-700">
                      OBP {sortKey === 'obp' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('ops')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      OPS {sortKey === 'ops' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                  </>
                )}

                {/* Pitching Columns */}
                {(typeFilter === 'ALL' || typeFilter === 'PITCHER') && (
                  <>
                    <th onClick={() => handleSort('ip')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      IP {sortKey === 'ip' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('k')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      K {sortKey === 'k' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('qs')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      QS {sortKey === 'qs' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('sv_hd')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      SV+H {sortKey === 'sv_hd' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('era')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50 text-blue-700">
                      ERA {sortKey === 'era' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('whip')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50 text-blue-700">
                      WHIP {sortKey === 'whip' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                    </th>
                    <th onClick={() => handleSort('w')} className="px-2.5 py-3 text-center cursor-pointer hover:bg-blue-50">
                      W-L
                    </th>
                  </>
                )}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 font-mono">
              {pagedRecords.map((r, idx) => {
                const teamObj = TEAMS[r.team_id] || { id: r.team_id, name: r.owner_name, owner: r.owner_name };
                const formattedOBP = r.obp ? r.obp.toFixed(4).replace(/^0/, '') : '.0000';
                const formattedAVG = r.avg ? r.avg.toFixed(3).replace(/^0/, '') : '.000';
                const formattedOPS = r.ops ? r.ops.toFixed(3).replace(/^0/, '') : '.000';
                const formattedERA = (r.ip > 0 || r.ip_outs > 0) ? r.era.toFixed(2) : '-';
                const formattedWHIP = (r.ip > 0 || r.ip_outs > 0) ? r.whip.toFixed(2) : '-';

                return (
                  <tr
                    key={`${r.player_id}_${r.team_id}_${r.season_year || idx}`}
                    className="hover:bg-blue-50/50 transition-colors"
                  >
                    {/* Player Name */}
                    <td className="px-3 py-2.5 sticky left-0 bg-white z-10 font-sans border-r border-gray-100 whitespace-nowrap">
                      <span
                        onClick={() => onPlayerClick && onPlayerClick({ id: r.player_id, name: r.player_name })}
                        className="font-bold text-gray-900 hover:text-blue-600 cursor-pointer"
                      >
                        {r.player_name}
                      </span>
                    </td>

                    {/* Owner */}
                    <td className="px-3 py-2.5 font-sans whitespace-nowrap">
                      <div
                        onClick={() => onOwnerClick && onOwnerClick(teamObj)}
                        className="flex items-center gap-2 cursor-pointer hover:text-blue-600"
                      >
                        <TeamAvatar team={teamObj} size="xs" />
                        <span className="font-bold text-gray-800">{r.owner_name}</span>
                      </div>
                    </td>

                    {/* Season / Span */}
                    <td className="px-3 py-2.5 text-center font-sans whitespace-nowrap">
                      <span className="px-2 py-0.5 rounded-md bg-gray-100 font-bold text-gray-700 text-[11px]">
                        {r.season_year}
                      </span>
                    </td>

                    {/* Positions */}
                    <td className="px-3 py-2.5 text-center font-sans whitespace-nowrap text-gray-500 font-medium">
                      {r.positions || '-'}
                    </td>

                    {/* Active Days */}
                    <td className="px-3 py-2.5 text-center text-gray-600">
                      {r.days_active}d
                    </td>

                    {/* Hitting */}
                    {(typeFilter === 'ALL' || typeFilter === 'BATTER') && (
                      <>
                        <td className="px-2.5 py-2.5 text-center font-bold text-gray-900">{r.pa || 0}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-700">{r.r || 0}</td>
                        <td className="px-2.5 py-2.5 text-center font-bold text-amber-700">{r.hr || 0}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-700">{r.rbi || 0}</td>
                        <td className="px-2.5 py-2.5 text-center text-emerald-700 font-bold">{r.sb || 0}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-600">{r.pa > 0 ? formattedAVG : '-'}</td>
                        <td className="px-2.5 py-2.5 text-center font-bold text-blue-700">{r.pa > 0 ? formattedOBP : '-'}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-700 font-bold">{r.pa > 0 ? formattedOPS : '-'}</td>
                      </>
                    )}

                    {/* Pitching */}
                    {(typeFilter === 'ALL' || typeFilter === 'PITCHER') && (
                      <>
                        <td className="px-2.5 py-2.5 text-center font-bold text-gray-900">{r.ip || 0}</td>
                        <td className="px-2.5 py-2.5 text-center font-bold text-indigo-700">{r.k || 0}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-700">{r.qs || 0}</td>
                        <td className="px-2.5 py-2.5 text-center font-bold text-purple-700">{r.sv_hd || 0}</td>
                        <td className="px-2.5 py-2.5 text-center font-bold text-blue-700">{formattedERA}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-700">{formattedWHIP}</td>
                        <td className="px-2.5 py-2.5 text-center text-gray-500 text-[11px] font-sans">
                          {r.w || 0}-{r.l || 0}
                        </td>
                      </>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination Bar */}
        {pageSize !== 'ALL' && totalPages > 1 && (
          <div className="p-4 bg-gray-50 border-t border-gray-200 flex items-center justify-between">
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
              className="px-3 py-1.5 rounded-lg border border-gray-300 bg-white text-xs font-bold text-gray-700 hover:bg-gray-100 disabled:opacity-40"
            >
              ◀ Previous
            </button>
            <span className="text-xs text-gray-500 font-medium">
              Page <span className="font-bold text-gray-900">{page}</span> of <span className="font-bold text-gray-900">{totalPages}</span>
            </span>
            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="px-3 py-1.5 rounded-lg border border-gray-300 bg-white text-xs font-bold text-gray-700 hover:bg-gray-100 disabled:opacity-40"
            >
              Next ▶
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
