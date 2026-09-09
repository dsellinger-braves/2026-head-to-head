// src/views/BenchStatsView.jsx
import React, { useState, useMemo } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, aggregateBenchStats, SCORING_CATS } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

const ESPN_STAT_NAMES = {
  '0': 'AB', '1': 'H', '3': '2B', '4': '3B', '5': 'HR',
  '10': 'BB', '12': 'HBP', '16': 'PA', '17': 'OBP',
  '20': 'R', '21': 'RBI', '23': 'SB', '24': 'CS',
  '33': 'GS', '34': 'IP', '37': 'H_Allowed', '39': 'BB_Allowed',
  '45': 'ER', '48': 'K', '53': 'W', '54': 'L', '57': 'SV', '60': 'HD', '63': 'QS'
};

function parseRecordStats(record) {
  const s = {};
  for (const [key, val] of Object.entries(record.stats || {})) {
    s[ESPN_STAT_NAMES[key] ?? key] = val;
  }
  const espn = record.stats || {};

  const pa = parseFloat(s.PA ?? espn['16']) || 0;
  const ab = parseFloat(s.AB ?? espn['0']) || 0;
  const h = parseFloat(s.H ?? espn['1']) || 0;
  const r = parseFloat(s.R ?? espn['20']) || 0;
  const hr = parseFloat(s.HR ?? espn['5']) || 0;
  const rbi = parseFloat(s.RBI ?? espn['21']) || 0;
  const sb = parseFloat(s.SB ?? espn['23']) || 0;
  const bb = parseFloat(s.BB ?? espn['10']) || 0;
  const d2 = parseFloat(s['2B'] ?? espn['3']) || 0;
  const d3 = parseFloat(s['3B'] ?? espn['4']) || 0;
  const obp = parseFloat(s.OBP ?? espn['17']) || 0;

  const rawIp = parseFloat(s.IP_raw ?? s.IP ?? espn['34']) || 0;
  const ip = rawIp > 0 ? (s.IP_raw !== undefined ? rawIp / 3 : (espn['34'] !== undefined ? rawIp / 3 : rawIp)) : 0;
  const k = parseFloat(s.K ?? espn['48']) || 0;
  const qs = parseFloat(s.QS ?? espn['63']) || 0;
  const sv = parseFloat(s.SV ?? espn['57']) || 0;
  const hd = parseFloat(s.HD ?? espn['60']) || 0;
  const er = parseFloat(s.ER ?? espn['45']) || 0;
  const w = parseFloat(s.W ?? espn['53']) || 0;
  const l = parseFloat(s.L ?? espn['54']) || 0;
  const bbAll = parseFloat(s.BB_Allowed ?? espn['39']) || 0;
  const hAll = parseFloat(s.H_Allowed ?? espn['37']) || 0;

  const isPitcher = ip > 0 || k > 0 || er > 0 || qs > 0 || sv > 0 || hd > 0 || w > 0 || l > 0;
  const isBatter = pa > 0 || ab > 0 || h > 0 || r > 0 || hr > 0 || rbi > 0 || sb > 0;

  return {
    pa, ab, h, r, hr, rbi, sb, bb, d2, d3, obp,
    ip, k, qs, sv, hd, svHd: sv + hd, er, w, l, bbAll, hAll,
    isPitcher, isBatter
  };
}

export default function BenchStatsView({ allStats = [], selectedSeason = 2026, onOwnerClick, onPlayerClick }) {
  const [subTab, setSubTab] = useState('performances'); // 'performances' | 'teamTotals'
  const [catFilter, setCatFilter] = useState('ALL'); // 'ALL', 'HR', 'RBI', 'R', 'SB', 'QS', 'K', 'SV_HD'
  const [typeFilter, setTypeFilter] = useState('ALL'); // 'ALL', 'BATTER', 'PITCHER'
  const [teamFilter, setTeamFilter] = useState('ALL');
  const [slotFilter, setSlotFilter] = useState('ALL'); // 'ALL', 'BENCH', 'IL'
  const [searchQuery, setSearchQuery] = useState('');

  const [tableSortKey, setTableSortKey] = useState('bench_hr');
  const [tableSortDir, setTableSortDir] = useState('desc');
  const [totalsTab, setTotalsTab] = useState('batting'); // 'batting' | 'pitching' | 'waste'

  const humanTeams = useMemo(() => {
    return Object.values(TEAMS).filter(t => t.id !== 99);
  }, []);

  // 1. Team-by-team Active vs Bench aggregates
  const teamAggregates = useMemo(() => {
    const activeByTeam = {};
    const benchByTeam = {};

    humanTeams.forEach(t => {
      activeByTeam[t.id] = [];
      benchByTeam[t.id] = [];
    });

    allStats.forEach(r => {
      if (!activeByTeam[r.team_id]) return;
      if (r.lineup_slot_id === 16 || r.lineup_slot_id === 17) {
        benchByTeam[r.team_id].push(r);
      } else {
        activeByTeam[r.team_id].push(r);
      }
    });

    return humanTeams.map(team => {
      const active = aggregateStats(activeByTeam[team.id] || []);
      const bench = aggregateBenchStats(benchByTeam[team.id] || []);

      const totHr = (active.HR || 0) + (bench.HR || 0);
      const totRbi = (active.RBI || 0) + (bench.RBI || 0);
      const totR = (active.R || 0) + (bench.R || 0);
      const totSb = (active.SB || 0) + (bench.SB || 0);
      const totK = (active.K || 0) + (bench.K || 0);
      const totQs = (active.QS || 0) + (bench.QS || 0);
      const totSvHd = (active['SV+HDs'] || 0) + (bench['SV+HDs'] || 0);

      return {
        team,
        active,
        bench,
        waste: {
          hrPct: totHr > 0 ? ((bench.HR || 0) / totHr) * 100 : 0,
          rbiPct: totRbi > 0 ? ((bench.RBI || 0) / totRbi) * 100 : 0,
          rPct: totR > 0 ? ((bench.R || 0) / totR) * 100 : 0,
          sbPct: totSb > 0 ? ((bench.SB || 0) / totSb) * 100 : 0,
          kPct: totK > 0 ? ((bench.K || 0) / totK) * 100 : 0,
          qsPct: totQs > 0 ? ((bench.QS || 0) / totQs) * 100 : 0,
          svHdPct: totSvHd > 0 ? ((bench['SV+HDs'] || 0) / totSvHd) * 100 : 0,
        }
      };
    });
  }, [allStats, humanTeams]);

  // 2. Extract notable single-day bench performances
  const benchPerformances = useMemo(() => {
    const list = [];

    allStats.forEach(r => {
      if (r.lineup_slot_id !== 16 && r.lineup_slot_id !== 17) return;
      const parsed = parseRecordStats(r);

      const isNotableBatter = (
        parsed.hr >= 1 ||
        parsed.rbi >= 3 ||
        parsed.r >= 3 ||
        parsed.sb >= 2 ||
        parsed.h >= 3 ||
        (parsed.hr >= 1 && parsed.rbi >= 2) ||
        (parsed.pa >= 4 && parsed.obp >= 0.75)
      );

      const isNotablePitcher = (
        parsed.qs >= 1 ||
        parsed.k >= 5 ||
        parsed.sv >= 1 ||
        parsed.hd >= 1 ||
        (parsed.w >= 1 && parsed.ip >= 3 && parsed.er <= 2) ||
        (parsed.ip >= 2 && parsed.er === 0 && parsed.k >= 4)
      );

      if (!isNotableBatter && !isNotablePitcher) return;

      // Calculate composite regret/impact score
      let regretScore = 0;
      const badges = [];

      if (isNotableBatter) {
        regretScore += (parsed.hr * 15) + (parsed.rbi * 5) + (parsed.r * 4) + (parsed.sb * 6) + (parsed.h * 3) + (parsed.bb * 2);
        if (parsed.hr >= 2) badges.push({ text: `💣 ${parsed.hr} HRs`, color: 'bg-rose-500/20 text-rose-300 border-rose-500/30' });
        else if (parsed.hr === 1) badges.push({ text: '💣 1 HR', color: 'bg-amber-500/20 text-amber-300 border-amber-500/30' });
        if (parsed.rbi >= 3) badges.push({ text: `🎯 ${parsed.rbi} RBI`, color: 'bg-blue-500/20 text-blue-300 border-blue-500/30' });
        if (parsed.sb >= 2) badges.push({ text: `⚡ ${parsed.sb} SB`, color: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30' });
        else if (parsed.sb === 1 && parsed.hr >= 1) badges.push({ text: '⚡ 1 SB', color: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30' });
        if (parsed.h >= 3) badges.push({ text: `🔥 ${parsed.h} Hits`, color: 'bg-purple-500/20 text-purple-300 border-purple-500/30' });
      }

      if (isNotablePitcher) {
        regretScore += (parsed.qs * 20) + (parsed.k * 3.5) + (parsed.svHd * 12) + (parsed.w * 8) + (parsed.ip * 3.5) - (parsed.er * 5) - ((parsed.bbAll + parsed.hAll) * 1);
        if (parsed.qs >= 1) badges.push({ text: '🛡️ Quality Start', color: 'bg-teal-500/20 text-teal-300 border-teal-500/30' });
        if (parsed.k >= 7) badges.push({ text: `🔥 ${parsed.k} Ks`, color: 'bg-rose-500/20 text-rose-300 border-rose-500/30' });
        else if (parsed.k >= 5) badges.push({ text: `🔥 ${parsed.k} Ks`, color: 'bg-amber-500/20 text-amber-300 border-amber-500/30' });
        if (parsed.sv >= 1) badges.push({ text: '🔒 Save', color: 'bg-indigo-500/20 text-indigo-300 border-indigo-500/30' });
        if (parsed.hd >= 1) badges.push({ text: '🤝 Hold', color: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/30' });
        if (parsed.w >= 1) badges.push({ text: '🏆 Win', color: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30' });
      }

      // Build clean stat line description
      const lineParts = [];
      if (parsed.isBatter) {
        if (parsed.hr > 0) lineParts.push(`${parsed.hr} HR`);
        if (parsed.rbi > 0) lineParts.push(`${parsed.rbi} RBI`);
        if (parsed.r > 0) lineParts.push(`${parsed.r} R`);
        if (parsed.sb > 0) lineParts.push(`${parsed.sb} SB`);
        if (parsed.ab > 0) lineParts.push(`${parsed.h}-for-${parsed.ab}`);
        if (parsed.bb > 0) lineParts.push(`${parsed.bb} BB`);
      }
      if (parsed.isPitcher) {
        const ipDisplay = `${Math.floor(parsed.ip)}.${Math.round((parsed.ip % 1) * 3)}`;
        lineParts.push(`${ipDisplay} IP`);
        lineParts.push(`${parsed.er} ER`);
        lineParts.push(`${parsed.k} K`);
        if (parsed.bbAll > 0) lineParts.push(`${parsed.bbAll} BB`);
        if (parsed.w > 0) lineParts.push('W');
        if (parsed.qs > 0) lineParts.push('QS');
        if (parsed.sv > 0) lineParts.push('SV');
        if (parsed.hd > 0) lineParts.push('HD');
      }

      const teamObj = TEAMS[r.team_id] || { id: r.team_id, name: `Team ${r.team_id}` };
      const dateStr = getDateFromPeriodId(r.scoring_period_id, r.season_year || selectedSeason);

      list.push({
        id: `${r.scoring_period_id}-${r.player_id}-${r.team_id}`,
        playerId: r.player_id,
        playerName: r.full_name,
        teamId: r.team_id,
        team: teamObj,
        period: r.scoring_period_id,
        date: dateStr,
        slotId: r.lineup_slot_id,
        slotName: r.lineup_slot_id === 17 ? 'IL' : 'Bench',
        parsed,
        badges,
        statLine: lineParts.join(', '),
        regretScore,
        isBatter: isNotableBatter,
        isPitcher: isNotablePitcher
      });
    });

    return list.sort((a, b) => b.regretScore - a.regretScore);
  }, [allStats, selectedSeason]);

  // Filtered performances
  const filteredPerformances = useMemo(() => {
    return benchPerformances.filter(item => {
      // Search
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        if (!item.playerName.toLowerCase().includes(q) && !item.team.name.toLowerCase().includes(q)) {
          return false;
        }
      }

      // Player Type
      if (typeFilter === 'BATTER' && !item.isBatter) return false;
      if (typeFilter === 'PITCHER' && !item.isPitcher) return false;

      // Team
      if (teamFilter !== 'ALL' && String(item.teamId) !== String(teamFilter)) return false;

      // Slot
      if (slotFilter === 'BENCH' && item.slotId !== 16) return false;
      if (slotFilter === 'IL' && item.slotId !== 17) return false;

      // Category filter
      if (catFilter === 'HR' && item.parsed.hr < 1) return false;
      if (catFilter === 'RBI' && item.parsed.rbi < 3) return false;
      if (catFilter === 'R' && item.parsed.r < 3) return false;
      if (catFilter === 'SB' && item.parsed.sb < 1) return false;
      if (catFilter === 'QS' && item.parsed.qs < 1) return false;
      if (catFilter === 'K' && item.parsed.k < 5) return false;
      if (catFilter === 'SV_HD' && item.parsed.svHd < 1) return false;

      return true;
    });
  }, [benchPerformances, searchQuery, typeFilter, teamFilter, slotFilter, catFilter]);

  // KPI calculations
  const summaryKpis = useMemo(() => {
    let totalBenchHr = 0;
    let totalBenchQs = 0;
    let totalBenchK = 0;
    let totalBenchSvHd = 0;

    teamAggregates.forEach(t => {
      totalBenchHr += t.bench.HR || 0;
      totalBenchQs += t.bench.QS || 0;
      totalBenchK += t.bench.K || 0;
      totalBenchSvHd += t.bench['SV+HDs'] || 0;
    });

    // Top bench victim (most wasted HRs)
    const sortedByHr = [...teamAggregates].sort((a, b) => (b.bench.HR || 0) - (a.bench.HR || 0));
    const topHrVictim = sortedByHr[0];

    // Single top bench performance
    const topGame = benchPerformances[0];

    return {
      totalBenchHr,
      totalBenchQs,
      totalBenchK,
      totalBenchSvHd,
      topHrVictim,
      topGame
    };
  }, [teamAggregates, benchPerformances]);

  // Sorted team aggregates
  const sortedTeamRows = useMemo(() => {
    const rows = [...teamAggregates];
    rows.sort((a, b) => {
      let valA = 0;
      let valB = 0;

      if (tableSortKey === 'bench_hr') { valA = a.bench.HR || 0; valB = b.bench.HR || 0; }
      else if (tableSortKey === 'bench_rbi') { valA = a.bench.RBI || 0; valB = b.bench.RBI || 0; }
      else if (tableSortKey === 'bench_r') { valA = a.bench.R || 0; valB = b.bench.R || 0; }
      else if (tableSortKey === 'bench_sb') { valA = a.bench.SB || 0; valB = b.bench.SB || 0; }
      else if (tableSortKey === 'bench_obp') { valA = a.bench.OBP_raw || 0; valB = b.bench.OBP_raw || 0; }
      else if (tableSortKey === 'bench_ops') { valA = a.bench.OPS_raw || 0; valB = b.bench.OPS_raw || 0; }
      else if (tableSortKey === 'bench_k') { valA = a.bench.K || 0; valB = b.bench.K || 0; }
      else if (tableSortKey === 'bench_qs') { valA = a.bench.QS || 0; valB = b.bench.QS || 0; }
      else if (tableSortKey === 'bench_svhd') { valA = a.bench['SV+HDs'] || 0; valB = b.bench['SV+HDs'] || 0; }
      else if (tableSortKey === 'bench_era') { valA = a.bench.ERA_raw || 0; valB = b.bench.ERA_raw || 0; }
      else if (tableSortKey === 'bench_whip') { valA = a.bench.WHIP_raw || 0; valB = b.bench.WHIP_raw || 0; }
      else if (tableSortKey === 'waste_hr_pct') { valA = a.waste.hrPct; valB = b.waste.hrPct; }
      else if (tableSortKey === 'waste_qs_pct') { valA = a.waste.qsPct; valB = b.waste.qsPct; }
      else if (tableSortKey === 'waste_k_pct') { valA = a.waste.kPct; valB = b.waste.kPct; }
      else { valA = a.bench.HR || 0; valB = b.bench.HR || 0; }

      return tableSortDir === 'desc' ? valB - valA : valA - valB;
    });
    return rows;
  }, [teamAggregates, tableSortKey, tableSortDir]);

  const requestTableSort = (key) => {
    if (tableSortKey === key) {
      setTableSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setTableSortKey(key);
      setTableSortDir('desc');
    }
  };

  return (
    <div className="space-y-6 animate-fadeIn pb-10">
      {/* Header KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-md flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-rose-500/10 border border-rose-500/20 flex items-center justify-center text-2xl text-rose-400">
            💣
          </div>
          <div>
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Wasted Home Runs</div>
            <div className="text-2xl font-black text-white">{summaryKpis.totalBenchHr} HRs</div>
            <div className="text-xs text-rose-400 font-semibold">Left on the bench league-wide</div>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-md flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-teal-500/10 border border-teal-500/20 flex items-center justify-center text-2xl text-teal-400">
            🛡️
          </div>
          <div>
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Benched Quality Starts</div>
            <div className="text-2xl font-black text-white">{summaryKpis.totalBenchQs} QS</div>
            <div className="text-xs text-teal-400 font-semibold">{summaryKpis.totalBenchK} Ks thrown on the pine</div>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-md flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-amber-500/10 border border-amber-500/20 flex items-center justify-center text-2xl text-amber-400">
            🛋️
          </div>
          <div>
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Top Bench Victim</div>
            <div className="text-base font-black text-white truncate max-w-[170px]">
              {summaryKpis.topHrVictim?.team?.name || 'N/A'}
            </div>
            <div className="text-xs text-amber-400 font-semibold">
              {summaryKpis.topHrVictim?.bench.HR || 0} HRs wasted ({summaryKpis.topHrVictim?.waste.hrPct.toFixed(1)}% of total)
            </div>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-md flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-purple-500/10 border border-purple-500/20 flex items-center justify-center text-2xl text-purple-400">
            👑
          </div>
          <div>
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">Top Benched Game</div>
            <div className="text-base font-black text-white truncate max-w-[170px]">
              {summaryKpis.topGame?.playerName || 'N/A'}
            </div>
            <div className="text-xs text-purple-400 font-semibold truncate max-w-[170px]">
              {summaryKpis.topGame?.statLine || 'No recorded games'}
            </div>
          </div>
        </div>
      </div>

      {/* Main View Navigation Switcher */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-2xl">🛋️</span>
            <h2 className="text-xl font-black text-white tracking-tight">Bench Stats & Pine Production</h2>
          </div>
          <p className="text-slate-400 text-xs mt-0.5">
            Identify biggest performances benched by owners, and analyze total production left on the pine.
          </p>
        </div>

        <div className="inline-flex bg-slate-950 p-1.5 rounded-xl border border-slate-800 shadow-inner">
          <button
            onClick={() => setSubTab('performances')}
            className={`px-4 py-2 rounded-lg text-xs font-black uppercase tracking-wider transition-all cursor-pointer ${
              subTab === 'performances'
                ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 shadow-md font-black'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            🏆 Top Bench Performances ({benchPerformances.length})
          </button>
          <button
            onClick={() => setSubTab('teamTotals')}
            className={`px-4 py-2 rounded-lg text-xs font-black uppercase tracking-wider transition-all cursor-pointer ${
              subTab === 'teamTotals'
                ? 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-md font-black'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            📊 Team Bench Totals & Waste
          </button>
        </div>
      </div>

      {/* SUBTAB 1: BEST BENCH PERFORMANCES */}
      {subTab === 'performances' && (
        <div className="space-y-4">
          {/* Filters Bar */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-md space-y-3">
            {/* Category Quick Filter Pills */}
            <div className="flex items-center gap-1.5 flex-wrap">
              <span className="text-xs font-bold text-slate-400 mr-2 uppercase tracking-wider">Highlight:</span>
              {[
                { id: 'ALL', label: '🔥 All Notable' },
                { id: 'HR', label: '💣 Home Runs' },
                { id: 'RBI', label: '🎯 3+ RBI' },
                { id: 'R', label: '🏃 3+ Runs' },
                { id: 'SB', label: '⚡ Stolen Bases' },
                { id: 'QS', label: '🛡️ Quality Starts' },
                { id: 'K', label: '🔥 5+ Ks' },
                { id: 'SV_HD', label: '🔒 Saves & Holds' },
              ].map(cat => (
                <button
                  key={cat.id}
                  onClick={() => setCatFilter(cat.id)}
                  className={`px-3 py-1 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                    catFilter === cat.id
                      ? 'bg-amber-500 text-slate-950 shadow-xs font-black'
                      : 'bg-slate-950 text-slate-400 hover:text-white hover:bg-slate-800'
                  }`}
                >
                  {cat.label}
                </button>
              ))}
            </div>

            {/* Dropdown Filters & Search */}
            <div className="flex items-center justify-between flex-wrap gap-3 pt-2 border-t border-slate-800/80">
              <div className="flex items-center gap-2 flex-wrap">
                <input
                  type="text"
                  placeholder="Search player or team..."
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  className="bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-amber-500 w-44"
                />

                <select
                  value={typeFilter}
                  onChange={e => setTypeFilter(e.target.value)}
                  className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-amber-500 cursor-pointer"
                >
                  <option value="ALL">All Player Types</option>
                  <option value="BATTER">Batters Only</option>
                  <option value="PITCHER">Pitchers Only</option>
                </select>

                <select
                  value={teamFilter}
                  onChange={e => setTeamFilter(e.target.value)}
                  className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-amber-500 cursor-pointer"
                >
                  <option value="ALL">All Teams</option>
                  {humanTeams.map(t => (
                    <option key={t.id} value={t.id}>{t.name}</option>
                  ))}
                </select>

                <select
                  value={slotFilter}
                  onChange={e => setSlotFilter(e.target.value)}
                  className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-amber-500 cursor-pointer"
                >
                  <option value="ALL">Bench & IL</option>
                  <option value="BENCH">Bench Only (Slot 16)</option>
                  <option value="IL">IL Only (Slot 17)</option>
                </select>
              </div>

              <div className="text-xs text-slate-400 font-semibold">
                Showing <strong className="text-white">{filteredPerformances.length}</strong> benched performances
              </div>
            </div>
          </div>

          {/* Performances Table */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl shadow-xl overflow-hidden">
            {filteredPerformances.length === 0 ? (
              <div className="py-16 text-center text-slate-400 text-sm">
                No bench performances match the selected filters.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs border-collapse">
                  <thead>
                    <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black select-none">
                      <th className="py-3 px-3 text-center w-12">#</th>
                      <th className="py-3 px-4">Player</th>
                      <th className="py-3 px-4">Fantasy Team</th>
                      <th className="py-3 px-3 text-center">Date & Period</th>
                      <th className="py-3 px-3 text-center">Slot</th>
                      <th className="py-3 px-4">Highlight Badges</th>
                      <th className="py-3 px-4">Stat Line</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                    {filteredPerformances.slice(0, 150).map((item, idx) => (
                      <tr key={item.id} className="hover:bg-slate-800/40 transition-colors group">
                        <td className="py-3 px-3 text-center font-bold text-slate-500 group-hover:text-amber-400">
                          {idx + 1}
                        </td>

                        {/* Player */}
                        <td className="py-3 px-4 font-bold">
                          <span
                            onClick={() => onPlayerClick && onPlayerClick(item.playerId, item.playerName)}
                            className="text-white hover:text-blue-400 hover:underline cursor-pointer"
                          >
                            {item.playerName}
                          </span>
                        </td>

                        {/* Team */}
                        <td className="py-3 px-4">
                          <button
                            onClick={() => onOwnerClick && onOwnerClick(item.team)}
                            className="flex items-center gap-2 hover:opacity-80 transition-opacity cursor-pointer text-left"
                          >
                            <TeamAvatar team={item.team} size="xs" />
                            <span className="font-bold text-slate-300 group-hover:text-white">
                              {item.team.name}
                            </span>
                          </button>
                        </td>

                        {/* Date & Scoring Period */}
                        <td className="py-3 px-3 text-center text-slate-400 font-mono text-[11px]">
                          <div>{item.date}</div>
                          <div className="text-[10px] text-slate-500">Day {item.period}</div>
                        </td>

                        {/* Slot */}
                        <td className="py-3 px-3 text-center">
                          <span
                            className={`px-2 py-0.5 rounded text-[10px] font-black uppercase ${
                              item.slotId === 17
                                ? 'bg-rose-500/20 text-rose-300 border border-rose-500/40'
                                : 'bg-slate-800 text-slate-300 border border-slate-700'
                            }`}
                          >
                            {item.slotName}
                          </span>
                        </td>

                        {/* Badges */}
                        <td className="py-3 px-4">
                          <div className="flex items-center gap-1.5 flex-wrap">
                            {item.badges.map((b, bIdx) => (
                              <span
                                key={bIdx}
                                className={`px-2 py-0.5 rounded text-[10px] font-black border shadow-xs ${b.color}`}
                              >
                                {b.text}
                              </span>
                            ))}
                          </div>
                        </td>

                        {/* Statline */}
                        <td className="py-3 px-4 font-mono font-bold text-white text-xs">
                          {item.statLine}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* SUBTAB 2: TEAM BENCH TOTALS & PRODUCTION WASTED */}
      {subTab === 'teamTotals' && (
        <div className="space-y-4">
          {/* Sub-navigation for Batting / Pitching / Waste */}
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="inline-flex bg-slate-900 p-1 rounded-xl border border-slate-800">
              <button
                onClick={() => setTotalsTab('batting')}
                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                  totalsTab === 'batting' ? 'bg-blue-600 text-white font-black shadow-xs' : 'text-slate-400 hover:text-white'
                }`}
              >
                ⚾ Bench Batting Totals
              </button>
              <button
                onClick={() => setTotalsTab('pitching')}
                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                  totalsTab === 'pitching' ? 'bg-teal-600 text-white font-black shadow-xs' : 'text-slate-400 hover:text-white'
                }`}
              >
                🔥 Bench Pitching Totals
              </button>
              <button
                onClick={() => setTotalsTab('waste')}
                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                  totalsTab === 'waste' ? 'bg-rose-600 text-white font-black shadow-xs' : 'text-slate-400 hover:text-white'
                }`}
              >
                📉 Pine Waste Percentage (%)
              </button>
            </div>

            <div className="text-xs text-slate-400">
              Click any column header to sort teams
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-2xl shadow-xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs border-collapse">
                <thead>
                  <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black select-none">
                    <th className="py-3 px-4 sticky left-0 bg-slate-950 z-10 w-44">Team</th>

                    {totalsTab === 'batting' && (
                      <>
                        <th onClick={() => requestTableSort('bench_hr')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          HR {tableSortKey === 'bench_hr' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_rbi')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          RBI {tableSortKey === 'bench_rbi' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_r')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          R {tableSortKey === 'bench_r' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_sb')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          SB {tableSortKey === 'bench_sb' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_obp')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          OBP {tableSortKey === 'bench_obp' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_ops')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          OPS {tableSortKey === 'bench_ops' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th className="py-3 px-3 text-center">Bench PA</th>
                      </>
                    )}

                    {totalsTab === 'pitching' && (
                      <>
                        <th onClick={() => requestTableSort('bench_qs')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          QS {tableSortKey === 'bench_qs' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_k')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          K {tableSortKey === 'bench_k' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_svhd')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          SV+HD {tableSortKey === 'bench_svhd' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_era')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          ERA {tableSortKey === 'bench_era' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('bench_whip')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          WHIP {tableSortKey === 'bench_whip' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th className="py-3 px-3 text-center">Bench IP</th>
                      </>
                    )}

                    {totalsTab === 'waste' && (
                      <>
                        <th onClick={() => requestTableSort('waste_hr_pct')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          Wasted HR % {tableSortKey === 'waste_hr_pct' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('waste_qs_pct')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          Wasted QS % {tableSortKey === 'waste_qs_pct' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th onClick={() => requestTableSort('waste_k_pct')} className="py-3 px-3 text-center cursor-pointer hover:text-white">
                          Wasted K % {tableSortKey === 'waste_k_pct' ? (tableSortDir === 'desc' ? '▼' : '▲') : ''}
                        </th>
                        <th className="py-3 px-3 text-center">Wasted RBI %</th>
                        <th className="py-3 px-3 text-center">Wasted R %</th>
                        <th className="py-3 px-3 text-center">Wasted SB %</th>
                      </>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                  {sortedTeamRows.map(row => (
                    <tr
                      key={row.team.id}
                      onClick={() => onOwnerClick && onOwnerClick(row.team)}
                      className="hover:bg-slate-800/40 transition-colors cursor-pointer group"
                    >
                      <td className="py-3 px-4 sticky left-0 bg-slate-900 group-hover:bg-slate-800/60 z-10 border-r border-slate-800">
                        <div className="flex items-center gap-3">
                          <TeamAvatar team={row.team} size="sm" />
                          <span className="font-bold text-white group-hover:text-blue-400">
                            {row.team.name}
                          </span>
                        </div>
                      </td>

                      {totalsTab === 'batting' && (
                        <>
                          <td className="py-3 px-3 text-center font-mono font-black text-rose-400">
                            {row.bench.HR || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-bold text-white">
                            {row.bench.RBI || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-bold text-white">
                            {row.bench.R || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-bold text-emerald-400">
                            {row.bench.SB || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.bench.OBP || '.000'}
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.bench.OPS || '.000'}
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-500">
                            {row.bench.PA || 0}
                          </td>
                        </>
                      )}

                      {totalsTab === 'pitching' && (
                        <>
                          <td className="py-3 px-3 text-center font-mono font-black text-teal-400">
                            {row.bench.QS || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-bold text-white">
                            {row.bench.K || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-bold text-amber-400">
                            {row.bench['SV+HDs'] || 0}
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.bench.ERA || '0.00'}
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.bench.WHIP || '0.00'}
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-500">
                            {row.bench.IP ? `${Math.floor(row.bench.IP)}.${Math.round((row.bench.IP % 1) * 3)}` : '0.0'}
                          </td>
                        </>
                      )}

                      {totalsTab === 'waste' && (
                        <>
                          <td className="py-3 px-3 text-center font-mono font-black text-rose-400">
                            {row.waste.hrPct.toFixed(1)}%
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-black text-teal-400">
                            {row.waste.qsPct.toFixed(1)}%
                          </td>
                          <td className="py-3 px-3 text-center font-mono font-bold text-white">
                            {row.waste.kPct.toFixed(1)}%
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.waste.rbiPct.toFixed(1)}%
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.waste.rPct.toFixed(1)}%
                          </td>
                          <td className="py-3 px-3 text-center font-mono text-slate-300">
                            {row.waste.sbPct.toFixed(1)}%
                          </td>
                        </>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
