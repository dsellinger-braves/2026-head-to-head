// src/views/OptimalLineupSimulatorView.jsx
import React, { useState, useMemo } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { simulateSeasonBestLineups, optimizeDailyTeamLineup, buildPlayerPositionRegistry } from '../utils/rosterOptimizer';
import TeamAvatar from '../components/TeamAvatar';

export default function OptimalLineupSimulatorView({
  allStats = [],
  selectedSeason = 2026,
  onOwnerClick,
  onPlayerClick
}) {
  const [activeTab, setActiveTab] = useState('efficiency'); // 'efficiency' | 'standings' | 'blunders' | 'inspector'
  const [selectedTeamId, setSelectedTeamId] = useState(5); // Default to Dan (5) or first team
  const [selectedPeriod, setSelectedPeriod] = useState(1);
  const [blunderFilter, setBlunderFilter] = useState('ALL'); // 'ALL' | 'BATTER' | 'PITCHER'
  const [searchBlunder, setSearchBlunder] = useState('');

  // 1. Run simulation across the season
  const simulation = useMemo(() => {
    if (!allStats || allStats.length === 0) return null;
    return simulateSeasonBestLineups(allStats, TEAMS);
  }, [allStats]);

  // Position registry for quick lookup
  const registry = useMemo(() => {
    return buildPlayerPositionRegistry(allStats);
  }, [allStats]);

  // Available periods from simulation
  const periods = useMemo(() => simulation?.periods || [], [simulation]);

  // Update selected period if out of range
  const currentPeriod = useMemo(() => {
    if (periods.length === 0) return 1;
    if (periods.includes(selectedPeriod)) return selectedPeriod;
    return periods[periods.length - 1];
  }, [periods, selectedPeriod]);

  // 2. Day-by-Day Inspector data for current team and current period
  const dayInspectorData = useMemo(() => {
    if (!allStats || allStats.length === 0) return null;
    const teamDayRecords = allStats.filter(
      r => r.team_id === selectedTeamId && r.scoring_period_id === currentPeriod
    );
    if (teamDayRecords.length === 0) return null;
    return optimizeDailyTeamLineup(teamDayRecords, registry);
  }, [allStats, selectedTeamId, currentPeriod, registry]);

  if (!simulation) {
    return (
      <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-12 text-center">
        <div className="animate-spin text-4xl mb-4">⚙️</div>
        <h3 className="text-lg font-bold text-gray-800">Calculating Best Possible Lineups...</h3>
        <p className="text-sm text-gray-500 mt-1">Analyzing all daily active, bench, and IL combinations across {selectedSeason}.</p>
      </div>
    );
  }

  const { managers, topBenchBlunders } = simulation;

  // Filtered blunders
  const filteredBlunders = topBenchBlunders.filter(b => {
    if (blunderFilter === 'BATTER' && b.isPitcher) return false;
    if (blunderFilter === 'PITCHER' && !b.isPitcher) return false;
    if (searchBlunder.trim()) {
      const q = searchBlunder.toLowerCase();
      return b.player.name.toLowerCase().includes(q) || b.teamName.toLowerCase().includes(q);
    }
    return true;
  });

  return (
    <div className="space-y-6">
      {/* Hero Banner */}
      <div className="bg-gradient-to-br from-indigo-900 via-slate-900 to-blue-950 rounded-3xl p-6 md:p-8 text-white shadow-xl border border-blue-800/40 relative overflow-hidden">
        <div className="absolute -right-10 -bottom-10 opacity-10 pointer-events-none text-9xl">
          ✨
        </div>
        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div>
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-500/20 border border-blue-400/30 text-blue-300 text-xs font-bold uppercase tracking-wider mb-2">
              <span>🔮 20/20 Hindsight Optimizer</span>
              <span>•</span>
              <span>{selectedSeason} Season</span>
            </div>
            <h2 className="text-2xl md:text-3xl font-black tracking-tight text-white flex items-center gap-3">
              <span>Best Lineup Possible Simulator</span>
            </h2>
            <p className="text-sm text-blue-200/80 mt-1.5 max-w-2xl">
              What if every manager played their absolute optimal lineup every single day?
              Simulating day-by-day optimal Bench & IL moves with zero ownership changes to reveal true roster ceilings and management efficiency.
            </p>
          </div>

          {/* Quick Metrics Badge Deck */}
          <div className="flex items-center gap-3 bg-white/5 border border-white/10 p-3 rounded-2xl backdrop-blur-md">
            <div className="text-center px-3 border-r border-white/10">
              <div className="text-xs text-blue-300 font-bold uppercase tracking-wider">Active Starters</div>
              <div className="text-lg font-black text-white">15 Bat · 12 Pit</div>
            </div>
            <div className="text-center px-3">
              <div className="text-xs text-blue-300 font-bold uppercase tracking-wider">Days Analyzed</div>
              <div className="text-lg font-black text-white">{periods.length} Days</div>
            </div>
          </div>
        </div>

        {/* Sub Navigation Bar */}
        <div className="flex flex-wrap gap-2 mt-6 pt-5 border-t border-white/10">
          <button
            onClick={() => setActiveTab('efficiency')}
            className={`px-4 py-2 rounded-xl text-xs font-black transition-all flex items-center gap-2 ${
              activeTab === 'efficiency'
                ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/30 ring-1 ring-blue-400'
                : 'bg-white/10 text-blue-200 hover:bg-white/20 hover:text-white'
            }`}
          >
            <span>📊</span>
            <span>Manager Efficiency</span>
          </button>
          <button
            onClick={() => setActiveTab('standings')}
            className={`px-4 py-2 rounded-xl text-xs font-black transition-all flex items-center gap-2 ${
              activeTab === 'standings'
                ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/30 ring-1 ring-blue-400'
                : 'bg-white/10 text-blue-200 hover:bg-white/20 hover:text-white'
            }`}
          >
            <span>🏆</span>
            <span>Optimal vs Actual Standings</span>
          </button>
          <button
            onClick={() => setActiveTab('blunders')}
            className={`px-4 py-2 rounded-xl text-xs font-black transition-all flex items-center gap-2 ${
              activeTab === 'blunders'
                ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/30 ring-1 ring-blue-400'
                : 'bg-white/10 text-blue-200 hover:bg-white/20 hover:text-white'
            }`}
          >
            <span>🤦</span>
            <span>Hall of Regret (Bench Blunders)</span>
          </button>
          <button
            onClick={() => setActiveTab('inspector')}
            className={`px-4 py-2 rounded-xl text-xs font-black transition-all flex items-center gap-2 ${
              activeTab === 'inspector'
                ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/30 ring-1 ring-blue-400'
                : 'bg-white/10 text-blue-200 hover:bg-white/20 hover:text-white'
            }`}
          >
            <span>🔍</span>
            <span>Day-by-Day Roster Inspector</span>
          </button>
        </div>
      </div>

      {/* TAB 1: Manager Efficiency */}
      {activeTab === 'efficiency' && (
        <div className="space-y-6">
          <div className="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden">
            <div className="p-5 border-b border-gray-100 flex flex-col sm:flex-row sm:items-center justify-between gap-3">
              <div>
                <h3 className="text-base font-bold text-gray-900">Lineup Management Efficiency Leaderboard</h3>
                <p className="text-xs text-gray-500">
                  Measures the percentage of total optimal rotisserie points captured by each manager through their actual lineup decisions.
                </p>
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead className="bg-gray-50 text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-200">
                  <tr>
                    <th className="px-4 py-3 text-left">Rank</th>
                    <th className="px-4 py-3 text-left">Team & Owner</th>
                    <th className="px-4 py-3 text-center">Efficiency %</th>
                    <th className="px-4 py-3 text-center">Actual Pts</th>
                    <th className="px-4 py-3 text-center">Optimal Pts</th>
                    <th className="px-4 py-3 text-center">Pts Left on Bench</th>
                    <th className="px-4 py-3 text-center">Suboptimal Starts</th>
                    <th className="px-4 py-3 text-center">Net HR Lost</th>
                    <th className="px-4 py-3 text-center">Net RBI Lost</th>
                    <th className="px-4 py-3 text-center">Net K Lost</th>
                    <th className="px-4 py-3 text-center">Net QS Lost</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {managers.map(m => {
                    const teamObj = TEAMS[m.teamId] || { id: m.teamId, name: m.teamName, owner: m.owner };
                    return (
                      <tr
                        key={m.teamId}
                        onClick={() => onOwnerClick && onOwnerClick(teamObj)}
                        className="hover:bg-blue-50/60 cursor-pointer transition-colors"
                      >
                        <td className="px-4 py-3.5 font-mono font-bold text-gray-600">
                          #{m.actualRank}
                        </td>
                        <td className="px-4 py-3.5">
                          <div className="flex items-center gap-3">
                            <TeamAvatar team={teamObj} size="sm" />
                            <div>
                              <div className="font-bold text-gray-900">{m.teamName}</div>
                              <div className="text-xs text-gray-400 font-medium">{m.owner}</div>
                            </div>
                          </div>
                        </td>
                        <td className="px-4 py-3.5 text-center">
                          <div className="flex flex-col items-center gap-1">
                            <span className="font-mono font-black text-sm text-gray-900">
                              {m.efficiencyPct.toFixed(1)}%
                            </span>
                            <div className="w-20 bg-gray-200 rounded-full h-1.5 overflow-hidden">
                              <div
                                className={`h-full rounded-full ${
                                  m.efficiencyPct >= 90 ? 'bg-emerald-500' : m.efficiencyPct >= 80 ? 'bg-blue-500' : 'bg-amber-500'
                                }`}
                                style={{ width: `${Math.min(100, m.efficiencyPct)}%` }}
                              />
                            </div>
                          </div>
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-bold text-gray-700">
                          {m.actualRotoPoints.toFixed(1)}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-bold text-blue-700">
                          {m.optimalRotoPoints.toFixed(1)}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-black text-rose-600">
                          -{m.pointsLeftOnTable.toFixed(1)}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono text-gray-600">
                          {m.suboptimalStarts}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-bold text-amber-600">
                          +{m.categoryDeltas.HR}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-bold text-amber-600">
                          +{m.categoryDeltas.RBI}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-bold text-indigo-600">
                          +{m.categoryDeltas.K}
                        </td>
                        <td className="px-4 py-3.5 text-center font-mono font-bold text-indigo-600">
                          +{m.categoryDeltas.QS}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* TAB 2: Standings Comparison */}
      {activeTab === 'standings' && (
        <div className="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="p-5 border-b border-gray-100">
            <h3 className="text-base font-bold text-gray-900">Actual vs. Optimal Standings</h3>
            <p className="text-xs text-gray-500">
              How the standings would change if every manager had made the optimal start/sit decision every day.
            </p>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50 text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-200">
                <tr>
                  <th className="px-4 py-3 text-center">Actual Rank</th>
                  <th className="px-4 py-3 text-center">Optimal Rank</th>
                  <th className="px-4 py-3 text-center">Movement</th>
                  <th className="px-4 py-3 text-left">Team & Owner</th>
                  <th className="px-4 py-3 text-center">Actual Pts</th>
                  <th className="px-4 py-3 text-center">Optimal Pts</th>
                  <th className="px-4 py-3 text-center">Pts Delta</th>
                  <th className="px-4 py-3 text-center">R</th>
                  <th className="px-4 py-3 text-center">HR</th>
                  <th className="px-4 py-3 text-center">RBI</th>
                  <th className="px-4 py-3 text-center">SB</th>
                  <th className="px-4 py-3 text-center">K</th>
                  <th className="px-4 py-3 text-center">QS</th>
                  <th className="px-4 py-3 text-center">SV+H</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {[...managers].sort((a, b) => a.optimalRank - b.optimalRank).map(m => {
                  const teamObj = TEAMS[m.teamId] || { id: m.teamId, name: m.teamName, owner: m.owner };
                  const movedUp = m.rankChange > 0;
                  const movedDown = m.rankChange < 0;

                  return (
                    <tr
                      key={m.teamId}
                      onClick={() => onOwnerClick && onOwnerClick(teamObj)}
                      className="hover:bg-blue-50/60 cursor-pointer transition-colors"
                    >
                      <td className="px-4 py-3.5 text-center font-mono text-gray-400">
                        #{m.actualRank}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono font-black text-base text-blue-900">
                        #{m.optimalRank}
                      </td>
                      <td className="px-4 py-3.5 text-center">
                        {movedUp && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-black bg-emerald-100 text-emerald-800">
                            ▲ +{m.rankChange}
                          </span>
                        )}
                        {movedDown && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-black bg-rose-100 text-rose-800">
                            ▼ {m.rankChange}
                          </span>
                        )}
                        {m.rankChange === 0 && (
                          <span className="text-gray-400 font-bold text-xs">—</span>
                        )}
                      </td>
                      <td className="px-4 py-3.5">
                        <div className="flex items-center gap-3">
                          <TeamAvatar team={teamObj} size="sm" />
                          <div>
                            <div className="font-bold text-gray-900">{m.teamName}</div>
                            <div className="text-xs text-gray-400 font-medium">{m.owner}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono font-bold text-gray-600">
                        {m.actualRotoPoints.toFixed(1)}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono font-black text-blue-700">
                        {m.optimalRotoPoints.toFixed(1)}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono font-black text-emerald-600">
                        +{m.rotoPointsDelta.toFixed(1)}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs">
                        +{m.categoryDeltas.R}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs font-bold text-amber-700">
                        +{m.categoryDeltas.HR}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs">
                        +{m.categoryDeltas.RBI}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs">
                        +{m.categoryDeltas.SB}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs font-bold text-indigo-700">
                        +{m.categoryDeltas.K}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs">
                        +{m.categoryDeltas.QS}
                      </td>
                      <td className="px-4 py-3.5 text-center font-mono text-xs">
                        +{m.categoryDeltas['SV+HDs']}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* TAB 3: Hall of Regret (Bench Blunders) */}
      {activeTab === 'blunders' && (
        <div className="space-y-4">
          <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div>
              <h3 className="text-base font-bold text-gray-900">Worst Bench Blunders of the Season</h3>
              <p className="text-xs text-gray-500">The single biggest performances left sitting on fantasy benches or IL slots.</p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <div className="flex bg-gray-100 p-1 rounded-xl">
                <button
                  onClick={() => setBlunderFilter('ALL')}
                  className={`px-3 py-1 text-xs font-bold rounded-lg transition ${blunderFilter === 'ALL' ? 'bg-white shadow-xs text-blue-600' : 'text-gray-600'}`}
                >
                  All
                </button>
                <button
                  onClick={() => setBlunderFilter('BATTER')}
                  className={`px-3 py-1 text-xs font-bold rounded-lg transition ${blunderFilter === 'BATTER' ? 'bg-white shadow-xs text-blue-600' : 'text-gray-600'}`}
                >
                  Batters
                </button>
                <button
                  onClick={() => setBlunderFilter('PITCHER')}
                  className={`px-3 py-1 text-xs font-bold rounded-lg transition ${blunderFilter === 'PITCHER' ? 'bg-white shadow-xs text-blue-600' : 'text-gray-600'}`}
                >
                  Pitchers
                </button>
              </div>

              <input
                type="text"
                placeholder="Search player or team..."
                value={searchBlunder}
                onChange={e => setSearchBlunder(e.target.value)}
                className="px-3 py-1.5 bg-gray-50 border border-gray-200 rounded-xl text-xs focus:ring-2 focus:ring-blue-500 outline-none w-48"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {filteredBlunders.map((b, idx) => {
              const teamObj = TEAMS[b.teamId] || { id: b.teamId, name: b.teamName };
              const s = b.stats || {};
              const hr = parseFloat(s.HR ?? s['5'] ?? 0);
              const rbi = parseFloat(s.RBI ?? s['21'] ?? 0);
              const r = parseFloat(s.R ?? s['20'] ?? 0);
              const sb = parseFloat(s.SB ?? s['23'] ?? 0);
              const k = parseFloat(s.K ?? s['48'] ?? 0);
              const qs = parseFloat(s.QS ?? s['63'] ?? 0);
              const sv = parseFloat(s.SV ?? s['57'] ?? 0);
              const ipRaw = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
              const ip = ipRaw > 0 ? (ipRaw < 40 ? ipRaw : (ipRaw / 3).toFixed(1)) : '0.0';

              return (
                <div
                  key={`${b.periodId}-${b.player.id}-${idx}`}
                  className="bg-white rounded-2xl shadow-sm border border-gray-200 p-4 hover:shadow-md hover:border-blue-300 transition-all"
                >
                  <div className="flex items-center justify-between pb-3 border-b border-gray-100">
                    <div className="flex items-center gap-2.5">
                      <TeamAvatar team={teamObj} size="xs" />
                      <div>
                        <div className="font-bold text-xs text-gray-900">{b.teamName}</div>
                        <div className="text-[11px] text-gray-400">
                          Scoring Period {b.periodId} · {getDateFromPeriodId(b.periodId) || `Day ${b.periodId}`}
                        </div>
                      </div>
                    </div>
                    <div className="text-right">
                      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-black bg-rose-100 text-rose-800">
                        Score: +{b.benchedScore.toFixed(1)}
                      </span>
                    </div>
                  </div>

                  <div className="mt-3 flex items-center justify-between">
                    <div>
                      <div
                        onClick={() => onPlayerClick && onPlayerClick({ id: b.player.id, name: b.player.name })}
                        className="font-black text-sm text-gray-900 hover:text-blue-600 cursor-pointer"
                      >
                        {b.player.name}
                      </div>
                      <div className="text-xs text-gray-500 font-medium">
                        Benched in slot: {b.player.actualSlotId === 17 ? 'IL' : 'Bench'}
                      </div>
                    </div>

                    <div className="bg-gray-50 px-3 py-1.5 rounded-xl border border-gray-100 text-right">
                      {!b.isPitcher ? (
                        <div className="font-mono text-xs font-bold text-gray-800">
                          {hr > 0 && <span className="text-amber-600 font-black">{hr} HR · </span>}
                          {rbi > 0 && <span>{rbi} RBI · </span>}
                          {r > 0 && <span>{r} R · </span>}
                          {sb > 0 && <span className="text-emerald-600">{sb} SB</span>}
                        </div>
                      ) : (
                        <div className="font-mono text-xs font-bold text-gray-800">
                          <span>{ip} IP · </span>
                          <span className="text-indigo-600 font-black">{k} K · </span>
                          {qs > 0 && <span className="text-blue-600 font-black">QS · </span>}
                          {sv > 0 && <span className="text-purple-600 font-black">{sv} SV</span>}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* TAB 4: Day-by-Day Roster Inspector */}
      {activeTab === 'inspector' && (
        <div className="space-y-6">
          {/* Controls Bar */}
          <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="flex flex-wrap items-center gap-4">
              <div>
                <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">Select Team</label>
                <select
                  value={selectedTeamId}
                  onChange={e => setSelectedTeamId(Number(e.target.value))}
                  className="bg-gray-50 border border-gray-300 rounded-xl px-3 py-2 text-xs font-bold text-gray-800 focus:ring-2 focus:ring-blue-500 outline-none"
                >
                  {managers.map(m => (
                    <option key={m.teamId} value={m.teamId}>
                      {m.teamName} ({m.owner})
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">
                  Scoring Period: #{currentPeriod} ({getDateFromPeriodId(currentPeriod) || `Day ${currentPeriod}`})
                </label>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setSelectedPeriod(prev => Math.max(periods[0] || 1, prev - 1))}
                    disabled={currentPeriod <= (periods[0] || 1)}
                    className="px-2.5 py-1.5 rounded-lg bg-gray-100 hover:bg-gray-200 text-xs font-bold disabled:opacity-30"
                  >
                    ◀ Prev
                  </button>
                  <input
                    type="range"
                    min={periods[0] || 1}
                    max={periods[periods.length - 1] || 180}
                    value={currentPeriod}
                    onChange={e => setSelectedPeriod(Number(e.target.value))}
                    className="w-48 accent-blue-600"
                  />
                  <button
                    onClick={() => setSelectedPeriod(prev => Math.min(periods[periods.length - 1] || 180, prev + 1))}
                    disabled={currentPeriod >= (periods[periods.length - 1] || 180)}
                    className="px-2.5 py-1.5 rounded-lg bg-gray-100 hover:bg-gray-200 text-xs font-bold disabled:opacity-30"
                  >
                    Next ▶
                  </button>
                </div>
              </div>
            </div>

            {dayInspectorData && (
              <div className="flex items-center gap-3 bg-blue-50 border border-blue-100 px-4 py-2.5 rounded-xl">
                <div>
                  <div className="text-[10px] text-blue-600 font-bold uppercase tracking-wider">Net Day Gain</div>
                  <div className="text-base font-black text-blue-900">
                    +{dayInspectorData.netScoreGain.toFixed(1)} Pts
                  </div>
                </div>
                <div className="text-xs font-bold text-blue-700 border-l border-blue-200 pl-3">
                  {dayInspectorData.promoted.length > 0 ? (
                    <span className="text-emerald-700">✨ {dayInspectorData.promoted.length} optimal swap(s) made</span>
                  ) : (
                    <span className="text-gray-500">👌 Lineup was already 100% optimal</span>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Side-by-Side Comparison */}
          {dayInspectorData && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Actual Lineup */}
              <div className="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden">
                <div className="p-4 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
                  <h4 className="font-bold text-sm text-gray-800 flex items-center gap-2">
                    <span>📋 Actual Lineup Started</span>
                  </h4>
                  <span className="text-xs font-mono font-bold text-gray-500">
                    {dayInspectorData.actualStarters.length} Starters
                  </span>
                </div>

                <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
                  {dayInspectorData.actualStarters.map(p => {
                    const wasDemoted = dayInspectorData.demoted.some(d => d.id === p.id);
                    return (
                      <div
                        key={`act-${p.id}`}
                        className={`p-3 flex items-center justify-between transition-colors ${
                          wasDemoted ? 'bg-rose-50/50' : 'hover:bg-gray-50'
                        }`}
                      >
                        <div className="flex items-center gap-3">
                          <span className="w-8 text-center text-xs font-bold text-gray-400 bg-gray-100 py-1 rounded">
                            {p.actualSlotId === 16 ? 'BN' : p.actualSlotId === 17 ? 'IL' : 'Act'}
                          </span>
                          <div>
                            <div className="font-bold text-xs text-gray-900">{p.name}</div>
                            <div className="text-[11px] text-gray-400">Score: {p.score.toFixed(1)}</div>
                          </div>
                        </div>

                        {wasDemoted && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-black bg-rose-100 text-rose-800">
                            ⬇️ Should Bench
                          </span>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Optimal Lineup */}
              <div className="bg-white rounded-2xl shadow-sm border border-blue-200 overflow-hidden">
                <div className="p-4 bg-blue-50/70 border-b border-blue-200 flex items-center justify-between">
                  <h4 className="font-bold text-sm text-blue-900 flex items-center gap-2">
                    <span>✨ Optimal Lineup (Maximized Stats)</span>
                  </h4>
                  <span className="text-xs font-mono font-bold text-blue-700">
                    {dayInspectorData.optimalStarters.length} Starters
                  </span>
                </div>

                <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
                  {dayInspectorData.optimalStarters.map(p => {
                    const wasPromoted = dayInspectorData.promoted.some(prom => prom.id === p.id);
                    return (
                      <div
                        key={`opt-${p.id}`}
                        className={`p-3 flex items-center justify-between transition-colors ${
                          wasPromoted ? 'bg-emerald-50/80' : 'hover:bg-blue-50/30'
                        }`}
                      >
                        <div className="flex items-center gap-3">
                          <span className="w-8 text-center text-xs font-bold text-blue-700 bg-blue-100 py-1 rounded">
                            Opt
                          </span>
                          <div>
                            <div className="font-bold text-xs text-gray-900">{p.name}</div>
                            <div className="text-[11px] text-gray-400">Score: {p.score.toFixed(1)}</div>
                          </div>
                        </div>

                        {wasPromoted && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-black bg-emerald-100 text-emerald-800">
                            ⬆️ Promoted from Bench
                          </span>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
