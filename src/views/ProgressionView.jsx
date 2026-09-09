import { useMemo, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, SCORING_CATS, calculateRotoPoints } from '../utils/scoring';
import { getHistoricalSeasons, getFranchiseLeaderboard, normalizeOwner } from '../data/historicalFranchiseData';

const STAT_OPTIONS = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

const ALL_TIME_METRICS = [
  { id: 'rank', label: 'Final Finish Rank (1st–10th)', isRank: true },
  { id: 'points', label: 'Total Roto Points', isRank: false },
  { id: 'hittingPoints', label: 'Hitting Points', isRank: false },
  { id: 'pitchingPoints', label: 'Pitching Points', isRank: false },
  { id: 'R', label: 'Runs (R) Points', isCat: true },
  { id: 'HR', label: 'Home Runs (HR) Points', isCat: true },
  { id: 'RBI', label: 'RBI Points', isCat: true },
  { id: 'SB', label: 'Stolen Bases (SB) Points', isCat: true },
  { id: 'OBP', label: 'On-Base % (OBP) Points', isCat: true },
  { id: 'K', label: 'Strikeouts (K) Points', isCat: true },
  { id: 'QS', label: 'Quality Starts (QS) Points', isCat: true },
  { id: 'SV+HDs', label: 'Saves + Holds Points', isCat: true },
  { id: 'ERA', label: 'ERA Points', isCat: true },
  { id: 'WHIP', label: 'WHIP Points', isCat: true }
];

const TEAM_COLORS = [
  '#2563eb', '#dc2626', '#16a34a', '#d97706',
  '#9333ea', '#db2777', '#0891b2', '#65a30d', '#ea580c',
  '#475569', '#0d9488', '#4f46e5'
];

function formatVal(val, cat, viewMode) {
  const n = parseFloat(val);
  if (isNaN(n)) return '-';
  if (viewMode === 'roto') return n % 1 === 0 ? n : n.toFixed(1);
  if (cat === 'ERA') return n.toFixed(2);
  if (cat === 'OBP') return n.toFixed(4).replace(/^0/, '');
  if (SCORING_CATS[cat]?.isRate) return n.toFixed(3).replace(/^0/, '');
  if (cat === 'IP') return `${Math.floor(n)}.${Math.round((n % 1) * 3)}`;
  if (cat === 'Matchup Points') return n % 1 === 0 ? n : n.toFixed(1);
  return Math.round(n);
}

export default function ProgressionView({ allStats, selectedSeason = 2026, processedWeeks = [] }) {
  const [scope, setScope] = useState('allTime'); // 'allTime' or 'season'

  // Single season progression state
  const [selectedStat, setSelectedStat] = useState('HR');
  const [selectedTeamId, setSelectedTeamId] = useState('all');
  const [viewMode, setViewMode] = useState('raw'); // 'raw' or 'roto'

  // All-time history progression state
  const [allTimeMetric, setAllTimeMetric] = useState('rank');
  const [allTimeOwner, setAllTimeOwner] = useState('all');
  const [activeOnly, setActiveOnly] = useState(true);
  const [expandedFranchise, setExpandedFranchise] = useState(null);

  const teamIds = useMemo(
    () => Object.keys(TEAMS).filter(id => parseInt(id) !== 99),
    []
  );

  // Precompute cumulative stats per team per scoring period for single season
  const progressionData = useMemo(() => {
    if (!allStats.length) return { allPeriods: [], cumulativeByTeam: {}, cumulativeRotoByTeam: {} };

    const allPeriodsRaw = [...new Set(allStats.map(r => r.scoring_period_id))].sort((a, b) => a - b);
    const todayStr = new Date().toISOString().slice(0, 10);
    const allPeriods = selectedSeason === new Date().getFullYear()
      ? allPeriodsRaw.filter(p => getDateFromPeriodId(p, selectedSeason) <= todayStr)
      : allPeriodsRaw;

    const byTeam = {};
    teamIds.forEach(id => { byTeam[id] = []; });
    allStats.forEach(r => {
      if (byTeam[r.team_id] !== undefined) byTeam[r.team_id].push(r);
    });

    const cumulativeByTeam = {};
    teamIds.forEach(teamId => {
      const records = [...byTeam[teamId]].sort((a, b) => a.scoring_period_id - b.scoring_period_id);
      cumulativeByTeam[teamId] = {};
      const accumulated = [];
      let idx = 0;

      allPeriods.forEach(period => {
        while (idx < records.length && records[idx].scoring_period_id <= period) {
          accumulated.push(records[idx]);
          idx++;
        }
        if (accumulated.length > 0) {
          cumulativeByTeam[teamId][period] = aggregateStats(accumulated);
        }
      });
    });

    // Calculate roto points at each period
    const cumulativeRotoByTeam = {};
    teamIds.forEach(id => { cumulativeRotoByTeam[id] = {}; });
    allPeriods.forEach(period => {
      const statsForPeriod = {};
      teamIds.forEach(id => {
        if (cumulativeByTeam[id][period]) statsForPeriod[id] = cumulativeByTeam[id][period];
      });
      const rotoForPeriod = calculateRotoPoints(statsForPeriod);
      teamIds.forEach(id => {
        if (rotoForPeriod[id]) cumulativeRotoByTeam[id][period] = rotoForPeriod[id];
      });
    });

    return { allPeriods, cumulativeByTeam, cumulativeRotoByTeam };
  }, [allStats, teamIds, selectedSeason]);

  const visibleTeamIds = useMemo(() => {
    return selectedTeamId === 'all' ? teamIds : [String(selectedTeamId)];
  }, [selectedTeamId, teamIds]);

  const chartData = useMemo(() => {
    return progressionData.allPeriods.map(period => {
      const point = { period, date: getDateFromPeriodId(period, selectedSeason) };
      visibleTeamIds.forEach(teamId => {
        if (viewMode === 'raw') {
          const stats = progressionData.cumulativeByTeam[teamId]?.[period];
          if (stats) point[TEAMS[teamId].name] = parseFloat(stats[selectedStat]) || 0;
        } else {
          const roto = progressionData.cumulativeRotoByTeam[teamId]?.[period];
          if (roto) {
            point[TEAMS[teamId].name] = parseFloat(selectedStat === 'Total Roto' ? roto.total : roto[selectedStat]) || 0;
          }
        }
      });
      return point;
    });
  }, [progressionData, selectedStat, visibleTeamIds, selectedSeason, viewMode]);

  // Matchup points data
  const matchupPtsChartData = useMemo(() => {
    if (!processedWeeks.length) return [];

    const todayStr = new Date().toISOString().slice(0, 10);
    const validWeeks = selectedSeason === new Date().getFullYear() 
      ? processedWeeks.filter(w => w.startDate <= todayStr) 
      : processedWeeks;

    const cumPts = {};
    teamIds.forEach(id => { cumPts[id] = 0; });

    return validWeeks.map(week => {
      week.matchups.forEach(m => {
        if (m.isPlaceholder) return;

        if (m.type === 'trio') {
          m.teams.forEach(team => {
            if (team.id !== null && cumPts[String(team.id)] !== undefined) {
              cumPts[String(team.id)] += m.result[team.id]?.points ?? 0;
            }
          });
        } else if (m.homeTeam && m.awayTeam) {
          const hId = String(m.homeTeam.id);
          const aId = String(m.awayTeam.id);
          if (cumPts[hId] !== undefined) cumPts[hId] += m.result?.homeScore ?? 0;
          if (cumPts[aId] !== undefined) cumPts[aId] += m.result?.awayScore ?? 0;
        }
      });

      const point = { period: week.weekNumber, date: `Week ${week.weekNumber}` };
      visibleTeamIds.forEach(teamId => {
        point[TEAMS[teamId]?.name] = cumPts[teamId];
      });
      return point;
    });
  }, [processedWeeks, teamIds, visibleTeamIds, selectedSeason]);

  // --- ALL-TIME DATA COMPUTATION ---
  const active2026Data = useMemo(() => {
    if (!allStats.length) return [];
    const groups = {};
    teamIds.forEach(id => { groups[id] = []; });
    allStats.forEach(r => {
      if (groups[r.team_id]) groups[r.team_id].push(r);
    });

    const statsMap = {};
    teamIds.forEach(id => {
      statsMap[id] = aggregateStats(groups[id] || []);
    });

    const rotoMap = calculateRotoPoints(statsMap);

    return teamIds.map(id => {
      const roto = rotoMap[id] || {};
      const hitPts = (roto.R || 0) + (roto.HR || 0) + (roto.RBI || 0) + (roto.SB || 0) + (roto.OBP || 0);
      const pitchPts = (roto.K || 0) + (roto.QS || 0) + (roto['SV+HDs'] || 0) + (roto.ERA || 0) + (roto.WHIP || 0);

      return {
        id,
        name: TEAMS[id].name,
        owner: TEAMS[id].owner,
        totalPoints: roto.total || 0,
        hittingPoints: hitPts,
        pitchingPoints: pitchPts,
        categories: roto
      };
    });
  }, [allStats, teamIds]);

  const allHistoricalRows = useMemo(() => {
    return getHistoricalSeasons(active2026Data);
  }, [active2026Data]);

  const franchiseLeaderboard = useMemo(() => {
    return getFranchiseLeaderboard(allHistoricalRows);
  }, [allHistoricalRows]);

  const availableAllTimeOwners = useMemo(() => {
    const list = franchiseLeaderboard.map(f => f.owner);
    if (activeOnly) {
      const activeSet = new Set(teamIds.map(id => normalizeOwner(TEAMS[id].owner)));
      return list.filter(o => activeSet.has(o));
    }
    return list;
  }, [franchiseLeaderboard, activeOnly, teamIds]);

  const allTimeChartData = useMemo(() => {
    const years = Array.from(new Set(allHistoricalRows.map(r => r.year))).sort((a, b) => a - b);
    const selectedOwners = allTimeOwner === 'all'
      ? availableAllTimeOwners
      : [allTimeOwner];

    const metricConf = ALL_TIME_METRICS.find(m => m.id === allTimeMetric) || ALL_TIME_METRICS[0];

    return years.map(year => {
      const point = { year: String(year) };
      const rowsForYear = allHistoricalRows.filter(r => r.year === year);

      selectedOwners.forEach(owner => {
        const row = rowsForYear.find(r => r.owner === owner);
        if (row) {
          if (metricConf.isRank) {
            point[owner] = row.rank;
          } else if (metricConf.isCat) {
            point[owner] = row.categories[metricConf.id] || 0;
          } else {
            point[owner] = row[metricConf.id] || 0;
          }
        }
      });
      return point;
    });
  }, [allHistoricalRows, allTimeMetric, allTimeOwner, availableAllTimeOwners]);

  const isLowBetter = viewMode === 'raw' && SCORING_CATS[selectedStat]?.type === 'low';
  const isMatchupPts = selectedStat === 'Matchup Points';
  const currentOptions = isMatchupPts ? ['Matchup Points'] : (viewMode === 'roto' ? ['Total Roto', ...STAT_OPTIONS] : STAT_OPTIONS);
  const activeData = isMatchupPts ? matchupPtsChartData : chartData;

  const isRankMetric = ALL_TIME_METRICS.find(m => m.id === allTimeMetric)?.isRank;

  return (
    <div className="space-y-6">
      {/* Top Scope Switcher */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-black text-gray-900">League Progression & History</h2>
          <p className="text-xs text-gray-400">
            Analyze team trajectory over the {selectedSeason} season or explore the entire 15-year league history (2012–2026).
          </p>
        </div>

        <div className="flex items-center bg-gray-200 rounded-lg p-1">
          <button
            onClick={() => setScope('allTime')}
            className={`px-4 py-2 text-xs font-bold rounded-md transition-all flex items-center gap-1.5 ${
              scope === 'allTime'
                ? 'bg-white text-blue-700 shadow-sm'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <span>📜</span>
            <span>All-Time History (2012–2026)</span>
          </button>
          <button
            onClick={() => setScope('season')}
            className={`px-4 py-2 text-xs font-bold rounded-md transition-all flex items-center gap-1.5 ${
              scope === 'season'
                ? 'bg-white text-blue-700 shadow-sm'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <span>📈</span>
            <span>Single Season ({selectedSeason})</span>
          </button>
        </div>
      </div>

      {scope === 'allTime' ? (
        /* --- ALL-TIME LEAGUE HISTORY VIEW --- */
        <div className="space-y-6">
          {/* Controls Bar */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div>
                <label className="text-xs font-bold uppercase text-gray-500 block mb-1">
                  Progression Metric:
                </label>
                <select
                  value={allTimeMetric}
                  onChange={e => setAllTimeMetric(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-xs font-bold text-gray-800 bg-gray-50 focus:bg-white focus:ring-2 focus:ring-blue-500"
                >
                  {ALL_TIME_METRICS.map(m => (
                    <option key={m.id} value={m.id}>{m.label}</option>
                  ))}
                </select>
              </div>

              <div>
                <label className="text-xs font-bold uppercase text-gray-500 block mb-1">
                  Filter Owner:
                </label>
                <select
                  value={allTimeOwner}
                  onChange={e => setAllTimeOwner(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-xs font-bold text-gray-800 bg-gray-50 focus:bg-white focus:ring-2 focus:ring-blue-500"
                >
                  <option value="all">All Included Owners</option>
                  {availableAllTimeOwners.map(o => (
                    <option key={o} value={o}>{o}</option>
                  ))}
                </select>
              </div>

              <div className="flex items-end pb-1">
                <label className="flex items-center gap-2 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={activeOnly}
                    onChange={e => setActiveOnly(e.target.checked)}
                    className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                  />
                  <span className="text-xs font-bold text-gray-700">
                    Active 2026 Owners Only ({teamIds.length} franchises)
                  </span>
                </label>
              </div>
            </div>
          </div>

          {/* Chart Container */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-base font-black text-gray-900">
                {ALL_TIME_METRICS.find(m => m.id === allTimeMetric)?.label} Year-over-Year (2012–2026)
              </h3>
              {isRankMetric && (
                <span className="text-xs font-semibold text-blue-600 bg-blue-50 px-2.5 py-1 rounded-full border border-blue-200">
                  ↑ Higher is better (#1 at top of chart)
                </span>
              )}
            </div>

            <ResponsiveContainer width="100%" height={450}>
              <LineChart data={allTimeChartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f4f8" />
                <XAxis
                  dataKey="year"
                  tick={{ fontSize: 11, fill: '#64748b' }}
                />
                <YAxis
                  reversed={isRankMetric}
                  domain={isRankMetric ? [1, 10] : ['auto', 'auto']}
                  ticks={isRankMetric ? [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] : undefined}
                  tickFormatter={v => isRankMetric ? `#${v}` : v}
                  tick={{ fontSize: 11, fill: '#64748b' }}
                  width={45}
                />
                <Tooltip
                  formatter={(val, name) => [isRankMetric ? `#${val} Place` : `${val} pts`, name]}
                  labelFormatter={label => `Season: ${label}`}
                  contentStyle={{ fontSize: 12, borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <Legend wrapperStyle={{ fontSize: 12, paddingTop: '10px' }} />
                {(allTimeOwner === 'all' ? availableAllTimeOwners : [allTimeOwner]).map((owner, idx) => (
                  <Line
                    key={owner}
                    type="monotone"
                    dataKey={owner}
                    stroke={TEAM_COLORS[idx % TEAM_COLORS.length]}
                    strokeWidth={2.5}
                    dot={{ r: 4 }}
                    activeDot={{ r: 6 }}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Franchise Hall of Fame & All-Time Leaderboard */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <div className="bg-gray-50 px-6 py-4 border-b border-gray-200 flex items-center justify-between">
              <div>
                <h3 className="text-lg font-black text-gray-900 flex items-center gap-2">
                  <span>🏆</span>
                  <span>All-Time Franchise Leaderboard & Hall of Fame (2012–2026)</span>
                </h3>
                <p className="text-xs text-gray-500 mt-0.5">
                  Lifetime achievements, titles, podiums, and average finishes across all 15 seasons. Click any franchise to drill into their season finishes.
                </p>
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead className="bg-white border-b border-gray-200 text-gray-500 text-xs uppercase font-bold tracking-wider">
                  <tr>
                    <th className="px-6 py-3 text-left w-12">Rank</th>
                    <th className="px-6 py-3 text-left">Franchise & Owner</th>
                    <th className="px-4 py-3 text-center">Seasons</th>
                    <th className="px-4 py-3 text-center">Titles 🏆</th>
                    <th className="px-4 py-3 text-center">Top 3 🥈🥉</th>
                    <th className="px-4 py-3 text-center">Avg Finish</th>
                    <th className="px-4 py-3 text-center">Best / Worst</th>
                    <th className="px-4 py-3 text-center">Lifetime Pts</th>
                    <th className="px-4 py-3 text-center">Avg Pts/Yr</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {franchiseLeaderboard
                    .filter(f => !activeOnly || availableAllTimeOwners.includes(f.owner))
                    .map((item, index) => {
                      const isExpanded = expandedFranchise === item.owner;

                      return (
                        <tr key={item.owner} className="hover:bg-blue-50/50 transition-colors">
                          <td className="px-6 py-4 text-xs font-mono font-bold text-gray-400">
                            {index + 1}
                          </td>
                          <td className="px-6 py-4">
                            <div
                              onClick={() => setExpandedFranchise(isExpanded ? null : item.owner)}
                              className="cursor-pointer group flex items-center gap-2"
                            >
                              <span className="font-bold text-gray-900 group-hover:text-blue-700 group-hover:underline">
                                {item.owner}
                              </span>
                              {item.titles.length > 0 && (
                                <span className="text-xs" title={`${item.titles.length}x League Champion`}>
                                  {'🏆'.repeat(item.titles.length)}
                                </span>
                              )}
                              <span className="text-gray-400 text-xs ml-1">
                                {isExpanded ? '▲' : '▼'}
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-4 text-center font-mono text-gray-600 font-semibold">
                            {item.seasonsCount}
                          </td>
                          <td className="px-4 py-4 text-center">
                            {item.titles.length > 0 ? (
                              <div className="flex items-center justify-center gap-1 flex-wrap">
                                {item.titles.map(yr => (
                                  <span key={yr} className="bg-amber-100 text-amber-900 text-[10px] font-black px-1.5 py-0.5 rounded border border-amber-300">
                                    {yr}
                                  </span>
                                ))}
                              </div>
                            ) : (
                              <span className="text-gray-300">-</span>
                            )}
                          </td>
                          <td className="px-4 py-4 text-center font-mono font-bold text-gray-800">
                            {item.top3Count}
                          </td>
                          <td className="px-4 py-4 text-center font-mono font-bold text-blue-700">
                            #{item.avgRank}
                          </td>
                          <td className="px-4 py-4 text-center text-xs font-mono text-gray-500">
                            #{item.bestRank} / #{item.worstRank}
                          </td>
                          <td className="px-4 py-4 text-center font-mono font-bold text-gray-900">
                            {Math.round(item.totalPoints).toLocaleString()}
                          </td>
                          <td className="px-4 py-4 text-center font-mono font-bold text-emerald-700">
                            {item.avgPoints}
                          </td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ) : (
        /* --- SINGLE SEASON PROGRESSION VIEW (ORIGINAL) --- */
        <div className="space-y-4">
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div>
              <h2 className="text-2xl font-black text-gray-900">{selectedSeason} Cumulative Progression</h2>
              <p className="text-xs text-gray-400">Cumulative stats updated daily through the season</p>
            </div>

            <div className="flex items-center gap-3 flex-wrap">
              <button
                onClick={() => setSelectedStat(s => s === 'Matchup Points' ? 'HR' : 'Matchup Points')}
                className={`px-3 py-1.5 text-xs font-bold rounded-lg border transition-all ${
                  isMatchupPts 
                    ? 'bg-blue-600 text-white border-blue-600 shadow-sm' 
                    : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
                }`}
              >
                Matchup Points
              </button>

              <div className="flex items-center bg-gray-200 rounded-lg p-1">
                <button
                  onClick={() => { setViewMode('raw'); if (selectedStat === 'Total Roto') setSelectedStat('HR'); }}
                  disabled={isMatchupPts}
                  className={`px-3 py-1 text-xs font-bold rounded-md transition-all ${
                    isMatchupPts ? 'opacity-40 cursor-not-allowed' :
                    viewMode === 'raw' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'
                  }`}
                >
                  Raw Stats
                </button>
                <button
                  onClick={() => setViewMode('roto')}
                  disabled={isMatchupPts}
                  className={`px-3 py-1 text-xs font-bold rounded-md transition-all ${
                    isMatchupPts ? 'opacity-40 cursor-not-allowed' :
                    viewMode === 'roto' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'
                  }`}
                >
                  Roto Points
                </button>
              </div>
              
              <select
                value={selectedStat}
                onChange={e => setSelectedStat(e.target.value)}
                className="border border-gray-200 rounded-lg px-3 py-2 text-sm font-semibold bg-white text-gray-700 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
              >
                {currentOptions.map(s => (
                  <option key={s} value={s}>{SCORING_CATS[s]?.label || s}</option>
                ))}
              </select>
              <select
                value={selectedTeamId}
                onChange={e => setSelectedTeamId(e.target.value)}
                className="border border-gray-200 rounded-lg px-3 py-2 text-sm font-semibold bg-white text-gray-700 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
              >
                <option value="all">All Teams</option>
                {teamIds.map(id => (
                  <option key={id} value={id}>{TEAMS[id].name}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
            {isLowBetter && (
              <p className="text-xs text-gray-400 italic mb-4">
                Lower is better for {SCORING_CATS[selectedStat]?.label || selectedStat} — leaders appear at the bottom.
              </p>
            )}
            {isMatchupPts && (
              <p className="text-xs text-gray-400 italic mb-4">
                Cumulative points earned in H2H & Trio matchups per week (ends on current week).
              </p>
            )}
            {viewMode === 'roto' && (
              <p className="text-xs text-gray-400 italic mb-4">
                Rotisserie points progression relative to the league (1-9 pts per category). Higher is better.
              </p>
            )}
            <ResponsiveContainer width="100%" height={420}>
              <LineChart data={activeData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f4f8" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 10, fill: '#9ca3af', angle: -40, textAnchor: 'end' }}
                  tickFormatter={v => v?.slice(5) || ''}
                  interval={isMatchupPts ? 0 : 6}
                  height={45}
                />
                <YAxis
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  tickFormatter={v => formatVal(v, selectedStat, viewMode)}
                  width={55}
                  domain={viewMode === 'raw' && selectedStat === 'ERA' ? [2, 5] : ['auto', 'auto']}
                />
                <Tooltip
                  formatter={(val, name) => [formatVal(val, selectedStat, viewMode), name]}
                  labelFormatter={label => `Date: ${label}`}
                  contentStyle={{ fontSize: 12, borderRadius: '8px', border: '1px solid #e5e7eb' }}
                />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                {visibleTeamIds.map((teamId, idx) => (
                  <Line
                    key={teamId}
                    type="monotone"
                    dataKey={TEAMS[teamId]?.name}
                    stroke={TEAM_COLORS[idx % TEAM_COLORS.length]}
                    strokeWidth={2}
                    dot={isMatchupPts ? { r: 3 } : false}
                    activeDot={{ r: 4 }}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  );
}
