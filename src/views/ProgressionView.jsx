import { useMemo, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, SCORING_CATS, calculateRotoPoints } from '../utils/scoring';

const STAT_OPTIONS = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

const TEAM_COLORS = [
  '#3b82f6', '#ef4444', '#10b981', '#f59e0b',
  '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316'
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
  const [selectedStat, setSelectedStat] = useState('HR');
  const [selectedTeamId, setSelectedTeamId] = useState('all');
  const [viewMode, setViewMode] = useState('raw'); // 'raw' or 'roto'

  const teamIds = useMemo(
    () => Object.keys(TEAMS).filter(id => parseInt(id) !== 99),
    []
  );

  // Precompute cumulative stats per team per scoring period
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
  }, [allStats, teamIds]);

  const visibleTeamIds = selectedTeamId === 'all' ? teamIds : [String(selectedTeamId)];

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

  // Cumulative roto points per week from processedWeeks
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
        } else if (m.type === 'h2h') {
          const hId = String(m.homeTeam?.id);
          const aId = String(m.awayTeam?.id);
          if (cumPts[hId] !== undefined) {
            cumPts[hId] += (m.result?.homeScore ?? 0) + (m.result?.ties ?? 0) * 0.5;
          }
          if (cumPts[aId] !== undefined) {
            cumPts[aId] += (m.result?.awayScore ?? 0) + (m.result?.ties ?? 0) * 0.5;
          }
        }
      });

      const point = { date: week.endDate };
      visibleTeamIds.forEach(teamId => {
        const v = cumPts[teamId] ?? 0;
        point[TEAMS[teamId]?.name] = v % 1 === 0 ? v : parseFloat(v.toFixed(1));
      });
      return point;
    });
  }, [processedWeeks, visibleTeamIds, teamIds, selectedSeason]);

  const isMatchupPts = selectedStat === 'Matchup Points';
  const activeData = isMatchupPts ? matchupPtsChartData : chartData;
  const isLowBetter = viewMode === 'raw' && !isMatchupPts && SCORING_CATS[selectedStat]?.type === 'low';

  const currentOptions = viewMode === 'raw' 
    ? [...STAT_OPTIONS, 'Matchup Points'] 
    : [...STAT_OPTIONS, 'Total Roto'];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h2 className="text-2xl font-black text-gray-900">Stat Progression — {selectedSeason} Season</h2>
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center bg-gray-200 rounded-lg p-1 mr-2">
            <button
              onClick={() => { setViewMode('raw'); setSelectedStat('HR'); }}
              className={`px-3 py-1.5 text-sm font-bold rounded-md transition-all ${viewMode === 'raw' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
            >
              Raw Stats
            </button>
            <button
              onClick={() => { setViewMode('roto'); setSelectedStat('Total Roto'); }}
              className={`px-3 py-1.5 text-sm font-bold rounded-md transition-all ${viewMode === 'roto' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
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
  );
}
