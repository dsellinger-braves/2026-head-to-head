import { useMemo, useState } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, SCORING_CATS, calculateRotoPoints } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

const STAT_COLS = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP', 'IP', 'K', 'QS', 'QS_PCT', 'SV+HDs', 'ERA', 'WHIP'];

const formatStat = (row, cat) => {
  const val = row[cat];
  if (val === undefined || val === null) return '-';
  if (cat === 'IP') {
    const ip = parseFloat(val) || 0;
    return `${Math.floor(ip)}.${Math.round((ip % 1) * 3)}`;
  }
  if (cat === 'ERA' || cat === 'WHIP') {
    const raw = row[`${cat}_raw`];
    if (raw !== undefined) return isNaN(raw) ? '-' : raw.toFixed(4);
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(2);
  }
  if (cat === 'OBP') {
    const raw = row[`${cat}_raw`];
    if (raw !== undefined) return isNaN(raw) ? '-' : raw.toFixed(4).replace(/^0/, '');
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(4).replace(/^0/, '');
  }
  if (cat === 'QS_PCT') {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(1) + '%';
  }
  if (SCORING_CATS[cat]?.isRate) {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
  }
  const n = parseFloat(val);
  return isNaN(n) ? '-' : Math.round(n);
};

const getLabel = (col) => {
  if (col === 'QS_PCT') return 'QS%';
  return SCORING_CATS[col]?.label || col;
};

export default function TeamsView({ allStats, onOwnerClick }) {
  const [sortKey, setSortKey] = useState('R');
  const [sortDir, setSortDir] = useState('desc');
  const [viewMode, setViewMode] = useState('raw'); // 'raw' or 'roto'

  const teamRows = useMemo(() => {
    const groups = {};
    Object.keys(TEAMS).forEach(id => {
      if (parseInt(id) === 99) return;
      groups[id] = [];
    });
    allStats.forEach(r => {
      if (groups[r.team_id] !== undefined) groups[r.team_id].push(r);
    });
    
    const teamStatsMap = {};
    Object.entries(groups).forEach(([id, records]) => {
      teamStatsMap[id] = aggregateStats(records);
    });
    
    const rotoPointsMap = calculateRotoPoints(teamStatsMap);

    return Object.entries(teamStatsMap).map(([id, stats]) => ({
      ...TEAMS[id],
      stats,
      rotoPoints: rotoPointsMap[id]
    }));
  }, [allStats]);

  const sorted = useMemo(() => {
    return [...teamRows].sort((a, b) => {
      const dataA = viewMode === 'roto' && SCORING_CATS[sortKey] ? a.rotoPoints : a.stats;
      const dataB = viewMode === 'roto' && SCORING_CATS[sortKey] ? b.rotoPoints : b.stats;
      const va = parseFloat(dataA[sortKey]) || 0;
      const vb = parseFloat(dataB[sortKey]) || 0;
      
      const isLow = viewMode === 'raw' && SCORING_CATS[sortKey]?.type === 'low';
      const cmp = isLow ? va - vb : vb - va;
      return sortDir === 'desc' ? cmp : -cmp;
    });
  }, [teamRows, sortKey, sortDir, viewMode]);

  const handleSort = (col) => {
    if (sortKey === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    else { setSortKey(col); setSortDir('desc'); }
  };

  const SortIcon = ({ col }) => (
    <span className={`ml-1 ${sortKey === col ? 'text-blue-500' : 'text-gray-300'}`}>
      {sortKey === col ? (sortDir === 'desc' ? '↓' : '↑') : '↕'}
    </span>
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-black text-gray-900">Team Stats — 2026 Season</h2>
          <p className="text-xs text-gray-400">Active roster only · click column header to sort · click row to drill in</p>
        </div>
        <div className="flex items-center bg-gray-200 rounded-lg p-1">
          <button
            onClick={() => setViewMode('raw')}
            className={`px-4 py-1.5 text-sm font-bold rounded-md transition-all ${viewMode === 'raw' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Raw Stats
          </button>
          <button
            onClick={() => setViewMode('roto')}
            className={`px-4 py-1.5 text-sm font-bold rounded-md transition-all ${viewMode === 'roto' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Roto Points
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">Team</th>
              {STAT_COLS.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className={`px-3 py-3 text-center text-xs font-bold uppercase tracking-wider cursor-pointer select-none hover:bg-blue-50 transition-colors whitespace-nowrap
                    ${sortKey === col ? 'text-blue-600 bg-blue-50' : 'text-gray-500'}`}
                >
                  {SCORING_CATS[col]?.label || col}
                  <SortIcon col={col} />
                </th>
              ))}
              {viewMode === 'roto' && (
                <th 
                  onClick={() => handleSort('total')}
                  className={`px-3 py-3 text-center text-xs font-bold uppercase tracking-wider cursor-pointer select-none hover:bg-blue-50 transition-colors whitespace-nowrap
                    ${sortKey === 'total' ? 'text-blue-600 bg-blue-50' : 'text-gray-500'}`}
                >
                  Total Roto
                  <SortIcon col="total" />
                </th>
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {sorted.map(team => {
              const rowData = viewMode === 'roto' ? team.rotoPoints : team.stats;
              return (
                <tr
                  key={team.id}
                  onClick={() => onOwnerClick(team)}
                  className="hover:bg-blue-50 cursor-pointer transition-colors group"
                >
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      <div className="group-hover:scale-110 transition-transform">
                        <TeamAvatar team={team} size="sm" />
                      </div>
                      <span className="font-bold text-gray-900 group-hover:text-blue-700">{team.name}</span>
                    </div>
                  </td>
                  {STAT_COLS.map(col => (
                    <td
                      key={col}
                      title={rowData[`${col}_raw`] !== undefined ? rowData[`${col}_raw`] : ''}
                      className={`px-3 py-3 text-center font-mono text-sm
                        ${sortKey === col ? 'text-blue-700 font-bold bg-blue-50/50' : 'text-gray-700'}`}
                    >
                      {viewMode === 'roto' ? (rowData[col] === undefined ? '-' : (rowData[col] % 1 === 0 ? rowData[col] : rowData[col].toFixed(1))) : formatStat(rowData, col)}
                    </td>
                  ))}
                  {viewMode === 'roto' && (
                    <td className={`px-3 py-3 text-center font-mono text-sm font-black ${sortKey === 'total' ? 'text-blue-700 bg-blue-50/50' : 'text-blue-900'}`}>
                      {rowData.total % 1 === 0 ? rowData.total : rowData.total.toFixed(1)}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
