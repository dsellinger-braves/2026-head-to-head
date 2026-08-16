import { useMemo, useState } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, SCORING_CATS } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

const STAT_COLS = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP', 'IP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

const formatStat = (val, cat) => {
  if (val === undefined || val === null) return '-';
  if (cat === 'IP') {
    const ip = parseFloat(val) || 0;
    return `${Math.floor(ip)}.${Math.round((ip % 1) * 3)}`;
  }
  if (cat === 'ERA') {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(2);
  }
  if (cat === 'OBP') {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(4).replace(/^0/, '');
  }
  if (SCORING_CATS[cat]?.isRate) {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
  }
  const n = parseFloat(val);
  return isNaN(n) ? '-' : Math.round(n);
};

export default function TeamsView({ allStats, onOwnerClick }) {
  const [sortKey, setSortKey] = useState('R');
  const [sortDir, setSortDir] = useState('desc');

  const teamRows = useMemo(() => {
    const groups = {};
    Object.keys(TEAMS).forEach(id => {
      if (parseInt(id) === 99) return;
      groups[id] = [];
    });
    allStats.forEach(r => {
      if (groups[r.team_id] !== undefined) groups[r.team_id].push(r);
    });
    return Object.entries(groups).map(([id, records]) => ({
      ...TEAMS[id],
      stats: aggregateStats(records),
    }));
  }, [allStats]);

  const sorted = useMemo(() => {
    return [...teamRows].sort((a, b) => {
      const va = parseFloat(a.stats[sortKey]) || 0;
      const vb = parseFloat(b.stats[sortKey]) || 0;
      const isLow = SCORING_CATS[sortKey]?.type === 'low';
      const cmp = isLow ? va - vb : vb - va;
      return sortDir === 'desc' ? cmp : -cmp;
    });
  }, [teamRows, sortKey, sortDir]);

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
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-black text-gray-900">Team Stats — 2026 Season</h2>
        <p className="text-xs text-gray-400">Active roster only · click column header to sort · click row to drill in</p>
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
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {sorted.map(team => (
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
                    className={`px-3 py-3 text-center font-mono text-sm
                      ${sortKey === col ? 'text-blue-700 font-bold bg-blue-50/50' : 'text-gray-700'}`}
                  >
                    {formatStat(team.stats[col], col)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
