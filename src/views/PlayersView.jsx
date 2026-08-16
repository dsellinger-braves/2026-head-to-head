import { useMemo, useState } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, SCORING_CATS } from '../utils/scoring';

const BAT_COLS = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP'];
const PITCH_COLS = ['IP', 'K', 'QS', 'QS_PCT', 'SV+HDs', 'ERA', 'WHIP'];

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
    return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
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

export default function PlayersView({ allStats, selectedSeason, onPlayerClick }) {
  const [tab, setTab] = useState('batters');
  const [sortKey, setSortKey] = useState('PA');
  const [sortDir, setSortDir] = useState('desc');

  const { batters, pitchers } = useMemo(() => {
    const map = {};
    allStats.forEach(r => {
      const mapKey = r.player_id || r.full_name;
      if (!map[mapKey]) {
        map[mapKey] = { id: r.player_id, name: r.full_name, teamIds: new Set(), records: [], latestPeriod: 0 };
      }
      const p = map[mapKey];
      p.records.push(r);
      p.teamIds.add(r.team_id);
      if (r.scoring_period_id > p.latestPeriod) {
        p.latestPeriod = r.scoring_period_id;
        p.name = r.full_name;
      }
    });

    const all = Object.values(map).map(p => ({
      ...p,
      stats: aggregateStats(p.records),
      owners: [...p.teamIds].map(tid => TEAMS[tid]?.name).filter(Boolean).join(', '),
    }));

    const PITCHER_SLOTS = new Set([13, 14, 15]);
    return {
      batters:  all.filter(p => !p.records.some(r => PITCHER_SLOTS.has(r.lineup_slot_id))),
      pitchers: all.filter(p =>  p.records.some(r => PITCHER_SLOTS.has(r.lineup_slot_id))),
    };
  }, [allStats]);

  const displayCols = tab === 'batters' ? BAT_COLS : PITCH_COLS;

  const sorted = useMemo(() => {
    const list = tab === 'batters' ? batters : pitchers;
    return [...list].sort((a, b) => {
      const va = parseFloat(a.stats[sortKey]) || 0;
      const vb = parseFloat(b.stats[sortKey]) || 0;
      const isLow = SCORING_CATS[sortKey]?.type === 'low';
      const cmp = isLow ? va - vb : vb - va;
      return sortDir === 'desc' ? cmp : -cmp;
    });
  }, [tab, batters, pitchers, sortKey, sortDir]);

  const handleSort = (col) => {
    if (sortKey === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    else { setSortKey(col); setSortDir('desc'); }
  };

  const handleTabChange = (newTab) => {
    setTab(newTab);
    setSortKey(newTab === 'batters' ? 'PA' : 'IP');
    setSortDir('desc');
  };

  const SortIcon = ({ col }) => (
    <span className={`ml-1 ${sortKey === col ? 'text-blue-500' : 'text-gray-300'}`}>
      {sortKey === col ? (sortDir === 'desc' ? '↓' : '↑') : '↕'}
    </span>
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h2 className="text-2xl font-black text-gray-900">Players — {selectedSeason} Season</h2>
        <div className="flex bg-gray-100 p-1 rounded-lg">
          <button
            onClick={() => handleTabChange('batters')}
            className={`px-4 py-2 text-sm font-bold rounded-md transition-all ${tab === 'batters' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Batters ({batters.length})
          </button>
          <button
            onClick={() => handleTabChange('pitchers')}
            className={`px-4 py-2 text-sm font-bold rounded-md transition-all ${tab === 'pitchers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Pitchers ({pitchers.length})
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-3 py-3 text-left text-xs font-bold text-gray-400 uppercase w-8">#</th>
              <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">Player</th>
              <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">Owned By</th>
              {displayCols.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className={`px-3 py-3 text-center text-xs font-bold uppercase tracking-wider cursor-pointer select-none hover:bg-blue-50 transition-colors whitespace-nowrap
                    ${sortKey === col ? 'text-blue-600 bg-blue-50' : 'text-gray-500'}`}
                >
                  {getLabel(col)}
                  <SortIcon col={col} />
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {sorted.map((player, i) => (
              <tr
                key={player.id || player.name}
                onClick={() => onPlayerClick(player.id, player.name)}
                className="hover:bg-blue-50 cursor-pointer transition-colors group"
              >
                <td className="px-3 py-3 text-gray-300 font-mono text-xs text-right">{i + 1}</td>
                <td className="px-4 py-3 font-bold text-gray-900 group-hover:text-blue-700 group-hover:underline">
                  {player.name}
                </td>
                <td className="px-4 py-3 text-gray-400 text-xs">{player.owners || '—'}</td>
                {displayCols.map(col => (
                  <td
                    key={col}
                    className={`px-3 py-3 text-center font-mono text-sm
                      ${sortKey === col ? 'text-blue-700 font-bold bg-blue-50/50' : 'text-gray-700'}`}
                  >
                    {formatStat(player.stats[col], col)}
                  </td>
                ))}
              </tr>
            ))}
            {sorted.length === 0 && (
              <tr>
                <td colSpan={3 + displayCols.length} className="p-10 text-center text-gray-400 italic">
                  No players found.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
