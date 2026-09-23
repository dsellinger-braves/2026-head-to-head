import { useMemo, useState } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, SCORING_CATS, MINUTIAE_STATS, getStatMeta, calculateRotoPoints } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';
import RotoGapView from './RotoGapView';
import BenchStatsView from './BenchStatsView';
import PlayerImpactSimulatorView from './PlayerImpactSimulatorView';
import OptimalLineupSimulatorView from './OptimalLineupSimulatorView';

const STAT_COLS = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP', 'IP', 'K', 'QS', 'QS_PCT', 'SV+HDs', 'ERA', 'WHIP'];

const ROTO_COLS = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

const MINUTIAE_COLS = [
  // Hitting
  'AB', 'H', '2B', '3B', 'BB', 'SO', 'HBP', 'CS', 'SB_PCT', 'E', 'AVG', 'SLG', 'OPS',
  // Pitching
  'W', 'L', 'SV', 'HD', 'BS', 'R_Allowed', 'ER', 'UER', 'UER/9', 'UER_PCT', 'HR_Allowed', 'K/9', 'BB/9', 'K/BB'
];

const formatStat = (row, cat) => {
  const val = row[cat];
  if (val === undefined || val === null) return '-';
  if (cat === 'IP') {
    const ip = parseFloat(val) || 0;
    return `${Math.floor(ip)}.${Math.round((ip % 1) * 3)}`;
  }
  if (cat === 'ERA' || cat === 'WHIP') {
    const raw = parseFloat(row[`${cat}_raw`]);
    if (!isNaN(raw)) return raw.toFixed(4);
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(2);
  }
  if (cat === 'OBP') {
    const raw = parseFloat(row[`${cat}_raw`]);
    if (!isNaN(raw)) return raw.toFixed(4).replace(/^0/, '');
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(4).replace(/^0/, '');
  }
  if (cat === 'AVG' || cat === 'SLG' || cat === 'OPS') {
    const raw = parseFloat(row[`${cat}_raw`]);
    if (!isNaN(raw)) return raw.toFixed(3).replace(/^0/, '');
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
  }
  if (cat === 'QS_PCT' || cat === 'SB_PCT' || cat === 'UER_PCT') {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(1) + '%';
  }
  if (cat === 'K/9' || cat === 'BB/9' || cat === 'K/BB' || cat === 'UER/9') {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(2);
  }
  if (SCORING_CATS[cat]?.isRate || MINUTIAE_STATS[cat]?.isRate) {
    const n = parseFloat(val);
    return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
  }
  const n = parseFloat(val);
  return isNaN(n) ? '-' : Math.round(n);
};

const formatCellTooltip = (rowData, col, viewMode) => {
  const meta = getStatMeta(col);
  const fullName = meta.name || meta.label || col;

  if (viewMode === 'roto') {
    const pts = rowData[col];
    return `${fullName}: ${pts !== undefined ? (pts % 1 === 0 ? pts : pts.toFixed(1)) : '-'} Roto Pts`;
  }

  if (col === 'UER') {
    const uer = rowData.UER ?? 0;
    const uer9 = rowData['UER/9'] ?? '0.00';
    const uerPct = rowData.UER_PCT ?? '0.0';
    const er = rowData.ER ?? 0;
    return `${fullName}: ${uer} (${uer9} UER/9 IP · ${uerPct}% of ${er} Earned Runs)`;
  }

  if (col === 'UER/9') {
    const uer = rowData.UER ?? 0;
    const uer9 = rowData['UER/9'] ?? '0.00';
    const ip = formatStat(rowData, 'IP');
    return `${fullName}: ${uer9} (${uer} Unearned Runs across ${ip} IP)`;
  }

  if (col === 'UER_PCT') {
    const uer = rowData.UER ?? 0;
    const er = rowData.ER ?? 0;
    const uerPct = rowData.UER_PCT ?? '0.0';
    return `${fullName}: ${uerPct}% (${uer} Unearned Runs vs ${er} Earned Runs)`;
  }

  if (col === 'SB_PCT') {
    const sb = rowData.SB ?? 0;
    const cs = rowData.CS ?? 0;
    const pct = rowData.SB_PCT ?? '0.0';
    return `${fullName}: ${pct}% (${sb} SB / ${sb + cs} Attempts)`;
  }

  if (col === 'QS_PCT') {
    const qs = rowData.QS ?? 0;
    const gs = rowData.GS ?? 0;
    const pct = rowData.QS_PCT ?? '0.0';
    return `${fullName}: ${pct}% (${qs} QS / ${gs} Starts)`;
  }

  const formatted = formatStat(rowData, col);
  const raw = rowData[`${col}_raw`];
  if (raw !== undefined && !isNaN(raw) && String(raw) !== formatted) {
    const rawStr = typeof raw === 'number' ? (raw % 1 === 0 ? raw : raw.toFixed(4)) : raw;
    return `${fullName}: ${formatted} (exact: ${rawStr})`;
  }

  return `${fullName}: ${formatted}`;
};

function SortIcon({ col, sortKey, sortDir }) {
  return (
    <span className={`ml-1 ${sortKey === col ? 'text-blue-500' : 'text-gray-300'}`}>
      {sortKey === col ? (sortDir === 'desc' ? '↓' : '↑') : '↕'}
    </span>
  );
}

export default function TeamsView({ allStats, onOwnerClick, onPlayerClick, selectedSeason = 2026, initialViewMode = 'raw', initialTeamId = 5 }) {
  const [sortKey, setSortKey] = useState(initialViewMode === 'roto' ? 'total' : 'R');
  const [sortDir, setSortDir] = useState('desc');
  const [viewMode, setViewMode] = useState(initialViewMode); // 'raw', 'roto', 'minutiae', 'bench', 'gap', 'simulator', 'best-lineup'
  const [prevInitial, setPrevInitial] = useState(initialViewMode);
  if (initialViewMode !== prevInitial) {
    setPrevInitial(initialViewMode);
    setViewMode(initialViewMode);
    if (initialViewMode === 'roto') {
      setSortKey('total');
      setSortDir('desc');
    } else if (initialViewMode === 'raw') {
      setSortKey('R');
      setSortDir('desc');
    }
  }

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
      rotoPoints: rotoPointsMap[id] || { total: 0 }
    }));
  }, [allStats]);

  const activeCols = viewMode === 'minutiae' ? MINUTIAE_COLS : (viewMode === 'roto' ? ROTO_COLS : STAT_COLS);

  const sorted = useMemo(() => {
    return [...teamRows].sort((a, b) => {
      let va = 0;
      let vb = 0;

      if (sortKey === 'total' || sortKey === 'roto' || sortKey === 'totalRoto' || sortKey === 'rotoPoints') {
        va = parseFloat(a.rotoPoints?.total) || 0;
        vb = parseFloat(b.rotoPoints?.total) || 0;
      } else if (viewMode === 'roto') {
        va = parseFloat(a.rotoPoints?.[sortKey]) || 0;
        vb = parseFloat(b.rotoPoints?.[sortKey]) || 0;
      } else {
        const valA = a.stats?.[`${sortKey}_raw`] !== undefined ? a.stats[`${sortKey}_raw`] : a.stats?.[sortKey];
        const valB = b.stats?.[`${sortKey}_raw`] !== undefined ? b.stats[`${sortKey}_raw`] : b.stats?.[sortKey];
        va = parseFloat(valA) || 0;
        vb = parseFloat(valB) || 0;
      }

      if (Math.abs(va - vb) < 0.00001) {
        return (a.name || '').localeCompare(b.name || '');
      }

      return sortDir === 'desc' ? vb - va : va - vb;
    });
  }, [teamRows, sortKey, sortDir, viewMode]);

  const handleSort = (col) => {
    if (sortKey === col) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setSortKey(col);
      const isLow = (viewMode !== 'roto' && col !== 'total' && col !== 'roto' && col !== 'totalRoto' && col !== 'rotoPoints') && getStatMeta(col)?.type === 'low';
      setSortDir(isLow ? 'asc' : 'desc');
    }
  };

  const handleViewModeChange = (mode) => {
    setViewMode(mode);
    if (mode === 'minutiae') {
      setSortKey('H');
      setSortDir('desc');
    } else if (mode === 'roto') {
      setSortKey('total');
      setSortDir('desc');
    } else if (mode === 'raw') {
      setSortKey('R');
      setSortDir('desc');
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-black text-gray-900">Team Stats — {selectedSeason} Season</h2>
          <p className="text-xs text-gray-400">Active roster only · click column header to sort · click row to drill in</p>
        </div>
        <div className="flex items-center bg-gray-200 rounded-lg p-1 gap-1 flex-wrap">
          <button
            onClick={() => handleViewModeChange('raw')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'raw' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            Raw Stats
          </button>
          <button
            onClick={() => handleViewModeChange('roto')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'roto' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            Roto Points
          </button>
          <button
            onClick={() => handleViewModeChange('minutiae')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'minutiae' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            Deep Stats & Minutiae
          </button>
          <button
            onClick={() => handleViewModeChange('bench')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'bench' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            🛋️ Bench Stats
          </button>
          <button
            onClick={() => handleViewModeChange('gap')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'gap' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            🎯 Roto Gap & Pace
          </button>
          <button
            onClick={() => handleViewModeChange('simulator')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'simulator' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            🧪 What-If Simulator
          </button>
          <button
            onClick={() => handleViewModeChange('best-lineup')}
            className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${viewMode === 'best-lineup' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'}`}
          >
            ✨ Best Lineup
          </button>
        </div>
      </div>

      {viewMode === 'bench' ? (
        <BenchStatsView
          allStats={allStats}
          selectedSeason={selectedSeason}
          onOwnerClick={onOwnerClick}
          onPlayerClick={onPlayerClick}
        />
      ) : viewMode === 'gap' ? (
        <RotoGapView allStats={allStats} selectedSeason={selectedSeason} onOwnerClick={onOwnerClick} />
      ) : viewMode === 'simulator' ? (
        <PlayerImpactSimulatorView
          allStats={allStats}
          selectedSeason={selectedSeason}
          initialTeamId={initialTeamId}
          onOwnerClick={onOwnerClick}
          onPlayerClick={onPlayerClick}
        />
      ) : viewMode === 'best-lineup' ? (
        <OptimalLineupSimulatorView
          allStats={allStats}
          selectedSeason={selectedSeason}
          onOwnerClick={onOwnerClick}
          onPlayerClick={onPlayerClick}
        />
      ) : (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">Team</th>
                {activeCols.map(col => {
                  const meta = getStatMeta(col);
                  const label = meta?.label || col;
                  const fullName = meta?.name || label;
                  const direction = meta?.type === 'low' ? 'Lower is better' : 'Higher is better';
                  return (
                    <th
                      key={col}
                      onClick={() => handleSort(col)}
                      className={`px-3 py-3 text-center text-xs font-bold uppercase tracking-wider cursor-pointer select-none hover:bg-blue-50 transition-colors whitespace-nowrap
                        ${sortKey === col ? 'text-blue-600 bg-blue-50' : 'text-gray-500'}`}
                      title={`${fullName} (${direction})`}
                    >
                      {label}
                      <SortIcon col={col} sortKey={sortKey} sortDir={sortDir} />
                    </th>
                  );
                })}
                {(viewMode === 'roto' || viewMode === 'raw') && (
                  <th 
                    onClick={() => handleSort('total')}
                    className={`px-3 py-3 text-center text-xs font-bold uppercase tracking-wider cursor-pointer select-none hover:bg-blue-50 transition-colors whitespace-nowrap
                      ${sortKey === 'total' || sortKey === 'roto' ? 'text-blue-600 bg-blue-50' : 'text-gray-500'}`}
                    title="Total Rotisserie Points (Higher is better)"
                  >
                    Total Roto
                    <SortIcon col={sortKey === 'roto' ? 'roto' : 'total'} sortKey={sortKey} sortDir={sortDir} />
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
                    <td className="px-4 py-3 sticky left-0 bg-white group-hover:bg-blue-50 z-10 border-r border-gray-100">
                      <div className="flex items-center gap-3">
                        <div className="group-hover:scale-110 transition-transform">
                          <TeamAvatar team={team} size="sm" />
                        </div>
                        <span className="font-bold text-gray-900 group-hover:text-blue-700 whitespace-nowrap">{team.name}</span>
                      </div>
                    </td>
                    {activeCols.map(col => (
                      <td
                        key={col}
                        title={formatCellTooltip(rowData, col, viewMode)}
                        className={`px-3 py-3 text-center font-mono text-sm whitespace-nowrap
                          ${sortKey === col ? 'text-blue-700 font-bold bg-blue-50/50' : 'text-gray-700'}`}
                      >
                        {viewMode === 'roto' ? (rowData[col] === undefined ? '-' : (rowData[col] % 1 === 0 ? rowData[col] : rowData[col].toFixed(1))) : formatStat(rowData, col)}
                      </td>
                    ))}
                    {(viewMode === 'roto' || viewMode === 'raw') && (
                      <td 
                        title={`Total Rotisserie Points: ${team.rotoPoints?.total !== undefined ? (team.rotoPoints.total % 1 === 0 ? team.rotoPoints.total : team.rotoPoints.total.toFixed(1)) : '-'}`}
                        className={`px-3 py-3 text-center font-mono text-sm font-black whitespace-nowrap ${sortKey === 'total' || sortKey === 'roto' ? 'text-blue-700 bg-blue-50/50' : 'text-blue-900'}`}
                      >
                        {team.rotoPoints?.total !== undefined ? (team.rotoPoints.total % 1 === 0 ? team.rotoPoints.total : team.rotoPoints.total.toFixed(1)) : '-'}
                      </td>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
