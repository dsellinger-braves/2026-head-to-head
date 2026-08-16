import { useState, useMemo } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, calculateBatterValue, calculatePitcherValue } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

export default function OwnerDisparitiesView({ allStats, selectedSeason, onPlayerClick, onOwnerClick }) {
  const [tab, setTab] = useState('batters');
  const [minVolume, setMinVolume] = useState(15); // 15 PA for batters, 5 IP for pitchers

  const handleTabChange = (newTab) => {
    setTab(newTab);
    setMinVolume(newTab === 'batters' ? 15 : 5);
  };

  const disparities = useMemo(() => {
    // 1. Group records by player, then by team
    const playerMap = {};

    allStats.forEach(r => {
      // Ignore Ghost Team (99)
      if (r.team_id == 99) return;
      
      const mapKey = r.player_id || r.full_name;
      if (!playerMap[mapKey]) {
        playerMap[mapKey] = {
          id: r.player_id,
          name: r.full_name,
          teamRecords: {},
          isPitcher: false,
        };
      }

      const p = playerMap[mapKey];
      if (!p.teamRecords[r.team_id]) p.teamRecords[r.team_id] = [];
      p.teamRecords[r.team_id].push(r);

      const PITCHER_SLOTS = new Set([13, 14, 15]);
      if (PITCHER_SLOTS.has(r.lineup_slot_id)) {
        p.isPitcher = true;
      }
    });

    const isBattersTab = tab === 'batters';
    const results = [];

    // 2. Calculate values per team and find disparities
    Object.values(playerMap).forEach(p => {
      // Filter out players who don't match the current tab
      if (isBattersTab && p.isPitcher) return;
      if (!isBattersTab && !p.isPitcher) return;

      const teamStints = [];
      Object.keys(p.teamRecords).forEach(teamId => {
        const records = p.teamRecords[teamId];
        const stats = aggregateStats(records);
        
        const volume = isBattersTab ? parseFloat(stats.PA) || 0 : parseFloat(stats.IP) || 0;
        
        if (volume >= minVolume) {
          const value = isBattersTab ? calculateBatterValue(stats) : calculatePitcherValue(stats);
          teamStints.push({
            teamId: parseInt(teamId),
            team: TEAMS[teamId] || { name: 'Unknown', owner: '' },
            volume,
            value,
            stats
          });
        }
      });

      // We need at least 2 qualified stints to have a disparity
      if (teamStints.length >= 2) {
        teamStints.sort((a, b) => b.value - a.value);
        const bestStint = teamStints[0];
        const worstStint = teamStints[teamStints.length - 1];
        
        results.push({
          id: p.id,
          name: p.name,
          bestStint,
          worstStint,
          disparity: bestStint.value - worstStint.value
        });
      }
    });

    // 3. Sort by disparity descending
    return results.sort((a, b) => b.disparity - a.disparity);

  }, [allStats, tab, minVolume]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h2 className="text-2xl font-black text-gray-900">Owner Disparities — {selectedSeason}</h2>
        <div className="flex bg-gray-100 p-1 rounded-lg">
          <button
            onClick={() => handleTabChange('batters')}
            className={`px-4 py-2 text-sm font-bold rounded-md transition-all ${tab === 'batters' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Batters
          </button>
          <button
            onClick={() => handleTabChange('pitchers')}
            className={`px-4 py-2 text-sm font-bold rounded-md transition-all ${tab === 'pitchers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Pitchers
          </button>
        </div>
      </div>

      <div className="flex flex-col sm:flex-row gap-4 items-center bg-gray-50 px-4 py-3 rounded-lg border border-gray-200">
        <span className="text-sm font-bold text-gray-500 uppercase tracking-wider flex items-center gap-2">
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"></path></svg>
          Filters
        </span>
        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-700 font-semibold">
            Min {tab === 'batters' ? 'Plate Apps (PA)' : 'Innings (IP)'} per Owner:
          </label>
          <input 
            type="number" 
            min="1" 
            value={minVolume} 
            onChange={e => setMinVolume(Number(e.target.value))} 
            className="w-20 border border-gray-300 rounded-md px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400" 
          />
        </div>
      </div>

      {disparities.length === 0 ? (
        <div className="text-center py-12 bg-white rounded-xl shadow-sm border border-gray-200 text-gray-400">
          No players found with multiple qualified stints.
        </div>
      ) : (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-gray-100 border-b border-gray-200 text-xs uppercase tracking-wider text-gray-500">
                <th className="p-4 font-bold">Player</th>
                <th className="p-4 font-bold text-center border-l border-gray-200">Best Stint</th>
                <th className="p-4 font-bold text-center border-l border-gray-200">Worst Stint</th>
                <th className="p-4 font-bold text-center border-l border-gray-200 bg-blue-50 text-blue-800">Disparity</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {disparities.map((row, i) => (
                <tr key={i} className="hover:bg-gray-50 transition-colors">
                  {/* Player */}
                  <td className="p-4 align-middle">
                    <button 
                      onClick={() => onPlayerClick(row.id, row.name)}
                      className="font-bold text-blue-600 hover:text-blue-800 transition-colors text-left"
                    >
                      {row.name}
                    </button>
                  </td>

                  {/* Best Stint */}
                  <td className="p-4 align-middle border-l border-gray-100">
                    <div className="flex items-center justify-between gap-4">
                      <div 
                        onClick={() => onOwnerClick(row.bestStint.team)}
                        className="flex items-center gap-2 cursor-pointer hover:opacity-80"
                      >
                        <TeamAvatar team={row.bestStint.team} size="sm" />
                        <div>
                          <div className="font-bold text-sm text-gray-900 leading-none">{row.bestStint.team.name}</div>
                          <div className="text-[10px] text-gray-500">{row.bestStint.team.owner}</div>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="font-mono font-black text-green-600">{row.bestStint.value.toFixed(2)}</div>
                        <div className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">{row.bestStint.volume.toFixed(1)} {tab === 'batters' ? 'PA' : 'IP'}</div>
                      </div>
                    </div>
                  </td>

                  {/* Worst Stint */}
                  <td className="p-4 align-middle border-l border-gray-100">
                    <div className="flex items-center justify-between gap-4">
                      <div 
                        onClick={() => onOwnerClick(row.worstStint.team)}
                        className="flex items-center gap-2 cursor-pointer hover:opacity-80"
                      >
                        <TeamAvatar team={row.worstStint.team} size="sm" />
                        <div>
                          <div className="font-bold text-sm text-gray-900 leading-none">{row.worstStint.team.name}</div>
                          <div className="text-[10px] text-gray-500">{row.worstStint.team.owner}</div>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="font-mono font-black text-red-600">{row.worstStint.value.toFixed(2)}</div>
                        <div className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">{row.worstStint.volume.toFixed(1)} {tab === 'batters' ? 'PA' : 'IP'}</div>
                      </div>
                    </div>
                  </td>

                  {/* Disparity */}
                  <td className="p-4 align-middle text-center border-l border-gray-100 bg-blue-50/30">
                    <div className="font-mono text-xl font-black text-gray-800">
                      +{row.disparity.toFixed(2)}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
