import { useState, useMemo } from 'react';
import { SCORING_CATS, aggregateStats, LINEUP_SLOTS } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

export default function BoxScoreModal({ matchup, onClose, onPlayerClick }) {
  const [activeTab, setActiveTab] = useState('home'); // 'home' or 'away'

  // Categories to display
  const batCats = ['R', 'HR', 'RBI', 'SB', 'OBP'];
  const pitchCats = ['K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

  // Helper: Format raw stats for display
  const formatStat = (val, catKey) => {
    if (val === undefined || val === null) return '-';
    // Remove leading zeros for rates (.300 instead of 0.300)
    if (SCORING_CATS[catKey]?.isRate) {
      const num = parseFloat(val);
      return isNaN(num) ? '-' : num.toFixed(3).replace(/^0+/, '');
    }
    return val;
  };

  // Helper: Process records into structured lineup
  const getTeamData = (records) => {
    // 1. Group records by Player
    const playerMap = {};
    records.forEach(r => {
      // Skip IL (17) completely, maybe keep Bench (16) if you want to see them
      if (r.lineup_slot_id === 17) return; 

      if (!playerMap[r.player_id]) {
        playerMap[r.player_id] = {
          id: r.player_id,
          name: r.full_name,
          slotId: r.lineup_slot_id,
          records: []
        };
      }
      playerMap[r.player_id].records.push(r);
    });

    // 2. Aggregate stats for each player
    const allPlayers = Object.values(playerMap).map(p => ({
      ...p,
      stats: aggregateStats(p.records)
    }));

    // 3. Split into Batters (Slots 0-12, 16) and Pitchers (13-15)
    // Note: Bench (16) usually goes to batters unless they are pitchers. 
    // For simplicity, we put standard pitching slots in Pitchers.
    const batters = allPlayers.filter(p => p.slotId <= 12 || p.slotId === 16).sort((a,b) => a.slotId - b.slotId);
    const pitchers = allPlayers.filter(p => p.slotId >= 13 && p.slotId <= 15).sort((a,b) => a.slotId - b.slotId);

    // 4. Calculate Section Totals (Active Lineup Only)
    const activeBatters = batters.filter(p => p.slotId !== 16);
    const activePitchers = pitchers.filter(p => p.slotId !== 16);

    // We reconstruct "dummy records" to use the existing aggregator for totals
    // (This ensures rate stats like OBP/ERA are calculated correctly from the sums, not averaged)
    const batTotalRecords = activeBatters.flatMap(p => p.records);
    const pitchTotalRecords = activePitchers.flatMap(p => p.records);

    return {
      batters,
      pitchers,
      batTotals: aggregateStats(batTotalRecords),
      pitchTotals: aggregateStats(pitchTotalRecords)
    };
  };

  const homeData = useMemo(() => getTeamData(matchup.homeRecords), [matchup.homeRecords]);
  const awayData = useMemo(() => getTeamData(matchup.awayRecords), [matchup.awayRecords]);

  const currentData = activeTab === 'home' ? homeData : awayData;

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4 backdrop-blur-sm animate-fade-in">
      <div className="bg-white rounded-lg shadow-2xl w-full max-w-5xl max-h-[95vh] flex flex-col overflow-hidden">
        
        {/* --- HEADER --- */}
        <div className="bg-gray-900 text-white p-4 flex justify-between items-center shrink-0">
          <div className="flex items-center gap-4">
             <div className="text-3xl font-black">{matchup.result.homeScore} - {matchup.result.awayScore}</div>
             <div className="text-sm text-gray-400 border-l border-gray-700 pl-4">
                <span className="block font-bold text-white">Matchup Details</span>
                Week {matchup.matchupId.split('-')[0]}
             </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white text-3xl font-light leading-none">&times;</button>
        </div>

        {/* --- TABS --- */}
        <div className="flex bg-gray-100 border-b border-gray-200 shrink-0">
          <button 
            onClick={() => setActiveTab('home')}
            className={`flex-1 py-3 text-sm font-bold flex items-center justify-center gap-3 transition-colors ${activeTab === 'home' ? 'bg-white text-blue-800 border-t-4 border-blue-600' : 'text-gray-500 hover:bg-gray-200'}`}
          >
            <TeamAvatar team={matchup.homeTeam} size="sm" />
            <span className="uppercase tracking-wider">{matchup.homeTeam.name}</span>
          </button>
          <div className="w-px bg-gray-300"></div>
          <button 
            onClick={() => setActiveTab('away')}
            className={`flex-1 py-3 text-sm font-bold flex items-center justify-center gap-3 transition-colors ${activeTab === 'away' ? 'bg-white text-red-800 border-t-4 border-red-600' : 'text-gray-500 hover:bg-gray-200'}`}
          >
            <TeamAvatar team={matchup.awayTeam} size="sm" />
            <span className="uppercase tracking-wider">{matchup.awayTeam.name}</span>
          </button>
        </div>

        {/* --- SCROLLABLE CONTENT --- */}
        <div className="overflow-y-auto p-6 space-y-8 bg-gray-50">
          
          {/* BATTING TABLE */}
          <div className="bg-white border border-gray-200 rounded-sm shadow-sm overflow-hidden">
            <div className="bg-gray-100 px-3 py-2 border-b border-gray-200 font-bold text-xs text-gray-600 uppercase tracking-wider">Batters</div>
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200 text-xs text-gray-500">
                  <th className="py-2 px-3 text-left w-12 font-semibold">Slot</th>
                  <th className="py-2 px-3 text-left font-semibold">Player</th>
                  {batCats.map(c => <th key={c} className="py-2 px-1 text-center w-14 font-semibold">{SCORING_CATS[c].label}</th>)}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {currentData.batters.map(p => (
                  <tr key={p.id} className={`hover:bg-blue-50 ${p.slotId === 16 ? 'bg-gray-50 text-gray-400 italic' : ''}`}>
                    <td className="py-2 px-3 text-xs font-mono text-gray-400">{LINEUP_SLOTS[p.slotId] || 'BE'}</td>
                    <td className="py-2 px-3 font-medium text-gray-800">
                    <button 
                      onClick={() => onPlayerClick(p.id, p.name)} 
                      className="hover:text-blue-600 hover:underline text-left"
                    >
                      {p.name}
                    </button>
                  </td>
                    {batCats.map(c => (
                      <td key={c} className={`py-2 px-1 text-center ${p.slotId === 16 ? '' : 'font-mono'}`}>
                        {formatStat(p.stats[c], c)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
              <tfoot className="bg-gray-50 border-t-2 border-gray-100 font-bold text-gray-900">
                <tr>
                  <td colSpan="2" className="py-2 px-3 text-right text-xs uppercase text-gray-500">Totals</td>
                  {batCats.map(c => (
                    <td key={c} className="py-2 px-1 text-center">{formatStat(currentData.batTotals[c], c)}</td>
                  ))}
                </tr>
              </tfoot>
            </table>
          </div>

          {/* PITCHING TABLE */}
          <div className="bg-white border border-gray-200 rounded-sm shadow-sm overflow-hidden">
            <div className="bg-gray-100 px-3 py-2 border-b border-gray-200 font-bold text-xs text-gray-600 uppercase tracking-wider">Pitchers</div>
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200 text-xs text-gray-500">
                  <th className="py-2 px-3 text-left w-12 font-semibold">Slot</th>
                  <th className="py-2 px-3 text-left font-semibold">Player</th>
                  <th className="py-2 px-1 text-center w-14 font-semibold">IP</th> {/* Explicitly add IP */}
                  {pitchCats.map(c => <th key={c} className="py-2 px-1 text-center w-14 font-semibold">{SCORING_CATS[c].label}</th>)}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {currentData.pitchers.map(p => (
                  <tr key={p.id} className="hover:bg-blue-50">
                    <td className="py-2 px-3 text-xs font-mono text-gray-400">{LINEUP_SLOTS[p.slotId] || 'P'}</td>
                    <td className="py-2 px-3 font-medium text-gray-800">
                    <button 
                      onClick={() => onPlayerClick(p.id, p.name)} 
                      className="hover:text-blue-600 hover:underline text-left"
                    >
                      {p.name}
                    </button>
                  </td>
                    <td className="py-2 px-1 text-center font-mono">{p.stats.IP || '0.0'}</td>
                    {pitchCats.map(c => (
                      <td key={c} className="py-2 px-1 text-center font-mono">
                        {formatStat(p.stats[c], c)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
              <tfoot className="bg-gray-50 border-t-2 border-gray-100 font-bold text-gray-900">
                <tr>
                  <td colSpan="2" className="py-2 px-3 text-right text-xs uppercase text-gray-500">Totals</td>
                  <td className="py-2 px-1 text-center">-</td> {/* IP Total is complex to display, skipping for now or use totals.IP */}
                  {pitchCats.map(c => (
                    <td key={c} className="py-2 px-1 text-center">{formatStat(currentData.pitchTotals[c], c)}</td>
                  ))}
                </tr>
              </tfoot>
            </table>
          </div>

        </div>
      </div>
    </div>
  );
}