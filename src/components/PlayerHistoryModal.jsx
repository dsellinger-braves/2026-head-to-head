import { useMemo, useState } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { SCORING_CATS, LINEUP_SLOTS, aggregateStats } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

export default function PlayerHistoryModal({ playerId, playerName, allStats, onClose }) {
  const [selectedTeamId, setSelectedTeamId] = useState(null);

  // 1. Helper: Determine if a record is an "Active Appearance"
  // Uses raw ESPN stat IDs: '16' = PA, '34' = IP (in outs)
  const isActiveAppearance = (record) => {
    const s = record.stats || {};
    return (
      parseFloat(s['16']) > 0 || parseFloat(s.PA) > 0 ||
      parseFloat(s['2']) > 0 || parseFloat(s.AB) > 0 ||
      parseFloat(s['34']) > 0 || parseFloat(s.IP) > 0 || parseFloat(s.IP_OUTS) > 0
    );
  };

  // 2. Get records, filter, and sort
  const fullGameLog = useMemo(() => {
    return allStats
      .filter(r => r.player_id === playerId)
      .filter(isActiveAppearance)
      .sort((a, b) => b.scoring_period_id - a.scoring_period_id);
  }, [allStats, playerId]);

  // 3. Auto-detect Position
  const isPitcher = useMemo(() => {
    return fullGameLog.some(r => {
        const s = r.stats || {};
        return (s.IP && parseFloat(s.IP) > 0) || (s.IP_OUTS && s.IP_OUTS > 0);
    });
  }, [fullGameLog]);

  const displayMode = isPitcher ? 'pitching' : 'batting';
  
  // --- UPDATED COLUMNS HERE ---
  const batCats = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP'];
  const pitchCats = ['IP', 'ER', 'K', 'QS', 'QS_PCT', 'SV+HDs', 'ERA', 'WHIP'];
  
  const displayCats = displayMode === 'batting' ? batCats : pitchCats;

  // 4. Build Owner Summary
  const ownerSummary = useMemo(() => {
    const groups = {};
    
    fullGameLog.forEach(r => {
      const tid = r.team_id;
      if (!groups[tid]) {
        groups[tid] = {
          teamId: tid,
          teamName: TEAMS[tid]?.name || `Team ${tid}`,
          records: [],
          games: 0
        };
      }
      groups[tid].records.push(r);
      groups[tid].games += 1;
    });

    return Object.values(groups).map(g => ({
      ...g,
      stats: aggregateStats(g.records)
    })).sort((a, b) => b.games - a.games);
  }, [fullGameLog]);

  // 5. Helper for formatting
  const formatStat = (val, catKey) => {
    if (val === undefined || val === null) return '-';
    if (catKey === 'IP') {
      const ip = parseFloat(val) || 0;
      return `${Math.floor(ip)}.${Math.round((ip % 1) * 3)}`;
    }
    if (catKey === 'ERA') {
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(2);
    }
    if (catKey === 'OBP') {
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(4).replace(/^0/, '');
    }
    if (catKey === 'QS_PCT') {
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(1) + '%';
    }
    if (SCORING_CATS[catKey]?.isRate) {
      const num = parseFloat(val);
      return isNaN(num) ? '-' : num.toFixed(3).replace(/^0+/, '');
    }
    return val;
  };

  const getLabel = (col) => {
    if (col === 'QS_PCT') return 'QS%';
    return SCORING_CATS[col]?.label || col;
  };

  // 6. Filter for Detail View
  const activeRecords = selectedTeamId 
    ? fullGameLog.filter(r => r.team_id === parseInt(selectedTeamId))
    : [];
  
  const activeTeamName = selectedTeamId ? (TEAMS[selectedTeamId]?.name || 'Unknown') : '';

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-[60] p-4 backdrop-blur-sm animate-fade-in">
      <div className="bg-white rounded-lg shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden">
        
        {/* --- HEADER --- */}
        <div className="bg-blue-900 text-white p-4 flex justify-between items-center shadow-md shrink-0">
          <div className="flex items-center gap-4">
            {selectedTeamId && (
              <button 
                onClick={() => setSelectedTeamId(null)}
                className="bg-blue-800 hover:bg-blue-700 text-white px-3 py-1 rounded-full text-sm font-semibold transition-colors flex items-center gap-1"
              >
                &larr; Back
              </button>
            )}
            <div>
              <h2 className="text-xl font-bold">{playerName}</h2>
              <p className="text-blue-200 text-xs uppercase tracking-wider font-semibold">
                {selectedTeamId ? `${activeTeamName} Game Log` : 'Season Summary by Owner'}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="text-blue-300 hover:text-white text-3xl leading-none font-light">&times;</button>
        </div>

        {/* --- CONTENT --- */}
        <div className="overflow-y-auto p-0 flex-1 bg-gray-50">
          
          {/* VIEW 1: OWNER SUMMARY */}
          {!selectedTeamId && (
            <table className="w-full text-sm border-collapse">
              <thead className="bg-gray-100 sticky top-0 shadow-sm z-10 text-xs text-gray-500 uppercase tracking-wider font-semibold">
                <tr>
                  <th className="p-3 text-left border-b border-gray-200">Owner</th>
                  <th className="p-3 text-center border-b border-gray-200 w-16">Games</th>
                  {displayCats.map(c => (
                    <th key={c} className="p-3 text-center border-b border-gray-200 min-w-[50px]">
                      {getLabel(c)}
                    </th>
                  ))}
                  <th className="p-3 border-b border-gray-200"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 bg-white">
                {ownerSummary.length === 0 ? (
                   <tr><td colSpan="12" className="p-8 text-center text-gray-400 italic">No active games recorded this season.</td></tr>
                ) : (
                  ownerSummary.map(row => (
                    <tr 
                      key={row.teamId} 
                      onClick={() => setSelectedTeamId(row.teamId)}
                      className="hover:bg-blue-50 cursor-pointer transition-colors group"
                    >
                      <td className="p-3">
                        <div className="flex items-center gap-3">
                          <TeamAvatar team={{ id: row.teamId, name: row.teamName }} size="sm" />
                          <span className="font-bold text-gray-900">{row.teamName}</span>
                        </div>
                      </td>
                      <td className="p-3 text-center font-bold text-gray-600">{row.games}</td>
                      {displayCats.map(cat => (
                        <td key={cat} className="p-3 text-center text-gray-700 font-mono">
                          {formatStat(row.stats[cat], cat)}
                        </td>
                      ))}
                      <td className="p-3 text-right text-xs font-semibold text-blue-500 opacity-0 group-hover:opacity-100 transition-opacity">
                        View Log &rarr;
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          )}

          {/* VIEW 2: GAME LOG (Filtered) */}
          {selectedTeamId && (
             <table className="w-full text-sm border-collapse">
             <thead className="bg-gray-100 sticky top-0 shadow-sm z-10 text-xs text-gray-500 uppercase tracking-wider font-semibold">
               <tr>
                 <th className="p-3 text-left border-b border-gray-200">Date</th>
                 <th className="p-3 text-center border-b border-gray-200">Pos</th>
                 {displayCats.map(c => (
                   <th key={c} className="p-3 text-center border-b border-gray-200 min-w-[50px]">
                     {getLabel(c)}
                   </th>
                 ))}
               </tr>
             </thead>
             <tbody className="divide-y divide-gray-100 bg-white">
               {activeRecords.map(record => {
                  const dateStr = getDateFromPeriodId(record.scoring_period_id, record.season_year);
                  const pos = LINEUP_SLOTS[record.lineup_slot_id] || 'BN';
                  
                  // Calculate single-game stats
                  const singleStat = aggregateStats([record]);

                  return (
                   <tr key={record.id} className="hover:bg-gray-50 transition-colors border-b border-gray-50 last:border-0">
                     <td className="p-3 text-gray-600 font-mono text-xs font-medium">{dateStr}</td>
                     <td className="p-3 text-center text-gray-400 text-xs">{pos}</td>
                     {displayCats.map(cat => (
                       <td key={cat} className="p-3 text-center text-gray-700 font-mono text-xs">
                         {formatStat(singleStat[cat], cat)}
                       </td>
                     ))}
                   </tr>
                  );
               })}
             </tbody>
           </table>
          )}

        </div>
      </div>
    </div>
  );
}