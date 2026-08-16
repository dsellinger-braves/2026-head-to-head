import { useMemo, useState } from 'react';
import { SCORING_CATS, aggregateStats } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

export default function OwnerDetailModal({ team, allStats, onClose, onPlayerClick }) {
  const [activeTab, setActiveTab] = useState('batters');

  // 1. Filter Data for this Team ONLY
  const teamRoster = useMemo(() => {
    const teamRecords = allStats.filter(r => r.team_id === team.id);
    
    // Group by Player
    const playerMap = {};
    teamRecords.forEach(r => {
      if (!playerMap[r.player_id]) {
        playerMap[r.player_id] = {
          id: r.player_id,
          name: r.full_name,
          records: []
        };
      }
      playerMap[r.player_id].records.push(r);
    });

    // Aggregate & Classify
    const roster = Object.values(playerMap).map(p => {
      const stats = aggregateStats(p.records);
      
      const PITCHER_SLOTS = new Set([13, 14, 15]);
      const isPitcher = p.records.some(r => PITCHER_SLOTS.has(r.lineup_slot_id));
      
      return { ...p, stats, isPitcher };
    });

    // Split & Sort by Volume
    const batters = roster
      .filter(p => !p.isPitcher)
      .sort((a, b) => (b.stats.PA || 0) - (a.stats.PA || 0));

    const pitchers = roster
      .filter(p => p.isPitcher)
      .sort((a, b) => (b.stats.IP || 0) - (a.stats.IP || 0)); // Sort by IP_OUTS (Math safe!)

    return { batters, pitchers };
  }, [allStats, team.id]);

  // Display Config
  const batCats = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP'];
  const pitchCats = ['IP', 'QS', 'K', 'SV+HDs', 'ERA', 'WHIP'];
  
  const currentList = activeTab === 'batters' ? teamRoster.batters : teamRoster.pitchers;
  const currentCats = activeTab === 'batters' ? batCats : pitchCats;

  // --- UPDATED FORMATTER ---
  const formatStat = (val, catKey, fullStatsObj) => {
    if (val === undefined || val === null) return '-';

    if (catKey === 'IP') {
      const innings = Math.floor(fullStatsObj.IP);
      const outs = Math.round((fullStatsObj.IP - innings) * 3);
      return `${innings}.${outs}`;
    }
    if (catKey === 'ERA') {
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(2);
    }
    if (catKey === 'OBP') {
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
    }
    if (SCORING_CATS[catKey]?.isRate) {
      const num = parseFloat(val);
      return isNaN(num) ? '-' : num.toFixed(3).replace(/^0+/, '');
    }
    return val;
  };

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-[70] p-4 backdrop-blur-sm animate-fade-in">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-5xl max-h-[90vh] flex flex-col overflow-hidden">
        
        {/* HEADER */}
        <div className="bg-gradient-to-r from-blue-900 to-blue-800 text-white p-6 shrink-0 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <TeamAvatar team={team} size="lg" className="border-2 border-white shadow-md" />
            <div>
              <h2 className="text-2xl font-black tracking-tight">{team.name}</h2>
              <p className="text-blue-200 font-medium text-sm uppercase tracking-wider">{team.owner}</p>
            </div>
          </div>
          <button onClick={onClose} className="bg-white/10 hover:bg-white/20 p-2 rounded-full transition-colors">
            <svg className="w-6 h-6 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M6 18L18 6M6 6l12 12" /></svg>
          </button>
        </div>

        {/* TABS */}
        <div className="flex border-b border-gray-200 shrink-0">
          <button 
            onClick={() => setActiveTab('batters')}
            className={`flex-1 py-4 text-sm font-bold uppercase tracking-wider transition-all ${activeTab === 'batters' ? 'border-b-4 border-blue-600 text-blue-800 bg-blue-50' : 'text-gray-500 hover:bg-gray-50'}`}
          >
            Batters ({teamRoster.batters.length})
          </button>
          <div className="w-px bg-gray-200"></div>
          <button 
            onClick={() => setActiveTab('pitchers')}
            className={`flex-1 py-4 text-sm font-bold uppercase tracking-wider transition-all ${activeTab === 'pitchers' ? 'border-b-4 border-blue-600 text-blue-800 bg-blue-50' : 'text-gray-500 hover:bg-gray-50'}`}
          >
            Pitchers ({teamRoster.pitchers.length})
          </button>
        </div>

        {/* TABLE */}
        <div className="overflow-y-auto bg-gray-50 flex-1 p-0">
          <table className="w-full text-sm border-collapse">
            <thead className="bg-white sticky top-0 shadow-sm z-10">
              <tr>
                <th className="p-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-100 pl-6">Player</th>
                {currentCats.map(c => (
                  <th key={c} className="p-3 text-center text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-100 w-24">
                    {SCORING_CATS[c]?.label || c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 bg-white">
              {currentList.length === 0 ? (
                <tr><td colSpan="10" className="p-10 text-center text-gray-400 italic">No players found in this category.</td></tr>
              ) : (
                currentList.map((p, i) => (
                  <tr key={p.id} className="hover:bg-blue-50 transition-colors group">
                    <td className="p-3 pl-6">
                      <div className="flex items-center gap-3">
                        <span className="text-gray-300 font-mono text-xs w-4 text-right">{i + 1}</span>
                        <button 
                          onClick={() => onPlayerClick(p.id, p.name)}
                          className="font-bold text-gray-800 hover:text-blue-600 hover:underline text-left"
                        >
                          {p.name}
                        </button>
                      </div>
                    </td>
                    {currentCats.map(cat => (
                      <td key={cat} className={`p-3 text-center font-mono text-gray-700 ${(cat === 'PA' || cat === 'IP') ? 'font-bold bg-gray-50 text-gray-900' : ''}`}>
                        {/* PASS THE FULL STATS OBJECT HERE */}
                        {formatStat(p.stats[cat], cat, p.stats)}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}