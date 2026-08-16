import { CATEGORIES } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

export default function BoxScoreModal({ matchup, onClose, onPlayerClick }) {
  if (!matchup) return null;

  const handleOverlayClick = (e) => {
    if (e.target === e.currentTarget) onClose();
  };

  // --- 1. TRIO BOX SCORE RENDER ---
  if (matchup.type === 'trio') {
    const { teams, teamStats, result } = matchup;

    const sortedTeams = [...teams].sort((a, b) => {
      const ptA = result[a.id]?.points ?? 0;
      const ptB = result[b.id]?.points ?? 0;
      return ptB - ptA;
    });

    // --- NEW: ADVANCED TRIO SHADING LOGIC ---
    const getTeamRanks = (cat) => {
      const isTie = (v1, v2) => Math.abs(v1 - v2) < 0.0001;
      
      // Pull and pair the values with their team IDs
      const vals = sortedTeams.map(t => ({ 
        id: t.id, 
        val: parseFloat(teamStats[t.id]?.[cat.id] || 0) 
      }));

      // Sort values based on whether Higher is Better
      vals.sort((a, b) => {
        if (isTie(a.val, b.val)) return 0;
        return cat.higherIsBetter ? b.val - a.val : a.val - b.val;
      });

      const bestVal = vals[0].val;
      const midVal = vals[1].val;
      const worstVal = vals[2].val;

      const is3WayTie = isTie(bestVal, worstVal);
      const isTieFor1st = !is3WayTie && isTie(bestVal, midVal);
      const isTieFor2nd = !is3WayTie && isTie(midVal, worstVal);

      const classes = {};

      vals.forEach(item => {
        if (is3WayTie) {
          // 3-Way Tie: Neutral Gray
          classes[item.id] = "bg-gray-100 text-gray-500 font-bold";
        } else if (isTieFor1st) {
          // 2-Way Tie for 1st: Mix of Green and Yellow
          if (isTie(item.val, bestVal)) classes[item.id] = "bg-gradient-to-br from-green-100 to-yellow-100 text-lime-800 font-bold";
          else classes[item.id] = "bg-red-50 text-red-700 font-bold"; // 3rd place outright
        } else if (isTieFor2nd) {
          // 2-Way Tie for 2nd: Mix of Yellow and Red
          if (isTie(item.val, bestVal)) classes[item.id] = "bg-green-100 text-green-800 font-bold"; // 1st place outright
          else classes[item.id] = "bg-gradient-to-br from-yellow-100 to-red-100 text-orange-800 font-bold";
        } else {
          // No Ties (Outright 1st, 2nd, 3rd)
          if (isTie(item.val, bestVal)) classes[item.id] = "bg-green-100 text-green-800 font-bold";
          else if (isTie(item.val, midVal)) classes[item.id] = "bg-yellow-100 text-yellow-800 font-bold";
          else classes[item.id] = "bg-red-50 text-red-700 font-bold";
        }
      });

      return classes;
    };

    return (
      <div onClick={handleOverlayClick} className="fixed inset-0 bg-black/70 flex items-center justify-center z-[60] p-4 backdrop-blur-sm animate-fade-in">
        <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[85vh] flex flex-col overflow-hidden">
          
          <div className="bg-slate-900 text-white p-4 flex justify-between items-center shrink-0">
            <div>
              <div className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">3-Way Matchup Box Score</div>
              <h2 className="text-lg font-black">{matchup.label || 'Regular Season'}</h2>
            </div>
            <button onClick={onClose} className="text-slate-400 hover:text-white text-3xl leading-none">&times;</button>
          </div>

          <div className="flex bg-gray-50 border-b border-gray-200 shrink-0">
            <div className="w-1/4 p-4 flex items-center justify-center border-r border-gray-200">
              <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Category</span>
            </div>
            {sortedTeams.map((team) => {
              const teamResult = result[team.id] || { points: 0, rank: 0 };
              const isOverallWinner = teamResult.rank === 1;
              const displayPts = Number.isInteger(teamResult.points) ? teamResult.points : teamResult.points.toFixed(1);

              return (
                <div key={team.id} className={`w-1/4 p-4 flex flex-col items-center border-r border-gray-200 last:border-0 ${isOverallWinner ? 'bg-green-50/50' : ''}`}>
                  <TeamAvatar team={team} size="md" />
                  <div className="mt-2 text-sm font-bold text-gray-900 text-center leading-tight truncate w-full">{team.name}</div>
                  <div className="text-[10px] text-gray-500">{team.owner}</div>
                  <div className={`mt-1 font-mono text-lg font-black ${isOverallWinner ? 'text-green-600' : 'text-gray-700'}`}>
                    {displayPts} pts
                  </div>
                </div>
              );
            })}
          </div>

          <div className="overflow-y-auto flex-1 bg-white">
            <div className="divide-y divide-gray-100">
              {CATEGORIES.map(cat => {
                // Get the shading classes for this specific category
                const rankClasses = getTeamRanks(cat);

                return (
                  <div key={cat.id} className="flex hover:bg-gray-50 transition-colors">
                    <div className="w-1/4 p-3 flex items-center justify-center border-r border-gray-100 bg-gray-50/50">
                      <span className="text-sm font-bold text-gray-700">{cat.name}</span>
                    </div>

                    {sortedTeams.map(team => {
                      const val = parseFloat(teamStats[team.id]?.[cat.id] || 0);
                      const displayVal = Number.isInteger(val) ? val : val.toFixed(3);
                      
                      // Apply the dynamically generated Tailwind class
                      const bgClass = rankClasses[team.id];

                      return (
                        <div key={team.id} className={`w-1/4 p-3 flex items-center justify-center border-r border-gray-100 last:border-0 ${bgClass}`}>
                          <span className="font-mono">{displayVal}</span>
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // --- 2. H2H BOX SCORE RENDER ---
  const { homeTeam, awayTeam, homeStats, awayStats, result } = matchup;
  
  return (
    <div onClick={handleOverlayClick} className="fixed inset-0 bg-black/70 flex items-center justify-center z-[60] p-4 backdrop-blur-sm animate-fade-in">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col overflow-hidden">
        
        <div className="bg-slate-900 text-white p-4 flex justify-between items-center shrink-0">
          <div>
            <div className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-1">Head-to-Head Box Score</div>
            <h2 className="text-lg font-black">{matchup.label || 'Regular Season Matchup'}</h2>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white text-3xl leading-none">&times;</button>
        </div>

        <div className="flex bg-gray-50 border-b border-gray-200 shrink-0">
          <div className="w-[40%] p-4 flex flex-col items-center justify-center">
            <TeamAvatar team={homeTeam} size="lg" />
            <div className="mt-2 text-base font-bold text-gray-900 text-center leading-tight truncate w-full">{homeTeam.name}</div>
            <div className="font-mono text-2xl font-black mt-1 text-gray-800">{result?.homeScore || 0}</div>
          </div>
          
          <div className="w-[20%] flex flex-col items-center justify-center border-x border-gray-200 bg-white">
            <span className="text-xs font-bold text-gray-400 uppercase tracking-widest">Ties</span>
            <span className="font-mono text-lg font-bold text-gray-600">{result?.ties || 0}</span>
          </div>

          <div className="w-[40%] p-4 flex flex-col items-center justify-center">
            <TeamAvatar team={awayTeam} size="lg" />
            <div className="mt-2 text-base font-bold text-gray-900 text-center leading-tight truncate w-full">{awayTeam.name}</div>
            <div className="font-mono text-2xl font-black mt-1 text-gray-800">{result?.awayScore || 0}</div>
          </div>
        </div>

        <div className="overflow-y-auto flex-1 bg-white">
          <div className="divide-y divide-gray-100">
            {CATEGORIES.map(cat => {
              const hVal = parseFloat(homeStats?.[cat.id] || 0);
              const aVal = parseFloat(awayStats?.[cat.id] || 0);
              
              let homeWins = false;
              let awayWins = false;

              if (Math.abs(hVal - aVal) > 0.0001) {
                if (cat.higherIsBetter) {
                  homeWins = hVal > aVal;
                  awayWins = aVal > hVal;
                } else {
                  homeWins = hVal < aVal;
                  awayWins = aVal < hVal;
                }
              }

              return (
                <div key={cat.id} className="flex hover:bg-gray-50 transition-colors">
                  <div className={`w-[40%] p-3 text-center font-mono ${homeWins ? 'bg-green-100 text-green-800 font-bold' : 'text-gray-600'}`}>
                    {Number.isInteger(hVal) ? hVal : hVal.toFixed(3)}
                  </div>
                  <div className="w-[20%] p-3 text-center border-x border-gray-100 bg-gray-50/50 flex items-center justify-center">
                    <span className="text-xs font-bold text-gray-700 uppercase">{cat.name}</span>
                  </div>
                  <div className={`w-[40%] p-3 text-center font-mono ${awayWins ? 'bg-green-100 text-green-800 font-bold' : 'text-gray-600'}`}>
                    {Number.isInteger(aVal) ? aVal : aVal.toFixed(3)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}