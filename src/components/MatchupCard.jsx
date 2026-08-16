import { CATEGORIES } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

// 1. ADD 'onViewBoxScore' to the props list here
export default function MatchupCard({ matchup, onViewBoxScore, onOwnerClick }) {
  
  // Safety check: if data is missing, show loading
 
 if (matchup.type === 'trio') {
    const { teams, result, label } = matchup;

    const sortedTeams = [...teams].sort((a, b) => {
      const ptA = result[a.id]?.points ?? 0;
      const ptB = result[b.id]?.points ?? 0;
      return ptB - ptA;
    });

    return (
      <div
        onClick={onViewBoxScore}
        className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden hover:border-blue-400 hover:shadow-md transition-all cursor-pointer group"
      >
        {label && (
          <div className="bg-gradient-to-r from-blue-600 to-indigo-600 text-white text-center text-xs font-bold uppercase tracking-wider py-1">
            {label}
          </div>
        )}
        <div className="flex divide-x divide-gray-100 p-2">
          {sortedTeams.map((team) => {
            const teamResult = result[team.id] || { points: 0, rank: 0 };
            const isWinner = teamResult.rank === 1;
            const displayPts = Number.isInteger(teamResult.points) ? teamResult.points : teamResult.points.toFixed(1);

            return (
              <div key={team.id} className={`flex-1 flex flex-col items-center justify-center p-3 transition-colors ${isWinner ? 'bg-green-50/30' : ''}`}>
                <div onClick={(e) => { e.stopPropagation(); onOwnerClick(team); }} className="hover:scale-110 transition-transform">
                  <TeamAvatar team={team} size="md" />
                </div>
                <div className="mt-2 text-sm font-bold text-gray-800 text-center leading-tight">
                  {team.name}
                </div>
                <div className="text-[10px] text-gray-500 mb-1">{team.owner}</div>
                <div className={`font-mono text-lg font-black mt-1 ${isWinner ? 'text-green-600' : 'text-gray-700'}`}>
                  {displayPts} pts
                </div>
              </div>
            );
          })}
        </div>
        <div className="bg-gray-50 py-2 text-center text-[10px] font-bold text-blue-500 uppercase tracking-wider opacity-0 group-hover:opacity-100 transition-opacity">
          View 3-Way Box Score
        </div>
      </div>
    );
  }
 
  const { homeStats, awayStats, result } = matchup;
 
  if (!homeStats || !awayStats || !result) {
    return (
      <div className="bg-white rounded-xl shadow-sm p-6 text-center text-gray-400 animate-pulse">
        <div className="h-4 bg-gray-200 rounded w-3/4 mx-auto mb-4"></div>
        <div className="h-32 bg-gray-100 rounded"></div>
      </div>
    );
  }

  const getStatStyle = (catKey, side) => {
    const winner = result.details?.[catKey];
    const isWin = winner === side;
    const isTie = winner === 'tie';
    
    if (isWin) return "bg-green-50 font-bold text-gray-900";
    if (isTie) return "bg-gray-50 text-gray-600";
    return "text-gray-400 opacity-80"; 
  };

  // Define categories to loop through
  const batCats = ['R', 'HR', 'RBI', 'SB', 'OBP'];
  const pitchCats = ['K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden hover:shadow-md transition-shadow duration-200 flex flex-col">
      
      {/* --- SCOREBOARD HEADER --- */}
      <div className="relative p-4 pb-6 bg-gradient-to-b from-white to-gray-50 border-b border-gray-100">
        <div className="flex justify-between items-center relative z-10">
          
          {/* Home Team */}
          <div 
          onClick={() => onOwnerClick(matchup.homeTeam)}
          className="flex flex-col items-center w-1/3 cursor-pointer">
            <TeamAvatar team={matchup.homeTeam} size="md" />
            <div className="mt-2 text-center">
              <div className="font-bold text-sm text-gray-900 leading-tight">{matchup.homeTeam.name}</div>
              <div className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">{matchup.homeTeam.owner}</div>
            </div>
          </div>

          {/* The Score */}
          <div className="flex flex-col items-center w-1/3">
            <div className="text-xs font-bold text-gray-400 uppercase tracking-widest mb-1">vs</div>
            <div className="flex items-baseline gap-1">
              <span className={`text-4xl font-black ${result.homeScore > result.awayScore ? 'text-gray-900' : 'text-gray-400'}`}>
                {result.homeScore}
              </span>
              <span className="text-gray-300 text-2xl">-</span>
              <span className={`text-4xl font-black ${result.awayScore > result.homeScore ? 'text-gray-900' : 'text-gray-400'}`}>
                {result.awayScore}
              </span>
            </div>
            {result.ties > 0 && (
              <span className="text-xs font-medium text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full mt-1">
                {result.ties} Ties
              </span>
            )}
          </div>

          {/* Away Team */}
          <div 
          onClick={() => onOwnerClick(matchup.awayTeam)}
          className="flex flex-col items-center w-1/3 cursor-pointer">
            <TeamAvatar team={matchup.awayTeam} size="md" />
            <div className="mt-2 text-center">
              <div className="font-bold text-sm text-gray-900 leading-tight">{matchup.awayTeam.name}</div>
              <div className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">{matchup.awayTeam.owner}</div>
            </div>
          </div>

        </div>
      </div>

      {/* --- STATS GRID --- */}
      <div className="text-sm flex-1">
        {/* Batting */}
        <div className="bg-gray-100 px-3 py-1 text-[10px] font-bold text-gray-500 uppercase tracking-wider border-y border-gray-200">Batting</div>
        {batCats.map(cat => {
            const config = CATEGORIES[cat] || { label: cat }; 
            return (
              <div key={cat} className="grid grid-cols-3 border-b border-gray-100 last:border-0">
                <div className={`py-2 text-center ${getStatStyle(cat, 'home')}`}>{homeStats[cat]}</div>
                <div className="py-2 text-center text-[10px] font-bold text-gray-400 uppercase tracking-wider flex items-center justify-center bg-white border-x border-gray-50">
                  {config.label}
                </div>
                <div className={`py-2 text-center ${getStatStyle(cat, 'away')}`}>{awayStats[cat]}</div>
              </div>
            );
        })}

        {/* Pitching */}
        <div className="bg-gray-100 px-3 py-1 text-[10px] font-bold text-gray-500 uppercase tracking-wider border-y border-gray-200">Pitching</div>
        {pitchCats.map(cat => {
            const config = CATEGORIES[cat] || { label: cat };
            return (
              <div key={cat} className="grid grid-cols-3 border-b border-gray-100 last:border-0">
                <div className={`py-2 text-center ${getStatStyle(cat, 'home')}`}>{homeStats[cat]}</div>
                <div className="py-2 text-center text-[10px] font-bold text-gray-400 uppercase tracking-wider flex items-center justify-center bg-white border-x border-gray-50">
                  {config.label}
                </div>
                <div className={`py-2 text-center ${getStatStyle(cat, 'away')}`}>{awayStats[cat]}</div>
              </div>
            );
        })}
      </div>

      {/* --- 2. THE BUTTON (This was missing) --- */}
      <div 
         onClick={onViewBoxScore}
         className="bg-gray-50 p-3 text-center border-t border-gray-200 hover:bg-gray-100 cursor-pointer transition-colors group mt-auto"
      >
        <span className="text-xs font-bold text-blue-600 group-hover:text-blue-800 uppercase tracking-wide flex items-center justify-center gap-1">
          View Full Box Score 
          <span className="transition-transform group-hover:translate-x-1">&rarr;</span>
        </span>
      </div>

    </div>
  );
}