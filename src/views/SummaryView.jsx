// src/views/SummaryView.jsx
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';

export default function SummaryView({ processedWeeks }) {
  
  // 1. Calculate Standings
  const standings = {};
  Object.keys(TEAMS).forEach(id => {
    standings[id] = { id, name: TEAMS[id].name, owner: TEAMS[id].owner, wins: 0, losses: 0, ties: 0 };
  });

  if (processedWeeks && processedWeeks.length > 0) {
    processedWeeks.forEach(week => {
      if (week.matchups) {
        week.matchups.forEach(m => {
          if (!m.result) return; 
          if (m.result.homeScore > m.result.awayScore) standings[m.homeTeam.id].wins++;
          else if (m.result.homeScore < m.result.awayScore) standings[m.homeTeam.id].losses++;
          else standings[m.homeTeam.id].ties++;

          if (m.result.awayScore > m.result.homeScore) standings[m.awayTeam.id].wins++;
          else if (m.result.awayScore < m.result.homeScore) standings[m.awayTeam.id].losses++;
          else standings[m.awayTeam.id].ties++;
        });
      }
    });
  }

  // Sort by Win % (Wins + 0.5 * Ties)
  const sortedStandings = Object.values(standings).sort((a, b) => {
    const scoreA = a.wins + (a.ties * 0.5);
    const scoreB = b.wins + (b.ties * 0.5);
    return scoreB - scoreA;
  });

  return (
    <div className="space-y-8 animate-fade-in-up">
      
      {/* --- STANDINGS CARD --- */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        <div className="bg-gray-50 px-6 py-4 border-b border-gray-200 flex justify-between items-center">
          <h3 className="text-lg font-bold text-gray-800">League Standings</h3>
          <span className="text-xs font-medium text-gray-500 bg-white border px-2 py-1 rounded">Regular Season</span>
        </div>
        <table className="min-w-full">
          <thead className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider font-semibold">
            <tr>
              <th className="px-6 py-3 text-left w-12">Rank</th>
              <th className="px-6 py-3 text-left">Team</th>
              <th className="px-6 py-3 text-center w-20">W</th>
              <th className="px-6 py-3 text-center w-20">L</th>
              <th className="px-6 py-3 text-center w-20">T</th>
              <th className="px-6 py-3 text-center w-24">Pct</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {sortedStandings.map((team, index) => {
              const totalGames = team.wins + team.losses + team.ties;
              const pct = totalGames > 0 ? ((team.wins + (team.ties * 0.5)) / totalGames).toFixed(3) : '.000';
              
              return (
                <tr key={team.id} className="hover:bg-blue-50 transition-colors group">
                  <td className="px-6 py-4 text-gray-400 font-bold group-hover:text-blue-600">{index + 1}</td>
                  <td className="px-6 py-4">
                    <div className="flex items-center">
                      <TeamAvatar team={team} size="sm" />
                      <div className="ml-3">
                        <div className="text-sm font-bold text-gray-900">{team.name}</div>
                        <div className="text-xs text-gray-500">{team.owner}</div>
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-center text-sm font-bold text-gray-900">{team.wins}</td>
                  <td className="px-6 py-4 text-center text-sm text-gray-600">{team.losses}</td>
                  <td className="px-6 py-4 text-center text-sm text-gray-400">{team.ties}</td>
                  <td className="px-6 py-4 text-center text-sm font-mono text-gray-600">{pct}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* --- SEASON GRID --- */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden p-6">
        <h3 className="text-lg font-bold text-gray-800 mb-6">Season Matchup Grid</h3>
        <div className="overflow-x-auto pb-4">
          <table className="border-collapse text-xs w-full">
            <thead>
              <tr>
                <th className="p-3 text-left bg-gray-50 border-b-2 border-gray-200 min-w-[150px] font-bold text-gray-600 sticky left-0 z-10">Team</th>
                {processedWeeks.map(w => (
                  <th key={w.weekId} className="p-2 text-center bg-white border-b border-gray-200 min-w-[60px] text-gray-400 font-medium">
                    {w.weekId}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {Object.keys(TEAMS).map(teamId => (
                <tr key={teamId} className="hover:bg-gray-50">
                  <td className="p-3 border-r border-gray-100 bg-white font-bold text-gray-800 sticky left-0 z-10 group-hover:bg-gray-50">
                    <div className="flex items-center gap-2">
                       {/* Optional: Small colored dot instead of full avatar to save space */}
                       <div className={`w-2 h-2 rounded-full bg-gray-400`}></div>
                       {TEAMS[teamId].name}
                    </div>
                  </td>
                  {processedWeeks.map(week => {
                    const m = week.matchups ? week.matchups.find(m => m.homeTeam.id == teamId || m.awayTeam.id == teamId) : null;
                    
                    if (!m || !m.result) return <td key={week.weekId} className="p-2 text-center text-gray-200">-</td>;

                    const isHome = m.homeTeam.id == teamId;
                    const myScore = isHome ? m.result.homeScore : m.result.awayScore;
                    const oppScore = isHome ? m.result.awayScore : m.result.homeScore;
                    
                    let outcome = 'T';
                    let bgClass = 'bg-gray-100 text-gray-500';
                    if (myScore > oppScore) { outcome = 'W'; bgClass = 'bg-green-100 text-green-700 font-bold'; }
                    if (myScore < oppScore) { outcome = 'L'; bgClass = 'bg-red-50 text-red-400'; }

                    return (
                      <td key={week.weekId} className="p-1 text-center border border-gray-50">
                        <div className={`rounded py-1 px-1 ${bgClass} text-[10px]`}>
                          {outcome}
                        </div>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}