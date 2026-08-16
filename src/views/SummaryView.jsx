import { useMemo } from 'react';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';

export default function SummaryView({ processedWeeks, onOwnerClick }) {

// --- 1. HELPER: CALCULATE STANDINGS FOR A SPECIFIC PHASE ---
  const getStandings = (phaseFilter) => {
    const stats = {};
    Object.keys(TEAMS).forEach(id => {
      if (parseInt(id) === 99) return;
      stats[id] = {
        id: parseInt(id),
        name: TEAMS[id].name,
        owner: TEAMS[id].owner,
        points: 0,
        possiblePoints: 0
      };
    });

    if (processedWeeks) {
      processedWeeks.forEach(week => {
        if (week.phase !== phaseFilter) return;
        if (!week.matchups) return;

        week.matchups.forEach(m => {
          if (!m.result || m.isPlaceholder) return;

          if (m.type === 'trio') {
            // Skip unplayed weeks: all teams have 0 points
            const totalPts = Object.values(m.result).reduce((sum, r) => sum + (r.points ?? 0), 0);
            if (totalPts === 0) return;

            m.teams.forEach(team => {
              const tr = m.result[team.id];
              if (!tr || !stats[team.id]) return;
              stats[team.id].points += tr.points;
              stats[team.id].possiblePoints += 20; // 10 cats × 2 pts max
            });
          } else {
            if (m.result.homeScore === 0 && m.result.awayScore === 0 && m.result.ties === 0) return;
            if (!m.homeTeam?.id || !m.awayTeam?.id) return;
            if (!stats[m.homeTeam.id] || !stats[m.awayTeam.id]) return;

            stats[m.homeTeam.id].points += m.result.homeScore + (m.result.ties * 0.5);
            stats[m.awayTeam.id].points += m.result.awayScore + (m.result.ties * 0.5);
            stats[m.homeTeam.id].possiblePoints += 10; // 10 cats max per H2H week
            stats[m.awayTeam.id].possiblePoints += 10;
          }
        });
      });
    }

    return Object.values(stats).sort((a, b) => {
      const pctA = a.possiblePoints > 0 ? a.points / a.possiblePoints : 0;
      const pctB = b.possiblePoints > 0 ? b.points / b.possiblePoints : 0;
      if (Math.abs(pctB - pctA) > 0.0001) return pctB - pctA;
      return b.points - a.points;
    });
  };

  // --- 2. GENERATE DATA SETS ---
  const phase1Standings = useMemo(() => getStandings(1), [processedWeeks]);
  const phase2Standings = useMemo(() => getStandings(2), [processedWeeks]);

  // Determine who is in which league based on Phase 1 results
  // Top 4 = Winners, Bottom 4 = Consolation
  const winnersLeagueIds = new Set(phase1Standings.slice(0, 4).map(t => t.id));
  
  const winnersStandings = phase2Standings.filter(t => winnersLeagueIds.has(t.id));
  const consolationStandings = phase2Standings.filter(t => !winnersLeagueIds.has(t.id));

  // --- 3. HELPER: GET PLAYOFF MATCHUPS ---
  const getPlayoffMatchup = (id) => {
    // Search weeks 24 and 25
    for (const w of processedWeeks) {
      if (w.phase === 3 && w.matchups) {
        const m = w.matchups.find(m => m.id === id || m.matchupId === id);
        if (m) return m;
      }
    }
    return null;
  };

  const sf1 = getPlayoffMatchup('sf1');
  const sf2 = getPlayoffMatchup('sf2');
  const final = getPlayoffMatchup('final');
  const third = getPlayoffMatchup('3rd');

  // --- 4. SHARED TABLE COMPONENT ---
  const StandingsTable = ({ title, data, showRank = true }) => (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden mb-8">
      <div className="bg-gray-50 px-6 py-4 border-b border-gray-200">
        <h3 className="text-lg font-bold text-gray-800">{title}</h3>
      </div>
      <table className="min-w-full">
        <thead className="bg-white border-b border-gray-100 text-gray-500 text-xs uppercase tracking-wider font-semibold">
          <tr>
            <th className="px-6 py-3 text-left w-12">Rank</th>
            <th className="px-6 py-3 text-left">Team</th>
            <th className="px-6 py-3 text-center w-24">Pts</th>
            <th className="px-6 py-3 text-center w-24">Possible</th>
            <th className="px-6 py-3 text-center w-24">Pts%</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-50">
          {data.map((team, index) => {
            const pct = team.possiblePoints > 0
              ? ((team.points / team.possiblePoints) * 100).toFixed(1)
              : '0.0';
            const displayPts = Number.isInteger(team.points) ? team.points : team.points.toFixed(1);
            return (
              <tr key={team.id} className="hover:bg-blue-50 transition-colors group">
                <td className="px-6 py-4 text-gray-400 font-bold group-hover:text-blue-600">
                  {showRank ? index + 1 : '-'}
                </td>
                <td className="px-6 py-4">
                  <div onClick={() => onOwnerClick(team)} className="flex items-center cursor-pointer">
                    <div className="transform group-hover:scale-110 transition-transform duration-200">
                      <TeamAvatar team={team} size="sm" />
                    </div>
                    <div className="ml-3">
                      <div className="text-sm font-bold text-gray-900 group-hover:text-blue-700 group-hover:underline">{team.name}</div>
                      <div className="text-xs text-gray-500">{team.owner}</div>
                    </div>
                  </div>
                </td>
                <td className="px-6 py-4 text-center text-sm font-bold font-mono text-gray-900">{displayPts}</td>
                <td className="px-6 py-4 text-center text-sm font-mono text-gray-400">{team.possiblePoints}</td>
                <td className="px-6 py-4 text-center text-sm font-mono font-bold text-blue-700">{pct}%</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );

  // --- 5. BRACKET CARD COMPONENT ---
  const BracketMatch = ({ title, m }) => {
    if (!m) return <div className="bg-gray-50 rounded border border-gray-200 p-4 h-24 flex items-center justify-center text-gray-400 text-xs">TBD</div>;
    
    // Check if placeholder
    const isPlaceholder = !m.homeTeam.id;
    
    return (
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden flex flex-col w-64">
        <div className="bg-gray-100 px-3 py-1 text-[10px] uppercase font-bold text-gray-500 border-b border-gray-200 text-center">
          {title}
        </div>
        {/* Home */}
        <div className={`flex justify-between items-center p-2 border-b border-gray-100 ${m.result?.homeScore > m.result?.awayScore ? 'bg-green-50' : ''}`}>
          <div className="flex items-center gap-2">
            <div className="text-xs font-bold text-gray-700 truncate w-24">
               {isPlaceholder ? m.homeTeam.name : TEAMS[m.homeTeam.id]?.name}
            </div>
          </div>
          <span className="font-mono font-bold text-sm">{m.result?.homeScore || 0}</span>
        </div>
        {/* Away */}
        <div className={`flex justify-between items-center p-2 ${m.result?.awayScore > m.result?.homeScore ? 'bg-green-50' : ''}`}>
           <div className="flex items-center gap-2">
            <div className="text-xs font-bold text-gray-700 truncate w-24">
              {isPlaceholder ? m.awayTeam.name : TEAMS[m.awayTeam.id]?.name}
            </div>
          </div>
          <span className="font-mono font-bold text-sm">{m.result?.awayScore || 0}</span>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-12 animate-fade-in-up pb-20">
      
      {/* --- SECTION 1: PLAYOFF BRACKET (Weeks 24-25) --- */}
      <div className="space-y-4">
        <h2 className="text-2xl font-black text-gray-900 flex items-center gap-3">
          <span className="bg-blue-600 text-white text-sm px-3 py-1 rounded-full">Phase 3</span>
          Championship Bracket
        </h2>
        
        <div className="bg-slate-800 rounded-xl p-8 overflow-x-auto">
          <div className="flex items-center justify-center gap-12 min-w-[800px]">
             
             {/* SEMIS COLUMN */}
             <div className="flex flex-col gap-12">
                <BracketMatch title="Semi-Final 1" m={sf1} />
                <BracketMatch title="Semi-Final 2" m={sf2} />
             </div>

             {/* CONNECTOR */}
             <div className="flex flex-col justify-center h-48">
                <div className="w-8 border-t-2 border-r-2 border-b-2 border-slate-600 h-24 rounded-r-xl"></div>
             </div>

             {/* FINALS COLUMN */}
             <div className="flex flex-col gap-12">
                <div className="relative">
                   <div className="absolute -top-8 left-0 right-0 text-center text-yellow-400 font-bold text-xs tracking-widest uppercase mb-2">🏆 Championship</div>
                   <BracketMatch title="The Finals" m={final} />
                </div>
                <div className="opacity-75 scale-90">
                   <BracketMatch title="3rd Place" m={third} />
                </div>
             </div>
          </div>
        </div>
      </div>

      {/* --- SECTION 2: PHASE 2 STANDINGS (Weeks 15-23) --- */}
      <div className="space-y-6">
        <h2 className="text-2xl font-black text-gray-900 flex items-center gap-3">
          <span className="bg-green-600 text-white text-sm px-3 py-1 rounded-full">Phase 2</span>
          Split Leagues
        </h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <StandingsTable title="🏆 Winner's League (Top 4)" data={winnersStandings} />
          <StandingsTable title="🛡️ Consolation League" data={consolationStandings} />
        </div>
      </div>

      {/* --- SECTION 3: PHASE 1 STANDINGS (Weeks 1-14) --- */}
      <div className="space-y-4 opacity-75 hover:opacity-100 transition-opacity">
        <h2 className="text-2xl font-black text-gray-500 flex items-center gap-3">
          <span className="bg-gray-400 text-white text-sm px-3 py-1 rounded-full">Phase 1</span>
          Regular Season History
        </h2>
        <StandingsTable title="Weeks 1-14 Round Robin" data={phase1Standings} />
      </div>

    </div>
  );
}