import { useEffect, useState } from 'react';
import { fetchGameBoxscore, normalizeName } from '../utils/liveMLB';
import { TEAMS } from '../schedule';
import TeamAvatar from './TeamAvatar';

export default function GameDetailModal({ game, rosterDict, onClose }) {
  const [boxscore, setBoxscore] = useState(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('away'); // 'away' or 'home'

  useEffect(() => {
    const loadBoxscore = async () => {
      setLoading(true);
      const data = await fetchGameBoxscore(game.gamePk);
      setBoxscore(data);
      setLoading(false);
    };
    loadBoxscore();
  }, [game.gamePk]);

  // Handle click outside to close
  const handleOverlayClick = (e) => {
    if (e.target === e.currentTarget) onClose();
  };

  // Helper to process the full team lineup and inject Fantasy Data
  const processTeamBoxscore = (team) => {
    if (!team || !team.players) return { batters: [], pitchers: [] };
    
    // Process Batters
    const batters = (team.batters || []).map(id => {
      const p = team.players[`ID${id}`];
      if (!p) return null;
      
      const cleanName = normalizeName(p.person.fullName);
      const fantasyData = rosterDict[cleanName];
      
      // MLB API battingOrder is typically "100" for starter, "101" for 1st sub, etc.
      const isSub = p.battingOrder && parseInt(p.battingOrder) % 100 !== 0;

      return {
        id,
        name: p.person.fullName,
        position: p.position.abbreviation,
        isSub,
        stats: p.stats?.batting || {},
        fantasyTeam: fantasyData ? TEAMS[fantasyData.teamId] : null,
        isBench: fantasyData?.isBench
      };
    }).filter(Boolean);

// Process Pitchers
    const pitchers = (team.pitchers || []).map(id => {
      const p = team.players[`ID${id}`];
      if (!p) return null;
      
      const cleanName = normalizeName(p.person.fullName);
      const fantasyData = rosterDict[cleanName];

      const stats = p.stats?.pitching || {};
      
      // Calculate Quality Start dynamically
      let qs = stats.qualityStarts;
      if (qs === undefined) {
          const ipString = String(stats.inningsPitched || "0");
          const fullInnings = parseInt(ipString.split('.')[0] || 0);
          const er = parseInt(stats.earnedRuns || 0);
          qs = (fullInnings >= 6 && er <= 3) ? 1 : 0;
      }

      return {
        id,
        name: p.person.fullName,
        stats: { ...stats, qs }, // inject qs into stats
        fantasyTeam: fantasyData ? TEAMS[fantasyData.teamId] : null,
        isBench: fantasyData?.isBench
      };
    }).filter(Boolean);

    return { batters, pitchers };
  };

  const awayData = processTeamBoxscore(boxscore?.teams?.away);
  const homeData = processTeamBoxscore(boxscore?.teams?.home);

  const activeData = activeTab === 'away' ? awayData : homeData;
  const activeTeamName = activeTab === 'away' ? game.teams.away.team.name : game.teams.home.team.name;

  // UI Component: Fantasy Badge
  const FantasyBadge = ({ team, isBench }) => {
    if (!team) return null;
    return (
      <div className={`ml-2 inline-flex items-center gap-1.5 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider ${isBench ? 'bg-gray-200 text-gray-500' : 'bg-blue-100 text-blue-800 border border-blue-200'}`}>
        <TeamAvatar team={team} size="xs" />
        <span className="truncate max-w-[80px] sm:max-w-none">{team.name}</span>
        {isBench && <span className="text-red-500">(B)</span>}
      </div>
    );
  };

  return (
    <div onClick={handleOverlayClick} className="fixed inset-0 bg-black/75 flex items-center justify-center z-[70] p-2 sm:p-4 backdrop-blur-sm animate-fade-in">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden">
        
        {/* HEADER */}
        <div className="bg-slate-900 text-white p-4 sm:p-5 flex justify-between items-center shrink-0">
          <div>
            <div className="flex items-center gap-2 mb-1">
              {game.status.abstractGameState === 'Live' && (
                <span className="relative flex h-2.5 w-2.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-red-500"></span>
                </span>
              )}
              <div className="text-xs font-bold text-slate-400 uppercase tracking-widest">
                {game.status.abstractGameState === 'Live' ? `${game.linescore?.inningState} ${game.linescore?.currentInning}` : game.status.detailedState}
              </div>
            </div>
            <h2 className="text-lg sm:text-2xl font-black flex items-center gap-2 sm:gap-3">
              <span>{game.teams.away.team.name} <span className="text-slate-400">({game.teams.away.score || 0})</span></span>
              <span className="text-slate-500 text-sm font-normal">@</span>
              <span>{game.teams.home.team.name} <span className="text-slate-400">({game.teams.home.score || 0})</span></span>
            </h2>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white text-3xl leading-none">&times;</button>
        </div>

        {/* TABS */}
        <div className="flex border-b border-gray-200 bg-gray-50 shrink-0">
          <button 
            onClick={() => setActiveTab('away')}
            className={`flex-1 py-3 text-sm sm:text-base font-bold transition-colors ${activeTab === 'away' ? 'bg-white text-blue-700 border-b-2 border-blue-600' : 'text-gray-500 hover:text-gray-800 hover:bg-gray-100'}`}
          >
            {game.teams.away.team.name}
          </button>
          <button 
            onClick={() => setActiveTab('home')}
            className={`flex-1 py-3 text-sm sm:text-base font-bold transition-colors ${activeTab === 'home' ? 'bg-white text-blue-700 border-b-2 border-blue-600' : 'text-gray-500 hover:text-gray-800 hover:bg-gray-100'}`}
          >
            {game.teams.home.team.name}
          </button>
        </div>

        {/* CONTENT */}
        <div className="overflow-y-auto p-0 sm:p-4 bg-white flex-1">
          {loading ? (
            <div className="flex justify-center items-center h-64 text-gray-400 font-bold animate-pulse">
              Loading full boxscore...
            </div>
          ) : (
            <div className="space-y-6">
              
              {/* BATTING TABLE */}
              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse min-w-[600px]">
                  <thead>
                    <tr className="bg-gray-100 border-y border-gray-200 text-xs uppercase tracking-wider text-gray-500 font-bold">
                      <th className="px-4 py-2 w-1/2">Batters</th>
                      <th className="px-2 py-2 text-center w-12">AB</th>
                      <th className="px-2 py-2 text-center w-12">R</th>
                      <th className="px-2 py-2 text-center w-12">H</th>
                      <th className="px-2 py-2 text-center w-12 text-blue-700">HR</th>
                      <th className="px-2 py-2 text-center w-12">RBI</th>
                      <th className="px-2 py-2 text-center w-12 text-green-700">SB</th>
                      <th className="px-2 py-2 text-center w-12">BB</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 text-sm">
                    {activeData.batters.map((p, idx) => (
                      <tr key={`${p.id}-${idx}`} className="hover:bg-blue-50/50 transition-colors">
                        <td className="px-4 py-2">
                          <div className={`flex items-center ${p.isSub ? 'pl-6' : ''}`}>
                            <span className="font-bold text-gray-800">{p.name}</span>
                            <span className="text-[10px] text-gray-400 ml-1.5">{p.position}</span>
                            <FantasyBadge team={p.fantasyTeam} isBench={p.isBench} />
                          </div>
                        </td>
                        <td className="px-2 py-2 text-center text-gray-600">{p.stats.atBats ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-gray-800">{p.stats.runs ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-gray-800">{p.stats.hits ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-blue-700">{p.stats.homeRuns ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-gray-800">{p.stats.rbi ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-green-700">{p.stats.stolenBases ?? '-'}</td>
                        <td className="px-2 py-2 text-center text-gray-600">{p.stats.baseOnBalls ?? '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

{/* PITCHING TABLE */}
              <div className="overflow-x-auto mt-4 border-t-4 border-gray-100 pt-4">
                <table className="w-full text-left border-collapse min-w-[600px]">
                  <thead>
                    <tr className="bg-gray-100 border-y border-gray-200 text-xs uppercase tracking-wider text-gray-500 font-bold">
                      <th className="px-4 py-2 w-1/2">Pitchers</th>
                      <th className="px-2 py-2 text-center w-12">IP</th>
                      <th className="px-2 py-2 text-center w-12">H</th>
                      <th className="px-2 py-2 text-center w-12">R</th>
                      <th className="px-2 py-2 text-center w-12">ER</th>
                      <th className="px-2 py-2 text-center w-12">BB</th>
                      <th className="px-2 py-2 text-center w-12">K</th>
                      <th className="px-2 py-2 text-center w-12 text-purple-700">QS</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 text-sm">
                    {activeData.pitchers.map((p, idx) => (
                      <tr key={`${p.id}-${idx}`} className="hover:bg-blue-50/50 transition-colors">
                        <td className="px-4 py-2">
                          <div className="flex items-center">
                            <span className="font-bold text-gray-800">{p.name}</span>
                            <FantasyBadge team={p.fantasyTeam} isBench={p.isBench} />
                          </div>
                        </td>
                        <td className="px-2 py-2 text-center font-bold text-gray-800">{p.stats.inningsPitched ?? '-'}</td>
                        <td className="px-2 py-2 text-center text-gray-600">{p.stats.hits ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-gray-800">{p.stats.runs ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-gray-800">{p.stats.earnedRuns ?? '-'}</td>
                        <td className="px-2 py-2 text-center text-gray-600">{p.stats.baseOnBalls ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-blue-700">{p.stats.strikeOuts ?? '-'}</td>
                        <td className="px-2 py-2 text-center font-bold text-purple-700">{p.stats.qs ? '1' : '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

            </div>
          )}
        </div>

      </div>
    </div>
  );
}