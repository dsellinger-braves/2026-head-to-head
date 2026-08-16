import { useEffect, useState, useMemo } from 'react';
import { fetchLiveScoreboard, normalizeName, buildRosterDictionary } from '../utils/liveMLB';
import { TEAMS } from '../schedule';
import TeamAvatar from '../components/TeamAvatar';

import GameDetailModal from '../components/GameDetailModal';

export default function LiveScoreboardView({ todaysRecords }) {
  const [games, setGames] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);

  // --- NEW STATE FOR MODAL ---
  const [selectedGame, setSelectedGame] = useState(null);

  const rosterDict = useMemo(() => buildRosterDictionary(todaysRecords), [todaysRecords]);

  const loadGames = async () => {
    setLoading(true);
    const liveGames = await fetchLiveScoreboard();
    setGames(liveGames);
    setLastUpdated(new Date());
    setLoading(false);
  };

  useEffect(() => {
    loadGames();
    const interval = setInterval(loadGames, 30000); // Refresh every 30 seconds
    return () => clearInterval(interval);
  }, []);

  const getFantasyOwner = (mlbName) => {
    const cleanName = normalizeName(mlbName);
    const record = rosterDict[cleanName];
    if (record) {
      return { team: TEAMS[record.teamId], isBench: record.isBench };
    }
    return null; 
  };

  if (loading && games.length === 0) {
    return <div className="p-12 text-center text-gray-500 font-bold animate-pulse text-lg">Loading Live MLB Feed...</div>;
  }

  return (
    <div className="space-y-6 animate-fade-in-up pb-12">
      <div className="flex justify-between items-center bg-white p-6 rounded-xl shadow-sm border border-gray-200">
        <div>
          <h2 className="text-2xl font-black text-gray-900 flex items-center gap-2">
            <span className="relative flex h-3 w-3 mr-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-3 w-3 bg-red-500"></span>
            </span>
            Live FantasyCast
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            Matching live MLB action to your league's rosters. Last updated: {lastUpdated?.toLocaleTimeString()}
          </p>
        </div>
        <button onClick={loadGames} className="text-sm bg-blue-100 text-blue-800 px-4 py-2 rounded-lg font-bold hover:bg-blue-200 transition-colors shadow-sm">
          Refresh Now
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
        {games.map(game => {
          const home = game.teams.home;
          const away = game.teams.away;
          const status = game.status.abstractGameState; 
          
          const currentBatter = game.linescore?.offense?.batter;
          const currentPitcher = game.linescore?.defense?.pitcher;
          
          const batterData = currentBatter ? getFantasyOwner(currentBatter.fullName) : null;
          const pitcherData = currentPitcher ? getFantasyOwner(currentPitcher.fullName) : null;

          return (
            <div key={game.gamePk} onClick={() => setSelectedGame(game)} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden hover:border-blue-400 transition-colors flex flex-col">
              
              {/* STATUS BAR */}
              <div className={`text-xs text-center py-1.5 font-bold text-white tracking-wider uppercase ${status === 'Live' ? 'bg-red-600' : 'bg-gray-800'}`}>
                {status === 'Live' ? `${game.linescore?.inningState} ${game.linescore?.currentInning}` : status}
              </div>
              
              {/* SCOREBOARD */}
              <div className="p-5 space-y-3 border-b border-gray-100 flex-1">
                <div className="flex justify-between items-center">
                  <span className="font-bold text-gray-800 text-base">{away.team.name}</span>
                  <span className="font-mono text-xl font-black">{away.score || 0}</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="font-bold text-gray-800 text-base">{home.team.name}</span>
                  <span className="font-mono text-xl font-black">{home.score || 0}</span>
                </div>
              </div>

              {/* LIVE FANTASY IMPACT */}
              {status === 'Live' && (currentPitcher || currentBatter) && (
                <div className="bg-gray-50 p-4 space-y-3 shrink-0">
                   {currentPitcher && (
                     <div className="flex items-center justify-between">
                        <div className="flex flex-col">
                          <span className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Pitching</span>
                          <span className="text-sm font-bold text-gray-800">{currentPitcher.fullName}</span>
                        </div>
                        {pitcherData ? (
                          <div className={`flex items-center gap-2 ${pitcherData.isBench ? 'opacity-50' : ''}`}>
                             <div className="text-right">
                               <div className="text-xs font-bold text-gray-900 leading-tight">{pitcherData.team.name}</div>
                               {pitcherData.isBench && <div className="text-[9px] text-red-500 font-bold uppercase">Bench</div>}
                             </div>
                             <TeamAvatar team={pitcherData.team} size="sm" />
                          </div>
                        ) : (
                          <span className="text-xs font-medium text-gray-400 bg-gray-100 px-2 py-1 rounded">Free Agent</span>
                        )}

                        {/* Helper text on hover */}
              <div className="bg-gray-50 py-2 text-center text-[10px] font-bold text-blue-500 uppercase tracking-wider opacity-0 group-hover:opacity-100 transition-opacity">
                 Click to view all players
              </div>
                     </div>
                   )}
                   
                   {currentBatter && (
                     <div className="flex items-center justify-between border-t border-gray-200 pt-3">
                        <div className="flex flex-col">
                          <span className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Batting</span>
                          <span className="text-sm font-bold text-gray-800">{currentBatter.fullName}</span>
                        </div>
                        {batterData ? (
                          <div className={`flex items-center gap-2 ${batterData.isBench ? 'opacity-50' : ''}`}>
                             <div className="text-right">
                               <div className="text-xs font-bold text-gray-900 leading-tight">{batterData.team.name}</div>
                               {batterData.isBench && <div className="text-[9px] text-red-500 font-bold uppercase">Bench</div>}
                             </div>
                             <TeamAvatar team={batterData.team} size="sm" />
                          </div>
                        ) : (
                          <span className="text-xs font-medium text-gray-400 bg-gray-100 px-2 py-1 rounded">Free Agent</span>
                        )}
                     </div>
                   )}
                </div>
              )}
            </div>
          );
        })}
      </div>
      {/* --- RENDER THE NEW MODAL --- */}
      {selectedGame && (
        <GameDetailModal 
          game={selectedGame} 
          rosterDict={rosterDict} 
          onClose={() => setSelectedGame(null)} 
        />
      )}
    </div>
  );
}