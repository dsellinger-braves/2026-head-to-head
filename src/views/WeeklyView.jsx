import { useState } from 'react';
import MatchupCard from '../components/MatchupCard';
import BoxScoreModal from '../components/BoxScoreModal';
import PlayerHistoryModal from '../components/PlayerHistoryModal';
import { TEAMS } from '../schedule';

export default function WeeklyView({ processedWeeks, allStats, onOwnerClick }) {
  const [viewMode, setViewMode] = useState('byWeek');
  const [selectedWeekId, setSelectedWeekId] = useState(1);
  const [selectedTeamId, setSelectedTeamId] = useState(1);
  const [selectedMatchup, setSelectedMatchup] = useState(null); 
  const [selectedPlayer, setSelectedPlayer] = useState(null); 

  // --- HELPER: Format Date ---
  const formatDate = (dateStr) => {
    if (!dateStr) return '';
    const [year, month, day] = dateStr.split('-');
    return `${parseInt(month)}/${parseInt(day)}`;
  };

  // --- NAVIGATION HELPERS ---
  const handlePrevWeek = () => {
    if (selectedWeekId > 1) setSelectedWeekId(Number(selectedWeekId) - 1);
  };

  const handleNextWeek = () => {
    if (selectedWeekId < processedWeeks.length) setSelectedWeekId(Number(selectedWeekId) + 1);
  };

  // --- FILTERING LOGIC ---
  const displayedMatchups = processedWeeks.flatMap(week => {
    if (viewMode === 'byWeek' && week.weekId !== parseInt(selectedWeekId)) return [];
    
    const teamMatchups = week.matchups.filter(m => {
      if (viewMode === 'byWeek') return true;
      
      // Handle Trio filtering
      if (m.type === 'trio') {
         if (m.teams) return m.teams.some(t => t.id == selectedTeamId);
         if (m.teamIds) return m.teamIds.includes(parseInt(selectedTeamId));
         return false;
      }
      
      // Handle H2H filtering
      return m.homeTeam?.id == selectedTeamId || m.awayTeam?.id == selectedTeamId;
    });

    return teamMatchups.map(m => ({ 
      ...m, 
      weekName: week.name,
      startDate: week.startDate,
      endDate: week.endDate
    }));
  });

  return (
    <>
      <div className="space-y-6">
        {/* --- CONTROLS BAR --- */}
        <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-200 flex flex-col md:flex-row justify-between items-center gap-4">
          
          {/* View Toggle */}
          <div className="flex bg-gray-100 p-1 rounded-lg">
            <button
              onClick={() => setViewMode('byWeek')}
              className={`px-4 py-2 text-sm font-bold rounded-md transition-all ${viewMode === 'byWeek' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
            >
              League Schedule
            </button>
            <button
              onClick={() => setViewMode('byTeam')}
              className={`px-4 py-2 text-sm font-bold rounded-md transition-all ${viewMode === 'byTeam' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
            >
              Team Schedule
            </button>
          </div>

          {/* Context Controls */}
          <div className="flex items-center gap-2">
            {viewMode === 'byWeek' ? (
              <>
                <button 
                  onClick={handlePrevWeek}
                  disabled={selectedWeekId <= 1}
                  className="p-2 rounded-full hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  <svg className="w-5 h-5 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M15 19l-7-7 7-7" /></svg>
                </button>
                
                <div className="flex flex-col items-center">
                   <select 
                    value={selectedWeekId} 
                    onChange={(e) => setSelectedWeekId(e.target.value)}
                    className="block w-48 rounded-lg border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm font-medium py-2 bg-gray-50 text-center"
                  >
                    {processedWeeks.map(w => <option key={w.weekId} value={w.weekId}>{w.name}</option>)}
                  </select>
                  <span className="text-[10px] text-gray-400 font-mono mt-1">
                    {formatDate(processedWeeks[selectedWeekId-1]?.startDate)} - {formatDate(processedWeeks[selectedWeekId-1]?.endDate)}
                  </span>
                </div>

                <button 
                  onClick={handleNextWeek}
                  disabled={selectedWeekId >= processedWeeks.length}
                  className="p-2 rounded-full hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                   <svg className="w-5 h-5 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 5l7 7-7 7" /></svg>
                </button>
              </>
            ) : (
              <select 
                value={selectedTeamId} 
                onChange={(e) => setSelectedTeamId(e.target.value)}
                className="block w-64 rounded-lg border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-sm font-medium py-2.5 bg-gray-50"
              >
                {Object.keys(TEAMS).map(id => <option key={id} value={id}>{TEAMS[id].name}</option>)}
              </select>
            )}
          </div>
        </div>

        {/* --- GRID OF MATCHUPS --- */}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
          {displayedMatchups.length === 0 ? (
            <div className="col-span-full text-center py-12 text-gray-400">No matchups found for this selection.</div>
          ) : (
            displayedMatchups.map((m, i) => (
              <div key={i} className="flex flex-col gap-2">
                {viewMode === 'byTeam' && (
                  <div className="text-center mb-1">
                    <div className="text-xs font-bold text-gray-400 uppercase tracking-wider">{m.weekName}</div>
                    <div className="text-[10px] text-gray-400 font-mono">
                      {formatDate(m.startDate)} - {formatDate(m.endDate)}
                    </div>
                  </div>
                )}
                
                {/* FIX: Passing the entire 'm' object intact! */}
                <MatchupCard 
                  matchup={m}
                  onViewBoxScore={() => setSelectedMatchup(m)}
                  onOwnerClick={onOwnerClick}
                />
              </div>
            ))
          )}
        </div>
      </div>

      {/* --- MODALS --- */}
      {selectedMatchup && (
        <BoxScoreModal 
          matchup={selectedMatchup} 
          onClose={() => setSelectedMatchup(null)} 
          onPlayerClick={(id, name) => setSelectedPlayer({ id, name })}
        />
      )}

      {selectedPlayer && (
        <PlayerHistoryModal 
          playerId={selectedPlayer.id}
          playerName={selectedPlayer.name}
          allStats={allStats} 
          onClose={() => setSelectedPlayer(null)}
        />
      )}
    </>
  );
}