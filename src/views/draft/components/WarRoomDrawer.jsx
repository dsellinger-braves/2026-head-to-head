import styles from '../styles/draftRoomStyles';
import PlayerPoolPanel from './PlayerPoolPanel';
import RosterManagerPanel from './RosterManagerPanel';
import MyPicksPanel from './MyPicksPanel';
import DraftLogPanel from './DraftLogPanel';
import AnalysisHistoryPanel from './AnalysisHistoryPanel';
import StandingsPanel from './StandingsPanel';

/**
 * WarRoomDrawer
 * Bottom slide-up drawer containing Draft Pool, Roster Manager, My Picks, Draft Log,
 * Analysis History, and Projected Roto Standings tabs.
 */
export default function WarRoomDrawer({
  isOpen,
  activeTab,
  setActiveTab,
  currentUser,
  displayPlayers,
  handleDraft,
  isMyTurn,
  queue,
  addToQueue,
  removeFromQueue,
  draftMode,
  testModePicks,
  displayPicks,
  setSelectedPlayer,
  playerInfo,
  smartRecommendations,
  draftTrades,
  roomSeason,
  analysisHistory,
  keepers
}) {
  if (!isOpen) return null;

  return (
    <div style={styles.warRoomPanel}>
      <div style={styles.panelNav}>
        {['Pool', 'Roster', 'MyPicks', 'DraftLog', 'Analysis', 'Standings'].map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={activeTab === tab ? styles.navBtnActive : styles.navBtn}
          >
            {tab === 'Pool' ? 'Draft Pool' : 
             tab === 'Roster' ? 'Roster Manager' : 
             tab === 'MyPicks' ? 'My Picks' :
             tab === 'DraftLog' ? 'Draft Log' :
             tab === 'Analysis' ? 'Analysis History' :
             '🏆 Projected Roto'}
          </button>
        ))}
        <div style={{ marginLeft: 'auto', color: '#888', fontSize: '13px', display: 'flex', alignItems: 'center' }}>
          Logged in as: <span style={{ color: 'var(--highlight)', fontWeight: 'bold', marginLeft: '5px' }}>{currentUser}</span>
        </div>
      </div>

      <div style={{ ...styles.panelContent, display: activeTab === 'Pool' ? 'grid' : 'none' }}>
        <PlayerPoolPanel
          players={displayPlayers}
          onDraft={handleDraft}
          isMyTurn={isMyTurn}
          queue={queue}
          onAddToQueue={addToQueue}
          onRemoveFromQueue={removeFromQueue}
          draftMode={draftMode}
          testModePicks={testModePicks}
          allPicks={displayPicks}
          onPlayerClick={setSelectedPlayer}
          playerInfo={playerInfo}
          currentUser={currentUser}
          recommendations={smartRecommendations}
        />
      </div>

      <div style={{ ...styles.panelContent, display: activeTab === 'Roster' ? 'grid' : 'none' }}>
        <RosterManagerPanel
          allPicks={displayPicks}
          players={displayPlayers}
          currentUser={currentUser}
        />
      </div>

      <div style={{ ...styles.panelContent, display: activeTab === 'MyPicks' ? 'grid' : 'none' }}>
        <MyPicksPanel
          allPicks={displayPicks}
          players={displayPlayers}
          currentUser={currentUser}
          draftTrades={draftTrades}
          roomSeason={roomSeason}
        />
      </div>

      <div style={{ ...styles.panelContent, display: activeTab === 'DraftLog' ? 'grid' : 'none' }}>
        <DraftLogPanel
          allPicks={displayPicks}
          players={displayPlayers}
          onPlayerClick={setSelectedPlayer}
        />
      </div>

      <div style={{ ...styles.panelContent, display: activeTab === 'Analysis' ? 'grid' : 'none' }}>
        <AnalysisHistoryPanel
          analysisHistory={analysisHistory}
          players={displayPlayers}
          roomSeason={roomSeason}
        />
      </div>

      <div style={{ ...styles.panelContent, display: activeTab === 'Standings' ? 'grid' : 'none' }}>
        <StandingsPanel 
          allPicks={displayPicks}
          keepers={keepers}
          roomSeason={roomSeason}
        />
      </div>
    </div>
  );
}
