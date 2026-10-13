import styles from '../styles/draftRoomStyles';
import { useState, useEffect, useCallback } from 'react';
import PlayerPoolPanel from './PlayerPoolPanel';
import RosterManagerPanel from './RosterManagerPanel';
import MyPicksPanel from './MyPicksPanel';
import DraftLogPanel from './DraftLogPanel';
import AnalysisHistoryPanel from './AnalysisHistoryPanel';
import StandingsPanel from './StandingsPanel';
import TierBoard from '../../../components/draft/TierBoard';
import {
  loadUserTiers,
  assignPlayerTier,
  togglePlayerTag,
  setPlayerNote
} from '../../../utils/tierStorageService';

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
  const [tierState, setTierState] = useState(() => loadUserTiers(roomSeason, currentUser));

  useEffect(() => {
    setTierState(loadUserTiers(roomSeason, currentUser));
  }, [roomSeason, currentUser]);

  useEffect(() => {
    const handleUpdate = () => {
      setTierState(loadUserTiers(roomSeason, currentUser));
    };
    window.addEventListener('hefty_tiers_updated', handleUpdate);
    return () => window.removeEventListener('hefty_tiers_updated', handleUpdate);
  }, [roomSeason, currentUser]);

  const handleMovePlayerToTier = useCallback((playerId, targetTierId) => {
    const updated = assignPlayerTier(roomSeason, currentUser, playerId, targetTierId);
    if (updated) setTierState({ ...updated });
  }, [roomSeason, currentUser]);

  const handleToggleTag = useCallback((playerId, tagId) => {
    const updated = togglePlayerTag(roomSeason, currentUser, playerId, tagId);
    if (updated) setTierState({ ...updated });
  }, [roomSeason, currentUser]);

  const handleUpdateNote = useCallback((playerId, noteText) => {
    const updated = setPlayerNote(roomSeason, currentUser, playerId, noteText);
    if (updated) setTierState({ ...updated });
  }, [roomSeason, currentUser]);

  if (!isOpen) return null;

  return (
    <div style={styles.warRoomPanel}>
      <div style={styles.panelNav}>
        {['Pool', 'Tiers', 'Roster', 'MyPicks', 'DraftLog', 'Analysis', 'Standings'].map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={activeTab === tab ? styles.navBtnActive : styles.navBtn}
          >
            {tab === 'Pool' ? 'Draft Pool' : 
             tab === 'Tiers' ? '🧱 My Tiers' :
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

      <div style={{ ...styles.panelContent, display: activeTab === 'Tiers' ? 'block' : 'none', overflowY: 'auto' }}>
        <TierBoard
          players={displayPlayers}
          tiers={tierState.tiers}
          playerTiers={tierState.playerTiers}
          playerTags={tierState.playerTags}
          playerNotes={tierState.playerNotes}
          queue={queue}
          onMovePlayerToTier={handleMovePlayerToTier}
          onToggleTag={handleToggleTag}
          onUpdateNote={handleUpdateNote}
          onAddToQueue={addToQueue}
          onRemoveFromQueue={removeFromQueue}
          onOpenPlayerModal={setSelectedPlayer}
          isLiveDraft={true}
          isMyTurn={isMyTurn}
          onDraftPlayer={handleDraft}
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
