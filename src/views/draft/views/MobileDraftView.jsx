import styles from '../styles/draftRoomStyles';
import { CountdownTimer, PositionalRunRadar } from '../components/DraftWidgets';
import PlayerPoolPanel from '../components/PlayerPoolPanel';
import RosterManagerPanel from '../components/RosterManagerPanel';
import PlayerModal from '../components/PlayerModal';

/**
 * MobileDraftView
 * Compact smartphone-optimized layout with bottom navigation tabs (Pool, Queue, Roster, Feed).
 */
export default function MobileDraftView({
  roomSeason,
  isMyTurn,
  currentPick,
  pickStartTime,
  handleResetMode,
  onSwitchView,
  activePositionalRun,
  activeTab,
  setActiveTab,
  displayPlayers = [],
  handleDraft,
  queue = [],
  addToQueue,
  removeFromQueue,
  draftMode,
  testModePicks = [],
  displayPicks = [],
  selectedPlayer,
  setSelectedPlayer,
  playerInfo,
  currentUser,
  smartRecommendations = [],
  recentPicks = [],
  onOpenPlayerModal,
  draftHistory = [],
  historicalFinish = [],
  savantBatters = [],
  savantPitchers = [],
  battersZips = [],
  pitchersZips = []
}) {
  return (
    <div style={{
      ...styles.body,
      gridTemplateColumns: '1fr',
      gridTemplateRows: '50px 1fr 50px',
      height: '100vh'
    }}>
      {/* Mobile Header */}
      <div style={{
        gridColumn: '1 / -1',
        background: '#1f1f1f',
        borderBottom: '2px solid #bb86fc',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '0 15px',
        zIndex: 10
      }}>
        <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#bb86fc' }}>
          HWR '{String(roomSeason).slice(-2)}
        </div>
        <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
          <div style={{
            fontSize: '12px',
            fontWeight: 'bold',
            color: isMyTurn ? '#03dac6' : '#888',
            animation: isMyTurn ? 'pulse 1s infinite' : 'none'
          }}>
            {currentPick ? `${currentPick.Owner} (#${currentPick['Overall Pick']})` : 'COMPLETE'}
          </div>
          <CountdownTimer pickStartTime={pickStartTime} />
          <button
            onClick={handleResetMode}
            style={{ background: 'transparent', color: '#888', border: '1px solid #444', padding: '3px 6px', borderRadius: '4px', fontSize: '10px', cursor: 'pointer' }}
          >
            Mode
          </button>
          {onSwitchView && (
            <button
              onClick={() => onSwitchView('summary')}
              style={{ background: '#1e3a8a', color: '#fff', border: '1px solid #3b82f6', padding: '3px 8px', borderRadius: '4px', fontSize: '10px', cursor: 'pointer', fontWeight: 'bold' }}
            >
              League
            </button>
          )}
        </div>
      </div>

      {/* Mobile Content Area */}
      <div style={{ gridColumn: '1 / -1', overflow: 'hidden', background: '#121212', padding: '10px' }}>
        {activePositionalRun && (
          <div style={{ marginBottom: '10px' }}>
            <PositionalRunRadar run={activePositionalRun} />
          </div>
        )}
        {activeTab === 'Pool' && (
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
            isMobile={true}
            currentUser={currentUser}
            recommendations={smartRecommendations}
          />
        )}
        {activeTab === 'Queue' && (
          <div style={{ height: '100%', overflowY: 'auto' }}>
            <div style={styles.wrHeader}>My Queue ({queue.length})</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              {queue.length === 0 ? (
                <div style={{ color: '#888', textAlign: 'center', padding: '20px' }}>No players in queue. Star players in the pool to add them here!</div>
              ) : (
                queue.map(p => (
                  <div key={p['ESPN PlayerID']} style={{ ...styles.queueItem, background: '#222' }}>
                    <div>
                      <div style={{ color: '#fff', fontWeight: 'bold' }}>{p.Player}</div>
                      <div style={{ fontSize: '12px', color: '#888' }}>{p.Position} - {p.Team}</div>
                    </div>
                    <button onClick={() => removeFromQueue(p['ESPN PlayerID'])} style={styles.btnStarActive}>✕</button>
                  </div>
                ))
              )}
            </div>
          </div>
        )}
        {activeTab === 'Roster' && (
          <RosterManagerPanel
            allPicks={displayPicks}
            players={displayPlayers}
            currentUser={currentUser}
          />
        )}
        {activeTab === 'Feed' && (
          <div style={{ height: '100%', overflowY: 'auto' }}>
            <div style={styles.wrHeader}>Draft Feed</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              {recentPicks.slice().reverse().map(p => {
                const playerObj = displayPlayers.find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID']));
                return (
                  <div key={p['Overall Pick']} style={{
                    padding: '10px',
                    background: '#222',
                    borderRadius: '4px',
                    borderLeft: `4px solid ${p.Owner === currentUser ? '#03dac6' : '#555'}`
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', color: '#888', fontSize: '11px' }}>
                      <span>Pick #{p['Overall Pick']}</span>
                      <span>{p.Owner}</span>
                    </div>
                    <div style={{ color: '#fff', fontWeight: 'bold', fontSize: '14px' }}>
                      {playerObj?.Player || p.Selection || 'Pick Made'}
                    </div>
                    <div style={{ color: '#aaa', fontSize: '12px' }}>
                      {playerObj?.Position} - {playerObj?.Team}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {/* Mobile Bottom Nav */}
      <div style={{
        gridColumn: '1 / -1',
        background: '#2c2c2c',
        borderTop: '1px solid #444',
        display: 'flex',
        justifyContent: 'space-around',
        alignItems: 'center'
      }}>
        {[
          { id: 'Pool', label: 'Pool', icon: '📋' },
          { id: 'Queue', label: 'Queue', icon: '⭐' },
          { id: 'Roster', label: 'Roster', icon: '👥' },
          { id: 'Feed', label: 'Feed', icon: '📢' }
        ].map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              background: 'none',
              border: 'none',
              color: activeTab === tab.id ? '#03dac6' : '#888',
              padding: '8px',
              fontSize: '11px',
              fontWeight: 'bold',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              cursor: 'pointer'
            }}
          >
            <span style={{ fontSize: '18px', marginBottom: '2px' }}>{tab.icon}</span>
            {tab.label}
          </button>
        ))}
      </div>

      {selectedPlayer && (
        <PlayerModal
          player={selectedPlayer}
          onClose={() => setSelectedPlayer(null)}
          onOpenInDepthModal={onOpenPlayerModal}
          players={displayPlayers}
          draftHistory={draftHistory}
          historicalFinish={historicalFinish}
          savantBatters={savantBatters}
          savantPitchers={savantPitchers}
          battersZips={battersZips}
          pitchersZips={pitchersZips}
          isMobile={true}
        />
      )}
    </div>
  );
}
