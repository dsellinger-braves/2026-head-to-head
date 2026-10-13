import styles from '../styles/draftRoomStyles';
import { CountdownTimer } from './DraftWidgets';

/**
 * DesktopHeader
 * Top navigation and status bar for desktop War Room.
 */
export default function DesktopHeader({
  roomSeason,
  handleSeasonChange,
  draftMode,
  handleResetDraft,
  resetting,
  handleResetMode,
  handleResetUser,
  currentUser,
  currentPick,
  audioEnabled,
  setAudioEnabled,
  onTestAudio,
  playOwnerSound,
  pickStartTime,
  onSwitchView
}) {
  return (
    <div style={styles.header}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
        <div style={styles.leagueLogo}>
          ⚾ Hefty War Room {roomSeason}
        </div>

        {/* In-Room Season Switcher */}
        <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
          <button
            onClick={() => handleSeasonChange(2027)}
            style={{
              background: roomSeason === 2027 ? '#bb86fc' : 'transparent',
              color: roomSeason === 2027 ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '3px 9px',
              fontSize: '11px',
              fontWeight: 'bold',
              cursor: 'pointer'
            }}
          >
            🚀 2027 Draft
          </button>
          <button
            onClick={() => handleSeasonChange(2026)}
            style={{
              background: roomSeason === 2026 ? '#03dac6' : 'transparent',
              color: roomSeason === 2026 ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '3px 9px',
              fontSize: '11px',
              fontWeight: 'bold',
              cursor: 'pointer'
            }}
          >
            🏛️ 2026 Archive
          </button>
        </div>
        {draftMode === 'test' && (
          <span style={{ 
            fontSize: '12px', 
            background: '#ffc10722', 
            color: '#ffc107', 
            border: '1px solid #ffc107', 
            padding: '3px 8px', 
            borderRadius: '4px', 
            fontWeight: 'bold' 
          }}>
            [TEST]
          </span>
        )}
        {draftMode === 'multitest' && (
          <span style={{ 
            fontSize: '12px', 
            background: '#ff980022', 
            color: '#ff9800', 
            border: '1px solid #ff9800', 
            padding: '3px 8px', 
            borderRadius: '4px', 
            fontWeight: 'bold' 
          }}>
            [MULTIPLAYER TEST]
          </span>
        )}
        {draftMode === 'mockdraft' && (
          <span style={{ 
            fontSize: '12px', 
            background: '#bb86fc22', 
            color: '#bb86fc', 
            border: '1px solid #bb86fc', 
            padding: '3px 8px', 
            borderRadius: '4px', 
            fontWeight: 'bold' 
          }}>
            [MOCK DRAFT]
          </span>
        )}
        {draftMode === 'multitest' && (
          <button
            onClick={handleResetDraft}
            disabled={resetting}
            style={{
              background: resetting ? '#666' : '#f44336',
              color: '#fff',
              border: 'none',
              padding: '4px 10px',
              borderRadius: '4px',
              cursor: resetting ? 'wait' : 'pointer',
              fontSize: '11px',
              fontWeight: 'bold'
            }}
          >
            {resetting ? 'Resetting...' : '🔄 Reset Draft'}
          </button>
        )}
        <button
          onClick={handleResetMode}
          style={{
            background: 'transparent',
            color: '#888',
            border: '1px solid #444',
            padding: '4px 8px',
            borderRadius: '4px',
            fontSize: '11px',
            cursor: 'pointer'
          }}
          title="Switch between Live and Test Mode"
        >
          Switch Mode
        </button>
        <button
          onClick={handleResetUser}
          style={{
            background: 'transparent',
            color: '#888',
            border: '1px solid #444',
            padding: '4px 8px',
            borderRadius: '4px',
            fontSize: '11px',
            cursor: 'pointer'
          }}
          title="Change active team"
        >
          Switch Team ({currentUser})
        </button>
      </div>

      <div style={{ textAlign: 'center', color: '#fff' }}>
        <div style={{ fontSize: '11px', color: '#888', letterSpacing: '1px' }}>ON THE CLOCK</div>
        <div style={{ fontSize: '22px', color: 'var(--highlight)', fontWeight: 'bold' }}>
          {currentPick ? currentPick.Owner : 'DRAFT COMPLETE'}
        </div>
      </div>

      <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
        <button
          onClick={() => setAudioEnabled(!audioEnabled)}
          style={{
            background: audioEnabled ? '#4caf50' : '#444',
            color: '#fff',
            border: 'none',
            padding: '8px 14px',
            borderRadius: '6px',
            cursor: 'pointer',
            fontSize: '13px',
            fontWeight: 'bold',
            display: 'flex',
            alignItems: 'center',
            gap: '6px'
          }}
          title="Toggle draft audio announcements"
        >
          <span>{audioEnabled ? '🔊' : '🔇'}</span>
          <span>Audio {audioEnabled ? 'ON' : 'OFF'}</span>
        </button>

        {draftMode === 'test' && (
          <button
            onClick={() => {
              if (!audioEnabled) {
                alert('Please enable audio first by clicking the Audio ON button');
                return;
              }
              if (onTestAudio) {
                onTestAudio();
              } else if (playOwnerSound) {
                playOwnerSound(currentUser);
              }
            }}
            style={{
              background: '#ffc107',
              color: '#000',
              border: 'none',
              padding: '8px 12px',
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '13px',
              fontWeight: 'bold'
            }}
          >
            🧪 Test Audio
          </button>
        )}

        <CountdownTimer pickStartTime={pickStartTime} />

        {onSwitchView && (
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            <button
              onClick={() => onSwitchView('draftprep')}
              style={{
                background: '#451a03',
                color: '#fcd34d',
                border: '1px solid #d97706',
                padding: '8px 12px',
                borderRadius: '6px',
                fontSize: '13px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                cursor: 'pointer'
              }}
              title="Open Custom Draft Prep & Positional Tier Sheets"
            >
              <span>📋</span>
              <span>Prep & Tiers</span>
            </button>
            <button
              onClick={() => onSwitchView('capital')}
              style={{
                background: '#2e1065',
                color: '#d8b4fe',
                border: '1px solid #7e22ce',
                padding: '8px 12px',
                borderRadius: '6px',
                fontSize: '13px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                cursor: 'pointer'
              }}
              title="View 2027 Draft Capital & Traded Picks"
            >
              <span>🎟️</span>
              <span>Draft Capital</span>
            </button>
            <button
              onClick={() => onSwitchView('keepers')}
              style={{
                background: '#064e3b',
                color: '#6ee7b7',
                border: '1px solid #059669',
                padding: '8px 12px',
                borderRadius: '6px',
                fontSize: '13px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                cursor: 'pointer'
              }}
              title="View Keepers & Budgets Panel"
            >
              <span>💎</span>
              <span>Keepers & Budget</span>
            </button>
            <button
              onClick={() => onSwitchView('weekly')}
              style={{
                background: '#1e3a8a',
                color: '#fff',
                border: '1px solid #3b82f6',
                padding: '8px 12px',
                borderRadius: '6px',
                fontSize: '13px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                cursor: 'pointer'
              }}
              title="Return to Main League Site"
            >
              <span>⚾</span>
              <span>League Site</span>
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
