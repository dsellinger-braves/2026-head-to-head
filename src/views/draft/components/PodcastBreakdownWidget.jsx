import { useState } from 'react';

export default function PodcastBreakdownWidget({
  script,
  isGenerating,
  isPlaying,
  activeLine,
  onPlay,
  onStop,
  compact = false,
  playerName = null
}) {
  const [showScript, setShowScript] = useState(true);

  if (!script && !isGenerating) return null;

  return (
    <div style={{
      marginTop: '12px',
      background: 'linear-gradient(135deg, rgba(30, 20, 50, 0.7) 0%, rgba(20, 20, 35, 0.85) 100%)',
      borderRadius: '12px',
      border: '1px solid rgba(187, 134, 252, 0.35)',
      padding: compact ? '10px 12px' : '14px 16px',
      boxShadow: '0 4px 20px rgba(0, 0, 0, 0.4)',
      position: 'relative',
      overflow: 'hidden'
    }}>
      <style>{`
        @keyframes soundWave {
          0%, 100% { height: 4px; }
          50% { height: 16px; }
        }
        @keyframes shimmerPulse {
          0%, 100% { opacity: 0.6; }
          50% { opacity: 1; }
        }
      `}</style>
      
      {/* Header bar */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{
            fontSize: compact ? '11px' : '12px',
            color: '#bb86fc',
            fontWeight: 'bold',
            letterSpacing: '1px',
            textTransform: 'uppercase',
            display: 'flex',
            alignItems: 'center',
            gap: '6px'
          }}>
            🎙️ Draft Broadcast & Skip / Chip Debate
          </span>
          <span style={{
            fontSize: '9px',
            background: 'rgba(3, 218, 198, 0.15)',
            border: '1px solid rgba(3, 218, 198, 0.4)',
            color: '#03dac6',
            padding: '1px 6px',
            borderRadius: '4px',
            fontWeight: 'bold'
          }}>
            Google Cloud Broadcast Voices
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          {/* Waveform indicator if playing */}
          {isPlaying && (
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: '3px', height: '18px', padding: '0 4px' }} title="Speaking via Google Cloud Voice Engine">
              <span style={{ width: '3px', background: '#00e5ff', borderRadius: '2px', animation: 'soundWave 0.7s ease-in-out infinite' }} />
              <span style={{ width: '3px', background: '#ff9800', borderRadius: '2px', animation: 'soundWave 0.8s ease-in-out infinite 0.1s' }} />
              <span style={{ width: '3px', background: '#bb86fc', borderRadius: '2px', animation: 'soundWave 0.6s ease-in-out infinite 0.2s' }} />
              <span style={{ width: '3px', background: '#03dac6', borderRadius: '2px', animation: 'soundWave 0.9s ease-in-out infinite 0.3s' }} />
            </div>
          )}

          {/* Play / Stop Button */}
          {isPlaying ? (
            <button
              onClick={onStop}
              style={{
                background: 'linear-gradient(135deg, #e53935 0%, #c62828 100%)',
                color: '#fff',
                border: 'none',
                borderRadius: '6px',
                padding: '4px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '5px',
                boxShadow: '0 2px 8px rgba(229, 57, 53, 0.4)'
              }}
            >
              ⏹ Stop Broadcast
            </button>
          ) : (
            <button
              onClick={onPlay}
              disabled={isGenerating || !script}
              style={{
                background: isGenerating ? '#444' : 'linear-gradient(135deg, #00b0ff 0%, #7c4dff 100%)',
                color: '#fff',
                border: 'none',
                borderRadius: '6px',
                padding: '4px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: isGenerating ? 'not-allowed' : 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '5px',
                boxShadow: '0 2px 8px rgba(0, 176, 255, 0.3)'
              }}
            >
              ▶ Play Broadcast
            </button>
          )}

          {/* Toggle Script */}
          {script && (
            <button
              onClick={() => setShowScript(prev => !prev)}
              style={{
                background: 'rgba(255, 255, 255, 0.08)',
                color: '#aaa',
                border: '1px solid #444',
                borderRadius: '6px',
                padding: '4px 8px',
                fontSize: '11px',
                cursor: 'pointer'
              }}
            >
              {showScript ? 'Hide Script' : 'View Script'}
            </button>
          )}
        </div>
      </div>

      {/* Generating State */}
      {isGenerating && (
        <div style={{
          color: '#bbb',
          fontSize: '12px',
          fontStyle: 'italic',
          padding: '12px',
          background: 'rgba(0, 0, 0, 0.3)',
          borderRadius: '8px',
          textAlign: 'center',
          animation: 'shimmerPulse 1.5s ease-in-out infinite'
        }}>
          🎙️ Preparing draft announcement and Skip & Chip reaction {playerName ? `for ${playerName}` : 'for this pick'}...
        </div>
      )}

      {/* Script Lines */}
      {script && showScript && !isGenerating && (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          gap: '8px',
          maxHeight: compact ? '170px' : '240px',
          overflowY: 'auto',
          paddingRight: '4px'
        }}>
          {script.map((line, idx) => {
            const speaker = (line.speaker || '').toUpperCase().trim();
            const isAnnouncer = speaker === 'ANNOUNCER' || speaker === 'COMMISSIONER';
            const isChip = speaker === 'CHIP';
            const isActive = activeLine === idx;

            if (isAnnouncer) {
              return (
                <div
                  key={idx}
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignSelf: 'stretch',
                    background: isActive
                      ? 'rgba(0, 229, 255, 0.22)'
                      : 'rgba(15, 23, 42, 0.75)',
                    border: isActive ? '1px solid #00e5ff' : '1px solid rgba(0, 229, 255, 0.3)',
                    borderLeft: `4px solid ${isActive ? '#00e5ff' : '#0288d1'}`,
                    borderRadius: '8px',
                    padding: '8px 12px',
                    transition: 'all 0.25s ease',
                    boxShadow: isActive
                      ? '0 0 16px rgba(0, 229, 255, 0.45)'
                      : 'none'
                  }}
                >
                  <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    marginBottom: '3px'
                  }}>
                    <span style={{
                      fontSize: '10px',
                      fontWeight: '900',
                      letterSpacing: '0.6px',
                      color: '#00e5ff',
                      textTransform: 'uppercase'
                    }}>
                      📢 Official Pick Announcement
                    </span>
                    {isActive && (
                      <span style={{
                        fontSize: '9px',
                        background: '#00e5ff',
                        color: '#000',
                        padding: '0 4px',
                        borderRadius: '3px',
                        fontWeight: 'bold',
                        letterSpacing: '0.5px'
                      }}>
                        ANNOUNCING
                      </span>
                    )}
                  </div>
                  <div style={{
                    fontSize: compact ? '12px' : '13px',
                    color: isActive ? '#fff' : '#e0f7fa',
                    fontWeight: '500',
                    lineHeight: '1.45'
                  }}>
                    {line.text}
                  </div>
                </div>
              );
            }

            return (
              <div
                key={idx}
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignSelf: isChip ? 'flex-end' : 'flex-start',
                  maxWidth: '88%',
                  background: isActive
                    ? (isChip ? 'rgba(187, 134, 252, 0.25)' : 'rgba(255, 152, 0, 0.25)')
                    : 'rgba(0, 0, 0, 0.4)',
                  borderLeft: isChip ? 'none' : `3px solid ${isActive ? '#ffb74d' : '#ff9800'}`,
                  borderRight: isChip ? `3px solid ${isActive ? '#d1c4e9' : '#bb86fc'}` : 'none',
                  borderRadius: isChip ? '10px 4px 10px 10px' : '4px 10px 10px 10px',
                  padding: '8px 12px',
                  transition: 'all 0.25s ease',
                  boxShadow: isActive
                    ? `0 0 14px ${isChip ? 'rgba(187, 134, 252, 0.4)' : 'rgba(255, 152, 0, 0.4)'}`
                    : 'none'
                }}
              >
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  marginBottom: '3px',
                  justifyContent: isChip ? 'flex-end' : 'flex-start'
                }}>
                  <span style={{
                    fontSize: '10px',
                    fontWeight: '900',
                    letterSpacing: '0.5px',
                    color: isChip ? '#bb86fc' : '#ff9800',
                    textTransform: 'uppercase'
                  }}>
                    {isChip ? '🔥 CHIP (Hype Man)' : '🎙️ SKIP (Skeptical Analyst)'}
                  </span>
                  {isActive && (
                    <span style={{
                      fontSize: '9px',
                      background: isChip ? '#bb86fc' : '#ff9800',
                      color: '#000',
                      padding: '0 4px',
                      borderRadius: '3px',
                      fontWeight: 'bold'
                    }}>
                      SPEAKING
                    </span>
                  )}
                </div>
                <div style={{
                  fontSize: compact ? '12px' : '13px',
                  color: isActive ? '#fff' : '#ddd',
                  lineHeight: '1.45'
                }}>
                  {line.text}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
