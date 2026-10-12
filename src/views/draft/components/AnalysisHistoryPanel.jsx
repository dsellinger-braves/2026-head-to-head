import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { DRAFT_OWNERS } from '../utils/draftCalculations';
import {
  stopPodcastAudio,
  generateLocalPodcastDialogue,
  playDualSpeakerPodcast,
  playPodcastTTS
} from '../services/draftAudio';

export default function AnalysisHistoryPanel({ analysisHistory, players = [], roomSeason = 2027 }) {
  const [filterOwner, setFilterOwner] = useState('');
  const [searchText, setSearchText] = useState('');
  const [playingPick, setPlayingPick] = useState(null);
  const [activeDebateLine, setActiveDebateLine] = useState(null);
  const [expandedPick, setExpandedPick] = useState(null);

  const filteredHistory = useMemo(() => {
    return analysisHistory.filter(item => {
      const ownerMatch = !filterOwner || item.owner === filterOwner;
      const textMatch = !searchText || 
        item.commentary.toLowerCase().includes(searchText.toLowerCase()) ||
        item.playerName.toLowerCase().includes(searchText.toLowerCase());
      return ownerMatch && textMatch;
    });
  }, [analysisHistory, filterOwner, searchText]);

  const handlePlayDebate = (item) => {
    if (playingPick === item.pickNumber) {
      stopPodcastAudio();
      setPlayingPick(null);
      setActiveDebateLine(null);
      return;
    }
    stopPodcastAudio();

    let script = item.podcastScript;
    if (!script || !Array.isArray(script) || script.length === 0) {
      const pl = players.find(p => p.Player === item.playerName) || {
        Player: item.playerName,
        Position: item.position,
        Team: item.team
      };
      script = generateLocalPodcastDialogue(pl, item.owner, item.pickNumber, {}, roomSeason);
    }

    setPlayingPick(item.pickNumber);
    setExpandedPick(item.pickNumber);

    playDualSpeakerPodcast(script, {
      onLineStart: (idx) => setActiveDebateLine(idx),
      onComplete: () => {
        setPlayingPick(null);
        setActiveDebateLine(null);
      },
      onError: () => {
        setPlayingPick(null);
        setActiveDebateLine(null);
      }
    });
  };

  const getEffectiveItemScript = (item) => {
    if (item.podcastScript && Array.isArray(item.podcastScript) && item.podcastScript.length > 0) {
      return item.podcastScript;
    }
    const pl = players.find(p => p.Player === item.playerName) || {
      Player: item.playerName,
      Position: item.position,
      Team: item.team
    };
    return generateLocalPodcastDialogue(pl, item.owner, item.pickNumber, {}, roomSeason);
  };

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1', minHeight: 0 }}>
      <div style={styles.wrHeader}>
        <span>Analysis History ({analysisHistory.length} picks analyzed)</span>
        <div style={{ display: 'flex', gap: '10px' }}>
          <select 
            value={filterOwner}
            onChange={e => setFilterOwner(e.target.value)}
            style={styles.filterControl}
          >
            <option value="">All Owners</option>
            {DRAFT_OWNERS.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
          <input 
            type="text"
            placeholder="Search commentary..."
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
            style={styles.filterControl}
          />
        </div>
      </div>
      
      <div style={{ ...styles.poolTableContainer, minHeight: 0 }}>
        {filteredHistory.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '40px', color: '#666' }}>
            {analysisHistory.length === 0 ? 
              'No analyses generated yet. Make draft picks to see Gemini AI commentary and Skip & Chip podcast debate!' :
              'No analyses match your filters.'
            }
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
            {filteredHistory.map((item, idx) => {
              const isThisPlaying = playingPick === item.pickNumber;
              const isThisExpanded = expandedPick === item.pickNumber;
              const itemScript = getEffectiveItemScript(item);

              return (
                <div key={idx} style={{
                  background: '#222',
                  padding: '16px',
                  borderRadius: '10px',
                  borderLeft: `4px solid ${isThisPlaying ? '#ff9800' : 'var(--accent)'}`,
                  boxShadow: isThisPlaying ? '0 0 16px rgba(255, 152, 0, 0.25)' : 'none',
                  transition: 'all 0.3s ease'
                }}>
                  <div style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    marginBottom: '10px',
                    paddingBottom: '10px',
                    borderBottom: '1px solid #333',
                    flexWrap: 'wrap',
                    gap: '8px'
                  }}>
                    <div>
                      <span style={{ color: '#888', fontSize: '12px' }}>
                        Pick #{item.pickNumber} • Round {item.round}
                      </span>
                      <div style={{ marginTop: '5px' }}>
                        <span style={{ color: 'var(--highlight)', fontWeight: 'bold', fontSize: '16px' }}>
                          {item.owner}
                        </span>
                        <span style={{ color: '#888', margin: '0 8px' }}>→</span>
                        <span style={{ color: '#fff', fontWeight: 'bold', fontSize: '16px' }}>
                          {item.playerName}
                        </span>
                        <span style={{ color: '#888', marginLeft: '8px', fontSize: '12px' }}>
                          {item.position} • {item.team}
                        </span>
                      </div>
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '6px' }}>
                      <div style={{ color: '#666', fontSize: '11px', textAlign: 'right' }}>
                        {item.timestamp}
                      </div>
                      <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                        {item.commentary && (
                          <button
                            onClick={() => playPodcastTTS(item.commentary)}
                            style={{
                              background: 'rgba(187, 134, 252, 0.15)',
                              border: '1px solid #bb86fc',
                              color: '#bb86fc',
                              borderRadius: '4px',
                              padding: '2px 8px',
                              fontSize: '11px',
                              cursor: 'pointer',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '4px'
                            }}
                            title="Play single-speaker reaction"
                          >
                            🔊 Reaction
                          </button>
                        )}
                        <button
                          onClick={() => handlePlayDebate(item)}
                          style={{
                            background: isThisPlaying ? 'rgba(255, 152, 0, 0.35)' : 'rgba(255, 152, 0, 0.15)',
                            border: `1px solid ${isThisPlaying ? '#ffb74d' : '#ff9800'}`,
                            color: '#ffb74d',
                            borderRadius: '4px',
                            padding: '2px 8px',
                            fontSize: '11px',
                            fontWeight: 'bold',
                            cursor: 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '4px'
                          }}
                          title="Play Skip & Chip 2-person debate podcast (Google Cloud Chirp 3 HD)"
                        >
                          {isThisPlaying ? '⏹ Stop Debate' : '🎙️ Skip & Chip Debate'}
                        </button>
                        <button
                          onClick={() => setExpandedPick(prev => prev === item.pickNumber ? null : item.pickNumber)}
                          style={{
                            background: 'rgba(255, 255, 255, 0.06)',
                            border: '1px solid #444',
                            color: '#aaa',
                            borderRadius: '4px',
                            padding: '2px 6px',
                            fontSize: '10px',
                            cursor: 'pointer'
                          }}
                        >
                          {isThisExpanded ? 'Hide Debate' : 'View Debate'}
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* Written Instant Reaction */}
                  <div 
                    style={{ 
                      color: '#ccc', 
                      fontSize: '13px', 
                      lineHeight: '1.6',
                      fontStyle: 'italic',
                      marginBottom: isThisExpanded ? '12px' : '0'
                    }}
                    dangerouslySetInnerHTML={{ __html: item.commentary }}
                  />

                  {/* Expanded Skip & Chip Dialogue */}
                  {isThisExpanded && itemScript && (
                    <div style={{
                      marginTop: '10px',
                      padding: '12px',
                      background: 'rgba(15, 15, 25, 0.75)',
                      borderRadius: '8px',
                      border: '1px solid rgba(187, 134, 252, 0.25)',
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '8px'
                    }}>
                      <div style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        fontSize: '11px',
                        color: '#bb86fc',
                        fontWeight: 'bold',
                        letterSpacing: '0.5px'
                      }}>
                        <span>🎙️ SKIP & CHIP 2-HOST DEBATE SCRIPT</span>
                        <span style={{ fontSize: '9px', color: '#03dac6', border: '1px solid rgba(3, 218, 198, 0.3)', padding: '1px 4px', borderRadius: '3px' }}>
                          Google Cloud Chirp 3 HD
                        </span>
                      </div>
                      {itemScript.map((line, lIdx) => {
                        const isChip = (line.speaker || '').toUpperCase().trim() === 'CHIP';
                        const isLineActive = isThisPlaying && activeDebateLine === lIdx;
                        return (
                          <div
                            key={lIdx}
                            style={{
                              display: 'flex',
                              flexDirection: 'column',
                              alignSelf: isChip ? 'flex-end' : 'flex-start',
                              maxWidth: '85%',
                              background: isLineActive
                                ? (isChip ? 'rgba(187, 134, 252, 0.3)' : 'rgba(255, 152, 0, 0.3)')
                                : 'rgba(0, 0, 0, 0.35)',
                              borderLeft: isChip ? 'none' : `3px solid ${isLineActive ? '#ffb74d' : '#ff9800'}`,
                              borderRight: isChip ? `3px solid ${isLineActive ? '#d1c4e9' : '#bb86fc'}` : 'none',
                              borderRadius: isChip ? '8px 2px 8px 8px' : '2px 8px 8px 8px',
                              padding: '6px 10px',
                              transition: 'all 0.2s ease',
                              boxShadow: isLineActive
                                ? `0 0 10px ${isChip ? 'rgba(187, 134, 252, 0.4)' : 'rgba(255, 152, 0, 0.4)'}`
                                : 'none'
                            }}
                          >
                            <span style={{
                              fontSize: '10px',
                              fontWeight: 'bold',
                              color: isChip ? '#bb86fc' : '#ff9800',
                              marginBottom: '2px',
                              textAlign: isChip ? 'right' : 'left'
                            }}>
                              {isChip ? '🔥 CHIP' : '🎙️ SKIP'} {isLineActive && '• SPEAKING'}
                            </span>
                            <span style={{ fontSize: '12px', color: isLineActive ? '#fff' : '#ddd', lineHeight: '1.4' }}>
                              {line.text}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
