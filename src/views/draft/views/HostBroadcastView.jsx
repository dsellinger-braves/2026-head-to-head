import { useState, useEffect } from 'react';
import { OWNER_AVATARS, getEffectiveQS } from '../utils/draftCalculations';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../../../utils/headshotUtils';
import { OnDeckSidebar, RecentActivityWidget, Ticker } from '../components/DraftWidgets';
import PlayerModal from '../components/PlayerModal';

/**
 * HostBroadcastView
 * Big-screen TV / Projector broadcast layout featuring big countdown timers,
 * marquee pick card, AI instant reaction commentary, and Skip & Chip podcast breakdown.
 */
export default function HostBroadcastView({
  roomSeason,
  pickStartTime,
  currentPick,
  upcomingPicks = [],
  lastPick,
  lastPickPlayer,
  lastPickCommentary,
  generatingCommentary,
  playPodcastTTS,
  lastPodcastScript,
  isPlayingPodcast,
  handlePlayPodcast,
  handleStopPodcast,
  recentPicks = [],
  displayPlayers = [],
  selectedPlayer,
  setSelectedPlayer,
  playerInfo,
  handleResetMode,
  onSwitchView,
  onOpenPlayerModal,
  draftHistory = [],
  historicalFinish = [],
  savantBatters = [],
  savantPitchers = [],
  battersZips = [],
  pitchersZips = []
}) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const elapsed = Math.floor((now - pickStartTime) / 1000);
  const secondsLeft = Math.max(0, 60 - elapsed);
  const isWarning = secondsLeft <= 30 && secondsLeft > 0;
  const isUrgent = secondsLeft <= 15 && secondsLeft > 0;
  const isExpired = secondsLeft === 0;

  const timerColor = isExpired ? '#f44336' : isUrgent ? '#ff5722' : isWarning ? '#ffc107' : '#03dac6';
  const timerBg = isExpired ? '#f44336' : isUrgent ? '#ff5722' : isWarning ? '#ffc107' : '#222';
  const timerTextColor = isExpired || isUrgent ? '#fff' : isWarning ? '#000' : '#03dac6';
  const timerClass = isExpired ? 'monospace timer-expired' : isUrgent ? 'monospace urgent-warning' : isWarning ? 'monospace timer-warning' : 'monospace';
  const headerBg = isExpired
    ? 'linear-gradient(180deg, #3d1a1a 0%, #2d1616 100%)'
    : isUrgent
    ? 'linear-gradient(180deg, #3d2a1a 0%, #2d1f16 100%)'
    : isWarning
    ? 'linear-gradient(180deg, #3d3a1a 0%, #2d2916 100%)'
    : 'linear-gradient(180deg, #1a1a2e 0%, #16213e 100%)';

  const isPitcher = lastPickPlayer ? (lastPickPlayer.Position || '').includes('SP') || (lastPickPlayer.Position || '').includes('RP') : false;

  return (
    <div className={`host-mode-container ${isExpired ? 'container-expired' : ''}`} style={{
      backgroundColor: '#000',
      color: '#e0e0e0',
      height: '100vh',
      overflow: 'hidden',
      display: 'grid',
      gridTemplateColumns: '60% 40%',
      gridTemplateRows: '90px 1fr 50px',
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
    }}>
      <style>{`
        .host-mode-container, .host-mode-container * {
          font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
        }
        .host-mode-container .monospace {
          font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Fira Mono', monospace !important;
        }
        @keyframes timerWarning { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.02); } }
        .timer-warning { animation: timerWarning 1s ease-in-out infinite !important; }
        @keyframes timerPulse { 0%, 100% { transform: scale(1); box-shadow: 0 0 20px rgba(244, 67, 54, 0.5); } 50% { transform: scale(1.05); box-shadow: 0 0 40px rgba(244, 67, 54, 0.8); } }
        @keyframes borderFlash { 0%, 100% { border-color: #f44336; } 50% { border-color: #ff8a80; } }
        @keyframes headerPulse { 0%, 100% { background: linear-gradient(180deg, #3d1a1a 0%, #2d1616 100%); } 50% { background: linear-gradient(180deg, #5d2a2a 0%, #3d1a1a 100%); } }
        @keyframes textFlash { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
        @keyframes urgentPulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.8; transform: scale(1.02); } }
        @keyframes redVignette { 0%, 100% { box-shadow: inset 0 0 150px 50px rgba(244, 67, 54, 0.3); } 50% { box-shadow: inset 0 0 200px 80px rgba(244, 67, 54, 0.5); } }
        @keyframes cardGlow { 0%, 100% { box-shadow: 0 0 30px rgba(244, 67, 54, 0.4), 0 0 60px rgba(244, 67, 54, 0.2); } 50% { box-shadow: 0 0 50px rgba(244, 67, 54, 0.6), 0 0 100px rgba(244, 67, 54, 0.3); } }
        @keyframes screenShake { 0%, 100% { transform: translateX(0); } 25% { transform: translateX(-2px); } 75% { transform: translateX(2px); } }
        .timer-expired { animation: timerPulse 0.8s ease-in-out infinite !important; }
        .header-expired { animation: headerPulse 1s ease-in-out infinite, borderFlash 0.5s ease-in-out infinite !important; }
        .times-up-flash { animation: textFlash 0.5s ease-in-out infinite !important; }
        .urgent-warning { animation: urgentPulse 0.5s ease-in-out infinite !important; }
        .container-expired { animation: screenShake 0.3s ease-in-out infinite !important; }
      `}</style>

      {/* Host Header */}
      <div className={isExpired ? 'header-expired' : ''} style={{
        gridColumn: '1 / -1',
        background: headerBg,
        borderBottom: `4px solid ${timerColor}`,
        display: 'flex',
        alignItems: 'center',
        padding: '0 25px',
        justifyContent: 'space-between',
        transition: 'all 0.3s ease'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
          <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#fff', letterSpacing: '2px' }}>
            ⚾ HEFTY WAR ROOM {roomSeason}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginLeft: '10px', paddingLeft: '15px', borderLeft: '2px solid #444', overflow: 'hidden' }}>
            {[currentPick, ...upcomingPicks].filter(Boolean).slice(0, 10).map((p, idx) => {
              const isCurrent = idx === 0;
              const avatar = OWNER_AVATARS[p.Owner] || OWNER_AVATARS.default;
              return (
                <div key={p['Overall Pick']} style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  background: isCurrent ? 'rgba(3, 218, 198, 0.15)' : '#1a1a2e',
                  border: `1px solid ${isCurrent ? '#03dac6' : '#333'}`,
                  padding: '4px 10px',
                  borderRadius: '20px',
                  minWidth: '100px'
                }}>
                  <div style={{ width: '24px', height: '24px', borderRadius: '50%', overflow: 'hidden', border: `2px solid ${isCurrent ? '#03dac6' : '#555'}` }}>
                    <img src={avatar} alt={p.Owner} style={{ width: '100%', height: '100%', objectFit: 'cover' }} onError={e => { e.target.style.display = 'none'; }} />
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column' }}>
                    <span style={{ fontSize: '9px', color: isCurrent ? '#03dac6' : '#888', fontWeight: 'bold' }}>#{p['Overall Pick']}</span>
                    <span style={{ fontSize: '11px', color: '#fff', fontWeight: 'bold', whiteSpace: 'nowrap' }}>{p.Owner}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          {isUrgent && !isExpired && (
            <div className="urgent-warning" style={{ background: '#ff5722', color: '#fff', padding: '6px 14px', borderRadius: '8px', fontSize: '14px', fontWeight: 'bold' }}>
              ⚠️ HURRY!
            </div>
          )}
          {isExpired && (
            <div className="times-up-flash" style={{ background: '#f44336', color: '#fff', padding: '6px 14px', borderRadius: '8px', fontSize: '15px', fontWeight: 'bold' }}>
              ⏰ TIME'S UP!
            </div>
          )}
          <div className={timerClass} style={{
            fontSize: '34px',
            background: timerBg,
            padding: '4px 16px',
            borderRadius: '8px',
            border: `3px solid ${timerColor}`,
            color: timerTextColor,
            minWidth: '90px',
            textAlign: 'center',
            fontWeight: 900
          }}>
            {secondsLeft}s
          </div>
          <button
            onClick={handleResetMode}
            style={{ background: 'transparent', color: '#888', border: '1px solid #555', padding: '6px 10px', borderRadius: '6px', fontSize: '12px', cursor: 'pointer' }}
          >
            Exit Host
          </button>
          {onSwitchView && (
            <button
              onClick={() => onSwitchView('summary')}
              style={{ background: '#1e3a8a', color: '#fff', border: '1px solid #3b82f6', padding: '6px 12px', borderRadius: '6px', fontSize: '12px', cursor: 'pointer', fontWeight: 'bold' }}
            >
              📊 League Site
            </button>
          )}
        </div>
      </div>

      {/* 60% Left Stage */}
      <div style={{
        gridColumn: '1 / 2',
        gridRow: '2 / 3',
        display: 'flex',
        flexDirection: 'column',
        padding: '24px',
        borderRight: '1px solid #333',
        background: isExpired ? 'radial-gradient(ellipse at center, #2d1a1a 0%, #1a0000 100%)' : 'radial-gradient(ellipse at center, #1a1a2e 0%, #000 100%)',
        gap: '16px',
        overflowY: 'auto'
      }}>
        <div style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          background: 'linear-gradient(135deg, #1e1e2f 0%, #252540 100%)',
          borderRadius: '16px',
          padding: '24px',
          border: '2px solid #333',
          position: 'relative',
          overflowY: 'auto'
        }}>
          <div style={{ fontSize: '16px', color: '#888', textTransform: 'uppercase', letterSpacing: '2px', textAlign: 'center', marginBottom: '16px' }}>
            {lastPick ? `Round ${lastPick.Round} • Pick ${lastPick['Overall Pick']}` : 'Waiting for first pick...'}
          </div>

          {lastPickPlayer ? (
            <div style={{ display: 'flex', width: '100%', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
              <div style={{ width: '120px', height: '120px', borderRadius: '16px', border: '3px solid #bb86fc', overflow: 'hidden', background: '#111', flexShrink: 0, boxShadow: '0 4px 20px rgba(187, 134, 252, 0.4)' }}>
                <img
                  src={getPlayerHeadshotUrl(lastPickPlayer)}
                  alt={lastPickPlayer.Player}
                  style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                  onError={(e) => handleHeadshotError(e, lastPickPlayer)}
                />
              </div>
              <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '0 16px' }}>
                <div style={{ fontSize: '38px', fontWeight: '900', lineHeight: '1.1', color: '#fff', textAlign: 'center', marginBottom: '10px' }}>
                  {lastPickPlayer.Player}
                </div>
                <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginBottom: '14px' }}>
                  <span style={{ background: 'linear-gradient(135deg, #bb86fc 0%, #9c5dd9 100%)', padding: '4px 14px', borderRadius: '20px', fontSize: '14px', fontWeight: 'bold', color: '#fff' }}>
                    {lastPickPlayer.Position}
                  </span>
                  <span style={{ background: 'linear-gradient(135deg, #03dac6 0%, #00a896 100%)', padding: '4px 14px', borderRadius: '20px', fontSize: '14px', fontWeight: 'bold', color: '#000' }}>
                    {lastPickPlayer.Team}
                  </span>
                </div>
                <div style={{ display: 'flex', gap: '10px' }}>
                  {isPitcher ? (
                    <>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>ERA</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSERA || '—'}</span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>WHIP</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSWHIP || '—'}</span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>K</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSK || '—'}</span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>QS</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{getEffectiveQS(lastPickPlayer) || lastPickPlayer.ZIPSQS || '—'}</span>
                      </div>
                    </>
                  ) : (
                    <>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>HR</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSHR || '—'}</span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>RBI</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSRBI || '—'}</span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>SB</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSSB || '—'}</span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                        <span style={{ fontSize: '10px', color: '#888' }}>OBP</span>
                        <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSOBP || '—'}</span>
                      </div>
                    </>
                  )}
                </div>
              </div>
              <div style={{ width: '110px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
                <div style={{ width: '90px', height: '90px', borderRadius: '50%', border: '3px solid #03dac6', overflow: 'hidden', background: '#111' }}>
                  <img
                    src={OWNER_AVATARS[lastPick?.Owner] || OWNER_AVATARS.default}
                    alt={lastPick?.Owner}
                    style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                    onError={e => { e.target.style.display = 'none'; }}
                  />
                </div>
                <div style={{ textAlign: 'center', background: 'rgba(0,0,0,0.5)', padding: '4px 10px', borderRadius: '8px', border: '1px solid #03dac644' }}>
                  <div style={{ fontSize: '9px', color: '#03dac6', fontWeight: 'bold', textTransform: 'uppercase' }}>Selected By</div>
                  <div style={{ fontSize: '14px', color: '#fff', fontWeight: 'bold' }}>{lastPick?.Owner}</div>
                </div>
              </div>
            </div>
          ) : (
            <div style={{ fontSize: '32px', color: '#444', fontWeight: 'bold', textAlign: 'center', padding: '40px 0' }}>
              WAR ROOM READY
            </div>
          )}

          {/* AI Commentary in Host Mode - Full visibility for text summary and grading */}
          <div style={{
            flex: 1,
            background: 'rgba(0,0,0,0.35)',
            padding: '20px',
            borderRadius: '14px',
            borderLeft: '5px solid #bb86fc',
            border: '1px solid rgba(187, 134, 252, 0.2)',
            display: 'flex',
            flexDirection: 'column',
            minHeight: '260px',
            overflow: 'hidden'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px', flexWrap: 'wrap', gap: '8px' }}>
              <span style={{ fontSize: '13px', color: '#bb86fc', fontWeight: 'bold', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                🎙️ HeftyMatic 3000 Instant Reaction & Grade
              </span>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                {lastPodcastScript && lastPodcastScript.length > 0 && (
                  <button
                    onClick={() => isPlayingPodcast ? handleStopPodcast() : handlePlayPodcast()}
                    style={{
                      background: isPlayingPodcast ? 'rgba(239, 68, 68, 0.2)' : 'rgba(3, 218, 198, 0.15)',
                      border: `1px solid ${isPlayingPodcast ? '#ef4444' : '#03dac6'}`,
                      color: isPlayingPodcast ? '#ef4444' : '#03dac6',
                      borderRadius: '6px',
                      padding: '3px 10px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '4px',
                      transition: 'all 0.2s'
                    }}
                    title={isPlayingPodcast ? "Stop Skip & Chip debate" : "Play Skip & Chip spoken debate audio"}
                  >
                    {isPlayingPodcast ? '⏹️ Stop Debate' : '🎙️ Play Debate'}
                  </button>
                )}
                {lastPickCommentary && lastPickCommentary !== 'Draft has not started.' && !generatingCommentary && (
                  <button
                    onClick={() => playPodcastTTS(lastPickCommentary)}
                    style={{
                      background: 'rgba(187, 134, 252, 0.15)',
                      border: '1px solid #bb86fc',
                      color: '#bb86fc',
                      borderRadius: '6px',
                      padding: '3px 10px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '4px',
                      transition: 'all 0.2s'
                    }}
                    title="Play podcast-style spoken analysis"
                  >
                    🔊 Listen
                  </button>
                )}
              </div>
            </div>
            <div style={{ flex: 1, overflowY: 'auto', fontSize: '15px', color: '#ddd', lineHeight: '1.5' }}>
              {generatingCommentary ? (
                <span style={{ color: '#888', fontStyle: 'italic' }}>🤔 HeftyMatic 3000 is analyzing this pick...</span>
              ) : (
                <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
              )}
            </div>
          </div>
        </div>
      </div>

      {/* 40% Right Stage */}
      <div style={{
        gridColumn: '2 / 3',
        gridRow: '2 / 3',
        background: '#111',
        padding: '20px',
        display: 'flex',
        flexDirection: 'column',
        gap: '15px',
        overflow: 'hidden'
      }}>
        <OnDeckSidebar upcomingPicks={upcomingPicks} currentUser={currentPick?.Owner} />
        <div style={{ flex: 1, overflowY: 'auto' }}>
          <RecentActivityWidget
            recentPicks={recentPicks}
            players={displayPlayers}
            onPlayerClick={setSelectedPlayer}
            playerInfo={playerInfo}
          />
        </div>
      </div>

      {/* Bottom Ticker */}
      <div style={{ gridColumn: '1 / -1', gridRow: '3 / 4', borderTop: '2px solid #333' }}>
        <Ticker recentPicks={recentPicks} players={displayPlayers} />
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
        />
      )}
    </div>
  );
}
