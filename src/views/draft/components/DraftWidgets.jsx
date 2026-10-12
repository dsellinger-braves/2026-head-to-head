import { useState, useEffect, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../../../utils/headshotUtils';
import { getInjuryIndicator } from '../utils/draftCalculations';
import { OWNER_ARCHETYPES, normalizeManager, formatRotoStat } from '../../../utils/draftRotoEngine';
import PlayerNameButton from './PlayerNameButton';

// --- COUNTDOWN TIMER ---
export function CountdownTimer({ pickStartTime }) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const elapsed = Math.floor((now - pickStartTime) / 1000);
  const secondsLeft = Math.max(0, 60 - elapsed);
  const isWarning = secondsLeft <= 10;

  return (
    <div style={{
      ...styles.clock,
      color: isWarning ? '#ff0000' : 'var(--alert)',
      animation: isWarning ? 'pulse 1s infinite' : 'none'
    }}>
      {secondsLeft}s
    </div>
  );
}

// --- TICKER ---
export function Ticker({ recentPicks, players }) {
  if (recentPicks.length === 0) return null;

  return (
    <div style={styles.tickerContainer}>
      <div style={styles.tickerContent}>
        {recentPicks.slice(-10).reverse().map(pick => {
          const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
          return (
            <span key={pick['Overall Pick']} style={styles.tickerItem}>
              #{pick['Overall Pick']} {pick.Owner}: {player ? player.Player : 'Unknown'} ({player?.Position || '?'})
            </span>
          );
        })}
      </div>
    </div>
  );
}

// --- ON DECK SIDEBAR ---
export function OnDeckSidebar({ upcomingPicks, currentUser }) {
  return (
    <div style={styles.sidebar}>
      <h3 style={{ color: 'var(--highlight)', margin: '0 0 15px 0', fontSize: '18px', textTransform: 'uppercase', letterSpacing: '1px' }}>
        On Deck
      </h3>
      {upcomingPicks.length === 0 ? (
        <div style={{ color: '#666', fontStyle: 'italic' }}>Draft complete</div>
      ) : (
        upcomingPicks.map(p => {
          const isUser = p.Owner === currentUser;
          return (
            <div 
              key={p['Overall Pick']} 
              style={{
                ...styles.deckItem,
                background: isUser ? '#2e2540' : '#222',
                borderLeft: isUser ? '4px solid #bb86fc' : 'none',
                boxShadow: isUser ? '0 0 10px rgba(187, 134, 252, 0.2)' : 'none'
              }}
            >
              <div>
                <span style={{ color: isUser ? '#bb86fc' : '#888', fontWeight: isUser ? 'bold' : 'normal', marginRight: '8px' }}>
                  #{p['Overall Pick']}
                </span>
                <span style={{ color: isUser ? '#fff' : '#ccc', fontWeight: isUser ? 'bold' : 'normal' }}>
                  {p.Owner}
                </span>
              </div>
              <div style={{ fontSize: '12px', color: '#666' }}>
                R{p.Round} P{p.Pick}
              </div>
            </div>
          );
        })
      )}
    </div>
  );
}

// --- TEAM NEEDS SUMMARY ---
export function TeamNeedsSummary({ myPicks, players }) {
  const needs = useMemo(() => {
    const counts = {
      C: 0, '1B': 0, '2B': 0, '3B': 0, SS: 0, OF: 0, DH: 0, SP: 0, RP: 0, Bench: 0
    };
    
    myPicks.forEach(pick => {
      const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
      if (!player) return;
      const pos = player.Position || '';
      if (pos.includes('C')) counts.C++;
      else if (pos.includes('1B')) counts['1B']++;
      else if (pos.includes('2B')) counts['2B']++;
      else if (pos.includes('3B')) counts['3B']++;
      else if (pos.includes('SS')) counts.SS++;
      else if (pos.includes('OF')) counts.OF++;
      else if (pos.includes('SP')) counts.SP++;
      else if (pos.includes('RP')) counts.RP++;
      else counts.Bench++;
    });
    
    return counts;
  }, [myPicks, players]);

  return (
    <div style={styles.infoWidget}>
      <div style={styles.infoWidgetTitle}>Team Roster Breakdown</div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '8px', fontSize: '12px' }}>
        {Object.entries(needs).map(([pos, count]) => (
          <div key={pos} style={{ textAlign: 'center', background: '#2a2a2a', padding: '6px', borderRadius: '4px' }}>
            <div style={{ color: '#888', fontSize: '10px' }}>{pos}</div>
            <div style={{ color: count > 0 ? '#03dac6' : '#666', fontWeight: 'bold', fontSize: '14px' }}>
              {count}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// --- RECENT ACTIVITY WIDGET ---
export function RecentActivityWidget({ recentPicks, players, onPlayerClick, playerInfo }) {
  const lastFive = recentPicks.slice(-5).reverse();

  return (
    <div style={styles.infoWidget}>
      <div style={styles.infoWidgetTitle}>Recent Picks</div>
      {lastFive.length === 0 ? (
        <div style={{ color: '#666', fontSize: '12px', fontStyle: 'italic' }}>No picks yet</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
          {lastFive.map(pick => {
            const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
            const injury = player ? getInjuryIndicator(player['ESPN PlayerID'], playerInfo) : null;
            return (
              <div key={pick['Overall Pick']} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '12px' }}>
                <div>
                  <span style={{ color: '#888', marginRight: '6px' }}>#{pick['Overall Pick']}</span>
                  <span style={{ color: 'var(--highlight)', fontWeight: 'bold' }}>{pick.Owner}</span>
                </div>
                {player ? (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                    <div style={{ width: '22px', height: '22px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #444' }}>
                      <img
                        src={getPlayerHeadshotUrl(player)}
                        alt=""
                        style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                        onError={(e) => handleHeadshotError(e, player)}
                        referrerPolicy="no-referrer"
                        loading="lazy"
                      />
                    </div>
                    <PlayerNameButton 
                      player={player} 
                      onClick={onPlayerClick}
                      style={{ color: injury ? injury.color : '#fff' }}
                    />
                    <span style={{ color: '#888', fontSize: '10px' }}>({player.Position})</span>
                  </div>
                ) : (
                  <span style={{ color: '#666' }}>--</span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// --- QUEUE PREVIEW WIDGET ---
export function QueuePreviewWidget({ queue, onPlayerClick, playerInfo }) {
  return (
    <div style={styles.infoWidget}>
      <div style={styles.infoWidgetTitle}>Queue ({queue.length})</div>
      {queue.length === 0 ? (
        <div style={{ color: '#666', fontSize: '12px', fontStyle: 'italic' }}>Queue is empty. Star players from the pool to add them.</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', maxHeight: '100px', overflowY: 'auto' }}>
          {queue.slice(0, 5).map(player => {
            const injury = getInjuryIndicator(player['ESPN PlayerID'], playerInfo);
            return (
              <div key={player['ESPN PlayerID']} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <div style={{ width: '22px', height: '22px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #444' }}>
                    <img
                      src={getPlayerHeadshotUrl(player)}
                      alt=""
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                      onError={(e) => handleHeadshotError(e, player)}
                      referrerPolicy="no-referrer"
                      loading="lazy"
                    />
                  </div>
                  <PlayerNameButton 
                    player={player} 
                    onClick={onPlayerClick}
                    style={{ color: injury ? injury.color : '#fff', fontWeight: 'bold' }}
                  />
                </div>
                <span style={{ color: '#888', fontSize: '10px' }}>{player.Position} • {player.Team}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// --- POSITIONAL RUN RADAR ---
export function PositionalRunRadar({ run }) {
  if (!run) return null;
  return (
    <div style={{
      width: '88%',
      maxWidth: '850px',
      margin: '0 auto 14px auto',
      background: 'linear-gradient(90deg, rgba(239, 68, 68, 0.25) 0%, rgba(249, 115, 22, 0.2) 100%)',
      border: '1.5px solid #ef4444',
      borderRadius: '8px',
      padding: '10px 16px',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: '12px',
      boxShadow: '0 0 18px rgba(239, 68, 68, 0.35)',
      boxSizing: 'border-box'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <span style={{ fontSize: '22px' }}>🔥</span>
        <div>
          <span style={{ color: '#fca5a5', fontWeight: 'bold', fontSize: '13px', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
            Positional Run Alert: {run.group} Run ({run.count} of last {run.window} picks)
          </span>
          <div style={{ color: '#e2e8f0', fontSize: '12px', marginTop: '2px' }}>
            Recent picks: <span style={{ color: '#fef08a' }}>{run.players.join(', ')}</span> — positional tier depth dropping rapidly!
          </div>
        </div>
      </div>
      <span style={{
        background: '#ef4444',
        color: '#fff',
        fontSize: '11px',
        fontWeight: 'bold',
        padding: '3px 8px',
        borderRadius: '4px',
        whiteSpace: 'nowrap'
      }}>
        {run.urgency} SCARCITY
      </span>
    </div>
  );
}

// --- ON-THE-CLOCK CATEGORY DEFICIT HUD ---
export function CategoryDeficitHUD({ currentPick, ownerRoto, isMyTurn, onSelectTab }) {
  if (!currentPick?.Owner || !ownerRoto) return null;
  const owner = currentPick.Owner;
  const archetype = OWNER_ARCHETYPES[owner] || OWNER_ARCHETYPES[normalizeManager(owner)];

  return (
    <div style={{
      width: '88%',
      maxWidth: '850px',
      margin: '0 auto 16px auto',
      background: isMyTurn 
        ? 'linear-gradient(90deg, rgba(3, 218, 198, 0.16) 0%, rgba(187, 134, 252, 0.16) 100%)' 
        : 'rgba(28, 28, 38, 0.92)',
      border: isMyTurn ? '1.5px solid #03dac6' : '1px solid #333',
      borderRadius: '10px',
      padding: '12px 18px',
      boxShadow: isMyTurn ? '0 0 20px rgba(3, 218, 198, 0.25)' : '0 8px 24px rgba(0,0,0,0.4)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      flexWrap: 'wrap',
      gap: '12px',
      boxSizing: 'border-box'
    }}>
      {/* Left: Manager & Archetype */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
        <div style={{
          width: '38px',
          height: '38px',
          borderRadius: '50%',
          background: '#222',
          border: '2px solid var(--highlight)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '18px'
        }}>
          {archetype?.archetype?.emoji || '⚾'}
        </div>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            <span style={{ color: '#fff', fontWeight: 'bold', fontSize: '15px' }}>{owner}</span>
            {archetype?.archetype?.name && (
              <span style={{
                background: 'rgba(187, 134, 252, 0.2)',
                color: '#bb86fc',
                border: '1px solid rgba(187, 134, 252, 0.4)',
                fontSize: '11px',
                fontWeight: '600',
                padding: '2px 8px',
                borderRadius: '12px'
              }}>
                {archetype.archetype.emoji} {archetype.archetype.name}
              </span>
            )}
            <span style={{ color: '#94a3b8', fontSize: '12px' }}>
              Proj: <strong style={{ color: '#38bdf8' }}>#{ownerRoto.rank}</strong> ({ownerRoto.totalPoints.toFixed(1)} pts)
            </span>
          </div>
          <div style={{ color: '#888', fontSize: '11px', marginTop: '2px' }}>
            {archetype?.archetype?.tagline || 'Live roto standings & active deficit tracker'}
          </div>
        </div>
      </div>

      {/* Middle/Right: Deficits & Surpluses */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
        {ownerRoto.deficits && ownerRoto.deficits.length > 0 && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
            <span style={{ fontSize: '11px', color: '#f43f5e', fontWeight: 'bold', letterSpacing: '0.5px' }}>
              🔻 DEFICIT:
            </span>
            {ownerRoto.deficits.map(d => (
              <span
                key={d.key}
                style={{
                  background: 'rgba(244, 63, 94, 0.18)',
                  color: '#fca5a5',
                  border: '1px solid rgba(244, 63, 94, 0.4)',
                  borderRadius: '6px',
                  padding: '2px 7px',
                  fontSize: '11px',
                  fontWeight: '600'
                }}
                title={`${d.label}: ${formatRotoStat(d.key, d.val)} (${d.pts.toFixed(1)} pts)`}
              >
                {d.label} ({d.pts.toFixed(1)} pts)
              </span>
            ))}
          </div>
        )}

        {ownerRoto.surpluses && ownerRoto.surpluses.length > 0 && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
            <span style={{ fontSize: '11px', color: '#10b981', fontWeight: 'bold', letterSpacing: '0.5px' }}>
              🟢 SURPLUS:
            </span>
            {ownerRoto.surpluses.slice(0, 2).map(s => (
              <span
                key={s.key}
                style={{
                  background: 'rgba(16, 185, 129, 0.18)',
                  color: '#6ee7b7',
                  border: '1px solid rgba(16, 185, 129, 0.4)',
                  borderRadius: '6px',
                  padding: '2px 7px',
                  fontSize: '11px',
                  fontWeight: '600'
                }}
                title={`${s.label}: ${formatRotoStat(s.key, s.val)} (${s.pts.toFixed(1)} pts)`}
              >
                {s.label} ({s.pts.toFixed(1)} pts)
              </span>
            ))}
          </div>
        )}

        {onSelectTab && (
          <button
            onClick={() => onSelectTab('Standings')}
            style={{
              background: 'transparent',
              border: '1px solid #475569',
              color: '#cbd5e1',
              padding: '4px 10px',
              borderRadius: '6px',
              fontSize: '11px',
              cursor: 'pointer'
            }}
          >
            📊 Standings →
          </button>
        )}
      </div>
    </div>
  );
}

// --- DRAFT ASSISTANT WIDGET ---
export function DraftAssistantWidget({
  recommendations = [],
  onDraft,
  isMyTurn,
  queue = [],
  onAddToQueue,
  onRemoveFromQueue,
  onPlayerClick
}) {
  if (!recommendations || recommendations.length === 0) return null;

  return (
    <div style={{
      background: 'linear-gradient(180deg, #181c24 0%, #11141a 100%)',
      border: '1px solid #2d3748',
      borderRadius: '10px',
      padding: '12px 14px',
      marginBottom: '14px',
      boxShadow: '0 4px 14px rgba(0, 0, 0, 0.35)'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '16px' }}>🤖</span>
          <span style={{ color: '#fff', fontWeight: 'bold', fontSize: '13px', letterSpacing: '0.5px' }}>
            LIVE DRAFT ASSISTANT: TOP STRATEGIC TARGETS
          </span>
          <span style={{
            background: 'rgba(3, 218, 198, 0.15)',
            color: '#03dac6',
            fontSize: '10px',
            padding: '2px 6px',
            borderRadius: '4px',
            fontWeight: '600'
          }}>
            AI OPTIMIZED
          </span>
        </div>
        <span style={{ color: '#64748b', fontSize: '11px' }}>
          Deficit synergy, open roster needs & ADP surplus
        </span>
      </div>

      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
        gap: '10px'
      }}>
        {recommendations.map((rec, i) => {
          const p = rec.player;
          const isQueued = queue.some(qp => String(qp['ESPN PlayerID']) === String(p['ESPN PlayerID']));
          return (
            <div
              key={p['ESPN PlayerID'] || i}
              style={{
                background: '#1a1f29',
                border: `1px solid ${rec.color}44`,
                borderLeft: `3px solid ${rec.color}`,
                borderRadius: '8px',
                padding: '10px 12px',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                gap: '8px'
              }}
            >
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '4px' }}>
                  <span style={{
                    fontSize: '10px',
                    fontWeight: 'bold',
                    color: rec.color,
                    letterSpacing: '0.5px',
                    textTransform: 'uppercase'
                  }}>
                    {rec.badge}
                  </span>
                  <span style={{ color: '#ffc107', fontSize: '11px', fontWeight: 'bold' }}>
                    Rank #{p['Hefty Keeper Rank'] || p.overall_rank || p.rank || '--'}{p['Hefty Keeper Price'] !== undefined || p.overall_price !== undefined ? ` • $${p['Hefty Keeper Price'] ?? p.overall_price}` : ''}
                  </span>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                  <div
                    onClick={() => onPlayerClick && onPlayerClick(p)}
                    style={{
                      color: '#fff',
                      fontWeight: 'bold',
                      fontSize: '14px',
                      cursor: 'pointer',
                      textDecoration: 'underline',
                      textUnderlineOffset: '2px'
                    }}
                    title="Click for full scouting card"
                  >
                    {p.Player}
                  </div>
                  <span style={{
                    background: '#2d3748',
                    color: '#94a3b8',
                    fontSize: '11px',
                    padding: '1px 6px',
                    borderRadius: '4px'
                  }}>
                    {p.Position} • {p.Team}
                  </span>
                </div>

                <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap', marginBottom: '6px' }}>
                  {rec.tags.map((t, idx) => (
                    <span
                      key={idx}
                      style={{
                        background: 'rgba(255, 255, 255, 0.06)',
                        color: '#cbd5e1',
                        fontSize: '10px',
                        padding: '1px 6px',
                        borderRadius: '4px',
                        fontWeight: '500'
                      }}
                    >
                      {t}
                    </span>
                  ))}
                </div>

                <div style={{ color: '#94a3b8', fontSize: '11px', fontStyle: 'italic', lineHeight: '1.3' }}>
                  "{rec.rationale}"
                </div>
              </div>

              {/* Action Buttons */}
              <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                {isMyTurn && (
                  <button
                    onClick={() => onDraft(p)}
                    style={{
                      flex: '1',
                      background: '#10b981',
                      color: '#fff',
                      border: 'none',
                      padding: '5px 10px',
                      borderRadius: '5px',
                      fontSize: '12px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    ⚡ DRAFT NOW
                  </button>
                )}
                <button
                  onClick={() => isQueued ? onRemoveFromQueue(p['ESPN PlayerID']) : onAddToQueue(p)}
                  style={{
                    flex: isMyTurn ? '0 0 auto' : '1',
                    background: isQueued ? '#334155' : 'transparent',
                    color: isQueued ? '#94a3b8' : '#03dac6',
                    border: `1px solid ${isQueued ? '#475569' : '#03dac6'}`,
                    padding: '5px 10px',
                    borderRadius: '5px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer'
                  }}
                >
                  {isQueued ? '✓ Queued' : '+ Queue'}
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
