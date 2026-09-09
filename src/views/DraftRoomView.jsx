import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { supabase } from '../supabaseClient';

// --- CONFIGURATION ---
const GEMINI_API_KEY = import.meta.env.VITE_GEMINI_API_KEY || 'AIzaSyDQ0eRBz6jSsORZrnG19jR5mzmd0QE0DWg';
const GEMINI_MODEL = 'gemini-2.5-flash';

// Google Cloud Storage for static player data
const GCS_BUCKET = "https://storage.googleapis.com/fantasy-draft-2026";
const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24 hours

// Cached fetch function for GCS data
async function fetchFromGCS(filename, cacheKey) {
  const cachedData = localStorage.getItem(cacheKey);
  const cacheTime = localStorage.getItem(`${cacheKey}_time`);
  
  const isCacheValid = cachedData && cacheTime && 
    (Date.now() - parseInt(cacheTime, 10)) < CACHE_DURATION;
  
  if (isCacheValid) {
    try {
      return JSON.parse(cachedData);
    } catch {
      // ignore parse error and re-fetch
    }
  }
  
  try {
    const response = await fetch(`${GCS_BUCKET}/${filename}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    
    localStorage.setItem(cacheKey, JSON.stringify(data));
    localStorage.setItem(`${cacheKey}_time`, Date.now().toString());
    
    return data;
  } catch (error) {
    console.warn(`Error fetching ${filename} from GCS:`, error);
    return cachedData ? JSON.parse(cachedData) : [];
  }
}

// --- CONSTANTS ---
export const DRAFT_OWNERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"].sort();

const ROSTER_SLOTS = [
  { id: 'C1', label: 'C', eligible: ['C'] },
  { id: 'C2', label: 'C', eligible: ['C'] },
  { id: '1B', label: '1B', eligible: ['1B'] },
  { id: '2B', label: '2B', eligible: ['2B'] },
  { id: '3B', label: '3B', eligible: ['3B'] },
  { id: 'SS', label: 'SS', eligible: ['SS'] },
  { id: 'OF1', label: 'OF', eligible: ['OF'] },
  { id: 'OF2', label: 'OF', eligible: ['OF'] },
  { id: 'OF3', label: 'OF', eligible: ['OF'] },
  { id: 'OF4', label: 'OF', eligible: ['OF'] },
  { id: 'OF5', label: 'OF', eligible: ['OF'] },
  { id: 'DH', label: 'DH', eligible: ['DH', 'C', '1B', '2B', '3B', 'SS', 'OF'] },
  { id: 'SP1', label: 'SP', eligible: ['SP'] },
  { id: 'SP2', label: 'SP', eligible: ['SP'] },
  { id: 'SP3', label: 'SP', eligible: ['SP'] },
  { id: 'SP4', label: 'SP', eligible: ['SP'] },
  { id: 'RP1', label: 'RP', eligible: ['RP'] },
  { id: 'RP2', label: 'RP', eligible: ['RP'] },
  { id: 'BN1', label: 'Bench', eligible: ['ALL'] },
  { id: 'BN2', label: 'Bench', eligible: ['ALL'] },
  { id: 'BN3', label: 'Bench', eligible: ['ALL'] }
];

function generateDefaultDraftOrder() {
  const order = [];
  let overall = 1;
  for (let round = 1; round <= 21; round++) {
    const roundOwners = round % 2 === 1 ? [...DRAFT_OWNERS] : [...DRAFT_OWNERS].reverse();
    roundOwners.forEach((owner, idx) => {
      order.push({
        'Overall Pick': overall,
        'Round': round,
        'Pick': idx + 1,
        'Owner': owner,
        'ESPN PlayerID': null,
        'Selection': null
      });
      overall++;
    });
  }
  return order;
}

// --- GEMINI INTEGRATION ---
async function callGemini(prompt) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;
  
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{
          parts: [{ text: prompt }]
        }]
      })
    });
    
    const data = await response.json();
    
    if (data.candidates && data.candidates[0]) {
      return data.candidates[0].content.parts[0].text;
    }
    return "Analysis unavailable";
  } catch (error) {
    console.error('Gemini API Error:', error);
    return "Analysis unavailable";
  }
}

function formatPlayerStats(player) {
  const isPitcher = player.Position?.includes('SP') || player.Position?.includes('RP');
  if (isPitcher) {
    return `ERA: ${player.ZIPSERA || 'N/A'}, WHIP: ${player.ZIPSWHIP || 'N/A'}, K: ${player.ZIPSK || 'N/A'}, QS: ${player.ZIPSQS || 'N/A'}, SV+H: ${player['ZIPSSV+HDs'] || 'N/A'}`;
  }
  return `R: ${player.ZIPSR || 'N/A'}, HR: ${player.ZIPSHR || 'N/A'}, RBI: ${player.ZIPSRBI || 'N/A'}, SB: ${player.ZIPSSB || 'N/A'}, OBP: ${player.ZIPSOBP || 'N/A'}`;
}

async function generateDraftCommentary(player, owner, pickNum, teamStats) {
  const prompt = `
Context: Fantasy Baseball Draft (2026 Season).

Action: ${owner} picked ${player.Player} (Pick #${pickNum}).

--- DATA PACKET ---
1. PLAYER VALUE: 
   - Position: ${player.Position}
   - Team: ${player.Team}
   - ADP: ${player.ADP || 'N/A'}
   - Stats: ${formatPlayerStats(player)}

2. OWNER PROFILE:
   - Current Team Projections: ${teamStats}

--- TASK ---
Write a witty, sharp reaction (Max 250 words).
- Comment on the value (was this a reach or a steal based on ADP?)
- Does this player fill a need for ${owner}?
- Keep it entertaining and insightful

Format your response as HTML. You can use <b> tags for emphasis and <br> for line breaks.
End with:
<br><br>
<b>Player Details:</b><br>
Position: ${player.Position} | Team: ${player.Team} | ADP: ${player.ADP || 'N/A'}
`;

  return await callGemini(prompt);
}

// --- AUDIO SYSTEM ---
function playOwnerSound(ownerName) {
  if (!ownerName) return;
  try {
    const rawBase = import.meta.env.BASE_URL || '/';
    const base = rawBase.endsWith('/') ? rawBase : `${rawBase}/`;
    const audioPath = `${base}audio/owners/${ownerName.toLowerCase()}.mp3`;
    const audio = new Audio(audioPath);
    audio.play().catch(err => {
      console.log('Audio playback blocked or file not found:', err);
    });
  } catch (error) {
    console.error('Error playing sound:', error);
  }
}

// --- INJURY HELPER ---
function getInjuryIndicator(playerId, playerInfoArray) {
  if (!playerInfoArray || !playerInfoArray.length) return null;
  const pId = String(playerId);
  const info = playerInfoArray.find(i => String(i.player_id) === pId);
  if (!info || !info.injured_status || info.injured_status === 'ACTIVE') return null;

  const status = info.injured_status.toUpperCase();
  let color = '#ff9800';
  let displayText = status;

  if (status === 'DAY_TO_DAY' || status === 'DTD') {
    color = '#ffc107';
    displayText = 'DTD';
  } else if (status.includes('IL') || status === 'OUT') {
    color = '#f44336';
    if (status === 'SEVEN_DAY_IL') displayText = 'IL-7';
    else if (status === 'TEN_DAY_IL') displayText = 'IL-10';
    else if (status === 'FIFTEEN_DAY_IL') displayText = 'IL-15';
    else if (status === 'SIXTY_DAY_IL') displayText = 'IL-60';
    else displayText = 'IL';
  }

  return { color, status: displayText };
}

// --- PLAYER MODAL ---
function PlayerModal({ player, onClose, onOpenInDepthModal }) {
  const [playerNews, setPlayerNews] = useState([]);
  const [playerInfo, setPlayerInfo] = useState(null);
  const [externalLinks, setExternalLinks] = useState(null);
  const [draftHistory, setDraftHistory] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchPlayerData() {
      if (!player) return;
      
      setLoading(true);
      const playerId = String(player['ESPN PlayerID']);

      try {
        const [newsData, infoData, linksData, historyData] = await Promise.all([
          fetchFromGCS('player-news.json', 'gcs_player_news'),
          fetchFromGCS('player-info.json', 'gcs_player_info'),
          fetchFromGCS('player-links.json', 'gcs_player_links'),
          fetchFromGCS('draft-history.json', 'gcs_draft_history')
        ]);
        
        const playerNewsFiltered = (newsData || [])
          .filter(n => String(n.player_id) === playerId)
          .sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified))
          .slice(0, 10);
        
        const playerInfoFiltered = (infoData || []).find(i => String(i.player_id) === playerId);
        const playerLinksFiltered = (linksData || []).find(l => String(l.player_id) === playerId);
        const playerHistoryFiltered = (historyData || [])
          .filter(h => String(h.player_id) === playerId)
          .sort((a, b) => b.year - a.year);

        setPlayerNews(playerNewsFiltered || []);
        setPlayerInfo(playerInfoFiltered || null);
        setExternalLinks(playerLinksFiltered || null);
        setDraftHistory(playerHistoryFiltered || []);
      } catch (error) {
        console.error('Error fetching player data:', error);
      } finally {
        setLoading(false);
      }
    }

    fetchPlayerData();
  }, [player]);

  if (!player) return null;

  const getInjuryColor = (status) => {
    if (!status) return null;
    const s = status.toUpperCase();
    if (s === 'ACTIVE') return '#4caf50';
    if (s === 'DAY_TO_DAY' || s === 'DTD') return '#ffc107';
    if (s.includes('IL') || s === 'OUT') return '#f44336';
    return '#ff9800';
  };

  const getInjuryDisplayText = (status) => {
    if (!status) return 'Unknown';
    const s = status.toUpperCase();
    if (s === 'ACTIVE') return 'Healthy';
    if (s === 'DAY_TO_DAY') return 'DTD';
    if (s === 'SEVEN_DAY_IL') return 'IL-7';
    if (s === 'TEN_DAY_IL') return 'IL-10';
    if (s === 'FIFTEEN_DAY_IL') return 'IL-15';
    if (s === 'SIXTY_DAY_IL') return 'IL-60';
    return status;
  };

  const injuryColor = playerInfo ? getInjuryColor(playerInfo.injured_status) : null;
  const injuryDisplay = playerInfo ? getInjuryDisplayText(playerInfo.injured_status) : null;

  return (
    <div style={styles.modalOverlay} onClick={onClose}>
      <div style={styles.modalContent} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div style={styles.modalHeader}>
          <div>
            <h2 style={{ margin: 0, color: '#fff', fontSize: '28px' }}>
              {player.Player}
            </h2>
            <div style={{ color: '#888', fontSize: '16px', marginTop: '5px' }}>
              {player.Position} • {player.Team}
              {playerInfo && (
                <span style={{
                  marginLeft: '15px',
                  padding: '4px 12px',
                  borderRadius: '4px',
                  background: injuryColor,
                  color: injuryDisplay === 'Healthy' ? '#000' : '#fff',
                  fontWeight: 'bold',
                  fontSize: '12px'
                }}>
                  {injuryDisplay}
                </span>
              )}
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            {onOpenInDepthModal && (
              <button
                onClick={() => {
                  onClose();
                  onOpenInDepthModal(player['ESPN PlayerID'], player.Player);
                }}
                style={{
                  background: '#2563eb',
                  color: '#fff',
                  border: 'none',
                  padding: '6px 12px',
                  borderRadius: '6px',
                  fontSize: '12px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
                title="View full historical matchup box scores and career logs"
              >
                📊 In-Season Log
              </button>
            )}
            <button onClick={onClose} style={styles.modalCloseBtn}>✕</button>
          </div>
        </div>

        {loading ? (
          <div style={{ textAlign: 'center', padding: '40px', color: '#888' }}>
            Loading player data...
          </div>
        ) : (
          <div style={styles.modalBody}>
            {/* Season Outlook */}
            {playerInfo && playerInfo.seasonOutlook && (
              <div style={{ ...styles.modalSection, borderLeft: '4px solid var(--accent)' }}>
                <h3 style={styles.modalSectionTitle}>2026 Season Outlook</h3>
                <p style={{ margin: 0, color: '#ccc', fontSize: '14px', lineHeight: '1.6' }}>
                  {playerInfo.seasonOutlook}
                </p>
              </div>
            )}

            {/* Recent News */}
            <div style={styles.modalSection}>
              <h3 style={styles.modalSectionTitle}>Recent News</h3>
              {playerNews.length === 0 ? (
                <p style={{ color: '#666', fontSize: '14px', fontStyle: 'italic' }}>
                  No recent news available
                </p>
              ) : (
                <div style={{ maxHeight: '250px', overflowY: 'auto' }}>
                  {playerNews.map((news, idx) => (
                    <div key={idx} style={styles.newsItem}>
                      <div style={{ color: '#888', fontSize: '12px', marginBottom: '4px' }}>
                        {new Date(news.lastModified).toLocaleDateString('en-US', { 
                          month: 'short', 
                          day: 'numeric', 
                          year: 'numeric' 
                        })}
                      </div>
                      <div style={{ color: '#fff', fontSize: '14px', fontWeight: 'bold', marginBottom: '6px' }}>
                        {news.headline}
                      </div>
                      <div style={{ color: '#ccc', fontSize: '13px', lineHeight: '1.5' }}>
                        {news.story}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* External Links */}
            <div style={styles.modalSection}>
              <h3 style={styles.modalSectionTitle}>External Resources</h3>
              <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                {externalLinks?.baseball_reference_url && (
                  <a 
                    href={externalLinks.baseball_reference_url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    style={styles.externalLink}
                  >
                    Baseball Reference
                  </a>
                )}
                {externalLinks?.fangraphs_url && (
                  <a 
                    href={externalLinks.fangraphs_url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    style={styles.externalLink}
                  >
                    FanGraphs
                  </a>
                )}
                {externalLinks?.baseball_savant_url && (
                  <a 
                    href={externalLinks.baseball_savant_url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    style={styles.externalLink}
                  >
                    Baseball Savant
                  </a>
                )}
                {externalLinks?.espn_url && (
                  <a 
                    href={externalLinks.espn_url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    style={styles.externalLink}
                  >
                    ESPN
                  </a>
                )}
                {!externalLinks && (
                  <p style={{ color: '#666', fontSize: '14px', fontStyle: 'italic' }}>
                    No external links available
                  </p>
                )}
              </div>
            </div>

            {/* Draft History */}
            <div style={styles.modalSection}>
              <h3 style={styles.modalSectionTitle}>Draft History in League (2012-2025)</h3>
              {draftHistory.length === 0 ? (
                <p style={{ color: '#666', fontSize: '14px', fontStyle: 'italic' }}>
                  No previous draft history in this league
                </p>
              ) : (
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                  <thead>
                    <tr style={{ borderBottom: '2px solid #444' }}>
                      <th style={{ textAlign: 'left', padding: '8px', color: 'var(--highlight)' }}>Year</th>
                      <th style={{ textAlign: 'left', padding: '8px', color: 'var(--highlight)' }}>Owner</th>
                      <th style={{ textAlign: 'left', padding: '8px', color: 'var(--highlight)' }}>Round</th>
                      <th style={{ textAlign: 'left', padding: '8px', color: 'var(--highlight)' }}>Pick #</th>
                    </tr>
                  </thead>
                  <tbody>
                    {draftHistory.map((entry, idx) => (
                      <tr key={idx} style={{ borderBottom: '1px solid #333' }}>
                        <td style={{ padding: '8px', color: '#ccc' }}>{entry.Year}</td>
                        <td style={{ padding: '8px', color: '#fff', fontWeight: 'bold' }}>{entry.Team_ID}</td>
                        <td style={{ padding: '8px', color: '#ccc' }}>{entry.Round}</td>
                        <td style={{ padding: '8px', color: '#ccc' }}>#{entry.Pick_Overall}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// --- HELPER: Clickable player name ---
function PlayerNameButton({ player, onClick, style = {} }) {
  return (
    <span
      onClick={(e) => {
        e.stopPropagation();
        onClick(player);
      }}
      style={{
        cursor: 'pointer',
        textDecoration: 'none',
        transition: 'opacity 0.2s',
        ...style
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.opacity = '0.8';
        e.currentTarget.style.textDecoration = 'underline';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.opacity = '1';
        e.currentTarget.style.textDecoration = 'none';
      }}
    >
      {player.Player}
    </span>
  );
}

// --- MODE SELECTION MODAL ---
function ModeSelectionModal({ onSelectMode }) {
  return (
    <div style={styles.loginOverlay}>
      <div style={styles.loginBox}>
        <h1 style={{ fontSize: '36px', marginBottom: '10px', color: 'var(--accent)' }}>
          ⚾ Hefty War Room 2026
        </h1>
        <p style={{ marginBottom: '30px', color: '#888' }}>
          Select draft mode to continue
        </p>
        <div style={{ display: 'flex', gap: '20px', marginTop: '30px' }}>
          <button 
            onClick={() => onSelectMode('test')}
            style={{
              ...styles.loginButton,
              background: '#ffc107',
              flex: 1
            }}
          >
            🧪 Test Mode
            <div style={{ fontSize: '12px', marginTop: '5px', fontWeight: 'normal' }}>
              Draft for all owners
            </div>
          </button>
          <button 
            onClick={() => onSelectMode('live')}
            style={{
              ...styles.loginButton,
              background: '#03dac6',
              flex: 1
            }}
          >
            🎯 Live Draft
            <div style={{ fontSize: '12px', marginTop: '5px', fontWeight: 'normal' }}>
              Official draft mode
            </div>
          </button>
        </div>
        <div style={{ marginTop: '20px', fontSize: '12px', color: '#888' }}>
          Test Mode: Draft for any owner, picks simulate locally
          <br />
          Live Mode: Draft only on your turn, picks sync to database
        </div>
      </div>
    </div>
  );
}

// --- LOGIN MODAL ---
function LoginModal({ owners, onLogin }) {
  const [selectedOwner, setSelectedOwner] = useState(owners[0] || "");

  return (
    <div style={styles.loginOverlay}>
      <div style={styles.loginBox}>
        <h1 style={{ fontSize: '36px', marginBottom: '10px', color: 'var(--accent)' }}>
          ⚾ Hefty War Room 2026
        </h1>
        <p style={{ marginBottom: '30px', color: '#888' }}>Select your team to enter the draft</p>
        <div>
          <select 
            value={selectedOwner} 
            onChange={(e) => setSelectedOwner(e.target.value)}
            style={styles.loginSelect}
          >
            {owners.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
        </div>
        <button 
          onClick={() => onLogin(selectedOwner)}
          style={styles.loginButton}
        >
          Enter Draft Room
        </button>
      </div>
    </div>
  );
}

// --- COUNTDOWN TIMER ---
function CountdownTimer({ pickStartTime }) {
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
function Ticker({ recentPicks, players }) {
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
function OnDeckSidebar({ upcomingPicks, currentUser }) {
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
function TeamNeedsSummary({ myPicks, players }) {
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
function RecentActivityWidget({ recentPicks, players, onPlayerClick, playerInfo }) {
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
                  <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
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
function QueuePreviewWidget({ queue, onPlayerClick, playerInfo }) {
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
              <div key={player['ESPN PlayerID']} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px' }}>
                <PlayerNameButton 
                  player={player} 
                  onClick={onPlayerClick}
                  style={{ color: injury ? injury.color : '#fff', fontWeight: 'bold' }}
                />
                <span style={{ color: '#888', fontSize: '10px' }}>{player.Position} • {player.Team}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// --- PLAYER POOL PANEL ---
function PlayerPoolPanel({ players, onDraft, isMyTurn, queue, onAddToQueue, onRemoveFromQueue, draftMode, testModePicks, onPlayerClick, playerInfo }) {
  const [sortConfig, setSortConfig] = useState({ key: 'ADP', direction: 'asc' });
  const [filterPos, setFilterPos] = useState('');
  const [searchText, setSearchText] = useState('');

  const sortedPlayers = useMemo(() => {
    let filtered = [...players].filter(p => {
      if (draftMode === 'test' && testModePicks) {
        const isDrafted = testModePicks.some(pick => String(pick['ESPN PlayerID']) === String(p['ESPN PlayerID']));
        if (isDrafted) return false;
      }
      return p.Availability === 'Available' || !p.Availability;
    });
    
    if (searchText) {
      filtered = filtered.filter(p => 
        p.Player?.toLowerCase().includes(searchText.toLowerCase()) ||
        p.Team?.toLowerCase().includes(searchText.toLowerCase())
      );
    }
    
    if (filterPos) {
      filtered = filtered.filter(p => p.Position?.includes(filterPos));
    }
    
    filtered.sort((a, b) => {
      const aVal = a[sortConfig.key] ?? '';
      const bVal = b[sortConfig.key] ?? '';
      const aNum = parseFloat(aVal);
      const bNum = parseFloat(bVal);
      
      let comparison = 0;
      if (!isNaN(aNum) && !isNaN(bNum)) {
        comparison = aNum - bNum;
      } else {
        comparison = String(aVal).localeCompare(String(bVal));
      }
      
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
    
    return filtered;
  }, [players, sortConfig, filterPos, searchText, draftMode, testModePicks]);

  const requestSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  const isQueued = (playerId) => queue.some(p => String(p['ESPN PlayerID']) === String(playerId));

  return (
    <div style={{ display: 'flex', gridColumn: '1 / -1', gap: '20px', height: '100%' }}>
      {/* Main Pool */}
      <div style={{ ...styles.wrColumn, flex: '3' }}>
        <div style={styles.wrHeader}>
          <span>Available Players {draftMode === 'test' && <span style={{ color: '#ffc107', fontSize: '14px' }}>(TEST MODE)</span>}</span>
          <div style={{ display: 'flex', gap: '10px' }}>
            <select 
              value={filterPos} 
              onChange={e => setFilterPos(e.target.value)}
              style={styles.filterControl}
            >
              <option value="">All Pos</option>
              <option value="C">C</option>
              <option value="1B">1B</option>
              <option value="2B">2B</option>
              <option value="3B">3B</option>
              <option value="SS">SS</option>
              <option value="OF">OF</option>
              <option value="SP">SP</option>
              <option value="RP">RP</option>
            </select>
            <input 
              type="text"
              placeholder="Search player or team..."
              value={searchText}
              onChange={e => setSearchText(e.target.value)}
              style={styles.filterControl}
            />
          </div>
        </div>
        
        <div style={styles.poolTableContainer}>
          <table style={styles.table}>
            <thead style={styles.tableHead}>
              <tr>
                <th style={styles.th} width="75">Action</th>
                <th style={styles.th} onClick={() => requestSort('Player')}>Player</th>
                <th style={styles.th} onClick={() => requestSort('Position')}>Pos</th>
                <th style={styles.th} onClick={() => requestSort('Team')}>Team</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSR')}>R</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSHR')}>HR</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSRBI')}>RBI</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSSB')}>SB</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSOBP')}>OBP</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSK')}>K</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSQS')}>QS</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSERA')}>ERA</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSWHIP')}>WHIP</th>
                <th style={styles.th} onClick={() => requestSort('ZIPSSV+HDs')}>SV+H</th>
              </tr>
            </thead>
            <tbody>
              {sortedPlayers.slice(0, 200).map(p => {
                const queued = isQueued(p['ESPN PlayerID']);
                const isPitcher = p.Position?.includes('SP') || p.Position?.includes('RP');
                const injury = getInjuryIndicator(p['ESPN PlayerID'], playerInfo);
                
                return (
                  <tr key={p['ESPN PlayerID']} style={styles.tableRow}>
                    <td style={styles.td}>
                      {isMyTurn && (
                        <button onClick={() => onDraft(p)} style={styles.btnDraft}>
                          DRAFT
                        </button>
                      )}
                      <button 
                        onClick={() => queued ? onRemoveFromQueue(p['ESPN PlayerID']) : onAddToQueue(p)}
                        style={queued ? styles.btnStarActive : styles.btnStar}
                        title={queued ? "Remove from queue" : "Add to queue"}
                      >
                        {queued ? '★' : '☆'}
                      </button>
                    </td>
                    <td style={styles.td}>
                      <PlayerNameButton 
                        player={p} 
                        onClick={onPlayerClick}
                        style={{ color: injury ? injury.color : '#fff', fontWeight: 'bold' }}
                      />
                      {injury && (
                        <span style={{
                          marginLeft: '8px',
                          padding: '2px 6px',
                          borderRadius: '3px',
                          background: injury.color,
                          color: '#000',
                          fontSize: '10px',
                          fontWeight: 'bold'
                        }}>
                          {injury.status}
                        </span>
                      )}
                    </td>
                    <td style={styles.td}>{p.Position}</td>
                    <td style={styles.td}>{p.Team}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSR || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSHR || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSRBI || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSSB || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSOBP || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSK || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSQS || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSERA || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSWHIP || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p['ZIPSSV+HDs'] || '-')}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Queue */}
      <div style={{ ...styles.wrColumn, flex: '1' }}>
        <div style={styles.wrHeader}>My Queue ({queue.length})</div>
        <div style={{ overflowY: 'auto', flexGrow: 1 }}>
          {queue.length === 0 ? (
            <div style={{ padding: '20px', color: '#666', textAlign: 'center', fontSize: '13px' }}>
              Click ☆ next to any player in the pool to queue them up.
            </div>
          ) : (
            queue.map(p => (
              <div key={p['ESPN PlayerID']} style={styles.queueItem}>
                <div>
                  <div style={{ fontWeight: 'bold' }}>{p.Player}</div>
                  <div style={{ fontSize: '12px', color: '#888' }}>{p.Position} - {p.Team}</div>
                </div>
                <button 
                  onClick={() => onRemoveFromQueue(p['ESPN PlayerID'])}
                  style={styles.btnStarActive}
                  title="Remove"
                >
                  ✕
                </button>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

// --- ROSTER MANAGER PANEL ---
function RosterManagerPanel({ allPicks, players, currentUser }) {
  const [selectedOwner, setSelectedOwner] = useState(currentUser || DRAFT_OWNERS[0]);
  const [assignments, setAssignments] = useState({});

  const ownerPicks = allPicks.filter(p => p.Owner === selectedOwner && p['ESPN PlayerID']);
  const ownerPlayers = ownerPicks.map(pick => 
    players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']))
  ).filter(Boolean);

  const slottedPlayerIds = new Set(
    Object.values(assignments).filter(Boolean).map(p => String(p['ESPN PlayerID']))
  );

  const unslottedPlayers = ownerPlayers.filter(p => !slottedPlayerIds.has(String(p['ESPN PlayerID'])));

  const totals = useMemo(() => {
    const slots = Object.values(assignments).filter(Boolean);
    const batters = slots.filter(p => !p.Position?.includes('SP') && !p.Position?.includes('RP'));
    const pitchers = slots.filter(p => p.Position?.includes('SP') || p.Position?.includes('RP'));
    
    return {
      r: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSR) || 0), 0),
      hr: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSHR) || 0), 0),
      rbi: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSRBI) || 0), 0),
      sb: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSSB) || 0), 0),
      obp: batters.length > 0 ? 
        batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSOBP) || 0), 0) / batters.length : 0,
      k: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSK) || 0), 0),
      qs: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSQS) || 0), 0),
      era: pitchers.length > 0 ?
        pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSERA) || 0), 0) / pitchers.length : 0,
      whip: pitchers.length > 0 ?
        pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSWHIP) || 0), 0) / pitchers.length : 0,
      sv: pitchers.reduce((sum, p) => sum + (parseFloat(p['ZIPSSV+HDs']) || 0), 0)
    };
  }, [assignments]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <span>Roster Manager</span>
        <div style={{ fontSize: '14px', color: '#ccc', display: 'flex', alignItems: 'center', gap: '8px' }}>
          Viewing: 
          <select 
            value={selectedOwner}
            onChange={e => setSelectedOwner(e.target.value)}
            style={styles.filterControl}
          >
            {DRAFT_OWNERS.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
        </div>
      </div>

      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th} width="60">Slot</th>
              <th style={styles.th} width="260">Player</th>
              <th style={styles.th}>R</th>
              <th style={styles.th}>HR</th>
              <th style={styles.th}>RBI</th>
              <th style={styles.th}>SB</th>
              <th style={styles.th}>OBP</th>
              <th style={styles.th}>K</th>
              <th style={styles.th}>QS</th>
              <th style={styles.th}>ERA</th>
              <th style={styles.th}>WHIP</th>
              <th style={styles.th}>SV+H</th>
            </tr>
          </thead>
          <tbody>
            {ROSTER_SLOTS.map(slot => {
              const player = assignments[slot.id];
              const isPitcher = player?.Position?.includes('SP') || player?.Position?.includes('RP');
              
              const eligiblePlayers = ownerPlayers.filter(p => {
                if (player && String(p['ESPN PlayerID']) === String(player['ESPN PlayerID'])) return true;
                if (slottedPlayerIds.has(String(p['ESPN PlayerID']))) return false;
                const pos = p.Position || '';
                return slot.eligible.includes('ALL') || slot.eligible.some(e => pos.includes(e));
              });
              
              return (
                <tr key={slot.id} style={styles.tableRow}>
                  <td style={{ ...styles.td, color: '#888', fontWeight: 'bold' }}>{slot.label}</td>
                  <td style={styles.td}>
                    <select 
                      style={styles.rosterSelect}
                      value={player ? player['ESPN PlayerID'] : ''}
                      onChange={(e) => {
                        if (e.target.value === '') {
                          setAssignments(prev => {
                            const newAssignments = { ...prev };
                            delete newAssignments[slot.id];
                            return newAssignments;
                          });
                        } else {
                          const newPlayer = ownerPlayers.find(p => 
                            String(p['ESPN PlayerID']) === String(e.target.value)
                          );
                          if (newPlayer) {
                            setAssignments(prev => ({ ...prev, [slot.id]: newPlayer }));
                          }
                        }
                      }}
                    >
                      <option value="">-- Empty --</option>
                      {eligiblePlayers.map(p => (
                        <option key={p['ESPN PlayerID']} value={p['ESPN PlayerID']}>
                          {p.Player} ({p.Position})
                        </option>
                      ))}
                    </select>
                  </td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSR || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSHR || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSRBI || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSSB || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? parseFloat(player.ZIPSOBP || 0).toFixed(3) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? Math.round(player.ZIPSK || 0) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? Math.round(player.ZIPSQS || 0) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? parseFloat(player.ZIPSERA || 0).toFixed(2) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? parseFloat(player.ZIPSWHIP || 0).toFixed(3) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? Math.round(player['ZIPSSV+HDs'] || 0) : '-'}</td>
                </tr>
              );
            })}
          </tbody>
          <tfoot>
            <tr style={{ background: '#444', fontWeight: 'bold', color: '#fff' }}>
              <td colSpan="2" style={{ ...styles.td, textAlign: 'right', paddingRight: '10px' }}>
                TOTALS:
              </td>
              <td style={styles.td}>{Math.round(totals.r)}</td>
              <td style={styles.td}>{Math.round(totals.hr)}</td>
              <td style={styles.td}>{Math.round(totals.rbi)}</td>
              <td style={styles.td}>{Math.round(totals.sb)}</td>
              <td style={styles.td}>{totals.obp.toFixed(3)}</td>
              <td style={styles.td}>{Math.round(totals.k)}</td>
              <td style={styles.td}>{Math.round(totals.qs)}</td>
              <td style={styles.td}>{totals.era.toFixed(2)}</td>
              <td style={styles.td}>{totals.whip.toFixed(3)}</td>
              <td style={styles.td}>{Math.round(totals.sv)}</td>
            </tr>
          </tfoot>
        </table>
        
        {/* Unslotted Players */}
        {unslottedPlayers.length > 0 && (
          <div style={{
            marginTop: '20px',
            padding: '15px',
            background: '#222',
            borderRadius: '8px',
            borderLeft: '4px solid #ffc107'
          }}>
            <div style={{ 
              fontWeight: 'bold', 
              color: 'var(--highlight)', 
              marginBottom: '10px',
              fontSize: '14px'
            }}>
              Unslotted Players ({unslottedPlayers.length})
            </div>
            <div style={{ 
              color: '#ccc', 
              fontSize: '13px',
              lineHeight: '1.8'
            }}>
              {unslottedPlayers.map((p, idx) => (
                <span key={p['ESPN PlayerID']}>
                  <strong style={{ color: '#fff' }}>{p.Player}</strong>
                  <span style={{ color: '#888' }}> ({p.Position})</span>
                  {idx < unslottedPlayers.length - 1 && ', '}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// --- MY PICKS PANEL ---
function MyPicksPanel({ allPicks, players, currentUser }) {
  const userPicks = allPicks.filter(p => p.Owner === currentUser);
  const filledPicks = userPicks.filter(p => p['ESPN PlayerID']);
  
  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <span>My Draft Picks</span>
        <span style={{ fontSize: '14px', color: '#888' }}>
          {filledPicks.length} of {userPicks.length} picks made
        </span>
      </div>
      
      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th}>Pick #</th>
              <th style={styles.th}>Round</th>
              <th style={styles.th}>Status</th>
              <th style={styles.th}>Player</th>
              <th style={styles.th}>Position</th>
              <th style={styles.th}>Team</th>
            </tr>
          </thead>
          <tbody>
            {userPicks.map(pick => {
              const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
              const isFilled = Boolean(player);
              
              return (
                <tr key={pick['Overall Pick']} style={styles.tableRow}>
                  <td style={styles.td}>
                    <strong style={{ color: isFilled ? '#03dac6' : '#888' }}>
                      #{pick['Overall Pick']}
                    </strong>
                  </td>
                  <td style={styles.td}>{pick.Round}</td>
                  <td style={styles.td}>
                    <span style={{
                      padding: '4px 8px',
                      borderRadius: '4px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      background: isFilled ? '#03dac620' : '#88888820',
                      color: isFilled ? '#03dac6' : '#888'
                    }}>
                      {isFilled ? '✓ FILLED' : 'UPCOMING'}
                    </span>
                  </td>
                  <td style={styles.td}>
                    {player ? (
                      <strong style={{ color: '#fff' }}>{player.Player}</strong>
                    ) : (
                      <span style={{ color: '#666', fontStyle: 'italic' }}>Not yet selected</span>
                    )}
                  </td>
                  <td style={styles.td}>{player?.Position || '-'}</td>
                  <td style={styles.td}>{player?.Team || '-'}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- ANALYSIS HISTORY PANEL ---
function AnalysisHistoryPanel({ analysisHistory }) {
  const [filterOwner, setFilterOwner] = useState('');
  const [searchText, setSearchText] = useState('');

  const filteredHistory = useMemo(() => {
    return analysisHistory.filter(item => {
      const ownerMatch = !filterOwner || item.owner === filterOwner;
      const textMatch = !searchText || 
        item.commentary.toLowerCase().includes(searchText.toLowerCase()) ||
        item.playerName.toLowerCase().includes(searchText.toLowerCase());
      return ownerMatch && textMatch;
    });
  }, [analysisHistory, filterOwner, searchText]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
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
      
      <div style={styles.poolTableContainer}>
        {filteredHistory.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '40px', color: '#666' }}>
            {analysisHistory.length === 0 ? 
              'No analyses generated yet. Make draft picks to see Gemini AI commentary!' :
              'No analyses match your filters.'
            }
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
            {filteredHistory.map((item, idx) => (
              <div key={idx} style={{
                background: '#222',
                padding: '15px',
                borderRadius: '8px',
                borderLeft: '4px solid var(--accent)'
              }}>
                <div style={{ 
                  display: 'flex', 
                  justifyContent: 'space-between', 
                  marginBottom: '10px',
                  paddingBottom: '10px',
                  borderBottom: '1px solid #333'
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
                  <div style={{ color: '#666', fontSize: '11px', textAlign: 'right' }}>
                    {item.timestamp}
                  </div>
                </div>
                <div 
                  style={{ 
                    color: '#ccc', 
                    fontSize: '13px', 
                    lineHeight: '1.6',
                    fontStyle: 'italic'
                  }}
                  dangerouslySetInnerHTML={{ __html: item.commentary }}
                />
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// --- DRAFT LOG PANEL ---
function DraftLogPanel({ allPicks, players, onPlayerClick }) {
  const [filterOwner, setFilterOwner] = useState('');
  const [filterPosition, setFilterPosition] = useState('');

  const completedPicks = allPicks.filter(p => p['ESPN PlayerID']);

  const filteredPicks = useMemo(() => {
    return completedPicks.filter(pick => {
      const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
      if (!player) return false;
      
      const ownerMatch = !filterOwner || pick.Owner === filterOwner;
      const posMatch = !filterPosition || player.Position?.includes(filterPosition);
      
      return ownerMatch && posMatch;
    });
  }, [completedPicks, players, filterOwner, filterPosition]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <span>Draft Log ({completedPicks.length} picks completed)</span>
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
          <select 
            value={filterPosition}
            onChange={e => setFilterPosition(e.target.value)}
            style={styles.filterControl}
          >
            <option value="">All Positions</option>
            <option value="C">C</option>
            <option value="1B">1B</option>
            <option value="2B">2B</option>
            <option value="3B">3B</option>
            <option value="SS">SS</option>
            <option value="OF">OF</option>
            <option value="SP">SP</option>
            <option value="RP">RP</option>
          </select>
        </div>
      </div>
      
      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th}>Pick #</th>
              <th style={styles.th}>Round</th>
              <th style={styles.th}>Owner</th>
              <th style={styles.th}>Player</th>
              <th style={styles.th}>Pos</th>
              <th style={styles.th}>Team</th>
              <th style={styles.th}>R</th>
              <th style={styles.th}>HR</th>
              <th style={styles.th}>RBI</th>
              <th style={styles.th}>SB</th>
              <th style={styles.th}>OBP</th>
              <th style={styles.th}>K</th>
              <th style={styles.th}>QS</th>
              <th style={styles.th}>ERA</th>
              <th style={styles.th}>WHIP</th>
              <th style={styles.th}>SV+H</th>
            </tr>
          </thead>
          <tbody>
            {filteredPicks.map(pick => {
              const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
              if (!player) return null;
              
              const isPitcher = player.Position?.includes('SP') || player.Position?.includes('RP');
              
              return (
                <tr key={pick['Overall Pick']} style={styles.tableRow}>
                  <td style={styles.td}>
                    <strong style={{ color: 'var(--highlight)' }}>#{pick['Overall Pick']}</strong>
                  </td>
                  <td style={styles.td}>{pick.Round}</td>
                  <td style={styles.td}>
                    <strong style={{ color: '#fff' }}>{pick.Owner}</strong>
                  </td>
                  <td style={styles.td}>
                    <PlayerNameButton 
                      player={player} 
                      onClick={onPlayerClick}
                      style={{ color: '#fff', fontWeight: 'bold' }}
                    />
                  </td>
                  <td style={styles.td}>{player.Position}</td>
                  <td style={styles.td}>{player.Team}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSR || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSHR || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSRBI || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSSB || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSOBP || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSK || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSQS || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSERA || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSWHIP || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player['ZIPSSV+HDs'] || '-')}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- STANDINGS PANEL ---
function StandingsPanel({ allPicks, players }) {
  const standings = useMemo(() => {
    return DRAFT_OWNERS.map(owner => {
      const ownerPicks = allPicks.filter(p => p.Owner === owner && p['ESPN PlayerID']);
      const ownerPlayers = ownerPicks.map(pick => 
        players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']))
      ).filter(Boolean);

      const batters = ownerPlayers.filter(p => !p.Position?.includes('SP') && !p.Position?.includes('RP'));
      const pitchers = ownerPlayers.filter(p => p.Position?.includes('SP') || p.Position?.includes('RP'));

      return {
        owner,
        picksCount: ownerPicks.length,
        r: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSR) || 0), 0),
        hr: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSHR) || 0), 0),
        rbi: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSRBI) || 0), 0),
        sb: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSSB) || 0), 0),
        obp: batters.length > 0 ? 
          batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSOBP) || 0), 0) / batters.length : 0,
        k: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSK) || 0), 0),
        qs: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSQS) || 0), 0),
        era: pitchers.length > 0 ?
          pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSERA) || 0), 0) / pitchers.length : 0,
        whip: pitchers.length > 0 ?
          pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSWHIP) || 0), 0) / pitchers.length : 0,
        sv: pitchers.reduce((sum, p) => sum + (parseFloat(p['ZIPSSV+HDs']) || 0), 0)
      };
    });
  }, [allPicks, players]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>Projected Standings from Drafted Rosters</div>
      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th}>Owner</th>
              <th style={styles.th}>Picks</th>
              <th style={styles.th}>Proj R</th>
              <th style={styles.th}>Proj HR</th>
              <th style={styles.th}>Proj RBI</th>
              <th style={styles.th}>Proj SB</th>
              <th style={styles.th}>Proj OBP</th>
              <th style={styles.th}>Proj K</th>
              <th style={styles.th}>Proj QS</th>
              <th style={styles.th}>Proj ERA</th>
              <th style={styles.th}>Proj WHIP</th>
              <th style={styles.th}>Proj SV+H</th>
            </tr>
          </thead>
          <tbody>
            {standings.map(s => (
              <tr key={s.owner} style={styles.tableRow}>
                <td style={{ ...styles.td, fontWeight: 'bold', color: 'var(--highlight)' }}>{s.owner}</td>
                <td style={styles.td}>{s.picksCount}</td>
                <td style={styles.td}>{Math.round(s.r)}</td>
                <td style={styles.td}>{Math.round(s.hr)}</td>
                <td style={styles.td}>{Math.round(s.rbi)}</td>
                <td style={styles.td}>{Math.round(s.sb)}</td>
                <td style={styles.td}>{s.obp.toFixed(3)}</td>
                <td style={styles.td}>{Math.round(s.k)}</td>
                <td style={styles.td}>{Math.round(s.qs)}</td>
                <td style={styles.td}>{s.era > 0 ? s.era.toFixed(2) : '-'}</td>
                <td style={styles.td}>{s.whip > 0 ? s.whip.toFixed(3) : '-'}</td>
                <td style={styles.td}>{Math.round(s.sv)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- MAIN DRAFT ROOM VIEW ---
export default function DraftRoomView({ onOpenPlayerModal, onSwitchView }) {
  const [draftMode, setDraftMode] = useState(() => localStorage.getItem('draftMode') || null);
  const [currentUser, setCurrentUser] = useState(() => localStorage.getItem('draftUser') || null);
  const [players, setPlayers] = useState([]);
  const [picks, setPicks] = useState([]);
  const [queue, setQueue] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem('draft_queue') || '[]');
    } catch {
      return [];
    }
  });
  const [pickStartTime, setPickStartTime] = useState(() => Date.now());
  const [activeTab, setActiveTab] = useState('Pool');
  const [showDashboard, setShowDashboard] = useState(false);
  
  // Test mode state
  const [testModePicks, setTestModePicks] = useState([]);
  
  // AI Commentary state
  const [lastPickCommentary, setLastPickCommentary] = useState("Draft has not started.");
  const [generatingCommentary, setGeneratingCommentary] = useState(false);
  
  // Analysis history storage
  const [analysisHistory, setAnalysisHistory] = useState([]);
  
  // Player modal state
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [playerInfo, setPlayerInfo] = useState([]);

  // Audio state
  const [audioEnabled, setAudioEnabled] = useState(false);
  const lastAnnouncedPickRef = useRef(null);
  const playersRef = useRef(players);
  useEffect(() => {
    playersRef.current = players;
  }, [players]);

  const syncSupabase = import.meta.env.VITE_SYNC_SUPABASE_DRAFT === 'true';

  const displayPicks = useMemo(() => {
    return draftMode === 'test' && testModePicks.length > 0 ? testModePicks : picks;
  }, [draftMode, testModePicks, picks]);

  const currentPick = useMemo(() => {
    return displayPicks.find(p => !p['ESPN PlayerID']);
  }, [displayPicks]);

  const currentPickId = currentPick?.['Overall Pick'];
  const currentPickOwner = currentPick?.Owner;

  // Sound effect when current pick changes
  useEffect(() => {
    if (currentPickOwner && audioEnabled && currentPickId) {
      if (currentPickId !== lastAnnouncedPickRef.current) {
        lastAnnouncedPickRef.current = currentPickId;
        playOwnerSound(currentPickOwner);
      }
    }
  }, [currentPickId, currentPickOwner, audioEnabled]);

  // Commentary callback - stable reference using playersRef
  const handleNewPick = useCallback(async (pick) => {
    const player = playersRef.current.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
    if (!player) return;
    
    setGeneratingCommentary(true);
    
    const teamStats = "Stats calculation pending";
    const commentary = await generateDraftCommentary(
      player,
      pick.Owner,
      pick['Overall Pick'],
      teamStats
    );
    
    setLastPickCommentary(commentary);
    setGeneratingCommentary(false);
    
    const historyEntry = {
      pickNumber: pick['Overall Pick'],
      round: pick.Round,
      owner: pick.Owner,
      playerName: player.Player,
      position: player.Position,
      team: player.Team,
      commentary: commentary,
      timestamp: new Date().toLocaleTimeString()
    };
    setAnalysisHistory(prev => [...prev, historyEntry]);
  }, []);

  useEffect(() => {
    if (!draftMode) return;
    let isCancelled = false;

    const loadData = async () => {
      let pData = null;
      let dData = null;

      // Only attempt Supabase in Live Mode if sync is explicitly enabled (Supabase endpoint is currently paused)
      if (draftMode === 'live' && syncSupabase) {
        try {
          const pRes = await supabase.from('player-pool').select('*');
          pData = pRes?.data;
          const dRes = await supabase.from('draft-order').select('*').order('Overall Pick', { ascending: true });
          dData = dRes?.data;
        } catch (err) {
          console.warn('Supabase fetch failed or unavailable, using fallback:', err);
        }
      }

      if (!pData || pData.length === 0) {
        pData = await fetchFromGCS('player-pool.json', 'gcs_player_pool');
      }

      if (!dData || dData.length === 0) {
        dData = generateDefaultDraftOrder();
      }

      const iData = await fetchFromGCS('player-info.json', 'gcs_player_info');
      
      if (!isCancelled) {
        if (pData) setPlayers(pData);
        if (dData) setPicks(dData);
        if (iData) setPlayerInfo(iData);
      }
    };

    loadData();

    if (draftMode === 'live' && syncSupabase) {
      const channel = supabase
        .channel('draft_updates')
        .on('postgres_changes', { 
          event: 'UPDATE', 
          schema: 'public', 
          table: 'draft-order' 
        }, (payload) => {
          setPicks(curr => {
            const updated = curr.map(p => 
              p['Overall Pick'] === payload.new['Overall Pick'] ? payload.new : p
            );
            
            if (payload.new['ESPN PlayerID']) {
              handleNewPick(payload.new);
            }
            
            return updated;
          });
          setPickStartTime(Date.now());
        })
        .subscribe();
        
      return () => {
        isCancelled = true;
        supabase.removeChannel(channel);
      };
    }

    return () => {
      isCancelled = true;
    };
  }, [draftMode, syncSupabase, handleNewPick]);

  const handleModeSelect = (mode) => {
    localStorage.setItem('draftMode', mode);
    setDraftMode(mode);
  };

  const handleLogin = (user) => {
    localStorage.setItem('draftUser', user);
    setCurrentUser(user);
  };

  const handleResetMode = () => {
    localStorage.removeItem('draftMode');
    setDraftMode(null);
  };

  const handleResetUser = () => {
    localStorage.removeItem('draftUser');
    setCurrentUser(null);
  };

  const lastPick = displayPicks.filter(p => p['ESPN PlayerID']).slice(-1)[0];
  const lastPickPlayer = lastPick ? players.find(p => String(p['ESPN PlayerID']) === String(lastPick['ESPN PlayerID'])) : null;
  
  const isMyTurn = (currentPick && currentUser && currentPick.Owner === currentUser) || draftMode === 'test';

  const upcomingPicks = useMemo(() => {
    const currentIdx = displayPicks.findIndex(p => !p['ESPN PlayerID']);
    if (currentIdx === -1) return [];
    return displayPicks.slice(currentIdx + 1, currentIdx + 21);
  }, [displayPicks]);

  const handleDraft = async (player) => {
    if (!isMyTurn && draftMode !== 'test') return alert("Not your turn!");
    if (!window.confirm(`Draft ${player.Player}${draftMode === 'test' ? ' (Test Mode)' : ''}?`)) return;

    const newQueue = queue.filter(p => String(p['ESPN PlayerID']) !== String(player['ESPN PlayerID']));
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));

    if (draftMode === 'test' || !syncSupabase) {
      setPickStartTime(Date.now());
      const basePicks = testModePicks.length > 0 ? testModePicks : picks;
      
      setTestModePicks(basePicks.map(p => 
        p['Overall Pick'] === currentPick['Overall Pick'] 
          ? { ...p, 'ESPN PlayerID': player['ESPN PlayerID'], 'Selection': player.Player }
          : p
      ));
      
      setPlayers(curr => curr.map(p => 
        String(p['ESPN PlayerID']) === String(player['ESPN PlayerID'])
          ? { ...p, Availability: currentPick.Owner }
          : p
      ));
      
      await handleNewPick({ ...currentPick, 'ESPN PlayerID': player['ESPN PlayerID'], Round: currentPick.Round });
    } else {
      const { error } = await supabase
        .from('draft-order')
        .update({ 
          'ESPN PlayerID': player['ESPN PlayerID'],
          'Selection': player.Player 
        })
        .eq('Overall Pick', currentPick['Overall Pick']);
        
      if (error) {
        alert('Database Update Error: ' + error.message);
        return;
      }
      
      await supabase
        .from('player-pool')
        .update({ 'Availability': currentUser })
        .eq('ESPN PlayerID', player['ESPN PlayerID']);
    }
    
    setShowDashboard(false);
  };

  const addToQueue = (player) => {
    const newQueue = [...queue, player];
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));
  };

  const removeFromQueue = (playerId) => {
    const newQueue = queue.filter(p => String(p['ESPN PlayerID']) !== String(playerId));
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));
  };

  if (!draftMode) {
    return <ModeSelectionModal onSelectMode={handleModeSelect} />;
  }

  if (!currentUser) {
    return <LoginModal owners={DRAFT_OWNERS} onLogin={handleLogin} />;
  }

  const myPicks = displayPicks.filter(p => p.Owner === currentUser && p['ESPN PlayerID']);
  const recentPicks = displayPicks.filter(p => p['ESPN PlayerID']);

  return (
    <>
      <style>{`
        @keyframes scroll {
          0% { transform: translate3d(0, 0, 0); }
          100% { transform: translate3d(-100%, 0, 0); }
        }
        @keyframes pulse {
          0% { opacity: 1; }
          50% { opacity: 0.5; }
          100% { opacity: 1; }
        }
        :root {
          --bg-dark: #121212;
          --bg-card: #1e1e1e;
          --accent: #bb86fc;
          --text-main: #e0e0e0;
          --highlight: #03dac6;
          --alert: #cf6679;
        }
      `}</style>
      
      <div style={styles.body}>
        {/* Header */}
        <div style={styles.header}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            <div style={styles.leagueLogo}>
              ⚾ Hefty War Room 2026
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
                TEST MODE
              </span>
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
                  playOwnerSound(currentUser);
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
              <button
                onClick={() => onSwitchView('summary')}
                style={{
                  background: '#1e3a8a',
                  color: '#fff',
                  border: '1px solid #3b82f6',
                  padding: '8px 14px',
                  borderRadius: '6px',
                  fontSize: '13px',
                  fontWeight: 'bold',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  cursor: 'pointer'
                }}
                title="Switch to in-season dashboard"
              >
                <span>📊</span>
                <span>League Dashboard</span>
              </button>
            )}
          </div>
        </div>

        {/* War Room Drawer Toggle */}
        <button 
          style={styles.warRoomToggle}
          onClick={() => setShowDashboard(!showDashboard)}
        >
          {showDashboard ? '✕ CLOSE DRAWER' : '📋 OPEN WAR ROOM DASHBOARD'}
        </button>

        {/* Main Stage */}
        <div style={styles.mainStage}>
          <div style={styles.pickCard}>
            <div style={styles.pickMeta}>
              {lastPick ? `Round ${lastPick.Round} • Pick #${lastPick['Overall Pick']}` : 'Waiting for first pick...'}
            </div>
            <div style={styles.pickPlayer}>
              {lastPickPlayer ? (
                <>
                  <PlayerNameButton 
                    player={lastPickPlayer} 
                    onClick={setSelectedPlayer}
                    style={{ color: '#fff', fontWeight: '700', fontSize: '56px' }}
                  />
                  {getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo) && (
                    <span style={{
                      marginLeft: '15px',
                      padding: '4px 10px',
                      borderRadius: '6px',
                      background: getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo).color,
                      color: '#000',
                      fontSize: '18px',
                      fontWeight: 'bold',
                      verticalAlign: 'middle'
                    }}>
                      {getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo).status}
                    </span>
                  )}
                </>
              ) : (
                'WAITING ON CLOCK...'
              )}
            </div>
            <div style={styles.pickOwner}>
              {lastPick ? `Selected by ${lastPick.Owner}` : '--'}
            </div>
            <div style={styles.aiBox}>
              {generatingCommentary ? (
                <span>🤔 Gemini is analyzing this selection...</span>
              ) : (
                <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
              )}
            </div>
          </div>
        </div>

        {/* On Deck Sidebar */}
        <OnDeckSidebar upcomingPicks={upcomingPicks} currentUser={currentUser} />

        {/* Bottom Info Bar */}
        <div style={styles.infoBar}>
          <TeamNeedsSummary myPicks={myPicks} players={players} />
          <RecentActivityWidget 
            recentPicks={recentPicks} 
            players={players} 
            onPlayerClick={setSelectedPlayer}
            playerInfo={playerInfo}
          />
          <QueuePreviewWidget 
            queue={queue} 
            onPlayerClick={setSelectedPlayer} 
            playerInfo={playerInfo} 
          />
        </div>

        {/* Scrolling Ticker */}
        <Ticker recentPicks={recentPicks} players={players} />

        {/* War Room Drawer / Panel */}
        {showDashboard && (
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
                   'Projected Standings'}
                </button>
              ))}
              <div style={{ marginLeft: 'auto', color: '#888', fontSize: '13px', display: 'flex', alignItems: 'center' }}>
                Logged in as: <span style={{ color: 'var(--highlight)', fontWeight: 'bold', marginLeft: '5px' }}>{currentUser}</span>
              </div>
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Pool' ? 'grid' : 'none' }}>
              <PlayerPoolPanel
                players={players}
                onDraft={handleDraft}
                isMyTurn={isMyTurn}
                queue={queue}
                onAddToQueue={addToQueue}
                onRemoveFromQueue={removeFromQueue}
                draftMode={draftMode}
                testModePicks={testModePicks}
                onPlayerClick={setSelectedPlayer}
                playerInfo={playerInfo}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Roster' ? 'grid' : 'none' }}>
              <RosterManagerPanel
                allPicks={displayPicks}
                players={players}
                currentUser={currentUser}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'MyPicks' ? 'grid' : 'none' }}>
              <MyPicksPanel
                allPicks={displayPicks}
                players={players}
                currentUser={currentUser}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'DraftLog' ? 'grid' : 'none' }}>
              <DraftLogPanel
                allPicks={displayPicks}
                players={players}
                onPlayerClick={setSelectedPlayer}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Analysis' ? 'grid' : 'none' }}>
              <AnalysisHistoryPanel
                analysisHistory={analysisHistory}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Standings' ? 'grid' : 'none' }}>
              <StandingsPanel 
                allPicks={displayPicks}
                players={players}
              />
            </div>
          </div>
        )}
      </div>
      
      {/* Player Detail Modal */}
      {selectedPlayer && (
        <PlayerModal 
          player={selectedPlayer} 
          onClose={() => setSelectedPlayer(null)}
          onOpenInDepthModal={onOpenPlayerModal}
        />
      )}
    </>
  );
}

// --- STYLES ---
const styles = {
  body: {
    backgroundColor: '#121212',
    color: '#e0e0e0',
    fontFamily: "'Roboto Condensed', sans-serif",
    margin: 0,
    height: 'calc(100vh - 64px)',
    overflow: 'hidden',
    display: 'grid',
    gridTemplateColumns: '1fr 300px',
    gridTemplateRows: '70px 1fr minmax(140px, auto) 45px'
  },
  header: {
    gridColumn: '1 / -1',
    background: '#1f1f1f',
    borderBottom: '2px solid #bb86fc',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0 25px',
    zIndex: 10
  },
  leagueLogo: {
    fontSize: '20px',
    fontWeight: 'bold',
    color: '#bb86fc',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  clock: {
    background: '#000',
    fontFamily: "'Orbitron', monospace",
    fontSize: '24px',
    padding: '4px 15px',
    border: '2px solid #333',
    borderRadius: '6px',
    userSelect: 'none',
    minWidth: '80px',
    textAlign: 'center'
  },
  warRoomToggle: {
    position: 'absolute',
    top: '80px',
    right: '320px',
    background: '#03dac6',
    color: '#000',
    border: 'none',
    padding: '8px 16px',
    borderRadius: '20px',
    cursor: 'pointer',
    fontWeight: 'bold',
    boxShadow: '0 4px 12px rgba(3, 218, 198, 0.4)',
    zIndex: 50,
    fontSize: '13px'
  },
  mainStage: {
    gridColumn: '1 / 2',
    gridRow: '2 / 3',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
    position: 'relative',
    padding: '20px'
  },
  pickCard: {
    background: 'linear-gradient(145deg, #252525, #1a1a1a)',
    border: '1px solid #333',
    borderRadius: '12px',
    padding: '30px',
    textAlign: 'center',
    width: '88%',
    maxWidth: '850px',
    boxShadow: '0 20px 50px rgba(0,0,0,0.5)'
  },
  pickMeta: {
    fontSize: '16px',
    color: '#888',
    marginBottom: '8px',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  pickPlayer: {
    fontSize: '56px',
    fontWeight: '700',
    color: '#fff',
    margin: '8px 0',
    lineHeight: '1.1',
    textShadow: '0 4px 10px rgba(0,0,0,0.5)'
  },
  pickOwner: {
    fontSize: '26px',
    color: '#03dac6',
    fontWeight: 'bold'
  },
  aiBox: {
    marginTop: '20px',
    background: 'rgba(187, 134, 252, 0.08)',
    borderLeft: '4px solid #bb86fc',
    padding: '14px 20px',
    textAlign: 'left',
    fontSize: '16px',
    lineHeight: '1.5',
    fontStyle: 'italic',
    color: '#ccc',
    maxHeight: '150px',
    overflowY: 'auto'
  },
  sidebar: {
    gridColumn: '2 / 3',
    gridRow: '2 / 3',
    background: '#181818',
    borderLeft: '1px solid #333',
    overflowY: 'auto',
    padding: '15px'
  },
  deckItem: {
    background: '#222',
    marginBottom: '8px',
    padding: '10px',
    borderRadius: '4px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  infoBar: {
    gridColumn: '1 / -1',
    gridRow: '3 / 4',
    display: 'grid',
    gridTemplateColumns: '1.2fr 1fr 1fr',
    gap: '12px',
    padding: '10px 20px',
    background: '#1a1a1a',
    borderTop: '1px solid #333'
  },
  infoWidget: {
    background: '#222',
    borderRadius: '8px',
    padding: '10px 14px',
    overflow: 'hidden'
  },
  infoWidgetTitle: {
    fontSize: '13px',
    fontWeight: 'bold',
    color: 'var(--highlight)',
    marginBottom: '8px',
    paddingBottom: '6px',
    borderBottom: '1px solid #333'
  },
  tickerContainer: {
    gridColumn: '1 / -1',
    gridRow: '4 / 5',
    background: '#bb86fc',
    color: '#000',
    overflow: 'hidden',
    display: 'flex',
    alignItems: 'center',
    whiteSpace: 'nowrap'
  },
  tickerContent: {
    display: 'inline-block',
    animation: 'scroll 40s linear infinite',
    paddingLeft: '100%',
    fontWeight: 'bold',
    fontSize: '16px'
  },
  tickerItem: {
    marginRight: '40px'
  },
  warRoomPanel: {
    display: 'grid',
    position: 'fixed',
    bottom: 0,
    left: 0,
    width: '100%',
    height: '82%',
    background: '#242424',
    borderTop: '4px solid #03dac6',
    zIndex: 100,
    padding: '16px 20px',
    boxSizing: 'border-box',
    gridTemplateRows: '40px 1fr',
    gap: '15px',
    boxShadow: '0 -10px 30px rgba(0,0,0,0.8)'
  },
  panelNav: {
    display: 'flex',
    gap: '10px',
    borderBottom: '1px solid #444',
    paddingBottom: '10px'
  },
  navBtn: {
    background: '#333',
    color: '#ccc',
    border: 'none',
    padding: '6px 14px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '13px'
  },
  navBtnActive: {
    background: '#03dac6',
    color: '#000',
    border: 'none',
    padding: '6px 14px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '13px'
  },
  panelContent: {
    height: '100%',
    overflow: 'hidden',
    gridTemplateColumns: '3fr 1fr',
    gap: '15px',
    paddingRight: '5px'
  },
  wrColumn: {
    display: 'flex',
    flexDirection: 'column',
    background: '#1a1a1a',
    padding: '12px 16px',
    borderRadius: '8px',
    height: '100%',
    overflow: 'hidden'
  },
  wrHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '10px',
    color: '#03dac6',
    fontSize: '16px',
    fontWeight: 'bold',
    borderBottom: '1px solid #444',
    paddingBottom: '6px',
    alignItems: 'center',
    flexWrap: 'wrap',
    gap: '10px'
  },
  poolTableContainer: {
    flexGrow: 1,
    overflowY: 'auto',
    overflowX: 'auto',
    marginTop: '6px'
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '12px',
    minWidth: '100%'
  },
  tableHead: {
    position: 'sticky',
    top: 0,
    background: '#2c2c2c',
    color: '#03dac6',
    zIndex: 10
  },
  th: {
    padding: '8px 6px',
    textAlign: 'left',
    cursor: 'pointer',
    borderBottom: '2px solid #555',
    userSelect: 'none',
    whiteSpace: 'nowrap'
  },
  td: {
    padding: '6px 6px',
    borderBottom: '1px solid #333',
    color: '#ccc',
    whiteSpace: 'nowrap'
  },
  tableRow: {
    transition: 'background 0.2s'
  },
  btnDraft: {
    background: '#03dac6',
    border: 'none',
    color: '#000',
    fontWeight: 'bold',
    padding: '3px 7px',
    cursor: 'pointer',
    borderRadius: '4px',
    fontSize: '11px',
    marginRight: '5px'
  },
  btnStar: {
    background: 'none',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: '14px'
  },
  btnStarActive: {
    background: 'none',
    border: 'none',
    color: 'gold',
    cursor: 'pointer',
    fontSize: '14px'
  },
  filterControl: {
    background: '#333',
    color: '#fff',
    border: '1px solid #555',
    padding: '4px 8px',
    borderRadius: '4px',
    fontSize: '12px'
  },
  rosterSelect: {
    background: '#333',
    color: '#fff',
    border: '1px solid #555',
    padding: '4px',
    width: '100%',
    borderRadius: '3px',
    cursor: 'pointer',
    fontSize: '12px'
  },
  queueItem: {
    padding: '8px',
    borderBottom: '1px solid #333',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    borderLeft: '3px solid #03dac6'
  },
  loginOverlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0,0,0,0.92)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 1000
  },
  loginBox: {
    background: '#1e1e1e',
    padding: '50px 40px',
    borderRadius: '12px',
    textAlign: 'center',
    border: '2px solid #bb86fc',
    boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
    maxWidth: '480px',
    width: '90%'
  },
  loginSelect: {
    fontSize: '16px',
    padding: '10px 14px',
    margin: '20px 0',
    width: '100%',
    borderRadius: '6px',
    border: '2px solid #444',
    background: '#333',
    color: '#fff'
  },
  loginButton: {
    fontSize: '16px',
    padding: '12px 24px',
    background: '#03dac6',
    color: '#000',
    border: 'none',
    borderRadius: '8px',
    cursor: 'pointer',
    fontWeight: 'bold',
    width: '100%'
  },
  modalOverlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0, 0, 0, 0.85)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 2000,
    backdropFilter: 'blur(4px)'
  },
  modalContent: {
    background: '#1e1e1e',
    borderRadius: '12px',
    width: '90%',
    maxWidth: '720px',
    maxHeight: '85vh',
    overflow: 'hidden',
    boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
    border: '2px solid #bb86fc',
    display: 'flex',
    flexDirection: 'column'
  },
  modalHeader: {
    padding: '20px 25px',
    borderBottom: '2px solid #333',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    background: '#252525'
  },
  modalCloseBtn: {
    background: 'none',
    border: 'none',
    color: '#888',
    fontSize: '24px',
    cursor: 'pointer',
    padding: '0',
    width: '28px',
    height: '28px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: '4px'
  },
  modalBody: {
    padding: '20px 25px',
    overflowY: 'auto',
    flexGrow: 1
  },
  modalSection: {
    marginBottom: '20px',
    padding: '14px',
    background: '#252525',
    borderRadius: '8px',
    borderLeft: '4px solid var(--accent)'
  },
  modalSectionTitle: {
    margin: '0 0 10px 0',
    color: 'var(--highlight)',
    fontSize: '15px',
    fontWeight: 'bold',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  newsItem: {
    padding: '10px 12px',
    marginBottom: '8px',
    background: '#1a1a1a',
    borderRadius: '6px',
    borderLeft: '3px solid #03dac6'
  },
  externalLink: {
    display: 'inline-block',
    padding: '8px 16px',
    background: '#03dac6',
    color: '#000',
    textDecoration: 'none',
    borderRadius: '6px',
    fontWeight: 'bold',
    fontSize: '13px',
    transition: 'all 0.2s'
  }
};
