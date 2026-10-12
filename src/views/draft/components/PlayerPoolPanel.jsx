import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../../../utils/headshotUtils';
import { getInjuryIndicator, getEffectiveQS } from '../utils/draftCalculations';
import { getPositionalTiers } from '../../../utils/draftRotoEngine';
import PlayerNameButton from './PlayerNameButton';
import { DraftAssistantWidget } from './DraftWidgets';

export default function PlayerPoolPanel({
  players,
  onDraft,
  isMyTurn,
  queue,
  onAddToQueue,
  onRemoveFromQueue,
  draftMode,
  testModePicks,
  allPicks,
  onPlayerClick,
  playerInfo,
  isMobile = false,
  recommendations = []
}) {
  const [viewMode, setViewMode] = useState('list'); // 'list' | 'tiers'
  const [tierPos, setTierPos] = useState('SP');
  const [showAssistant, setShowAssistant] = useState(true);
  const [sortConfig, setSortConfig] = useState({ key: 'Hefty Keeper Rank', direction: 'asc' });
  const [filterPos, setFilterPos] = useState('');
  const [searchText, setSearchText] = useState('');

  const sortedPlayers = useMemo(() => {
    const activePicks = (draftMode === 'test' || draftMode === 'mockdraft') && testModePicks?.length > 0
      ? testModePicks
      : (allPicks || []);
    const draftedSet = new Set(activePicks.map(p => String(p['ESPN PlayerID'])).filter(id => id && id !== 'null' && id !== 'undefined'));

    let filtered = [...players].filter(p => {
      const pid = String(p['ESPN PlayerID']);
      if (draftedSet.has(pid)) return false;
      if (p.isKeeper) return false;
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

    const getPlayerRank = (p) => {
      const r = p['Hefty Keeper Rank'] ?? p.overall_rank ?? p['Hefty Single Season Rank'] ?? p['ESPN Keeper Rank'] ?? p.rank;
      const num = parseFloat(r);
      return isNaN(num) || num <= 0 ? 9999 : num;
    };

    const getPlayerPrice = (p) => {
      const pr = p['Hefty Keeper Price'] ?? p.overall_price ?? p['Hefty Single Season Price'] ?? p['ESPN Price'] ?? p.price;
      const num = parseFloat(pr);
      return isNaN(num) ? -999 : num;
    };
    
    filtered.sort((a, b) => {
      let comparison = 0;
      if (sortConfig.key === 'Hefty Keeper Rank' || sortConfig.key === 'rank' || sortConfig.key === 'overall_rank') {
        comparison = getPlayerRank(a) - getPlayerRank(b);
      } else if (sortConfig.key === 'Hefty Keeper Price' || sortConfig.key === 'price' || sortConfig.key === 'overall_price') {
        comparison = getPlayerPrice(a) - getPlayerPrice(b);
      } else if (sortConfig.key === 'ZIPSERA' || sortConfig.key === 'ZIPSWHIP') {
        const aVal = parseFloat(a[sortConfig.key]);
        const bVal = parseFloat(b[sortConfig.key]);
        const aSafe = isNaN(aVal) || aVal <= 0 ? 999 : aVal;
        const bSafe = isNaN(bVal) || bVal <= 0 ? 999 : bVal;
        comparison = aSafe - bSafe;
      } else if (sortConfig.key === 'ZIPSQS' || sortConfig.key === 'ESPNQS') {
        const aVal = getEffectiveQS(a);
        const bVal = getEffectiveQS(b);
        comparison = aVal - bVal;
      } else {
        const aVal = a[sortConfig.key] ?? '';
        const bVal = b[sortConfig.key] ?? '';
        const aNum = parseFloat(aVal);
        const bNum = parseFloat(bVal);
        if (!isNaN(aNum) && !isNaN(bNum)) {
          comparison = aNum - bNum;
        } else {
          comparison = String(aVal).localeCompare(String(bVal));
        }
      }
      
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
    
    return filtered;
  }, [players, sortConfig, filterPos, searchText, draftMode, testModePicks, allPicks]);

  const DESC_FIRST_KEYS = useMemo(() => new Set([
    'Hefty Keeper Price',
    'overall_price',
    'price',
    'ZIPSR',
    'ZIPSHR',
    'ZIPSRBI',
    'ZIPSSB',
    'ZIPSOBP',
    'ZIPSK',
    'ZIPSQS',
    'ESPNQS',
    'ZIPSSV+HDs'
  ]), []);

  const requestSort = (key) => {
    setSortConfig(prev => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === 'asc' ? 'desc' : 'asc'
        };
      }
      const initialDirection = DESC_FIRST_KEYS.has(key) ? 'desc' : 'asc';
      return { key, direction: initialDirection };
    });
  };

  const getSortIcon = (key) => {
    if (sortConfig.key !== key) return '';
    return sortConfig.direction === 'asc' ? ' ▲' : ' ▼';
  };

  const isQueued = (playerId) => queue.some(p => String(p['ESPN PlayerID']) === String(playerId));

  // Compute Positional Tiers for Tiers View
  const positionalTiers = useMemo(() => {
    return getPositionalTiers({
      availablePlayers: sortedPlayers,
      position: tierPos
    });
  }, [sortedPlayers, tierPos]);

  return (
    <div style={{ display: 'flex', gridColumn: '1 / -1', gap: '20px', height: '100%', minHeight: 0 }}>
      {/* Main Pool */}
      <div style={{ ...styles.wrColumn, flex: '3', minHeight: 0 }}>
        <div style={styles.wrHeader}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            <span>Available Players {draftMode === 'test' && <span style={{ color: '#ffc107', fontSize: '14px' }}>(TEST MODE)</span>}{draftMode === 'mockdraft' && <span style={{ color: '#bb86fc', fontSize: '14px' }}>(MOCK DRAFT)</span>}</span>
            {recommendations.length > 0 && (
              <button
                onClick={() => setShowAssistant(!showAssistant)}
                style={{
                  background: showAssistant ? 'rgba(3, 218, 198, 0.15)' : 'transparent',
                  color: showAssistant ? '#03dac6' : '#888',
                  border: '1px solid #444',
                  borderRadius: '4px',
                  padding: '2px 8px',
                  fontSize: '11px',
                  cursor: 'pointer'
                }}
              >
                🤖 Assistant ({recommendations.length})
              </button>
            )}
          </div>

          <div style={{ display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' }}>
            {/* View Switcher: List vs Tiers */}
            <div style={{ display: 'flex', background: '#1e293b', padding: '2px', borderRadius: '5px' }}>
              <button
                onClick={() => setViewMode('list')}
                style={{
                  background: viewMode === 'list' ? '#03dac6' : 'transparent',
                  color: viewMode === 'list' ? '#000' : '#94a3b8',
                  border: 'none',
                  borderRadius: '3px',
                  padding: '3px 8px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                📋 List
              </button>
              <button
                onClick={() => setViewMode('tiers')}
                style={{
                  background: viewMode === 'tiers' ? '#bb86fc' : 'transparent',
                  color: viewMode === 'tiers' ? '#000' : '#94a3b8',
                  border: 'none',
                  borderRadius: '3px',
                  padding: '3px 8px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                🧱 Positional Tiers
              </button>
            </div>

            {viewMode === 'list' && (
              <>
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
              </>
            )}
          </div>
        </div>

        {/* Live Draft Assistant Widget */}
        {showAssistant && recommendations.length > 0 && (
          <DraftAssistantWidget
            recommendations={recommendations}
            onDraft={onDraft}
            isMyTurn={isMyTurn}
            queue={queue}
            onAddToQueue={onAddToQueue}
            onRemoveFromQueue={onRemoveFromQueue}
            onPlayerClick={onPlayerClick}
          />
        )}

        {viewMode === 'tiers' ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '14px', overflowY: 'auto', flexGrow: 1, padding: '4px' }}>
            {/* Position Picker Tabs for Tiers */}
            <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', alignItems: 'center', background: '#181b22', padding: '8px 12px', borderRadius: '8px' }}>
              <span style={{ fontSize: '12px', color: '#94a3b8', fontWeight: 'bold', marginRight: '6px' }}>
                Position Tiers:
              </span>
              {['SP', 'RP', 'C', '1B', '2B', '3B', 'SS', 'OF'].map(pos => (
                <button
                  key={pos}
                  onClick={() => setTierPos(pos)}
                  style={{
                    background: tierPos === pos ? '#bb86fc' : '#222836',
                    color: tierPos === pos ? '#000' : '#cbd5e1',
                    border: '1px solid ' + (tierPos === pos ? '#bb86fc' : '#334155'),
                    borderRadius: '6px',
                    padding: '4px 10px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer'
                  }}
                >
                  {pos}
                </button>
              ))}
            </div>

            {/* Render Tiers 1 through 4 */}
            {Object.entries(positionalTiers).map(([tierKey, tier]) => {
              const count = tier.players.length;
              const isCliff = (tierKey === '1' || tierKey === '2') && count <= 2 && count > 0;
              return (
                <div
                  key={tierKey}
                  style={{
                    background: '#1a1f29',
                    border: `1.5px solid ${isCliff ? '#ef4444' : tier.color + '55'}`,
                    borderRadius: '8px',
                    padding: '12px 14px',
                    boxShadow: isCliff ? '0 0 16px rgba(239, 68, 68, 0.25)' : 'none'
                  }}
                >
                  {/* Tier Header */}
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{
                        background: tier.color,
                        color: '#000',
                        fontSize: '11px',
                        fontWeight: 'bold',
                        padding: '2px 8px',
                        borderRadius: '4px'
                      }}>
                        {tier.name}
                      </span>
                      <span style={{ color: '#94a3b8', fontSize: '12px' }}>
                        ({count} Available)
                      </span>
                    </div>

                    {isCliff && (
                      <span style={{
                        background: 'rgba(239, 68, 68, 0.2)',
                        color: '#fca5a5',
                        border: '1px solid #ef4444',
                        borderRadius: '4px',
                        padding: '2px 8px',
                        fontSize: '11px',
                        fontWeight: 'bold',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '4px'
                      }}>
                        ⚠️ CLIFF ALERT: Only {count} Left!
                      </span>
                    )}
                  </div>

                  {/* Cliff Alert Banner */}
                  {isCliff && (
                    <div style={{
                      background: 'rgba(239, 68, 68, 0.12)',
                      border: '1px solid rgba(239, 68, 68, 0.3)',
                      color: '#fecdd3',
                      fontSize: '11px',
                      borderRadius: '6px',
                      padding: '6px 10px',
                      marginBottom: '10px',
                      lineHeight: '1.4'
                    }}>
                      ⚡ Positional drop-off cliff imminent! Only <strong>{count}</strong> {tierPos} remaining before dropping to Tier {parseInt(tierKey, 10) + 1}. Secure talent now.
                    </div>
                  )}

                  {/* Player Cards */}
                  {count === 0 ? (
                    <div style={{ color: '#64748b', fontSize: '12px', fontStyle: 'italic', padding: '6px 0' }}>
                      Tier is completely drafted.
                    </div>
                  ) : (
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: '8px' }}>
                      {tier.players.map(p => {
                        const queued = isQueued(p['ESPN PlayerID']);
                        const heftyPrice = p['Hefty Keeper Price'] ?? p.overall_price ?? p['Hefty Single Season Price'] ?? p.price;
                        const heftyRank = p['Hefty Keeper Rank'] ?? p.overall_rank ?? p['Hefty Single Season Rank'] ?? p.rank;
                        return (
                          <div
                            key={p['ESPN PlayerID']}
                            style={{
                              background: '#242b38',
                              border: '1px solid #334155',
                              borderRadius: '6px',
                              padding: '8px 10px',
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'space-between',
                              gap: '8px'
                            }}
                          >
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                              <div style={{ width: '28px', height: '28px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #475569' }}>
                                <img
                                  src={getPlayerHeadshotUrl(p)}
                                  alt=""
                                  style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                                  onError={(e) => handleHeadshotError(e, p)}
                                  referrerPolicy="no-referrer"
                                  loading="lazy"
                                />
                              </div>
                              <div style={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                                <div
                                  onClick={() => onPlayerClick(p)}
                                  style={{ color: '#fff', fontWeight: 'bold', fontSize: '13px', cursor: 'pointer', whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden' }}
                                  title="Click for scouting card"
                                >
                                  {p.Player}
                                </div>
                                <div style={{ fontSize: '11px', color: '#94a3b8' }}>
                                  {p.Team} • {heftyRank ? `#${heftyRank}` : '--'} {heftyPrice ? `• $${heftyPrice}` : ''}
                                </div>
                              </div>
                            </div>

                            <div style={{ display: 'flex', gap: '4px', flexShrink: 0 }}>
                              {isMyTurn && (
                                <button
                                  onClick={() => onDraft(p)}
                                  style={{
                                    background: '#10b981',
                                    color: '#fff',
                                    border: 'none',
                                    padding: '3px 8px',
                                    borderRadius: '4px',
                                    fontSize: '11px',
                                    fontWeight: 'bold',
                                    cursor: 'pointer'
                                  }}
                                >
                                  Pick
                                </button>
                              )}
                              <button
                                onClick={() => queued ? onRemoveFromQueue(p['ESPN PlayerID']) : onAddToQueue(p)}
                                style={queued ? styles.btnStarActive : styles.btnStar}
                                title={queued ? "Remove from queue" : "Add to queue"}
                              >
                                {queued ? '★' : '☆'}
                              </button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        ) : (
          <div style={styles.poolTableContainer}>
            <table style={styles.table}>
              <thead style={styles.tableHead}>
                <tr>
                  <th style={styles.th} width="75">Action</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Player')}>Player{getSortIcon('Player')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Position')}>Pos{getSortIcon('Position')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Team')}>Team{getSortIcon('Team')}</th>
                  <th style={{ ...styles.th, color: '#ffc107', cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Hefty Keeper Price')}>Hefty ${getSortIcon('Hefty Keeper Price')}</th>
                  <th style={{ ...styles.th, color: '#90caf9', cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Hefty Keeper Rank')}>Rank{getSortIcon('Hefty Keeper Rank')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSR')}>R{getSortIcon('ZIPSR')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSHR')}>HR{getSortIcon('ZIPSHR')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSRBI')}>RBI{getSortIcon('ZIPSRBI')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSSB')}>SB{getSortIcon('ZIPSSB')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSOBP')}>OBP{getSortIcon('ZIPSOBP')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSK')}>K{getSortIcon('ZIPSK')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSQS')}>QS{getSortIcon('ZIPSQS')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSERA')}>ERA{getSortIcon('ZIPSERA')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSWHIP')}>WHIP{getSortIcon('ZIPSWHIP')}</th>
                  <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSSV+HDs')}>SV+H{getSortIcon('ZIPSSV+HDs')}</th>
                </tr>
              </thead>
              <tbody>
                {sortedPlayers.slice(0, 200).map(p => {
                  const queued = isQueued(p['ESPN PlayerID']);
                  const isPitcher = p.Position?.includes('SP') || p.Position?.includes('RP');
                  const injury = getInjuryIndicator(p['ESPN PlayerID'], playerInfo);
                  const heftyPrice = p['Hefty Keeper Price'] ?? p.overall_price ?? p['Hefty Single Season Price'] ?? p.price;
                  const heftyRank = p['Hefty Keeper Rank'] ?? p.overall_rank ?? p['Hefty Single Season Rank'] ?? p.rank;
                  
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
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <div style={{
                            width: '26px',
                            height: '26px',
                            borderRadius: '50%',
                            overflow: 'hidden',
                            background: '#222',
                            flexShrink: 0,
                            border: '1px solid #444'
                          }}>
                            <img
                              src={getPlayerHeadshotUrl(p)}
                              alt=""
                              style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                              onError={(e) => handleHeadshotError(e, p)}
                              referrerPolicy="no-referrer"
                              loading="lazy"
                            />
                          </div>
                          <PlayerNameButton 
                            player={p} 
                            onClick={onPlayerClick}
                            style={{ color: injury ? injury.color : '#fff', fontWeight: 'bold' }}
                          />
                          {injury && (
                            <span style={{
                              marginLeft: '4px',
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
                        </div>
                      </td>
                      <td style={styles.td}>{p.Position}</td>
                      <td style={styles.td}>{p.Team}</td>
                      <td style={{ ...styles.td, textAlign: 'right', fontWeight: 'bold', color: (heftyPrice !== undefined && heftyPrice !== null && heftyPrice !== '') ? '#ffc107' : '#666' }}>
                        {heftyPrice !== undefined && heftyPrice !== null && heftyPrice !== '' ? `$${heftyPrice}` : '-'}
                      </td>
                      <td style={{ ...styles.td, textAlign: 'center', color: '#90caf9', fontSize: '11px', fontWeight: '600' }}>
                        {heftyRank ? `#${heftyRank}` : '-'}
                      </td>
                      <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSR || '-')}</td>
                      <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSHR || '-')}</td>
                      <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSRBI || '-')}</td>
                      <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSSB || '-')}</td>
                      <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSOBP || '-')}</td>
                      <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSK || '-')}</td>
                      <td style={styles.td}>{!isPitcher ? '-' : (() => { const q = getEffectiveQS(p); return q > 0 ? q : (p.ZIPSQS || '-'); })()}</td>
                      <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSERA || '-')}</td>
                      <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSWHIP || '-')}</td>
                      <td style={styles.td}>{!isPitcher ? '-' : (p['ZIPSSV+HDs'] || '-')}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Queue */}
      {!isMobile && (
        <div style={{ ...styles.wrColumn, flex: '1', minHeight: 0 }}>
          <div style={styles.wrHeader}>My Queue ({queue.length})</div>
          <div style={{ overflowY: 'auto', flexGrow: 1 }}>
            {queue.length === 0 ? (
              <div style={{ padding: '20px', color: '#666', textAlign: 'center', fontSize: '13px' }}>
                Click ☆ next to any player in the pool to queue them up.
              </div>
            ) : (
              queue.map(p => (
                <div key={p['ESPN PlayerID']} style={styles.queueItem}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <div style={{ width: '28px', height: '28px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #444' }}>
                      <img
                        src={getPlayerHeadshotUrl(p)}
                        alt=""
                        style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                        onError={(e) => handleHeadshotError(e, p)}
                        referrerPolicy="no-referrer"
                        loading="lazy"
                      />
                    </div>
                    <div>
                      <div style={{ fontWeight: 'bold' }}>{p.Player}</div>
                      <div style={{ fontSize: '12px', color: '#888' }}>{p.Position} - {p.Team}</div>
                    </div>
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
      )}
    </div>
  );
}
