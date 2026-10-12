import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { compute2027DraftPicks } from '../utils/draftCalculations';

export default function MyPicksPanel({ allPicks, players, currentUser, draftTrades = [], roomSeason = 2027 }) {
  const [localSeasonView, setLocalSeasonView] = useState(null);
  const seasonView = localSeasonView || String(roomSeason || '2027');
  const setSeasonView = setLocalSeasonView;

  const userPicks = allPicks.filter(p => p.Owner === currentUser);
  const filledPicks = userPicks.filter(p => p['ESPN PlayerID']);

  // 2027 Picks calculation
  const computed2027Picks = useMemo(() => {
    return compute2027DraftPicks(draftTrades);
  }, [draftTrades]);

  const user2027Owned = useMemo(() => {
    return computed2027Picks.filter(p => p.currentOwner === currentUser);
  }, [computed2027Picks, currentUser]);

  const user2027TradedAway = useMemo(() => {
    return computed2027Picks.filter(p => p.originalOwner === currentUser && p.currentOwner !== currentUser);
  }, [computed2027Picks, currentUser]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          <span>Draft Picks ({currentUser})</span>
          <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
            <button
              onClick={() => setSeasonView('2026')}
              style={{
                background: seasonView === '2026' ? 'var(--highlight)' : 'transparent',
                color: seasonView === '2026' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              2026 Selections
            </button>
            <button
              onClick={() => setSeasonView('2027')}
              style={{
                background: seasonView === '2027' ? '#ffb74d' : 'transparent',
                color: seasonView === '2027' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              🎟️ 2027 Upcoming Draft Capital
            </button>
          </div>
        </div>

        <span style={{ fontSize: '13px', color: '#888' }}>
          {seasonView === '2026' ? (
            `${filledPicks.length} of ${userPicks.length} picks made`
          ) : (
            `${user2027Owned.length} Total Picks Held for 2027 Draft (${user2027Owned.length - 27 >= 0 ? `+${user2027Owned.length - 27}` : user2027Owned.length - 27} Net)`
          )}
        </span>
      </div>
      
      <div style={styles.poolTableContainer}>
        {seasonView === '2026' ? (
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
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            <div>
              <div style={{ fontWeight: 'bold', color: '#ffb74d', marginBottom: '10px', fontSize: '13px' }}>
                Active Pick Inventory ({user2027Owned.length} picks owned for 2027)
              </div>
              <table style={styles.table}>
                <thead style={styles.tableHead}>
                  <tr>
                    <th style={styles.th}>Round</th>
                    <th style={styles.th}>Status</th>
                    <th style={styles.th}>Original Owner</th>
                    <th style={styles.th}>Trade Details / Notes</th>
                  </tr>
                </thead>
                <tbody>
                  {user2027Owned.map((pick, pIdx) => {
                    const isAcquired = pick.originalOwner !== currentUser;
                    return (
                      <tr key={`2027-owned-${pIdx}`} style={styles.tableRow}>
                        <td style={styles.td}>
                          <strong style={{ color: isAcquired ? '#ffb74d' : '#03dac6', fontSize: '13px' }}>
                            Round {pick.round}
                          </strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{
                            padding: '3px 8px',
                            borderRadius: '4px',
                            fontSize: '10px',
                            fontWeight: 'bold',
                            background: isAcquired ? '#ffb74d25' : '#03dac620',
                            color: isAcquired ? '#ffb74d' : '#03dac6',
                            border: isAcquired ? '1px solid #ffb74d50' : '1px solid #03dac650'
                          }}>
                            {isAcquired ? '✓ ACQUIRED' : 'ORIGINAL'}
                          </span>
                        </td>
                        <td style={styles.td}>
                          <span style={{ color: isAcquired ? '#fff' : '#aaa' }}>
                            {pick.originalOwner}
                          </span>
                        </td>
                        <td style={styles.td}>
                          {isAcquired ? (
                            <span style={{ color: '#ffcc80', fontSize: '11px' }}>
                              Acquired via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                              {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                            </span>
                          ) : (
                            <span style={{ color: '#666', fontSize: '11px' }}>Own draft pick</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {user2027TradedAway.length > 0 && (
              <div>
                <div style={{ fontWeight: 'bold', color: '#ff5252', marginBottom: '10px', fontSize: '13px' }}>
                  Picks Traded Away ({user2027TradedAway.length} picks)
                </div>
                <table style={styles.table}>
                  <thead style={styles.tableHead}>
                    <tr>
                      <th style={styles.th}>Round</th>
                      <th style={styles.th}>Status</th>
                      <th style={styles.th}>Traded To</th>
                      <th style={styles.th}>Trade Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {user2027TradedAway.map((pick, pIdx) => (
                      <tr key={`2027-away-${pIdx}`} style={{ ...styles.tableRow, opacity: 0.85 }}>
                        <td style={styles.td}>
                          <strong style={{ color: '#ff5252', textDecoration: 'line-through' }}>
                            Round {pick.round}
                          </strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{
                            padding: '3px 8px',
                            borderRadius: '4px',
                            fontSize: '10px',
                            fontWeight: 'bold',
                            background: '#ff525220',
                            color: '#ff5252',
                            border: '1px solid #ff525240'
                          }}>
                            TRADED AWAY
                          </span>
                        </td>
                        <td style={styles.td}>
                          <strong style={{ color: '#fff' }}>{pick.currentOwner}</strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{ color: '#ff8a80', fontSize: '11px' }}>
                            Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                            {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
