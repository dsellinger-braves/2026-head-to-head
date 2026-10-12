import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { DRAFT_OWNERS, compute2027DraftPicks } from '../utils/draftCalculations';

export default function DraftCapitalPanel({ draftTrades = [], compPicks = [], keepers = [], currentUser }) {
  const [selectedOwner, setSelectedOwner] = useState(currentUser || DRAFT_OWNERS[0]);
  const [activeSubTab, setActiveSubTab] = useState('board'); // 'board' | 'ledgers' | 'history' | '2026board'
  const [roundFilter, setRoundFilter] = useState('ALL');

  const computedPicks = useMemo(() => {
    return compute2027DraftPicks(draftTrades);
  }, [draftTrades]);

  // Compute owner statistics
  const ownerStats = useMemo(() => {
    const stats = {};
    DRAFT_OWNERS.forEach(owner => {
      const ownedPicks = computedPicks.filter(p => p.currentOwner === owner);
      const acquired = computedPicks.filter(p => p.currentOwner === owner && p.originalOwner !== owner);
      const tradedAway = computedPicks.filter(p => p.originalOwner === owner && p.currentOwner !== owner);
      stats[owner] = {
        owner,
        total: ownedPicks.length,
        diff: ownedPicks.length - 26,
        acquired,
        tradedAway,
        ownedPicks
      };
    });
    return stats;
  }, [computedPicks]);

  // Distinct trades involving draft assets
  const assetTradesList = useMemo(() => {
    return (draftTrades || []).filter(t => 
      t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget'
    );
  }, [draftTrades]);

  // Filtered rounds for round-by-round board
  const rounds = useMemo(() => {
    const list = [];
    for (let r = 7; r <= 32; r++) list.push(r);
    if (roundFilter === 'ALL') return list;
    if (roundFilter === 'EARLY') return list.filter(r => r <= 15);
    if (roundFilter === 'MID') return list.filter(r => r >= 16 && r <= 23);
    if (roundFilter === 'LATE') return list.filter(r => r >= 24);
    return list;
  }, [roundFilter]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      {/* Header */}
      <div style={styles.wrHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          <span>🎟️ Draft Capital & Pick Board</span>
          <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
            <button
              onClick={() => setActiveSubTab('board')}
              style={{
                background: activeSubTab === 'board' ? '#03dac6' : 'transparent',
                color: activeSubTab === 'board' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              📊 2027 Traded Board
            </button>
            <button
              onClick={() => setActiveSubTab('ledgers')}
              style={{
                background: activeSubTab === 'ledgers' ? '#ffb74d' : 'transparent',
                color: activeSubTab === 'ledgers' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              📑 Owner Pick Ledgers
            </button>
            <button
              onClick={() => setActiveSubTab('history')}
              style={{
                background: activeSubTab === 'history' ? '#bb86fc' : 'transparent',
                color: activeSubTab === 'history' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              📜 Trade Log History ({assetTradesList.length})
            </button>
            <button
              onClick={() => setActiveSubTab('2026board')}
              style={{
                background: activeSubTab === '2026board' ? '#00e676' : 'transparent',
                color: activeSubTab === '2026board' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              💎 2026 Ground Truth Board
            </button>
          </div>
        </div>

        <span style={{ fontSize: '13px', color: '#aaa' }}>
          234 Total Draft Picks (Rounds 7–32 across 9 teams)
        </span>
      </div>

      <div style={styles.poolTableContainer}>
        {/* Owner Net Balance Cards */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
          gap: '8px',
          marginBottom: '20px'
        }}>
          {DRAFT_OWNERS.map(owner => {
            const stat = ownerStats[owner] || { total: 26, diff: 0, acquired: [], tradedAway: [] };
            const isSelected = selectedOwner === owner;
            const hasActivity = stat.acquired.length > 0 || stat.tradedAway.length > 0;

            return (
              <div
                key={owner}
                onClick={() => setSelectedOwner(owner)}
                style={{
                  background: isSelected ? '#2a2a2a' : '#1a1a1a',
                  border: isSelected ? '2px solid #03dac6' : '1px solid #333',
                  borderRadius: '8px',
                  padding: '10px 12px',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease',
                  position: 'relative'
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
                  <span style={{ fontWeight: 'bold', fontSize: '13px', color: isSelected ? '#03dac6' : '#fff' }}>
                    {owner}
                  </span>
                  <span style={{
                    fontSize: '11px',
                    fontWeight: 'bold',
                    padding: '2px 6px',
                    borderRadius: '4px',
                    background: stat.diff > 0 ? '#00e67620' : stat.diff < 0 ? '#ff525220' : '#88888820',
                    color: stat.diff > 0 ? '#00e676' : stat.diff < 0 ? '#ff5252' : '#888'
                  }}>
                    {stat.diff > 0 ? `+${stat.diff}` : stat.diff < 0 ? `${stat.diff}` : 'Even'}
                  </span>
                </div>

                <div style={{ fontSize: '12px', color: '#ccc', fontWeight: 'bold' }}>
                  {stat.total} picks
                </div>

                {hasActivity ? (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '3px', marginTop: '6px' }}>
                    {stat.acquired.map((a, i) => (
                      <span
                        key={`acq-${i}`}
                        style={{
                          fontSize: '9px',
                          background: '#00e67625',
                          color: '#69f0ae',
                          padding: '1px 4px',
                          borderRadius: '3px',
                          fontWeight: 'bold'
                        }}
                        title={`Acquired Round ${a.round} from ${a.originalOwner}`}
                      >
                        +R{a.round}
                      </span>
                    ))}
                    {stat.tradedAway.map((t, i) => (
                      <span
                        key={`away-${i}`}
                        style={{
                          fontSize: '9px',
                          background: '#ff525225',
                          color: '#ff8a80',
                          padding: '1px 4px',
                          borderRadius: '3px',
                          fontWeight: 'bold'
                        }}
                        title={`Traded away Round ${t.round} to ${t.currentOwner}`}
                      >
                        -R{t.round}
                      </span>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: '10px', color: '#666', marginTop: '6px' }}>
                    All original picks
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Sub-tab Content */}
        {activeSubTab === 'board' && (
          <div>
            {/* Filter Bar */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  Filter Rounds:
                </span>
                {['ALL', 'EARLY', 'MID', 'LATE'].map(rf => (
                  <button
                    key={rf}
                    onClick={() => setRoundFilter(rf)}
                    style={{
                      background: roundFilter === rf ? '#333' : '#1e1e1e',
                      color: roundFilter === rf ? '#03dac6' : '#888',
                      border: roundFilter === rf ? '1px solid #03dac6' : '1px solid #333',
                      borderRadius: '4px',
                      padding: '3px 8px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    {rf === 'ALL' ? 'All (R7-32)' : rf === 'EARLY' ? 'Early (R7-15)' : rf === 'MID' ? 'Mid (R16-23)' : 'Late (R24-32)'}
                  </button>
                ))}
              </div>

              <span style={{ fontSize: '11px', color: '#ffb74d', fontStyle: 'italic' }}>
                * Amber cards indicate pick ownership transferred via approved trade.
              </span>
            </div>

            <div style={{ overflowX: 'auto' }}>
              <table style={{ ...styles.table, minWidth: '900px' }}>
                <thead style={styles.tableHead}>
                  <tr>
                    <th style={{ ...styles.th, width: '90px' }}>Round</th>
                    {DRAFT_OWNERS.map(o => (
                      <th key={o} style={{ ...styles.th, textAlign: 'center' }}>
                        {o}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rounds.map(roundNum => {
                    return (
                      <tr key={`round-${roundNum}`} style={styles.tableRow}>
                        <td style={{ ...styles.td, fontWeight: 'bold', color: '#03dac6', whiteSpace: 'nowrap' }}>
                          Round {roundNum}
                        </td>
                        {DRAFT_OWNERS.map(origOwner => {
                          const pick = computedPicks.find(p => p.round === roundNum && p.originalOwner === origOwner);
                          if (!pick) return <td key={origOwner} style={styles.td}>-</td>;

                          const isTraded = pick.isTraded;
                          return (
                            <td key={origOwner} style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                              {isTraded ? (
                                <div
                                  style={{
                                    background: 'linear-gradient(135deg, rgba(255,183,77,0.25), rgba(255,152,0,0.15))',
                                    border: '1px solid #ffb74d',
                                    borderRadius: '6px',
                                    padding: '4px 6px',
                                    display: 'inline-block',
                                    minWidth: '78px'
                                  }}
                                  title={`Trade #${pick.tradeDetails?.trade_id} (${pick.tradeDetails?.trade_date}): ${pick.tradeDetails?.asset_name} transferred from ${pick.originalOwner} to ${pick.currentOwner}`}
                                >
                                  <div style={{ fontWeight: 'bold', color: '#ffcc80', fontSize: '12px' }}>
                                    {pick.currentOwner}
                                  </div>
                                  <div style={{ fontSize: '9px', color: '#ffab40', textDecoration: 'line-through' }}>
                                    via {pick.originalOwner}
                                  </div>
                                </div>
                              ) : (
                                <span style={{ color: '#aaa', fontSize: '12px' }}>
                                  {pick.currentOwner}
                                </span>
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeSubTab === 'ledgers' && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  Select Manager:
                </span>
                <select
                  value={selectedOwner}
                  onChange={e => setSelectedOwner(e.target.value)}
                  style={styles.filterControl}
                >
                  {DRAFT_OWNERS.map(o => (
                    <option key={o} value={o}>{o} ({ownerStats[o]?.total || 26} picks)</option>
                  ))}
                </select>
              </div>

              <span style={{ color: '#03dac6', fontSize: '12px', fontWeight: 'bold' }}>
                {selectedOwner} holds {ownerStats[selectedOwner]?.total} picks for 2027
              </span>
            </div>

            <table style={styles.table}>
              <thead style={styles.tableHead}>
                <tr>
                  <th style={styles.th}>Round</th>
                  <th style={styles.th}>Status</th>
                  <th style={styles.th}>Original Owner</th>
                  <th style={styles.th}>Trade Details & Notes</th>
                </tr>
              </thead>
              <tbody>
                {ownerStats[selectedOwner]?.ownedPicks.map((pick, idx) => {
                  const isAcquired = pick.originalOwner !== selectedOwner;
                  return (
                    <tr key={`ledger-${selectedOwner}-${idx}`} style={styles.tableRow}>
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
                          <span style={{ color: '#ffcc80', fontSize: '12px' }}>
                            Acquired from {pick.originalOwner} via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                            {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                          </span>
                        ) : (
                          <span style={{ color: '#666', fontSize: '12px' }}>Original round selection</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>

            {ownerStats[selectedOwner]?.tradedAway.length > 0 && (
              <div style={{ marginTop: '25px' }}>
                <div style={{ fontWeight: 'bold', color: '#ff5252', marginBottom: '10px', fontSize: '13px' }}>
                  Original Picks Traded Away by {selectedOwner} ({ownerStats[selectedOwner]?.tradedAway.length})
                </div>
                <table style={styles.table}>
                  <thead style={styles.tableHead}>
                    <tr>
                      <th style={styles.th}>Round</th>
                      <th style={styles.th}>Status</th>
                      <th style={styles.th}>New Owner</th>
                      <th style={styles.th}>Trade Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ownerStats[selectedOwner]?.tradedAway.map((pick, idx) => (
                      <tr key={`away-${idx}`} style={{ ...styles.tableRow, opacity: 0.85 }}>
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
                          <span style={{ color: '#ff8a80', fontSize: '12px' }}>
                            Traded to {pick.currentOwner} via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
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

        {activeSubTab === 'history' && (
          <div>
            <table style={styles.table}>
              <thead style={styles.tableHead}>
                <tr>
                  <th style={styles.th}>Date</th>
                  <th style={styles.th}>Trade #</th>
                  <th style={styles.th}>Sending Owner</th>
                  <th style={styles.th}>Receiving Owner</th>
                  <th style={styles.th}>Draft Asset Traded</th>
                  <th style={styles.th}>Associated ESPN Move / Details</th>
                </tr>
              </thead>
              <tbody>
                {assetTradesList.map((t, idx) => (
                  <tr key={`history-${idx}`} style={styles.tableRow}>
                    <td style={{ ...styles.td, fontFamily: 'monospace', color: '#aaa', fontSize: '11px' }}>
                      {t.trade_date}
                    </td>
                    <td style={styles.td}>
                      <span style={{
                        padding: '2px 6px',
                        background: '#333',
                        color: '#03dac6',
                        borderRadius: '4px',
                        fontWeight: 'bold',
                        fontSize: '11px'
                      }}>
                        #{t.trade_id}
                      </span>
                    </td>
                    <td style={{ ...styles.td, fontWeight: 'bold', color: '#ff8a80' }}>
                      {t.sending_owner}
                    </td>
                    <td style={{ ...styles.td, fontWeight: 'bold', color: '#69f0ae' }}>
                      {t.receiving_owner}
                    </td>
                    <td style={styles.td}>
                      <span style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: '4px',
                        padding: '3px 8px',
                        borderRadius: '4px',
                        background: '#ffb74d25',
                        color: '#ffb74d',
                        border: '1px solid #ffb74d60',
                        fontWeight: 'bold',
                        fontSize: '11px'
                      }}>
                        <span>🎟️</span>
                        <span>{t.asset_name}</span>
                        {t.round_num && <span style={{ fontSize: '10px', color: '#ffa726' }}>(Round {t.round_num})</span>}
                      </span>
                    </td>
                    <td style={{ ...styles.td, color: '#ccc', fontSize: '12px' }}>
                      {t.notes || `Trade #${t.trade_id}`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {activeSubTab === '2026board' && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  Filter 2026 Board:
                </span>
                {['ALL', 'KEEPERS', 'EARLY', 'MID', 'LATE'].map(rf => (
                  <button
                    key={rf}
                    onClick={() => setRoundFilter(rf)}
                    style={{
                      background: roundFilter === rf ? '#333' : '#1e1e1e',
                      color: roundFilter === rf ? '#00e676' : '#888',
                      border: roundFilter === rf ? '1px solid #00e676' : '1px solid #333',
                      borderRadius: '4px',
                      padding: '3px 8px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    {rf === 'ALL' ? 'All (R1-32)' : rf === 'KEEPERS' ? 'Keepers (R1-5)' : rf === 'EARLY' ? 'Early (R6-14)' : rf === 'MID' ? 'Mid (R15-23)' : 'Late (R24-32)'}
                  </button>
                ))}
              </div>

              <span style={{ fontSize: '11px', color: '#03dac6', fontStyle: 'italic' }}>
                * Rounds 1–5 are Keepers; Rounds 6–20 include purchased Comp Picks; Rounds 27–32 show Offset picks.
              </span>
            </div>

            <div style={{ overflowX: 'auto' }}>
              <table style={{ ...styles.table, minWidth: '950px' }}>
                <thead style={styles.tableHead}>
                  <tr>
                    <th style={{ ...styles.th, width: '100px' }}>Round</th>
                    {DRAFT_OWNERS.map(o => (
                      <th key={o} style={{ ...styles.th, textAlign: 'center' }}>
                        {o}
                      </th>
                    ))}
                    <th style={{ ...styles.th, textAlign: 'center', width: '160px', color: '#bb86fc' }}>
                      End-of-Round Comp Picks
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {Array.from({ length: 32 }, (_, i) => i + 1)
                    .filter(r => {
                      if (roundFilter === 'ALL') return true;
                      if (roundFilter === 'KEEPERS') return r <= 5;
                      if (roundFilter === 'EARLY') return r >= 6 && r <= 14;
                      if (roundFilter === 'MID') return r >= 15 && r <= 23;
                      if (roundFilter === 'LATE') return r >= 24;
                      return true;
                    })
                    .map(rNum => {
                      const isKeeperRound = rNum <= 5;
                      const compPicksThisRound = (compPicks || []).filter(cp => cp.round_num === rNum && cp.action_type === 'BOUGHT');

                      return (
                        <tr key={`2026-round-${rNum}`} style={styles.tableRow}>
                          <td style={{ ...styles.td, fontWeight: 'bold', color: isKeeperRound ? '#ffb74d' : '#03dac6', whiteSpace: 'nowrap' }}>
                            {isKeeperRound ? `💎 Round ${rNum} (Keeper)` : `Round ${rNum}`}
                          </td>

                          {DRAFT_OWNERS.map(owner => {
                            if (isKeeperRound) {
                              const keeper = (keepers || []).find(k => k.owner === owner && k.keeper_slot === rNum);
                              return (
                                <td key={owner} style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                                  {keeper ? (
                                    <div style={{
                                      background: 'rgba(255, 183, 77, 0.15)',
                                      border: '1px solid rgba(255, 183, 77, 0.4)',
                                      borderRadius: '6px',
                                      padding: '4px 6px',
                                      display: 'inline-block',
                                      minWidth: '85px'
                                    }}>
                                      <div style={{ fontWeight: 'bold', fontSize: '11px', color: '#fff' }}>
                                        {keeper.player_name}
                                      </div>
                                      <div style={{ fontSize: '9px', color: '#ffb74d' }}>
                                        ${keeper.cost} • Rank {keeper.rank || 'N/A'}
                                      </div>
                                    </div>
                                  ) : (
                                    <span style={{ color: '#666' }}>-</span>
                                  )}
                                </td>
                              );
                            }

                            // Standard draft rounds (6 to 32)
                            const isOffset = (compPicks || []).some(cp => cp.owner === owner && cp.round_num === rNum && cp.action_type === 'OFFSET_LOST');

                            return (
                              <td key={owner} style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                                {isOffset ? (
                                  <div style={{
                                    background: 'rgba(244, 67, 54, 0.15)',
                                    border: '1px solid rgba(244, 67, 54, 0.4)',
                                    borderRadius: '6px',
                                    padding: '4px 6px',
                                    display: 'inline-block',
                                    minWidth: '85px'
                                  }}>
                                    <div style={{ fontSize: '11px', fontWeight: 'bold', color: '#f44336', textDecoration: 'line-through' }}>
                                      Pick Slot
                                    </div>
                                    <div style={{ fontSize: '9px', color: '#ff8a80', fontWeight: 'bold' }}>
                                      🚫 Offset (Comp Pick)
                                    </div>
                                  </div>
                                ) : (
                                  <div style={{
                                    background: '#1a1a1a',
                                    border: '1px solid #333',
                                    borderRadius: '6px',
                                    padding: '4px 6px',
                                    display: 'inline-block',
                                    minWidth: '75px'
                                  }}>
                                    <span style={{ fontSize: '11px', color: '#aaa' }}>Standard Pick</span>
                                  </div>
                                )}
                              </td>
                            );
                          })}

                          {/* End-of-round compensation picks column */}
                          <td style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                            {compPicksThisRound.length > 0 ? (
                              <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
                                {compPicksThisRound.map(cp => (
                                  <span key={cp.id || `${cp.owner}-${cp.round_num}`} style={{
                                    background: 'rgba(187, 134, 252, 0.2)',
                                    border: '1px solid #bb86fc',
                                    borderRadius: '4px',
                                    padding: '2px 6px',
                                    fontSize: '10px',
                                    fontWeight: 'bold',
                                    color: '#bb86fc'
                                  }}>
                                    🎟️ {cp.owner} (${cp.cost_or_income})
                                  </span>
                                ))}
                              </div>
                            ) : (
                              <span style={{ color: '#555', fontSize: '11px' }}>-</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
