import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { DRAFT_OWNERS, ROSTER_SLOTS, getEffectiveQS } from '../utils/draftCalculations';

export default function RosterManagerPanel({ allPicks, players, currentUser }) {
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
      qs: pitchers.reduce((sum, p) => sum + (getEffectiveQS(p) || parseFloat(p.ZIPSQS) || 0), 0),
      era: pitchers.length > 0 ?
        pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSERA) || 0), 0) / pitchers.length : 0,
      whip: pitchers.length > 0 ?
        pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSWHIP) || 0), 0) / pitchers.length : 0,
      sv: pitchers.reduce((sum, p) => sum + (parseFloat(p['ZIPSSV+HDs']) || 0), 0)
    };
  }, [assignments]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1', minHeight: 0 }}>
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
                  <td style={styles.td}>{player && isPitcher ? Math.round(getEffectiveQS(player) || player.ZIPSQS || 0) : '-'}</td>
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
