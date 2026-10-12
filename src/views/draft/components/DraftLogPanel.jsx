import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import { DRAFT_OWNERS, getEffectiveQS } from '../utils/draftCalculations';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../../../utils/headshotUtils';
import PlayerNameButton from './PlayerNameButton';

export default function DraftLogPanel({ allPicks, players, onPlayerClick }) {
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
                        style={{ color: '#fff', fontWeight: 'bold' }}
                      />
                    </div>
                  </td>
                  <td style={styles.td}>{player.Position}</td>
                  <td style={styles.td}>{player.Team}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSR || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSHR || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSRBI || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSSB || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSOBP || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSK || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (getEffectiveQS(player) || player.ZIPSQS || '-')}</td>
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
