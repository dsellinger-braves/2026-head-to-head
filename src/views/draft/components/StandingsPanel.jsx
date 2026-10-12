import { useState, useMemo } from 'react';
import styles from '../styles/draftRoomStyles';
import {
  CATEGORIES,
  OWNER_ARCHETYPES,
  normalizeManager,
  getRotoBadgeStyle,
  formatRotoStat,
  computeLiveDraftRoto
} from '../../../utils/draftRotoEngine';

export default function StandingsPanel({ allPicks = [], keepers = [], roomSeason = 2027 }) {
  const [viewMode, setViewMode] = useState('points'); // 'points' | 'stats'

  const roto = useMemo(() => {
    return computeLiveDraftRoto({
      allPicks,
      keepers,
      seasonYear: roomSeason
    });
  }, [allPicks, keepers, roomSeason]);

  const teams = roto.teams || [];

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={{ ...styles.wrHeader, display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <span style={{ fontSize: '18px' }}>🏆</span>
          <div>
            <div style={{ fontSize: '15px', fontWeight: 'bold', color: '#fff' }}>
              Live 10-Category Projected Rotisserie Standings
            </div>
            <div style={{ fontSize: '11px', color: '#94a3b8' }}>
              Full Roto Scoring (Keepers + Live Draft Picks) • Max 90.0 Total Points
            </div>
          </div>
        </div>

        {/* View Toggle */}
        <div style={{ display: 'flex', gap: '6px', background: '#1e293b', padding: '3px', borderRadius: '6px' }}>
          <button
            onClick={() => setViewMode('points')}
            style={{
              background: viewMode === 'points' ? '#03dac6' : 'transparent',
              color: viewMode === 'points' ? '#000' : '#94a3b8',
              border: 'none',
              borderRadius: '4px',
              padding: '4px 10px',
              fontSize: '11px',
              fontWeight: 'bold',
              cursor: 'pointer'
            }}
          >
            📊 Roto Points (1–9)
          </button>
          <button
            onClick={() => setViewMode('stats')}
            style={{
              background: viewMode === 'stats' ? '#03dac6' : 'transparent',
              color: viewMode === 'stats' ? '#000' : '#94a3b8',
              border: 'none',
              borderRadius: '4px',
              padding: '4px 10px',
              fontSize: '11px',
              fontWeight: 'bold',
              cursor: 'pointer'
            }}
          >
            📈 Raw Projected Stats
          </button>
        </div>
      </div>

      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th} width="40">#</th>
              <th style={styles.th} width="160">Owner & Archetype</th>
              <th style={{ ...styles.th, textAlign: 'center' }} width="75">Players</th>
              <th style={{ ...styles.th, color: '#ffc107', textAlign: 'center', fontWeight: 'bold' }} width="80">Total Pts</th>
              <th style={{ ...styles.th, color: '#38bdf8', textAlign: 'center' }} width="65">Bat Pts</th>
              <th style={{ ...styles.th, color: '#a855f7', textAlign: 'center' }} width="65">Pit Pts</th>
              {CATEGORIES.map(c => (
                <th key={c.key} style={{ ...styles.th, textAlign: 'center' }}>
                  {c.label}
                </th>
              ))}
              <th style={styles.th} width="160">Primary Deficit</th>
            </tr>
          </thead>
          <tbody>
            {teams.map((t, idx) => {
              const arch = OWNER_ARCHETYPES[t.owner] || OWNER_ARCHETYPES[normalizeManager(t.owner)];
              const rankColor = idx === 0 ? '#ffd700' : idx === 1 ? '#c0c0c0' : idx === 2 ? '#cd7f32' : '#fff';
              return (
                <tr key={t.owner} style={styles.tableRow}>
                  <td style={{ ...styles.td, textAlign: 'center', fontWeight: 'bold', color: rankColor, fontSize: '13px' }}>
                    {idx === 0 ? '🥇' : idx === 1 ? '🥈' : idx === 2 ? '🥉' : idx + 1}
                  </td>
                  <td style={styles.td}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontSize: '15px' }}>{arch?.archetype?.emoji || '⚾'}</span>
                      <div>
                        <div style={{ fontWeight: 'bold', color: '#fff', fontSize: '13px' }}>
                          {t.owner}
                        </div>
                        {arch?.archetype?.name && (
                          <div style={{ fontSize: '10px', color: '#94a3b8' }}>
                            {arch.archetype.name}
                          </div>
                        )}
                      </div>
                    </div>
                  </td>
                  <td style={{ ...styles.td, textAlign: 'center', fontSize: '11px', color: '#cbd5e1' }}>
                    <span title={`${t.keeperCount} Keepers + ${t.pickCount} Draft Picks`}>
                      {t.totalPlayers} <span style={{ color: '#64748b' }}>({t.keeperCount}k/{t.pickCount}p)</span>
                    </span>
                  </td>
                  <td style={{ ...styles.td, textAlign: 'center', fontWeight: 'bold', fontSize: '14px', color: '#ffc107', background: 'rgba(255, 193, 7, 0.08)' }}>
                    {t.totalPoints.toFixed(1)}
                  </td>
                  <td style={{ ...styles.td, textAlign: 'center', color: '#38bdf8', fontWeight: '600' }}>
                    {t.battingPoints.toFixed(1)}
                  </td>
                  <td style={{ ...styles.td, textAlign: 'center', color: '#a855f7', fontWeight: '600' }}>
                    {t.pitchingPoints.toFixed(1)}
                  </td>

                  {/* 10 Category Columns */}
                  {CATEGORIES.map(c => {
                    const pts = t.points[c.key];
                    const raw = t.rawStats[c.key];
                    const badge = getRotoBadgeStyle(pts);

                    return (
                      <td key={c.key} style={{ ...styles.td, textAlign: 'center', verticalAlign: 'middle', padding: '6px 4px' }}>
                        {viewMode === 'points' ? (
                          <div style={{ display: 'inline-flex', flexDirection: 'column', alignItems: 'center' }}>
                            <span style={{
                              background: badge.bg,
                              color: badge.text,
                              border: `1px solid ${badge.border}`,
                              borderRadius: '4px',
                              padding: '2px 6px',
                              fontSize: '11px',
                              fontWeight: 'bold',
                              minWidth: '32px'
                            }}>
                              {pts.toFixed(1)}
                            </span>
                            <span style={{ fontSize: '9px', color: '#64748b', marginTop: '2px' }}>
                              {formatRotoStat(c.key, raw)}
                            </span>
                          </div>
                        ) : (
                          <div style={{ display: 'inline-flex', flexDirection: 'column', alignItems: 'center' }}>
                            <span style={{ fontWeight: 'bold', color: '#fff', fontSize: '12px' }}>
                              {formatRotoStat(c.key, raw)}
                            </span>
                            <span style={{ fontSize: '9px', color: badge.text }}>
                              ({pts.toFixed(1)} pts)
                            </span>
                          </div>
                        )}
                      </td>
                    );
                  })}

                  {/* Primary Deficit pill */}
                  <td style={styles.td}>
                    {t.deficits && t.deficits.length > 0 ? (
                      <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                        {t.deficits.map(d => (
                          <span
                            key={d.key}
                            style={{
                              background: 'rgba(244, 63, 94, 0.15)',
                              color: '#f87171',
                              border: '1px solid rgba(244, 63, 94, 0.3)',
                              borderRadius: '4px',
                              padding: '1px 5px',
                              fontSize: '10px',
                              fontWeight: '600'
                            }}
                          >
                            {d.label} ({d.pts.toFixed(1)})
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span style={{ color: '#64748b', fontSize: '11px' }}>Balanced</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
