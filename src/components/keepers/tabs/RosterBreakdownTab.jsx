import React from 'react';

export default function RosterBreakdownTab({
  keeperOwnerFilter,
  setKeeperOwnerFilter,
  seasonYear,
  DRAFT_MANAGERS,
  keepersByOwner,
  onPlayerClick
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Filter Bar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <span style={{ fontSize: '13px', color: '#888' }}>Filter Owner:</span>
        <select
          value={keeperOwnerFilter}
          onChange={e => setKeeperOwnerFilter(e.target.value)}
          style={{
            background: '#222',
            color: '#fff',
            border: '1px solid #444',
            borderRadius: '4px',
            padding: '6px 12px',
            fontSize: '12px'
          }}
        >
          <option value="ALL">All Owners ({seasonYear >= 2027 ? 54 : 45} Keepers)</option>
          {DRAFT_MANAGERS.map(m => (
            <option key={m} value={m}>{m}</option>
          ))}
        </select>
      </div>

      {/* Grid of Owner Keeper Rosters */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))',
        gap: '14px'
      }}>
        {DRAFT_MANAGERS.filter(m => keeperOwnerFilter === 'ALL' || keeperOwnerFilter === m).map(owner => {
          const ownerKeepers = keepersByOwner[owner] || [];
          const totalCost = ownerKeepers.reduce((sum, k) => sum + (k.cost || 0), 0);
          const validRanks = ownerKeepers.map(k => k.rank).filter(Boolean);
          const avgRank = validRanks.length ? (validRanks.reduce((s, r) => s + r, 0) / validRanks.length).toFixed(1) : 0;

          return (
            <div
              key={owner}
              style={{
                background: '#181818',
                border: '1px solid #2a2a2a',
                borderRadius: '8px',
                padding: '14px',
                display: 'flex',
                flexDirection: 'column',
                gap: '10px'
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid #2a2a2a', paddingBottom: '8px' }}>
                <span style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff' }}>{owner}</span>
                <div style={{ display: 'flex', gap: '8px', fontSize: '11px' }}>
                  <span style={{ color: '#ffb74d' }}>Spend: <strong>${totalCost}</strong></span>
                  <span style={{ color: '#888' }}>Avg Rank: <strong>{avgRank}</strong></span>
                </div>
              </div>

              {/* Keepers list */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                {ownerKeepers.map(k => {
                  const isHigh = k.cost >= 30;
                  const isMid = k.cost >= 15 && k.cost < 30;
                  const isSleeper = k.cost === 0;

                  return (
                    <div
                      key={k.keeper_slot}
                      onClick={() => onPlayerClick && onPlayerClick(k.espn_player_id)}
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        background: '#222',
                        padding: '8px 10px',
                        borderRadius: '6px',
                        border: k.token_applied ? '1px solid #059669' : '1px solid #333',
                        cursor: onPlayerClick ? 'pointer' : 'default',
                        transition: 'background 0.15s ease'
                      }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{
                          fontSize: '11px',
                          fontWeight: 'bold',
                          color: '#666',
                          width: '18px'
                        }}>
                          #{k.keeper_slot}
                        </span>
                        <div>
                          <div style={{ fontWeight: 'bold', fontSize: '13px', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <span>{k.player_name}</span>
                            {k.token_applied && (
                              <span style={{
                                background: 'rgba(16, 185, 129, 0.2)',
                                color: '#10b981',
                                border: '1px solid #059669',
                                fontSize: '10px',
                                padding: '1px 5px',
                                borderRadius: '4px',
                                fontWeight: 'bold'
                              }}>
                                🎫 Midpoint Token
                              </span>
                            )}
                          </div>
                          <div style={{ fontSize: '11px', color: '#888' }}>
                            {k.position || 'N/A'} • {k.mlb_team || 'MLB'} • Rank {k.rank || 'N/A'}
                            {k.token_applied && k.prior_cost != null && k.new_cost != null && (
                              <span style={{ color: '#03dac6', marginLeft: '6px' }}>
                                (Prior: ${k.prior_cost} | 2027: ${k.new_cost} | Midpoint: ${k.cost})
                              </span>
                            )}
                          </div>
                        </div>
                      </div>

                      <div style={{ textAlign: 'right' }}>
                        <span style={{
                          fontWeight: 'bold',
                          fontSize: '13px',
                          padding: '2px 8px',
                          borderRadius: '4px',
                          background: isHigh ? 'rgba(255, 183, 77, 0.2)' : isMid ? 'rgba(3, 218, 198, 0.2)' : isSleeper ? 'rgba(187, 134, 252, 0.2)' : 'rgba(76, 175, 80, 0.2)',
                          color: isHigh ? '#ffb74d' : isMid ? '#03dac6' : isSleeper ? '#bb86fc' : '#4caf50',
                          border: `1px solid ${isHigh ? 'rgba(255, 183, 77, 0.4)' : isMid ? 'rgba(3, 218, 198, 0.4)' : isSleeper ? 'rgba(187, 134, 252, 0.4)' : 'rgba(76, 175, 80, 0.4)'}`
                        }}>
                          ${k.cost}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
