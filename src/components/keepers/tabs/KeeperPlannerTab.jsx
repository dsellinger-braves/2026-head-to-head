import React from 'react';

export default function KeeperPlannerTab({
  plannerOwner,
  setPlannerOwner,
  setReplacedKeepers,
  DRAFT_MANAGERS,
  handleSaveKeepers,
  savingKeepers,
  isDeadlinePassed,
  isCommissioner,
  profile,
  plannerData,
  seasonYear,
  keeperSlots,
  setTokenSlot,
  plannerScope,
  setPlannerScope,
  plannerOwnerRosterCount,
  players,
  plannerSearch,
  setPlannerSearch,
  plannerCandidates,
  calculateKeeperCostFromRank,
  normalizeManager,
  getPriorCost
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Planner Controls */}
      <div style={{
        background: '#1e1e1e',
        padding: '14px 16px',
        borderRadius: '8px',
        border: '1px solid #333',
        display: 'flex',
        flexWrap: 'wrap',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: '12px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#fff' }}>Submitting For Manager:</span>
          <select
            value={plannerOwner}
            onChange={e => {
              setPlannerOwner(e.target.value);
              setReplacedKeepers({});
            }}
            style={{
              background: '#111',
              color: '#fff',
              border: '1px solid #444',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '13px',
              fontWeight: 'bold'
            }}
          >
            {DRAFT_MANAGERS.map(m => (
              <option key={m} value={m}>{m}</option>
            ))}
          </select>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <button
            onClick={() => setReplacedKeepers({})}
            style={{
              background: '#333',
              color: '#fff',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '11px',
              cursor: 'pointer'
            }}
          >
            🔄 Reset Keepers
          </button>

          <button
            onClick={handleSaveKeepers}
            disabled={savingKeepers || (isDeadlinePassed && !isCommissioner)}
            style={{
              background: (isDeadlinePassed && !isCommissioner)
                ? '#444'
                : isCommissioner && plannerOwner !== profile?.owner_name
                  ? '#d97706'
                  : '#059669',
              color: '#fff',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 14px',
              fontSize: '11px',
              fontWeight: 'bold',
              cursor: ((isDeadlinePassed && !isCommissioner) || savingKeepers) ? 'not-allowed' : 'pointer',
              opacity: ((isDeadlinePassed && !isCommissioner) || savingKeepers) ? 0.6 : 1,
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              boxShadow: '0 2px 4px rgba(0,0,0,0.3)'
            }}
          >
            {savingKeepers
              ? 'Saving...'
              : isDeadlinePassed && !isCommissioner
                ? '🔒 Keepers Locked'
                : isCommissioner && plannerOwner !== profile?.owner_name
                  ? `👑 Commish Save for ${plannerOwner}`
                  : '💾 Save & Submit Official Keepers'}
          </button>
        </div>
      </div>

      {/* Keeper Submission Overview Dashboard */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
        gap: '10px'
      }}>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Keeper Spend</div>
          <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#ffb74d', marginTop: '4px' }}>
            ${plannerData.totalCost}
          </div>
          <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Cap: ${plannerData.baseBudget}</div>
        </div>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Remaining for Draft/Comp</div>
          <div style={{
            fontSize: '20px',
            fontWeight: 'bold',
            color: plannerData.remainingBudget < 0 ? '#f44336' : '#03dac6',
            marginTop: '4px'
          }}>
            ${plannerData.remainingBudget}
          </div>
          <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>
            {plannerData.remainingBudget < 0 ? '⚠️ OVER BUDGET' : 'Under Cap'}
          </div>
        </div>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Average Keeper Rank</div>
          <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#bb86fc', marginTop: '4px' }}>
            {plannerData.avgRank}
          </div>
          <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Across {keeperSlots.length} Keepers</div>
        </div>
        {seasonYear === 2027 && (
          <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
            <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Keeper Discount Token</div>
            <div style={{ fontSize: '20px', fontWeight: 'bold', color: plannerData.tokenSlot ? '#10b981' : '#aaa', marginTop: '4px' }}>
              {plannerData.tokenSlot ? `Slot #${plannerData.tokenSlot}` : '1 Available'}
            </div>
            <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>
              {plannerData.tokenSlot ? `Saved $${plannerData.totalTokenSavings}` : 'Pays midpoint of 2026 & 2027 (rounded down)'}
            </div>
          </div>
        )}
      </div>

      {/* Keepers with Swap Options */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
        gap: '12px'
      }}>
        {plannerData.keepers.map(k => (
          <div
            key={k.keeper_slot}
            style={{
              background: '#181818',
              border: k.token_applied ? '1px solid #10b981' : k.isReplaced ? '1px solid #4caf50' : '1px solid #2e2e2e',
              borderRadius: '8px',
              padding: '12px',
              display: 'flex',
              flexDirection: 'column',
              gap: '8px'
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontSize: '12px', fontWeight: 'bold', color: '#888' }}>
                Keeper Slot #{k.keeper_slot}
              </span>
              <div style={{ display: 'flex', gap: '4px' }}>
                {k.token_applied && (
                  <span style={{
                    background: 'rgba(16, 185, 129, 0.2)',
                    color: '#10b981',
                    fontSize: '10px',
                    padding: '1px 6px',
                    borderRadius: '4px',
                    fontWeight: 'bold'
                  }}>
                    🎫 MIDPOINT TOKEN
                  </span>
                )}
                {k.isReplaced && (
                  <span style={{
                    background: 'rgba(76, 175, 80, 0.2)',
                    color: '#4caf50',
                    fontSize: '10px',
                    padding: '1px 6px',
                    borderRadius: '4px',
                    fontWeight: 'bold'
                  }}>
                    TEST SWAP
                  </span>
                )}
              </div>
            </div>

            <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff' }}>
              {k.player_name}
            </div>
            <div style={{ fontSize: '11px', color: '#aaa' }}>
              {k.position || 'N/A'} • {k.mlb_team || 'MLB'} • Rank: {k.rank || 'N/A'}
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '4px' }}>
              <div>
                <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#ffb74d' }}>
                  Cost: ${k.cost}
                </span>
                {k.token_applied && (
                  <span style={{
                    marginLeft: '6px',
                    fontSize: '10px',
                    fontWeight: 'bold',
                    color: '#10b981',
                    background: 'rgba(16, 185, 129, 0.15)',
                    padding: '1px 5px',
                    borderRadius: '4px',
                    border: '1px solid rgba(16, 185, 129, 0.3)'
                  }}>
                    -${k.token_savings}
                  </span>
                )}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                {seasonYear === 2027 && !k.isEmpty && (
                  <button
                    onClick={() => setTokenSlot(prev => prev === k.keeper_slot ? null : k.keeper_slot)}
                    style={{
                      background: k.token_applied ? 'rgba(16, 185, 129, 0.25)' : '#262626',
                      color: k.token_applied ? '#6ee7b7' : '#bbb',
                      border: k.token_applied ? '1px solid #10b981' : '1px solid #444',
                      borderRadius: '4px',
                      padding: '3px 8px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '4px',
                      transition: 'all 0.15s ease'
                    }}
                    title={k.token_applied ? 'Click to remove token' : 'Apply 1 token to pay midpoint of prior year price and 2027 price (rounded down to whole dollar)'}
                  >
                    <span>{k.token_applied ? '✅ Active' : '🎫 Token'}</span>
                  </button>
                )}
                {k.isReplaced && (
                  <button
                    onClick={() => {
                      setReplacedKeepers(prev => {
                        const copy = { ...prev };
                        delete copy[k.keeper_slot];
                        return copy;
                      });
                    }}
                    style={{
                      background: 'transparent',
                      border: 'none',
                      color: '#f44336',
                      fontSize: '11px',
                      cursor: 'pointer',
                      textDecoration: 'underline'
                    }}
                  >
                    Revert
                  </button>
                )}
              </div>
            </div>
            {k.token_applied && (
              <div style={{ fontSize: '10px', color: '#03dac6', marginTop: '2px', background: '#111', padding: '4px 6px', borderRadius: '4px' }}>
                Prior (2026): <strong>${k.prior_cost ?? k.cost}</strong> • Standard (2027): <strong>${k.new_cost ?? k.cost}</strong> • You Pay Midpoint: <strong>${k.cost}</strong>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Search & Select Player Pool to Swap */}
      <div style={{
        background: '#181818',
        borderRadius: '8px',
        border: '1px solid #2a2a2a',
        padding: '16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px'
      }}>
        <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: '8px' }}>
          <div>
            <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span>🎯</span>
              <span>Swap Into Keepers: Select From Roster or Search Pool</span>
            </div>
            <div style={{ fontSize: '11px', color: '#888', marginTop: '2px' }}>
              Defaulting to <strong>{plannerOwner}</strong>'s current roster. Pick any player and click [Slot 1] – [Slot {keeperSlots.length}] to test swapping them into your keepers.
            </div>
          </div>

          {/* Scope filter pills */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
            <button
              type="button"
              onClick={() => setPlannerScope('roster')}
              style={{
                background: plannerScope === 'roster' ? '#059669' : '#262626',
                color: plannerScope === 'roster' ? '#fff' : '#aaa',
                border: plannerScope === 'roster' ? '1px solid #10b981' : '1px solid #3e3e3e',
                borderRadius: '6px',
                padding: '5px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '5px',
                transition: 'all 0.15s ease'
              }}
            >
              <span>👤</span>
              <span>{plannerOwner}'s Current Roster ({plannerOwnerRosterCount})</span>
            </button>

            <button
              type="button"
              onClick={() => setPlannerScope('roster_fa')}
              style={{
                background: plannerScope === 'roster_fa' ? '#0284c7' : '#262626',
                color: plannerScope === 'roster_fa' ? '#fff' : '#aaa',
                border: plannerScope === 'roster_fa' ? '1px solid #38bdf8' : '1px solid #3e3e3e',
                borderRadius: '6px',
                padding: '5px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '5px',
                transition: 'all 0.15s ease'
              }}
            >
              <span>🆓</span>
              <span>Roster + Free Agents</span>
            </button>

            <button
              type="button"
              onClick={() => setPlannerScope('all')}
              style={{
                background: plannerScope === 'all' ? '#7c3aed' : '#262626',
                color: plannerScope === 'all' ? '#fff' : '#aaa',
                border: plannerScope === 'all' ? '1px solid #a78bfa' : '1px solid #3e3e3e',
                borderRadius: '6px',
                padding: '5px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '5px',
                transition: 'all 0.15s ease'
              }}
            >
              <span>🌐</span>
              <span>All Players ({players.length})</span>
            </button>
          </div>
        </div>

        {/* Filter Search Input */}
        <div style={{ position: 'relative', width: '100%' }}>
          <input
            type="text"
            placeholder={
              plannerScope === 'roster'
                ? `Filter ${plannerOwner}'s roster by player name, position (SP, OF, SS), or MLB team...`
                : 'Search player pool by name, position (e.g. SP, OF, C), or MLB team (e.g. LAD, NYY)...'
            }
            value={plannerSearch}
            onChange={e => setPlannerSearch(e.target.value)}
            style={{
              width: '100%',
              background: '#111',
              color: '#fff',
              border: '1px solid #444',
              borderRadius: '6px',
              padding: '9px 36px 9px 12px',
              fontSize: '13px',
              boxSizing: 'border-box',
              outline: 'none'
            }}
          />
          {plannerSearch && (
            <button
              type="button"
              onClick={() => setPlannerSearch('')}
              style={{
                position: 'absolute',
                right: '10px',
                top: '50%',
                transform: 'translateY(-50%)',
                background: 'transparent',
                border: 'none',
                color: '#888',
                fontSize: '14px',
                cursor: 'pointer',
                padding: '2px 6px'
              }}
              title="Clear search filter"
            >
              ✕
            </button>
          )}
        </div>

        {/* Candidate List Counter & Status */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '11px', color: '#888', px: '2px' }}>
          <span>
            {plannerCandidates.length === 0
              ? 'No matching players found'
              : `Showing ${Math.min(plannerCandidates.length, 60)} of ${plannerCandidates.length} ${plannerScope === 'roster' ? `${plannerOwner} rostered` : ''} players`}
          </span>
          <span style={{ color: '#666' }}>
            💡 Click any slot button to preview keeper budget impact
          </span>
        </div>

        {/* Candidate List */}
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          gap: '6px',
          maxHeight: '360px',
          overflowY: 'auto',
          paddingRight: '4px'
        }}>
          {plannerCandidates.length === 0 ? (
            <div style={{
              padding: '30px 16px',
              textAlign: 'center',
              color: '#777',
              fontSize: '13px',
              background: '#141414',
              borderRadius: '6px',
              border: '1px dashed #333'
            }}>
              {plannerSearch.trim()
                ? `No players found matching "${plannerSearch}" in ${plannerScope === 'roster' ? `${plannerOwner}'s roster` : plannerScope === 'roster_fa' ? 'roster + free agents' : 'player pool'}.`
                : `No players available in selected scope.`}
            </div>
          ) : (
            plannerCandidates.slice(0, 60).map(p => {
              const pid = String(p.id || p.espn_player_id || p['ESPN PlayerID'] || '');
              const pName = p.name || p.Player || p.full_name || 'Player';
              const rank = p.rank || 999;
              const cost = p.cost !== undefined ? p.cost : calculateKeeperCostFromRank(rank);
              const isCurrentManagerRoster = normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner);
              const isFreeAgent = !p.rosterOwner || p.rosterOwner === 'Available' || p.rosterOwner === 'Free Agent' || p.isFreeAgent;

              // Check if player is currently in one of the keeper slots
              const currentKeeperSlot = keeperSlots.find(slotNum => {
                const slotKeeper = plannerData.keepers.find(k => k.keeper_slot === slotNum);
                if (!slotKeeper || slotKeeper.isEmpty) return false;
                const skId = String(slotKeeper.espn_player_id || '');
                const skName = (slotKeeper.player_name || '').toLowerCase().trim();
                return (pid && pid === skId) || (skName && skName === pName.toLowerCase().trim());
              });

              return (
                <div
                  key={`${pid || pName}_${p.position}`}
                  style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    background: currentKeeperSlot ? '#1c261e' : '#202020',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    border: currentKeeperSlot ? '1px solid #059669' : '1px solid #303030',
                    gap: '8px',
                    transition: 'background 0.15s ease'
                  }}
                >
                  <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '8px' }}>
                    <strong style={{ color: '#fff', fontSize: '13px' }}>
                      {pName}
                    </strong>

                    {/* Position & Team badge */}
                    <span style={{
                      background: '#2d2d2d',
                      color: '#ccc',
                      fontSize: '10px',
                      padding: '2px 6px',
                      borderRadius: '4px',
                      fontWeight: '600'
                    }}>
                      {p.position || p.Position || 'UTIL'} • {p.team || p.Team || 'MLB'}
                    </span>

                    {/* Rank & Cost badge */}
                    <span style={{
                      background: '#332914',
                      color: '#fbbf24',
                      border: '1px solid #78350f',
                      fontSize: '10px',
                      padding: '2px 6px',
                      borderRadius: '4px',
                      fontWeight: 'bold'
                    }}>
                      Rank #{rank} • ${cost}
                    </span>

                    {seasonYear === 2027 && (
                      <span style={{
                        background: '#132e27',
                        color: '#6ee7b7',
                        border: '1px solid #065f46',
                        fontSize: '10px',
                        padding: '2px 6px',
                        borderRadius: '4px',
                        fontWeight: 'bold'
                      }} title="If your 1 keeper token is applied to this player">
                        Midpoint: ${Math.floor((getPriorCost(pid, pName, cost) + cost) / 2)}
                      </span>
                    )}

                    {/* Ownership badge */}
                    {isCurrentManagerRoster ? (
                      <span style={{
                        background: '#064e3b',
                        color: '#6ee7b7',
                        border: '1px solid #047857',
                        fontSize: '10px',
                        padding: '2px 6px',
                        borderRadius: '4px',
                        fontWeight: '600'
                      }}>
                        👤 {plannerOwner}'s Roster
                      </span>
                    ) : isFreeAgent ? (
                      <span style={{
                        background: '#0c4a6e',
                        color: '#7dd3fc',
                        border: '1px solid #0284c7',
                        fontSize: '10px',
                        padding: '2px 6px',
                        borderRadius: '4px',
                        fontWeight: '600'
                      }}>
                        🆓 Free Agent
                      </span>
                    ) : (
                      <span style={{
                        background: '#1e293b',
                        color: '#94a3b8',
                        border: '1px solid #334155',
                        fontSize: '10px',
                        padding: '2px 6px',
                        borderRadius: '4px',
                        fontWeight: '600'
                      }}>
                        🔒 {p.rosterOwner}
                      </span>
                    )}

                    {/* Already Kept Indicator */}
                    {currentKeeperSlot && (
                      <span style={{
                        background: '#059669',
                        color: '#fff',
                        fontSize: '10px',
                        padding: '2px 6px',
                        borderRadius: '4px',
                        fontWeight: 'bold'
                      }}>
                        ✓ Slot {currentKeeperSlot}
                      </span>
                    )}
                  </div>

                  {/* Swap Buttons */}
                  <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                    {keeperSlots.map(slot => {
                      const isThisSlot = currentKeeperSlot === slot;
                      return (
                        <button
                          key={slot}
                          type="button"
                          onClick={() => {
                            setReplacedKeepers(prev => ({
                              ...prev,
                              [slot]: p
                            }));
                          }}
                          style={{
                            background: isThisSlot ? '#059669' : '#2b2b2b',
                            color: isThisSlot ? '#ffffff' : '#38bdf8',
                            border: isThisSlot ? '1px solid #10b981' : '1px solid #444',
                            borderRadius: '4px',
                            padding: '4px 8px',
                            fontSize: '11px',
                            fontWeight: 'bold',
                            cursor: 'pointer',
                            transition: 'all 0.15s ease'
                          }}
                          title={`Swap ${pName} into Keeper Slot ${slot}`}
                        >
                          {isThisSlot ? `✓ Slot ${slot}` : `Slot ${slot}`}
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}
