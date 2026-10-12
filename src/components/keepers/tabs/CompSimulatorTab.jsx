import React from 'react';

export default function CompSimulatorTab({
  simulatedOwner,
  setSimulatedOwner,
  compPicks,
  setSimBoughtRounds,
  setSimSoldRounds,
  DRAFT_MANAGERS,
  resetSimulatorToReal,
  handleSaveCompPicks,
  savingCompPicks,
  simCalculations,
  isCommissioner,
  profile,
  seasonYear,
  compBuyPrices,
  simBoughtRounds,
  compSellPrices,
  simSoldRounds
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Simulator Controls & Manager Selector */}
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
          <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#fff' }}>Simulate For Manager:</span>
          <select
            value={simulatedOwner}
            onChange={e => {
              setSimulatedOwner(e.target.value);
              // Reset simulator to that owner's real picks
              const bought = {};
              const sold = {};
              compPicks.filter(p => p.owner === e.target.value).forEach(p => {
                if (p.action_type === 'BOUGHT') bought[p.round_num] = true;
                if (p.action_type === 'SOLD') sold[p.round_num] = true;
              });
              setSimBoughtRounds(bought);
              setSimSoldRounds(sold);
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
            onClick={resetSimulatorToReal}
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
            🔄 Reset Picks
          </button>

          <button
            onClick={handleSaveCompPicks}
            disabled={savingCompPicks || simCalculations.simulatedRemaining < 0}
            style={{
              background: simCalculations.simulatedRemaining < 0
                ? '#444'
                : isCommissioner && simulatedOwner !== profile?.owner_name
                  ? '#d97706'
                  : '#7c3aed',
              color: '#fff',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 14px',
              fontSize: '11px',
              fontWeight: 'bold',
              cursor: (savingCompPicks || simCalculations.simulatedRemaining < 0) ? 'not-allowed' : 'pointer',
              opacity: (savingCompPicks || simCalculations.simulatedRemaining < 0) ? 0.6 : 1,
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              boxShadow: '0 2px 4px rgba(0,0,0,0.3)'
            }}
          >
            {savingCompPicks
              ? 'Saving...'
              : isCommissioner && simulatedOwner !== profile?.owner_name
                ? `👑 Commish Save for ${simulatedOwner}`
                : '💾 Save & Submit Consolation Picks'}
          </button>
        </div>
      </div>

      {/* Real-time Calculation Dashboard */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
        gap: '10px'
      }}>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Base Budget</div>
          <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#03dac6', marginTop: '4px' }}>
            ${simCalculations.baseBudget}
          </div>
        </div>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Keeper Spend</div>
          <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#ffb74d', marginTop: '4px' }}>
            ${simCalculations.keeperSpend}
          </div>
        </div>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Comp Spend (Simulated)</div>
          <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#bb86fc', marginTop: '4px' }}>
            ${simCalculations.spend}
          </div>
        </div>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Comp Income (Simulated)</div>
          <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#4caf50', marginTop: '4px' }}>
            +${simCalculations.income}
          </div>
        </div>
        <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
          <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Simulated Remaining</div>
          <div style={{
            fontSize: '18px',
            fontWeight: 'bold',
            color: simCalculations.simulatedRemaining < 0 ? '#f44336' : '#03dac6',
            marginTop: '4px'
          }}>
            ${simCalculations.simulatedRemaining}
          </div>
        </div>
      </div>

      {/* Pick Impact Analysis Box */}
      <div style={{
        background: '#16191f',
        border: '1px solid #233446',
        borderRadius: '8px',
        padding: '14px',
        display: 'flex',
        flexDirection: 'column',
        gap: '8px'
      }}>
        <span style={{ fontWeight: 'bold', fontSize: '13px', color: '#03dac6' }}>
          🎯 Simulated Draft Pick Order & Roster Impact:
        </span>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'center', fontSize: '12px' }}>
          <span style={{ color: '#aaa' }}>Added Picks (End of Round):</span>
          {simCalculations.boughtList.map(b => (
            <span key={b.round} style={{
              background: 'rgba(3, 218, 198, 0.15)',
              border: '1px solid #03dac6',
              color: '#03dac6',
              padding: '2px 8px',
              borderRadius: '4px',
              fontWeight: 'bold'
            }}>
              🎟️ Round {b.round} (+1 pick, -${b.cost})
            </span>
          ))}
          {simCalculations.boughtList.length === 0 && <span style={{ color: '#666' }}>None</span>}
        </div>

        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'center', fontSize: '12px' }}>
          <span style={{ color: '#aaa' }}>Forfeited Picks (Offset from Draft End):</span>
          {simCalculations.offsetRounds.map(r => (
            <span key={r} style={{
              background: 'rgba(244, 67, 54, 0.15)',
              border: '1px solid #f44336',
              color: '#f44336',
              padding: '2px 8px',
              borderRadius: '4px',
              fontWeight: 'bold'
            }}>
              🚫 Round {r} (Offset/Lost)
            </span>
          ))}
          {simCalculations.offsetRounds.length === 0 && <span style={{ color: '#666' }}>None</span>}
        </div>

        <div style={{ fontSize: '11px', color: '#888', borderTop: '1px solid #222', paddingTop: '6px', marginTop: '4px' }}>
          ⚖️ <strong>Roster Rule:</strong> Fixed roster size of 32 players ({seasonYear >= 2027 ? '6 keepers + 26 drafted players' : '5 keepers + 27 drafted players'}). Buying earlier picks automatically drops late-round selections.
        </div>
      </div>

      {/* Interactive Buy Board */}
      <div style={{
        background: '#181818',
        borderRadius: '8px',
        border: '1px solid #2a2a2a',
        padding: '14px'
      }}>
        <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', marginBottom: '10px' }}>
          🛍️ Purchase Additional Draft Picks ({seasonYear >= 2027 ? 'Rounds 7–21' : 'Rounds 6–20'})
        </div>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
          gap: '8px'
        }}>
          {Object.keys(compBuyPrices).map(rStr => {
            const r = parseInt(rStr, 10);
            const cost = compBuyPrices[r];
            const isBought = !!simBoughtRounds[r];

            return (
              <button
                key={r}
                onClick={() => {
                  setSimBoughtRounds(prev => ({
                    ...prev,
                    [r]: !prev[r]
                  }));
                }}
                style={{
                  background: isBought ? 'rgba(3, 218, 198, 0.2)' : '#222',
                  border: isBought ? '1px solid #03dac6' : '1px solid #333',
                  borderRadius: '6px',
                  padding: '10px 8px',
                  cursor: 'pointer',
                  textAlign: 'center',
                  transition: 'all 0.15s ease'
                }}
              >
                <div style={{ fontWeight: 'bold', fontSize: '13px', color: isBought ? '#03dac6' : '#fff' }}>
                  Round {r}
                </div>
                <div style={{ fontSize: '12px', color: '#ffb74d', marginTop: '2px' }}>
                  Cost: ${cost}
                </div>
                <div style={{
                  fontSize: '10px',
                  fontWeight: 'bold',
                  color: isBought ? '#03dac6' : '#888',
                  marginTop: '4px'
                }}>
                  {isBought ? '✓ PURCHASED' : '+ BUY PICK'}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Interactive Sell Board */}
      <div style={{
        background: '#181818',
        borderRadius: '8px',
        border: '1px solid #2a2a2a',
        padding: '14px'
      }}>
        <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', marginBottom: '10px' }}>
          💵 Sell Draft Picks for Budget Income ({seasonYear >= 2027 ? 'Rounds 7–10' : 'Rounds 6–9'})
        </div>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
          gap: '8px'
        }}>
          {Object.keys(compSellPrices).map(rStr => {
            const r = parseInt(rStr, 10);
            const income = compSellPrices[r];
            const isSold = !!simSoldRounds[r];

            return (
              <button
                key={r}
                onClick={() => {
                  setSimSoldRounds(prev => ({
                    ...prev,
                    [r]: !prev[r]
                  }));
                }}
                style={{
                  background: isSold ? 'rgba(76, 175, 80, 0.2)' : '#222',
                  border: isSold ? '1px solid #4caf50' : '1px solid #333',
                  borderRadius: '6px',
                  padding: '10px 8px',
                  cursor: 'pointer',
                  textAlign: 'center',
                  transition: 'all 0.15s ease'
                }}
              >
                <div style={{ fontWeight: 'bold', fontSize: '13px', color: isSold ? '#4caf50' : '#fff' }}>
                  Round {r}
                </div>
                <div style={{ fontSize: '12px', color: '#4caf50', marginTop: '2px' }}>
                  Gain: +${income}
                </div>
                <div style={{
                  fontSize: '10px',
                  fontWeight: 'bold',
                  color: isSold ? '#4caf50' : '#888',
                  marginTop: '4px'
                }}>
                  {isSold ? '✓ SOLD FOR CASH' : '- SELL PICK'}
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
