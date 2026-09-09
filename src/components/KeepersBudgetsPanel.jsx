import React, { useState, useMemo } from 'react';

// Price schedule constants matching Google Sheet 'Compensation Picks' (tab 904314503)
const COMP_BUY_PRICES = {
  6: 15, 7: 14, 8: 13, 9: 12, 10: 11,
  11: 10, 12: 9, 13: 8, 14: 7, 15: 6,
  16: 5, 17: 4, 18: 3, 19: 2, 20: 1
};

const COMP_SELL_PRICES = {
  6: 8, 7: 4, 8: 2, 9: 1
};

const DRAFT_MANAGERS = [
  'Tim', 'Daniel', 'Will', 'Adrian', 'Garrett', 'Alex', 'Mark', 'Preston', 'Anil'
];

// Helper to compute keeper cost from rank based on Keeper Costs tab
function calculateKeeperCostFromRank(rank) {
  if (!rank || rank > 150) return 0;
  if (rank === 1) return 40;
  if (rank === 2) return 36;
  if (rank === 3) return 33;
  if (rank === 4) return 31;
  if (rank === 5) return 30;
  if (rank === 6) return 29;
  if (rank === 7) return 28;
  if (rank === 8) return 27;
  if (rank === 9) return 26;
  if (rank === 10) return 25;
  if (rank <= 14) return 23;
  if (rank <= 16) return 22;
  if (rank <= 19) return 21;
  if (rank <= 22) return 20;
  if (rank <= 25) return 19;
  if (rank <= 28) return 18;
  if (rank <= 31) return 17;
  if (rank <= 35) return 16;
  if (rank <= 40) return 15;
  if (rank <= 45) return 14;
  if (rank <= 50) return 13;
  if (rank <= 55) return 12;
  if (rank <= 65) return 11;
  if (rank <= 75) return 9;
  if (rank <= 85) return 8;
  if (rank <= 95) return 7;
  if (rank <= 110) return 5;
  if (rank <= 125) return 4;
  if (rank <= 140) return 3;
  if (rank <= 150) return 2;
  return 0;
}

export default function KeepersBudgetsPanel({
  teamBudgets = [],
  compPicks = [],
  keepers = [],
  players = [],
  currentUser = 'Daniel',
  onPlayerClick
}) {
  const [activeTab, setActiveTab] = useState('matrix'); // 'matrix' | 'rosters' | 'simulator' | 'planner'
  const [selectedOwner, setSelectedOwner] = useState(currentUser || 'Daniel');
  const [keeperOwnerFilter, setKeeperOwnerFilter] = useState('ALL');

  // Simulation state for the interactive comp pick simulator
  const [simulatedOwner, setSimulatedOwner] = useState(currentUser || 'Daniel');
  const [simBoughtRounds, setSimBoughtRounds] = useState({});
  const [simSoldRounds, setSimSoldRounds] = useState({});

  // Planner state for keeper what-if planning
  const [plannerOwner, setPlannerOwner] = useState(currentUser || 'Daniel');
  const [plannerSearch, setPlannerSearch] = useState('');
  const [replacedKeepers, setReplacedKeepers] = useState({}); // { slot: newPlayerObj }

  // Load active owner's real-world comp picks into simulator when owner changes
  const activeRealBudgets = useMemo(() => {
    return teamBudgets.find(b => b.owner === simulatedOwner) || {
      base_budget: 100,
      keeper_spend: 0,
      comp_pick_spend: 0,
      comp_pick_income: 0,
      final_budget: 100,
      net_picks: 0
    };
  }, [teamBudgets, simulatedOwner]);

  // Real-world picks for the simulated owner
  const realOwnerCompPicks = useMemo(() => {
    return compPicks.filter(p => p.owner === simulatedOwner);
  }, [compPicks, simulatedOwner]);

  // Sync simulator defaults from real world when owner changes
  const resetSimulatorToReal = () => {
    const bought = {};
    const sold = {};
    realOwnerCompPicks.forEach(p => {
      if (p.action_type === 'BOUGHT') bought[p.round_num] = true;
      if (p.action_type === 'SOLD') sold[p.round_num] = true;
    });
    setSimBoughtRounds(bought);
    setSimSoldRounds(sold);
  };

  // Group keepers by owner
  const keepersByOwner = useMemo(() => {
    const map = {};
    DRAFT_MANAGERS.forEach(mgr => { map[mgr] = []; });
    (keepers || []).forEach(k => {
      if (!map[k.owner]) map[k.owner] = [];
      map[k.owner].push(k);
    });
    // Sort each owner's keepers by slot
    Object.keys(map).forEach(mgr => {
      map[mgr].sort((a, b) => (a.keeper_slot || 0) - (b.keeper_slot || 0));
    });
    return map;
  }, [keepers]);

  // Group comp picks by owner
  const compPicksByOwner = useMemo(() => {
    const map = {};
    DRAFT_MANAGERS.forEach(mgr => {
      map[mgr] = { bought: [], lost: [], sold: [] };
    });
    (compPicks || []).forEach(p => {
      if (!map[p.owner]) map[p.owner] = { bought: [], lost: [], sold: [] };
      if (p.action_type === 'BOUGHT') map[p.owner].bought.push(p);
      else if (p.action_type === 'OFFSET_LOST') map[p.owner].lost.push(p);
      else if (p.action_type === 'SOLD') map[p.owner].sold.push(p);
    });
    return map;
  }, [compPicks]);

  // Dynamic simulation calculations
  const simCalculations = useMemo(() => {
    let spend = 0;
    let income = 0;
    const boughtList = [];
    const soldList = [];

    Object.keys(simBoughtRounds).forEach(rStr => {
      if (simBoughtRounds[rStr]) {
        const r = parseInt(rStr, 10);
        const cost = COMP_BUY_PRICES[r] || (21 - r);
        spend += cost;
        boughtList.push({ round: r, cost });
      }
    });

    Object.keys(simSoldRounds).forEach(rStr => {
      if (simSoldRounds[rStr]) {
        const r = parseInt(rStr, 10);
        const inc = COMP_SELL_PRICES[r] || 0;
        income += inc;
        soldList.push({ round: r, income: inc });
      }
    });

    boughtList.sort((a, b) => a.round - b.round);
    soldList.sort((a, b) => a.round - b.round);

    // Compute forfeited latest rounds (starting from 32 down)
    const offsetCount = boughtList.length;
    const offsetRounds = [];
    for (let i = 0; i < offsetCount; i++) {
      offsetRounds.push(32 - i);
    }

    const keeperSpend = activeRealBudgets.keeper_spend || 0;
    const baseBudget = activeRealBudgets.base_budget || 100;
    const simulatedRemaining = baseBudget - keeperSpend - spend + income;
    const netPicks = boughtList.length - soldList.length;

    return {
      spend,
      income,
      boughtList,
      soldList,
      offsetRounds,
      baseBudget,
      keeperSpend,
      simulatedRemaining,
      netPicks
    };
  }, [simBoughtRounds, simSoldRounds, activeRealBudgets]);

  // Planner calculations for keeper replacements
  const plannerData = useMemo(() => {
    const originalKeepers = keepersByOwner[plannerOwner] || [];
    const currentKeepers = originalKeepers.map(k => {
      if (replacedKeepers[k.keeper_slot]) {
        const rep = replacedKeepers[k.keeper_slot];
        const rank = rep['Hefty Single Season Rank'] || rep['Dynasty Rank'] || 150;
        const cost = calculateKeeperCostFromRank(rank);
        return {
          ...k,
          player_name: rep.Player || rep.full_name || 'Selected Player',
          espn_player_id: rep['ESPN PlayerID'] || rep.id,
          position: rep.Position,
          mlb_team: rep.Team,
          rank: rank,
          cost: cost,
          isReplaced: true
        };
      }
      return { ...k, isReplaced: false };
    });

    const totalCost = currentKeepers.reduce((sum, k) => sum + (k.cost || 0), 0);
    const validRanks = currentKeepers.map(k => k.rank).filter(Boolean);
    const avgRank = validRanks.length ? (validRanks.reduce((s, r) => s + r, 0) / validRanks.length).toFixed(1) : 0;
    const baseBudget = (teamBudgets.find(b => b.owner === plannerOwner)?.base_budget) || 100;
    const remainingBudget = baseBudget - totalCost;

    return {
      keepers: currentKeepers,
      totalCost,
      avgRank,
      baseBudget,
      remainingBudget
    };
  }, [keepersByOwner, plannerOwner, replacedKeepers, teamBudgets]);

  return (
    <div style={{ width: '100%', gridColumn: '1 / -1', display: 'flex', flexDirection: 'column', gap: '15px' }}>
      {/* Top Header & Sub-Tab Bar */}
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '12px 16px',
        background: '#1a1a1a',
        borderRadius: '8px',
        border: '1px solid #2e2e2e',
        gap: '12px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <span style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span>💎</span> Keepers, Compensation Picks & Budgets
          </span>
          <span style={{
            background: 'rgba(3, 218, 198, 0.15)',
            color: '#03dac6',
            padding: '2px 8px',
            borderRadius: '12px',
            fontSize: '11px',
            fontWeight: '600',
            border: '1px solid rgba(3, 218, 198, 0.3)'
          }}>
            2026 Season Ground Truth
          </span>
        </div>

        {/* Sub-Tab Buttons */}
        <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '3px', border: '1px solid #333', gap: '4px' }}>
          <button
            onClick={() => setActiveTab('matrix')}
            style={{
              background: activeTab === 'matrix' ? '#03dac6' : 'transparent',
              color: activeTab === 'matrix' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            💰 Budget Matrix
          </button>
          <button
            onClick={() => setActiveTab('rosters')}
            style={{
              background: activeTab === 'rosters' ? '#ffb74d' : 'transparent',
              color: activeTab === 'rosters' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            💎 Keeper Rosters (45)
          </button>
          <button
            onClick={() => { setActiveTab('simulator'); resetSimulatorToReal(); }}
            style={{
              background: activeTab === 'simulator' ? '#bb86fc' : 'transparent',
              color: activeTab === 'simulator' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            🎮 Comp Pick Simulator
          </button>
          <button
            onClick={() => setActiveTab('planner')}
            style={{
              background: activeTab === 'planner' ? '#4caf50' : 'transparent',
              color: activeTab === 'planner' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            📋 Keeper What-If Planner
          </button>
        </div>
      </div>

      {/* SUB-TAB 1: BUDGET & COMP PICK MATRIX */}
      {activeTab === 'matrix' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Quick Metrics Header */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
            gap: '10px'
          }}>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>League Base Budget</div>
              <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#03dac6', marginTop: '4px' }}>$100.00</div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Per Team Cap</div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Total Keeper Spend</div>
              <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#ffb74d', marginTop: '4px' }}>
                ${teamBudgets.reduce((s, b) => s + (b.keeper_spend || 0), 0)}
              </div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Across 45 Keepers</div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Comp Pick Spend</div>
              <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#bb86fc', marginTop: '4px' }}>
                ${teamBudgets.reduce((s, b) => s + (b.comp_pick_spend || 0), 0)}
              </div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>11 Compensations Added</div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Net Late Picks Offset</div>
              <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#f44336', marginTop: '4px' }}>
                -11 Picks
              </div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Forfeited from Rds 27-32</div>
            </div>
          </div>

          {/* 9 Owner Cards */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
            gap: '12px'
          }}>
            {teamBudgets.map(b => {
              const picks = compPicksByOwner[b.owner] || { bought: [], lost: [], sold: [] };
              const isSelected = selectedOwner === b.owner;
              return (
                <div
                  key={b.owner}
                  onClick={() => setSelectedOwner(b.owner)}
                  style={{
                    background: isSelected ? '#22272e' : '#181818',
                    border: isSelected ? '1px solid #03dac6' : '1px solid #2a2a2a',
                    borderRadius: '8px',
                    padding: '14px',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease'
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff' }}>{b.owner}</span>
                      <span style={{ fontSize: '11px', color: '#888' }}>Fin #{b.finish_rank}</span>
                    </div>
                    <span style={{
                      fontSize: '11px',
                      fontWeight: 'bold',
                      padding: '2px 8px',
                      borderRadius: '10px',
                      background: b.final_budget < 0 ? 'rgba(244, 67, 54, 0.2)' : 'rgba(76, 175, 80, 0.2)',
                      color: b.final_budget < 0 ? '#f44336' : '#4caf50'
                    }}>
                      Rem: ${b.final_budget}
                    </span>
                  </div>

                  {/* Budget breakdown progress bar */}
                  <div style={{ display: 'flex', height: '6px', borderRadius: '3px', overflow: 'hidden', background: '#333', marginBottom: '10px' }}>
                    <div style={{ width: `${Math.min(100, (b.keeper_spend / b.base_budget) * 100)}%`, background: '#ffb74d' }} title={`Keepers: $${b.keeper_spend}`} />
                    <div style={{ width: `${Math.min(100, (b.comp_pick_spend / b.base_budget) * 100)}%`, background: '#bb86fc' }} title={`Comp Picks: $${b.comp_pick_spend}`} />
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px', fontSize: '11px', color: '#aaa', marginBottom: '10px' }}>
                    <div>Base Budget: <strong style={{ color: '#fff' }}>${b.base_budget}</strong></div>
                    <div>Keeper Spend: <strong style={{ color: '#ffb74d' }}>${b.keeper_spend}</strong></div>
                    <div>Comp Pick Spend: <strong style={{ color: '#bb86fc' }}>${b.comp_pick_spend}</strong></div>
                    <div>Net Draft Picks: <strong style={{ color: b.net_picks > 0 ? '#03dac6' : '#fff' }}>{b.net_picks > 0 ? `+${b.net_picks}` : b.net_picks}</strong></div>
                  </div>

                  {/* Bought & Offset Pills */}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                    {picks.bought.map(p => (
                      <span key={p.round_num} style={{
                        background: 'rgba(187, 134, 252, 0.15)',
                        border: '1px solid rgba(187, 134, 252, 0.4)',
                        color: '#bb86fc',
                        fontSize: '10px',
                        padding: '1px 6px',
                        borderRadius: '4px'
                      }}>
                        +Rd {p.round_num} (${p.cost_or_income})
                      </span>
                    ))}
                    {picks.lost.map(p => (
                      <span key={p.round_num} style={{
                        background: 'rgba(244, 67, 54, 0.15)',
                        border: '1px solid rgba(244, 67, 54, 0.4)',
                        color: '#f44336',
                        fontSize: '10px',
                        padding: '1px 6px',
                        borderRadius: '4px'
                      }}>
                        -Rd {p.round_num} (Offset)
                      </span>
                    ))}
                    {picks.bought.length === 0 && picks.lost.length === 0 && (
                      <span style={{ fontSize: '10px', color: '#666', fontStyle: 'italic' }}>No comp picks traded</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Full Tabular Breakdown */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            overflow: 'hidden'
          }}>
            <div style={{ padding: '12px 16px', borderBottom: '1px solid #2a2a2a', fontWeight: 'bold', color: '#fff', fontSize: '13px' }}>
              📊 Complete 2026 Budget & Compensation Pick Ledger
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'left' }}>
                <thead>
                  <tr style={{ background: '#1f1f1f', color: '#888', borderBottom: '1px solid #333' }}>
                    <th style={{ padding: '10px 14px' }}>Finish</th>
                    <th style={{ padding: '10px 14px' }}>Owner</th>
                    <th style={{ padding: '10px 14px' }}>Base Budget</th>
                    <th style={{ padding: '10px 14px' }}>Keeper Spend</th>
                    <th style={{ padding: '10px 14px' }}>Comp Pick Spend</th>
                    <th style={{ padding: '10px 14px' }}>Comp Pick Income</th>
                    <th style={{ padding: '10px 14px' }}>Final Remaining</th>
                    <th style={{ padding: '10px 14px' }}>Purchased Picks</th>
                    <th style={{ padding: '10px 14px' }}>Offset Picks</th>
                    <th style={{ padding: '10px 14px' }}>Net Picks</th>
                  </tr>
                </thead>
                <tbody>
                  {teamBudgets.map((b, idx) => {
                    const picks = compPicksByOwner[b.owner] || { bought: [], lost: [], sold: [] };
                    return (
                      <tr
                        key={b.owner}
                        style={{
                          borderBottom: '1px solid #262626',
                          background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)'
                        }}
                      >
                        <td style={{ padding: '10px 14px', color: '#888' }}>#{b.finish_rank}</td>
                        <td style={{ padding: '10px 14px', fontWeight: 'bold', color: '#fff' }}>{b.owner}</td>
                        <td style={{ padding: '10px 14px', color: '#03dac6' }}>${b.base_budget}</td>
                        <td style={{ padding: '10px 14px', color: '#ffb74d' }}>${b.keeper_spend}</td>
                        <td style={{ padding: '10px 14px', color: '#bb86fc' }}>${b.comp_pick_spend}</td>
                        <td style={{ padding: '10px 14px', color: '#4caf50' }}>${b.comp_pick_income}</td>
                        <td style={{ padding: '10px 14px', fontWeight: 'bold', color: b.final_budget < 0 ? '#f44336' : '#fff' }}>
                          ${b.final_budget}
                        </td>
                        <td style={{ padding: '10px 14px' }}>
                          {picks.bought.map(p => `Rd ${p.round_num} ($${p.cost_or_income})`).join(', ') || 'None'}
                        </td>
                        <td style={{ padding: '10px 14px', color: '#f44336' }}>
                          {picks.lost.map(p => `Rd ${p.round_num}`).join(', ') || 'None'}
                        </td>
                        <td style={{ padding: '10px 14px', fontWeight: 'bold', color: b.net_picks > 0 ? '#03dac6' : '#fff' }}>
                          {b.net_picks > 0 ? `+${b.net_picks}` : b.net_picks}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* SUB-TAB 2: KEEPER ROSTERS */}
      {activeTab === 'rosters' && (
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
              <option value="ALL">All Owners (45 Keepers)</option>
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

                  {/* 5 Keepers */}
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
                            border: '1px solid #333',
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
                              <div style={{ fontWeight: 'bold', fontSize: '13px', color: '#fff' }}>
                                {k.player_name}
                              </div>
                              <div style={{ fontSize: '11px', color: '#888' }}>
                                {k.position || 'N/A'} • {k.mlb_team || 'MLB'} • Rank {k.rank || 'N/A'}
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
      )}

      {/* SUB-TAB 3: INTERACTIVE COMPENSATION PICK SIMULATOR */}
      {activeTab === 'simulator' && (
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
              🔄 Reset to Actual 2026 Selections
            </button>
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
              ⚖️ <strong>Roster Rule:</strong> Fixed roster size of 32 players (5 keepers + 27 drafted players). Buying earlier picks automatically drops late-round selections.
            </div>
          </div>

          {/* Interactive Buy Board (Rounds 6 to 20) */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            padding: '14px'
          }}>
            <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', marginBottom: '10px' }}>
              🛍️ Purchase Additional Draft Picks (Rounds 6–20)
            </div>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
              gap: '8px'
            }}>
              {Object.keys(COMP_BUY_PRICES).map(rStr => {
                const r = parseInt(rStr, 10);
                const cost = COMP_BUY_PRICES[r];
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

          {/* Interactive Sell Board (Rounds 6 to 9) */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            padding: '14px'
          }}>
            <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', marginBottom: '10px' }}>
              💵 Sell Draft Picks for Budget Income (Rounds 6–9)
            </div>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
              gap: '8px'
            }}>
              {Object.keys(COMP_SELL_PRICES).map(rStr => {
                const r = parseInt(rStr, 10);
                const income = COMP_SELL_PRICES[r];
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
      )}

      {/* SUB-TAB 4: KEEPER WHAT-IF PLANNER */}
      {activeTab === 'planner' && (
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
              <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#fff' }}>Planning For Manager:</span>
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
              🔄 Reset to Confirmed Keepers
            </button>
          </div>

          {/* Real-time What-If Keeper Dashboard */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
            gap: '10px'
          }}>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Simulated Keeper Spend</div>
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
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Across 5 Keepers</div>
            </div>
          </div>

          {/* 5 Keepers with Swap Options */}
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
                  border: k.isReplaced ? '1px solid #4caf50' : '1px solid #2e2e2e',
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

                <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff' }}>
                  {k.player_name}
                </div>
                <div style={{ fontSize: '11px', color: '#aaa' }}>
                  {k.position || 'N/A'} • {k.mlb_team || 'MLB'} • Rank: {k.rank || 'N/A'}
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '4px' }}>
                  <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#ffb74d' }}>
                    Cost: ${k.cost}
                  </span>
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
            ))}
          </div>

          {/* Search Player Pool to Swap */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            padding: '14px',
            display: 'flex',
            flexDirection: 'column',
            gap: '10px'
          }}>
            <div style={{ fontWeight: 'bold', fontSize: '13px', color: '#fff' }}>
              🔍 Search Player Pool to Test Swapping into Keepers:
            </div>
            <input
              type="text"
              placeholder="Search by player name or position (e.g. Bobby Witt, Soto, SP)..."
              value={plannerSearch}
              onChange={e => setPlannerSearch(e.target.value)}
              style={{
                background: '#111',
                color: '#fff',
                border: '1px solid #444',
                borderRadius: '6px',
                padding: '8px 12px',
                fontSize: '13px'
              }}
            />

            {plannerSearch.trim().length >= 2 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', maxHeight: '240px', overflowY: 'auto' }}>
                {players
                  .filter(p => {
                    const name = (p.Player || p.full_name || '').toLowerCase();
                    const pos = (p.Position || '').toLowerCase();
                    const q = plannerSearch.toLowerCase();
                    return name.includes(q) || pos.includes(q);
                  })
                  .slice(0, 8)
                  .map(p => {
                    const rank = p['Hefty Single Season Rank'] || p['Dynasty Rank'] || 150;
                    const cost = calculateKeeperCostFromRank(rank);

                    return (
                      <div
                        key={p['ESPN PlayerID'] || p.id}
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          background: '#222',
                          padding: '8px 12px',
                          borderRadius: '6px',
                          border: '1px solid #333'
                        }}
                      >
                        <div>
                          <strong style={{ color: '#fff', fontSize: '13px' }}>{p.Player || p.full_name}</strong>
                          <span style={{ color: '#888', fontSize: '11px', marginLeft: '8px' }}>
                            {p.Position} • {p.Team} • Rank #{rank} • Cost: ${cost}
                          </span>
                        </div>

                        <div style={{ display: 'flex', gap: '4px' }}>
                          {[1, 2, 3, 4, 5].map(slot => (
                            <button
                              key={slot}
                              onClick={() => {
                                setReplacedKeepers(prev => ({
                                  ...prev,
                                  [slot]: p
                                }));
                              }}
                              style={{
                                background: '#333',
                                color: '#03dac6',
                                border: '1px solid #444',
                                borderRadius: '3px',
                                padding: '3px 6px',
                                fontSize: '10px',
                                fontWeight: 'bold',
                                cursor: 'pointer'
                              }}
                              title={`Swap into Keeper Slot ${slot}`}
                            >
                              Slot {slot}
                            </button>
                          ))}
                        </div>
                      </div>
                    );
                  })}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
