import React from 'react';

export default function LeagueSettingsTab({
  isCommissioner,
  user,
  profile,
  deadlineInput,
  setDeadlineInput,
  baselineBudgetInput,
  setBaselineBudgetInput,
  handleSaveLeagueSettings,
  savingSettings,
  handleSaveManualAdjustments,
  savingAdjustments,
  teamBudgets,
  adjustmentsForm,
  setAdjustmentsForm
}) {
  if (!isCommissioner) {
    return (
      <div style={{
        background: '#1a1a1a',
        borderRadius: '8px',
        border: '1px solid #333',
        padding: '40px 20px',
        textAlign: 'center',
        maxWidth: '500px',
        margin: '20px auto'
      }}>
        <div style={{ fontSize: '36px', marginBottom: '12px' }}>🔒</div>
        <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff' }}>Commissioner & Admin Access Only</div>
        <div style={{ fontSize: '12px', color: '#888', marginTop: '8px', lineHeight: '1.5' }}>
          Setting the official keeper deadline, base budget, and applying punitive or award adjustments to individual owners is restricted to Commissioner (<strong>Adrian</strong>) and Admin (<strong>Dan</strong>).
        </div>
        {user ? (
          <div style={{ fontSize: '11px', color: '#818cf8', marginTop: '16px', paddingTop: '12px', borderTop: '1px solid #282828' }}>
            Logged in as <strong>{profile?.owner_name || user.email}</strong> (Owner)
          </div>
        ) : (
          <div style={{ fontSize: '11px', color: '#666', marginTop: '16px', paddingTop: '12px', borderTop: '1px solid #282828' }}>
            Log in with Discord via the top menu to verify commissioner status.
          </div>
        )}
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* Header banner */}
      <div style={{
        background: 'rgba(245, 158, 11, 0.12)',
        border: '1px solid rgba(245, 158, 11, 0.35)',
        borderRadius: '8px',
        padding: '14px 18px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '12px',
        flexWrap: 'wrap'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <span style={{ fontSize: '20px' }}>👑</span>
          <div>
            <div style={{ fontSize: '14px', fontWeight: 'bold', color: '#fbbf24' }}>
              Commissioner Offseason Control Suite
            </div>
            <div style={{ fontSize: '12px', color: '#cbd5e1' }}>
              Authorized for <strong>Dan</strong> and <strong>Adrian</strong>. Changes take effect across the entire league.
            </div>
          </div>
        </div>
        <span style={{
          background: '#f59e0b',
          color: '#000',
          padding: '3px 8px',
          borderRadius: '4px',
          fontSize: '11px',
          fontWeight: '900',
          textTransform: 'uppercase',
          letterSpacing: '0.5px'
        }}>
          Full Admin Rights
        </span>
      </div>

      {/* Section 1: Global Settings (Deadline & Base Budget) */}
      <div style={{
        background: '#181818',
        borderRadius: '8px',
        border: '1px solid #2a2a2a',
        padding: '18px',
        display: 'flex',
        flexDirection: 'column',
        gap: '16px'
      }}>
        <div style={{ fontSize: '14px', fontWeight: 'bold', color: '#fff', borderBottom: '1px solid #282828', paddingBottom: '8px' }}>
          ⏱️ Offseason Schedule & Budget Baseline
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px' }}>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: 'bold', color: '#aaa', marginBottom: '6px' }}>
              Official Keeper Locking Deadline:
            </label>
            <input
              type="datetime-local"
              value={deadlineInput}
              onChange={e => setDeadlineInput(e.target.value)}
              style={{
                width: '100%',
                background: '#111',
                border: '1px solid #444',
                borderRadius: '6px',
                padding: '8px 12px',
                color: '#fff',
                fontSize: '13px'
              }}
            />
            <div style={{ fontSize: '11px', color: '#666', marginTop: '4px' }}>
              After this timestamp, all non-commissioner keeper selections are locked.
            </div>
          </div>

          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: 'bold', color: '#aaa', marginBottom: '6px' }}>
              League Baseline Draft Budget ($):
            </label>
            <input
              type="number"
              value={baselineBudgetInput}
              onChange={e => setBaselineBudgetInput(e.target.value)}
              style={{
                width: '100%',
                background: '#111',
                border: '1px solid #444',
                borderRadius: '6px',
                padding: '8px 12px',
                color: '#fff',
                fontSize: '13px'
              }}
            />
            <div style={{ fontSize: '11px', color: '#666', marginTop: '4px' }}>
              Standard draft budget baseline allocated to all 9 managers (default: $100).
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: '8px' }}>
          <button
            onClick={handleSaveLeagueSettings}
            disabled={savingSettings}
            style={{
              background: '#f59e0b',
              color: '#000',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: savingSettings ? 'not-allowed' : 'pointer',
              opacity: savingSettings ? 0.6 : 1,
              display: 'flex',
              alignItems: 'center',
              gap: '6px'
            }}
          >
            {savingSettings ? 'Saving...' : '💾 Save Global Settings'}
          </button>
        </div>
      </div>

      {/* Section 2: Owner Manual Adjustments (Punitive Penalties & Awards) */}
      <div style={{
        background: '#181818',
        borderRadius: '8px',
        border: '1px solid #2a2a2a',
        overflow: 'hidden'
      }}>
        <div style={{
          padding: '16px 18px',
          borderBottom: '1px solid #282828',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: '10px'
        }}>
          <div>
            <div style={{ fontSize: '14px', fontWeight: 'bold', color: '#fff' }}>
              ⚖️ Owner Punitive & Award Manual Budget Adjustments
            </div>
            <div style={{ fontSize: '11px', color: '#888', marginTop: '2px' }}>
              Specify positive adjustments (awards/bonuses) or negative adjustments (fines/penalties) for each owner.
            </div>
          </div>

          <button
            onClick={handleSaveManualAdjustments}
            disabled={savingAdjustments}
            style={{
              background: '#059669',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 18px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: savingAdjustments ? 'not-allowed' : 'pointer',
              opacity: savingAdjustments ? 0.6 : 1,
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              boxShadow: '0 2px 4px rgba(0,0,0,0.3)'
            }}
          >
            {savingAdjustments ? 'Saving...' : '💾 Save All Budget Adjustments'}
          </button>
        </div>

        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'left' }}>
            <thead>
              <tr style={{ background: '#111', color: '#888', borderBottom: '1px solid #333' }}>
                <th style={{ padding: '10px 14px' }}>Owner</th>
                <th style={{ padding: '10px 12px', textAlign: 'center' }}>Base</th>
                <th style={{ padding: '10px 12px', textAlign: 'center' }}>Keepers</th>
                <th style={{ padding: '10px 12px', textAlign: 'center' }}>Comp Net</th>
                <th style={{ padding: '10px 12px', minWidth: '130px' }}>Manual Adj ($)</th>
                <th style={{ padding: '10px 12px', minWidth: '220px' }}>Adjustment Reason / Notes</th>
                <th style={{ padding: '10px 14px', textAlign: 'right' }}>Calculated Final</th>
              </tr>
            </thead>
            <tbody>
              {teamBudgets.map((b, idx) => {
                const formVal = adjustmentsForm[b.owner] || { adjustment: b.manual_adjustment || 0, notes: b.adjustment_notes || '' };
                const adjNum = parseFloat(formVal.adjustment) || 0;
                const calculatedFinal = b.base_budget - (b.keeper_spend || 0) - (b.comp_pick_spend || 0) + (b.comp_pick_income || 0) + adjNum;

                return (
                  <tr key={b.owner} style={{
                    borderBottom: '1px solid #222',
                    background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)'
                  }}>
                    <td style={{ padding: '10px 14px', fontWeight: 'bold', color: '#fff' }}>
                      {b.owner}
                      <span style={{ fontSize: '10px', color: '#666', marginLeft: '6px' }}>Team {b.team_id}</span>
                    </td>
                    <td style={{ padding: '10px 12px', textAlign: 'center', color: '#aaa' }}>
                      ${b.base_budget}
                    </td>
                    <td style={{ padding: '10px 12px', textAlign: 'center', color: '#ffb74d' }}>
                      ${b.keeper_spend || 0}
                    </td>
                    <td style={{ padding: '10px 12px', textAlign: 'center', color: '#bb86fc' }}>
                      ${((b.comp_pick_income || 0) - (b.comp_pick_spend || 0))}
                    </td>
                    <td style={{ padding: '10px 12px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <span style={{ color: adjNum > 0 ? '#4caf50' : adjNum < 0 ? '#f44336' : '#666', fontWeight: 'bold' }}>$</span>
                        <input
                          type="number"
                          value={formVal.adjustment}
                          onChange={e => {
                            const val = e.target.value;
                            setAdjustmentsForm(prev => ({
                              ...prev,
                              [b.owner]: {
                                ...prev[b.owner],
                                adjustment: val
                              }
                            }));
                          }}
                          placeholder="0"
                          style={{
                            width: '80px',
                            background: '#111',
                            border: adjNum !== 0 ? (adjNum > 0 ? '1px solid #4caf50' : '1px solid #f44336') : '1px solid #333',
                            borderRadius: '4px',
                            padding: '4px 8px',
                            color: adjNum > 0 ? '#4caf50' : adjNum < 0 ? '#f44336' : '#fff',
                            fontSize: '12px',
                            fontWeight: 'bold'
                          }}
                        />
                      </div>
                    </td>
                    <td style={{ padding: '10px 12px' }}>
                      <input
                        type="text"
                        value={formVal.notes}
                        onChange={e => {
                          const val = e.target.value;
                          setAdjustmentsForm(prev => ({
                            ...prev,
                            [b.owner]: {
                              ...prev[b.owner],
                              notes: val
                            }
                          }));
                        }}
                        placeholder="e.g. Late fee fine (-$5) or Toilet bowl prize (+$5)"
                        style={{
                          width: '100%',
                          background: '#111',
                          border: '1px solid #333',
                          borderRadius: '4px',
                          padding: '4px 8px',
                          color: '#ddd',
                          fontSize: '11px'
                        }}
                      />
                    </td>
                    <td style={{ padding: '10px 14px', textAlign: 'right' }}>
                      <span style={{
                        fontWeight: 'bold',
                        fontSize: '13px',
                        color: calculatedFinal < 0 ? '#f44336' : '#4caf50',
                        padding: '2px 8px',
                        borderRadius: '4px',
                        background: calculatedFinal < 0 ? 'rgba(244, 67, 54, 0.15)' : 'rgba(76, 175, 80, 0.15)'
                      }}>
                        ${calculatedFinal}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
