import React, { useState, useMemo } from 'react';
import { getActualFinish, getRotoBadgeStyle, formatRotoStat } from '../services/keeperPricing';

/**
 * KeeperMatrixTab
 * Keeper Summary & Roto Standings Tab:
 * - Sub-view selector (Keeper Roto Standings, Budget & Pick Ledger, All-in-One Overview)
 * - 10-category projected rotisserie standings table with expandable keeper rows
 * - Category leaders pill bar
 * - 9-owner budget breakdown cards and complete audit table
 */
export default function KeeperMatrixTab({
  seasonYear,
  sortedTeamBudgets = [],
  keepersByOwner = {},
  compPicksByOwner = {},
  keeperRotoData,
  selectedOwner,
  setSelectedOwner
}) {
  const [summaryViewMode, setSummaryViewMode] = useState('both'); // 'roto' | 'ledger' | 'both'
  const [rotoDisplayMode, setRotoDisplayMode] = useState('both'); // 'both' | 'points' | 'stats'
  const [rotoSortCol, setRotoSortCol] = useState('total');
  const [rotoSortDir, setRotoSortDir] = useState('desc');
  const [expandedRotoOwner, setExpandedRotoOwner] = useState(null);

  const handleRotoSort = (col) => {
    if (rotoSortCol === col) {
      setRotoSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setRotoSortCol(col);
      setRotoSortDir('desc');
    }
  };

  const sortedRotoTeams = useMemo(() => {
    if (!keeperRotoData?.teams) return [];
    const list = [...keeperRotoData.teams];
    return list.sort((a, b) => {
      let valA = 0;
      let valB = 0;
      if (rotoSortCol === 'rank') { valA = a.rank; valB = b.rank; }
      else if (rotoSortCol === 'owner') { valA = a.owner; valB = b.owner; }
      else if (rotoSortCol === 'total') { valA = a.totalPoints; valB = b.totalPoints; }
      else if (rotoSortCol === 'batting') { valA = a.battingPoints; valB = b.battingPoints; }
      else if (rotoSortCol === 'pitching') { valA = a.pitchingPoints; valB = b.pitchingPoints; }
      else if (a.points[rotoSortCol] !== undefined) {
        valA = a.points[rotoSortCol];
        valB = b.points[rotoSortCol];
      }
      if (typeof valA === 'string') {
        return rotoSortDir === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      }
      return rotoSortDir === 'asc' ? valA - valB : valB - valA;
    });
  }, [keeperRotoData, rotoSortCol, rotoSortDir]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Sub-view switcher bar within Keeper Summary */}
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '12px',
        background: '#161616',
        padding: '12px 16px',
        borderRadius: '8px',
        border: '1px solid #2a2a2a'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
          <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Summary View:
          </span>
          <div style={{ display: 'flex', background: '#202020', borderRadius: '6px', padding: '3px', border: '1px solid #333', gap: '4px' }}>
            <button
              onClick={() => setSummaryViewMode('roto')}
              style={{
                background: summaryViewMode === 'roto' ? '#10b981' : 'transparent',
                color: summaryViewMode === 'roto' ? '#000' : '#aaa',
                border: 'none',
                borderRadius: '4px',
                padding: '6px 14px',
                fontSize: '12px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              🏆 Keeper Roto Standings
            </button>
            <button
              onClick={() => setSummaryViewMode('ledger')}
              style={{
                background: summaryViewMode === 'ledger' ? '#03dac6' : 'transparent',
                color: summaryViewMode === 'ledger' ? '#000' : '#aaa',
                border: 'none',
                borderRadius: '4px',
                padding: '6px 14px',
                fontSize: '12px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              💼 Budget & Pick Ledger
            </button>
            <button
              onClick={() => setSummaryViewMode('both')}
              style={{
                background: summaryViewMode === 'both' ? '#bb86fc' : 'transparent',
                color: summaryViewMode === 'both' ? '#000' : '#aaa',
                border: 'none',
                borderRadius: '4px',
                padding: '6px 14px',
                fontSize: '12px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              📊 All-in-One Overview
            </button>
          </div>
        </div>

        {/* Display Mode Toggle for Roto */}
        {(summaryViewMode === 'roto' || summaryViewMode === 'both') && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '11px', color: '#777' }}>Detail:</span>
            <div style={{ display: 'flex', background: '#202020', borderRadius: '4px', padding: '2px', border: '1px solid #333' }}>
              <button
                onClick={() => setRotoDisplayMode('both')}
                style={{
                  background: rotoDisplayMode === 'both' ? '#333' : 'transparent',
                  color: rotoDisplayMode === 'both' ? '#fff' : '#888',
                  border: 'none',
                  borderRadius: '3px',
                  padding: '4px 10px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                Pts + Stats
              </button>
              <button
                onClick={() => setRotoDisplayMode('points')}
                style={{
                  background: rotoDisplayMode === 'points' ? '#333' : 'transparent',
                  color: rotoDisplayMode === 'points' ? '#fff' : '#888',
                  border: 'none',
                  borderRadius: '3px',
                  padding: '4px 10px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                Points Only
              </button>
              <button
                onClick={() => setRotoDisplayMode('stats')}
                style={{
                  background: rotoDisplayMode === 'stats' ? '#333' : 'transparent',
                  color: rotoDisplayMode === 'stats' ? '#fff' : '#888',
                  border: 'none',
                  borderRadius: '3px',
                  padding: '4px 10px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                Raw Stats
              </button>
            </div>
          </div>
        )}
      </div>

      {/* SECTION A: KEEPER ROTO STANDINGS */}
      {(summaryViewMode === 'roto' || summaryViewMode === 'both') && (
        <div style={{
          background: '#181818',
          borderRadius: '8px',
          border: '1px solid #2a2a2a',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
          gap: '12px',
          padding: '16px'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px' }}>
            <div>
              <h3 style={{ margin: 0, fontSize: '16px', fontWeight: 'bold', color: '#fff', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span>🏆</span>
                <span>{seasonYear} Keeper Projected Roto Standings</span>
              </h3>
              <div style={{ fontSize: '12px', color: '#888', marginTop: '2px' }}>
                10-category rotisserie standings computed from currently selected keepers ({seasonYear >= 2027 ? 6 : 5} per owner). Max 90.0 pts (45.0 Batting / 45.0 Pitching).
              </div>
            </div>
            <div style={{ fontSize: '11px', color: '#aaa', background: 'rgba(255,255,255,0.04)', padding: '4px 10px', borderRadius: '6px', border: '1px solid #333' }}>
              Click any column header to sort • Click row to expand roster
            </div>
          </div>

          {/* Category Leaders Pill Bar */}
          {keeperRotoData?.leaders && (
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              flexWrap: 'wrap',
              background: '#121212',
              padding: '8px 12px',
              borderRadius: '6px',
              border: '1px solid #242424'
            }}>
              <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', marginRight: '4px' }}>Leaders:</span>
              <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                🏃 R: <strong>{keeperRotoData.leaders.R?.owner}</strong> ({formatRotoStat('R', keeperRotoData.leaders.R?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                💥 HR: <strong>{keeperRotoData.leaders.HR?.owner}</strong> ({formatRotoStat('HR', keeperRotoData.leaders.HR?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                🎯 RBI: <strong>{keeperRotoData.leaders.RBI?.owner}</strong> ({formatRotoStat('RBI', keeperRotoData.leaders.RBI?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                ⚡ SB: <strong>{keeperRotoData.leaders.SB?.owner}</strong> ({formatRotoStat('SB', keeperRotoData.leaders.SB?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                👁️ OBP: <strong>{keeperRotoData.leaders.OBP?.owner}</strong> ({formatRotoStat('OBP', keeperRotoData.leaders.OBP?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                🔥 K: <strong>{keeperRotoData.leaders.K?.owner}</strong> ({formatRotoStat('K', keeperRotoData.leaders.K?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                ⭐ QS: <strong>{keeperRotoData.leaders.QS?.owner}</strong> ({formatRotoStat('QS', keeperRotoData.leaders.QS?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                🛡️ SV+HD: <strong>{keeperRotoData.leaders.SVHD?.owner}</strong> ({formatRotoStat('SVHD', keeperRotoData.leaders.SVHD?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                🎯 ERA: <strong>{keeperRotoData.leaders.ERA?.owner}</strong> ({formatRotoStat('ERA', keeperRotoData.leaders.ERA?.val)})
              </span>
              <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                🔒 WHIP: <strong>{keeperRotoData.leaders.WHIP?.owner}</strong> ({formatRotoStat('WHIP', keeperRotoData.leaders.WHIP?.val)})
              </span>
            </div>
          )}

          {/* Roto Table */}
          <div style={{ overflowX: 'auto', borderRadius: '6px', border: '1px solid #292929' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'center' }}>
              <thead>
                <tr style={{ background: '#121212', borderBottom: '1px solid #2d2d2d', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  <th colSpan={5} style={{ padding: '6px 10px', textAlign: 'left', color: '#888' }}>
                    Team Standings
                  </th>
                  <th colSpan={5} style={{ padding: '6px 10px', color: '#10b981', borderLeft: '1px solid #333', borderRight: '1px solid #333', background: 'rgba(16, 185, 129, 0.05)' }}>
                    ⚾ Batting Categories
                  </th>
                  <th colSpan={5} style={{ padding: '6px 10px', color: '#bb86fc', background: 'rgba(187, 134, 252, 0.05)' }}>
                    🎯 Pitching Categories
                  </th>
                </tr>

                <tr style={{ background: '#1c1c1c', color: '#aaa', borderBottom: '2px solid #333', userSelect: 'none' }}>
                  <th
                    onClick={() => handleRotoSort('rank')}
                    style={{ padding: '10px 8px', width: '50px', cursor: 'pointer', textAlign: 'center' }}
                  >
                    Rank {rotoSortCol === 'rank' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => handleRotoSort('owner')}
                    style={{ padding: '10px 14px', textAlign: 'left', minWidth: '120px', cursor: 'pointer' }}
                  >
                    Owner {rotoSortCol === 'owner' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => handleRotoSort('total')}
                    style={{ padding: '10px 12px', cursor: 'pointer', color: '#03dac6', minWidth: '85px' }}
                  >
                    Total Pts {rotoSortCol === 'total' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => handleRotoSort('batting')}
                    style={{ padding: '10px 10px', cursor: 'pointer', color: '#10b981', minWidth: '65px' }}
                  >
                    Bat Pts {rotoSortCol === 'batting' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => handleRotoSort('pitching')}
                    style={{ padding: '10px 10px', cursor: 'pointer', color: '#bb86fc', borderRight: '1px solid #333', minWidth: '65px' }}
                  >
                    Pit Pts {rotoSortCol === 'pitching' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                  </th>

                  {['R', 'HR', 'RBI', 'SB', 'OBP'].map(cat => (
                    <th
                      key={cat}
                      onClick={() => handleRotoSort(cat)}
                      style={{
                        padding: '10px 8px',
                        cursor: 'pointer',
                        color: '#10b981',
                        background: rotoSortCol === cat ? 'rgba(16, 185, 129, 0.15)' : 'rgba(16, 185, 129, 0.03)',
                        minWidth: rotoDisplayMode === 'both' ? '68px' : '55px'
                      }}
                    >
                      {cat} {rotoSortCol === cat ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                    </th>
                  ))}

                  {[
                    { key: 'K', label: 'K' },
                    { key: 'QS', label: 'QS' },
                    { key: 'SVHD', label: 'SV+HD' },
                    { key: 'ERA', label: 'ERA' },
                    { key: 'WHIP', label: 'WHIP' }
                  ].map(cat => (
                    <th
                      key={cat.key}
                      onClick={() => handleRotoSort(cat.key)}
                      style={{
                        padding: '10px 8px',
                        cursor: 'pointer',
                        color: '#bb86fc',
                        background: rotoSortCol === cat.key ? 'rgba(187, 134, 252, 0.15)' : 'rgba(187, 134, 252, 0.03)',
                        minWidth: rotoDisplayMode === 'both' ? '68px' : '55px'
                      }}
                    >
                      {cat.label} {rotoSortCol === cat.key ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRotoTeams.map((t, idx) => {
                  const isExpanded = expandedRotoOwner === t.owner;
                  const isUser = t.owner === 'Daniel';
                  const rankBadge = t.rank === 1 ? '🥇' : t.rank === 2 ? '🥈' : t.rank === 3 ? '🥉' : `#${t.rank}`;

                  return (
                    <React.Fragment key={t.owner}>
                      <tr
                        onClick={() => setExpandedRotoOwner(isExpanded ? null : t.owner)}
                        style={{
                          borderBottom: '1px solid #262626',
                          background: isUser ? 'rgba(3, 218, 198, 0.07)' : idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)',
                          cursor: 'pointer',
                          transition: 'background 0.12s ease'
                        }}
                      >
                        <td style={{ padding: '10px 8px', fontWeight: 'bold', fontSize: '13px', color: t.rank <= 3 ? '#fff' : '#888' }}>
                          {rankBadge}
                        </td>
                        <td style={{ padding: '10px 14px', textAlign: 'left' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span style={{ fontWeight: 'bold', color: '#fff', fontSize: '13px' }}>
                              {t.owner}
                            </span>
                            {isUser && (
                              <span style={{ fontSize: '9px', background: '#03dac6', color: '#000', padding: '1px 5px', borderRadius: '3px', fontWeight: 'bold' }}>
                                YOU
                              </span>
                            )}
                            <span style={{ fontSize: '10px', color: '#666', marginLeft: 'auto' }}>
                              {isExpanded ? '▲' : '▼'}
                            </span>
                          </div>
                          <div style={{ fontSize: '10px', color: '#777', marginTop: '1px' }}>
                            {t.keeperCount} {t.keeperCount === 1 ? 'Keeper' : 'Keepers'}
                          </div>
                        </td>

                        {/* Total Points */}
                        <td style={{ padding: '10px 12px' }}>
                          <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#03dac6' }}>
                            {t.totalPoints.toFixed(1)}
                          </div>
                          <div style={{
                            width: '100%',
                            height: '4px',
                            background: '#333',
                            borderRadius: '2px',
                            overflow: 'hidden',
                            marginTop: '3px'
                          }}>
                            <div style={{
                              width: `${Math.min(100, (t.totalPoints / 90) * 100)}%`,
                              height: '100%',
                              background: 'linear-gradient(90deg, #10b981, #03dac6)'
                            }} />
                          </div>
                        </td>

                        {/* Batting Points */}
                        <td style={{ padding: '10px 10px', fontWeight: 'bold', color: '#10b981' }}>
                          {t.battingPoints.toFixed(1)}
                        </td>

                        {/* Pitching Points */}
                        <td style={{ padding: '10px 10px', fontWeight: 'bold', color: '#bb86fc', borderRight: '1px solid #333' }}>
                          {t.pitchingPoints.toFixed(1)}
                        </td>

                        {/* 5 Batting Categories */}
                        {['R', 'HR', 'RBI', 'SB', 'OBP'].map(cat => {
                          const pts = t.points[cat];
                          const raw = t.rawStats[cat];
                          const bStyle = getRotoBadgeStyle(pts);

                          return (
                            <td key={cat} style={{ padding: '8px 6px' }}>
                              {rotoDisplayMode !== 'stats' && (
                                <div style={{
                                  display: 'inline-block',
                                  background: bStyle.bg,
                                  color: bStyle.text,
                                  border: `1px solid ${bStyle.border}`,
                                  borderRadius: '4px',
                                  padding: '1px 6px',
                                  fontSize: '11px',
                                  fontWeight: 'bold',
                                  minWidth: '28px'
                                }}>
                                  {pts.toFixed(1)}
                                </div>
                              )}
                              {rotoDisplayMode !== 'points' && (
                                <div style={{ fontSize: '10px', color: '#aaa', marginTop: rotoDisplayMode === 'both' ? '2px' : '0' }}>
                                  {formatRotoStat(cat, raw)}
                                </div>
                              )}
                            </td>
                          );
                        })}

                        {/* 5 Pitching Categories */}
                        {[
                          { key: 'K', label: 'K' },
                          { key: 'QS', label: 'QS' },
                          { key: 'SVHD', label: 'SV+HD' },
                          { key: 'ERA', label: 'ERA' },
                          { key: 'WHIP', label: 'WHIP' }
                        ].map(cat => {
                          const pts = t.points[cat.key];
                          const raw = t.rawStats[cat.key];
                          const bStyle = getRotoBadgeStyle(pts);

                          return (
                            <td key={cat.key} style={{ padding: '8px 6px' }}>
                              {rotoDisplayMode !== 'stats' && (
                                <div style={{
                                  display: 'inline-block',
                                  background: bStyle.bg,
                                  color: bStyle.text,
                                  border: `1px solid ${bStyle.border}`,
                                  borderRadius: '4px',
                                  padding: '1px 6px',
                                  fontSize: '11px',
                                  fontWeight: 'bold',
                                  minWidth: '28px'
                                }}>
                                  {pts.toFixed(1)}
                                </div>
                              )}
                              {rotoDisplayMode !== 'points' && (
                                <div style={{ fontSize: '10px', color: '#aaa', marginTop: rotoDisplayMode === 'both' ? '2px' : '0' }}>
                                  {formatRotoStat(cat.key, raw)}
                                </div>
                              )}
                            </td>
                          );
                        })}
                      </tr>

                      {/* Expanded Keepers Detail Row */}
                      {isExpanded && (
                        <tr style={{ background: '#121212', borderBottom: '1px solid #333' }}>
                          <td colSpan={15} style={{ padding: '14px 16px', textAlign: 'left' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                              <span style={{ fontSize: '12px', fontWeight: 'bold', color: '#03dac6' }}>
                                💎 {t.owner}'s Kept Players ({t.keepers.length})
                              </span>
                              <span style={{ fontSize: '11px', color: '#777' }}>
                                Total Spend: ${t.keepers.reduce((s, k) => s + (k.cost || 0), 0)}
                              </span>
                            </div>
                            <div style={{ overflowX: 'auto' }}>
                              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
                                <thead>
                                  <tr style={{ background: '#1c1c1c', color: '#888', borderBottom: '1px solid #333' }}>
                                    <th style={{ padding: '6px 8px', textAlign: 'center', width: '35px' }}>Slot</th>
                                    <th style={{ padding: '6px 10px', textAlign: 'left' }}>Player</th>
                                    <th style={{ padding: '6px 8px', textAlign: 'center' }}>Pos</th>
                                    <th style={{ padding: '6px 8px', textAlign: 'center' }}>MLB</th>
                                    <th style={{ padding: '6px 8px', textAlign: 'center' }}>Rank</th>
                                    <th style={{ padding: '6px 8px', textAlign: 'center' }}>Cost</th>
                                    <th style={{ padding: '6px 8px', textAlign: 'center' }}>Token</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>R</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>HR</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>RBI</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>SB</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>OBP</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>K</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>QS</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>SV+HD</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>ERA</th>
                                    <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>WHIP</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {t.keepers.map(k => (
                                    <tr key={k.player_name || k.keeper_slot} style={{ borderBottom: '1px solid #222' }}>
                                      <td style={{ padding: '6px 8px', textAlign: 'center', color: '#888' }}>#{k.keeper_slot}</td>
                                      <td style={{ padding: '6px 10px', fontWeight: 'bold', color: '#fff' }}>{k.player_name}</td>
                                      <td style={{ padding: '6px 8px', textAlign: 'center', color: '#aaa' }}>{k.position || '-'}</td>
                                      <td style={{ padding: '6px 8px', textAlign: 'center', color: '#aaa' }}>{k.mlb_team || '-'}</td>
                                      <td style={{ padding: '6px 8px', textAlign: 'center', color: '#888' }}>#{k.rank}</td>
                                      <td style={{ padding: '6px 8px', textAlign: 'center', fontWeight: 'bold', color: '#ffb74d' }}>${k.cost}</td>
                                      <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                                        {k.token_applied ? (
                                          <span style={{ background: 'rgba(16, 185, 129, 0.2)', color: '#10b981', padding: '1px 6px', borderRadius: '4px', fontSize: '10px', fontWeight: 'bold' }}>
                                            🎫 Midpoint
                                          </span>
                                        ) : (
                                          <span style={{ color: '#555' }}>-</span>
                                        )}
                                      </td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.R > 0 ? k.stats.R : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.HR > 0 ? k.stats.HR : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.RBI > 0 ? k.stats.RBI : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.SB > 0 ? k.stats.SB : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.OBP > 0 ? formatRotoStat('OBP', k.stats.OBP) : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.K > 0 ? k.stats.K : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.QS > 0 ? k.stats.QS : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.SVHD > 0 ? k.stats.SVHD : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.ERA > 0 && k.stats.IP > 0 ? formatRotoStat('ERA', k.stats.ERA) : '-'}</td>
                                      <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.WHIP > 0 && k.stats.IP > 0 ? formatRotoStat('WHIP', k.stats.WHIP) : '-'}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* SECTION B: 9 OWNER KEEPER & BUDGET CARDS */}
      {(summaryViewMode === 'ledger' || summaryViewMode === 'both') && (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
          gap: '12px'
        }}>
          {sortedTeamBudgets.map(b => {
            const picks = compPicksByOwner[b.owner] || { bought: [], lost: [], sold: [] };
            const ownerKeepers = keepersByOwner[b.owner] || [];
            const tokenKeeper = ownerKeepers.find(k => k.token_applied);
            const isSelected = selectedOwner === b.owner;
            const actualFinish = getActualFinish(seasonYear, b.owner, b.finish_rank);

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
                    <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold' }}>Fin #{actualFinish}</span>
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

                {/* Token Status Badge for 2027 */}
                {seasonYear === 2027 && (
                  <div style={{ marginTop: '10px', paddingTop: '8px', borderTop: '1px solid #2a2a2a', fontSize: '11px' }}>
                    {tokenKeeper ? (
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ color: '#10b981', display: 'flex', alignItems: 'center', gap: '4px', fontWeight: 'bold' }}>
                          <span>🎫</span>
                          <span>Token: {tokenKeeper.player_name}</span>
                        </span>
                        <span style={{ color: '#03dac6', fontSize: '10px', fontWeight: 'bold' }}>
                          Midpoint: ${tokenKeeper.cost}
                        </span>
                      </div>
                    ) : (
                      <span style={{ color: '#888', fontStyle: 'italic', display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <span>🎫</span>
                        <span>Midpoint Token: Unused (1 Available)</span>
                      </span>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* SECTION C: COMPLETE BUDGET & PICK LEDGER TABLE */}
      {(summaryViewMode === 'ledger' || summaryViewMode === 'both') && (
        <div style={{
          background: '#181818',
          borderRadius: '8px',
          border: '1px solid #2a2a2a',
          overflow: 'hidden'
        }}>
          <div style={{ padding: '12px 16px', borderBottom: '1px solid #2a2a2a', fontWeight: 'bold', color: '#fff', fontSize: '13px' }}>
            📊 Complete {seasonYear} Budget & Compensation Pick Ledger
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
                {sortedTeamBudgets.map((b, idx) => {
                  const picks = compPicksByOwner[b.owner] || { bought: [], lost: [], sold: [] };
                  const actualFinish = getActualFinish(seasonYear, b.owner, b.finish_rank);

                  return (
                    <tr
                      key={b.owner}
                      style={{
                        borderBottom: '1px solid #262626',
                        background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)'
                      }}
                    >
                      <td style={{ padding: '10px 14px', color: '#fff', fontWeight: 'bold' }}>#{actualFinish}</td>
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
      )}
    </div>
  );
}
