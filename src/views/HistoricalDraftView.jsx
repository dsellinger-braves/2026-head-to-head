import React, { useState, useMemo } from 'react';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../utils/headshotUtils';

const DRAFT_OWNERS = ['Adrian', 'Alex', 'Anil', 'Daniel', 'Garrett', 'Mark', 'Preston', 'Tim', 'Will'];

const OWNER_COLORS = {
  Adrian: '#4fc3f7',
  Alex: '#ffb74d',
  Anil: '#ba68c8',
  Daniel: '#81c784',
  Garrett: '#f06292',
  Mark: '#fff176',
  Preston: '#ff8a65',
  Tim: '#4db6ac',
  Will: '#aed581'
};

export default function HistoricalDraftView({
  allPicks = [],
  players = [],
  onSeasonChange,
  onOpenPlayerModal,
  onSwitchView,
  analysisHistory = []
}) {
  const [activeTab, setActiveTab] = useState('board'); // 'board' | 'log' | 'rosters' | 'standings' | 'pool' | 'analysis'
  const [roundFilter, setRoundFilter] = useState('ALL'); // 'ALL' | 'KEEPERS' | 'EARLY' | 'MID' | 'LATE'
  const [selectedOwner, setSelectedOwner] = useState(DRAFT_OWNERS[0]);
  const [logOwnerFilter, setLogOwnerFilter] = useState('');
  const [logPosFilter, setLogPosFilter] = useState('');
  const [logSearch, setLogSearch] = useState('');
  const [poolSearch, setPoolSearch] = useState('');
  const [poolPosFilter, setPoolPosFilter] = useState('');

  // 1. Build lookup map for fast player data retrieval
  const playerMap = useMemo(() => {
    const map = new Map();
    players.forEach(p => {
      const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
      if (pid) map.set(pid, p);
    });
    return map;
  }, [players]);

  // 2. Completed picks with player objects
  const completedPicks = useMemo(() => {
    return allPicks
      .filter(p => p['ESPN PlayerID'])
      .map(pick => {
        const player = playerMap.get(String(pick['ESPN PlayerID']));
        return {
          ...pick,
          playerObj: player || null
        };
      });
  }, [allPicks, playerMap]);

  // 3. Draft Day Metrics
  const metrics = useMemo(() => {
    const totalPicks = completedPicks.length;
    const keepersCount = completedPicks.filter(p => p.Round <= 5).length;
    let battersCount = 0;
    let pitchersCount = 0;

    completedPicks.forEach(p => {
      const pos = p.playerObj?.Position || '';
      if (pos.includes('SP') || pos.includes('RP')) {
        pitchersCount++;
      } else {
        battersCount++;
      }
    });

    const tradedPicksCount = allPicks.filter(p => p['Pick Traded?'] === 'Y').length;

    return {
      totalPicks,
      keepersCount,
      battersCount,
      pitchersCount,
      tradedPicksCount
    };
  }, [completedPicks, allPicks]);

  // 4. Draft Day Team Rosters
  const rostersByOwner = useMemo(() => {
    const map = {};
    DRAFT_OWNERS.forEach(owner => {
      const ownerPicks = completedPicks
        .filter(p => p.Owner === owner)
        .sort((a, b) => a['Overall Pick'] - b['Overall Pick']);
      
      const batters = [];
      const pitchers = [];

      ownerPicks.forEach(pick => {
        const p = pick.playerObj;
        const pos = p?.Position || '';
        if (pos.includes('SP') || pos.includes('RP')) {
          pitchers.push(pick);
        } else {
          batters.push(pick);
        }
      });

      // Totals
      const allPlayers = ownerPicks.map(p => p.playerObj).filter(Boolean);
      const batterObjs = batters.map(p => p.playerObj).filter(Boolean);
      const pitcherObjs = pitchers.map(p => p.playerObj).filter(Boolean);

      const r = batterObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSR) || 0), 0);
      const hr = batterObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSHR) || 0), 0);
      const rbi = batterObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSRBI) || 0), 0);
      const sb = batterObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSSB) || 0), 0);
      const obp = batterObjs.length > 0 ? (batterObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSOBP) || 0), 0) / batterObjs.length) : 0;

      const k = pitcherObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSK) || 0), 0);
      const qs = pitcherObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSQS) || 0), 0);
      const era = pitcherObjs.length > 0 ? (pitcherObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSERA) || 0), 0) / pitcherObjs.length) : 0;
      const whip = pitcherObjs.length > 0 ? (pitcherObjs.reduce((sum, p) => sum + (parseFloat(p.ZIPSWHIP) || 0), 0) / pitcherObjs.length) : 0;
      const sv = pitcherObjs.reduce((sum, p) => sum + (parseFloat(p['ZIPSSV+HDs']) || 0), 0);

      map[owner] = {
        picks: ownerPicks,
        batters,
        pitchers,
        totals: { r, hr, rbi, sb, obp, k, qs, era, whip, sv, count: allPlayers.length }
      };
    });
    return map;
  }, [completedPicks]);

  // 5. Projected Roto Standings Leaderboard
  const standingsLeaderboard = useMemo(() => {
    const list = DRAFT_OWNERS.map(owner => {
      const data = rostersByOwner[owner]?.totals || { r: 0, hr: 0, rbi: 0, sb: 0, obp: 0, k: 0, qs: 0, era: 0, whip: 0, sv: 0, count: 0 };
      return {
        owner,
        ...data
      };
    });

    // Compute category ranks & roto points (9 down to 1)
    const assignPoints = (field, ascending = false) => {
      const sorted = [...list].sort((a, b) => ascending ? a[field] - b[field] : b[field] - a[field]);
      sorted.forEach((team, idx) => {
        const points = 9 - idx;
        const target = list.find(t => t.owner === team.owner);
        if (target) {
          target[`${field}_pts`] = points;
          target[`${field}_rank`] = idx + 1;
        }
      });
    };

    assignPoints('r');
    assignPoints('hr');
    assignPoints('rbi');
    assignPoints('sb');
    assignPoints('obp');
    assignPoints('k');
    assignPoints('qs');
    assignPoints('era', true); // lower is better
    assignPoints('whip', true); // lower is better
    assignPoints('sv');

    list.forEach(team => {
      team.totalPoints = (
        (team.r_pts || 0) +
        (team.hr_pts || 0) +
        (team.rbi_pts || 0) +
        (team.sb_pts || 0) +
        (team.obp_pts || 0) +
        (team.k_pts || 0) +
        (team.qs_pts || 0) +
        (team.era_pts || 0) +
        (team.whip_pts || 0) +
        (team.sv_pts || 0)
      );
    });

    return list.sort((a, b) => b.totalPoints - a.totalPoints);
  }, [rostersByOwner]);

  // 6. Filtered Draft Log
  const filteredLog = useMemo(() => {
    return completedPicks.filter(pick => {
      if (logOwnerFilter && pick.Owner !== logOwnerFilter) return false;
      const player = pick.playerObj;
      if (logPosFilter && (!player || !player.Position?.includes(logPosFilter))) return false;
      if (logSearch) {
        const q = logSearch.toLowerCase();
        const pName = (pick.Selection || player?.Player || '').toLowerCase();
        const team = (player?.Team || '').toLowerCase();
        if (!pName.includes(q) && !team.includes(q)) return false;
      }
      return true;
    });
  }, [completedPicks, logOwnerFilter, logPosFilter, logSearch]);

  // 7. Player Pool Reference with Draft Outcome
  const poolWithDraftStatus = useMemo(() => {
    const draftPickMap = new Map();
    completedPicks.forEach(p => {
      if (p['ESPN PlayerID']) {
        draftPickMap.set(String(p['ESPN PlayerID']), p);
      }
    });

    let list = players.map(p => {
      const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
      const pick = draftPickMap.get(pid);
      return {
        ...p,
        draftPick: pick || null
      };
    });

    if (poolPosFilter) {
      list = list.filter(p => p.Position?.includes(poolPosFilter));
    }

    if (poolSearch) {
      const q = poolSearch.toLowerCase();
      list = list.filter(p => (p.Player || '').toLowerCase().includes(q) || (p.Team || '').toLowerCase().includes(q));
    }

    // Default sort: drafted first (by overall pick), then by ADP/Rank
    list.sort((a, b) => {
      if (a.draftPick && b.draftPick) {
        return a.draftPick['Overall Pick'] - b.draftPick['Overall Pick'];
      }
      if (a.draftPick) return -1;
      if (b.draftPick) return 1;
      const rankA = parseFloat(a['Hefty Single Season Rank'] || a.ADP || 9999);
      const rankB = parseFloat(b['Hefty Single Season Rank'] || b.ADP || 9999);
      return rankA - rankB;
    });

    return list;
  }, [players, completedPicks, poolPosFilter, poolSearch]);

  return (
    <div style={{
      minHeight: '100vh',
      backgroundColor: '#0d0e15',
      color: '#e2e8f0',
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
      paddingBottom: '60px'
    }}>
      {/* Archival Top Banner */}
      <header style={{
        background: 'linear-gradient(180deg, #151824 0%, #0d0e15 100%)',
        borderBottom: '1px solid #2d3748',
        padding: '16px 24px',
        position: 'sticky',
        top: 0,
        zIndex: 100,
        boxShadow: '0 4px 20px rgba(0,0,0,0.5)'
      }}>
        <div style={{ maxWidth: '1440px', margin: '0 auto', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '16px' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '4px' }}>
              <span style={{
                background: 'rgba(3, 218, 198, 0.15)',
                color: '#03dac6',
                border: '1px solid rgba(3, 218, 198, 0.4)',
                fontSize: '11px',
                fontWeight: 'bold',
                padding: '3px 8px',
                borderRadius: '4px',
                letterSpacing: '1px',
                textTransform: 'uppercase'
              }}>
                🏛️ Historical Retrospective
              </span>
              <span style={{ color: '#718096', fontSize: '13px' }}>•</span>
              <span style={{ color: '#a0aec0', fontSize: '13px', fontWeight: '500' }}>
                Completed March 2026 • 32 Rounds
              </span>
            </div>
            <h1 style={{ margin: 0, fontSize: '24px', fontWeight: '800', color: '#fff', letterSpacing: '-0.5px' }}>
              2026 Draft Day Snapshot
            </h1>
          </div>

          {/* Controls: Season Toggle & Navigation */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
            {/* Season Switcher */}
            <div style={{
              display: 'flex',
              background: '#1a202c',
              borderRadius: '8px',
              padding: '3px',
              border: '1px solid #2d3748'
            }}>
              <button
                style={{
                  background: '#03dac6',
                  color: '#000',
                  border: 'none',
                  borderRadius: '6px',
                  padding: '6px 14px',
                  fontSize: '12px',
                  fontWeight: 'bold',
                  cursor: 'default'
                }}
              >
                🏛️ 2026 Archive
              </button>
              <button
                onClick={() => onSeasonChange && onSeasonChange(2027)}
                style={{
                  background: 'transparent',
                  color: '#cbd5e0',
                  border: 'none',
                  borderRadius: '6px',
                  padding: '6px 14px',
                  fontSize: '12px',
                  fontWeight: 'bold',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease'
                }}
                onMouseEnter={e => { e.currentTarget.style.background = 'rgba(187, 134, 252, 0.2)'; e.currentTarget.style.color = '#bb86fc'; }}
                onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = '#cbd5e0'; }}
              >
                🚀 Switch to 2027 Draft Prep
              </button>
            </div>

            {/* Nav Quicklinks */}
            {onSwitchView && (
              <div style={{ display: 'flex', gap: '8px' }}>
                <button
                  onClick={() => onSwitchView('capital')}
                  style={{
                    background: '#2e1065',
                    color: '#d8b4fe',
                    border: '1px solid #7e22ce',
                    padding: '6px 12px',
                    borderRadius: '6px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px'
                  }}
                >
                  <span>🎟️</span>
                  <span>Draft Capital</span>
                </button>
                <button
                  onClick={() => onSwitchView('keepers')}
                  style={{
                    background: '#064e3b',
                    color: '#6ee7b7',
                    border: '1px solid #059669',
                    padding: '6px 12px',
                    borderRadius: '6px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px'
                  }}
                >
                  <span>💎</span>
                  <span>Keepers</span>
                </button>
                <button
                  onClick={() => onSwitchView('weekly')}
                  style={{
                    background: '#1e3a8a',
                    color: '#93c5fd',
                    border: '1px solid #3b82f6',
                    padding: '6px 12px',
                    borderRadius: '6px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px'
                  }}
                >
                  <span>⚾</span>
                  <span>League Site</span>
                </button>
              </div>
            )}
          </div>
        </div>
      </header>

      <main style={{ maxWidth: '1440px', margin: '0 auto', padding: '24px 20px' }}>
        {/* Metrics Strip */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))',
          gap: '14px',
          marginBottom: '28px'
        }}>
          <div style={{
            background: '#151824',
            border: '1px solid #232936',
            borderRadius: '12px',
            padding: '16px 20px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.25)'
          }}>
            <div style={{ fontSize: '11px', color: '#a0aec0', textTransform: 'uppercase', letterSpacing: '1px', fontWeight: 'bold' }}>
              Total Draft Picks
            </div>
            <div style={{ fontSize: '28px', fontWeight: '900', color: '#03dac6', marginTop: '4px' }}>
              {metrics.totalPicks} <span style={{ fontSize: '14px', color: '#718096', fontWeight: 'normal' }}>/ 243</span>
            </div>
            <div style={{ fontSize: '12px', color: '#718096', marginTop: '2px' }}>
              32 Complete Rounds
            </div>
          </div>

          <div style={{
            background: '#151824',
            border: '1px solid #232936',
            borderRadius: '12px',
            padding: '16px 20px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.25)'
          }}>
            <div style={{ fontSize: '11px', color: '#a0aec0', textTransform: 'uppercase', letterSpacing: '1px', fontWeight: 'bold' }}>
              Keepers Locked
            </div>
            <div style={{ fontSize: '28px', fontWeight: '900', color: '#ffb74d', marginTop: '4px' }}>
              {metrics.keepersCount}
            </div>
            <div style={{ fontSize: '12px', color: '#718096', marginTop: '2px' }}>
              5 Keepers × 9 Franchises
            </div>
          </div>

          <div style={{
            background: '#151824',
            border: '1px solid #232936',
            borderRadius: '12px',
            padding: '16px 20px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.25)'
          }}>
            <div style={{ fontSize: '11px', color: '#a0aec0', textTransform: 'uppercase', letterSpacing: '1px', fontWeight: 'bold' }}>
              Hitters Drafted
            </div>
            <div style={{ fontSize: '28px', fontWeight: '900', color: '#4fc3f7', marginTop: '4px' }}>
              {metrics.battersCount}
            </div>
            <div style={{ fontSize: '12px', color: '#718096', marginTop: '2px' }}>
              {Math.round((metrics.battersCount / (metrics.totalPicks || 1)) * 100)}% of total pool
            </div>
          </div>

          <div style={{
            background: '#151824',
            border: '1px solid #232936',
            borderRadius: '12px',
            padding: '16px 20px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.25)'
          }}>
            <div style={{ fontSize: '11px', color: '#a0aec0', textTransform: 'uppercase', letterSpacing: '1px', fontWeight: 'bold' }}>
              Pitchers Drafted
            </div>
            <div style={{ fontSize: '28px', fontWeight: '900', color: '#ba68c8', marginTop: '4px' }}>
              {metrics.pitchersCount}
            </div>
            <div style={{ fontSize: '12px', color: '#718096', marginTop: '2px' }}>
              {Math.round((metrics.pitchersCount / (metrics.totalPicks || 1)) * 100)}% of total pool
            </div>
          </div>

          <div style={{
            background: '#151824',
            border: '1px solid #232936',
            borderRadius: '12px',
            padding: '16px 20px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.25)'
          }}>
            <div style={{ fontSize: '11px', color: '#a0aec0', textTransform: 'uppercase', letterSpacing: '1px', fontWeight: 'bold' }}>
              Draft Day Proj #1
            </div>
            <div style={{ fontSize: '24px', fontWeight: '900', color: '#aed581', marginTop: '4px' }}>
              {standingsLeaderboard[0]?.owner || 'Tim'}
            </div>
            <div style={{ fontSize: '12px', color: '#718096', marginTop: '2px' }}>
              {standingsLeaderboard[0]?.totalPoints || 0} Draft Day Points
            </div>
          </div>
        </div>

        {/* Tab Navigation */}
        <div style={{
          display: 'flex',
          gap: '8px',
          borderBottom: '2px solid #232936',
          marginBottom: '24px',
          overflowX: 'auto',
          paddingBottom: '2px'
        }}>
          {[
            { id: 'board', label: '📋 Full Draft Board', desc: '32-Round Grid' },
            { id: 'log', label: '📜 Chronological Draft Log', desc: '243 Picks' },
            { id: 'rosters', label: '👥 Draft Day Rosters', desc: 'Team Rosters' },
            { id: 'standings', label: '📊 Projected Standings', desc: 'Roto Points' },
            { id: 'pool', label: '🔍 Draft Day Player Pool', desc: 'Pick Results' },
            { id: 'analysis', label: '🤖 AI Commentary Feed', desc: 'Pick Analyses' }
          ].map(tab => {
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                style={{
                  background: isActive ? '#1e2330' : 'transparent',
                  color: isActive ? '#03dac6' : '#a0aec0',
                  border: 'none',
                  borderBottom: isActive ? '3px solid #03dac6' : '3px solid transparent',
                  padding: '12px 18px',
                  borderRadius: '8px 8px 0 0',
                  fontSize: '14px',
                  fontWeight: isActive ? '700' : '500',
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  transition: 'all 0.15s ease'
                }}
              >
                {tab.label}
              </button>
            );
          })}
        </div>

        {/* ================================================================= */}
        {/* TAB 1: DRAFT BOARD (32 ROUNDS MATRIX) */}
        {/* ================================================================= */}
        {activeTab === 'board' && (
          <div>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '12px',
              marginBottom: '16px'
            }}>
              <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                <span style={{ fontSize: '13px', color: '#a0aec0', marginRight: '6px', fontWeight: 'bold' }}>Rounds:</span>
                {[
                  { id: 'ALL', label: 'All (R1-32)' },
                  { id: 'KEEPERS', label: '💎 Keepers (R1-5)' },
                  { id: 'EARLY', label: 'Early (R6-14)' },
                  { id: 'MID', label: 'Mid (R15-23)' },
                  { id: 'LATE', label: 'Late (R24-32)' }
                ].map(rf => (
                  <button
                    key={rf.id}
                    onClick={() => setRoundFilter(rf.id)}
                    style={{
                      background: roundFilter === rf.id ? '#03dac6' : '#1e2330',
                      color: roundFilter === rf.id ? '#000' : '#cbd5e0',
                      border: '1px solid #2d3748',
                      borderRadius: '6px',
                      padding: '5px 12px',
                      fontSize: '12px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    {rf.label}
                  </button>
                ))}
              </div>

              <div style={{ fontSize: '12px', color: '#a0aec0' }}>
                Click any player card to inspect complete stats, FanGraphs, and news profile.
              </div>
            </div>

            <div style={{
              background: '#151824',
              borderRadius: '12px',
              border: '1px solid #232936',
              overflowX: 'auto',
              boxShadow: '0 8px 30px rgba(0,0,0,0.4)'
            }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '1000px' }}>
                <thead>
                  <tr style={{ background: '#1a1f2c', borderBottom: '2px solid #2d3748' }}>
                    <th style={{ padding: '12px 14px', textAlign: 'left', color: '#a0aec0', fontSize: '12px', width: '90px' }}>
                      Round
                    </th>
                    {DRAFT_OWNERS.map(owner => (
                      <th
                        key={owner}
                        style={{
                          padding: '12px 8px',
                          textAlign: 'center',
                          color: OWNER_COLORS[owner] || '#fff',
                          fontSize: '13px',
                          fontWeight: '800'
                        }}
                      >
                        {owner}
                      </th>
                    ))}
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
                      return (
                        <tr
                          key={`round-${rNum}`}
                          style={{
                            borderBottom: '1px solid #1f2533',
                            background: isKeeperRound ? 'rgba(255, 183, 77, 0.03)' : 'transparent'
                          }}
                        >
                          <td style={{
                            padding: '10px 14px',
                            fontWeight: 'bold',
                            fontSize: '12px',
                            color: isKeeperRound ? '#ffb74d' : '#03dac6',
                            whiteSpace: 'nowrap'
                          }}>
                            {isKeeperRound ? `💎 R${rNum}` : `Round ${rNum}`}
                          </td>

                          {DRAFT_OWNERS.map(owner => {
                            const pick = completedPicks.find(p => p.Round === rNum && p.Owner === owner);
                            const player = pick?.playerObj;
                            const isTraded = pick && pick['Pick Traded?'] === 'Y';
                            const origOwner = pick?.['Original Owner'];

                            if (!pick) {
                              return (
                                <td key={owner} style={{ padding: '6px 4px', textAlign: 'center', color: '#4a5568', fontSize: '11px' }}>
                                  —
                                </td>
                              );
                            }

                            return (
                              <td key={owner} style={{ padding: '6px 4px', verticalAlign: 'middle' }}>
                                <div
                                  onClick={() => player && onOpenPlayerModal && onOpenPlayerModal(player['ESPN PlayerID'], player.Player)}
                                  style={{
                                    background: isKeeperRound ? 'rgba(255, 183, 77, 0.12)' : '#1a1f2c',
                                    border: isKeeperRound ? '1px solid rgba(255, 183, 77, 0.35)' : '1px solid #2d3748',
                                    borderRadius: '8px',
                                    padding: '6px 8px',
                                    cursor: player ? 'pointer' : 'default',
                                    transition: 'all 0.15s ease',
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '6px'
                                  }}
                                  onMouseEnter={e => {
                                    if (player) {
                                      e.currentTarget.style.borderColor = '#03dac6';
                                      e.currentTarget.style.transform = 'translateY(-1px)';
                                    }
                                  }}
                                  onMouseLeave={e => {
                                    if (player) {
                                      e.currentTarget.style.borderColor = isKeeperRound ? 'rgba(255, 183, 77, 0.35)' : '#2d3748';
                                      e.currentTarget.style.transform = 'translateY(0)';
                                    }
                                  }}
                                >
                                  {/* Headshot */}
                                  <div style={{
                                    width: '26px',
                                    height: '26px',
                                    borderRadius: '50%',
                                    overflow: 'hidden',
                                    background: '#111',
                                    flexShrink: 0,
                                    border: '1px solid #444'
                                  }}>
                                    <img
                                      src={getPlayerHeadshotUrl(player || { 'ESPN PlayerID': pick['ESPN PlayerID'] })}
                                      alt=""
                                      referrerPolicy="no-referrer"
                                      onError={(e) => handleHeadshotError(e, player)}
                                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                                      loading="lazy"
                                    />
                                  </div>

                                  <div style={{ minWidth: 0, flex: 1 }}>
                                    <div style={{
                                      fontSize: '11px',
                                      fontWeight: 'bold',
                                      color: '#fff',
                                      overflow: 'hidden',
                                      textOverflow: 'ellipsis',
                                      whiteSpace: 'nowrap'
                                    }}>
                                      {pick.Selection || player?.Player || 'Player'}
                                    </div>
                                    <div style={{ fontSize: '10px', color: '#a0aec0', display: 'flex', gap: '4px', alignItems: 'center' }}>
                                      <span>{player?.Position || '—'}</span>
                                      <span>•</span>
                                      <span>{player?.Team || '—'}</span>
                                      {isTraded && origOwner && origOwner !== owner && (
                                        <span style={{ color: '#ffb74d', fontSize: '9px', fontWeight: 'bold' }}>
                                          (via {origOwner})
                                        </span>
                                      )}
                                    </div>
                                  </div>
                                </div>
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

        {/* ================================================================= */}
        {/* TAB 2: CHRONOLOGICAL DRAFT LOG (243 PICKS) */}
        {/* ================================================================= */}
        {activeTab === 'log' && (
          <div>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '12px',
              marginBottom: '16px'
            }}>
              <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                <select
                  value={logOwnerFilter}
                  onChange={e => setLogOwnerFilter(e.target.value)}
                  style={{
                    background: '#1a202c',
                    color: '#fff',
                    border: '1px solid #2d3748',
                    borderRadius: '6px',
                    padding: '6px 12px',
                    fontSize: '13px'
                  }}
                >
                  <option value="">All 9 Owners</option>
                  {DRAFT_OWNERS.map(o => (
                    <option key={o} value={o}>{o}</option>
                  ))}
                </select>

                <select
                  value={logPosFilter}
                  onChange={e => setLogPosFilter(e.target.value)}
                  style={{
                    background: '#1a202c',
                    color: '#fff',
                    border: '1px solid #2d3748',
                    borderRadius: '6px',
                    padding: '6px 12px',
                    fontSize: '13px'
                  }}
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

                <input
                  type="text"
                  placeholder="Search player or MLB team..."
                  value={logSearch}
                  onChange={e => setLogSearch(e.target.value)}
                  style={{
                    background: '#1a202c',
                    color: '#fff',
                    border: '1px solid #2d3748',
                    borderRadius: '6px',
                    padding: '6px 12px',
                    fontSize: '13px',
                    minWidth: '220px'
                  }}
                />
              </div>

              <div style={{ fontSize: '13px', color: '#a0aec0' }}>
                Showing {filteredLog.length} of {completedPicks.length} draft day picks
              </div>
            </div>

            <div style={{
              background: '#151824',
              borderRadius: '12px',
              border: '1px solid #232936',
              overflowX: 'auto',
              boxShadow: '0 8px 30px rgba(0,0,0,0.4)'
            }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '1000px' }}>
                <thead>
                  <tr style={{ background: '#1a1f2c', borderBottom: '2px solid #2d3748' }}>
                    <th style={{ padding: '10px 12px', textAlign: 'center', color: '#03dac6', width: '60px' }}>Pick #</th>
                    <th style={{ padding: '10px 10px', textAlign: 'center', color: '#a0aec0', width: '60px' }}>Round</th>
                    <th style={{ padding: '10px 14px', textAlign: 'left', color: '#a0aec0', width: '110px' }}>Owner</th>
                    <th style={{ padding: '10px 14px', textAlign: 'left', color: '#fff', minWidth: '220px' }}>Player Selected</th>
                    <th style={{ padding: '10px 8px', textAlign: 'center', color: '#a0aec0', width: '60px' }}>Pos</th>
                    <th style={{ padding: '10px 8px', textAlign: 'center', color: '#a0aec0', width: '60px' }}>Team</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>R</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>HR</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>RBI</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>SB</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>OBP</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>K</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>QS</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>ERA</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>WHIP</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>SV+H</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredLog.map(pick => {
                    const player = pick.playerObj;
                    const isPitcher = player?.Position?.includes('SP') || player?.Position?.includes('RP');
                    const isKeeper = pick.Round <= 5;

                    return (
                      <tr
                        key={pick['Overall Pick']}
                        style={{
                          borderBottom: '1px solid #1f2533',
                          transition: 'background 0.1s ease'
                        }}
                        onMouseEnter={e => { e.currentTarget.style.background = '#1a1f2c'; }}
                        onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
                      >
                        <td style={{ padding: '8px 12px', textAlign: 'center', fontWeight: 'bold', color: '#03dac6', fontSize: '13px' }}>
                          #{pick['Overall Pick']}
                        </td>
                        <td style={{ padding: '8px 10px', textAlign: 'center', color: isKeeper ? '#ffb74d' : '#a0aec0', fontSize: '12px', fontWeight: isKeeper ? 'bold' : 'normal' }}>
                          {isKeeper ? `💎 R${pick.Round}` : `R${pick.Round}`}
                        </td>
                        <td style={{ padding: '8px 14px' }}>
                          <span style={{
                            color: OWNER_COLORS[pick.Owner] || '#fff',
                            fontWeight: 'bold',
                            fontSize: '13px'
                          }}>
                            {pick.Owner}
                          </span>
                        </td>
                        <td style={{ padding: '8px 14px' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <div style={{
                              width: '28px',
                              height: '28px',
                              borderRadius: '50%',
                              overflow: 'hidden',
                              background: '#111',
                              flexShrink: 0,
                              border: '1px solid #444'
                            }}>
                              <img
                                src={getPlayerHeadshotUrl(player || { 'ESPN PlayerID': pick['ESPN PlayerID'] })}
                                alt=""
                                referrerPolicy="no-referrer"
                                onError={(e) => handleHeadshotError(e, player)}
                                style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                                loading="lazy"
                              />
                            </div>
                            <button
                              onClick={() => player && onOpenPlayerModal && onOpenPlayerModal(player['ESPN PlayerID'], player.Player)}
                              style={{
                                background: 'transparent',
                                border: 'none',
                                color: '#fff',
                                fontWeight: 'bold',
                                fontSize: '13px',
                                textAlign: 'left',
                                cursor: 'pointer',
                                padding: 0
                              }}
                              onMouseEnter={e => { e.currentTarget.style.color = '#03dac6'; }}
                              onMouseLeave={e => { e.currentTarget.style.color = '#fff'; }}
                            >
                              {pick.Selection || player?.Player || 'Player'}
                            </button>
                          </div>
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'center', color: '#a0aec0', fontSize: '12px' }}>
                          {player?.Position || '—'}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'center', color: '#a0aec0', fontSize: '12px' }}>
                          {player?.Team || '—'}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSR || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSHR || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSRBI || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSSB || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSOBP || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSK || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSQS || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSERA || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSWHIP || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.['ZIPSSV+HDs'] || '—')}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ================================================================= */}
        {/* TAB 3: DRAFT DAY ROSTERS (BY TEAM) */}
        {/* ================================================================= */}
        {activeTab === 'rosters' && (
          <div>
            {/* Owner Selector Tabs */}
            <div style={{
              display: 'flex',
              gap: '8px',
              flexWrap: 'wrap',
              marginBottom: '20px'
            }}>
              {DRAFT_OWNERS.map(owner => {
                const isSelected = selectedOwner === owner;
                const rData = rostersByOwner[owner];
                return (
                  <button
                    key={owner}
                    onClick={() => setSelectedOwner(owner)}
                    style={{
                      background: isSelected ? '#1e2330' : '#151824',
                      color: isSelected ? (OWNER_COLORS[owner] || '#03dac6') : '#a0aec0',
                      border: isSelected ? `2px solid ${OWNER_COLORS[owner] || '#03dac6'}` : '1px solid #2d3748',
                      borderRadius: '8px',
                      padding: '8px 16px',
                      fontSize: '13px',
                      fontWeight: 'bold',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px'
                    }}
                  >
                    <span>{owner}</span>
                    <span style={{ fontSize: '11px', color: '#718096', fontWeight: 'normal' }}>
                      ({rData?.picks.length || 0})
                    </span>
                  </button>
                );
              })}
            </div>

            {/* Team Summary Bar */}
            {rostersByOwner[selectedOwner] && (
              <div style={{
                background: '#151824',
                borderRadius: '12px',
                border: '1px solid #232936',
                padding: '18px 24px',
                marginBottom: '20px'
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '12px', marginBottom: '14px' }}>
                  <h2 style={{ margin: 0, fontSize: '20px', fontWeight: '800', color: OWNER_COLORS[selectedOwner] || '#fff' }}>
                    {selectedOwner}'s 2026 Draft Day Roster
                  </h2>
                  <span style={{ color: '#a0aec0', fontSize: '13px' }}>
                    {rostersByOwner[selectedOwner].batters.length} Hitters • {rostersByOwner[selectedOwner].pitchers.length} Pitchers • 27 Total Picks
                  </span>
                </div>

                {/* Stat Aggregate Grid */}
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(100px, 1fr))',
                  gap: '10px',
                  background: '#0d0e15',
                  padding: '14px',
                  borderRadius: '8px',
                  border: '1px solid #1f2533'
                }}>
                  {[
                    { label: 'Proj R', val: Math.round(rostersByOwner[selectedOwner].totals.r) },
                    { label: 'Proj HR', val: Math.round(rostersByOwner[selectedOwner].totals.hr) },
                    { label: 'Proj RBI', val: Math.round(rostersByOwner[selectedOwner].totals.rbi) },
                    { label: 'Proj SB', val: Math.round(rostersByOwner[selectedOwner].totals.sb) },
                    { label: 'Proj OBP', val: rostersByOwner[selectedOwner].totals.obp.toFixed(3) },
                    { label: 'Proj K', val: Math.round(rostersByOwner[selectedOwner].totals.k) },
                    { label: 'Proj QS', val: Math.round(rostersByOwner[selectedOwner].totals.qs) },
                    { label: 'Proj ERA', val: rostersByOwner[selectedOwner].totals.era.toFixed(2) },
                    { label: 'Proj WHIP', val: rostersByOwner[selectedOwner].totals.whip.toFixed(3) },
                    { label: 'Proj SV+H', val: Math.round(rostersByOwner[selectedOwner].totals.sv) }
                  ].map(stat => (
                    <div key={stat.label} style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: '10px', color: '#718096', fontWeight: 'bold', textTransform: 'uppercase' }}>{stat.label}</div>
                      <div style={{ fontSize: '16px', fontWeight: '800', color: '#03dac6', marginTop: '2px' }}>{stat.val}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Roster Table */}
            <div style={{
              background: '#151824',
              borderRadius: '12px',
              border: '1px solid #232936',
              overflowX: 'auto',
              boxShadow: '0 8px 30px rgba(0,0,0,0.4)'
            }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '950px' }}>
                <thead>
                  <tr style={{ background: '#1a1f2c', borderBottom: '2px solid #2d3748' }}>
                    <th style={{ padding: '10px 12px', textAlign: 'center', color: '#03dac6', width: '70px' }}>Pick #</th>
                    <th style={{ padding: '10px 10px', textAlign: 'center', color: '#a0aec0', width: '70px' }}>Round</th>
                    <th style={{ padding: '10px 14px', textAlign: 'left', color: '#fff', minWidth: '220px' }}>Player</th>
                    <th style={{ padding: '10px 8px', textAlign: 'center', color: '#a0aec0', width: '70px' }}>Pos</th>
                    <th style={{ padding: '10px 8px', textAlign: 'center', color: '#a0aec0', width: '70px' }}>Team</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>R</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>HR</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>RBI</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>SB</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>OBP</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>K</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>QS</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>ERA</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>WHIP</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>SV+H</th>
                  </tr>
                </thead>
                <tbody>
                  {rostersByOwner[selectedOwner]?.picks.map(pick => {
                    const player = pick.playerObj;
                    const isPitcher = player?.Position?.includes('SP') || player?.Position?.includes('RP');
                    const isKeeper = pick.Round <= 5;

                    return (
                      <tr
                        key={pick['Overall Pick']}
                        style={{ borderBottom: '1px solid #1f2533' }}
                      >
                        <td style={{ padding: '8px 12px', textAlign: 'center', fontWeight: 'bold', color: '#03dac6', fontSize: '13px' }}>
                          #{pick['Overall Pick']}
                        </td>
                        <td style={{ padding: '8px 10px', textAlign: 'center', color: isKeeper ? '#ffb74d' : '#a0aec0', fontSize: '12px', fontWeight: isKeeper ? 'bold' : 'normal' }}>
                          {isKeeper ? `💎 R${pick.Round}` : `R${pick.Round}`}
                        </td>
                        <td style={{ padding: '8px 14px' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <div style={{
                              width: '28px',
                              height: '28px',
                              borderRadius: '50%',
                              overflow: 'hidden',
                              background: '#111',
                              flexShrink: 0,
                              border: '1px solid #444'
                            }}>
                              <img
                                src={getPlayerHeadshotUrl(player || { 'ESPN PlayerID': pick['ESPN PlayerID'] })}
                                alt=""
                                referrerPolicy="no-referrer"
                                onError={(e) => handleHeadshotError(e, player)}
                                style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                                loading="lazy"
                              />
                            </div>
                            <button
                              onClick={() => player && onOpenPlayerModal && onOpenPlayerModal(player['ESPN PlayerID'], player.Player)}
                              style={{
                                background: 'transparent',
                                border: 'none',
                                color: '#fff',
                                fontWeight: 'bold',
                                fontSize: '13px',
                                textAlign: 'left',
                                cursor: 'pointer',
                                padding: 0
                              }}
                              onMouseEnter={e => { e.currentTarget.style.color = '#03dac6'; }}
                              onMouseLeave={e => { e.currentTarget.style.color = '#fff'; }}
                            >
                              {pick.Selection || player?.Player || 'Player'}
                            </button>
                          </div>
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'center', color: '#a0aec0', fontSize: '12px' }}>
                          {player?.Position || '—'}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'center', color: '#a0aec0', fontSize: '12px' }}>
                          {player?.Team || '—'}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSR || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSHR || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSRBI || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSSB || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (player?.ZIPSOBP || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSK || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSQS || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSERA || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.ZIPSWHIP || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (player?.['ZIPSSV+HDs'] || '—')}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ================================================================= */}
        {/* TAB 4: DRAFT DAY PROJECTED STANDINGS */}
        {/* ================================================================= */}
        {activeTab === 'standings' && (
          <div>
            <div style={{ marginBottom: '16px', color: '#a0aec0', fontSize: '13px' }}>
              Projected rotisserie standings calculated directly from 2026 preseason ZiPS projections across all 27 draft day roster spots (9 pts awarded for 1st in category down to 1 pt for 9th).
            </div>

            <div style={{
              background: '#151824',
              borderRadius: '12px',
              border: '1px solid #232936',
              overflowX: 'auto',
              boxShadow: '0 8px 30px rgba(0,0,0,0.4)'
            }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '1100px' }}>
                <thead>
                  <tr style={{ background: '#1a1f2c', borderBottom: '2px solid #2d3748' }}>
                    <th style={{ padding: '12px 10px', textAlign: 'center', color: '#03dac6', width: '60px' }}>Rank</th>
                    <th style={{ padding: '12px 14px', textAlign: 'left', color: '#fff', width: '130px' }}>Franchise</th>
                    <th style={{ padding: '12px 10px', textAlign: 'center', color: '#ffb74d', width: '90px' }}>Total Pts</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>R</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>HR</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>RBI</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>SB</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>OBP</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>K</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>QS</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>ERA</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>WHIP</th>
                    <th style={{ padding: '12px 8px', textAlign: 'right', color: '#cbd5e0' }}>SV+H</th>
                  </tr>
                </thead>
                <tbody>
                  {standingsLeaderboard.map((team, idx) => {
                    const isPodium = idx < 3;
                    return (
                      <tr
                        key={team.owner}
                        style={{
                          borderBottom: '1px solid #1f2533',
                          background: idx === 0 ? 'rgba(3, 218, 198, 0.05)' : 'transparent'
                        }}
                      >
                        <td style={{ padding: '12px 10px', textAlign: 'center', fontWeight: '900', fontSize: '15px', color: isPodium ? '#03dac6' : '#a0aec0' }}>
                          {idx === 0 ? '🥇 1' : idx === 1 ? '🥈 2' : idx === 2 ? '🥉 3' : `${idx + 1}`}
                        </td>
                        <td style={{ padding: '12px 14px' }}>
                          <span style={{ fontWeight: '800', fontSize: '14px', color: OWNER_COLORS[team.owner] || '#fff' }}>
                            {team.owner}
                          </span>
                        </td>
                        <td style={{ padding: '12px 10px', textAlign: 'center', fontWeight: '900', fontSize: '16px', color: '#ffb74d' }}>
                          {team.totalPoints}
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.r)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.r_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.hr)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.hr_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.rbi)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.rbi_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.sb)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.sb_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{team.obp.toFixed(3)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.obp_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.k)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.k_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.qs)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.qs_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{team.era.toFixed(2)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.era_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{team.whip.toFixed(3)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.whip_pts}p)</span>
                        </td>
                        <td style={{ padding: '12px 8px', textAlign: 'right', fontSize: '13px' }}>
                          <span style={{ fontWeight: 'bold', color: '#fff' }}>{Math.round(team.sv)}</span>
                          <span style={{ fontSize: '11px', color: '#718096', marginLeft: '4px' }}>({team.sv_pts}p)</span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ================================================================= */}
        {/* TAB 5: DRAFT DAY PLAYER POOL REFERENCE (READ-ONLY) */}
        {/* ================================================================= */}
        {activeTab === 'pool' && (
          <div>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '12px',
              marginBottom: '16px'
            }}>
              <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                <select
                  value={poolPosFilter}
                  onChange={e => setPoolPosFilter(e.target.value)}
                  style={{
                    background: '#1a202c',
                    color: '#fff',
                    border: '1px solid #2d3748',
                    borderRadius: '6px',
                    padding: '6px 12px',
                    fontSize: '13px'
                  }}
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

                <input
                  type="text"
                  placeholder="Search player or team..."
                  value={poolSearch}
                  onChange={e => setPoolSearch(e.target.value)}
                  style={{
                    background: '#1a202c',
                    color: '#fff',
                    border: '1px solid #2d3748',
                    borderRadius: '6px',
                    padding: '6px 12px',
                    fontSize: '13px',
                    minWidth: '220px'
                  }}
                />
              </div>

              <div style={{ fontSize: '12px', color: '#a0aec0', fontStyle: 'italic' }}>
                * Archival player reference as of 2026 Draft Day. Read-only historical snapshot.
              </div>
            </div>

            <div style={{
              background: '#151824',
              borderRadius: '12px',
              border: '1px solid #232936',
              overflowX: 'auto',
              boxShadow: '0 8px 30px rgba(0,0,0,0.4)'
            }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '1050px' }}>
                <thead>
                  <tr style={{ background: '#1a1f2c', borderBottom: '2px solid #2d3748' }}>
                    <th style={{ padding: '10px 14px', textAlign: 'left', color: '#fff', minWidth: '220px' }}>Player</th>
                    <th style={{ padding: '10px 10px', textAlign: 'center', color: '#a0aec0', width: '60px' }}>Pos</th>
                    <th style={{ padding: '10px 10px', textAlign: 'center', color: '#a0aec0', width: '60px' }}>Team</th>
                    <th style={{ padding: '10px 14px', textAlign: 'left', color: '#03dac6', width: '180px' }}>Draft Day Status</th>
                    <th style={{ padding: '10px 10px', textAlign: 'center', color: '#ffb74d', width: '80px' }}>Hefty $</th>
                    <th style={{ padding: '10px 10px', textAlign: 'center', color: '#90caf9', width: '80px' }}>Rank</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>R</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>HR</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>RBI</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>SB</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>OBP</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>K</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>QS</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>ERA</th>
                    <th style={{ padding: '10px 8px', textAlign: 'right', color: '#cbd5e0' }}>WHIP</th>
                  </tr>
                </thead>
                <tbody>
                  {poolWithDraftStatus.slice(0, 150).map(p => {
                    const isPitcher = p.Position?.includes('SP') || p.Position?.includes('RP');
                    const pick = p.draftPick;
                    const price = p['Hefty Keeper Price'] ?? p['Hefty Single Season Price'];
                    const rank = p['Hefty Keeper Rank'] ?? p['Hefty Single Season Rank'];

                    return (
                      <tr
                        key={p['ESPN PlayerID'] || p.Player}
                        style={{ borderBottom: '1px solid #1f2533' }}
                      >
                        <td style={{ padding: '8px 14px' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <div style={{
                              width: '28px',
                              height: '28px',
                              borderRadius: '50%',
                              overflow: 'hidden',
                              background: '#111',
                              flexShrink: 0,
                              border: '1px solid #444'
                            }}>
                              <img
                                src={getPlayerHeadshotUrl(p)}
                                alt=""
                                referrerPolicy="no-referrer"
                                onError={(e) => handleHeadshotError(e, p)}
                                style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                                loading="lazy"
                              />
                            </div>
                            <button
                              onClick={() => onOpenPlayerModal && onOpenPlayerModal(p['ESPN PlayerID'], p.Player)}
                              style={{
                                background: 'transparent',
                                border: 'none',
                                color: '#fff',
                                fontWeight: 'bold',
                                fontSize: '13px',
                                textAlign: 'left',
                                cursor: 'pointer',
                                padding: 0
                              }}
                              onMouseEnter={e => { e.currentTarget.style.color = '#03dac6'; }}
                              onMouseLeave={e => { e.currentTarget.style.color = '#fff'; }}
                            >
                              {p.Player}
                            </button>
                          </div>
                        </td>
                        <td style={{ padding: '8px 10px', textAlign: 'center', color: '#a0aec0', fontSize: '12px' }}>
                          {p.Position}
                        </td>
                        <td style={{ padding: '8px 10px', textAlign: 'center', color: '#a0aec0', fontSize: '12px' }}>
                          {p.Team}
                        </td>
                        <td style={{ padding: '8px 14px' }}>
                          {pick ? (
                            <span style={{
                              background: 'rgba(3, 218, 198, 0.12)',
                              color: '#03dac6',
                              border: '1px solid rgba(3, 218, 198, 0.3)',
                              borderRadius: '4px',
                              padding: '2px 8px',
                              fontSize: '11px',
                              fontWeight: 'bold'
                            }}>
                              #{pick['Overall Pick']} by {pick.Owner} (R{pick.Round})
                            </span>
                          ) : (
                            <span style={{ color: '#718096', fontSize: '11px' }}>
                              Undrafted
                            </span>
                          )}
                        </td>
                        <td style={{ padding: '8px 10px', textAlign: 'center', fontWeight: 'bold', color: price ? '#ffb74d' : '#4a5568', fontSize: '12px' }}>
                          {price !== undefined && price !== null && price !== '' ? `$${price}` : '—'}
                        </td>
                        <td style={{ padding: '8px 10px', textAlign: 'center', color: rank ? '#90caf9' : '#4a5568', fontSize: '12px', fontWeight: 'bold' }}>
                          {rank ? `#${rank}` : '—'}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (p.ZIPSR || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (p.ZIPSHR || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (p.ZIPSRBI || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (p.ZIPSSB || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {isPitcher ? '—' : (p.ZIPSOBP || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (p.ZIPSK || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (p.ZIPSQS || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (p.ZIPSERA || '—')}
                        </td>
                        <td style={{ padding: '8px 8px', textAlign: 'right', color: !isPitcher ? '#4a5568' : '#cbd5e0', fontSize: '12px' }}>
                          {!isPitcher ? '—' : (p.ZIPSWHIP || '—')}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ================================================================= */}
        {/* TAB 6: AI COMMENTARY FEED */}
        {/* ================================================================= */}
        {activeTab === 'analysis' && (
          <div>
            <div style={{ marginBottom: '16px', color: '#a0aec0', fontSize: '13px' }}>
              Archive of Gemini AI analytical pick reactions and commentary recorded during the 2026 Draft.
            </div>

            {analysisHistory && analysisHistory.length > 0 ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
                {analysisHistory.slice().reverse().map(item => (
                  <div
                    key={item.pickNumber || item.timestamp}
                    style={{
                      background: '#151824',
                      border: '1px solid #232936',
                      borderRadius: '10px',
                      padding: '16px 20px',
                      boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{
                          background: 'rgba(3, 218, 198, 0.15)',
                          color: '#03dac6',
                          fontWeight: 'bold',
                          fontSize: '11px',
                          padding: '2px 8px',
                          borderRadius: '4px'
                        }}>
                          Pick #{item.pickNumber} (R{item.round})
                        </span>
                        <span style={{ color: OWNER_COLORS[item.owner] || '#fff', fontWeight: 'bold', fontSize: '13px' }}>
                          {item.owner}
                        </span>
                        <span style={{ color: '#718096' }}>selected</span>
                        <span style={{ color: '#fff', fontWeight: 'bold', fontSize: '13px' }}>
                          {item.playerName} ({item.position} • {item.team})
                        </span>
                      </div>
                      {item.timestamp && (
                        <span style={{ fontSize: '11px', color: '#718096' }}>{item.timestamp}</span>
                      )}
                    </div>
                    <div
                      style={{ color: '#cbd5e0', fontSize: '13px', lineHeight: '1.5' }}
                      dangerouslySetInnerHTML={{ __html: item.commentary }}
                    />
                  </div>
                ))}
              </div>
            ) : (
              <div style={{
                background: '#151824',
                borderRadius: '12px',
                border: '1px solid #232936',
                padding: '40px',
                textAlign: 'center',
                color: '#718096'
              }}>
                <div style={{ fontSize: '24px', marginBottom: '8px' }}>🤖</div>
                <div style={{ fontSize: '15px', color: '#a0aec0', fontWeight: 'bold' }}>
                  No AI commentary records saved for 2026 draft
                </div>
                <p style={{ fontSize: '12px', color: '#718096', maxWidth: '400px', margin: '8px auto 0' }}>
                  Live commentary is recorded in real time for ongoing drafts like 2027 and persists to the draft picks ledger.
                </p>
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
