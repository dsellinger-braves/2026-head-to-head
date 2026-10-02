// src/utils/draftRotoEngine.js
// Specialized analytics engine for Live In-Draft Roto Standings, Category Deficit HUD,
// Positional Run Detection, Smart Draft Recommendations, and Positional Tiers with Cliff Alerts.

import defaultCalculations from '../data/keeperCalculations.json';
import defaultKeepers2027 from '../data/keeperInput2027.json';
import defaultKeepers2026 from '../data/keeperInput2026.json';

export const DRAFT_MANAGERS = [
  'Tim', 'Daniel', 'Will', 'Adrian', 'Garrett', 'Alex', 'Mark', 'Preston', 'Anil'
];

export function normalizeManager(mgr) {
  if (!mgr) return '';
  const s = String(mgr).trim();
  if (s.toLowerCase() === 'dan') return 'Daniel';
  return s;
}

export const CATEGORIES = [
  { key: 'R', label: 'R', type: 'batting', lowerIsBetter: false },
  { key: 'HR', label: 'HR', type: 'batting', lowerIsBetter: false },
  { key: 'RBI', label: 'RBI', type: 'batting', lowerIsBetter: false },
  { key: 'SB', label: 'SB', type: 'batting', lowerIsBetter: false },
  { key: 'OBP', label: 'OBP', type: 'batting', lowerIsBetter: false },
  { key: 'K', label: 'K', type: 'pitching', lowerIsBetter: false },
  { key: 'QS', label: 'QS', type: 'pitching', lowerIsBetter: false },
  { key: 'SVHD', label: 'SV+HD', type: 'pitching', lowerIsBetter: false },
  { key: 'ERA', label: 'ERA', type: 'pitching', lowerIsBetter: true },
  { key: 'WHIP', label: 'WHIP', type: 'pitching', lowerIsBetter: true }
];

// Rich manager draft archetypes reflecting historical league tendencies
export const OWNER_ARCHETYPES = {
  Adrian: {
    archetype: { name: 'Five-Tool Upside', emoji: '🚀', tagline: 'Young high-ceiling bats, elite speed, and swing-and-miss stuff' },
    targetStats: ['SB', 'OBP', 'K'],
    boostPositions: ['OF', 'SS', 'RP'],
    fadePositions: ['1B'],
    pitcherBias: 0.35
  },
  Alex: {
    archetype: { name: 'Rotation Anchor', emoji: '🛡️', tagline: 'Durable quality start floor and clean ratio protection' },
    targetStats: ['QS', 'ERA', 'WHIP'],
    boostPositions: ['SP', '2B'],
    fadePositions: ['RP'],
    pitcherBias: 0.45
  },
  Anil: {
    archetype: { name: 'Contact & Inning Eaters', emoji: '🏏', tagline: 'Reliable at-bat volume, durable innings, and steady run producers' },
    targetStats: ['R', 'RBI', 'QS'],
    boostPositions: ['OF', 'SP', '3B'],
    fadePositions: ['RP'],
    pitcherBias: 0.25
  },
  Daniel: {
    archetype: { name: 'Balanced Architect', emoji: '👑', tagline: 'Five-tool synergy, speed anchors, and strikeout efficiency' },
    targetStats: ['SB', 'OBP', 'K', 'QS'],
    boostPositions: ['OF', 'SS', 'SP'],
    fadePositions: [],
    pitcherBias: 0.35
  },
  Dan: {
    archetype: { name: 'Balanced Architect', emoji: '👑', tagline: 'Five-tool synergy, speed anchors, and strikeout efficiency' },
    targetStats: ['SB', 'OBP', 'K', 'QS'],
    boostPositions: ['OF', 'SS', 'SP'],
    fadePositions: [],
    pitcherBias: 0.35
  },
  Garrett: {
    archetype: { name: 'Value Opportunist', emoji: '🎲', tagline: 'Punishes ADP slides, stacks middle-order power hitters' },
    targetStats: ['RBI', 'HR'],
    boostPositions: ['OF', '1B', 'SP'],
    fadePositions: ['C'],
    pitcherBias: 0.35
  },
  Mark: {
    archetype: { name: 'Strikeout & Arms Czar', emoji: '⚡', tagline: 'Elite K/9 anchors, rotation aces, and high-leverage closers' },
    targetStats: ['K', 'QS', 'SVHD'],
    boostPositions: ['SP', 'RP'],
    fadePositions: ['C', '2B'],
    pitcherBias: 0.55
  },
  Preston: {
    archetype: { name: 'Bullpen & Power Arms', emoji: '🔥', tagline: 'Triple-digit heat, strikeout upside, and explosive relievers' },
    targetStats: ['K', 'SVHD', 'HR'],
    boostPositions: ['RP', 'SP'],
    fadePositions: ['C'],
    pitcherBias: 0.45
  },
  Tim: {
    archetype: { name: 'Slugger Maximalist', emoji: '💥', tagline: 'Heavy power, RBI anchors, and high OBP sluggers' },
    targetStats: ['HR', 'RBI', 'OBP'],
    boostPositions: ['1B', '3B', 'OF'],
    fadePositions: ['RP'],
    pitcherBias: 0.2
  },
  Will: {
    archetype: { name: 'Category Specialist', emoji: '🎯', tagline: 'Ratio aces, elite WHIP suppression, and premium steals' },
    targetStats: ['WHIP', 'ERA', 'SB'],
    boostPositions: ['SP', 'OF'],
    fadePositions: ['RP'],
    pitcherBias: 0.35
  }
};

// Build fast global player projection index from keeperCalculations
const calcIndexById = new Map();
const calcIndexByName = new Map();
(defaultCalculations?.players || []).forEach(p => {
  const pid = String(p.espn_player_id || p.player_id || '').trim();
  const name = (p.player_name || '').toLowerCase().replace(/\./g, '').replace(/'/g, '').trim();
  if (pid) calcIndexById.set(pid, p);
  if (name) calcIndexByName.set(name, p);
});

export function getPlayerProjectionStats(playerOrPick, seasonYear = 2027) {
  const pid = String(playerOrPick?.['ESPN PlayerID'] || playerOrPick?.espn_player_id || playerOrPick?.player_id || playerOrPick?.id || '').trim();
  const name = (playerOrPick?.Player || playerOrPick?.player_name || playerOrPick?.name || playerOrPick?.Selection || '').toLowerCase().replace(/\./g, '').replace(/'/g, '').trim();

  const calcMatch = (pid ? calcIndexById.get(pid) : null) || (name ? calcIndexByName.get(name) : null);
  const seasonStats = seasonYear >= 2027
    ? (calcMatch?.y2?.stats || calcMatch?.y1?.stats)
    : (calcMatch?.y1?.stats || calcMatch?.y2?.stats);

  const fallback = playerOrPick?.stats || {};
  const zipsFallback = {
    R: parseFloat(playerOrPick?.ZIPSR) || 0,
    HR: parseFloat(playerOrPick?.ZIPSHR) || 0,
    RBI: parseFloat(playerOrPick?.ZIPSRBI) || 0,
    SB: parseFloat(playerOrPick?.ZIPSSB) || 0,
    OBP: parseFloat(playerOrPick?.ZIPSOBP) || 0,
    SO: parseFloat(playerOrPick?.ZIPSK) || 0,
    QS: parseFloat(playerOrPick?.ZIPSQS) || 0,
    SVHD: parseFloat(playerOrPick?.['ZIPSSV+HDs']) || 0,
    ERA: parseFloat(playerOrPick?.ZIPSERA) || 0,
    WHIP: parseFloat(playerOrPick?.ZIPSWHIP) || 0,
    AB: parseFloat(playerOrPick?.ZIPSAB) || 500,
    IP: parseFloat(playerOrPick?.ZIPSIP) || 120
  };

  return {
    ...zipsFallback,
    ...fallback,
    ...(seasonStats || {})
  };
}

export function getRotoBadgeStyle(pts) {
  if (pts >= 8.0) return { bg: 'rgba(16, 185, 129, 0.2)', text: '#10b981', border: 'rgba(16, 185, 129, 0.45)', label: 'Elite' };
  if (pts >= 6.0) return { bg: 'rgba(3, 218, 198, 0.18)', text: '#03dac6', border: 'rgba(3, 218, 198, 0.4)', label: 'Strong' };
  if (pts >= 4.0) return { bg: 'rgba(251, 191, 36, 0.18)', text: '#fbbf24', border: 'rgba(251, 191, 36, 0.4)', label: 'Avg' };
  if (pts >= 2.5) return { bg: 'rgba(249, 115, 22, 0.18)', text: '#f97316', border: 'rgba(249, 115, 22, 0.4)', label: 'Low' };
  return { bg: 'rgba(244, 63, 94, 0.18)', text: '#f43f5e', border: 'rgba(244, 63, 94, 0.4)', label: 'Deficit' };
}

export function formatRotoStat(key, val) {
  if (val === undefined || val === null || isNaN(val)) return '-';
  if (key === 'OBP') return (val < 1 ? val.toFixed(3).replace(/^0/, '') : val.toFixed(3));
  if (key === 'ERA' || key === 'WHIP') return val >= 90 ? 'N/A' : val.toFixed(2);
  if (key === 'QS') return val.toFixed(1);
  return String(Math.round(val));
}

// 1. COMPUTE LIVE IN-DRAFT ROTO STANDINGS
export function computeLiveDraftRoto({ allPicks = [], keepers = [], seasonYear = 2027 }) {
  const fallbackKeepers = seasonYear >= 2027 ? (defaultKeepers2027?.keepers || []) : (defaultKeepers2026?.keepers || []);
  const activeKeepers = keepers?.length > 0 ? keepers : fallbackKeepers;

  // Group keepers by manager
  const keepersByOwner = {};
  DRAFT_MANAGERS.forEach(mgr => { keepersByOwner[mgr] = []; });
  activeKeepers.forEach(k => {
    const owner = normalizeManager(k.owner || k.fantasy_owner);
    if (keepersByOwner[owner]) keepersByOwner[owner].push(k);
  });

  // Group picks by manager
  const picksByOwner = {};
  DRAFT_MANAGERS.forEach(mgr => { picksByOwner[mgr] = []; });
  allPicks.forEach(p => {
    const owner = normalizeManager(p.Owner);
    const pid = String(p['ESPN PlayerID'] || p.espn_player_id || '').trim();
    if (picksByOwner[owner] && pid && pid !== 'null' && pid !== 'undefined') {
      picksByOwner[owner].push(p);
    }
  });

  const teamData = DRAFT_MANAGERS.map(owner => {
    const ownerKeepers = keepersByOwner[owner] || [];
    const ownerPicks = picksByOwner[owner] || [];

    const statsAccum = {
      R: 0, HR: 0, RBI: 0, SB: 0, AB: 0, totOBPNum: 0,
      K: 0, QS: 0, SVHD: 0, IP: 0, totERANum: 0, totWHIPNum: 0
    };

    const addPlayerStats = (playerItem) => {
      const s = getPlayerProjectionStats(playerItem, seasonYear);
      const r = parseFloat(s.R) || 0;
      const hr = parseFloat(s.HR) || 0;
      const rbi = parseFloat(s.RBI) || 0;
      const sb = parseFloat(s.SB) || 0;
      const ab = parseFloat(s.AB) || 0;
      const obp = parseFloat(s.OBP) || 0;

      const k = parseFloat(s.SO !== undefined ? s.SO : (s.K !== undefined ? s.K : 0)) || 0;
      const qs = parseFloat(s.QS) || 0;
      const svhd = parseFloat(s.SVHD !== undefined ? s.SVHD : (s.SV_HD !== undefined ? s.SV_HD : 0)) || 0;
      const ip = parseFloat(s.IP) || 0;
      const era = parseFloat(s.ERA) || 0;
      const whip = parseFloat(s.WHIP) || 0;

      statsAccum.R += r;
      statsAccum.HR += hr;
      statsAccum.RBI += rbi;
      statsAccum.SB += sb;
      if (ab > 0 && obp > 0) {
        statsAccum.AB += ab;
        statsAccum.totOBPNum += ab * obp;
      }

      statsAccum.K += k;
      statsAccum.QS += qs;
      statsAccum.SVHD += svhd;
      if (ip > 0) {
        statsAccum.IP += ip;
        statsAccum.totERANum += era * ip;
        statsAccum.totWHIPNum += whip * ip;
      }
    };

    ownerKeepers.forEach(addPlayerStats);
    ownerPicks.forEach(addPlayerStats);

    const teamOBP = statsAccum.AB > 0 ? (statsAccum.totOBPNum / statsAccum.AB) : 0;
    const teamERA = statsAccum.IP > 0 ? (statsAccum.totERANum / statsAccum.IP) : 99.0;
    const teamWHIP = statsAccum.IP > 0 ? (statsAccum.totWHIPNum / statsAccum.IP) : 99.0;

    return {
      owner,
      keeperCount: ownerKeepers.length,
      pickCount: ownerPicks.length,
      totalPlayers: ownerKeepers.length + ownerPicks.length,
      rawStats: {
        R: Math.round(statsAccum.R),
        HR: Math.round(statsAccum.HR),
        RBI: Math.round(statsAccum.RBI),
        SB: Math.round(statsAccum.SB),
        OBP: teamOBP,
        K: Math.round(statsAccum.K),
        QS: Math.round(statsAccum.QS * 10) / 10,
        SVHD: Math.round(statsAccum.SVHD),
        ERA: teamERA,
        WHIP: teamWHIP,
        IP: Math.round(statsAccum.IP * 10) / 10,
        AB: Math.round(statsAccum.AB)
      },
      points: {
        R: 0, HR: 0, RBI: 0, SB: 0, OBP: 0,
        K: 0, QS: 0, SVHD: 0, ERA: 0, WHIP: 0
      },
      battingPoints: 0,
      pitchingPoints: 0,
      totalPoints: 0,
      deficits: [],
      surpluses: []
    };
  });

  // Rank in each category and allocate 1.0 to 9.0 points
  CATEGORIES.forEach(({ key, lowerIsBetter }) => {
    const sorted = [...teamData].sort((a, b) => {
      const valA = a.rawStats[key];
      const valB = b.rawStats[key];
      return lowerIsBetter ? valA - valB : valB - valA;
    });

    let i = 0;
    while (i < sorted.length) {
      let j = i;
      const val = sorted[i].rawStats[key];
      while (j < sorted.length && Math.abs(sorted[j].rawStats[key] - val) < 0.0001) {
        j++;
      }
      let sumPts = 0;
      for (let k = i; k < j; k++) {
        sumPts += (9.0 - k);
      }
      const avgPts = sumPts / (j - i);
      for (let k = i; k < j; k++) {
        sorted[k].points[key] = avgPts;
      }
      i = j;
    }
  });

  // Compute sub-totals, deficits, and surpluses
  teamData.forEach(t => {
    t.battingPoints = t.points.R + t.points.HR + t.points.RBI + t.points.SB + t.points.OBP;
    t.pitchingPoints = t.points.K + t.points.QS + t.points.SVHD + t.points.ERA + t.points.WHIP;
    t.totalPoints = t.battingPoints + t.pitchingPoints;

    const catPointsList = CATEGORIES.map(c => ({
      key: c.key,
      label: c.label,
      pts: t.points[c.key],
      val: t.rawStats[c.key]
    })).sort((a, b) => a.pts - b.pts);

    // Lowest 2 categories are primary deficits
    t.deficits = catPointsList.slice(0, 2);
    // Categories with 7.0+ points are surpluses
    t.surpluses = catPointsList.filter(c => c.pts >= 7.0).reverse();
  });

  // Overall sort by totalPoints descending
  teamData.sort((a, b) => b.totalPoints - a.totalPoints);
  teamData.forEach((t, idx) => {
    t.rank = idx + 1;
  });

  const byOwner = {};
  teamData.forEach(t => { byOwner[t.owner] = t; });

  return { teams: teamData, byOwner };
}

// 2. POSITIONAL RUN RADAR
export function detectPositionalRun(recentPicks = [], windowSize = 4) {
  if (!recentPicks || recentPicks.length < 3) return null;
  const sample = recentPicks.slice(-windowSize);

  const getPositionGroup = (pos = '') => {
    const p = String(pos).toUpperCase();
    if (p.includes('C')) return 'Catcher';
    if (p.includes('1B')) return 'First Base';
    if (p.includes('2B')) return 'Second Base';
    if (p.includes('3B')) return 'Third Base';
    if (p.includes('SS')) return 'Shortstop';
    if (p.includes('OF')) return 'Outfield';
    if (p.includes('SP')) return 'Starting Pitcher';
    if (p.includes('RP')) return 'Relief Pitcher';
    return null;
  };

  const groupCounts = {};
  const groupPlayers = {};

  sample.forEach(pick => {
    const grp = getPositionGroup(pick.Position || pick.position);
    if (grp) {
      groupCounts[grp] = (groupCounts[grp] || 0) + 1;
      if (!groupPlayers[grp]) groupPlayers[grp] = [];
      groupPlayers[grp].push(pick.Player || pick.Selection || pick.player_name || 'Player');
    }
  });

  for (const [grp, count] of Object.entries(groupCounts)) {
    if (count >= 3) {
      return {
        group: grp,
        count,
        window: sample.length,
        players: groupPlayers[grp] || [],
        urgency: count === sample.length ? 'CRITICAL' : 'HIGH'
      };
    }
  }

  return null;
}

// 3. SMART DRAFT ASSISTANT RECOMMENDATIONS
export function getSmartDraftRecommendations({
  availablePlayers = [],
  myRoster = [],
  userDeficits = [],
  currentPickNumber = 1
}) {
  if (!availablePlayers || availablePlayers.length === 0) return [];

  const slotLimits = { C: 1, '1B': 1, '2B': 1, '3B': 1, SS: 1, OF: 6, DH: 1, SP: 4, RP: 2 };
  const currentCounts = { C: 0, '1B': 0, '2B': 0, '3B': 0, SS: 0, OF: 0, DH: 0, SP: 0, RP: 0 };

  myRoster.forEach(p => {
    const pos = p.Position || p.position || '';
    Object.keys(slotLimits).forEach(slot => {
      if (pos.includes(slot)) currentCounts[slot] = (currentCounts[slot] || 0) + 1;
    });
  });

  const deficitKeys = new Set(userDeficits.map(d => d.key));

  const scored = availablePlayers.slice(0, 100).map(player => {
    const pos = player.Position || player.position || '';
    const st = getPlayerProjectionStats(player);

    const pr = parseFloat(player['Projected PR'] || player.overall_pr) || 0;
    const rank = parseFloat(player['Hefty Keeper Rank'] || player.rank) || 999;
    const adp = parseFloat(player.ADP) || rank;
    const surplus = Math.max(0, currentPickNumber - adp);

    let synergyScore = 0;
    const synergyTags = [];

    if (deficitKeys.has('SB') && st.SB >= 18) {
      synergyScore += 18;
      synergyTags.push(`⚡ Speed (+${Math.round(st.SB)} SB)`);
    }
    if (deficitKeys.has('SVHD') && (st.SVHD >= 15 || pos.includes('RP'))) {
      synergyScore += 22;
      synergyTags.push(`🛡️ Closer (+${Math.round(st.SVHD)} SV+H)`);
    }
    if (deficitKeys.has('HR') && st.HR >= 26) {
      synergyScore += 16;
      synergyTags.push(`💥 Power (+${Math.round(st.HR)} HR)`);
    }
    if (deficitKeys.has('K') && (st.SO >= 150 || (st.IP > 0 && (st.SO / st.IP) * 9 >= 9.5))) {
      synergyScore += 18;
      synergyTags.push(`🔥 Strikeouts (+${Math.round(st.SO || 0)} K)`);
    }
    if (deficitKeys.has('QS') && st.QS >= 12) {
      synergyScore += 15;
      synergyTags.push(`⭐ Quality Starts (+${Math.round(st.QS)} QS)`);
    }
    if (deficitKeys.has('OBP') && st.OBP >= 0.355) {
      synergyScore += 14;
      synergyTags.push(`👁️ OBP Anchor (${formatRotoStat('OBP', st.OBP)})`);
    }
    if (deficitKeys.has('ERA') && st.ERA > 0 && st.ERA <= 3.45 && st.IP >= 60) {
      synergyScore += 14;
      synergyTags.push(`🎯 Ratio Ace (${st.ERA.toFixed(2)} ERA)`);
    }
    if (deficitKeys.has('WHIP') && st.WHIP > 0 && st.WHIP <= 1.15 && st.IP >= 60) {
      synergyScore += 14;
      synergyTags.push(`🔒 WHIP Lock (${st.WHIP.toFixed(2)})`);
    }

    let needScore = 10;
    Object.entries(slotLimits).forEach(([slot, limit]) => {
      if (pos.includes(slot)) {
        const count = currentCounts[slot] || 0;
        if (count === 0) needScore = 30;
        else if (count < limit) needScore = 20;
        else needScore = 5;
      }
    });

    const totalScore = (pr * 12) + (synergyScore * 1.5) + (needScore * 1.2) + (surplus * 0.4);

    return {
      player,
      pr,
      rank,
      adp,
      surplus,
      synergyScore,
      synergyTags,
      needScore,
      totalScore
    };
  });

  scored.sort((a, b) => b.totalScore - a.totalScore);

  // Pick top 3 recommendations with distinct strategic profiles
  const topNeed = scored.find(s => s.synergyScore > 0) || scored[0];
  const bestValue = scored.find(s => s.player['ESPN PlayerID'] !== topNeed?.player['ESPN PlayerID'] && s.surplus >= 5) ||
                    scored.find(s => s.player['ESPN PlayerID'] !== topNeed?.player['ESPN PlayerID']) || scored[1];
  const stabilizer = scored.find(s => 
    s.player['ESPN PlayerID'] !== topNeed?.player['ESPN PlayerID'] && 
    s.player['ESPN PlayerID'] !== bestValue?.player['ESPN PlayerID'] &&
    s.needScore >= 20
  ) || scored[2];

  const results = [];
  if (topNeed) {
    results.push({
      badge: '⭐ TOP DEFICIT FIT',
      color: '#10b981',
      player: topNeed.player,
      tags: topNeed.synergyTags.length > 0 ? topNeed.synergyTags : ['Synergy Target'],
      rationale: topNeed.synergyTags.length > 0 
        ? `Directly targets your team's lowest roto category with high projected return.`
        : `High-impact starting option to solidify your category balance.`
    });
  }

  if (bestValue) {
    results.push({
      badge: '💎 BEST AVAILABLE VALUE',
      color: '#03dac6',
      player: bestValue.player,
      tags: bestValue.surplus > 5 ? [`+${bestValue.surplus} Pick Surplus`] : ['Consensus Value'],
      rationale: `Sliding past expected draft capital; elite consensus PR value at this draft slot.`
    });
  }

  if (stabilizer) {
    results.push({
      badge: '🛡️ ROSTER STABILIZER',
      color: '#bb86fc',
      player: stabilizer.player,
      tags: [`Position Need (${stabilizer.player.Position})`],
      rationale: `Locks down an unfilled starting lineup requirement before positional scarcity kicks in.`
    });
  }

  return results;
}

// 4. POSITIONAL TIERS & CLIFF ALERTS
const POSITION_TIER_CUTOFFS = {
  C: [2, 5, 10],
  '1B': [3, 8, 14],
  '2B': [3, 8, 14],
  '3B': [3, 8, 14],
  SS: [3, 8, 14],
  OF: [8, 20, 36],
  SP: [6, 16, 30],
  RP: [3, 8, 15]
};

export function getPositionalTiers({ availablePlayers = [], position = 'SP' }) {
  const posUpper = position.toUpperCase();
  const pool = availablePlayers.filter(p => (p.Position || '').includes(posUpper));

  const cutoffs = POSITION_TIER_CUTOFFS[posUpper] || [3, 8, 15];

  const tiers = {
    1: { name: 'Tier 1: Franchise Anchors', players: [], color: '#10b981' },
    2: { name: 'Tier 2: All-Star Starters', players: [], color: '#03dac6' },
    3: { name: 'Tier 3: Everyday Contributors', players: [], color: '#fbbf24' },
    4: { name: 'Tier 4: Late-Round Upside & Depth', players: [], color: '#888' }
  };

  pool.forEach((p, idx) => {
    if (idx < cutoffs[0]) tiers[1].players.push(p);
    else if (idx < cutoffs[1]) tiers[2].players.push(p);
    else if (idx < cutoffs[2]) tiers[3].players.push(p);
    else tiers[4].players.push(p);
  });

  return tiers;
}
