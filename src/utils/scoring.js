// src/utils/scoring.js

export const LINEUP_SLOTS = {
  0: 'C', 1: '1B', 2: '2B', 3: '3B', 4: 'SS', 
  5: 'OF', 6: '2B/SS', 7: '1B/3B', 12: 'UTIL',
  13: 'P', 14: 'SP', 15: 'RP',
  16: 'Bench', 17: 'IL'
};

// 1. Define your scoring categories
export const SCORING_CATS = {
  // Hitting
  R: { label: 'Runs', type: 'high' },   // 'high' means higher is better
  HR: { label: 'HR', type: 'high' },
  RBI: { label: 'RBI', type: 'high' },
  SB: { label: 'SB', type: 'high' },
  OBP: { label: 'OBP', type: 'high', isRate: true }, // Rate stats need special math

  // Pitching
  K: { label: 'K', type: 'high' },
  QS: { label: 'Quality Starts', type: 'high' },
  'SV+HDs': { label: 'Save+Holds', type: 'high' },
  ERA: { label: 'ERA', type: 'low' },   // 'low' means lower is better
  WHIP: { label: 'WHIP', type: 'low', isRate: true }
};

export const CATEGORIES = Object.keys(SCORING_CATS).map(key => ({
  id: key,
  name: SCORING_CATS[key].label,
  higherIsBetter: SCORING_CATS[key].type === 'high'
}));

// 2. ESPN numeric stat ID → named key used throughout aggregateStats
const ESPN_STAT_IDS = {
  // Batting
  '16': 'PA',
  '5':  'HR',
  '20': 'R',
  '21': 'RBI',
  '23': 'SB',
  '17': 'OBP',   // direct daily OBP from ESPN
  // Pitching
  '34': 'IP_raw', // ESPN stores IP as outs (thirds); divide by 3 for real IP
  '63': 'QS',
  '45': 'ER',
  '37': 'BB_Allowed',
  '39': 'H_Allowed',
  '48': 'K',
  '57': 'SV',
  '60': 'HD',
};

// 3. Helper to aggregate stats

export const calculateTrioMatchupResult = (teamStats, teamIds) => {
  const points = {};
  teamIds.forEach(id => { points[id] = 0; });

  // Award 2/1/0 pts per category; tied positions share their points equally
  CATEGORIES.forEach(cat => {
    const vals = teamIds.map(id => ({
      id,
      val: parseFloat(teamStats[id]?.[cat.id] || 0)
    }));
    vals.sort((a, b) => cat.higherIsBetter ? b.val - a.val : a.val - b.val);

    const ptMap = [2, 1, 0];
    let i = 0;
    while (i < vals.length) {
      let j = i;
      while (j < vals.length && Math.abs(vals[j].val - vals[i].val) < 0.0001) j++;
      const avgPts = ptMap.slice(i, j).reduce((s, p) => s + p, 0) / (j - i);
      for (let k = i; k < j; k++) points[vals[k].id] += avgPts;
      i = j;
    }
  });

  // Rank: count how many teams have strictly more points (handles tied ranks)
  const results = {};
  teamIds.forEach(id => {
    const rank = teamIds.filter(other => points[other] > points[id] + 0.0001).length + 1;
    results[id] = { points: points[id], rank };
  });

  return results;
};

export function aggregateStats(dailyRecords) {
  const totals = {
    R: 0, HR: 0, RBI: 0, SB: 0, K: 0, QS: 0, 'SV+HDs': 0,
    ER: 0, IP: 0, BB_Allowed: 0, H_Allowed: 0,
    OBP_num: 0, PA: 0, GS: 0
  };

  dailyRecords.forEach(record => {
    if (record.lineup_slot_id === 16 || record.lineup_slot_id === 17) return;

    // Normalize ESPN numeric stat IDs → named keys
    const s = {};
    for (const [key, val] of Object.entries(record.stats || {})) {
      s[ESPN_STAT_IDS[key] ?? key] = val;
    }

    const pa  = parseFloat(s.PA)  || 0;
    const obp = parseFloat(s.OBP) || 0;
    
    // Determine Games Started strictly by ESPN Stat 33
    const espnStats = record.stats || {};
    const gs = parseFloat(espnStats['33']) > 0 ? parseFloat(espnStats['33']) : 0;

    totals.GS          += gs;
    totals.R           += parseFloat(s.R)  || 0;
    totals.HR          += parseFloat(s.HR) || 0;
    totals.RBI         += parseFloat(s.RBI) || 0;
    totals.SB          += parseFloat(s.SB)  || 0;
    totals.K           += parseFloat(s.K)   || 0;
    totals.QS          += parseFloat(s.QS)  || 0;
    totals['SV+HDs']   += (parseFloat(s.SV) || 0) + (parseFloat(s.HD) || 0);

    // OBP: accumulate PA-weighted so we can average correctly across days
    totals.OBP_num += obp * pa;
    totals.PA      += pa;

    totals.ER         += parseFloat(s.ER)         || 0;
    totals.IP         += (parseFloat(s.IP_raw ?? s.IP) || 0) / 3;
    totals.BB_Allowed += parseFloat(s.BB_Allowed)  || 0;
    totals.H_Allowed  += parseFloat(s.H_Allowed)   || 0;
  });

  const calculated = { ...totals };
  calculated.OBP  = totals.PA > 0 ? (totals.OBP_num / totals.PA).toFixed(3) : ".000";
  calculated.ERA  = totals.IP > 0 ? ((totals.ER * 9) / totals.IP).toFixed(2) : "0.00";
  calculated.WHIP = totals.IP > 0 ? ((totals.BB_Allowed + totals.H_Allowed) / totals.IP).toFixed(2) : "0.00";
  calculated.QS_PCT = totals.GS > 0 ? ((totals.QS / totals.GS) * 100).toFixed(1) : "0.0";
  
  // Unrounded values for tooltips and precise display
  calculated.OBP_raw  = totals.PA > 0 ? totals.OBP_num / totals.PA : 0;
  calculated.ERA_raw  = totals.IP > 0 ? (totals.ER * 9) / totals.IP : 0;
  calculated.WHIP_raw = totals.IP > 0 ? (totals.BB_Allowed + totals.H_Allowed) / totals.IP : 0;

  return calculated;
}

// 3. Determine the "Score" (e.g. 6-3-1)
export function calculateMatchupResult(homeStats, awayStats) {
  let homeScore = 0;
  let awayScore = 0;
  let ties = 0;

  Object.keys(SCORING_CATS).forEach(cat => {
    const config = SCORING_CATS[cat];
    const hVal = parseFloat(homeStats[cat]);
    const aVal = parseFloat(awayStats[cat]);

    if (hVal === aVal) {
      ties++;
    } else if (config.type === 'high') {
      hVal > aVal ? homeScore++ : awayScore++;
    } else {
      // For 'low' stats like ERA
      hVal < aVal ? homeScore++ : awayScore++;
    }
  });

  return { homeScore, awayScore, ties };
}

// 4. Calculate Roto Points across a league
export function calculateRotoPoints(teamStatsMap) {
  const teamIds = Object.keys(teamStatsMap);
  const rotoPoints = {};
  teamIds.forEach(id => { rotoPoints[id] = { total: 0 }; });

  const cats = Object.keys(SCORING_CATS);
  cats.forEach(cat => {
    const config = SCORING_CATS[cat];
    const vals = teamIds.map(id => ({
      id,
      val: parseFloat(teamStatsMap[id]?.[cat] || 0)
    }));
    
    // Sort: 1 pt for worst, N pts for best
    vals.sort((a, b) => config.type === 'high' ? a.val - b.val : b.val - a.val);

    // Assign points and handle ties
    let i = 0;
    while (i < vals.length) {
      let j = i;
      while (j < vals.length && Math.abs(vals[j].val - vals[i].val) < 0.0001) j++;
      
      let sum = 0;
      for (let k = i; k < j; k++) sum += (k + 1);
      const avgPts = sum / (j - i);
      
      for (let k = i; k < j; k++) {
        rotoPoints[vals[k].id][cat] = avgPts;
        rotoPoints[vals[k].id].total += avgPts;
      }
      i = j;
    }
  });

  return rotoPoints;
}

// 5. Value Calculators for Disparity Analysis
export function calculateBatterValue(stats) {
  const pa = parseFloat(stats.PA) || 0;
  if (pa === 0) return 0;
  const r = parseFloat(stats.R) || 0;
  const hr = parseFloat(stats.HR) || 0;
  const rbi = parseFloat(stats.RBI) || 0;
  const sb = parseFloat(stats.SB) || 0;
  const obp = parseFloat(stats.OBP) || 0;
  
  return ((r + (hr * 3) + rbi + sb) / pa) + obp;
}

export function calculatePitcherValue(stats) {
  const ip = parseFloat(stats.IP) || 0;
  if (ip === 0) return 0;
  const k = parseFloat(stats.K) || 0;
  const qs = parseFloat(stats.QS) || 0;
  const svhds = parseFloat(stats['SV+HDs']) || 0;
  const er = parseFloat(stats.ER) || 0;
  
  return ((ip * 3) - (er * 2) + k + (qs * 3) + (svhds * 3)) / ip;
}