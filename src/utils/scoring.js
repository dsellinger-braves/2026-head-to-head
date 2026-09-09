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
  R: { label: 'Runs', name: 'Runs Scored', type: 'high' },
  HR: { label: 'HR', name: 'Home Runs', type: 'high' },
  RBI: { label: 'RBI', name: 'Runs Batted In', type: 'high' },
  SB: { label: 'SB', name: 'Stolen Bases', type: 'high' },
  OBP: { label: 'OBP', name: 'On-Base Percentage', type: 'high', isRate: true },

  // Pitching
  K: { label: 'K', name: 'Strikeouts', type: 'high' },
  QS: { label: 'Quality Starts', name: 'Quality Starts', type: 'high' },
  'SV+HDs': { label: 'Save+Holds', name: 'Saves + Holds', type: 'high' },
  ERA: { label: 'ERA', name: 'Earned Run Average', type: 'low' },
  WHIP: { label: 'WHIP', name: 'Walks + Hits per Inning Pitched', type: 'low', isRate: true }
};

export const CATEGORIES = Object.keys(SCORING_CATS).map(key => ({
  id: key,
  name: SCORING_CATS[key].name || SCORING_CATS[key].label,
  higherIsBetter: SCORING_CATS[key].type === 'high'
}));

// 1b. Secondary & Minutiae Stats metadata with full names for tooltips
export const MINUTIAE_STATS = {
  // Hitting
  PA: { label: 'PA', name: 'Plate Appearances', type: 'high' },
  AB: { label: 'AB', name: 'At Bats', type: 'high' },
  H: { label: 'H', name: 'Hits', type: 'high' },
  '2B': { label: '2B', name: 'Doubles', type: 'high' },
  '3B': { label: '3B', name: 'Triples', type: 'high' },
  BB: { label: 'BB', name: 'Base on Balls (Walks)', type: 'high' },
  SO: { label: 'SO', name: 'Batter Strikeouts', type: 'low' },
  HBP: { label: 'HBP', name: 'Hit By Pitch', type: 'high' },
  CS: { label: 'CS', name: 'Caught Stealing', type: 'low' },
  SB_PCT: { label: 'SB%', name: 'Stolen Base Success %', type: 'high', isRate: true },
  E: { label: 'E', name: 'Fielding Errors', type: 'low' },
  GDP: { label: 'GDP', name: 'Grounded into Double Play', type: 'low' },
  AVG: { label: 'AVG', name: 'Batting Average', type: 'high', isRate: true },
  SLG: { label: 'SLG', name: 'Slugging Percentage', type: 'high', isRate: true },
  OPS: { label: 'OPS', name: 'On-Base Plus Slugging', type: 'high', isRate: true },

  // Pitching
  IP: { label: 'IP', name: 'Innings Pitched', type: 'high' },
  QS_PCT: { label: 'QS%', name: 'Quality Start Percentage', type: 'high', isRate: true },
  W: { label: 'W', name: 'Pitching Wins', type: 'high' },
  L: { label: 'L', name: 'Pitching Losses', type: 'low' },
  R_Allowed: { label: 'RA', name: 'Total Runs Allowed', type: 'low' },
  ER: { label: 'ER', name: 'Earned Runs Allowed', type: 'low' },
  UER: { label: 'UER', name: 'Unearned Runs Allowed', type: 'low' },
  'UER/9': { label: 'UER/9', name: 'Unearned Runs per 9 Innings', type: 'low', isRate: true },
  UER_PCT: { label: 'UER%', name: 'Unearned Runs % (relative to Earned Runs)', type: 'low', isRate: true },
  HR_Allowed: { label: 'HRA', name: 'Home Runs Allowed', type: 'low' },
  SV: { label: 'SV', name: 'Saves', type: 'high' },
  HD: { label: 'HD', name: 'Holds', type: 'high' },
  BS: { label: 'BS', name: 'Blown Saves', type: 'low' },
  'K/9': { label: 'K/9', name: 'Strikeouts per 9 Innings', type: 'high', isRate: true },
  'BB/9': { label: 'BB/9', name: 'Walks Allowed per 9 Innings', type: 'low', isRate: true },
  'K/BB': { label: 'K/BB', name: 'Strikeout-to-Walk Ratio', type: 'high', isRate: true }
};

export const getStatMeta = (cat) => {
  return MINUTIAE_STATS[cat] || SCORING_CATS[cat] || { label: cat, name: cat, type: 'high' };
};

// 2. ESPN numeric stat ID → named key used throughout aggregateStats
const ESPN_STAT_IDS = {
  // Batting
  '0':  'AB',
  '1':  'H',
  '2':  'AVG_raw',
  '3':  '2B',
  '4':  '3B',
  '5':  'HR',
  '6':  'TB_raw',
  '8':  'TB',
  '10': 'BB',
  '12': 'HBP',
  '13': 'SO_raw',
  '16': 'PA',
  '17': 'OBP',
  '20': 'R',
  '21': 'RBI',
  '23': 'SB',
  '24': 'CS',
  '26': 'GDP',
  '27': 'SO',
  '72': 'E',

  // Pitching
  '32': 'W_app',
  '33': 'GS',
  '34': 'IP_raw',
  '35': 'TBF',
  '36': 'Pitches',
  '37': 'H_Allowed',
  '39': 'BB_Allowed',
  '44': 'R_Allowed',
  '45': 'ER',
  '46': 'HR_Allowed',
  '48': 'K',
  '50': 'WP',
  '53': 'W',
  '54': 'L',
  '57': 'SV',
  '58': 'BS',
  '60': 'HD',
  '63': 'QS',
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
    // Primary Categories
    R: 0, HR: 0, RBI: 0, SB: 0, K: 0, QS: 0, 'SV+HDs': 0,
    ER: 0, IP: 0, BB_Allowed: 0, H_Allowed: 0,
    OBP_num: 0, PA: 0, GS: 0,

    // Deep / Minutiae Hitting Stats
    AB: 0, H: 0, '2B': 0, '3B': 0, BB: 0, SO: 0, HBP: 0,
    CS: 0, E: 0, GDP: 0, TB: 0,

    // Deep / Minutiae Pitching Stats
    W: 0, L: 0, SV: 0, HD: 0, BS: 0, R_Allowed: 0, HR_Allowed: 0, WP: 0, UER: 0
  };

  dailyRecords.forEach(record => {
    if (record.lineup_slot_id === 16 || record.lineup_slot_id === 17) return;

    // Normalize ESPN numeric stat IDs → named keys
    const s = {};
    for (const [key, val] of Object.entries(record.stats || {})) {
      s[ESPN_STAT_IDS[key] ?? key] = val;
    }

    const slot = record.lineup_slot_id;
    const isPitcher = slot >= 13 && slot <= 15;
    const isBatter = slot >= 0 && slot <= 12;

    const pa  = parseFloat(s.PA)  || 0;
    const obp = parseFloat(s.OBP) || 0;
    
    // Determine Games Started strictly by ESPN Stat 33
    const espnStats = record.stats || {};
    const gs = parseFloat(espnStats['33']) > 0 ? parseFloat(espnStats['33']) : 0;

    totals.GS        += gs;
    totals.R         += parseFloat(s.R)  || 0;
    totals.HR        += parseFloat(s.HR) || 0;
    totals.RBI       += parseFloat(s.RBI) || 0;
    totals.SB        += parseFloat(s.SB)  || 0;
    totals.K         += parseFloat(s.K)   || 0;
    totals.QS        += parseFloat(s.QS)  || 0;
    totals['SV+HDs'] += (parseFloat(s.SV) || 0) + (parseFloat(s.HD) || 0);

    // OBP: accumulate PA-weighted so we can average correctly across days
    totals.OBP_num += obp * pa;
    totals.PA      += pa;

    const er       = parseFloat(s.ER) || 0;
    const ip       = (parseFloat(s.IP_raw ?? s.IP) || 0) / 3;
    const bbAll    = parseFloat(s.BB_Allowed) || 0;
    const hAll     = parseFloat(s.H_Allowed)  || 0;
    const rAll     = parseFloat(s.R_Allowed ?? espnStats['44']) || 0;

    totals.ER         += er;
    totals.IP         += ip;
    totals.BB_Allowed += bbAll;
    totals.H_Allowed  += hAll;

    if (isBatter) {
      const ab  = parseFloat(s.AB ?? espnStats['0']) || 0;
      const h   = parseFloat(s.H ?? espnStats['1']) || 0;
      const d2  = parseFloat(s['2B'] ?? espnStats['3']) || 0;
      const d3  = parseFloat(s['3B'] ?? espnStats['4']) || 0;
      const hr  = parseFloat(s.HR ?? espnStats['5']) || 0;
      const bb  = parseFloat(s.BB ?? espnStats['10']) || 0;
      const so  = parseFloat(s.SO ?? espnStats['27'] ?? espnStats['13']) || 0;
      const hbp = parseFloat(s.HBP ?? espnStats['12']) || 0;
      const cs  = parseFloat(s.CS ?? espnStats['24']) || 0;
      const e   = parseFloat(s.E ?? espnStats['72']) || 0;
      const gdp = parseFloat(s.GDP ?? espnStats['26'] ?? espnStats['14']) || 0;
      const singles = Math.max(0, h - (d2 + d3 + hr));
      const tb  = parseFloat(s.TB ?? espnStats['8']) || (singles + (d2 * 2) + (d3 * 3) + (hr * 4));

      totals.AB  += ab;
      totals.H   += h;
      totals['2B'] += d2;
      totals['3B'] += d3;
      totals.BB  += bb;
      totals.SO  += so;
      totals.HBP += hbp;
      totals.CS  += cs;
      totals.E   += e;
      totals.GDP += gdp;
      totals.TB  += tb;
    }

    if (isPitcher) {
      totals.W          += parseFloat(s.W ?? espnStats['53']) || 0;
      totals.L          += parseFloat(s.L ?? espnStats['54']) || 0;
      totals.SV         += parseFloat(s.SV ?? espnStats['57']) || 0;
      totals.HD         += parseFloat(s.HD ?? espnStats['60']) || 0;
      totals.BS         += parseFloat(s.BS ?? espnStats['58']) || 0;
      totals.R_Allowed  += rAll;
      totals.HR_Allowed += parseFloat(s.HR_Allowed ?? espnStats['46']) || 0;
      totals.WP         += parseFloat(s.WP ?? espnStats['50']) || 0;
      totals.UER        += s.UER !== undefined ? (parseFloat(s.UER) || 0) : Math.max(0, rAll - er);
    }
  });

  const calculated = { ...totals };
  calculated.OBP  = totals.PA > 0 ? (totals.OBP_num / totals.PA).toFixed(3) : ".000";
  calculated.ERA  = totals.IP > 0 ? ((totals.ER * 9) / totals.IP).toFixed(2) : "0.00";
  calculated.WHIP = totals.IP > 0 ? ((totals.BB_Allowed + totals.H_Allowed) / totals.IP).toFixed(2) : "0.00";
  calculated.QS_PCT = totals.GS > 0 ? ((totals.QS / totals.GS) * 100).toFixed(1) : "0.0";
  
  // Rate minutiae stats
  calculated.AVG    = totals.AB > 0 ? (totals.H / totals.AB).toFixed(3) : ".000";
  calculated.SLG    = totals.AB > 0 ? (totals.TB / totals.AB).toFixed(3) : ".000";
  const avgNum      = totals.AB > 0 ? totals.H / totals.AB : 0;
  const slgNum      = totals.AB > 0 ? totals.TB / totals.AB : 0;
  const obpNum      = totals.PA > 0 ? totals.OBP_num / totals.PA : 0;
  calculated.OPS    = (obpNum + slgNum).toFixed(3);
  calculated.SB_PCT = (totals.SB + totals.CS) > 0 ? ((totals.SB / (totals.SB + totals.CS)) * 100).toFixed(1) : "0.0";

  calculated['K/9']   = totals.IP > 0 ? ((totals.K * 9) / totals.IP).toFixed(2) : "0.00";
  calculated['BB/9']  = totals.IP > 0 ? ((totals.BB_Allowed * 9) / totals.IP).toFixed(2) : "0.00";
  calculated['K/BB']  = totals.BB_Allowed > 0 ? (totals.K / totals.BB_Allowed).toFixed(2) : totals.K.toFixed(2);

  // Unearned runs rate metrics
  calculated['UER/9']  = totals.IP > 0 ? ((totals.UER * 9) / totals.IP).toFixed(2) : "0.00";
  calculated.UER_PCT   = totals.ER > 0 ? ((totals.UER / totals.ER) * 100).toFixed(1) : "0.0";

  // Unrounded values for tooltips and precise display
  calculated.OBP_raw      = obpNum;
  calculated.ERA_raw      = totals.IP > 0 ? (totals.ER * 9) / totals.IP : 0;
  calculated.WHIP_raw     = totals.IP > 0 ? (totals.BB_Allowed + totals.H_Allowed) / totals.IP : 0;
  calculated.AVG_raw      = avgNum;
  calculated.SLG_raw      = slgNum;
  calculated.OPS_raw      = obpNum + slgNum;
  calculated['UER/9_raw'] = totals.IP > 0 ? (totals.UER * 9) / totals.IP : 0;
  calculated.UER_PCT_raw  = totals.ER > 0 ? (totals.UER / totals.ER) * 100 : 0;

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