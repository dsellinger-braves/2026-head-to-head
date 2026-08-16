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

// 2. Helper to aggregate stats
export function aggregateStats(dailyRecords) {
  const totals = { 
    R: 0, HR: 0, RBI: 0, SB: 0, K: 0, QS: 0, 'SV+HDs': 0, 
    // Intermediate vars for rate stats
    H: 0, AB: 0, ER: 0, IP: 0, BB_Allowed: 0, H_Allowed: 0 
  };

  dailyRecords.forEach(record => {
   if (record.lineup_slot_id === 16 || record.lineup_slot_id === 17) return;

    const s = record.stats || {};
    // Sum up counting stats
    totals.R += (s.R || 0);
    totals.HR += (s.HR || 0);
    totals.RBI += (s.RBI || 0);
    totals.SB += (s.SB || 0);
    totals.K += (s.K || 0);
    totals.QS += (s.QS || 0);
    totals['SV+HDs'] += (s['SV'] || 0) + (s['HD'] || 0);
    
    // Sum up components for rate stats
    totals.H += (s.H || 0);
    totals.AB += (s.AB || 0);
    totals.ER += (s.ER || 0);
    totals.IP += (s.IP || 0)/3; // Note: Ensure IP is decimal (3.1 -> 3.33) in your Python or handle here
    totals.BB_Allowed += (s.BB_Allowed || 0);
    totals.H_Allowed += (s.H_Allowed || 0);
  });

  // Calculate Rate Stats
  const calculated = { ...totals };
  calculated.OBP = totals.AB > 0 ? (totals.H / totals.AB).toFixed(3) : ".000";
  calculated.ERA = totals.IP > 0 ? ((totals.ER * 9) / totals.IP).toFixed(2) : "0.00";
  calculated.WHIP = totals.IP > 0 ? ((totals.BB_Allowed + totals.H_Allowed) / totals.IP).toFixed(2) : "0.00";

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