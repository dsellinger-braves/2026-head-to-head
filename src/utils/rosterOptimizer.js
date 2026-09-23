// src/utils/rosterOptimizer.js
import { SCORING_CATS, calculateRotoPoints, aggregateStats } from './scoring';

/**
 * Standard ESPN Fantasy Baseball Lineup Slots:
 * 0: C (1)
 * 1: 1B (1)
 * 2: 2B (1)
 * 3: 3B (1)
 * 4: SS (1)
 * 6: 2B/SS (1) - Middle Infield
 * 7: 1B/3B (1) - Corner Infield
 * 19: IF (1)   - Any Infield
 * 5: OF (6)    - Outfield
 * 11: DH (1)   - Designated Hitter
 * 12: UTIL (1) - Any Batter
 *
 * Total Starting Batters = 15
 *
 * 14: SP (4)   - Starting Pitcher
 * 15: RP (2)   - Relief Pitcher
 * 13: P (6)    - Any Pitcher
 *
 * Total Starting Pitchers = 12
 *
 * Inactive slots:
 * 16: Bench
 * 17: IL
 */

export const BATTER_SLOT_DEFS = [
  { id: 'C', name: 'C', slotId: 0, eligible: (p) => p.positions.has('C') },
  { id: '1B', name: '1B', slotId: 1, eligible: (p) => p.positions.has('1B') },
  { id: '2B', name: '2B', slotId: 2, eligible: (p) => p.positions.has('2B') },
  { id: '3B', name: '3B', slotId: 3, eligible: (p) => p.positions.has('3B') },
  { id: 'SS', name: 'SS', slotId: 4, eligible: (p) => p.positions.has('SS') },
  { id: '2B/SS', name: '2B/SS', slotId: 6, eligible: (p) => p.positions.has('2B') || p.positions.has('SS') },
  { id: '1B/3B', name: '1B/3B', slotId: 7, eligible: (p) => p.positions.has('1B') || p.positions.has('3B') },
  { id: 'IF', name: 'IF', slotId: 19, eligible: (p) => p.positions.has('1B') || p.positions.has('2B') || p.positions.has('3B') || p.positions.has('SS') },
  { id: 'OF1', name: 'OF', slotId: 5, eligible: (p) => p.positions.has('OF') },
  { id: 'OF2', name: 'OF', slotId: 5, eligible: (p) => p.positions.has('OF') },
  { id: 'OF3', name: 'OF', slotId: 5, eligible: (p) => p.positions.has('OF') },
  { id: 'OF4', name: 'OF', slotId: 5, eligible: (p) => p.positions.has('OF') },
  { id: 'OF5', name: 'OF', slotId: 5, eligible: (p) => p.positions.has('OF') },
  { id: 'OF6', name: 'OF', slotId: 5, eligible: (p) => p.positions.has('OF') },
  { id: 'DH', name: 'DH', slotId: 11, eligible: (p) => p.isBatter },
  { id: 'UTIL', name: 'UTIL', slotId: 12, eligible: (p) => p.isBatter },
];

export const PITCHER_SLOT_DEFS = [
  { id: 'SP1', name: 'SP', slotId: 14, eligible: (p) => p.positions.has('SP') },
  { id: 'SP2', name: 'SP', slotId: 14, eligible: (p) => p.positions.has('SP') },
  { id: 'SP3', name: 'SP', slotId: 14, eligible: (p) => p.positions.has('SP') },
  { id: 'SP4', name: 'SP', slotId: 14, eligible: (p) => p.positions.has('SP') },
  { id: 'RP1', name: 'RP', slotId: 15, eligible: (p) => p.positions.has('RP') },
  { id: 'RP2', name: 'RP', slotId: 15, eligible: (p) => p.positions.has('RP') },
  { id: 'P1', name: 'P', slotId: 13, eligible: (p) => p.isPitcher },
  { id: 'P2', name: 'P', slotId: 13, eligible: (p) => p.isPitcher },
  { id: 'P3', name: 'P', slotId: 13, eligible: (p) => p.isPitcher },
  { id: 'P4', name: 'P', slotId: 13, eligible: (p) => p.isPitcher },
  { id: 'P5', name: 'P', slotId: 13, eligible: (p) => p.isPitcher },
  { id: 'P6', name: 'P', slotId: 13, eligible: (p) => p.isPitcher },
];

/**
 * Infer player positions from all historical daily records in the season
 */
export function buildPlayerPositionRegistry(allRecords) {
  const registry = {};

  allRecords.forEach(r => {
    const pid = r.player_id;
    if (!pid) return;

    if (!registry[pid]) {
      registry[pid] = {
        id: pid,
        name: r.full_name || 'Unknown',
        positions: new Set(),
        isBatter: false,
        isPitcher: false,
        slotCounts: {}
      };
    }

    const reg = registry[pid];
    const slot = r.lineup_slot_id;

    if (slot !== undefined && slot !== null) {
      reg.slotCounts[slot] = (reg.slotCounts[slot] || 0) + 1;
      if (slot === 0) reg.positions.add('C');
      if (slot === 1) reg.positions.add('1B');
      if (slot === 2) reg.positions.add('2B');
      if (slot === 3) reg.positions.add('3B');
      if (slot === 4) reg.positions.add('SS');
      if (slot === 5) reg.positions.add('OF');
      if (slot === 11) reg.positions.add('DH');
      if (slot === 14) reg.positions.add('SP');
      if (slot === 15) reg.positions.add('RP');
    }

    const s = r.stats || {};
    const ip = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
    const k = parseFloat(s.K ?? s['48'] ?? 0);
    const er = parseFloat(s.ER ?? s['45'] ?? 0);
    const qs = parseFloat(s.QS ?? s['63'] ?? 0);
    const sv = parseFloat(s.SV ?? s['57'] ?? 0);
    const hd = parseFloat(s.HD ?? s['60'] ?? 0);
    const gs = parseFloat(s.GS ?? s['33'] ?? 0);

    const pa = parseFloat(s.PA ?? s['16'] ?? 0);
    const ab = parseFloat(s.AB ?? s['0'] ?? 0);
    const h = parseFloat(s.H ?? s['1'] ?? 0);
    const rbi = parseFloat(s.RBI ?? s['21'] ?? 0);
    const hr = parseFloat(s.HR ?? s['5'] ?? 0);

    if (ip > 0 || k > 0 || er > 0 || qs > 0 || sv > 0 || hd > 0 || gs > 0) {
      reg.isPitcher = true;
      if (gs > 0) reg.positions.add('SP');
      if (sv > 0 || hd > 0) reg.positions.add('RP');
    }

    if (pa > 0 || ab > 0 || h > 0 || rbi > 0 || hr > 0) {
      reg.isBatter = true;
    }
  });

  // Second pass: fill fallbacks
  Object.values(registry).forEach(reg => {
    if (reg.isPitcher && !reg.positions.has('SP') && !reg.positions.has('RP')) {
      reg.positions.add('SP');
      reg.positions.add('RP');
    }
    if (reg.isBatter && reg.positions.size === 0) {
      reg.positions.add('UTIL');
      reg.positions.add('DH');
    }
  });

  return registry;
}

/**
 * Calculate single-day fantasy contribution score for a batter
 */
export function getDailyBatterScore(stats) {
  if (!stats) return 0;
  const pa = parseFloat(stats.PA ?? stats['16'] ?? 0);
  const ab = parseFloat(stats.AB ?? stats['0'] ?? 0);
  const h = parseFloat(stats.H ?? stats['1'] ?? 0);
  const r = parseFloat(stats.R ?? stats['20'] ?? 0);
  const hr = parseFloat(stats.HR ?? stats['5'] ?? 0);
  const rbi = parseFloat(stats.RBI ?? stats['21'] ?? 0);
  const sb = parseFloat(stats.SB ?? stats['23'] ?? 0);
  const bb = parseFloat(stats.BB ?? stats['10'] ?? 0);
  const hbp = parseFloat(stats.HBP ?? stats['12'] ?? 0);

  // If did not play: 0
  if (pa === 0 && h === 0 && r === 0 && rbi === 0 && bb === 0 && hr === 0 && sb === 0) {
    return 0;
  }

  // Weight counting stats heavily + positive on-base reward - penalty for high empty outs dragging OBP
  const countingScore = (r * 2.0) + (hr * 4.0) + (rbi * 2.0) + (sb * 3.0);
  const onBaseScore = (h * 1.0) + (bb * 1.0) + (hbp * 1.0);
  const hitlessOuts = Math.max(0, ab - h);
  const outPenalty = hitlessOuts * 0.25;

  return parseFloat((countingScore + onBaseScore - outPenalty).toFixed(2));
}

/**
 * Calculate single-day fantasy contribution score for a pitcher
 */
export function getDailyPitcherScore(stats) {
  if (!stats) return 0;
  const ipRaw = parseFloat(stats.IP_raw ?? stats.IP ?? stats['34'] ?? 0);
  const ip = ipRaw > 0 ? (ipRaw < 40 ? ipRaw : ipRaw / 3) : 0;
  const er = parseFloat(stats.ER ?? stats['45'] ?? 0);
  const k = parseFloat(stats.K ?? stats['48'] ?? 0);
  const qs = parseFloat(stats.QS ?? stats['63'] ?? 0);
  const sv = parseFloat(stats.SV ?? stats['57'] ?? 0);
  const hd = parseFloat(stats.HD ?? stats['60'] ?? 0);
  const hAll = parseFloat(stats.H_Allowed ?? stats['37'] ?? 0);
  const bbAll = parseFloat(stats.BB_Allowed ?? stats['39'] ?? 0);

  // If did not pitch: 0
  if (ip === 0 && k === 0 && er === 0 && qs === 0 && sv === 0 && hd === 0) {
    return 0;
  }

  // Base score: IP, K, QS, Saves/Holds
  const positiveScore = (ip * 3.0) + (k * 1.0) + (qs * 5.0) + ((sv + hd) * 5.0);
  // Negative drag: ER, Base Runners
  const negativeDrag = (er * 3.0) + ((hAll + bbAll) * 0.5);

  return parseFloat((positiveScore - negativeDrag).toFixed(2));
}

/**
 * Solve Maximum Weight Bipartite Matching using Augmenting Paths
 * Assigns players to slots to maximize total score
 */
function solveOptimalAssignment(players, slots) {
  // Sort players by score descending
  const sortedPlayers = [...players].sort((a, b) => b.score - a.score);
  
  // slotIndex -> playerIndex
  const slotAssignments = new Array(slots.length).fill(null);

  // Try to find augmenting path for player p
  function findAugmentingPath(playerIdx, visitedSlots) {
    const p = sortedPlayers[playerIdx];

    for (let sIdx = 0; sIdx < slots.length; sIdx++) {
      if (visitedSlots.has(sIdx)) continue;
      
      const slotDef = slots[sIdx];
      if (!slotDef.eligible(p)) continue;

      visitedSlots.add(sIdx);

      // If slot is empty, we can assign
      if (slotAssignments[sIdx] === null) {
        slotAssignments[sIdx] = playerIdx;
        return true;
      }

      // If slot is occupied by someone else, see if that other person can be moved elsewhere
      const currentTenant = slotAssignments[sIdx];
      // Only displace if current tenant can find another slot without losing a superior assignment
      if (findAugmentingPath(currentTenant, visitedSlots)) {
        slotAssignments[sIdx] = playerIdx;
        return true;
      }
    }

    return false;
  }

  // Assign players in descending score order
  // Only assign players if their score > 0 (or if they are better than leaving slot empty)
  for (let pIdx = 0; pIdx < sortedPlayers.length; pIdx++) {
    const p = sortedPlayers[pIdx];
    // For pitchers, don't start negative-scoring blowouts
    if (p.isPitcher && p.score < 0) continue;

    const visited = new Set();
    findAugmentingPath(pIdx, visited);
  }

  // Map result: slotDef -> assigned player
  const result = slots.map((slotDef, sIdx) => {
    const pIdx = slotAssignments[sIdx];
    return {
      slot: slotDef,
      player: pIdx !== null ? sortedPlayers[pIdx] : null
    };
  });

  const assignedPlayerIds = new Set(
    result.filter(r => r.player !== null).map(r => r.player.id)
  );

  const benchedPlayers = players.filter(p => !assignedPlayerIds.has(p.id));

  return {
    assignments: result,
    starters: result.filter(r => r.player !== null).map(r => r.player),
    bench: benchedPlayers
  };
}

/**
 * Optimize daily lineup for a specific team on a specific scoring period (day)
 */
export function optimizeDailyTeamLineup(teamRecords, registry) {
  if (!teamRecords || teamRecords.length === 0) return null;

  const dateOrPeriod = teamRecords[0]?.scoring_period_id;
  const teamId = teamRecords[0]?.team_id;

  // Prepare player objects
  const playerMap = {};
  teamRecords.forEach(r => {
    const pid = r.player_id;
    const reg = registry[pid] || {
      positions: new Set(['UTIL']),
      isBatter: true,
      isPitcher: false
    };

    const isPitcher = reg.isPitcher;
    const isBatter = reg.isBatter || !isPitcher;

    const score = isPitcher
      ? getDailyPitcherScore(r.stats)
      : getDailyBatterScore(r.stats);

    playerMap[pid] = {
      id: pid,
      name: r.full_name,
      teamId: r.team_id,
      actualSlotId: r.lineup_slot_id,
      stats: r.stats || {},
      positions: reg.positions,
      isPitcher,
      isBatter,
      score,
      rawRecord: r
    };
  });

  const allTeamPlayers = Object.values(playerMap);
  const batters = allTeamPlayers.filter(p => p.isBatter);
  const pitchers = allTeamPlayers.filter(p => p.isPitcher);

  // Solve Batter & Pitcher assignments
  const batterSolution = solveOptimalAssignment(batters, BATTER_SLOT_DEFS);
  const pitcherSolution = solveOptimalAssignment(pitchers, PITCHER_SLOT_DEFS);

  const optimalStarters = [...batterSolution.starters, ...pitcherSolution.starters];
  const optimalStarterIds = new Set(optimalStarters.map(p => p.id));

  // Determine actual starters vs bench
  const actualStarters = allTeamPlayers.filter(p => p.actualSlotId !== 16 && p.actualSlotId !== 17);
  const actualStarterIds = new Set(actualStarters.map(p => p.id));

  // Identify swaps:
  // Promoted = In optimal starters, but was on actual Bench/IL
  const promoted = optimalStarters.filter(p => !actualStarterIds.has(p.id));
  // Demoted = Was in actual starters, but benched in optimal
  const demoted = actualStarters.filter(p => !optimalStarterIds.has(p.id));

  // Compute stat totals for actual vs optimal
  const actualStats = aggregateStats(teamRecords, { includeBenchOnly: false });
  const optimalRecords = optimalStarters.map(p => p.rawRecord);
  const optimalStats = aggregateStats(optimalRecords, { includeAll: true });

  const statDelta = {
    R: (optimalStats.R || 0) - (actualStats.R || 0),
    HR: (optimalStats.HR || 0) - (actualStats.HR || 0),
    RBI: (optimalStats.RBI || 0) - (actualStats.RBI || 0),
    SB: (optimalStats.SB || 0) - (actualStats.SB || 0),
    OBP_raw: (optimalStats.OBP_raw || 0) - (actualStats.OBP_raw || 0),
    K: (optimalStats.K || 0) - (actualStats.K || 0),
    QS: (optimalStats.QS || 0) - (actualStats.QS || 0),
    'SV+HDs': (optimalStats['SV+HDs'] || 0) - (actualStats['SV+HDs'] || 0),
    ERA_raw: (actualStats.ERA_raw || 0) - (optimalStats.ERA_raw || 0), // positive delta means ERA improved (went down)
    WHIP_raw: (actualStats.WHIP_raw || 0) - (optimalStats.WHIP_raw || 0)
  };

  const netScoreGain = optimalStarters.reduce((acc, p) => acc + p.score, 0) -
                       actualStarters.reduce((acc, p) => acc + p.score, 0);

  return {
    scoringPeriodId: dateOrPeriod,
    teamId,
    allPlayers: allTeamPlayers,
    actualStarters,
    optimalStarters,
    batterAssignments: batterSolution.assignments,
    pitcherAssignments: pitcherSolution.assignments,
    promoted,
    demoted,
    actualStats,
    optimalStats,
    statDelta,
    netScoreGain: parseFloat(netScoreGain.toFixed(2)),
    suboptimalStartCount: promoted.length
  };
}

/**
 * Simulate season-long "Best Lineup Possible" across all teams and all scoring periods
 */
export function simulateSeasonBestLineups(allStats = [], teams = {}) {
  if (!allStats || allStats.length === 0) return null;

  const registry = buildPlayerPositionRegistry(allStats);

  // Group by (scoring_period_id, team_id)
  const byPeriodAndTeam = {};
  allStats.forEach(r => {
    const sp = r.scoring_period_id;
    const tid = r.team_id;
    if (!sp || !tid) return;
    if (!byPeriodAndTeam[sp]) byPeriodAndTeam[sp] = {};
    if (!byPeriodAndTeam[sp][tid]) byPeriodAndTeam[sp][tid] = [];
    byPeriodAndTeam[sp][tid].push(r);
  });

  const humanTeamIds = Object.keys(teams)
    .map(Number)
    .filter(id => id !== 99);

  const teamDailyLogs = {};
  humanTeamIds.forEach(tid => {
    teamDailyLogs[tid] = [];
  });

  const benchBlunders = [];

  const sortedPeriods = Object.keys(byPeriodAndTeam)
    .map(Number)
    .sort((a, b) => a - b);

  sortedPeriods.forEach(sp => {
    const teamObj = byPeriodAndTeam[sp];
    humanTeamIds.forEach(tid => {
      const records = teamObj[tid] || [];
      if (records.length === 0) return;

      const dailyResult = optimizeDailyTeamLineup(records, registry);
      if (dailyResult) {
        teamDailyLogs[tid].push(dailyResult);

        // Record notable bench blunders
        dailyResult.promoted.forEach(prom => {
          if (prom.score >= 5.0) {
            benchBlunders.push({
              periodId: sp,
              teamId: tid,
              teamName: teams[tid]?.name || `Team ${tid}`,
              player: prom,
              benchedScore: prom.score,
              stats: prom.stats,
              isPitcher: prom.isPitcher
            });
          }
        });
      }
    });
  });

  // Calculate cumulative stats for each team: Actual vs Optimal
  const teamCumulativeActual = {};
  const teamCumulativeOptimal = {};

  humanTeamIds.forEach(tid => {
    const dailyList = teamDailyLogs[tid];
    const allActualRecords = [];
    const allOptimalRecords = [];

    dailyList.forEach(day => {
      day.actualStarters.forEach(p => allActualRecords.push(p.rawRecord));
      day.optimalStarters.forEach(p => allOptimalRecords.push(p.rawRecord));
    });

    teamCumulativeActual[tid] = aggregateStats(allActualRecords, { includeAll: true });
    teamCumulativeOptimal[tid] = aggregateStats(allOptimalRecords, { includeAll: true });
  });

  // Roto standings: Actual vs Optimal
  const actualRoto = calculateRotoPoints(teamCumulativeActual);
  const optimalRoto = calculateRotoPoints(teamCumulativeOptimal);

  // Compile summary table per manager
  const managerSummary = humanTeamIds.map(tid => {
    const actStats = teamCumulativeActual[tid] || {};
    const optStats = teamCumulativeOptimal[tid] || {};
    const actRoto = actualRoto[tid] || { total: 0 };
    const optRoto = optimalRoto[tid] || { total: 0 };

    const dailyList = teamDailyLogs[tid];
    const totalSuboptimalStarts = dailyList.reduce((acc, d) => acc + d.suboptimalStartCount, 0);
    const totalNetScoreGained = dailyList.reduce((acc, d) => acc + d.netScoreGain, 0);

    const actualTotalPoints = actRoto.total || 0;
    const optimalTotalPoints = optRoto.total || 0;
    const pointsLeftOnTable = Math.max(0, optimalTotalPoints - actualTotalPoints);
    const efficiencyPct = optimalTotalPoints > 0 ? ((actualTotalPoints / optimalTotalPoints) * 100).toFixed(1) : '100.0';

    return {
      teamId: tid,
      teamName: teams[tid]?.name || `Team ${tid}`,
      owner: teams[tid]?.owner || teams[tid]?.name || `Owner ${tid}`,
      actualStats: actStats,
      optimalStats: optStats,
      actualRotoPoints: actualTotalPoints,
      optimalRotoPoints: optimalTotalPoints,
      rotoPointsDelta: parseFloat((optimalTotalPoints - actualTotalPoints).toFixed(1)),
      efficiencyPct: parseFloat(efficiencyPct),
      pointsLeftOnTable: parseFloat(pointsLeftOnTable.toFixed(1)),
      suboptimalStarts: totalSuboptimalStarts,
      totalNetScoreGained: parseFloat(totalNetScoreGained.toFixed(1)),
      categoryDeltas: {
        R: (optStats.R || 0) - (actStats.R || 0),
        HR: (optStats.HR || 0) - (actStats.HR || 0),
        RBI: (optStats.RBI || 0) - (actStats.RBI || 0),
        SB: (optStats.SB || 0) - (actStats.SB || 0),
        OBP_raw: (optStats.OBP_raw || 0) - (actStats.OBP_raw || 0),
        K: (optStats.K || 0) - (actStats.K || 0),
        QS: (optStats.QS || 0) - (actStats.QS || 0),
        'SV+HDs': (optStats['SV+HDs'] || 0) - (actStats['SV+HDs'] || 0),
        ERA_raw: (actStats.ERA_raw || 0) - (optStats.ERA_raw || 0),
        WHIP_raw: (actStats.WHIP_raw || 0) - (optStats.WHIP_raw || 0)
      }
    };
  });

  // Assign ranks for actual and optimal
  const sortedActual = [...managerSummary].sort((a, b) => b.actualRotoPoints - a.actualRotoPoints);
  sortedActual.forEach((m, idx) => { m.actualRank = idx + 1; });

  const sortedOptimal = [...managerSummary].sort((a, b) => b.optimalRotoPoints - a.optimalRotoPoints);
  sortedOptimal.forEach((m, idx) => {
    m.optimalRank = idx + 1;
    m.rankChange = m.actualRank - m.optimalRank; // positive = moved up in optimal
  });

  // Sort bench blunders descending by blunder magnitude
  const topBenchBlunders = [...benchBlunders].sort((a, b) => b.benchedScore - a.benchedScore).slice(0, 50);

  return {
    managers: managerSummary,
    topBenchBlunders,
    teamDailyLogs,
    actualRoto,
    optimalRoto,
    periods: sortedPeriods
  };
}
