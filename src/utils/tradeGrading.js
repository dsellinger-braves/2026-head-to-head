// src/utils/tradeGrading.js

/**
 * Trade Grading & Valuation Engine for 2026 Head to Head Heftystrong
 * 
 * Evaluates trade packages across 4 core dimensions:
 * 1. Draft Pick Equity (exponential empirical pick curve)
 * 2. Keeper Budget Cash ($1 = 3.5 value points)
 * 3. In-Season Player Production (post-trade counting & rate stats on acquiring team)
 * 4. Long-Term Franchise Retention Value (surplus equity from players kept in subsequent seasons)
 */

import keeperData2026 from '../data/keeperInput2026.json' with { type: 'json' };

// Empirical Draft Pick Valuation Curve: 100 * exp(-0.015 * (pick - 1))
export function calculatePickValue(pickNumber) {
  if (!pickNumber || pickNumber < 1) return 0;
  const num = Math.min(Math.max(1, pickNumber), 300);
  const val = 100 * Math.exp(-0.015 * (num - 1));
  return parseFloat(val.toFixed(1));
}

// Budget cash exchange rate
export function calculateBudgetValue(amount) {
  if (!amount || isNaN(amount)) return 0;
  return parseFloat((parseFloat(amount) * 3.5).toFixed(1));
}

// Known players kept in subsequent seasons
const HISTORICAL_KEEPERS_BY_YEAR = {
  2024: new Set([
    'gunnar henderson', 'corbin carroll', 'bobby witt jr.', 'bobby witt jr',
    'michael harris ii', 'grayson rodriguez', 'trea turner'
  ]),
  2025: new Set([
    'bobby witt jr.', 'bobby witt jr', 'corbin carroll', 'austin riley',
    'bryce harper', 'chris sale', 'oneil cruz', 'bryce miller'
  ]),
  2026: new Set(
    (keeperData2026?.keepers || []).map(k => k.player_name?.toLowerCase())
  )
};

/**
 * Check if player was kept in the subsequent season following the trade
 */
export function getKeeperSurplus(playerName, seasonYear) {
  if (!playerName) return { isKept: false, bonus: 0 };
  const clean = playerName.toLowerCase().trim();
  const nextYear = seasonYear + 1;
  const nextYearKeepers = HISTORICAL_KEEPERS_BY_YEAR[nextYear] || HISTORICAL_KEEPERS_BY_YEAR[2026];

  if (nextYearKeepers && (nextYearKeepers.has(clean) || Array.from(nextYearKeepers).some(k => k.includes(clean) || clean.includes(k)))) {
    return {
      isKept: true,
      nextYear,
      bonus: 28.0, // High-leverage franchise keeper equity
      label: `Kept in ${nextYear}`
    };
  }

  return { isKept: false, bonus: 0, label: null };
}

/**
 * Approximate player baseline fantasy production points from stats
 */
export function calculatePlayerStatValue(playerName, seasonYear, statsData = []) {
  if (!playerName) return { points: 0, summary: 'No stats' };
  const clean = playerName.toLowerCase().trim();

  // Find relevant player records
  const matchingRecords = statsData.filter(r => {
    const rName = (r.full_name || r['fullName'] || '').toLowerCase();
    return rName.includes(clean) || clean.includes(rName);
  });

  if (!matchingRecords || matchingRecords.length === 0) {
    // Calibrate reasonable baseline from known player tiers
    const elitePlayers = ['bobby witt jr.', 'corbin carroll', 'gunnar henderson', 'chris sale', 'mike trout', 'trea turner', 'gerrit cole', 'corbin burnes', 'cody bellinger', 'bryce harper', 'josh hader'];
    const midPlayers = ['austin riley', 'michael harris ii', 'kevin gausman', 'max fried', 'oneil cruz', 'bryce miller', 'randy arozarena', 'bo bichette', 'matt olson', 'ketel marte'];
    
    if (elitePlayers.some(p => clean.includes(p))) {
      return { points: 55.0, summary: 'Elite Performance (estimated)' };
    }
    if (midPlayers.some(p => clean.includes(p))) {
      return { points: 38.0, summary: 'Solid Starter (estimated)' };
    }
    return { points: 22.0, summary: 'Role Player / Streamer (estimated)' };
  }

  // If we have records, sum counting stats
  let r = 0, hr = 0, rbi = 0, sb = 0, h = 0, ab = 0, bb = 0, ip = 0, k = 0, qs = 0, sv = 0, hd = 0, er = 0;
  let isPitcher = false;

  matchingRecords.forEach(rec => {
    const st = rec.stats || rec;
    const rAB = parseFloat(st['0'] || 0);
    const rH = parseFloat(st['1'] || 0);
    const rHR = parseFloat(st['5'] || 0);
    const rBB = parseFloat(st['10'] || 0);
    const rR = parseFloat(st['20'] || 0);
    const rRBI = parseFloat(st['21'] || 0);
    const rSB = parseFloat(st['23'] || 0);
    const rIP = parseFloat(st['34'] || 0) / 3;
    const rER = parseFloat(st['45'] || 0);
    const rK = parseFloat(st['48'] || 0);
    const rSV = parseFloat(st['57'] || 0);
    const rHD = parseFloat(st['60'] || 0);
    const rQS = parseFloat(st['63'] || 0);

    if (rIP > 0 || rK > 0 || rSV > 0) isPitcher = true;

    ab += rAB; h += rH; hr += rHR; bb += rBB; r += rR; rbi += rRBI; sb += rSB;
    ip += rIP; er += rER; k += rK; sv += rSV; hd += rHD; qs += rQS;
  });

  if (isPitcher && ip > 0) {
    const era = (er * 9) / ip;
    const eraBonus = Math.max(-15, (4.00 - era) * (ip / 9) * 2.5);
    const pts = (k * 0.4) + (qs * 3.5) + ((sv + hd) * 3.0) + eraBonus;
    return {
      points: parseFloat(Math.max(10, pts).toFixed(1)),
      summary: `${ip.toFixed(1)} IP · ${k} K · ${era.toFixed(2)} ERA · ${qs} QS · ${sv + hd} SV+H`
    };
  } else if (ab > 0) {
    const obp = (h + bb) / (ab + bb || 1);
    const obpBonus = (obp - 0.320) * (ab + bb) * 0.2;
    const pts = (r * 0.4) + (hr * 2.2) + (rbi * 0.4) + (sb * 1.5) + obpBonus;
    return {
      points: parseFloat(Math.max(10, pts).toFixed(1)),
      summary: `${hr} HR · ${rbi} RBI · ${r} R · ${sb} SB · ${obp.toFixed(3).replace(/^0/, '')} OBP`
    };
  }

  return { points: 25.0, summary: 'Standard Contributor' };
}

/**
 * Grade an individual asset in a trade package
 */
export function evaluateAsset(asset, seasonYear, statsData = []) {
  if (!asset) return { ...asset, value: 0, detail: '' };

  const type = asset.asset_type;
  if (type === 'Pick') {
    const val = calculatePickValue(asset.pick_number);
    return {
      ...asset,
      value: val,
      detail: `Pick #${asset.pick_number || '?'} (Round ${asset.round_number || '?'}) · ${val} pts`
    };
  }

  if (type === 'Budget') {
    const val = calculateBudgetValue(asset.budget_amount);
    return {
      ...asset,
      value: val,
      detail: `$${asset.budget_amount} Budget Cash · ${val} pts`
    };
  }

  if (type === 'Player') {
    const statVal = calculatePlayerStatValue(asset.asset_name, seasonYear, statsData);
    const keeper = getKeeperSurplus(asset.asset_name, seasonYear);
    const totalVal = parseFloat((statVal.points + keeper.bonus).toFixed(1));

    return {
      ...asset,
      value: totalVal,
      statPoints: statVal.points,
      statSummary: statVal.summary,
      isKept: keeper.isKept,
      keeperBonus: keeper.bonus,
      keeperLabel: keeper.label,
      detail: `${statVal.points} stat pts${keeper.isKept ? ` + ${keeper.bonus} keeper equity` : ''}`
    };
  }

  return { ...asset, value: 0, detail: '' };
}

/**
 * Determine letter grade from net point margin
 */
export function getLetterGrade(margin) {
  if (margin >= 30.0) return { grade: 'A+', text: 'Franchise Win', color: 'emerald' };
  if (margin >= 20.0) return { grade: 'A', text: 'Decisive Win', color: 'emerald' };
  if (margin >= 12.0) return { grade: 'A-', text: 'Strong Win', color: 'teal' };
  if (margin >= 6.0) return { grade: 'B+', text: 'Favorable', color: 'cyan' };
  if (margin >= 2.0) return { grade: 'B', text: 'Slight Edge', color: 'cyan' };
  if (margin >= -2.0) return { grade: 'C+', text: 'Fair / Balanced', color: 'slate' };
  if (margin >= -6.0) return { grade: 'C-', text: 'Slight Deficit', color: 'amber' };
  if (margin >= -12.0) return { grade: 'D+', text: 'Unfavorable', color: 'amber' };
  if (margin >= -20.0) return { grade: 'D', text: 'Significant Loss', color: 'rose' };
  return { grade: 'F', text: 'Lopsided Loss', color: 'rose' };
}

/**
 * Grade a complete multi-team trade
 */
export function gradeTrade(trade, statsData = []) {
  if (!trade || !trade.owner_packages) return null;

  const seasonYear = trade.season_year || 2026;
  const ownerGrades = {};

  Object.keys(trade.owner_packages).forEach(owner => {
    const pkg = trade.owner_packages[owner];
    const evaluatedSent = (pkg.sent || []).map(a => evaluateAsset(a, seasonYear, statsData));
    const evaluatedReceived = (pkg.received || []).map(a => evaluateAsset(a, seasonYear, statsData));

    const totalSentVal = evaluatedSent.reduce((sum, a) => sum + (a.value || 0), 0);
    const totalReceivedVal = evaluatedReceived.reduce((sum, a) => sum + (a.value || 0), 0);
    const netMargin = parseFloat((totalReceivedVal - totalSentVal).toFixed(1));
    const gradeInfo = getLetterGrade(netMargin);

    ownerGrades[owner] = {
      owner,
      team_id: pkg.team_id,
      sent: evaluatedSent,
      received: evaluatedReceived,
      totalSentVal: parseFloat(totalSentVal.toFixed(1)),
      totalReceivedVal: parseFloat(totalReceivedVal.toFixed(1)),
      netMargin,
      grade: gradeInfo.grade,
      gradeText: gradeInfo.text,
      gradeColor: gradeInfo.color
    };
  });

  // Determine primary winner
  const ownersList = Object.values(ownerGrades);
  ownersList.sort((a, b) => b.netMargin - a.netMargin);
  const topOwner = ownersList[0];
  const bottomOwner = ownersList[ownersList.length - 1];

  let outcomeSummary = 'Evenly Matched Deal';
  if (topOwner.netMargin >= 6.0) {
    outcomeSummary = `${topOwner.owner} won by +${topOwner.netMargin} pts (${topOwner.grade})`;
  } else if (topOwner.netMargin > 0) {
    outcomeSummary = `Slight edge to ${topOwner.owner} (+${topOwner.netMargin} pts)`;
  }

  return {
    ...trade,
    gradedPackages: ownerGrades,
    winner: topOwner.netMargin >= 4.0 ? topOwner.owner : null,
    loser: bottomOwner.netMargin <= -4.0 ? bottomOwner.owner : null,
    outcomeSummary
  };
}
