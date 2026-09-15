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
 * and generate an itemized cumulative category breakdown
 */
export function calculatePlayerStatValue(playerName, seasonYear, statsData = [], assetStats = null) {
  if (!playerName) return { points: 0, summary: 'No stats', statBreakdown: [] };
  const clean = playerName.toLowerCase().trim();

  let r = 0, hr = 0, rbi = 0, sb = 0, h = 0, ab = 0, bb = 0, ip = 0, k = 0, qs = 0, sv = 0, hd = 0, er = 0;
  let isPitcher = false;
  let hasStats = false;

  if (assetStats && typeof assetStats === 'object') {
    hasStats = true;
    isPitcher = Boolean(assetStats.is_pitcher);
    ab = parseFloat(assetStats.AB || 0);
    h = parseFloat(assetStats.H || 0);
    r = parseFloat(assetStats.R || 0);
    hr = parseFloat(assetStats.HR || 0);
    rbi = parseFloat(assetStats.RBI || 0);
    sb = parseFloat(assetStats.SB || 0);
    bb = parseFloat(assetStats.BB || 0);
    ip = parseFloat(assetStats.IP || 0);
    er = parseFloat(assetStats.ER || 0);
    k = parseFloat(assetStats.K || 0);
    qs = parseFloat(assetStats.QS || 0);
    sv = parseFloat(assetStats.SV || 0);
    hd = parseFloat(assetStats.HD || 0);
    if (ip > 0 || k > 0 || sv > 0) isPitcher = true;
  } else {
    // Find relevant player records in statsData
    const matchingRecords = (statsData || []).filter(rec => {
      const rName = (rec.full_name || rec['fullName'] || '').toLowerCase();
      return rName.includes(clean) || clean.includes(rName);
    });

    if (matchingRecords.length > 0) {
      hasStats = true;
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
    }
  }

  if (!hasStats || (ab === 0 && ip === 0 && k === 0 && r === 0 && hr === 0)) {
    // Calibrate reasonable baseline from known player tiers
    const elitePlayers = ['bobby witt jr.', 'corbin carroll', 'gunnar henderson', 'chris sale', 'mike trout', 'trea turner', 'gerrit cole', 'corbin burnes', 'cody bellinger', 'bryce harper', 'josh hader', 'matt olson'];
    const midPlayers = ['austin riley', 'michael harris ii', 'kevin gausman', 'max fried', 'oneil cruz', 'bryce miller', 'randy arozarena', 'bo bichette', 'ketel marte', 'josh naylor', 'sandy alcantara', 'jesus luzardo'];
    
    let baselinePts = 22.0;
    let baselineTier = 'Role Player / Streamer (estimated)';
    if (elitePlayers.some(p => clean.includes(p))) {
      baselinePts = 55.0;
      baselineTier = 'Elite Performance (estimated)';
    } else if (midPlayers.some(p => clean.includes(p))) {
      baselinePts = 38.0;
      baselineTier = 'Solid Starter (estimated)';
    }

    return {
      points: baselinePts,
      summary: baselineTier,
      isEstimated: true,
      statBreakdown: [
        { cat: 'Baseline Tier', val: baselineTier, pts: baselinePts, formula: 'Calibrated Player Tier Equity' }
      ]
    };
  }

  if (isPitcher && ip > 0) {
    const era = (er * 9) / ip;
    const eraBonus = Math.max(-15, (4.00 - era) * (ip / 9) * 2.5);
    const kPts = parseFloat((k * 0.4).toFixed(1));
    const qsPts = parseFloat((qs * 3.5).toFixed(1));
    const svHdPts = parseFloat(((sv + hd) * 3.0).toFixed(1));
    const eraPts = parseFloat(eraBonus.toFixed(1));
    const totalPts = parseFloat(Math.max(10, kPts + qsPts + svHdPts + eraPts).toFixed(1));

    const statBreakdown = [
      { cat: 'IP', val: ip.toFixed(1), pts: null, formula: `${ip.toFixed(1)} IP total` },
      { cat: 'K', val: Math.round(k), pts: kPts, formula: `${Math.round(k)} K × 0.4` },
      { cat: 'QS', val: Math.round(qs), pts: qsPts, formula: `${Math.round(qs)} QS × 3.5` },
      { cat: 'SV+HD', val: Math.round(sv + hd), pts: svHdPts, formula: `${Math.round(sv + hd)} SV+HD × 3.0` },
      { cat: 'ERA', val: era.toFixed(2), pts: eraPts, formula: `(4.00 - ${era.toFixed(2)}) × (${ip.toFixed(1)}/9) × 2.5` }
    ];

    return {
      points: totalPts,
      isPitcher: true,
      summary: `${ip.toFixed(1)} IP · ${Math.round(k)} K · ${era.toFixed(2)} ERA · ${Math.round(qs)} QS · ${Math.round(sv + hd)} SV+H`,
      statBreakdown
    };
  } else if (ab > 0 || r > 0 || hr > 0) {
    const pa = ab + bb || 1;
    const obp = (h + bb) / pa;
    const obpBonus = (obp - 0.320) * pa * 0.2;
    const rPts = parseFloat((r * 0.4).toFixed(1));
    const hrPts = parseFloat((hr * 2.2).toFixed(1));
    const rbiPts = parseFloat((rbi * 0.4).toFixed(1));
    const sbPts = parseFloat((sb * 1.5).toFixed(1));
    const obpPts = parseFloat(obpBonus.toFixed(1));
    const totalPts = parseFloat(Math.max(10, rPts + hrPts + rbiPts + sbPts + obpPts).toFixed(1));

    const obpDisplay = obp.toFixed(3).replace(/^0/, '');
    const statBreakdown = [
      { cat: 'R', val: Math.round(r), pts: rPts, formula: `${Math.round(r)} R × 0.4` },
      { cat: 'HR', val: Math.round(hr), pts: hrPts, formula: `${Math.round(hr)} HR × 2.2` },
      { cat: 'RBI', val: Math.round(rbi), pts: rbiPts, formula: `${Math.round(rbi)} RBI × 0.4` },
      { cat: 'SB', val: Math.round(sb), pts: sbPts, formula: `${Math.round(sb)} SB × 1.5` },
      { cat: 'OBP', val: obpDisplay, pts: obpPts, formula: `(${obpDisplay} - .320) × ${Math.round(pa)} PA × 0.2` }
    ];

    return {
      points: totalPts,
      isPitcher: false,
      summary: `${Math.round(hr)} HR · ${Math.round(rbi)} RBI · ${Math.round(r)} R · ${Math.round(sb)} SB · ${obpDisplay} OBP`,
      statBreakdown
    };
  }

  return { points: 25.0, summary: 'Standard Contributor', statBreakdown: [] };
}

/**
 * Grade an individual asset in a trade package
 */
export function evaluateAsset(asset, seasonYear, statsData = []) {
  if (!asset) return { ...asset, value: 0, detail: '' };

  const type = asset.asset_type;
  if (type === 'Pick') {
    const curveVal = calculatePickValue(asset.pick_number);
    
    // If an actual drafted player is attached from draft history
    if (asset.drafted_player) {
      const dp = asset.drafted_player;
      const statVal = calculatePlayerStatValue(dp.player_name, seasonYear, statsData, dp.stats);
      const keeper = getKeeperSurplus(dp.player_name, seasonYear);
      const draftedPlayerValue = parseFloat((statVal.points + keeper.bonus).toFixed(1));
      const surplus = parseFloat((draftedPlayerValue - curveVal).toFixed(1));

      const breakdown = [...(statVal.statBreakdown || [])];
      if (keeper.isKept) {
        breakdown.push({
          cat: 'Keeper Equity',
          val: keeper.label,
          pts: keeper.bonus,
          formula: 'Franchise Keeper Retention Bonus'
        });
      }

      return {
        ...asset,
        value: draftedPlayerValue, // Realized draft selection equity
        curveValue: curveVal,
        draftedPlayerValue,
        surplus,
        statPoints: statVal.points,
        statSummary: statVal.summary,
        statBreakdown: breakdown,
        isKept: keeper.isKept,
        keeperBonus: keeper.bonus,
        keeperLabel: keeper.label,
        detail: `Selected: ${dp.player_name} (${draftedPlayerValue} pts · ${surplus >= 0 ? '+' : ''}${surplus} vs curve)`
      };
    }

    // Unselected or future draft pick
    return {
      ...asset,
      value: curveVal,
      curveValue: curveVal,
      statBreakdown: [
        {
          cat: 'Pick Curve',
          val: `Pick #${asset.pick_number || '?'}`,
          pts: curveVal,
          formula: '100 × e^(-0.015 × (Pick - 1))'
        }
      ],
      detail: `Pick #${asset.pick_number || '?'} (Round ${asset.round_number || '?'}) · ${curveVal} pts`
    };
  }

  if (type === 'Budget') {
    const val = calculateBudgetValue(asset.budget_amount);
    return {
      ...asset,
      value: val,
      statBreakdown: [
        {
          cat: 'Budget Cash',
          val: `$${asset.budget_amount}`,
          pts: val,
          formula: '$1.00 = 3.5 pts purchasing leverage'
        }
      ],
      detail: `$${asset.budget_amount} Budget Cash · ${val} pts`
    };
  }

  if (type === 'Player') {
    const statVal = calculatePlayerStatValue(asset.asset_name, seasonYear, statsData, asset.stats);
    const keeper = getKeeperSurplus(asset.asset_name, seasonYear);
    const totalVal = parseFloat((statVal.points + keeper.bonus).toFixed(1));

    const breakdown = [...(statVal.statBreakdown || [])];
    if (keeper.isKept) {
      breakdown.push({
        cat: 'Keeper Equity',
        val: keeper.label,
        pts: keeper.bonus,
        formula: 'Next-Season Franchise Retention Bonus'
      });
    }

    return {
      ...asset,
      value: totalVal,
      statPoints: statVal.points,
      statSummary: statVal.summary,
      statBreakdown: breakdown,
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
