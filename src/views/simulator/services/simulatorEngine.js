/**
 * @file simulatorEngine.js
 * @description Pure calculation engine and data transformation service
 * for the What-If Player Impact Simulator.
 */

import { aggregateStats, calculateRotoPoints } from '../../../utils/scoring';

export const ESPN_STAT_NAMES = {
  '0': 'AB', '1': 'H', '3': '2B', '4': '3B', '5': 'HR',
  '10': 'BB', '12': 'HBP', '13': 'SF', '16': 'PA', '17': 'OBP',
  '20': 'R', '21': 'RBI', '23': 'SB', '24': 'CS',
  '33': 'GS', '34': 'IP', '37': 'H_Allowed', '39': 'BB_Allowed',
  '44': 'R_Allowed', '45': 'ER', '48': 'K', '53': 'W', '54': 'L',
  '57': 'SV', '60': 'HD', '63': 'QS'
};

export const BATTING_CATS = ['R', 'HR', 'RBI', 'OBP', 'SB'];
export const PITCHING_CATS = ['K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

/**
 * Parses raw ESPN daily stat record into structured pitcher/batter metrics.
 */
export function parseRecord(record) {
  const s = {};
  for (const [key, val] of Object.entries(record.stats || {})) {
    s[ESPN_STAT_NAMES[key] ?? key] = val;
  }
  const espn = record.stats || {};

  const pa = parseFloat(s.PA ?? espn['16']) || 0;
  const ab = parseFloat(s.AB ?? espn['0']) || 0;
  const h = parseFloat(s.H ?? espn['1']) || 0;
  const r = parseFloat(s.R ?? espn['20']) || 0;
  const hr = parseFloat(s.HR ?? espn['5']) || 0;
  const rbi = parseFloat(s.RBI ?? espn['21']) || 0;
  const sb = parseFloat(s.SB ?? espn['23']) || 0;
  const bb = parseFloat(s.BB ?? espn['10']) || 0;
  const hbp = parseFloat(s.HBP ?? espn['12']) || 0;
  const sf = parseFloat(s.SF ?? espn['13']) || 0;
  const obp = parseFloat(s.OBP ?? espn['17']) || 0;

  const rawIp = parseFloat(s.IP_raw ?? s.IP ?? espn['34']) || 0;
  const ip = rawIp > 0 ? rawIp / 3 : 0;
  const er = parseFloat(s.ER ?? espn['45']) || 0;
  const bbAll = parseFloat(s.BB_Allowed ?? espn['39']) || 0;
  const hAll = parseFloat(s.H_Allowed ?? espn['37']) || 0;
  const k = parseFloat(s.K ?? espn['48']) || 0;
  const qs = parseFloat(s.QS ?? espn['63']) || 0;
  const sv = parseFloat(s.SV ?? espn['57']) || 0;
  const hd = parseFloat(s.HD ?? espn['60']) || 0;
  const w = parseFloat(s.W ?? espn['53']) || 0;
  const l = parseFloat(s.L ?? espn['54']) || 0;

  const isPitcher = ip > 0 || er > 0 || k > 0 || qs > 0 || sv > 0 || hd > 0 || bbAll > 0 || hAll > 0;
  const isBatter = pa > 0 || ab > 0 || h > 0 || r > 0 || hr > 0 || rbi > 0 || sb > 0;

  const era = ip > 0 ? (er * 9) / ip : null;
  const whip = ip > 0 ? (bbAll + hAll) / ip : null;

  return {
    recordId: record.id || `${record.player_id}_${record.scoring_period_id}`,
    playerId: record.player_id,
    fullName: record.full_name || 'Unknown Player',
    scoringPeriodId: record.scoring_period_id,
    lineupSlotId: record.lineup_slot_id,
    pa, ab, h, r, hr, rbi, sb, bb, hbp, sf, obp,
    ip, er, bbAll, hAll, k, qs, sv, hd, svHd: sv + hd, w, l,
    era, whip,
    isPitcher, isBatter
  };
}

/**
 * Builds player profiles for the active team, calculating ERA Damage and OBP Drag.
 */
export function buildPlayerProfiles(records = [], selectedTeamId, baselineTeamStatsMap = {}) {
  const pMap = {};

  records.forEach(r => {
    const parsed = parseRecord(r);
    const pid = parsed.playerId;
    if (!pMap[pid]) {
      pMap[pid] = {
        playerId: pid,
        fullName: parsed.fullName,
        isPitcher: false,
        isBatter: false,
        activeAppearances: 0,
        outings: [],
        ip: 0, er: 0, bbAll: 0, hAll: 0, k: 0, qs: 0, sv: 0, hd: 0, w: 0, l: 0,
        pa: 0, ab: 0, h: 0, r: 0, hr: 0, rbi: 0, sb: 0, bb: 0, hbp: 0, sf: 0
      };
    }

    const p = pMap[pid];
    p.activeAppearances += 1;
    p.outings.push(parsed);

    if (parsed.isPitcher) p.isPitcher = true;
    if (parsed.isBatter) p.isBatter = true;

    p.ip += parsed.ip;
    p.er += parsed.er;
    p.bbAll += parsed.bbAll;
    p.hAll += parsed.hAll;
    p.k += parsed.k;
    p.qs += parsed.qs;
    p.sv += parsed.sv;
    p.hd += parsed.hd;
    p.w += parsed.w;
    p.l += parsed.l;

    p.pa += parsed.pa;
    p.ab += parsed.ab;
    p.h += parsed.h;
    p.r += parsed.r;
    p.hr += parsed.hr;
    p.rbi += parsed.rbi;
    p.sb += parsed.sb;
    p.bb += parsed.bb;
    p.hbp += parsed.hbp;
    p.sf += parsed.sf;
  });

  const teamBaseline = baselineTeamStatsMap[selectedTeamId] || {};
  const teamBaseEra = parseFloat(teamBaseline.ERA_raw ?? teamBaseline.ERA) || 4.0;
  const teamBaseWhip = parseFloat(teamBaseline.WHIP_raw ?? teamBaseline.WHIP) || 1.25;
  const teamBaseObp = parseFloat(teamBaseline.OBP_raw ?? teamBaseline.OBP) || 0.320;

  return Object.values(pMap).map(p => {
    const era = p.ip > 0 ? (p.er * 9) / p.ip : null;
    const whip = p.ip > 0 ? (p.bbAll + p.hAll) / p.ip : null;
    const obpDenom = p.ab + p.bb + p.hbp + p.sf;
    const obp = obpDenom > 0 ? (p.h + p.bb + p.hbp) / obpDenom : (p.pa > 0 ? (p.h + p.bb) / p.pa : null);

    const eraDamage = p.ip > 0 ? (p.er - (p.ip * teamBaseEra / 9)) : 0;
    const whipDamage = p.ip > 0 ? ((p.bbAll + p.hAll) - (p.ip * teamBaseWhip)) : 0;

    const onBaseEvents = p.h + p.bb + p.hbp;
    const expectedOnBase = p.pa * teamBaseObp;
    const obpDrag = p.pa > 0 ? (expectedOnBase - onBaseEvents) : 0;

    p.outings.sort((a, b) => b.scoringPeriodId - a.scoringPeriodId);

    return {
      ...p,
      era,
      whip,
      obp,
      eraDamage,
      whipDamage,
      obpDrag,
      svHd: p.sv + p.hd
    };
  });
}

/**
 * Computes league-wide benchmark rates across all human teams.
 */
export function computeLeagueAverages(baselineTeamStatsMap = {}) {
  let totIp = 0;
  let totEr = 0;
  let totBb = 0;
  let totH = 0;
  let totK = 0;

  let totPa = 0;
  let totR = 0;
  let totHr = 0;
  let totRbi = 0;
  let totSb = 0;
  let totObpNum = 0;
  let totObpDenom = 0;

  Object.values(baselineTeamStatsMap).forEach(st => {
    totIp += parseFloat(st.IP) || 0;
    totEr += parseFloat(st.ER) || 0;
    totBb += parseFloat(st.BB_Allowed) || 0;
    totH += parseFloat(st.H_Allowed) || 0;
    totK += parseFloat(st.K) || 0;

    totPa += parseFloat(st.PA) || 0;
    totR += parseFloat(st.R) || 0;
    totHr += parseFloat(st.HR) || 0;
    totRbi += parseFloat(st.RBI) || 0;
    totSb += parseFloat(st.SB) || 0;
    totObpNum += parseFloat(st.OBP_num) || 0;
    totObpDenom += parseFloat(st.OBP_denom) || 0;
  });

  const era = totIp > 0 ? (totEr * 9) / totIp : 4.15;
  const whip = totIp > 0 ? (totBb + totH) / totIp : 1.25;
  const k9 = totIp > 0 ? (totK * 9) / totIp : 8.5;

  const obp = totObpDenom > 0 ? totObpNum / totObpDenom : 0.320;
  const rPerPa = totPa > 0 ? totR / totPa : 0.125;
  const hrPerPa = totPa > 0 ? totHr / totPa : 0.033;
  const rbiPerPa = totPa > 0 ? totRbi / totPa : 0.120;
  const sbPerPa = totPa > 0 ? totSb / totPa : 0.025;

  return {
    era, whip, k9,
    obp, rPerPa, hrPerPa, rbiPerPa, sbPerPa
  };
}

/**
 * Calculates simulated team totals and simulated rotisserie standings.
 */
export function runImpactSimulation({
  rawActive = [],
  isRecordExcluded,
  pitcherReplacementMode = 'empty',
  customPitcherRates = { era: 4.5, whip: 1.35, k9: 8.0 },
  batterReplacementMode = 'empty',
  customBatterRates = { obp: 0.325, hrPer600: 20, rPer600: 65, rbiPer600: 70, sbPer600: 12 },
  leagueAverages,
  baselineTeamStatsMap,
  selectedTeamId
}) {
  const keptRecords = [];
  const excludedRecords = [];

  rawActive.forEach(r => {
    if (isRecordExcluded(r)) {
      excludedRecords.push(r);
    } else {
      keptRecords.push(r);
    }
  });

  const pureKeptStats = aggregateStats(keptRecords);

  let vacatedIp = 0;
  let vacatedEr = 0;
  let vacatedBbAll = 0;
  let vacatedHAll = 0;
  let vacatedK = 0;
  let vacatedQs = 0;
  let vacatedSvHds = 0;
  let vacatedPa = 0;
  let vacatedAb = 0;
  let vacatedH = 0;
  let vacatedR = 0;
  let vacatedHr = 0;
  let vacatedRbi = 0;
  let vacatedSb = 0;

  excludedRecords.forEach(r => {
    const p = parseRecord(r);
    vacatedIp += p.ip;
    vacatedEr += p.er;
    vacatedBbAll += p.bbAll;
    vacatedHAll += p.hAll;
    vacatedK += p.k;
    vacatedQs += p.qs;
    vacatedSvHds += p.svHd;
    vacatedPa += p.pa;
    vacatedAb += p.ab;
    vacatedH += p.h;
    vacatedR += p.r;
    vacatedHr += p.hr;
    vacatedRbi += p.rbi;
    vacatedSb += p.sb;
  });

  // Pitcher Replacement Calculations
  let repEr = 0;
  let repBbH = 0;
  let repK = 0;

  if (pitcherReplacementMode === 'league_avg') {
    repEr = vacatedIp * (leagueAverages.era / 9);
    repBbH = vacatedIp * leagueAverages.whip;
    repK = vacatedIp * (leagueAverages.k9 / 9);
  } else if (pitcherReplacementMode === 'replacement_level') {
    repEr = vacatedIp * (4.65 / 9);
    repBbH = vacatedIp * 1.38;
    repK = vacatedIp * (7.5 / 9);
  } else if (pitcherReplacementMode === 'custom') {
    repEr = vacatedIp * (customPitcherRates.era / 9);
    repBbH = vacatedIp * customPitcherRates.whip;
    repK = vacatedIp * (customPitcherRates.k9 / 9);
  }

  // Batter Replacement Calculations
  let repR = 0;
  let repHr = 0;
  let repRbi = 0;
  let repSb = 0;
  let repObpNum = 0;
  const repObpDenom = vacatedPa;

  if (batterReplacementMode !== 'empty' && vacatedPa > 0) {
    let rRate = 0;
    let hrRate = 0;
    let rbiRate = 0;
    let sbRate = 0;
    let obpRate = 0.320;

    if (batterReplacementMode === 'league_avg') {
      rRate = leagueAverages.rPerPa;
      hrRate = leagueAverages.hrPerPa;
      rbiRate = leagueAverages.rbiPerPa;
      sbRate = leagueAverages.sbPerPa;
      obpRate = leagueAverages.obp;
    } else if (batterReplacementMode === 'replacement_level') {
      rRate = 50 / 600;
      hrRate = 14 / 600;
      rbiRate = 50 / 600;
      sbRate = 5 / 600;
      obpRate = 0.300;
    } else if (batterReplacementMode === 'power_slugger') {
      rRate = 75 / 600;
      hrRate = 28 / 600;
      rbiRate = 85 / 600;
      sbRate = 2 / 600;
      obpRate = 0.330;
    } else if (batterReplacementMode === 'speedster') {
      rRate = 80 / 600;
      hrRate = 8 / 600;
      rbiRate = 35 / 600;
      sbRate = 35 / 600;
      obpRate = 0.335;
    } else if (batterReplacementMode === 'contact') {
      rRate = 70 / 600;
      hrRate = 12 / 600;
      rbiRate = 55 / 600;
      sbRate = 10 / 600;
      obpRate = 0.365;
    } else if (batterReplacementMode === 'custom') {
      rRate = customBatterRates.rPer600 / 600;
      hrRate = customBatterRates.hrPer600 / 600;
      rbiRate = customBatterRates.rbiPer600 / 600;
      sbRate = customBatterRates.sbPer600 / 600;
      obpRate = customBatterRates.obp;
    }

    repR = vacatedPa * rRate;
    repHr = vacatedPa * hrRate;
    repRbi = vacatedPa * rbiRate;
    repSb = vacatedPa * sbRate;
    repObpNum = vacatedPa * obpRate;
  }

  const finalTotals = { ...pureKeptStats };

  if (pitcherReplacementMode !== 'empty') {
    finalTotals.IP += vacatedIp;
    finalTotals.ER += repEr;
    finalTotals.BB_Allowed += repBbH * 0.32;
    finalTotals.H_Allowed += repBbH * 0.68;
    finalTotals.K += repK;
  }

  if (batterReplacementMode !== 'empty') {
    finalTotals.R += repR;
    finalTotals.HR += repHr;
    finalTotals.RBI += repRbi;
    finalTotals.SB += repSb;
    finalTotals.PA = (finalTotals.PA || 0) + vacatedPa;
    finalTotals.OBP_num = (finalTotals.OBP_num || 0) + repObpNum;
    finalTotals.OBP_denom = (finalTotals.OBP_denom || 0) + repObpDenom;
  }

  const simIp = finalTotals.IP;
  finalTotals.ERA = simIp > 0 ? ((finalTotals.ER * 9) / simIp).toFixed(2) : "0.00";
  finalTotals.ERA_raw = simIp > 0 ? (finalTotals.ER * 9) / simIp : 0;
  finalTotals.WHIP = simIp > 0 ? ((finalTotals.BB_Allowed + finalTotals.H_Allowed) / simIp).toFixed(2) : "0.00";
  finalTotals.WHIP_raw = simIp > 0 ? (finalTotals.BB_Allowed + finalTotals.H_Allowed) / simIp : 0;

  const obpDenom = finalTotals.OBP_denom || finalTotals.PA || 0;
  const obpNum = finalTotals.OBP_num || 0;
  const simObpRaw = obpDenom > 0 ? obpNum / obpDenom : 0;
  finalTotals.OBP = obpDenom > 0 ? simObpRaw.toFixed(4) : ".0000";
  finalTotals.OBP_raw = simObpRaw;

  const simMap = { ...baselineTeamStatsMap };
  simMap[selectedTeamId] = finalTotals;

  const simRotoPoints = calculateRotoPoints(simMap);

  return {
    simulatedTeamStats: finalTotals,
    simulatedTeamStatsMap: simMap,
    simulatedRotoPoints: simRotoPoints,
    vacated: {
      vacatedIp, vacatedEr, vacatedBbAll, vacatedHAll, vacatedK, vacatedQs, vacatedSvHds,
      vacatedPa, vacatedAb, vacatedH, vacatedR, vacatedHr, vacatedRbi, vacatedSb
    },
    excludedCount: excludedRecords.length
  };
}

/**
 * Formats a category number for display.
 */
export function formatCatNum(catKey, n) {
  if (catKey === 'ERA' || catKey === 'WHIP') return Number(n).toFixed(2);
  if (catKey === 'OBP') return Number(n).toFixed(3).replace(/^0/, '');
  return Math.round(Number(n));
}

/**
 * Formats a delta differential with arrow signs for display.
 */
export function formatDelta(catKey, diff) {
  if (Math.abs(diff) < 0.0001) return '0';
  if (catKey === 'ERA' || catKey === 'WHIP') {
    const sign = diff > 0 ? '▲ +' : '▼ ';
    return `${sign}${Math.abs(diff).toFixed(2)}`;
  }
  if (catKey === 'OBP') {
    const sign = diff > 0 ? '▲ +' : '▼ ';
    return `${sign}${Math.abs(diff).toFixed(3).replace(/^0/, '')}`;
  }
  const sign = diff > 0 ? '+' : '';
  return `${sign}${Math.round(diff)}`;
}
