// src/views/PlayerImpactSimulatorView.jsx
import React, { useState, useMemo, useCallback } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, calculateRotoPoints, SCORING_CATS, getStatMeta } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

const ESPN_STAT_NAMES = {
  '0': 'AB', '1': 'H', '3': '2B', '4': '3B', '5': 'HR',
  '10': 'BB', '12': 'HBP', '13': 'SF', '16': 'PA', '17': 'OBP',
  '20': 'R', '21': 'RBI', '23': 'SB', '24': 'CS',
  '33': 'GS', '34': 'IP', '37': 'H_Allowed', '39': 'BB_Allowed',
  '44': 'R_Allowed', '45': 'ER', '48': 'K', '53': 'W', '54': 'L',
  '57': 'SV', '60': 'HD', '63': 'QS'
};

const BATTING_CATS = ['R', 'HR', 'RBI', 'OBP', 'SB'];
const PITCHING_CATS = ['K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

function parseRecord(record) {
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

export default function PlayerImpactSimulatorView({
  allStats = [],
  selectedSeason = 2026,
  initialTeamId = 5,
  onOwnerClick,
  onPlayerClick
}) {
  const [selectedTeamId, setSelectedTeamId] = useState(initialTeamId);
  const [playerFilter, setPlayerFilter] = useState('pitchers'); // 'all' | 'pitchers' | 'batters' | 'streamers'
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('era_damage'); // 'era_damage' | 'obp_drag' | 'er' | 'ip' | 'pa' | 'lowest_obp' | 'apps' | 'name'

  // Exclusions: Set of excluded player IDs (full removal) & Set of excluded outing keys (partial removal)
  const [excludedPlayerIds, setExcludedPlayerIds] = useState(new Set());
  const [excludedOutingKeys, setExcludedOutingKeys] = useState(new Set());

  // Expanded players for start-by-start outing drilldown
  const [expandedPlayerIds, setExpandedPlayerIds] = useState(new Set());

  // Independent replacement modeling modes
  // Pitcher: 'empty' | 'league_avg' | 'replacement_level' | 'custom'
  const [pitcherReplacementMode, setPitcherReplacementMode] = useState('empty');
  const [customPitcherRates, setCustomPitcherRates] = useState({
    era: 4.50,
    whip: 1.35,
    k9: 8.0
  });

  // Batter: 'empty' | 'league_avg' | 'replacement_level' | 'power_slugger' | 'speedster' | 'contact' | 'custom'
  const [batterReplacementMode, setBatterReplacementMode] = useState('empty');
  const [customBatterRates, setCustomBatterRates] = useState({
    obp: 0.325,
    hrPer600: 20,
    rPer600: 65,
    rbiPer600: 70,
    sbPer600: 12
  });

  const humanTeams = useMemo(() => {
    return Object.values(TEAMS).filter(t => t.id !== 99);
  }, []);

  const activeTeamObj = useMemo(() => {
    return TEAMS[selectedTeamId] || { id: selectedTeamId, name: 'Team', owner: 'Manager' };
  }, [selectedTeamId]);

  // 1. Group active records by team for all 9 teams
  const teamActiveRecords = useMemo(() => {
    const groups = {};
    humanTeams.forEach(t => { groups[t.id] = []; });
    allStats.forEach(r => {
      if (!groups[r.team_id]) return;
      // Active roster only (exclude bench and IL)
      if (r.lineup_slot_id === 16 || r.lineup_slot_id === 17) return;
      groups[r.team_id].push(r);
    });
    return groups;
  }, [allStats, humanTeams]);

  // 2. Baseline stats for all teams and baseline roto points
  const baselineTeamStatsMap = useMemo(() => {
    const map = {};
    humanTeams.forEach(t => {
      map[t.id] = aggregateStats(teamActiveRecords[t.id] || []);
    });
    return map;
  }, [teamActiveRecords, humanTeams]);

  const baselineRotoPoints = useMemo(() => {
    return calculateRotoPoints(baselineTeamStatsMap);
  }, [baselineTeamStatsMap]);

  // 3. Aggregate active player statistics for the selected team
  const playerProfiles = useMemo(() => {
    const records = teamActiveRecords[selectedTeamId] || [];
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
          // Pitching cumulative
          ip: 0, er: 0, bbAll: 0, hAll: 0, k: 0, qs: 0, sv: 0, hd: 0, w: 0, l: 0,
          // Batting cumulative
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

      // Pitching ERA Damage: How much did this pitcher inflate team ER above baseline expectation?
      const eraDamage = p.ip > 0 ? (p.er - (p.ip * teamBaseEra / 9)) : 0;
      const whipDamage = p.ip > 0 ? ((p.bbAll + p.hAll) - (p.ip * teamBaseWhip)) : 0;

      // Batting OBP Drag: How many on-base appearances did this batter cost the team relative to baseline?
      const onBaseEvents = p.h + p.bb + p.hbp;
      const expectedOnBase = p.pa * teamBaseObp;
      const obpDrag = p.pa > 0 ? (expectedOnBase - onBaseEvents) : 0;

      // Sort outings chronologically (latest first)
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
  }, [teamActiveRecords, selectedTeamId, baselineTeamStatsMap]);

  // League-wide averages for replacement benchmark
  const leagueAverages = useMemo(() => {
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
  }, [baselineTeamStatsMap]);

  // 4. Determine which records are excluded by active player/outing filters
  const isRecordExcluded = useCallback((record) => {
    if (excludedPlayerIds.has(record.player_id)) return true;
    const key = `${record.player_id}_${record.scoring_period_id}`;
    if (excludedOutingKeys.has(key)) return true;
    return false;
  }, [excludedPlayerIds, excludedOutingKeys]);

  // 5. Calculate simulated active team records and aggregate stats
  const simulationResults = useMemo(() => {
    const rawActive = teamActiveRecords[selectedTeamId] || [];
    const keptRecords = [];
    const excludedRecords = [];

    rawActive.forEach(r => {
      if (isRecordExcluded(r)) {
        excludedRecords.push(r);
      } else {
        keptRecords.push(r);
      }
    });

    // Baseline stats of kept records
    const pureKeptStats = aggregateStats(keptRecords);

    // Sum of vacated metrics
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

    // A. Pitcher Replacement Calculations
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

    // B. Batter Replacement Calculations
    let repR = 0;
    let repHr = 0;
    let repRbi = 0;
    let repSb = 0;
    let repObpNum = 0;
    let repObpDenom = vacatedPa;

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

    // Combine kept stats + replacement stats
    const finalTotals = { ...pureKeptStats };

    if (pitcherReplacementMode !== 'empty') {
      finalTotals.IP += vacatedIp;
      finalTotals.ER += repEr;
      finalTotals.BB_Allowed += repBbH * 0.32; // estimate ~32% BB
      finalTotals.H_Allowed += repBbH * 0.68;  // estimate ~68% H
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

    // Recompute rate stats
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

    // Build simulated league map and roto points
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
  }, [
    teamActiveRecords,
    selectedTeamId,
    isRecordExcluded,
    pitcherReplacementMode,
    batterReplacementMode,
    leagueAverages,
    customPitcherRates,
    customBatterRates,
    baselineTeamStatsMap
  ]);

  // Derived baseline vs simulated totals
  const baseTeamStats = baselineTeamStatsMap[selectedTeamId] || {};
  const simTeamStats = simulationResults.simulatedTeamStats || {};
  const baseRoto = baselineRotoPoints[selectedTeamId] || { total: 0 };
  const simRoto = simulationResults.simulatedRotoPoints[selectedTeamId] || { total: 0 };

  // Calculate league rank before & after
  const leagueStandingsComparison = useMemo(() => {
    const baseStandings = humanTeams.map(t => ({
      ...t,
      rotoTotal: baselineRotoPoints[t.id]?.total || 0,
      isTarget: t.id === selectedTeamId
    })).sort((a, b) => b.rotoTotal - a.rotoTotal);

    const simStandings = humanTeams.map(t => ({
      ...t,
      rotoTotal: simulationResults.simulatedRotoPoints[t.id]?.total || 0,
      isTarget: t.id === selectedTeamId
    })).sort((a, b) => b.rotoTotal - a.rotoTotal);

    const baseRank = baseStandings.findIndex(t => t.id === selectedTeamId) + 1;
    const simRank = simStandings.findIndex(t => t.id === selectedTeamId) + 1;

    return {
      baseStandings,
      simStandings,
      baseRank,
      simRank,
      rankDelta: baseRank - simRank // positive means improved rank!
    };
  }, [humanTeams, baselineRotoPoints, simulationResults.simulatedRotoPoints, selectedTeamId]);

  // Filter and sort player profiles
  const filteredPlayers = useMemo(() => {
    let list = [...playerProfiles];

    if (playerFilter === 'pitchers') {
      list = list.filter(p => p.isPitcher);
    } else if (playerFilter === 'batters') {
      list = list.filter(p => p.isBatter);
    } else if (playerFilter === 'streamers') {
      list = list.filter(p => p.isPitcher && p.activeAppearances <= 4);
    }

    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase().trim();
      list = list.filter(p => p.fullName.toLowerCase().includes(q));
    }

    list.sort((a, b) => {
      if (sortBy === 'era_damage') return b.eraDamage - a.eraDamage;
      if (sortBy === 'obp_drag') return b.obpDrag - a.obpDrag;
      if (sortBy === 'lowest_obp') {
        const aObp = (a.pa >= 15 && a.obp !== null) ? a.obp : 1;
        const bObp = (b.pa >= 15 && b.obp !== null) ? b.obp : 1;
        return aObp - bObp;
      }
      if (sortBy === 'er') return b.er - a.er;
      if (sortBy === 'ip') return b.ip - a.ip;
      if (sortBy === 'pa') return b.pa - a.pa;
      if (sortBy === 'apps') return b.activeAppearances - a.activeAppearances;
      if (sortBy === 'name') return a.fullName.localeCompare(b.fullName);
      return 0;
    });

    return list;
  }, [playerProfiles, playerFilter, searchQuery, sortBy]);

  // Quick Action Presets
  const handleExcludeTop3EraKillers = () => {
    const top3 = [...playerProfiles]
      .filter(p => p.isPitcher && p.eraDamage > 0)
      .sort((a, b) => b.eraDamage - a.eraDamage)
      .slice(0, 3)
      .map(p => p.playerId);

    setExcludedPlayerIds(new Set(top3));
    setExcludedOutingKeys(new Set());
  };

  const handleExcludeSubInningMeltdowns = () => {
    const disasterOutingKeys = new Set();
    playerProfiles.forEach(p => {
      p.outings.forEach(out => {
        if (out.isPitcher && out.ip < 1.0 && out.er >= 2.0) {
          disasterOutingKeys.add(`${out.playerId}_${out.scoringPeriodId}`);
        }
      });
    });
    setExcludedOutingKeys(disasterOutingKeys);
  };

  const handleExcludeTop3ObpAnchors = () => {
    const top3 = [...playerProfiles]
      .filter(p => p.isBatter && p.obpDrag > 0)
      .sort((a, b) => b.obpDrag - a.obpDrag)
      .slice(0, 3)
      .map(p => p.playerId);

    setExcludedPlayerIds(new Set(top3));
    setExcludedOutingKeys(new Set());
  };

  const handleExcludeHitlessSlumpDays = () => {
    const collarKeys = new Set();
    playerProfiles.forEach(p => {
      p.outings.forEach(out => {
        if (out.isBatter && out.ab >= 3 && out.h === 0 && out.bb === 0) {
          collarKeys.add(`${out.playerId}_${out.scoringPeriodId}`);
        }
      });
    });
    setExcludedOutingKeys(collarKeys);
  };

  const handleClearAllExclusions = () => {
    setExcludedPlayerIds(new Set());
    setExcludedOutingKeys(new Set());
  };

  const togglePlayerExclusion = (playerId) => {
    setExcludedPlayerIds(prev => {
      const next = new Set(prev);
      if (next.has(playerId)) next.delete(playerId);
      else next.add(playerId);
      return next;
    });
  };

  const toggleOutingExclusion = (playerId, scoringPeriodId) => {
    const key = `${playerId}_${scoringPeriodId}`;
    setExcludedOutingKeys(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const toggleAccordion = (playerId) => {
    setExpandedPlayerIds(prev => {
      const next = new Set(prev);
      if (next.has(playerId)) next.delete(playerId);
      else next.add(playerId);
      return next;
    });
  };

  const handleFilterTabClick = (filter) => {
    setPlayerFilter(filter);
    if (filter === 'batters' && sortBy === 'era_damage') {
      setSortBy('obp_drag');
    } else if ((filter === 'pitchers' || filter === 'streamers') && sortBy === 'obp_drag') {
      setSortBy('era_damage');
    }
  };

  // Deltas for display
  const rotoPointsDiff = simRoto.total - baseRoto.total;
  const rankDiff = leagueStandingsComparison.rankDelta;
  const totalExclusionsCount = excludedPlayerIds.size + excludedOutingKeys.size;

  // Category counts and splits
  const allCategoryKeys = Object.keys(SCORING_CATS);
  const categoriesUp = allCategoryKeys.filter(c => (simRoto[c] || 0) > (baseRoto[c] || 0) + 0.001).length;
  const categoriesDown = allCategoryKeys.filter(c => (simRoto[c] || 0) < (baseRoto[c] || 0) - 0.001).length;
  const categoriesEven = 10 - categoriesUp - categoriesDown;

  const battingPointsDelta = BATTING_CATS.reduce((sum, c) => sum + ((simRoto[c] || 0) - (baseRoto[c] || 0)), 0);
  const pitchingPointsDelta = PITCHING_CATS.reduce((sum, c) => sum + ((simRoto[c] || 0) - (baseRoto[c] || 0)), 0);

  // Helper to format category numbers
  const formatCatNum = (catKey, n) => {
    if (catKey === 'ERA' || catKey === 'WHIP') return Number(n).toFixed(2);
    if (catKey === 'OBP') return Number(n).toFixed(3).replace(/^0/, '');
    return Math.round(Number(n));
  };

  const formatDelta = (catKey, diff) => {
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
  };

  return (
    <div className="space-y-6">
      {/* 1. VIEW HEADER & TEAM SELECTOR */}
      <div className="bg-gradient-to-r from-slate-900 via-indigo-950 to-blue-950 text-white p-6 rounded-3xl shadow-lg border border-slate-800 space-y-4">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-2.5">
              <span className="text-3xl">🧪</span>
              <div>
                <h2 className="text-2xl font-black tracking-tight">What-If Player Impact Simulator</h2>
                <p className="text-xs text-indigo-200 mt-0.5">
                  Model team statistics and league standings without specific players or disaster spot-starts.
                </p>
              </div>
            </div>
          </div>

          {/* Team Switcher */}
          <div className="flex items-center gap-2 bg-white/10 backdrop-blur p-1.5 rounded-2xl border border-white/10 self-start md:self-auto">
            <span className="text-xs font-bold text-indigo-200 pl-2">Team:</span>
            <select
              value={selectedTeamId}
              onChange={(e) => {
                setSelectedTeamId(Number(e.target.value));
                handleClearAllExclusions();
              }}
              className="bg-slate-900/90 text-white font-black text-xs px-3 py-1.5 rounded-xl border border-indigo-400/40 focus:outline-none focus:ring-2 focus:ring-blue-400 cursor-pointer"
            >
              {humanTeams.map(t => (
                <option key={t.id} value={t.id}>
                  {t.name} ({t.owner})
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Quick Exclusions Status & Action Presets */}
        <div className="flex flex-wrap items-center justify-between gap-3 pt-3 border-t border-white/10 text-xs">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-bold text-gray-300">Quick Presets:</span>
            <button
              onClick={handleExcludeTop3EraKillers}
              className="px-2.5 py-1 rounded-lg bg-rose-500/20 text-rose-300 border border-rose-500/40 hover:bg-rose-500/30 transition font-bold"
              title="Exclude top 3 pitchers causing highest ERA inflation"
            >
              💥 Top 3 ERA Killers
            </button>
            <button
              onClick={handleExcludeSubInningMeltdowns}
              className="px-2.5 py-1 rounded-lg bg-amber-500/20 text-amber-300 border border-amber-500/40 hover:bg-amber-500/30 transition font-bold"
              title="Exclude all pitching outings with < 1 IP and >= 2 ER"
            >
              🛑 Pitching Meltdowns (&lt;1 IP)
            </button>
            <button
              onClick={handleExcludeTop3ObpAnchors}
              className="px-2.5 py-1 rounded-lg bg-orange-500/20 text-orange-300 border border-orange-500/40 hover:bg-orange-500/30 transition font-bold"
              title="Exclude top 3 batters with highest OBP drag"
            >
              📉 Top 3 OBP Anchors
            </button>
            <button
              onClick={handleExcludeHitlessSlumpDays}
              className="px-2.5 py-1 rounded-lg bg-cyan-500/20 text-cyan-300 border border-cyan-500/40 hover:bg-cyan-500/30 transition font-bold"
              title="Exclude all batting outings with 0 H, 0 BB, and at least 3 AB"
            >
              ❄️ 0-fer Slump Days (0 H, 0 BB)
            </button>
            {totalExclusionsCount > 0 && (
              <button
                onClick={handleClearAllExclusions}
                className="px-2.5 py-1 rounded-lg bg-gray-700/80 text-gray-200 hover:bg-gray-700 transition font-bold"
              >
                🔄 Reset All ({totalExclusionsCount})
              </button>
            )}
          </div>

          <div className="font-mono text-indigo-200">
            Active Exclusions: <span className="font-black text-white">{excludedPlayerIds.size}</span> players, <span className="font-black text-white">{excludedOutingKeys.size}</span> single outings
          </div>
        </div>
      </div>

      {/* 2. FULL 10-CATEGORY ROTISSERIE SUMMARY DASHBOARD */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-5">
        {/* High-Level Impact Bar */}
        <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4 border-b border-gray-100 pb-4">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-xl">📊</span>
              <h3 className="text-lg font-black text-gray-900">
                Full Rotisserie Summary ({activeTeamObj.name})
              </h3>
            </div>
            <p className="text-xs text-gray-500 mt-0.5">
              Instant snapshot across all 10 rotisserie categories under the counterfactual simulation.
            </p>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            {/* Standings Rank Card */}
            <div className="bg-purple-50/80 border border-purple-200 rounded-2xl px-3.5 py-2 flex items-center gap-3">
              <div>
                <div className="text-[10px] font-bold text-purple-700 uppercase tracking-wide">League Rank</div>
                <div className="text-xl font-black text-purple-950 font-mono">
                  {leagueStandingsComparison.simRank}
                  <span className="text-xs font-bold text-purple-600 ml-0.5">
                    {leagueStandingsComparison.simRank === 1 ? 'st 👑' :
                     leagueStandingsComparison.simRank === 2 ? 'nd' :
                     leagueStandingsComparison.simRank === 3 ? 'rd' : 'th'}
                  </span>
                </div>
              </div>
              <span className={`text-xs font-black font-mono px-2 py-0.5 rounded-lg ${
                rankDiff > 0 ? 'bg-emerald-100 text-emerald-800' :
                rankDiff < 0 ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-600'
              }`}>
                {rankDiff > 0 ? `▲ +${rankDiff}` : rankDiff < 0 ? `▼ ${rankDiff}` : 'Even'}
              </span>
            </div>

            {/* Total Points Card */}
            <div className="bg-blue-50/80 border border-blue-200 rounded-2xl px-3.5 py-2 flex items-center gap-3">
              <div>
                <div className="text-[10px] font-bold text-blue-700 uppercase tracking-wide">Total Roto Pts</div>
                <div className="text-xl font-black text-blue-950 font-mono">
                  {simRoto.total.toFixed(1)}
                  <span className="text-xs text-gray-400 font-normal ml-1">/ 90</span>
                </div>
              </div>
              <span className={`text-xs font-black font-mono px-2 py-0.5 rounded-lg ${
                rotoPointsDiff > 0.001 ? 'bg-emerald-100 text-emerald-800' :
                rotoPointsDiff < -0.001 ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-600'
              }`}>
                {rotoPointsDiff > 0 ? '+' : ''}{rotoPointsDiff.toFixed(1)} pts
              </span>
            </div>

            {/* Shift Breakdown Pills */}
            <div className="flex items-center gap-1.5 bg-gray-50 border border-gray-200 rounded-2xl p-2 text-xs font-bold font-mono">
              <span className="px-2 py-1 bg-emerald-100/80 text-emerald-800 rounded-lg" title="Categories gained">
                🟢 {categoriesUp} Up
              </span>
              <span className="px-2 py-1 bg-rose-100/80 text-rose-800 rounded-lg" title="Categories lost">
                🔴 {categoriesDown} Down
              </span>
              <span className="px-2 py-1 bg-gray-200/80 text-gray-700 rounded-lg" title="Categories unchanged">
                ⚪ {categoriesEven} Even
              </span>
            </div>
          </div>
        </div>

        {/* 10 Category Grid Layout */}
        <div className="space-y-4">
          {/* Batting Header & 5 Cards */}
          <div>
            <div className="flex items-center justify-between text-xs font-black uppercase text-amber-800 tracking-wider mb-2">
              <div className="flex items-center gap-1.5">
                <span>⚾</span>
                <span>Batting Categories (5)</span>
              </div>
              <span className={`font-mono text-[11px] px-2 py-0.5 rounded ${
                battingPointsDelta > 0.001 ? 'bg-emerald-100 text-emerald-800' :
                battingPointsDelta < -0.001 ? 'bg-rose-100 text-rose-800' : 'text-gray-400'
              }`}>
                Net: {battingPointsDelta > 0 ? '+' : ''}{battingPointsDelta.toFixed(1)} Pts
              </span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
              {BATTING_CATS.map(catKey => {
                const meta = getStatMeta(catKey);
                const isLow = SCORING_CATS[catKey]?.type === 'low';

                const baseValRaw = baseTeamStats[`${catKey}_raw`] ?? baseTeamStats[catKey];
                const simValRaw = simTeamStats[`${catKey}_raw`] ?? simTeamStats[catKey];

                const baseNum = parseFloat(baseValRaw) || 0;
                const simNum = parseFloat(simValRaw) || 0;
                const diff = simNum - baseNum;

                const basePts = baseRoto[catKey] ?? 0;
                const simPts = simRoto[catKey] ?? 0;
                const ptsDiff = simPts - basePts;

                const isBeneficial = isLow ? diff < -0.0001 : diff > 0.0001;
                const isDetrimental = isLow ? diff > 0.0001 : diff < -0.0001;

                return (
                  <div
                    key={`top-cat-${catKey}`}
                    className={`p-3 rounded-2xl border transition-all ${
                      ptsDiff > 0.001
                        ? 'bg-emerald-50/70 border-emerald-300 ring-1 ring-emerald-200'
                        : ptsDiff < -0.001
                        ? 'bg-rose-50/70 border-rose-300 ring-1 ring-rose-200'
                        : 'bg-slate-50/70 border-gray-200'
                    }`}
                  >
                    <div className="flex items-center justify-between text-[11px] font-bold text-gray-500 mb-1">
                      <span className="text-gray-900 font-black">{meta.label}</span>
                      <span className="text-[10px] text-gray-400">{catKey === 'OBP' ? 'Rate' : 'Counting'}</span>
                    </div>

                    <div className="flex items-baseline justify-between mt-1">
                      <div>
                        <div className="text-lg font-black text-gray-900 font-mono">
                          {formatCatNum(catKey, simNum)}
                        </div>
                        <div className="text-[10px] text-gray-400 font-mono">
                          Base: {formatCatNum(catKey, baseNum)}
                        </div>
                      </div>

                      <div className="text-right">
                        <span className={`inline-block text-[10px] font-black font-mono px-1.5 py-0.5 rounded ${
                          isBeneficial ? 'bg-emerald-100 text-emerald-800' :
                          isDetrimental ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-500'
                        }`}>
                          {formatDelta(catKey, diff)}
                        </span>
                      </div>
                    </div>

                    <div className="mt-2 pt-2 border-t border-gray-200/60 flex items-center justify-between text-[11px] font-mono">
                      <span className="font-bold text-blue-900">{simPts.toFixed(1)} pts</span>
                      <span className={`font-black text-[10px] px-1.5 py-0.2 rounded ${
                        ptsDiff > 0.001 ? 'bg-emerald-200 text-emerald-900 font-bold' :
                        ptsDiff < -0.001 ? 'bg-rose-200 text-rose-900 font-bold' : 'text-gray-400'
                      }`}>
                        {ptsDiff > 0 ? `+${ptsDiff.toFixed(1)}` : ptsDiff < 0 ? ptsDiff.toFixed(1) : 'Even'}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Pitching Header & 5 Cards */}
          <div>
            <div className="flex items-center justify-between text-xs font-black uppercase text-indigo-800 tracking-wider mb-2">
              <div className="flex items-center gap-1.5">
                <span>🎯</span>
                <span>Pitching Categories (5)</span>
              </div>
              <span className={`font-mono text-[11px] px-2 py-0.5 rounded ${
                pitchingPointsDelta > 0.001 ? 'bg-emerald-100 text-emerald-800' :
                pitchingPointsDelta < -0.001 ? 'bg-rose-100 text-rose-800' : 'text-gray-400'
              }`}>
                Net: {pitchingPointsDelta > 0 ? '+' : ''}{pitchingPointsDelta.toFixed(1)} Pts
              </span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
              {PITCHING_CATS.map(catKey => {
                const meta = getStatMeta(catKey);
                const isLow = SCORING_CATS[catKey]?.type === 'low';

                const baseValRaw = baseTeamStats[`${catKey}_raw`] ?? baseTeamStats[catKey];
                const simValRaw = simTeamStats[`${catKey}_raw`] ?? simTeamStats[catKey];

                const baseNum = parseFloat(baseValRaw) || 0;
                const simNum = parseFloat(simValRaw) || 0;
                const diff = simNum - baseNum;

                const basePts = baseRoto[catKey] ?? 0;
                const simPts = simRoto[catKey] ?? 0;
                const ptsDiff = simPts - basePts;

                const isBeneficial = isLow ? diff < -0.0001 : diff > 0.0001;
                const isDetrimental = isLow ? diff > 0.0001 : diff < -0.0001;

                return (
                  <div
                    key={`top-cat-${catKey}`}
                    className={`p-3 rounded-2xl border transition-all ${
                      ptsDiff > 0.001
                        ? 'bg-emerald-50/70 border-emerald-300 ring-1 ring-emerald-200'
                        : ptsDiff < -0.001
                        ? 'bg-rose-50/70 border-rose-300 ring-1 ring-rose-200'
                        : 'bg-slate-50/70 border-gray-200'
                    }`}
                  >
                    <div className="flex items-center justify-between text-[11px] font-bold text-gray-500 mb-1">
                      <span className="text-gray-900 font-black">{meta.label}</span>
                      <span className="text-[10px] text-gray-400">{isLow ? 'Lower' : 'Higher'}</span>
                    </div>

                    <div className="flex items-baseline justify-between mt-1">
                      <div>
                        <div className="text-lg font-black text-gray-900 font-mono">
                          {formatCatNum(catKey, simNum)}
                        </div>
                        <div className="text-[10px] text-gray-400 font-mono">
                          Base: {formatCatNum(catKey, baseNum)}
                        </div>
                      </div>

                      <div className="text-right">
                        <span className={`inline-block text-[10px] font-black font-mono px-1.5 py-0.5 rounded ${
                          isBeneficial ? 'bg-emerald-100 text-emerald-800' :
                          isDetrimental ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-500'
                        }`}>
                          {formatDelta(catKey, diff)}
                        </span>
                      </div>
                    </div>

                    <div className="mt-2 pt-2 border-t border-gray-200/60 flex items-center justify-between text-[11px] font-mono">
                      <span className="font-bold text-blue-900">{simPts.toFixed(1)} pts</span>
                      <span className={`font-black text-[10px] px-1.5 py-0.2 rounded ${
                        ptsDiff > 0.001 ? 'bg-emerald-200 text-emerald-900 font-bold' :
                        ptsDiff < -0.001 ? 'bg-rose-200 text-rose-900 font-bold' : 'text-gray-400'
                      }`}>
                        {ptsDiff > 0 ? `+${ptsDiff.toFixed(1)}` : ptsDiff < 0 ? ptsDiff.toFixed(1) : 'Even'}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* 3. REPLACEMENT PLAYER MODELING CONTROLS */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-5">
        <div className="border-b border-gray-100 pb-3">
          <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
            <span>⚖️</span>
            <span>Replacement Player Simulation Modes</span>
          </h3>
          <p className="text-xs text-gray-500 mt-0.5">
            Configure independent replacement assumptions for vacated pitching innings ({simulationResults.vacated.vacatedIp.toFixed(1)} IP) and batting plate appearances ({simulationResults.vacated.vacatedPa} PA).
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 text-xs">
          {/* Pitcher Replacement Section */}
          <div className="space-y-3 p-4 rounded-2xl bg-indigo-50/40 border border-indigo-100">
            <div className="flex items-center justify-between">
              <span className="font-black text-indigo-950 flex items-center gap-1.5">
                <span>🎯</span>
                <span>Pitcher Replacement Strategy</span>
              </span>
              <span className="font-mono text-indigo-700 font-bold text-[11px]">
                {simulationResults.vacated.vacatedIp.toFixed(1)} IP Vacated
              </span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-1 bg-white p-1 rounded-xl border border-indigo-200/60">
              <button
                onClick={() => setPitcherReplacementMode('empty')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  pitcherReplacementMode === 'empty' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                🚫 Empty Slot
              </button>
              <button
                onClick={() => setPitcherReplacementMode('league_avg')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  pitcherReplacementMode === 'league_avg' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                📊 League Avg
              </button>
              <button
                onClick={() => setPitcherReplacementMode('replacement_level')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  pitcherReplacementMode === 'replacement_level' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                ⚖️ Waiver Wire
              </button>
              <button
                onClick={() => setPitcherReplacementMode('custom')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  pitcherReplacementMode === 'custom' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                ✏️ Custom Rates
              </button>
            </div>

            <div className="text-gray-600 text-[11px]">
              {pitcherReplacementMode === 'empty' && (
                <p>Pure omission: leaves vacated slots blank. 0 IP or ER added back.</p>
              )}
              {pitcherReplacementMode === 'league_avg' && (
                <p>
                  Replaces vacated IP at 2026 league mean: <span className="font-mono font-bold text-gray-900">{leagueAverages.era.toFixed(2)} ERA</span>, <span className="font-mono font-bold text-gray-900">{leagueAverages.whip.toFixed(2)} WHIP</span>, <span className="font-mono font-bold text-gray-900">{leagueAverages.k9.toFixed(1)} K/9</span>.
                </p>
              )}
              {pitcherReplacementMode === 'replacement_level' && (
                <p>
                  Replaces vacated IP at standard waiver streamer rates: <span className="font-mono font-bold text-gray-900">4.65 ERA</span>, <span className="font-mono font-bold text-gray-900">1.38 WHIP</span>, <span className="font-mono font-bold text-gray-900">7.5 K/9</span>.
                </p>
              )}
              {pitcherReplacementMode === 'custom' && (
                <div className="grid grid-cols-3 gap-2 pt-1">
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">ERA</label>
                    <input
                      type="number"
                      step="0.1"
                      min="0"
                      max="15"
                      value={customPitcherRates.era}
                      onChange={(e) => setCustomPitcherRates({ ...customPitcherRates, era: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-indigo-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">WHIP</label>
                    <input
                      type="number"
                      step="0.05"
                      min="0"
                      max="3"
                      value={customPitcherRates.whip}
                      onChange={(e) => setCustomPitcherRates({ ...customPitcherRates, whip: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-indigo-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">K/9</label>
                    <input
                      type="number"
                      step="0.5"
                      min="0"
                      max="20"
                      value={customPitcherRates.k9}
                      onChange={(e) => setCustomPitcherRates({ ...customPitcherRates, k9: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-indigo-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Batter Replacement Section */}
          <div className="space-y-3 p-4 rounded-2xl bg-amber-50/40 border border-amber-100">
            <div className="flex items-center justify-between">
              <span className="font-black text-amber-950 flex items-center gap-1.5">
                <span>⚾</span>
                <span>Batter Replacement Strategy & Archetypes</span>
              </span>
              <span className="font-mono text-amber-700 font-bold text-[11px]">
                {simulationResults.vacated.vacatedPa} PA Vacated
              </span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-1 bg-white p-1 rounded-xl border border-amber-200/60">
              <button
                onClick={() => setBatterReplacementMode('empty')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'empty' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                🚫 Empty Slot
              </button>
              <button
                onClick={() => setBatterReplacementMode('league_avg')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'league_avg' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                📊 League Avg
              </button>
              <button
                onClick={() => setBatterReplacementMode('replacement_level')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'replacement_level' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                ⚖️ Waiver Bat
              </button>
              <button
                onClick={() => setBatterReplacementMode('power_slugger')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'power_slugger' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                💥 Slugger
              </button>
              <button
                onClick={() => setBatterReplacementMode('speedster')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'speedster' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                ⚡ Speedster
              </button>
              <button
                onClick={() => setBatterReplacementMode('contact')}
                className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'contact' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                🛡️ High OBP
              </button>
              <button
                onClick={() => setBatterReplacementMode('custom')}
                className={`col-span-2 px-2 py-1.5 font-bold rounded-lg transition text-[11px] ${
                  batterReplacementMode === 'custom' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                ✏️ Custom Batter Profile
              </button>
            </div>

            <div className="text-gray-600 text-[11px]">
              {batterReplacementMode === 'empty' && (
                <p>Pure omission: models leaving the active hitting slot empty or benched (0 stats added).</p>
              )}
              {batterReplacementMode === 'league_avg' && (
                <p>
                  Replaces vacated PA across all categories at 2026 league rates: <span className="font-mono font-bold text-gray-900">.{Math.round(leagueAverages.obp * 1000)} OBP</span>, ~{Math.round(leagueAverages.hrPerPa * 600)} HR, ~{Math.round(leagueAverages.rPerPa * 600)} R, ~{Math.round(leagueAverages.rbiPerPa * 600)} RBI, ~{Math.round(leagueAverages.sbPerPa * 600)} SB per 600 PA.
                </p>
              )}
              {batterReplacementMode === 'replacement_level' && (
                <p>
                  Replaces vacated PA with typical waiver hitter rates: <span className="font-mono font-bold text-gray-900">.300 OBP</span>, 14 HR, 50 R, 50 RBI, 5 SB per 600 PA.
                </p>
              )}
              {batterReplacementMode === 'power_slugger' && (
                <p>
                  Replaces vacated PA with corner slugger rates: <span className="font-mono font-bold text-gray-900">.330 OBP</span>, 28 HR, 75 R, 85 RBI, 2 SB per 600 PA.
                </p>
              )}
              {batterReplacementMode === 'speedster' && (
                <p>
                  Replaces vacated PA with leadoff speedster rates: <span className="font-mono font-bold text-gray-900">.335 OBP</span>, 8 HR, 80 R, 35 RBI, 35 SB per 600 PA.
                </p>
              )}
              {batterReplacementMode === 'contact' && (
                <p>
                  Replaces vacated PA with on-base specialist rates: <span className="font-mono font-bold text-gray-900">.365 OBP</span>, 12 HR, 70 R, 55 RBI, 10 SB per 600 PA.
                </p>
              )}
              {batterReplacementMode === 'custom' && (
                <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 pt-1">
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">OBP</label>
                    <input
                      type="number"
                      step="0.005"
                      min="0.100"
                      max="0.500"
                      value={customBatterRates.obp}
                      onChange={(e) => setCustomBatterRates({ ...customBatterRates, obp: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">HR / 600</label>
                    <input
                      type="number"
                      step="1"
                      min="0"
                      max="70"
                      value={customBatterRates.hrPer600}
                      onChange={(e) => setCustomBatterRates({ ...customBatterRates, hrPer600: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">R / 600</label>
                    <input
                      type="number"
                      step="1"
                      min="0"
                      max="150"
                      value={customBatterRates.rPer600}
                      onChange={(e) => setCustomBatterRates({ ...customBatterRates, rPer600: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">RBI / 600</label>
                    <input
                      type="number"
                      step="1"
                      min="0"
                      max="150"
                      value={customBatterRates.rbiPer600}
                      onChange={(e) => setCustomBatterRates({ ...customBatterRates, rbiPer600: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-bold text-gray-500 mb-0.5">SB / 600</label>
                    <input
                      type="number"
                      step="1"
                      min="0"
                      max="100"
                      value={customBatterRates.sbPer600}
                      onChange={(e) => setCustomBatterRates({ ...customBatterRates, sbPer600: parseFloat(e.target.value) || 0 })}
                      className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                    />
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* 4. INTERACTIVE ROSTER EXCLUSION WORKBENCH */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4">
        {/* Filter / Sort bar */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-gray-100 pb-3">
          <div>
            <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>📋</span>
              <span>Roster & Outing Exclusion Workbench</span>
            </h3>
            <p className="text-xs text-gray-400 mt-0.5">
              Uncheck a player to drop them entirely, or expand to drop individual games/spot-starts.
            </p>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            {/* Position filter pill */}
            <div className="flex items-center gap-1 bg-gray-100 p-1 rounded-xl text-xs">
              <button
                onClick={() => handleFilterTabClick('pitchers')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'pitchers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                Pitchers
              </button>
              <button
                onClick={() => handleFilterTabClick('streamers')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'streamers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                Streamers (≤4)
              </button>
              <button
                onClick={() => handleFilterTabClick('batters')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'batters' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                Batters
              </button>
              <button
                onClick={() => handleFilterTabClick('all')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'all' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                All
              </button>
            </div>

            {/* Sort by dropdown */}
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
              className="px-2.5 py-1 text-xs border border-gray-200 rounded-xl bg-gray-50 focus:bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 font-bold text-gray-700 cursor-pointer"
            >
              {playerFilter === 'batters' ? (
                <>
                  <option value="obp_drag">Sort: Worst OBP Drag</option>
                  <option value="lowest_obp">Sort: Lowest OBP (Min 15 PA)</option>
                  <option value="pa">Sort: Plate Appearances</option>
                  <option value="apps">Sort: Appearances</option>
                  <option value="name">Sort: Name</option>
                </>
              ) : (
                <>
                  <option value="era_damage">Sort: ERA Damage</option>
                  <option value="er">Sort: Most ER</option>
                  <option value="ip">Sort: Innings Pitched</option>
                  <option value="apps">Sort: Appearances</option>
                  <option value="name">Sort: Name</option>
                </>
              )}
            </select>

            {/* Search */}
            <input
              type="text"
              placeholder="Search player..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="px-2.5 py-1 text-xs border border-gray-200 rounded-xl bg-gray-50 focus:bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 w-36"
            />
          </div>
        </div>

        {/* Players List Table */}
        <div className="divide-y divide-gray-100 text-xs">
          {filteredPlayers.length === 0 ? (
            <div className="py-8 text-center text-gray-400">
              No active players found matching the current filters.
            </div>
          ) : (
            filteredPlayers.map(p => {
              const isFullyExcluded = excludedPlayerIds.has(p.playerId);
              const isExpanded = expandedPlayerIds.has(p.playerId);

              // Count how many individual outings are excluded
              const excludedOutingsCount = p.outings.filter(o => 
                isFullyExcluded || excludedOutingKeys.has(`${p.playerId}_${o.scoringPeriodId}`)
              ).length;

              const isPartiallyExcluded = !isFullyExcluded && excludedOutingsCount > 0;

              return (
                <div key={p.playerId} className={`transition-colors ${isFullyExcluded ? 'bg-rose-50/50' : isPartiallyExcluded ? 'bg-amber-50/40' : 'hover:bg-gray-50/60'}`}>
                  {/* Player Summary Row */}
                  <div className="py-3 px-2 flex items-center justify-between gap-3">
                    <div className="flex items-center gap-3 min-w-0">
                      {/* Checkbox */}
                      <input
                        type="checkbox"
                        checked={isFullyExcluded}
                        onChange={() => togglePlayerExclusion(p.playerId)}
                        className="w-4 h-4 rounded text-blue-600 focus:ring-blue-500 cursor-pointer"
                        title={isFullyExcluded ? 'Include player' : 'Exclude player from simulation'}
                      />

                      <div className="min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <span
                            onClick={() => onPlayerClick && onPlayerClick(p.playerId)}
                            className={`font-black text-sm cursor-pointer hover:underline ${isFullyExcluded ? 'line-through text-gray-400' : 'text-gray-900'}`}
                          >
                            {p.fullName}
                          </span>

                          <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                            p.isPitcher ? 'bg-indigo-100 text-indigo-800' : 'bg-amber-100 text-amber-800'
                          }`}>
                            {p.isPitcher ? 'Pitcher' : 'Batter'}
                          </span>

                          <span className="text-gray-400 text-[11px]">
                            {p.activeAppearances} active app{p.activeAppearances === 1 ? '' : 's'}
                          </span>

                          {/* Damage Badge for high ERA */}
                          {p.isPitcher && p.eraDamage > 1.5 && (
                            <span className="inline-flex items-center gap-1 text-[10px] font-black px-2 py-0.5 rounded-md bg-rose-100 text-rose-800 border border-rose-200">
                              <span>⚠️</span>
                              <span>+{p.eraDamage.toFixed(1)} ER Burden</span>
                            </span>
                          )}

                          {/* Drag Badge for negative OBP impact */}
                          {p.isBatter && p.obpDrag >= 4.0 && (
                            <span className="inline-flex items-center gap-1 text-[10px] font-black px-2 py-0.5 rounded-md bg-orange-100 text-orange-800 border border-orange-200">
                              <span>📉</span>
                              <span>+{p.obpDrag.toFixed(1)} OB Drag</span>
                            </span>
                          )}

                          {isPartiallyExcluded && (
                            <span className="text-[10px] font-bold px-2 py-0.5 rounded bg-amber-100 text-amber-800">
                              {excludedOutingsCount} of {p.activeAppearances} outings excluded
                            </span>
                          )}

                          {isFullyExcluded && (
                            <span className="text-[10px] font-black px-2 py-0.5 rounded bg-rose-100 text-rose-800">
                              Excluded
                            </span>
                          )}
                        </div>

                        {/* Player Stat Line */}
                        <div className="text-[11px] text-gray-500 font-mono mt-0.5 flex flex-wrap items-center gap-2">
                          {p.isPitcher ? (
                            <>
                              <span>{p.ip.toFixed(1)} IP</span>
                              <span>•</span>
                              <span className={p.era > 5.0 ? 'font-black text-rose-700' : 'font-bold text-gray-700'}>
                                {p.era !== null ? p.era.toFixed(2) : '-'} ERA
                              </span>
                              <span>•</span>
                              <span>{p.whip !== null ? p.whip.toFixed(2) : '-'} WHIP</span>
                              <span>•</span>
                              <span>{p.er} ER</span>
                              <span>•</span>
                              <span>{p.k} K</span>
                              <span>•</span>
                              <span>{p.qs} QS</span>
                              <span>•</span>
                              <span>{p.svHd} SV+H</span>
                            </>
                          ) : (
                            <>
                              <span>{p.pa} PA</span>
                              <span>•</span>
                              <span className={p.obp !== null && p.obp < 0.280 ? 'font-black text-rose-700' : 'font-bold text-gray-700'}>
                                {p.obp !== null ? p.obp.toFixed(3) : '-'} OBP
                              </span>
                              <span>•</span>
                              <span>{p.r} R</span>
                              <span>•</span>
                              <span>{p.hr} HR</span>
                              <span>•</span>
                              <span>{p.rbi} RBI</span>
                              <span>•</span>
                              <span>{p.sb} SB</span>
                              <span>•</span>
                              <span className="text-gray-400">({p.h}/{p.ab})</span>
                            </>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Accordion expand button for outing-level drilldown */}
                    <div className="flex items-center gap-2 shrink-0">
                      <button
                        onClick={() => toggleAccordion(p.playerId)}
                        className="px-2.5 py-1 rounded-lg text-xs font-bold text-gray-600 bg-gray-100 hover:bg-gray-200 transition flex items-center gap-1"
                      >
                        <span>{isExpanded ? 'Hide Starts' : `Starts (${p.outings.length})`}</span>
                        <span className="text-[10px]">{isExpanded ? '▲' : '▼'}</span>
                      </button>
                    </div>
                  </div>

                  {/* Outing-Level Drilldown Accordion */}
                  {isExpanded && (
                    <div className="bg-gray-50/80 p-3 rounded-2xl border border-gray-200/80 m-2 space-y-2 text-xs">
                      <div className="flex items-center justify-between text-[11px] font-bold text-gray-500 px-1 border-b border-gray-200 pb-1">
                        <span>Daily Active Starts for {p.fullName}</span>
                        <span className="font-normal text-gray-400">Uncheck a specific box to drop that start alone</span>
                      </div>

                      <div className="space-y-1">
                        {p.outings.map((out, idx) => {
                          const outingKey = `${p.playerId}_${out.scoringPeriodId}`;
                          const isOutingExcluded = isFullyExcluded || excludedOutingKeys.has(outingKey);
                          const dateStr = getDateFromPeriodId(out.scoringPeriodId, selectedSeason);
                          const isPitcherMeltdown = out.isPitcher && out.er >= 4;
                          const isBatterCollar = out.isBatter && out.ab >= 2 && out.h === 0 && out.bb === 0;

                          return (
                            <div
                              key={`out-${out.recordId}-${idx}`}
                              className={`p-2 rounded-xl flex items-center justify-between gap-2 border transition ${
                                isOutingExcluded
                                  ? 'bg-rose-100/60 border-rose-300 text-rose-950'
                                  : isPitcherMeltdown || isBatterCollar
                                  ? 'bg-amber-50/80 border-amber-200 text-gray-800'
                                  : 'bg-white border-gray-200 text-gray-800'
                              }`}
                            >
                              <div className="flex items-center gap-2.5 min-w-0">
                                <input
                                  type="checkbox"
                                  checked={!isOutingExcluded}
                                  disabled={isFullyExcluded}
                                  onChange={() => toggleOutingExclusion(p.playerId, out.scoringPeriodId)}
                                  className="w-3.5 h-3.5 rounded text-blue-600 cursor-pointer"
                                  title={isOutingExcluded ? 'Include this start' : 'Exclude this start'}
                                />
                                <span className="font-mono font-bold text-xs bg-gray-200/70 px-1.5 py-0.5 rounded">
                                  {dateStr}
                                </span>
                                {out.isPitcher ? (
                                  <div className="flex items-center gap-1.5 flex-wrap font-mono">
                                    <span className="font-black text-gray-900">{out.ip.toFixed(1)} IP</span>
                                    <span>•</span>
                                    <span className={out.er >= 4 ? 'font-black text-rose-700' : 'text-gray-700'}>
                                      {out.er} ER
                                    </span>
                                    <span>•</span>
                                    <span>{out.era !== null ? out.era.toFixed(1) : '-'} ERA</span>
                                    <span>•</span>
                                    <span>{out.k} K</span>
                                    <span>•</span>
                                    <span>{out.bbAll} BB, {out.hAll} H</span>
                                    {out.qs > 0 && <span className="text-[10px] font-bold text-emerald-700">QS</span>}
                                    {out.er >= 5 && <span className="text-[10px] font-black text-rose-600">💥 Meltdown</span>}
                                  </div>
                                ) : (
                                  <div className="flex items-center gap-1.5 flex-wrap font-mono">
                                    <span className="font-black text-gray-900">{out.h}/{out.ab}</span>
                                    <span>•</span>
                                    <span>{out.r} R</span>
                                    <span>•</span>
                                    <span>{out.hr} HR</span>
                                    <span>•</span>
                                    <span>{out.rbi} RBI</span>
                                    <span>•</span>
                                    <span>{out.sb} SB</span>
                                    <span>•</span>
                                    <span>{out.bb} BB</span>
                                    {isBatterCollar && (
                                      <span className="text-[10px] font-black text-cyan-800 bg-cyan-100 px-1.5 py-0.2 rounded">
                                        ❄️ 0-fer Collar
                                      </span>
                                    )}
                                  </div>
                                )}
                              </div>

                              <div className="text-right shrink-0">
                                {isOutingExcluded && (
                                  <span className="text-[10px] font-black text-rose-700 bg-rose-200/80 px-2 py-0.5 rounded">
                                    Excluded
                                  </span>
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* 5. LEAGUE-WIDE STANDINGS SHIFT TABLE */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4">
        <div className="flex items-center justify-between border-b border-gray-100 pb-3">
          <div>
            <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>🏆</span>
              <span>Full League Rotisserie Standings (Ripple Effect)</span>
            </h3>
            <p className="text-xs text-gray-400 mt-0.5">
              See how this simulated adjustment impacts every team in the league.
            </p>
          </div>
          <span className="text-xs font-mono font-bold text-gray-500 bg-gray-100 px-2.5 py-1 rounded-lg">
            9 Teams
          </span>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-xs">
          {/* Baseline Standings Column */}
          <div className="space-y-2 border border-gray-100 rounded-2xl p-3 bg-gray-50/50">
            <div className="font-bold text-gray-500 uppercase tracking-wider text-[11px] px-1">
              Actual Current Standings
            </div>
            <div className="divide-y divide-gray-100">
              {leagueStandingsComparison.baseStandings.map((t, idx) => (
                <div
                  key={`base-st-${t.id}`}
                  onClick={() => onOwnerClick && onOwnerClick(t)}
                  className={`py-2 px-2 flex items-center justify-between rounded-xl cursor-pointer hover:bg-blue-50/50 transition ${
                    t.isTarget ? 'bg-blue-100/70 font-black text-blue-950 ring-1 ring-blue-300' : ''
                  }`}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="w-5 font-mono font-bold text-gray-400">{idx + 1}.</span>
                    <TeamAvatar team={t} size="xs" />
                    <span className="truncate">{t.name}</span>
                  </div>
                  <span className="font-mono font-bold">{t.rotoTotal.toFixed(1)} pts</span>
                </div>
              ))}
            </div>
          </div>

          {/* Simulated Standings Column */}
          <div className="space-y-2 border border-emerald-200 rounded-2xl p-3 bg-emerald-50/30">
            <div className="font-bold text-emerald-800 uppercase tracking-wider text-[11px] px-1 flex items-center justify-between">
              <span>Simulated Standings</span>
              <span className="text-[10px] font-mono">Counterfactual</span>
            </div>
            <div className="divide-y divide-emerald-100/60">
              {leagueStandingsComparison.simStandings.map((t, idx) => {
                const diff = (simulationResults.simulatedRotoPoints[t.id]?.total || 0) - (baselineRotoPoints[t.id]?.total || 0);
                return (
                  <div
                    key={`sim-st-${t.id}`}
                    className={`py-2 px-2 flex items-center justify-between rounded-xl ${
                      t.isTarget ? 'bg-emerald-200/60 font-black text-emerald-950 ring-1 ring-emerald-400' : ''
                    }`}
                  >
                    <div className="flex items-center gap-2 min-w-0">
                      <span className="w-5 font-mono font-bold text-emerald-700">{idx + 1}.</span>
                      <TeamAvatar team={t} size="xs" />
                      <span className="truncate">{t.name}</span>
                    </div>
                    <div className="flex items-center gap-2 font-mono">
                      <span className="font-bold">{t.rotoTotal.toFixed(1)} pts</span>
                      {Math.abs(diff) > 0.001 && (
                        <span className={`text-[10px] font-bold px-1.5 py-0.2 rounded ${
                          diff > 0 ? 'bg-emerald-100 text-emerald-800' : 'bg-rose-100 text-rose-800'
                        }`}>
                          {diff > 0 ? '+' : ''}{diff.toFixed(1)}
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
