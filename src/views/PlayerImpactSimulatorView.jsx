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
  const [sortBy, setSortBy] = useState('era_damage'); // 'era_damage' | 'er' | 'ip' | 'apps' | 'name'

  // Exclusions: Set of excluded player IDs (full removal) & Set of excluded outing keys (partial removal)
  const [excludedPlayerIds, setExcludedPlayerIds] = useState(new Set());
  const [excludedOutingKeys, setExcludedOutingKeys] = useState(new Set());

  // Expanded players for start-by-start outing drilldown
  const [expandedPlayerIds, setExpandedPlayerIds] = useState(new Set());

  // Replacement player modeling mode: 'empty' | 'league_avg' | 'replacement_level' | 'custom'
  const [replacementMode, setReplacementMode] = useState('empty');
  const [customRates, setCustomRates] = useState({
    pitcherEra: 4.50,
    pitcherWhip: 1.35,
    pitcherK9: 8.0,
    batterObp: 0.315
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

    return Object.values(pMap).map(p => {
      const era = p.ip > 0 ? (p.er * 9) / p.ip : null;
      const whip = p.ip > 0 ? (p.bbAll + p.hAll) / p.ip : null;
      const obpDenom = p.ab + p.bb + p.hbp + p.sf;
      const obp = obpDenom > 0 ? (p.h + p.bb + p.hbp) / obpDenom : (p.pa > 0 ? (p.h + p.bb) / p.pa : null);

      // ERA Damage Index: How much did this pitcher inflate team ERA above baseline?
      const eraDamage = p.ip > 0 ? (p.er - (p.ip * teamBaseEra / 9)) : 0;
      const whipDamage = p.ip > 0 ? ((p.bbAll + p.hAll) - (p.ip * teamBaseWhip)) : 0;

      // Sort outings chronologically (latest first)
      p.outings.sort((a, b) => b.scoringPeriodId - a.scoringPeriodId);

      return {
        ...p,
        era,
        whip,
        obp,
        eraDamage,
        whipDamage,
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
    let totObpNum = 0;
    let totObpDenom = 0;

    Object.values(baselineTeamStatsMap).forEach(st => {
      totIp += parseFloat(st.IP) || 0;
      totEr += parseFloat(st.ER) || 0;
      totBb += parseFloat(st.BB_Allowed) || 0;
      totH += parseFloat(st.H_Allowed) || 0;
      totK += parseFloat(st.K) || 0;
      totObpNum += parseFloat(st.OBP_num) || 0;
      totObpDenom += parseFloat(st.OBP_denom) || 0;
    });

    const era = totIp > 0 ? (totEr * 9) / totIp : 4.15;
    const whip = totIp > 0 ? (totBb + totH) / totIp : 1.25;
    const k9 = totIp > 0 ? (totK * 9) / totIp : 8.5;
    const obp = totObpDenom > 0 ? totObpNum / totObpDenom : 0.320;

    return { era, whip, k9, obp };
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

    // Calculate synthetic replacement stats if replacement mode is selected
    let repEr = 0;
    let repBbH = 0;
    let repK = 0;
    let repObpNum = 0;
    let repObpDenom = vacatedPa;

    if (replacementMode === 'league_avg') {
      repEr = vacatedIp * (leagueAverages.era / 9);
      repBbH = vacatedIp * leagueAverages.whip;
      repK = vacatedIp * (leagueAverages.k9 / 9);
      repObpNum = vacatedPa * leagueAverages.obp;
    } else if (replacementMode === 'replacement_level') {
      const repEra = 4.65;
      const repWhip = 1.38;
      const repK9 = 7.5;
      const repObp = 0.300;
      repEr = vacatedIp * (repEra / 9);
      repBbH = vacatedIp * repWhip;
      repK = vacatedIp * (repK9 / 9);
      repObpNum = vacatedPa * repObp;
    } else if (replacementMode === 'custom') {
      repEr = vacatedIp * (customRates.pitcherEra / 9);
      repBbH = vacatedIp * customRates.pitcherWhip;
      repK = vacatedIp * (customRates.pitcherK9 / 9);
      repObpNum = vacatedPa * customRates.batterObp;
    }

    // Combine kept stats + replacement stats
    const finalTotals = { ...pureKeptStats };

    if (replacementMode !== 'empty') {
      finalTotals.IP += vacatedIp;
      finalTotals.ER += repEr;
      finalTotals.BB_Allowed += repBbH * 0.32; // estimate ~32% BB
      finalTotals.H_Allowed += repBbH * 0.68;  // estimate ~68% H
      finalTotals.K += repK;

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
    replacementMode,
    leagueAverages,
    customRates,
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
      if (sortBy === 'er') return b.er - a.er;
      if (sortBy === 'ip') return b.ip - a.ip;
      if (sortBy === 'apps') return b.activeAppearances - a.activeAppearances;
      if (sortBy === 'name') return a.fullName.localeCompare(b.fullName);
      return 0;
    });

    return list;
  }, [playerProfiles, playerFilter, searchQuery, sortBy]);

  // Preset handlers
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
        // Less than 1.0 IP and 2+ ER
        if (out.isPitcher && out.ip < 1.0 && out.er >= 2.0) {
          disasterOutingKeys.add(`${out.playerId}_${out.scoringPeriodId}`);
        }
      });
    });
    setExcludedOutingKeys(disasterOutingKeys);
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

  // Deltas for display
  const baseEra = parseFloat(baseTeamStats.ERA_raw ?? baseTeamStats.ERA) || 0;
  const simEra = parseFloat(simTeamStats.ERA_raw ?? simTeamStats.ERA) || 0;
  const eraDiff = simEra - baseEra;

  const baseWhip = parseFloat(baseTeamStats.WHIP_raw ?? baseTeamStats.WHIP) || 0;
  const simWhip = parseFloat(simTeamStats.WHIP_raw ?? simTeamStats.WHIP) || 0;
  const whipDiff = simWhip - baseWhip;

  const rotoPointsDiff = simRoto.total - baseRoto.total;
  const rankDiff = leagueStandingsComparison.rankDelta;

  const totalExclusionsCount = excludedPlayerIds.size + excludedOutingKeys.size;

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
            >
              💥 Exclude Top 3 ERA Killers
            </button>
            <button
              onClick={handleExcludeSubInningMeltdowns}
              className="px-2.5 py-1 rounded-lg bg-amber-500/20 text-amber-300 border border-amber-500/40 hover:bg-amber-500/30 transition font-bold"
            >
              🛑 Exclude Sub-Inning Meltdowns (&lt;1 IP, ≥2 ER)
            </button>
            {totalExclusionsCount > 0 && (
              <button
                onClick={handleClearAllExclusions}
                className="px-2.5 py-1 rounded-lg bg-gray-700/60 text-gray-200 hover:bg-gray-700 transition font-bold"
              >
                🔄 Reset ({totalExclusionsCount} active)
              </button>
            )}
          </div>

          <div className="font-mono text-indigo-200">
            Active Exclusions: <span className="font-black text-white">{excludedPlayerIds.size}</span> players, <span className="font-black text-white">{excludedOutingKeys.size}</span> single outings
          </div>
        </div>
      </div>

      {/* 2. HERO COUNTERFACTUAL COMPARISON CARDS */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* ERA Card */}
        <div className={`p-5 rounded-2xl border transition-all ${
          eraDiff < -0.001
            ? 'bg-emerald-50/70 border-emerald-300 shadow-sm ring-1 ring-emerald-200'
            : eraDiff > 0.001
            ? 'bg-rose-50/70 border-rose-300 shadow-sm'
            : 'bg-white border-gray-200 shadow-sm'
        }`}>
          <div className="flex items-center justify-between text-xs font-bold text-gray-500 mb-1">
            <span>Earned Run Average</span>
            <span className="font-mono text-[11px]">ERA (Lower is better)</span>
          </div>
          <div className="flex items-baseline justify-between">
            <div>
              <div className="text-2xl font-black text-gray-900 font-mono">
                {simEra.toFixed(2)}
              </div>
              <div className="text-xs text-gray-400 font-mono mt-0.5">
                Baseline: {baseEra.toFixed(2)}
              </div>
            </div>
            <div className="text-right">
              <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-black font-mono ${
                eraDiff < -0.001
                  ? 'bg-emerald-100 text-emerald-800'
                  : eraDiff > 0.001
                  ? 'bg-rose-100 text-rose-800'
                  : 'bg-gray-100 text-gray-600'
              }`}>
                {eraDiff < 0 ? '▼' : eraDiff > 0 ? '▲' : ''} {eraDiff.toFixed(3)}
              </span>
              <div className="text-[10px] text-gray-400 mt-1">
                {eraDiff < 0 ? 'Better' : eraDiff > 0 ? 'Worse' : 'Unchanged'}
              </div>
            </div>
          </div>
        </div>

        {/* WHIP Card */}
        <div className={`p-5 rounded-2xl border transition-all ${
          whipDiff < -0.001
            ? 'bg-emerald-50/70 border-emerald-300 shadow-sm ring-1 ring-emerald-200'
            : whipDiff > 0.001
            ? 'bg-rose-50/70 border-rose-300 shadow-sm'
            : 'bg-white border-gray-200 shadow-sm'
        }`}>
          <div className="flex items-center justify-between text-xs font-bold text-gray-500 mb-1">
            <span>Walks + Hits / IP</span>
            <span className="font-mono text-[11px]">WHIP</span>
          </div>
          <div className="flex items-baseline justify-between">
            <div>
              <div className="text-2xl font-black text-gray-900 font-mono">
                {simWhip.toFixed(3)}
              </div>
              <div className="text-xs text-gray-400 font-mono mt-0.5">
                Baseline: {baseWhip.toFixed(3)}
              </div>
            </div>
            <div className="text-right">
              <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-black font-mono ${
                whipDiff < -0.001
                  ? 'bg-emerald-100 text-emerald-800'
                  : whipDiff > 0.001
                  ? 'bg-rose-100 text-rose-800'
                  : 'bg-gray-100 text-gray-600'
              }`}>
                {whipDiff < 0 ? '▼' : whipDiff > 0 ? '▲' : ''} {whipDiff.toFixed(3)}
              </span>
              <div className="text-[10px] text-gray-400 mt-1">
                {whipDiff < 0 ? 'Better' : whipDiff > 0 ? 'Worse' : 'Unchanged'}
              </div>
            </div>
          </div>
        </div>

        {/* Roto Points Card */}
        <div className={`p-5 rounded-2xl border transition-all ${
          rotoPointsDiff > 0.001
            ? 'bg-emerald-50/70 border-emerald-300 shadow-sm ring-1 ring-emerald-200'
            : rotoPointsDiff < -0.001
            ? 'bg-rose-50/70 border-rose-300 shadow-sm'
            : 'bg-white border-gray-200 shadow-sm'
        }`}>
          <div className="flex items-center justify-between text-xs font-bold text-gray-500 mb-1">
            <span>Total Roto Points</span>
            <span className="font-mono text-[11px]">Points</span>
          </div>
          <div className="flex items-baseline justify-between">
            <div>
              <div className="text-2xl font-black text-blue-700 font-mono">
                {simRoto.total.toFixed(1)}
              </div>
              <div className="text-xs text-gray-400 font-mono mt-0.5">
                Baseline: {baseRoto.total.toFixed(1)}
              </div>
            </div>
            <div className="text-right">
              <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-black font-mono ${
                rotoPointsDiff > 0.001
                  ? 'bg-emerald-100 text-emerald-800'
                  : rotoPointsDiff < -0.001
                  ? 'bg-rose-100 text-rose-800'
                  : 'bg-gray-100 text-gray-600'
              }`}>
                {rotoPointsDiff > 0 ? '+' : ''}{rotoPointsDiff.toFixed(1)} pts
              </span>
              <div className="text-[10px] text-gray-400 mt-1">
                {rotoPointsDiff > 0 ? '▲ Gained' : rotoPointsDiff < 0 ? '▼ Lost' : 'Even'}
              </div>
            </div>
          </div>
        </div>

        {/* League Standings Rank Card */}
        <div className={`p-5 rounded-2xl border transition-all ${
          rankDiff > 0
            ? 'bg-emerald-50/70 border-emerald-300 shadow-sm ring-1 ring-emerald-200'
            : rankDiff < 0
            ? 'bg-rose-50/70 border-rose-300 shadow-sm'
            : 'bg-white border-gray-200 shadow-sm'
        }`}>
          <div className="flex items-center justify-between text-xs font-bold text-gray-500 mb-1">
            <span>Standings Position</span>
            <span className="font-mono text-[11px]">Out of 9 Teams</span>
          </div>
          <div className="flex items-baseline justify-between">
            <div>
              <div className="text-2xl font-black text-purple-950 font-mono">
                {leagueStandingsComparison.simRank}
                <span className="text-xs font-bold text-gray-400 ml-1">
                  {leagueStandingsComparison.simRank === 1 ? 'st 👑' :
                   leagueStandingsComparison.simRank === 2 ? 'nd' :
                   leagueStandingsComparison.simRank === 3 ? 'rd' : 'th'}
                </span>
              </div>
              <div className="text-xs text-gray-400 font-mono mt-0.5">
                Baseline: {leagueStandingsComparison.baseRank}th
              </div>
            </div>
            <div className="text-right">
              <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-black font-mono ${
                rankDiff > 0
                  ? 'bg-emerald-100 text-emerald-800'
                  : rankDiff < 0
                  ? 'bg-rose-100 text-rose-800'
                  : 'bg-gray-100 text-gray-600'
              }`}>
                {rankDiff > 0 ? `▲ +${rankDiff} Spot${rankDiff > 1 ? 's' : ''}` : rankDiff < 0 ? `▼ ${rankDiff} Spots` : 'Unchanged'}
              </span>
              <div className="text-[10px] text-gray-400 mt-1">
                {rankDiff > 0 ? 'Moved Up!' : rankDiff < 0 ? 'Fell Behind' : 'Same Rank'}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* 3. REPLACEMENT PLAYER MODELING CONTROLS */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
          <div>
            <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
              <span>⚖️</span>
              <span>Replacement Player Simulation Mode</span>
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Choose what happens to the innings or plate appearances vacated by excluded players.
            </p>
          </div>

          <div className="flex items-center gap-1 bg-gray-100 p-1 rounded-xl self-start sm:self-auto flex-wrap">
            <button
              onClick={() => setReplacementMode('empty')}
              className={`px-3 py-1 text-xs font-bold rounded-lg transition ${
                replacementMode === 'empty' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              🚫 Empty Slot (Never Added)
            </button>
            <button
              onClick={() => setReplacementMode('league_avg')}
              className={`px-3 py-1 text-xs font-bold rounded-lg transition ${
                replacementMode === 'league_avg' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              📊 League Average Streamer
            </button>
            <button
              onClick={() => setReplacementMode('replacement_level')}
              className={`px-3 py-1 text-xs font-bold rounded-lg transition ${
                replacementMode === 'replacement_level' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ⚖️ Waiver-Level Streamer
            </button>
            <button
              onClick={() => setReplacementMode('custom')}
              className={`px-3 py-1 text-xs font-bold rounded-lg transition ${
                replacementMode === 'custom' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ✏️ Custom Rates
            </button>
          </div>
        </div>

        {/* Explanation text per mode */}
        <div className="text-xs text-gray-600">
          {replacementMode === 'empty' && (
            <p className="bg-slate-50 p-2.5 rounded-xl border border-slate-200">
              <span className="font-bold text-slate-800">Pure Omission (Default):</span> All vacated IP ({simulationResults.vacated.vacatedIp.toFixed(1)} IP) and PA ({simulationResults.vacated.vacatedPa} PA) are completely subtracted with 0 stats added back. Models: <em>"What if I never rostered these streamers and left the slot empty?"</em>
            </p>
          )}

          {replacementMode === 'league_avg' && (
            <p className="bg-blue-50/60 p-2.5 rounded-xl border border-blue-200 text-blue-900">
              <span className="font-bold">League Average Rates:</span> Replaces the {simulationResults.vacated.vacatedIp.toFixed(1)} vacated IP with the 2026 league mean pitching rates (<span className="font-mono font-bold">{leagueAverages.era.toFixed(2)} ERA</span>, <span className="font-mono font-bold">{leagueAverages.whip.toFixed(2)} WHIP</span>, <span className="font-mono font-bold">{leagueAverages.k9.toFixed(1)} K/9</span>) and {simulationResults.vacated.vacatedPa} PA at <span className="font-mono font-bold">.{Math.round(leagueAverages.obp * 1000)} OBP</span>. Models: <em>"What if I had streamed an average pitcher instead?"</em>
            </p>
          )}

          {replacementMode === 'replacement_level' && (
            <p className="bg-amber-50/60 p-2.5 rounded-xl border border-amber-200 text-amber-900">
              <span className="font-bold">Waiver Replacement Level:</span> Replaces the {simulationResults.vacated.vacatedIp.toFixed(1)} vacated IP with waiver-tier production (<span className="font-mono font-bold">4.65 ERA</span>, <span className="font-mono font-bold">1.38 WHIP</span>, <span className="font-mono font-bold">7.5 K/9</span>) and {simulationResults.vacated.vacatedPa} PA at <span className="font-mono font-bold">.300 OBP</span>.
            </p>
          )}

          {replacementMode === 'custom' && (
            <div className="bg-purple-50/60 p-3 rounded-xl border border-purple-200 space-y-3">
              <div className="font-bold text-purple-950">Set Hypothetical Replacement Benchmark Rates:</div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
                <div>
                  <label className="block text-[11px] font-bold text-gray-700 mb-1">Pitcher ERA</label>
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    max="15"
                    value={customRates.pitcherEra}
                    onChange={(e) => setCustomRates({ ...customRates, pitcherEra: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-purple-300 rounded-lg px-2 py-1 font-mono font-bold text-xs"
                  />
                </div>
                <div>
                  <label className="block text-[11px] font-bold text-gray-700 mb-1">Pitcher WHIP</label>
                  <input
                    type="number"
                    step="0.05"
                    min="0"
                    max="3"
                    value={customRates.pitcherWhip}
                    onChange={(e) => setCustomRates({ ...customRates, pitcherWhip: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-purple-300 rounded-lg px-2 py-1 font-mono font-bold text-xs"
                  />
                </div>
                <div>
                  <label className="block text-[11px] font-bold text-gray-700 mb-1">Pitcher K/9</label>
                  <input
                    type="number"
                    step="0.5"
                    min="0"
                    max="20"
                    value={customRates.pitcherK9}
                    onChange={(e) => setCustomRates({ ...customRates, pitcherK9: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-purple-300 rounded-lg px-2 py-1 font-mono font-bold text-xs"
                  />
                </div>
                <div>
                  <label className="block text-[11px] font-bold text-gray-700 mb-1">Batter OBP</label>
                  <input
                    type="number"
                    step="0.005"
                    min="0"
                    max="1"
                    value={customRates.batterObp}
                    onChange={(e) => setCustomRates({ ...customRates, batterObp: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-purple-300 rounded-lg px-2 py-1 font-mono font-bold text-xs"
                  />
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* 4. MAIN WORKBENCH: PLAYER SELECTION & OUTING ACCORDION */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-gray-100 pb-3">
          <div>
            <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>📋</span>
              <span>Active Players on {activeTeamObj.name} ({activeTeamObj.owner})</span>
            </h3>
            <p className="text-xs text-gray-400 mt-0.5">
              Toggle checkboxes to exclude whole players, or click an outing row to exclude individual starts.
            </p>
          </div>

          {/* Filter Pills & Search */}
          <div className="flex items-center gap-2 flex-wrap">
            <div className="flex items-center bg-gray-100 p-1 rounded-xl text-xs">
              <button
                onClick={() => setPlayerFilter('pitchers')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'pitchers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                Pitchers
              </button>
              <button
                onClick={() => setPlayerFilter('streamers')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'streamers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                Spot Streamers (≤4)
              </button>
              <button
                onClick={() => setPlayerFilter('batters')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'batters' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                Batters
              </button>
              <button
                onClick={() => setPlayerFilter('all')}
                className={`px-2.5 py-1 font-bold rounded-lg transition ${playerFilter === 'all' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'}`}
              >
                All
              </button>
            </div>

            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
              className="px-2.5 py-1 text-xs border border-gray-200 rounded-xl bg-gray-50 focus:bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 font-bold text-gray-700 cursor-pointer"
            >
              <option value="era_damage">Sort: ERA Damage</option>
              <option value="er">Sort: Most ER</option>
              <option value="ip">Sort: Innings Pitched</option>
              <option value="apps">Sort: Appearances</option>
              <option value="name">Sort: Name</option>
            </select>

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
                              <span className="font-bold text-gray-700">
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

                          return (
                            <div
                              key={`out-${out.recordId}-${idx}`}
                              className={`p-2 rounded-xl flex items-center justify-between gap-2 border transition ${
                                isOutingExcluded
                                  ? 'bg-rose-100/60 border-rose-300 text-rose-950'
                                  : out.er >= 4
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

      {/* 5. FULL CATEGORY IMPACT MATRIX TABLE */}
      <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4">
        <div className="flex items-center justify-between border-b border-gray-100 pb-3">
          <div>
            <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>📊</span>
              <span>10-Category Impact & Roto Points Matrix</span>
            </h3>
            <p className="text-xs text-gray-400 mt-0.5">
              Category-by-category shift for {activeTeamObj.name} under the current simulation.
            </p>
          </div>
          <span className="text-xs font-mono font-bold text-blue-700 bg-blue-50 px-2.5 py-1 rounded-lg border border-blue-200">
            Net: {rotoPointsDiff >= 0 ? `+${rotoPointsDiff.toFixed(1)}` : rotoPointsDiff.toFixed(1)} Pts
          </span>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-gray-50 border-b border-gray-200 text-gray-500 font-bold uppercase">
              <tr>
                <th className="py-2.5 px-3 text-left">Category</th>
                <th className="py-2.5 px-3 text-right">Baseline Stat</th>
                <th className="py-2.5 px-3 text-right">Simulated Stat</th>
                <th className="py-2.5 px-3 text-right">Raw Delta</th>
                <th className="py-2.5 px-3 text-right">Base Roto Pts</th>
                <th className="py-2.5 px-3 text-right">Sim Roto Pts</th>
                <th className="py-2.5 px-3 text-right">Pts Delta</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 font-mono">
              {Object.keys(SCORING_CATS).map(catKey => {
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

                const formatNum = (n) => {
                  if (catKey === 'ERA' || catKey === 'WHIP') return n.toFixed(3);
                  if (catKey === 'OBP') return n.toFixed(4).replace(/^0/, '');
                  return Math.round(n);
                };

                const isBeneficial = isLow ? diff < -0.0001 : diff > 0.0001;
                const isDetrimental = isLow ? diff > 0.0001 : diff < -0.0001;

                return (
                  <tr key={catKey} className="hover:bg-gray-50/80 transition">
                    <td className="py-2.5 px-3 font-sans font-black text-gray-900 flex items-center gap-1.5">
                      <span>{meta.label}</span>
                      <span className="text-[10px] font-normal text-gray-400 font-sans">({isLow ? 'low' : 'high'})</span>
                    </td>
                    <td className="py-2.5 px-3 text-right text-gray-600">
                      {formatNum(baseNum)}
                    </td>
                    <td className="py-2.5 px-3 text-right font-bold text-gray-900">
                      {formatNum(simNum)}
                    </td>
                    <td className="py-2.5 px-3 text-right font-black">
                      <span className={`px-1.5 py-0.5 rounded text-[11px] ${
                        isBeneficial ? 'bg-emerald-100 text-emerald-800' :
                        isDetrimental ? 'bg-rose-100 text-rose-800' :
                        'text-gray-400'
                      }`}>
                        {diff > 0 ? '+' : ''}{formatNum(diff)}
                      </span>
                    </td>
                    <td className="py-2.5 px-3 text-right text-gray-500">
                      {basePts.toFixed(1)}
                    </td>
                    <td className="py-2.5 px-3 text-right font-bold text-blue-700">
                      {simPts.toFixed(1)}
                    </td>
                    <td className="py-2.5 px-3 text-right font-black">
                      <span className={`px-1.5 py-0.5 rounded text-[11px] ${
                        ptsDiff > 0.001 ? 'bg-emerald-100 text-emerald-800' :
                        ptsDiff < -0.001 ? 'bg-rose-100 text-rose-800' :
                        'text-gray-400'
                      }`}>
                        {ptsDiff > 0 ? '+' : ''}{ptsDiff.toFixed(1)}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* 6. LEAGUE-WIDE STANDINGS SHIFT TABLE */}
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
