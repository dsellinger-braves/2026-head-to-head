import { useState, useMemo } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, SCORING_CATS, calculateRotoPoints } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

// 10 Roto scoring categories with icons and descriptions
const ROTO_CATEGORIES = [
  { id: 'R', name: 'Runs', type: 'high', isRate: false, icon: '🏃', unit: 'runs' },
  { id: 'HR', name: 'Home Runs', type: 'high', isRate: false, icon: '💥', unit: 'HRs' },
  { id: 'RBI', name: 'Runs Batted In', type: 'high', isRate: false, icon: '🎯', unit: 'RBIs' },
  { id: 'SB', name: 'Stolen Bases', type: 'high', isRate: false, icon: '⚡', unit: 'SBs' },
  { id: 'OBP', name: 'On-Base %', type: 'high', isRate: true, icon: '👁️', unit: 'OBP' },
  { id: 'K', name: 'Strikeouts', type: 'high', isRate: false, icon: '🔥', unit: 'Ks' },
  { id: 'QS', name: 'Quality Starts', type: 'high', isRate: false, icon: '🛡️', unit: 'QS' },
  { id: 'SV+HDs', name: 'Saves + Holds', type: 'high', isRate: false, icon: '🔒', unit: 'SV+HDs' },
  { id: 'ERA', name: 'Earned Run Avg', type: 'low', isRate: true, icon: '🧱', unit: 'ERA' },
  { id: 'WHIP', name: 'WHIP', type: 'low', isRate: true, icon: '🧤', unit: 'WHIP' }
];

export default function RotoGapView({ allStats, selectedSeason = 2026, onOwnerClick }) {
  // Default to Daniel (ID 5) if present, else first team
  const [selectedOwnerId, setSelectedOwnerId] = useState(() => {
    return TEAMS['5'] ? '5' : Object.keys(TEAMS).find(id => parseInt(id) !== 99) || '1';
  });

  const humanTeamIds = useMemo(() => {
    return Object.keys(TEAMS).filter(id => parseInt(id) !== 99);
  }, []);

  // Compute team aggregates and roto points
  const { teamStatsMap, rotoPointsMap } = useMemo(() => {
    const groups = {};
    humanTeamIds.forEach(id => { groups[id] = []; });
    allStats.forEach(r => {
      if (groups[r.team_id]) groups[r.team_id].push(r);
    });

    const statsMap = {};
    humanTeamIds.forEach(id => {
      statsMap[id] = aggregateStats(groups[id] || []);
    });

    const rotoMap = calculateRotoPoints(statsMap);
    return { teamStatsMap: statsMap, rotoPointsMap: rotoMap };
  }, [allStats, humanTeamIds]);

  // Days remaining calculation
  const daysRemainingInfo = useMemo(() => {
    const now = new Date();
    const seasonEnd = new Date('2026-09-27T23:59:59');
    const diffMs = seasonEnd.getTime() - now.getTime();
    const days = Math.max(1, Math.ceil(diffMs / (1000 * 60 * 60 * 24)));
    const isPastSeason = selectedSeason < 2026;
    return {
      days: isPastSeason ? 0 : days,
      label: isPastSeason
        ? 'Season Concluded (Final Deficit)'
        : `${days} Days Remaining (Season ends Sep 27, 2026)`
    };
  }, [selectedSeason]);

  // Compute category rankings and gaps for the selected owner
  const categoryAnalysis = useMemo(() => {
    const selStats = teamStatsMap[selectedOwnerId];
    if (!selStats) return [];

    return ROTO_CATEGORIES.map(cat => {
      const isHigh = cat.type === 'high';
      const isRate = cat.isRate;

      // Build sorted leaderboard for this category
      const rankedList = humanTeamIds.map(id => {
        const stats = teamStatsMap[id];
        const rawVal = stats[`${cat.id}_raw`] !== undefined ? stats[`${cat.id}_raw`] : parseFloat(stats[cat.id]) || 0;
        return {
          id,
          team: TEAMS[id],
          stats,
          rawVal,
          rotoPts: rotoPointsMap[id]?.[cat.id] || 0
        };
      });

      // Sort: best to worst (index 0 is #1 rank)
      rankedList.sort((a, b) => isHigh ? b.rawVal - a.rawVal : a.rawVal - b.rawVal);

      // Find my position
      const myIdx = rankedList.findIndex(item => item.id === selectedOwnerId);
      const myData = rankedList[myIdx] || {};
      const myRank = myIdx + 1;
      const myVal = myData.rawVal || 0;
      const myRoto = myData.rotoPts || 0;

      // Leader
      const leader = rankedList[0];

      // Team directly ahead (+1 Roto Point target)
      const targetAhead = myIdx > 0 ? rankedList[myIdx - 1] : null;

      // Team directly behind (defending lead)
      const defendingBehind = myIdx < rankedList.length - 1 ? rankedList[myIdx + 1] : null;

      // Gap calculation to target ahead
      let gapAhead = null;
      let numeratorDifference = null;
      let rateNeededPerDay = null;
      let dilutionText = null;

      if (targetAhead) {
        if (!isRate) {
          // Counting stat: absolute difference to tie/beat
          const diff = Math.abs(targetAhead.rawVal - myVal);
          gapAhead = diff;
          if (daysRemainingInfo.days > 0) {
            rateNeededPerDay = (diff / daysRemainingInfo.days).toFixed(2);
          }
        } else {
          // Ratio stat: OBP, ERA, WHIP
          if (cat.id === 'OBP') {
            const targetOBP = targetAhead.rawVal;
            const currentPA = selStats.PA || 1;
            const currentTOB = selStats.OBP_num || 0;
            // delta TOB needed without extra PA
            const neededTOB = Math.max(0, (targetOBP * currentPA) - currentTOB);
            // consecutive reach base events needed: (targetOBP * PA - TOB) / (1 - targetOBP)
            const consecutiveOnBase = targetOBP < 1 ? Math.ceil(neededTOB / (1 - targetOBP)) : Math.ceil(neededTOB);
            numeratorDifference = {
              label: 'Times on Base (TOB)',
              needed: neededTOB.toFixed(1),
              context: `+${neededTOB.toFixed(1)} TOB at current PA (or ~${consecutiveOnBase} straight on-base appearances)`
            };
            if (daysRemainingInfo.days > 0) {
              rateNeededPerDay = `+${(neededTOB / daysRemainingInfo.days).toFixed(2)} TOB / day`;
            }
          } else if (cat.id === 'ERA') {
            const targetERA = targetAhead.rawVal;
            const currentIP = selStats.IP || 1;
            const currentER = selStats.ER || 0;
            // Equivalent ER at current IP
            const targetER = (targetERA * currentIP) / 9;
            const erReduction = Math.max(0, currentER - targetER);
            // Scoreless innings to dilute current ER down to target ERA: (ER * 9 / targetERA) - IP
            const dilutionIP = targetERA > 0 ? Math.max(0, ((currentER * 9) / targetERA) - currentIP) : 0;
            numeratorDifference = {
              label: 'Earned Runs (ER)',
              needed: `-${erReduction.toFixed(1)} ER`,
              context: `Needs ${erReduction.toFixed(1)} fewer ER, or ${dilutionIP.toFixed(1)} consecutive scoreless IP`
            };
            dilutionText = `${dilutionIP.toFixed(1)} scoreless IP`;
            if (daysRemainingInfo.days > 0) {
              rateNeededPerDay = `${(dilutionIP / daysRemainingInfo.days).toFixed(1)} scoreless IP / day`;
            }
          } else if (cat.id === 'WHIP') {
            const targetWHIP = targetAhead.rawVal;
            const currentIP = selStats.IP || 1;
            const currentBaserunners = (selStats.BB_Allowed || 0) + (selStats.H_Allowed || 0);
            const targetBaserunners = targetWHIP * currentIP;
            const baserunnerReduction = Math.max(0, currentBaserunners - targetBaserunners);
            // Baserunner-free innings to dilute WHIP down: (Baserunners / targetWHIP) - IP
            const dilutionIP = targetWHIP > 0 ? Math.max(0, (currentBaserunners / targetWHIP) - currentIP) : 0;
            numeratorDifference = {
              label: 'Baserunners Allowed (H+BB)',
              needed: `-${baserunnerReduction.toFixed(1)} H+BB`,
              context: `Needs ${baserunnerReduction.toFixed(1)} fewer baserunners, or ${dilutionIP.toFixed(1)} clean IP (0 WH)`
            };
            dilutionText = `${dilutionIP.toFixed(1)} clean IP`;
            if (daysRemainingInfo.days > 0) {
              rateNeededPerDay = `${(dilutionIP / daysRemainingInfo.days).toFixed(1)} clean IP / day`;
            }
          }
        }
      }

      // Gap defending behind (-1 Roto Point buffer & downside pace risk)
      let cushionBehind = null;
      let downsideNumeratorBuffer = null;
      let downsideRateRiskPerDay = null;

      if (defendingBehind) {
        if (!isRate) {
          const diff = Math.abs(myVal - defendingBehind.rawVal);
          cushionBehind = diff;
          if (diff === 0) {
            downsideRateRiskPerDay = `Tied (any +1 ${cat.unit} surrenders point)`;
          } else if (daysRemainingInfo.days > 0) {
            const daily = (diff / daysRemainingInfo.days).toFixed(2);
            downsideRateRiskPerDay = `+${daily} ${cat.unit} / day`;
          }
        } else {
          cushionBehind = Math.abs(myVal - defendingBehind.rawVal);
          if (cat.id === 'OBP') {
            const trailingOBP = defendingBehind.rawVal;
            const currentPA = selStats.PA || 1;
            const currentTOB = selStats.OBP_num || 0;
            const tobBuffer = Math.max(0, currentTOB - (trailingOBP * currentPA));
            const slumpTolerance = trailingOBP > 0 ? Math.ceil(tobBuffer / trailingOBP) : 0;
            const dailyTOB = daysRemainingInfo.days > 0 ? (tobBuffer / daysRemainingInfo.days).toFixed(2) : '0';
            downsideNumeratorBuffer = {
              label: 'Times on Base (TOB) Cushion',
              needed: `+${tobBuffer.toFixed(1)} TOB lead`,
              context: `Can absorb a 0-for-${slumpTolerance} slump (or chaser outpacing by +${dailyTOB} TOB/day) before dropping`
            };
            if (daysRemainingInfo.days > 0) {
              downsideRateRiskPerDay = `+${dailyTOB} TOB / day`;
            }
          } else if (cat.id === 'ERA') {
            const trailingERA = defendingBehind.rawVal;
            const currentIP = selStats.IP || 1;
            const currentER = selStats.ER || 0;
            const allowedERTotal = (trailingERA * currentIP) / 9;
            const erAllowance = Math.max(0, allowedERTotal - currentER);
            const dailyER = daysRemainingInfo.days > 0 ? (erAllowance / daysRemainingInfo.days).toFixed(2) : '0';
            downsideNumeratorBuffer = {
              label: 'Earned Runs (ER) Buffer',
              needed: `+${erAllowance.toFixed(1)} ER buffer`,
              context: `Can surrender up to ${erAllowance.toFixed(1)} extra ER before being passed (or +${dailyER} ER/day)`
            };
            if (daysRemainingInfo.days > 0) {
              downsideRateRiskPerDay = `+${dailyER} ER / day`;
            }
          } else if (cat.id === 'WHIP') {
            const trailingWHIP = defendingBehind.rawVal;
            const currentIP = selStats.IP || 1;
            const currentBaserunners = (selStats.BB_Allowed || 0) + (selStats.H_Allowed || 0);
            const allowedBaserunners = trailingWHIP * currentIP;
            const brAllowance = Math.max(0, allowedBaserunners - currentBaserunners);
            const dailyBR = daysRemainingInfo.days > 0 ? (brAllowance / daysRemainingInfo.days).toFixed(2) : '0';
            downsideNumeratorBuffer = {
              label: 'Baserunners Allowed (H+BB) Buffer',
              needed: `+${brAllowance.toFixed(1)} H+BB buffer`,
              context: `Can allow up to ${brAllowance.toFixed(1)} extra baserunners before being passed (or +${dailyBR} H+BB/day)`
            };
            if (daysRemainingInfo.days > 0) {
              downsideRateRiskPerDay = `+${dailyBR} H+BB / day`;
            }
          }
        }
      }

      // Format display values
      const formatValue = (v) => {
        if (cat.id === 'OBP') return v.toFixed(4).replace(/^0/, '');
        if (cat.id === 'ERA' || cat.id === 'WHIP') return v.toFixed(3);
        return Math.round(v);
      };

      return {
        cat,
        myRank,
        myVal: formatValue(myVal),
        myRoto,
        leader: {
          team: leader.team,
          val: formatValue(leader.rawVal)
        },
        targetAhead: targetAhead ? {
          team: targetAhead.team,
          val: formatValue(targetAhead.rawVal),
          rotoPts: targetAhead.rotoPts,
          gap: gapAhead,
          numeratorDifference,
          rateNeededPerDay,
          dilutionText
        } : null,
        defendingBehind: defendingBehind ? {
          team: defendingBehind.team,
          val: formatValue(defendingBehind.rawVal),
          rotoPts: defendingBehind.rotoPts,
          cushion: cushionBehind,
          numeratorBuffer: downsideNumeratorBuffer,
          rateRiskPerDay: downsideRateRiskPerDay
        } : null
      };
    });
  }, [selectedOwnerId, humanTeamIds, teamStatsMap, rotoPointsMap, daysRemainingInfo]);

  // Top upside targets (where deficit is smallest / closest to +1 pt)
  const topOpportunities = useMemo(() => {
    return categoryAnalysis
      .filter(c => c.targetAhead)
      .map(c => {
        let score = 999;
        if (!c.cat.isRate) score = c.targetAhead.gap;
        else if (c.cat.id === 'OBP') score = parseFloat(c.targetAhead.numeratorDifference?.needed) || 999;
        else if (c.cat.id === 'ERA') score = parseFloat(c.targetAhead.numeratorDifference?.needed?.replace('-', '')) || 999;
        else if (c.cat.id === 'WHIP') score = parseFloat(c.targetAhead.numeratorDifference?.needed?.replace('-', '')) || 999;
        return { ...c, score };
      })
      .sort((a, b) => a.score - b.score)
      .slice(0, 3);
  }, [categoryAnalysis]);

  // Highest downside risks (where cushion is smallest / closest to -1 pt)
  const highestRisks = useMemo(() => {
    return categoryAnalysis
      .filter(c => c.defendingBehind)
      .map(c => {
        let score = 999;
        if (!c.cat.isRate) score = c.defendingBehind.cushion;
        else if (c.cat.id === 'OBP') score = parseFloat(c.defendingBehind.numeratorBuffer?.needed?.replace('+', '')) || 999;
        else if (c.cat.id === 'ERA') score = parseFloat(c.defendingBehind.numeratorBuffer?.needed?.replace('+', '')) || 999;
        else if (c.cat.id === 'WHIP') score = parseFloat(c.defendingBehind.numeratorBuffer?.needed?.replace('+', '')) || 999;
        return { ...c, score };
      })
      .sort((a, b) => a.score - b.score)
      .slice(0, 3);
  }, [categoryAnalysis]);

  const selectedTeam = TEAMS[selectedOwnerId];
  const totalRotoPoints = rotoPointsMap[selectedOwnerId]?.total || 0;

  return (
    <div className="space-y-6">
      {/* Header & Owner Selector */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-4 pb-4 border-b border-gray-100">
          <div>
            <div className="flex items-center gap-3">
              <TeamAvatar team={selectedTeam} size="md" />
              <div>
                <h2 className="text-2xl font-black text-gray-900">
                  {selectedTeam?.name} — Roto Gap & Pace Analyzer
                </h2>
                <p className="text-xs font-semibold text-blue-600">
                  Owner: {selectedTeam?.owner} · Total Roto Points: <span className="font-mono text-sm font-black">{totalRotoPoints % 1 === 0 ? totalRotoPoints : totalRotoPoints.toFixed(1)}</span>
                </p>
              </div>
            </div>
            <p className="text-xs text-gray-500 mt-2">
              Inspect your current position in each category, the exact gap to gain +1 Roto Point, the downside pace risk of getting caught (-1 Roto Point), numerator buffers, and required daily pace.
            </p>
          </div>

          <div className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-2 text-right">
            <span className="text-[11px] font-bold uppercase text-blue-700 block tracking-wider">Season Pace Clock</span>
            <span className="text-sm font-black text-blue-950 font-mono">{daysRemainingInfo.label}</span>
          </div>
        </div>

        {/* Owner Selector Pills */}
        <div className="mt-4">
          <label className="text-xs font-bold text-gray-500 uppercase tracking-wider block mb-2">
            Select Team to Analyze:
          </label>
          <div className="flex items-center gap-2 flex-wrap">
            {humanTeamIds.map(id => {
              const team = TEAMS[id];
              const isSelected = id === selectedOwnerId;
              const pts = rotoPointsMap[id]?.total || 0;
              return (
                <button
                  key={id}
                  onClick={() => setSelectedOwnerId(id)}
                  className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${
                    isSelected
                      ? 'bg-blue-700 text-white shadow-md scale-105'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  <TeamAvatar team={team} size="sm" />
                  <span>{team.name}</span>
                  <span className={`font-mono text-[11px] px-1.5 py-0.5 rounded ${isSelected ? 'bg-blue-800 text-blue-100' : 'bg-gray-200 text-gray-600'}`}>
                    {pts % 1 === 0 ? pts : pts.toFixed(1)} pt
                  </span>
                </button>
              );
            })}
          </div>
        </div>

        {/* Quick Strategic Summary: Upside Opportunities vs Downside Risks */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-5 pt-4 border-t border-gray-100">
          <div className="bg-blue-50/70 border border-blue-200/80 rounded-xl p-3.5">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-black uppercase text-blue-900 tracking-wider flex items-center gap-1.5">
                <span>🎯 Top Upside Targets (+1 Roto Pt)</span>
              </span>
              <span className="text-[10px] text-blue-700 font-semibold">Closest deficits to overtake</span>
            </div>
            <div className="space-y-1.5">
              {topOpportunities.length > 0 ? (
                topOpportunities.map(opp => (
                  <div key={opp.cat.id} className="flex items-center justify-between text-xs bg-white/90 px-2.5 py-1.5 rounded border border-blue-200/60 font-sans">
                    <span className="font-bold text-gray-900 flex items-center gap-1.5">
                      <span>{opp.cat.icon}</span>
                      <span>{opp.cat.name}</span>
                      <span className="text-[11px] font-normal text-gray-500 font-mono">(#{opp.myRank})</span>
                    </span>
                    <span className="font-mono text-blue-800 font-bold text-[11px]">
                      {!opp.cat.isRate ? `-${opp.targetAhead.gap} ${opp.cat.unit} (+${opp.targetAhead.rateNeededPerDay}/day)` : `${opp.targetAhead.numeratorDifference?.needed} (${opp.targetAhead.rateNeededPerDay})`}
                    </span>
                  </div>
                ))
              ) : (
                <div className="text-xs text-blue-700 italic">Holding 1st place in all categories!</div>
              )}
            </div>
          </div>

          <div className="bg-rose-50/70 border border-rose-200/80 rounded-xl p-3.5">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-black uppercase text-rose-900 tracking-wider flex items-center gap-1.5">
                <span>🛡️ Highest Downside Risks (-1 Roto Pt)</span>
              </span>
              <span className="text-[10px] text-rose-700 font-semibold">Most vulnerable leads</span>
            </div>
            <div className="space-y-1.5">
              {highestRisks.length > 0 ? (
                highestRisks.map(risk => (
                  <div key={risk.cat.id} className="flex items-center justify-between text-xs bg-white/90 px-2.5 py-1.5 rounded border border-rose-200/60 font-sans">
                    <span className="font-bold text-gray-900 flex items-center gap-1.5">
                      <span>{risk.cat.icon}</span>
                      <span>{risk.cat.name}</span>
                      <span className="text-[11px] font-normal text-gray-500 font-mono">(#{risk.myRank})</span>
                    </span>
                    <span className="font-mono text-rose-800 font-bold text-[11px]">
                      {!risk.cat.isRate ? `+${risk.defendingBehind.cushion} lead (${risk.defendingBehind.rateRiskPerDay})` : `${risk.defendingBehind.numeratorBuffer?.needed} (${risk.defendingBehind.rateRiskPerDay})`}
                    </span>
                  </div>
                ))
              ) : (
                <div className="text-xs text-rose-700 italic">No vulnerable leads (category floor).</div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* 10 Category Gap Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {categoryAnalysis.map(({ cat, myRank, myVal, myRoto, leader, targetAhead, defendingBehind }) => {
          const isFirstPlace = myRank === 1;
          const isLastPlace = myRank === humanTeamIds.length;

          return (
            <div
              key={cat.id}
              className={`rounded-xl border shadow-sm p-5 transition-all hover:shadow-md ${
                isFirstPlace
                  ? 'bg-gradient-to-br from-amber-50/50 to-white border-amber-300'
                  : 'bg-white border-gray-200'
              }`}
            >
              {/* Category Header */}
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <div className="flex items-center gap-2">
                  <span className="text-2xl select-none">{cat.icon}</span>
                  <div>
                    <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
                      {cat.name} ({cat.id})
                      {isFirstPlace && (
                        <span className="bg-amber-100 text-amber-800 text-[10px] font-black uppercase px-2 py-0.5 rounded-full border border-amber-300">
                          👑 Category Leader
                        </span>
                      )}
                    </h3>
                    <span className="text-xs text-gray-400">
                      {cat.isRate ? 'Rate Statistic (Ratio)' : 'Counting Statistic'} · {cat.type === 'high' ? 'Higher is better' : 'Lower is better'}
                    </span>
                  </div>
                </div>

                <div className="text-right">
                  <span className="text-xs font-bold text-gray-400 block uppercase">Rank</span>
                  <span className={`text-lg font-black font-mono ${isFirstPlace ? 'text-amber-600' : isLastPlace ? 'text-red-500' : 'text-blue-700'}`}>
                    #{myRank} <span className="text-xs font-normal text-gray-500">of {humanTeamIds.length}</span>
                  </span>
                </div>
              </div>

              {/* Current Value & Roto Points */}
              <div className="grid grid-cols-2 gap-3 py-3 my-2 bg-gray-50/80 rounded-lg px-3 border border-gray-100">
                <div>
                  <span className="text-[10px] font-bold uppercase text-gray-400 tracking-wider block">Your Value</span>
                  <span className="text-lg font-black font-mono text-gray-900">{myVal}</span>
                </div>
                <div className="text-right">
                  <span className="text-[10px] font-bold uppercase text-gray-400 tracking-wider block">Roto Points</span>
                  <span className="text-lg font-black font-mono text-blue-700">
                    {myRoto % 1 === 0 ? myRoto : myRoto.toFixed(1)} <span className="text-xs font-normal text-gray-500">pts</span>
                  </span>
                </div>
              </div>

              {/* Target Ahead Section (+1 Roto Point) */}
              {targetAhead ? (
                <div className="bg-blue-50/70 border border-blue-200 rounded-lg p-3 space-y-2 mt-3">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-bold text-blue-900 flex items-center gap-1">
                      <span>🎯 Target Ahead (+1 Roto Point):</span>
                    </span>
                    <button
                      onClick={() => onOwnerClick?.(targetAhead.team)}
                      className="font-bold text-blue-700 hover:underline flex items-center gap-1"
                    >
                      <TeamAvatar team={targetAhead.team} size="sm" />
                      <span>{targetAhead.team.name}</span>
                    </button>
                  </div>

                  <div className="flex items-baseline justify-between text-xs font-mono">
                    <span className="text-gray-600">{targetAhead.team.name}&apos;s Total: <strong>{targetAhead.val}</strong></span>
                    {!cat.isRate ? (
                      <span className="font-bold text-blue-800 bg-blue-100 px-2 py-0.5 rounded">
                        Deficit: {targetAhead.gap} {cat.unit}
                      </span>
                    ) : (
                      <span className="font-bold text-blue-800 bg-blue-100 px-2 py-0.5 rounded">
                        {targetAhead.numeratorDifference?.needed}
                      </span>
                    )}
                  </div>

                  {/* Ratio Numerator Specifics */}
                  {cat.isRate && targetAhead.numeratorDifference && (
                    <div className="text-[11px] text-blue-900 bg-white/80 p-2 rounded border border-blue-200/60 leading-relaxed font-sans">
                      <div className="font-bold text-blue-950 mb-0.5">
                        📐 Numerator Difference ({targetAhead.numeratorDifference.label}):
                      </div>
                      <div>{targetAhead.numeratorDifference.context}</div>
                    </div>
                  )}

                  {/* Pace Required */}
                  {targetAhead.rateNeededPerDay && (
                    <div className="flex items-center justify-between text-xs pt-1 border-t border-blue-200/60 font-sans">
                      <span className="text-gray-600 font-medium">Daily Pace to Catch Up:</span>
                      <span className="font-bold font-mono text-blue-800 bg-white px-2 py-0.5 rounded border border-blue-300">
                        {cat.isRate ? targetAhead.rateNeededPerDay : `+${targetAhead.rateNeededPerDay} ${cat.unit} / day`}
                      </span>
                    </div>
                  )}
                </div>
              ) : (
                <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 text-center text-xs font-bold text-amber-800 mt-3">
                  🌟 You currently hold 1st Place (Max {humanTeamIds.length} Roto Points)!
                </div>
              )}

              {/* Defending Behind & Downside Risk Section (-1 Roto Point) */}
              {defendingBehind ? (
                <div className="bg-rose-50/70 border border-rose-200 rounded-lg p-3 space-y-2 mt-3">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-bold text-rose-950 flex items-center gap-1">
                      <span>🛡️ Downside Risk (-1 Roto Point):</span>
                    </span>
                    <button
                      onClick={() => onOwnerClick?.(defendingBehind.team)}
                      className="font-bold text-rose-700 hover:underline flex items-center gap-1"
                    >
                      <TeamAvatar team={defendingBehind.team} size="sm" />
                      <span>{defendingBehind.team.name}</span>
                    </button>
                  </div>

                  <div className="flex items-baseline justify-between text-xs font-mono">
                    <span className="text-gray-600">{defendingBehind.team.name}&apos;s Total: <strong>{defendingBehind.val}</strong></span>
                    {!cat.isRate ? (
                      <span className="font-bold text-rose-800 bg-rose-100 px-2 py-0.5 rounded">
                        Lead Cushion: +{defendingBehind.cushion} {cat.unit}
                      </span>
                    ) : (
                      <span className="font-bold text-rose-800 bg-rose-100 px-2 py-0.5 rounded">
                        {defendingBehind.numeratorBuffer?.needed || `Lead: +${defendingBehind.cushion.toFixed(3)}`}
                      </span>
                    )}
                  </div>

                  {/* Ratio Numerator Specifics */}
                  {cat.isRate && defendingBehind.numeratorBuffer && (
                    <div className="text-[11px] text-rose-950 bg-white/80 p-2 rounded border border-rose-200/60 leading-relaxed font-sans">
                      <div className="font-bold text-rose-950 mb-0.5">
                        📐 Buffer Allowance ({defendingBehind.numeratorBuffer.label}):
                      </div>
                      <div>{defendingBehind.numeratorBuffer.context}</div>
                    </div>
                  )}

                  {/* Downside Pace Risk */}
                  {defendingBehind.rateRiskPerDay && (
                    <div className="flex items-center justify-between text-xs pt-1 border-t border-rose-200/60 font-sans">
                      <span className="text-gray-700 font-medium">Chaser Breakeven Pace:</span>
                      <span className="font-bold font-mono text-rose-800 bg-white px-2 py-0.5 rounded border border-rose-300">
                        {cat.isRate ? `Caught if outpaced by ${defendingBehind.rateRiskPerDay}` : `Caught if chaser outpaces by ${defendingBehind.rateRiskPerDay}`}
                      </span>
                    </div>
                  )}
                </div>
              ) : (
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center text-xs font-bold text-gray-500 mt-3">
                  🛡️ Category Floor: You are in last place ({myRoto % 1 === 0 ? myRoto : myRoto.toFixed(1)} pt floor — cannot drop further).
                </div>
              )}

              {/* Leader Callout if not #1 */}
              {!isFirstPlace && (
                <div className="text-[11px] text-gray-400 text-right mt-1 pt-1">
                  Category Leader: <strong className="text-gray-700">{leader.team.name}</strong> ({leader.val})
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
