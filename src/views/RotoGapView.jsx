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

      // Gap defending behind
      let cushionBehind = null;
      if (defendingBehind) {
        if (!isRate) {
          cushionBehind = Math.abs(myVal - defendingBehind.rawVal);
        } else {
          cushionBehind = Math.abs(myVal - defendingBehind.rawVal);
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
          cushion: cushionBehind
        } : null
      };
    });
  }, [selectedOwnerId, humanTeamIds, teamStatsMap, rotoPointsMap, daysRemainingInfo]);

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
              Inspect your current position in each category, the exact gap to gain +1 Roto Point, numerator differences for ratio stats, and required daily pace.
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

              {/* Defending Behind Section (-1 Roto Point Buffer) */}
              <div className="flex items-center justify-between text-xs pt-3 mt-3 border-t border-gray-100 text-gray-500">
                {defendingBehind ? (
                  <>
                    <span className="truncate">
                      Defending vs <strong>{defendingBehind.team.name}</strong> ({defendingBehind.val}):
                    </span>
                    <span className="font-mono font-bold text-emerald-600 bg-emerald-50 px-2 py-0.5 rounded border border-emerald-200 whitespace-nowrap ml-2">
                      +{cat.isRate ? defendingBehind.cushion.toFixed(3) : defendingBehind.cushion} lead
                    </span>
                  </>
                ) : (
                  <span className="text-gray-400 italic">No teams behind you in this category.</span>
                )}
              </div>

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
