/**
 * @file useProcessedSchedule.js
 * @description Processes and computes four-phase fantasy schedule matchups, dynamic playoff/split
 * bracket seeding, Ghost team averages, and live category scoring results.
 */

import { useMemo } from 'react';
import { TEAMS } from '../constants/league';
import { getPeriodRangeForWeek } from '../schedule';
import { aggregateStats, calculateMatchupResult, calculateTrioMatchupResult } from '../utils/scoring';
import { calculateStandings } from '../utils/standings';

export function useProcessedSchedule(rawData = [], baseSchedule = []) {
  const processedWeeks = useMemo(() => {
    if (!rawData.length || !baseSchedule.length) return [];

    const resolveMatchupStats = (matchup, week) => {
      // ---------------------------------------------------
      // 1. TRIO MATCHUPS (Phases 1 & 2)
      // ---------------------------------------------------
      if (matchup.teamIds) {
        // Handle Placeholders (e.g. SEED_1)
        if (matchup.teamIds.some(id => typeof id === 'string')) {
          return {
            ...matchup,
            isPlaceholder: true,
            type: 'trio',
            teams: matchup.teamIds.map(id => ({ name: id, owner: 'TBD', id: null })),
            teamStats: {},
            teamRecords: {},
            result: {},
          };
        }

        const { startId, endId } = getPeriodRangeForWeek(week);
        const teamRecords = {};
        matchup.teamIds.forEach(id => { teamRecords[id] = []; });

        for (const r of rawData) {
          if (r.scoring_period_id >= startId && r.scoring_period_id <= endId) {
            if (teamRecords[r.team_id]) {
              teamRecords[r.team_id].push(r);
            }
          }
        }

        const teamStats = {};
        matchup.teamIds.forEach(id => {
          teamStats[id] = aggregateStats(teamRecords[id]);
        });

        // Fail gracefully if trio scoring isn't built yet
        let result = {};
        if (typeof calculateTrioMatchupResult === 'function') {
          result = calculateTrioMatchupResult(teamStats, matchup.teamIds);
        }

        const teams = matchup.teamIds.map(id => ({ ...(TEAMS[id] || { name: 'Unknown', owner: '' }), id }));

        return {
          matchupId: matchup.id,
          type: 'trio',
          label: matchup.label,
          teams,
          teamStats,
          teamRecords,
          result,
          isPlaceholder: false,
        };
      }

      // ---------------------------------------------------
      // 2. HEAD-TO-HEAD MATCHUPS (Phases 3 & 4)
      // ---------------------------------------------------
      if (typeof matchup.homeTeamId === 'string' || typeof matchup.awayTeamId === 'string') {
        return {
          ...matchup,
          isPlaceholder: true,
          type: 'h2h',
          homeTeam: { name: matchup.homeTeamId, owner: 'TBD', id: null },
          awayTeam: { name: matchup.awayTeamId, owner: 'TBD', id: null },
          homeStats: {},
          awayStats: {},
          result: { homeScore: 0, awayScore: 0, ties: 0 },
          homeRecords: [],
          awayRecords: [],
        };
      }

      const { startId, endId } = getPeriodRangeForWeek(week);
      const homeRecords = [];
      const awayRecords = [];
      const allHumanRecords = [];

      for (const r of rawData) {
        if (r.scoring_period_id >= startId && r.scoring_period_id <= endId) {
          if (r.team_id == matchup.homeTeamId) homeRecords.push(r);
          else if (r.team_id == matchup.awayTeamId) awayRecords.push(r);

          if (r.team_id != 99) allHumanRecords.push(r);
        }
      }

      const computeAverageTeamStats = (records) => {
        const totalStats = aggregateStats(records);
        const avgStats = { ...totalStats };
        const numTeams = 9; // 9 human teams in the league

        // Divide counting components by 9 to get the average
        const fieldsToDivide = [
          'R', 'HR', 'RBI', 'SB', 'K', 'QS', 'SV+HDs', 'ER', 'IP',
          'BB_Allowed', 'H_Allowed', 'OBP_num', 'OBP_denom', 'PA',
          'AB', 'H', 'BB', 'HBP', 'SF',
        ];
        fieldsToDivide.forEach(key => {
          if (avgStats[key]) avgStats[key] = avgStats[key] / numTeams;
        });

        // Recalculate rate stats from the divided components
        const obpDenom = (avgStats.AB + avgStats.BB + avgStats.HBP + avgStats.SF) || avgStats.OBP_denom || avgStats.PA;
        const obpNum = (avgStats.H + avgStats.BB + avgStats.HBP) || avgStats.OBP_num;
        const obpRaw = obpDenom > 0 ? obpNum / obpDenom : 0;
        avgStats.OBP_raw = obpRaw;
        avgStats.OBP = obpDenom > 0 ? obpRaw.toFixed(4) : '.0000';
        avgStats.ERA = avgStats.IP > 0 ? ((avgStats.ER * 9) / avgStats.IP).toFixed(2) : '0.00';
        avgStats.WHIP = avgStats.IP > 0 ? ((avgStats.BB_Allowed + avgStats.H_Allowed) / avgStats.IP).toFixed(2) : '0.00';

        return avgStats;
      };

      const homeStats = matchup.homeTeamId == 99 ? computeAverageTeamStats(allHumanRecords) : aggregateStats(homeRecords);
      const awayStats = matchup.awayTeamId == 99 ? computeAverageTeamStats(allHumanRecords) : aggregateStats(awayRecords);
      const result = calculateMatchupResult(homeStats, awayStats);

      const homeTeamInfo = TEAMS[matchup.homeTeamId] || { name: 'Unknown', owner: '' };
      const awayTeamInfo = TEAMS[matchup.awayTeamId] || { name: 'Unknown', owner: '' };

      return {
        matchupId: matchup.id,
        type: 'h2h',
        label: matchup.label,
        homeTeam: { ...homeTeamInfo, id: matchup.homeTeamId },
        awayTeam: { ...awayTeamInfo, id: matchup.awayTeamId },
        homeStats,
        awayStats,
        result,
        homeRecords,
        awayRecords,
        isPlaceholder: false,
      };
    };

    let resolvedSchedule = [];

    // --- STEP A: PROCESS PHASE 1 (Weeks 1-12) ---
    for (let i = 0; i < 12; i++) {
      const week = baseSchedule[i];
      if (!week) continue;
      const resolvedMatchups = week.matchups.map(m => resolveMatchupStats(m, week));
      resolvedSchedule.push({ ...week, matchups: resolvedMatchups });
    }

    // --- STEP B: PROCESS PHASE 2 (Weeks 13-14) MID-SEASON CHAMPIONSHIP ---
    const standingsAfter12 = calculateStandings(resolvedSchedule, 12);
    const midSeeds = {};
    standingsAfter12.forEach((team, index) => {
      midSeeds[`SEED_${index + 1}`] = team.id;
    });

    for (let i = 12; i < 14; i++) {
      const week = baseSchedule[i];
      if (!week) continue;
      const dynamicMatchups = week.matchups.map(m => {
        if (m.teamIds) {
          const realIds = m.teamIds.map(id => typeof id === 'string' ? midSeeds[id] : id);
          return { ...m, teamIds: realIds };
        }
        return m;
      });
      const resolvedMatchups = dynamicMatchups.map(m => resolveMatchupStats(m, week));
      resolvedSchedule.push({ ...week, matchups: resolvedMatchups });
    }

    // --- STEP C: PROCESS PHASE 3 (Weeks 15-23) SPLIT LEAGUES ---
    const standingsAfter14 = calculateStandings(resolvedSchedule, 14);
    const splitSeeds = {};
    standingsAfter14.forEach((team, index) => {
      splitSeeds[`SEED_${index + 1}`] = team.id;
    });

    for (let i = 14; i < 23; i++) {
      const week = baseSchedule[i];
      if (!week) continue;
      const dynamicMatchups = week.matchups.map(m => {
        if (m.homeTeamId !== undefined && m.awayTeamId !== undefined) {
          const realHome = typeof m.homeTeamId === 'string' ? splitSeeds[m.homeTeamId] : m.homeTeamId;
          const realAway = typeof m.awayTeamId === 'string' ? splitSeeds[m.awayTeamId] : m.awayTeamId;
          return { ...m, homeTeamId: realHome, awayTeamId: realAway };
        }
        return m;
      });

      const resolvedMatchups = dynamicMatchups.map(m => resolveMatchupStats(m, week));
      resolvedSchedule.push({ ...week, matchups: resolvedMatchups });
    }

    // --- STEP D: PROCESS PHASE 4 (Weeks 24-25) PLAYOFFS ---
    // Seeding is determined by Phase 3 (Weeks 15-23) head-to-head records
    // within each respective tier (Winners League Seeds 1-4, Consolation League Seeds 5-9).
    const phase3Standings = calculateStandings(resolvedSchedule, { startWeek: 15, upToWeek: 23, phase: 3 });
    const standingsAfter23 = calculateStandings(resolvedSchedule, 23);

    const getFullSeasonRank = (teamId) => {
      const idx = standingsAfter23.findIndex(t => t.id === teamId);
      return idx >= 0 ? idx : 999;
    };

    const sortTier = (teamA, teamB) => {
      const totalA = teamA.wins + teamA.losses + teamA.ties;
      const totalB = teamB.wins + teamB.losses + teamB.ties;
      const pctA = totalA > 0 ? (teamA.wins + teamA.ties * 0.5) / totalA : 0;
      const pctB = totalB > 0 ? (teamB.wins + teamB.ties * 0.5) / totalB : 0;
      if (Math.abs(pctB - pctA) > 0.0001) return pctB - pctA;
      if (teamB.score !== teamA.score) return teamB.score - teamA.score;
      return getFullSeasonRank(teamA.id) - getFullSeasonRank(teamB.id);
    };

    const winnersTeamIds = [
      splitSeeds.SEED_1,
      splitSeeds.SEED_2,
      splitSeeds.SEED_3,
      splitSeeds.SEED_4,
    ].filter(Boolean);

    const champSeeds = winnersTeamIds
      .map(id => phase3Standings.find(t => t.id === id) || { id, wins: 0, losses: 0, ties: 0, score: 0 })
      .sort(sortTier);

    const consolTeamIds = [
      splitSeeds.SEED_5,
      splitSeeds.SEED_6,
      splitSeeds.SEED_7,
      splitSeeds.SEED_8,
      splitSeeds.SEED_9,
    ].filter(Boolean);

    const consolSeeds = consolTeamIds
      .map(id => phase3Standings.find(t => t.id === id) || { id, wins: 0, losses: 0, ties: 0, score: 0 })
      .sort(sortTier);

    if (baseSchedule[23]) {
      const week24 = baseSchedule[23];
      const sfMatchups = [
        { id: 'sf1', homeTeamId: champSeeds[0]?.id, awayTeamId: champSeeds[3]?.id, label: 'Semi-Final A' },
        { id: 'sf2', homeTeamId: champSeeds[1]?.id, awayTeamId: champSeeds[2]?.id, label: 'Semi-Final B' },
        { id: 'c1', homeTeamId: consolSeeds[0]?.id, awayTeamId: consolSeeds[3]?.id, label: 'Consolation A' },
        { id: 'c2', homeTeamId: consolSeeds[1]?.id, awayTeamId: consolSeeds[2]?.id, label: 'Consolation B' },
      ];

      const resolvedSF = sfMatchups.map(m => resolveMatchupStats(m, week24));
      resolvedSchedule.push({ ...week24, matchups: resolvedSF });
    }

    if (baseSchedule[24]) {
      const week25 = baseSchedule[24];
      const prevWeek = resolvedSchedule[23];

      const isUnplayed = (m) => {
        if (!m || !m.result) return true;
        return m.result.homeScore === 0 && m.result.awayScore === 0 && (m.result.ties === 0 || m.result.ties === undefined);
      };

      const getWinner = (matchId) => {
        const m = prevWeek?.matchups.find(pm => pm.id === matchId || pm.matchupId === matchId);
        if (isUnplayed(m)) return 'TBD';
        return m.result.homeScore >= m.result.awayScore ? m.homeTeam?.id : m.awayTeam?.id;
      };

      const getLoser = (matchId) => {
        const m = prevWeek?.matchups.find(pm => pm.id === matchId || pm.matchupId === matchId);
        if (isUnplayed(m)) return 'TBD';
        return m.result.homeScore >= m.result.awayScore ? m.awayTeam?.id : m.homeTeam?.id;
      };

      const finalMatchups = [
        { id: 'final', homeTeamId: getWinner('sf1'), awayTeamId: getWinner('sf2'), label: '🏆 Championship' },
        { id: '3rd', homeTeamId: getLoser('sf1'), awayTeamId: getLoser('sf2'), label: '3rd Place Match' },
        { id: 'c_final', homeTeamId: getWinner('c1'), awayTeamId: getWinner('c2'), label: 'Consolation Final (5th Place)' },
        { id: 'c_3rd', homeTeamId: getLoser('c1'), awayTeamId: getLoser('c2'), label: '7th Place Match' },
      ];

      const resolvedFinals = finalMatchups.map(m => resolveMatchupStats(m, week25));
      resolvedSchedule.push({ ...week25, matchups: resolvedFinals });
    }

    return resolvedSchedule;
  }, [rawData, baseSchedule]);

  return processedWeeks;
}
