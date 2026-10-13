// src/views/PlayerImpactSimulatorView.jsx
import React, { useState, useMemo, useCallback } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, calculateRotoPoints, SCORING_CATS } from '../utils/scoring';
import {
  BATTING_CATS,
  PITCHING_CATS,
  buildPlayerProfiles,
  computeLeagueAverages,
  runImpactSimulation
} from './simulator/services/simulatorEngine';
import SimulatorHeader from './simulator/components/SimulatorHeader';
import SimulatorRotoSummary from './simulator/components/SimulatorRotoSummary';
import SimulatorReplacementControls from './simulator/components/SimulatorReplacementControls';
import SimulatorRosterWorkbench from './simulator/components/SimulatorRosterWorkbench';
import SimulatorStandingsRipple from './simulator/components/SimulatorStandingsRipple';

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
  const [pitcherReplacementMode, setPitcherReplacementMode] = useState('empty');
  const [customPitcherRates, setCustomPitcherRates] = useState({
    era: 4.50,
    whip: 1.35,
    k9: 8.0
  });

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
    return buildPlayerProfiles(teamActiveRecords[selectedTeamId] || [], selectedTeamId, baselineTeamStatsMap);
  }, [teamActiveRecords, selectedTeamId, baselineTeamStatsMap]);

  // 4. League-wide averages for replacement benchmark
  const leagueAverages = useMemo(() => {
    return computeLeagueAverages(baselineTeamStatsMap);
  }, [baselineTeamStatsMap]);

  // 5. Determine which records are excluded by active player/outing filters
  const isRecordExcluded = useCallback((record) => {
    if (excludedPlayerIds.has(record.player_id)) return true;
    const key = `${record.player_id}_${record.scoring_period_id}`;
    if (excludedOutingKeys.has(key)) return true;
    return false;
  }, [excludedPlayerIds, excludedOutingKeys]);

  // 6. Calculate simulated active team records and aggregate stats
  const simulationResults = useMemo(() => {
    return runImpactSimulation({
      rawActive: teamActiveRecords[selectedTeamId] || [],
      isRecordExcluded,
      pitcherReplacementMode,
      customPitcherRates,
      batterReplacementMode,
      customBatterRates,
      leagueAverages,
      baselineTeamStatsMap,
      selectedTeamId
    });
  }, [
    teamActiveRecords,
    selectedTeamId,
    isRecordExcluded,
    pitcherReplacementMode,
    customPitcherRates,
    batterReplacementMode,
    customBatterRates,
    leagueAverages,
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

  const totalExclusionsCount = excludedPlayerIds.size + excludedOutingKeys.size;

  // Category counts and splits
  const allCategoryKeys = Object.keys(SCORING_CATS);
  const categoriesUp = allCategoryKeys.filter(c => (simRoto[c] || 0) > (baseRoto[c] || 0) + 0.001).length;
  const categoriesDown = allCategoryKeys.filter(c => (simRoto[c] || 0) < (baseRoto[c] || 0) - 0.001).length;
  const categoriesEven = 10 - categoriesUp - categoriesDown;

  const battingPointsDelta = BATTING_CATS.reduce((sum, c) => sum + ((simRoto[c] || 0) - (baseRoto[c] || 0)), 0);
  const pitchingPointsDelta = PITCHING_CATS.reduce((sum, c) => sum + ((simRoto[c] || 0) - (baseRoto[c] || 0)), 0);

  return (
    <div className="space-y-6">
      {/* 1. VIEW HEADER & TEAM SELECTOR */}
      <SimulatorHeader
        selectedTeamId={selectedTeamId}
        setSelectedTeamId={setSelectedTeamId}
        humanTeams={humanTeams}
        handleClearAllExclusions={handleClearAllExclusions}
        handleExcludeTop3EraKillers={handleExcludeTop3EraKillers}
        handleExcludeSubInningMeltdowns={handleExcludeSubInningMeltdowns}
        handleExcludeTop3ObpAnchors={handleExcludeTop3ObpAnchors}
        handleExcludeHitlessSlumpDays={handleExcludeHitlessSlumpDays}
        totalExclusionsCount={totalExclusionsCount}
        excludedPlayerIdsCount={excludedPlayerIds.size}
        excludedOutingKeysCount={excludedOutingKeys.size}
      />

      {/* 2. FULL 10-CATEGORY ROTISSERIE SUMMARY DASHBOARD */}
      <SimulatorRotoSummary
        activeTeamObj={activeTeamObj}
        leagueStandingsComparison={leagueStandingsComparison}
        baseRoto={baseRoto}
        simRoto={simRoto}
        baseTeamStats={baseTeamStats}
        simTeamStats={simTeamStats}
        categoriesUp={categoriesUp}
        categoriesDown={categoriesDown}
        categoriesEven={categoriesEven}
        battingPointsDelta={battingPointsDelta}
        pitchingPointsDelta={pitchingPointsDelta}
      />

      {/* 3. REPLACEMENT PLAYER MODELING CONTROLS */}
      <SimulatorReplacementControls
        simulationResults={simulationResults}
        pitcherReplacementMode={pitcherReplacementMode}
        setPitcherReplacementMode={setPitcherReplacementMode}
        customPitcherRates={customPitcherRates}
        setCustomPitcherRates={setCustomPitcherRates}
        batterReplacementMode={batterReplacementMode}
        setBatterReplacementMode={setBatterReplacementMode}
        customBatterRates={customBatterRates}
        setCustomBatterRates={setCustomBatterRates}
        leagueAverages={leagueAverages}
      />

      {/* 4. INTERACTIVE ROSTER EXCLUSION WORKBENCH */}
      <SimulatorRosterWorkbench
        filteredPlayers={filteredPlayers}
        playerFilter={playerFilter}
        handleFilterTabClick={handleFilterTabClick}
        sortBy={sortBy}
        setSortBy={setSortBy}
        searchQuery={searchQuery}
        setSearchQuery={setSearchQuery}
        excludedPlayerIds={excludedPlayerIds}
        excludedOutingKeys={excludedOutingKeys}
        expandedPlayerIds={expandedPlayerIds}
        togglePlayerExclusion={togglePlayerExclusion}
        toggleOutingExclusion={toggleOutingExclusion}
        toggleAccordion={toggleAccordion}
        selectedSeason={selectedSeason}
        onPlayerClick={onPlayerClick}
      />

      {/* 5. LEAGUE-WIDE STANDINGS SHIFT TABLE */}
      <SimulatorStandingsRipple
        leagueStandingsComparison={leagueStandingsComparison}
        simulationResults={simulationResults}
        baselineRotoPoints={baselineRotoPoints}
        onOwnerClick={onOwnerClick}
      />
    </div>
  );
}
