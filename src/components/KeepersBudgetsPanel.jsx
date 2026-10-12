import React, { useState, useMemo, useEffect } from 'react';
import { useAuth } from '../context/useAuth';
import { supabase } from '../supabaseClient';
import KeeperCalculationsView from '../views/KeeperCalculationsView';
import defaultCalculations from '../data/keeperCalculations.json';

import {
  getCompBuyPrices,
  getCompSellPrices,
  getKeeperSlots,
  DRAFT_MANAGERS,
  normalizeManager,
  getActualFinish,
  calculateKeeperCostFromRank,
  isSameOwner
} from './keepers/services/keeperPricing';

import KeepersHeaderNav from './keepers/components/KeepersHeaderNav';
import KeeperMatrixTab from './keepers/tabs/KeeperMatrixTab';
import RosterBreakdownTab from './keepers/tabs/RosterBreakdownTab';
import CompSimulatorTab from './keepers/tabs/CompSimulatorTab';
import KeeperPlannerTab from './keepers/tabs/KeeperPlannerTab';
import LeagueSettingsTab from './keepers/tabs/LeagueSettingsTab';

const HASH_BY_KEEPER_TAB = {
  matrix: '#/keepers/summary',
  rosters: '#/keepers/rosters',
  simulator: '#/keepers/comp-picks',
  planner: '#/keepers/planner',
  calculations: '#/keeper-calculations',
  settings: '#/keepers/settings'
};

export default function KeepersBudgetsPanel({
  teamBudgets = [],
  compPicks = [],
  keepers = [],
  players = [],
  currentUser = 'Daniel',
  isCommissioner: propIsCommissioner = false,
  seasonYear = 2027,
  onSeasonYearChange,
  onPlayerClick,
  onRefresh,
  priorKeepers = [],
  initialTab = 'matrix'
}) {
  const { user, profile, isCommissioner: authIsCommissioner, effectiveOwner, governanceTitle = 'Commissioner' } = useAuth();
  const isCommissioner = propIsCommissioner || authIsCommissioner;

  const [activeTab, setActiveTab] = useState(initialTab || 'matrix');

  const compBuyPrices = useMemo(() => getCompBuyPrices(seasonYear), [seasonYear]);
  const compSellPrices = useMemo(() => getCompSellPrices(seasonYear), [seasonYear]);
  const keeperSlots = useMemo(() => getKeeperSlots(seasonYear), [seasonYear]);

  useEffect(() => {
    if (initialTab) {
      setActiveTab(initialTab);
    }
  }, [initialTab]);

  const handleTabChange = (tabKey) => {
    setActiveTab(tabKey);
    const targetHash = HASH_BY_KEEPER_TAB[tabKey];
    if (targetHash && window.location.hash !== targetHash) {
      window.location.hash = targetHash;
    }
  };

  const [selectedOwner, setSelectedOwner] = useState(currentUser || 'Daniel');
  const [keeperOwnerFilter, setKeeperOwnerFilter] = useState('ALL');

  // Simulation state for interactive comp pick simulator
  const [simulatedOwner, setSimulatedOwner] = useState(currentUser || 'Daniel');
  const [simBoughtRounds, setSimBoughtRounds] = useState({});
  const [simSoldRounds, setSimSoldRounds] = useState({});

  // Planner state for keeper what-if planning
  const [plannerOwner, setPlannerOwner] = useState(currentUser || 'Daniel');
  const [plannerScope, setPlannerScope] = useState('roster'); // 'roster' | 'roster_fa' | 'all'
  const [plannerSearch, setPlannerSearch] = useState('');
  const [replacedKeepers, setReplacedKeepers] = useState({}); // { slot: newPlayerObj }
  const [tokenSlot, setTokenSlot] = useState(null);

  // Offseason settings & Commissioner controls state
  const [leagueSettings, setLeagueSettings] = useState(null);
  const [deadlineInput, setDeadlineInput] = useState('');
  const [baselineBudgetInput, setBaselineBudgetInput] = useState(100);
  const [adjustmentsForm, setAdjustmentsForm] = useState({});
  const [savingKeepers, setSavingKeepers] = useState(false);
  const [savingCompPicks, setSavingCompPicks] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);
  const [savingAdjustments, setSavingAdjustments] = useState(false);

  // Sync owners when effectiveOwner changes (e.g. via Commish Switcher)
  useEffect(() => {
    if (effectiveOwner) {
      setSelectedOwner(effectiveOwner);
      setSimulatedOwner(effectiveOwner);
      setPlannerOwner(effectiveOwner);
    }
  }, [effectiveOwner]);

  // Lookup map for prior year (2026) keeper cost
  const priorCostLookup = useMemo(() => {
    const map = new Map();
    const pKeepers = Array.isArray(priorKeepers) ? priorKeepers : (priorKeepers?.keepers || []);
    pKeepers.forEach(k => {
      if (k.espn_player_id) map.set(String(k.espn_player_id), parseFloat(k.cost) || 0);
      if (k.player_name) map.set(k.player_name.toLowerCase().trim(), parseFloat(k.cost) || 0);
    });
    const nameCounts = new Map();
    (players || []).forEach(p => {
      const pName = (p.Player || p.name || p.full_name || '').toLowerCase().trim();
      if (pName) nameCounts.set(pName, (nameCounts.get(pName) || 0) + 1);
    });

    (players || []).forEach(p => {
      const pid = String(p['ESPN PlayerID'] || p.id || '');
      const pName = (p.Player || p.name || p.full_name || '').toLowerCase().trim();
      const rawPrice = p['Hefty Keeper Price'];
      if (rawPrice !== undefined && rawPrice !== null && rawPrice !== '') {
        const val = parseFloat(rawPrice);
        if (!isNaN(val)) {
          if (pid && !map.has(pid)) map.set(pid, val);
          if (pName && !map.has(pName) && (nameCounts.get(pName) || 0) <= 1) {
            map.set(pName, val);
          }
        }
      }
    });
    return map;
  }, [priorKeepers, players]);

  const getPriorCost = React.useCallback((pid, name, defaultCost = 0) => {
    if (pid && priorCostLookup.has(String(pid))) return priorCostLookup.get(String(pid));
    if (name && priorCostLookup.has(name.toLowerCase().trim())) return priorCostLookup.get(name.toLowerCase().trim());
    return defaultCost;
  }, [priorCostLookup]);

  // Load league settings (Deadline & Base Budget)
  useEffect(() => {
    async function loadSettings() {
      try {
        const { data } = await supabase.from('league_settings').select('*').eq('league_id', 130215).single();
        if (data) {
          setLeagueSettings(data);
          if (data.keeper_lock_deadline) {
            const dt = new Date(data.keeper_lock_deadline);
            const localIso = new Date(dt.getTime() - dt.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
            setDeadlineInput(localIso);
          }
          if (data.base_budget) {
            setBaselineBudgetInput(data.base_budget);
          }
        }
      } catch (err) {
        console.warn('Could not load league settings:', err);
      }
    }
    loadSettings();
  }, []);

  // Initialize adjustments form when teamBudgets load
  useEffect(() => {
    if (teamBudgets?.length > 0) {
      const initial = {};
      teamBudgets.forEach(b => {
        initial[b.owner] = {
          adjustment: b.manual_adjustment || 0,
          notes: b.adjustment_notes || ''
        };
      });
      setAdjustmentsForm(initial);
    }
  }, [teamBudgets]);

  const isDeadlinePassed = useMemo(() => {
    const deadlineStr = leagueSettings?.keeper_lock_deadline || (seasonYear >= 2027 ? '2027-03-15T23:59:59Z' : null);
    if (!deadlineStr) return false;
    return new Date() > new Date(deadlineStr);
  }, [leagueSettings, seasonYear]);

  // Load active owner's real-world comp picks into simulator when owner changes
  const activeRealBudgets = useMemo(() => {
    const list = Array.isArray(teamBudgets) ? teamBudgets : (teamBudgets?.budgets || []);
    return list.find(b => normalizeManager(b.owner) === normalizeManager(simulatedOwner)) || {
      base_budget: 100,
      keeper_spend: 0,
      comp_pick_spend: 0,
      comp_pick_income: 0,
      final_budget: 100,
      net_picks: 0
    };
  }, [teamBudgets, simulatedOwner]);

  const realOwnerCompPicks = useMemo(() => {
    const list = Array.isArray(compPicks) ? compPicks : (compPicks?.comp_picks || []);
    return list.filter(p => normalizeManager(p.owner) === normalizeManager(simulatedOwner));
  }, [compPicks, simulatedOwner]);

  const resetSimulatorToReal = () => {
    const bought = {};
    const sold = {};
    realOwnerCompPicks.forEach(p => {
      if (p.action_type === 'BOUGHT') bought[p.round_num] = true;
      if (p.action_type === 'SOLD') sold[p.round_num] = true;
    });
    setSimBoughtRounds(bought);
    setSimSoldRounds(sold);
  };

  // Group keepers by owner
  const keepersByOwner = useMemo(() => {
    const map = {};
    DRAFT_MANAGERS.forEach(mgr => { map[mgr] = []; });
    const list = Array.isArray(keepers) ? keepers : (keepers?.keepers || []);
    list.forEach(k => {
      const owner = normalizeManager(k.owner);
      if (!map[owner]) map[owner] = [];
      map[owner].push({
        ...k,
        owner
      });
    });
    Object.keys(map).forEach(mgr => {
      map[mgr].sort((a, b) => (a.keeper_slot || 0) - (b.keeper_slot || 0));
    });
    return map;
  }, [keepers]);

  // Group comp picks by owner
  const compPicksByOwner = useMemo(() => {
    const map = {};
    DRAFT_MANAGERS.forEach(mgr => {
      map[mgr] = { bought: [], lost: [], sold: [] };
    });
    const list = Array.isArray(compPicks) ? compPicks : (compPicks?.comp_picks || []);
    list.forEach(p => {
      const owner = normalizeManager(p.owner);
      if (!map[owner]) map[owner] = { bought: [], lost: [], sold: [] };
      if (p.action_type === 'BOUGHT') map[owner].bought.push(p);
      else if (p.action_type === 'OFFSET_LOST') map[owner].lost.push(p);
      else if (p.action_type === 'SOLD') map[owner].sold.push(p);
    });
    return map;
  }, [compPicks]);

  const sortedTeamBudgets = useMemo(() => {
    return [...teamBudgets].sort((a, b) => {
      const finA = getActualFinish(seasonYear, a.owner, a.finish_rank);
      const finB = getActualFinish(seasonYear, b.owner, b.finish_rank);
      return (typeof finA === 'number' ? finA : 99) - (typeof finB === 'number' ? finB : 99);
    });
  }, [teamBudgets, seasonYear]);

  // Fast lookup for player projection stats from keeperCalculations.json and players prop
  const calcLookup = useMemo(() => {
    const byId = new Map();
    const byName = new Map();
    (defaultCalculations?.players || []).forEach(p => {
      const pid = String(p.espn_player_id || p.player_id || '').trim();
      const name = (p.player_name || '').toLowerCase().replace(/\./g, '').replace(/'/g, '').trim();
      if (pid) byId.set(pid, p);
      if (name) byName.set(name, p);
    });
    return { byId, byName };
  }, []);

  const keeperRotoData = useMemo(() => {
    const getKeeperStats = (keeper) => {
      const pid = String(keeper.espn_player_id || keeper.player_id || keeper.id || '').trim();
      const cleanName = (keeper.player_name || keeper.Player || keeper.name || '').toLowerCase().replace(/\./g, '').replace(/'/g, '').trim();
      const calcMatch = (pid ? calcLookup.byId.get(pid) : null) || (cleanName ? calcLookup.byName.get(cleanName) : null);

      const seasonStats = seasonYear >= 2027
        ? (calcMatch?.y2?.stats || calcMatch?.y1?.stats)
        : (calcMatch?.y1?.stats || calcMatch?.y2?.stats);

      const fallbackStats = keeper.stats || {};
      return {
        ...(fallbackStats || {}),
        ...(seasonStats || {})
      };
    };

    const CATEGORIES = [
      { key: 'R', label: 'R', type: 'batting', lowerIsBetter: false },
      { key: 'HR', label: 'HR', type: 'batting', lowerIsBetter: false },
      { key: 'RBI', label: 'RBI', type: 'batting', lowerIsBetter: false },
      { key: 'SB', label: 'SB', type: 'batting', lowerIsBetter: false },
      { key: 'OBP', label: 'OBP', type: 'batting', lowerIsBetter: false },
      { key: 'K', label: 'K', type: 'pitching', lowerIsBetter: false },
      { key: 'QS', label: 'QS', type: 'pitching', lowerIsBetter: false },
      { key: 'SVHD', label: 'SV+HD', type: 'pitching', lowerIsBetter: false },
      { key: 'ERA', label: 'ERA', type: 'pitching', lowerIsBetter: true },
      { key: 'WHIP', label: 'WHIP', type: 'pitching', lowerIsBetter: true }
    ];

    const teamAggs = DRAFT_MANAGERS.map(owner => {
      const ownerKeepers = keepersByOwner[owner] || [];
      const statsAccum = {
        R: 0, HR: 0, RBI: 0, SB: 0, AB: 0, totOBPNum: 0,
        K: 0, QS: 0, SVHD: 0, IP: 0, totERANum: 0, totWHIPNum: 0
      };

      const enrichedKeepers = ownerKeepers.map(k => {
        const s = getKeeperStats(k);
        const r = parseFloat(s.R) || 0;
        const hr = parseFloat(s.HR) || 0;
        const rbi = parseFloat(s.RBI) || 0;
        const sb = parseFloat(s.SB) || 0;
        const ab = parseFloat(s.AB) || 0;
        const obp = parseFloat(s.OBP) || 0;

        const kCount = parseFloat(s.SO !== undefined ? s.SO : (s.K !== undefined ? s.K : 0)) || 0;
        const qs = parseFloat(s.QS) || 0;
        const svhd = parseFloat(s.SVHD !== undefined ? s.SVHD : (s.SV_HD !== undefined ? s.SV_HD : 0)) || 0;
        const ip = parseFloat(s.IP) || 0;
        const era = parseFloat(s.ERA) || 0;
        const whip = parseFloat(s.WHIP) || 0;

        statsAccum.R += r;
        statsAccum.HR += hr;
        statsAccum.RBI += rbi;
        statsAccum.SB += sb;
        if (ab > 0 && obp > 0) {
          statsAccum.AB += ab;
          statsAccum.totOBPNum += ab * obp;
        }

        statsAccum.K += kCount;
        statsAccum.QS += qs;
        statsAccum.SVHD += svhd;
        if (ip > 0) {
          statsAccum.IP += ip;
          statsAccum.totERANum += era * ip;
          statsAccum.totWHIPNum += whip * ip;
        }

        return {
          ...k,
          stats: {
            R: r, HR: hr, RBI: rbi, SB: sb, OBP: obp,
            K: kCount, QS: qs, SVHD: svhd, ERA: era, WHIP: whip, IP: ip, AB: ab
          }
        };
      });

      const teamOBP = statsAccum.AB > 0 ? (statsAccum.totOBPNum / statsAccum.AB) : 0;
      const teamERA = statsAccum.IP > 0 ? (statsAccum.totERANum / statsAccum.IP) : 99.0;
      const teamWHIP = statsAccum.IP > 0 ? (statsAccum.totWHIPNum / statsAccum.IP) : 99.0;

      return {
        owner,
        keeperCount: ownerKeepers.length,
        keepers: enrichedKeepers,
        rawStats: {
          R: Math.round(statsAccum.R),
          HR: Math.round(statsAccum.HR),
          RBI: Math.round(statsAccum.RBI),
          SB: Math.round(statsAccum.SB),
          OBP: teamOBP,
          K: Math.round(statsAccum.K),
          QS: Math.round(statsAccum.QS * 10) / 10,
          SVHD: Math.round(statsAccum.SVHD),
          ERA: teamERA,
          WHIP: teamWHIP,
          IP: Math.round(statsAccum.IP * 10) / 10,
          AB: Math.round(statsAccum.AB)
        },
        points: {
          R: 0, HR: 0, RBI: 0, SB: 0, OBP: 0,
          K: 0, QS: 0, SVHD: 0, ERA: 0, WHIP: 0
        },
        battingPoints: 0,
        pitchingPoints: 0,
        totalPoints: 0
      };
    });

    CATEGORIES.forEach(({ key, lowerIsBetter }) => {
      const sorted = [...teamAggs].sort((a, b) => {
        const valA = a.rawStats[key];
        const valB = b.rawStats[key];
        return lowerIsBetter ? valA - valB : valB - valA;
      });

      let i = 0;
      while (i < sorted.length) {
        let j = i;
        const val = sorted[i].rawStats[key];
        while (j < sorted.length && Math.abs(sorted[j].rawStats[key] - val) < 0.0001) {
          j++;
        }
        let sumPts = 0;
        for (let k = i; k < j; k++) {
          sumPts += (9.0 - k);
        }
        const avgPts = sumPts / (j - i);
        for (let k = i; k < j; k++) {
          sorted[k].points[key] = avgPts;
        }
        i = j;
      }
    });

    teamAggs.forEach(t => {
      t.battingPoints = t.points.R + t.points.HR + t.points.RBI + t.points.SB + t.points.OBP;
      t.pitchingPoints = t.points.K + t.points.QS + t.points.SVHD + t.points.ERA + t.points.WHIP;
      t.totalPoints = t.battingPoints + t.pitchingPoints;
    });

    teamAggs.sort((a, b) => b.totalPoints - a.totalPoints);
    teamAggs.forEach((t, idx) => {
      t.rank = idx + 1;
    });

    const categoryLeaders = {};
    CATEGORIES.forEach(({ key, lowerIsBetter }) => {
      const leader = [...teamAggs].sort((a, b) => lowerIsBetter ? a.rawStats[key] - b.rawStats[key] : b.rawStats[key] - a.rawStats[key])[0];
      categoryLeaders[key] = {
        owner: leader.owner,
        val: leader.rawStats[key]
      };
    });

    return { teams: teamAggs, leaders: categoryLeaders, categories: CATEGORIES };
  }, [keepersByOwner, seasonYear, calcLookup]);

  // Dynamic simulation calculations
  const simCalculations = useMemo(() => {
    let spend = 0;
    let income = 0;
    const boughtList = [];
    const soldList = [];

    Object.keys(simBoughtRounds).forEach(rStr => {
      if (simBoughtRounds[rStr]) {
        const r = parseInt(rStr, 10);
        const cost = compBuyPrices[r] || (seasonYear >= 2027 ? (22 - r) : (21 - r));
        spend += cost;
        boughtList.push({ round: r, cost });
      }
    });

    Object.keys(simSoldRounds).forEach(rStr => {
      if (simSoldRounds[rStr]) {
        const r = parseInt(rStr, 10);
        const inc = compSellPrices[r] || 0;
        income += inc;
        soldList.push({ round: r, income: inc });
      }
    });

    boughtList.sort((a, b) => a.round - b.round);
    soldList.sort((a, b) => a.round - b.round);

    const offsetCount = boughtList.length;
    const offsetRounds = [];
    for (let i = 0; i < offsetCount; i++) {
      offsetRounds.push(32 - i);
    }

    const keeperSpend = activeRealBudgets.keeper_spend || 0;
    const baseBudget = activeRealBudgets.base_budget || 100;
    const simulatedRemaining = baseBudget - keeperSpend - spend + income;
    const netPicks = boughtList.length - soldList.length;

    return {
      spend,
      income,
      boughtList,
      soldList,
      offsetRounds,
      baseBudget,
      keeperSpend,
      simulatedRemaining,
      netPicks
    };
  }, [simBoughtRounds, simSoldRounds, activeRealBudgets, compBuyPrices, compSellPrices, seasonYear]);

  // Sync tokenSlot when plannerOwner changes or keepers update
  useEffect(() => {
    const originalKeepers = keepersByOwner[plannerOwner] || [];
    const existingToken = originalKeepers.find(k => k.token_applied);
    setTokenSlot(existingToken ? existingToken.keeper_slot : null);
  }, [plannerOwner, keepersByOwner]);

  // Planner calculations for keeper replacements
  const plannerData = useMemo(() => {
    const originalKeepers = keepersByOwner[plannerOwner] || [];
    const currentKeepers = keeperSlots.map(slotNum => {
      const existing = originalKeepers.find(k => k.keeper_slot === slotNum);
      const isToken = tokenSlot === slotNum;

      if (replacedKeepers[slotNum]) {
        const rep = replacedKeepers[slotNum];
        const rank = rep.rank || rep['Hefty Keeper Rank'] || rep['Hefty Single Season Rank'] || rep['Dynasty Rank'] || 150;
        const newCost = (rep.new_cost !== undefined && rep.new_cost !== null)
          ? parseFloat(rep.new_cost)
          : (rep.cost !== undefined && rep.cost !== null)
            ? parseFloat(rep.cost)
            : calculateKeeperCostFromRank(rank);
        const pid = rep.id || rep.espn_player_id || rep['ESPN PlayerID'];
        const pName = rep.name || rep.Player || rep.full_name || 'Selected Player';
        const priorCost = (rep.prior_cost !== undefined && rep.prior_cost !== null)
          ? parseFloat(rep.prior_cost)
          : getPriorCost(pid, pName, newCost);
        const midpoint = Math.floor((priorCost + newCost) / 2);
        const finalCost = isToken ? midpoint : newCost;
        const savings = isToken ? Math.max(0, newCost - midpoint) : 0;

        return {
          keeper_slot: slotNum,
          player_name: pName,
          espn_player_id: pid,
          position: rep.position || rep.Position || '---',
          mlb_team: rep.team || rep.Team || '---',
          rank: rank,
          cost: finalCost,
          token_applied: isToken,
          prior_cost: priorCost,
          new_cost: newCost,
          token_savings: savings,
          isReplaced: true,
          isEmpty: false
        };
      }

      if (existing) {
        const rank = existing.rank;
        const newCost = (existing.new_cost !== null && existing.new_cost !== undefined)
          ? parseFloat(existing.new_cost)
          : calculateKeeperCostFromRank(rank);
        const priorCost = (existing.prior_cost !== null && existing.prior_cost !== undefined)
          ? parseFloat(existing.prior_cost)
          : getPriorCost(existing.espn_player_id, existing.player_name, newCost);
        const midpoint = Math.floor((priorCost + newCost) / 2);
        const finalCost = isToken ? midpoint : newCost;
        const savings = isToken ? Math.max(0, newCost - midpoint) : 0;

        return {
          ...existing,
          rank,
          cost: finalCost,
          token_applied: isToken,
          prior_cost: priorCost,
          new_cost: newCost,
          token_savings: savings,
          isReplaced: false,
          isEmpty: false
        };
      }

      const alreadyChosenIds = new Set(
        [
          ...originalKeepers.map(k => String(k.espn_player_id || '')),
          ...Object.values(replacedKeepers).map(k => String(k.id || k.espn_player_id || k['ESPN PlayerID'] || ''))
        ].filter(Boolean)
      );

      const ownerRoster = (players || [])
        .filter(p => normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner))
        .filter(p => !alreadyChosenIds.has(String(p.id || p.espn_player_id || p['ESPN PlayerID'] || '')))
        .sort((a, b) => {
          const costA = a.cost !== undefined ? a.cost : calculateKeeperCostFromRank(a.rank);
          const costB = b.cost !== undefined ? b.cost : calculateKeeperCostFromRank(b.rank);
          if (costA !== costB) return costB - costA;
          return (a.rank || 999) - (b.rank || 999);
        });

      if (ownerRoster.length > 0) {
        const rep = ownerRoster[0];
        const rank = rep.rank || 999;
        const newCost = rep.cost !== undefined ? rep.cost : calculateKeeperCostFromRank(rank);
        const pid = rep.id || rep.espn_player_id || rep['ESPN PlayerID'];
        const pName = rep.name || rep.Player || rep.full_name || 'Selected Player';
        const priorCost = getPriorCost(pid, pName, newCost);
        const midpoint = Math.floor((priorCost + newCost) / 2);
        const finalCost = isToken ? midpoint : newCost;
        const savings = isToken ? Math.max(0, newCost - midpoint) : 0;

        return {
          keeper_slot: slotNum,
          player_name: pName,
          espn_player_id: pid,
          position: rep.position || rep.Position || '---',
          mlb_team: rep.team || rep.Team || '---',
          rank: rank,
          cost: finalCost,
          token_applied: isToken,
          prior_cost: priorCost,
          new_cost: newCost,
          token_savings: savings,
          isReplaced: true,
          isEmpty: false
        };
      }

      return {
        keeper_slot: slotNum,
        player_name: 'Empty Slot - Click Replace to Pick',
        espn_player_id: null,
        position: '---',
        mlb_team: '---',
        rank: null,
        cost: 0,
        token_applied: false,
        prior_cost: null,
        new_cost: null,
        token_savings: 0,
        isReplaced: false,
        isEmpty: true
      };
    });

    const totalCost = currentKeepers.reduce((sum, k) => sum + (k.cost || 0), 0);
    const totalTokenSavings = currentKeepers.reduce((sum, k) => sum + (k.token_savings || 0), 0);
    const validRanks = currentKeepers.map(k => k.rank).filter(Boolean);
    const avgRank = validRanks.length ? (validRanks.reduce((s, r) => s + r, 0) / validRanks.length).toFixed(1) : 0;
    const baseBudget = (teamBudgets.find(b => normalizeManager(b.owner) === normalizeManager(plannerOwner))?.base_budget) || 100;
    const remainingBudget = baseBudget - totalCost;

    return {
      keepers: currentKeepers,
      totalCost,
      totalTokenSavings,
      tokenSlot,
      avgRank,
      baseBudget,
      remainingBudget
    };
  }, [keepersByOwner, plannerOwner, replacedKeepers, teamBudgets, tokenSlot, getPriorCost, keeperSlots, players]);

  const plannerOwnerRosterCount = useMemo(() => {
    return (players || []).filter(p => normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner)).length;
  }, [players, plannerOwner]);

  const plannerCandidates = useMemo(() => {
    if (!players || players.length === 0) return [];
    const q = plannerSearch.trim().toLowerCase();

    let list = [];
    if (plannerScope === 'roster') {
      list = players.filter(p => normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner));
    } else if (plannerScope === 'roster_fa') {
      list = players.filter(p => {
        const isMyRoster = normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner);
        const isFA = !p.rosterOwner || p.rosterOwner === 'Available' || p.rosterOwner === 'Free Agent' || p.isFreeAgent;
        return isMyRoster || isFA;
      });
    } else {
      list = players;
    }

    if (q) {
      list = list.filter(p => {
        const name = (p.name || p.Player || p.full_name || '').toLowerCase();
        const pos = (p.position || p.Position || '').toLowerCase();
        const team = (p.team || p.Team || '').toLowerCase();
        return name.includes(q) || pos.includes(q) || team.includes(q);
      });
    }

    return [...list].sort((a, b) => {
      const aIsMyRoster = normalizeManager(a.rosterOwner) === normalizeManager(plannerOwner) ? 1 : 0;
      const bIsMyRoster = normalizeManager(b.rosterOwner) === normalizeManager(plannerOwner) ? 1 : 0;
      if (aIsMyRoster !== bIsMyRoster) return bIsMyRoster - aIsMyRoster;

      const rankA = (a.rank && a.rank > 0) ? a.rank : 999;
      const rankB = (b.rank && b.rank > 0) ? b.rank : 999;
      if (rankA !== rankB) return rankA - rankB;

      const nameA = a.name || a.Player || '';
      const nameB = b.name || b.Player || '';
      return nameA.localeCompare(nameB);
    });
  }, [players, plannerScope, plannerSearch, plannerOwner]);

  // Handle Save Keepers to Supabase
  const handleSaveKeepers = async () => {
    if (!user) {
      alert('Please log in with Discord in the top navigation bar to save official keepers.');
      return;
    }

    const isTargetMe = isSameOwner(profile?.owner_name, plannerOwner);

    if (isDeadlinePassed && !isCommissioner) {
      alert('The keeper selection deadline has passed. Only Commissioner (Adrian) or Admin (Dan) can submit keeper changes now.');
      return;
    }

    if (!isCommissioner && !isTargetMe) {
      alert(`You are logged in as ${profile?.owner_name}. You can only set official keepers for your own team.`);
      return;
    }

    if (plannerData.remainingBudget < 0) {
      if (!window.confirm(`⚠️ Warning: Total keeper spend ($${plannerData.totalCost}) exceeds the base budget ($${plannerData.baseBudget}). Do you still want to proceed?`)) {
        return;
      }
    }

    setSavingKeepers(true);
    try {
      const targetTeamId = (teamBudgets.find(b => b.owner === plannerOwner)?.team_id) || 0;
      const validKeepers = plannerData.keepers.filter(k => k.espn_player_id && !k.isEmpty);
      if (validKeepers.length === 0) {
        alert('Please select at least one keeper before saving.');
        setSavingKeepers(false);
        return;
      }

      const keeperRows = validKeepers.map(k => ({
        season_year: seasonYear,
        owner: plannerOwner,
        team_id: targetTeamId,
        keeper_slot: k.keeper_slot,
        player_name: k.player_name,
        espn_player_id: k.espn_player_id ? String(k.espn_player_id) : null,
        position: k.position,
        mlb_team: k.mlb_team,
        rank: k.rank,
        cost: k.cost,
        token_applied: Boolean(k.token_applied),
        prior_cost: k.prior_cost !== undefined ? k.prior_cost : null,
        new_cost: k.new_cost !== undefined ? k.new_cost : null,
        updated_at: new Date().toISOString()
      }));

      await supabase
        .from('draft_keepers')
        .delete()
        .eq('season_year', seasonYear)
        .eq('owner', plannerOwner);

      const { error: keepersErr } = await supabase
        .from('draft_keepers')
        .insert(keeperRows);

      if (keepersErr) throw keepersErr;

      const existingBudget = teamBudgets.find(b => b.owner === plannerOwner);
      const base = existingBudget?.base_budget || 100;
      const compSpend = existingBudget?.comp_pick_spend || 0;
      const compIncome = existingBudget?.comp_pick_income || 0;
      const manualAdj = existingBudget?.manual_adjustment || 0;
      const newKeeperSpend = plannerData.totalCost;
      const newFinalBudget = base - newKeeperSpend - compSpend + compIncome + manualAdj;

      await supabase
        .from('draft_team_budgets')
        .upsert({
          season_year: seasonYear,
          owner: plannerOwner,
          team_id: targetTeamId,
          base_budget: base,
          keeper_spend: newKeeperSpend,
          comp_pick_spend: compSpend,
          comp_pick_income: compIncome,
          final_budget: newFinalBudget,
          manual_adjustment: manualAdj,
          adjustment_notes: existingBudget?.adjustment_notes || null,
          net_picks: existingBudget?.net_picks || 0,
          updated_at: new Date().toISOString()
        }, { onConflict: 'season_year,owner' });

      alert(`Official ${seasonYear} keepers for ${plannerOwner} successfully saved to the league database! 🎉`);
      setReplacedKeepers({});
      if (onRefresh) onRefresh();
    } catch (err) {
      console.error('Error saving keepers:', err);
      alert('Failed to save keepers: ' + err.message);
    } finally {
      setSavingKeepers(false);
    }
  };

  // Handle Save Comp Picks to Supabase
  const handleSaveCompPicks = async () => {
    if (!user) {
      alert('Please log in with Discord in the top navigation bar to save consolation pick purchases.');
      return;
    }

    const isTargetMe = isSameOwner(profile?.owner_name, simulatedOwner);

    if (!isCommissioner && !isTargetMe) {
      alert(`You are logged in as ${profile?.owner_name}. You can only submit consolation picks for your own team.`);
      return;
    }

    if (simCalculations.simulatedRemaining < 0) {
      alert('Error: Purchases exceed remaining budget. You cannot submit an over-budget draft plan.');
      return;
    }

    setSavingCompPicks(true);
    try {
      const targetTeamId = (teamBudgets.find(b => b.owner === simulatedOwner)?.team_id) || 0;

      await supabase
        .from('draft_compensation_picks')
        .delete()
        .eq('season_year', seasonYear)
        .eq('owner', simulatedOwner)
        .in('action_type', ['BOUGHT', 'OFFSET_LOST']);

      const newRows = [];
      simCalculations.boughtList.forEach(b => {
        newRows.push({
          season_year: seasonYear,
          owner: simulatedOwner,
          team_id: targetTeamId,
          action_type: 'BOUGHT',
          round_num: b.round,
          cost_or_income: -b.cost,
          notes: `Purchased via Consolation Portal (-$${b.cost})`,
          updated_at: new Date().toISOString()
        });
      });

      simCalculations.offsetRounds.forEach(r => {
        newRows.push({
          season_year: seasonYear,
          owner: simulatedOwner,
          team_id: targetTeamId,
          action_type: 'OFFSET_LOST',
          round_num: r,
          cost_or_income: 0,
          notes: `Roster size offset: Round ${r} forfeited`,
          updated_at: new Date().toISOString()
        });
      });

      if (newRows.length > 0) {
        const { error: insErr } = await supabase
          .from('draft_compensation_picks')
          .insert(newRows);
        if (insErr) throw insErr;
      }

      const existingBudget = teamBudgets.find(b => b.owner === simulatedOwner);
      const base = existingBudget?.base_budget || 100;
      const kSpend = existingBudget?.keeper_spend || 0;
      const manualAdj = existingBudget?.manual_adjustment || 0;
      const newCompSpend = simCalculations.spend;
      const newCompIncome = simCalculations.income;
      const newFinalBudget = base - kSpend - newCompSpend + newCompIncome + manualAdj;
      const newNetPicks = simCalculations.boughtList.length - simCalculations.offsetRounds.length;

      await supabase
        .from('draft_team_budgets')
        .upsert({
          season_year: seasonYear,
          owner: simulatedOwner,
          team_id: targetTeamId,
          base_budget: base,
          keeper_spend: kSpend,
          comp_pick_spend: newCompSpend,
          comp_pick_income: newCompIncome,
          final_budget: newFinalBudget,
          manual_adjustment: manualAdj,
          adjustment_notes: existingBudget?.adjustment_notes || null,
          net_picks: newNetPicks,
          updated_at: new Date().toISOString()
        }, { onConflict: 'season_year,owner' });

      alert(`Consolation pick purchases for ${simulatedOwner} (${seasonYear}) successfully saved to league records! 🎟️`);
      if (onRefresh) onRefresh();
    } catch (err) {
      console.error('Error saving comp picks:', err);
      alert('Failed to save compensation picks: ' + err.message);
    } finally {
      setSavingCompPicks(false);
    }
  };

  // Commissioner: Handle Save League Settings
  const handleSaveLeagueSettings = async () => {
    if (!isCommissioner) {
      alert('Only Commissioner (Adrian) or Admin (Dan) can modify league settings.');
      return;
    }

    setSavingSettings(true);
    try {
      const deadlineIso = deadlineInput ? new Date(deadlineInput).toISOString() : null;
      const baseVal = parseFloat(baselineBudgetInput) || 100;

      const { error } = await supabase
        .from('league_settings')
        .update({
          keeper_lock_deadline: deadlineIso,
          base_budget: baseVal,
          updated_by: profile?.owner_name || (governanceTitle === 'Admin' ? 'Admin' : 'Commissioner'),
          updated_at: new Date().toISOString()
        })
        .eq('league_id', 130215);

      if (error) throw error;

      setLeagueSettings(prev => ({
        ...prev,
        keeper_lock_deadline: deadlineIso,
        base_budget: baseVal
      }));

      alert('League settings (Keeper Deadline & Base Budget) successfully updated! 👑');
    } catch (err) {
      console.error('Error updating league settings:', err);
      alert('Failed to update league settings: ' + err.message);
    } finally {
      setSavingSettings(false);
    }
  };

  // Commissioner / Admin: Handle Save Manual Adjustments
  const handleSaveManualAdjustments = async () => {
    if (!isCommissioner) {
      alert('Only Commissioner (Adrian) or Admin (Dan) can modify owner budget adjustments.');
      return;
    }

    setSavingAdjustments(true);
    try {
      const updates = [];
      for (const b of teamBudgets) {
        const formVal = adjustmentsForm[b.owner];
        const manualAdj = formVal !== undefined && formVal.adjustment !== '' ? parseFloat(formVal.adjustment) || 0 : (b.manual_adjustment || 0);
        const notes = formVal ? formVal.notes : (b.adjustment_notes || '');

        const newFinal = b.base_budget - (b.keeper_spend || 0) - (b.comp_pick_spend || 0) + (b.comp_pick_income || 0) + manualAdj;

        updates.push({
          season_year: seasonYear,
          owner: b.owner,
          team_id: b.team_id,
          finish_rank: b.finish_rank,
          base_budget: b.base_budget,
          keeper_spend: b.keeper_spend,
          comp_pick_spend: b.comp_pick_spend,
          comp_pick_income: b.comp_pick_income,
          final_budget: newFinal,
          manual_adjustment: manualAdj,
          adjustment_notes: notes,
          net_picks: b.net_picks,
          updated_at: new Date().toISOString()
        });
      }

      const { error } = await supabase
        .from('draft_team_budgets')
        .upsert(updates, { onConflict: 'season_year,owner' });

      if (error) throw error;

      alert(`All owner manual budget adjustments for ${seasonYear} successfully saved! ⚖️`);
      if (onRefresh) onRefresh();
    } catch (err) {
      console.error('Error saving manual adjustments:', err);
      alert('Failed to save manual adjustments: ' + err.message);
    } finally {
      setSavingAdjustments(false);
    }
  };

  return (
    <div style={{ width: '100%', gridColumn: '1 / -1', display: 'flex', flexDirection: 'column', gap: '15px' }}>
      {/* Top Header & Sub-Tab Bar */}
      <KeepersHeaderNav
        seasonYear={seasonYear}
        onSeasonYearChange={onSeasonYearChange}
        activeTab={activeTab}
        onTabChange={handleTabChange}
        keepersCount={keepers.length}
        leagueSettings={leagueSettings}
        isDeadlinePassed={isDeadlinePassed}
        isCommissioner={isCommissioner}
        governanceTitle={governanceTitle}
      />

      {/* SUB-TAB 1: KEEPER SUMMARY & ROTO STANDINGS */}
      {activeTab === 'matrix' && (
        <KeeperMatrixTab
          seasonYear={seasonYear}
          sortedTeamBudgets={sortedTeamBudgets}
          keepersByOwner={keepersByOwner}
          compPicksByOwner={compPicksByOwner}
          keeperRotoData={keeperRotoData}
          selectedOwner={selectedOwner}
          setSelectedOwner={setSelectedOwner}
        />
      )}

      {/* SUB-TAB 2: KEEPER ROSTERS */}
      {activeTab === 'rosters' && (
        <RosterBreakdownTab
          keeperOwnerFilter={keeperOwnerFilter}
          setKeeperOwnerFilter={setKeeperOwnerFilter}
          seasonYear={seasonYear}
          DRAFT_MANAGERS={DRAFT_MANAGERS}
          keepersByOwner={keepersByOwner}
          onPlayerClick={onPlayerClick}
        />
      )}

      {/* SUB-TAB 3: INTERACTIVE COMPENSATION PICK SIMULATOR */}
      {activeTab === 'simulator' && (
        <CompSimulatorTab
          simulatedOwner={simulatedOwner}
          setSimulatedOwner={setSimulatedOwner}
          compPicks={compPicks}
          setSimBoughtRounds={setSimBoughtRounds}
          setSimSoldRounds={setSimSoldRounds}
          DRAFT_MANAGERS={DRAFT_MANAGERS}
          resetSimulatorToReal={resetSimulatorToReal}
          handleSaveCompPicks={handleSaveCompPicks}
          savingCompPicks={savingCompPicks}
          simCalculations={simCalculations}
          isCommissioner={isCommissioner}
          profile={profile}
          seasonYear={seasonYear}
          compBuyPrices={compBuyPrices}
          simBoughtRounds={simBoughtRounds}
          compSellPrices={compSellPrices}
          simSoldRounds={simSoldRounds}
        />
      )}

      {/* SUB-TAB 4: KEEPER SUBMISSION & WHAT-IF PLANNER */}
      {activeTab === 'planner' && (
        <KeeperPlannerTab
          plannerOwner={plannerOwner}
          setPlannerOwner={setPlannerOwner}
          setReplacedKeepers={setReplacedKeepers}
          DRAFT_MANAGERS={DRAFT_MANAGERS}
          handleSaveKeepers={handleSaveKeepers}
          savingKeepers={savingKeepers}
          isDeadlinePassed={isDeadlinePassed}
          isCommissioner={isCommissioner}
          profile={profile}
          plannerData={plannerData}
          seasonYear={seasonYear}
          keeperSlots={keeperSlots}
          setTokenSlot={setTokenSlot}
          plannerScope={plannerScope}
          setPlannerScope={setPlannerScope}
          plannerOwnerRosterCount={plannerOwnerRosterCount}
          players={players}
          plannerSearch={plannerSearch}
          setPlannerSearch={setPlannerSearch}
          plannerCandidates={plannerCandidates}
          calculateKeeperCostFromRank={calculateKeeperCostFromRank}
          normalizeManager={normalizeManager}
          getPriorCost={getPriorCost}
        />
      )}

      {/* SUB-TAB 5: COMMISSIONER SETTINGS & MANUAL ADJUSTMENTS */}
      {activeTab === 'settings' && (
        <LeagueSettingsTab
          isCommissioner={isCommissioner}
          user={user}
          profile={profile}
          deadlineInput={deadlineInput}
          setDeadlineInput={setDeadlineInput}
          baselineBudgetInput={baselineBudgetInput}
          setBaselineBudgetInput={setBaselineBudgetInput}
          handleSaveLeagueSettings={handleSaveLeagueSettings}
          savingSettings={savingSettings}
          handleSaveManualAdjustments={handleSaveManualAdjustments}
          savingAdjustments={savingAdjustments}
          teamBudgets={teamBudgets}
          adjustmentsForm={adjustmentsForm}
          setAdjustmentsForm={setAdjustmentsForm}
        />
      )}

      {/* SUB-TAB 6: KEEPER VALUATION MATH & DETAILED PR CALCULATIONS */}
      {activeTab === 'calculations' && (
        <KeeperCalculationsView
          onPlayerClick={onPlayerClick}
          seasonYear={seasonYear}
          onSeasonYearChange={onSeasonYearChange}
          keepers={keepers}
          players={players}
        />
      )}
    </div>
  );
}
