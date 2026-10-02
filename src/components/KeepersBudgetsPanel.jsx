import React, { useState, useMemo, useEffect } from 'react';
import { useAuth } from '../context/useAuth';
import { supabase } from '../supabaseClient';
import KeeperCalculationsView from '../views/KeeperCalculationsView';
import defaultCalculations from '../data/keeperCalculations.json';

// Price schedule constants matching Google Sheet 'Compensation Picks'
// For 2026: 5 keepers, comp picks start at Round 6 (buy $15, sell $8)
const COMP_BUY_PRICES_2026 = {
  6: 15, 7: 14, 8: 13, 9: 12, 10: 11,
  11: 10, 12: 9, 13: 8, 14: 7, 15: 6,
  16: 5, 17: 4, 18: 3, 19: 2, 20: 1
};
const COMP_SELL_PRICES_2026 = {
  6: 8, 7: 4, 8: 2, 9: 1
};

// Starting in 2027: 6 keepers, comp picks start at Round 7 (buy $15, sell $8)
const COMP_BUY_PRICES_2027 = {
  7: 15, 8: 14, 9: 13, 10: 12, 11: 11,
  12: 10, 13: 9, 14: 8, 15: 7, 16: 6,
  17: 5, 18: 4, 19: 3, 20: 2, 21: 1
};
const COMP_SELL_PRICES_2027 = {
  7: 8, 8: 4, 9: 2, 10: 1
};

function getCompBuyPrices(year) {
  return year >= 2027 ? COMP_BUY_PRICES_2027 : COMP_BUY_PRICES_2026;
}

function getCompSellPrices(year) {
  return year >= 2027 ? COMP_SELL_PRICES_2027 : COMP_SELL_PRICES_2026;
}

function getKeeperSlots(year) {
  return year >= 2027 ? [1, 2, 3, 4, 5, 6] : [1, 2, 3, 4, 5];
}

const DRAFT_MANAGERS = [
  'Tim', 'Daniel', 'Will', 'Adrian', 'Garrett', 'Alex', 'Mark', 'Preston', 'Anil'
];

function normalizeManager(mgr) {
  if (!mgr) return '';
  const s = String(mgr).trim();
  if (s.toLowerCase() === 'dan') return 'Daniel';
  return s;
}

// Canonical league finishes to guarantee correct #1-#9 display regardless of DB schema team IDs
const CANONICAL_FINISHES = {
  2026: {
    'Tim': 1,
    'Daniel': 2,
    'Dan': 2,
    'Will': 3,
    'Adrian': 4,
    'Garrett': 5,
    'Alex': 6,
    'Mark': 7,
    'Preston': 8,
    'Anil': 9
  },
  2027: {
    'Daniel': 1,
    'Dan': 1,
    'Tim': 2,
    'Will': 3,
    'Adrian': 4,
    'Garrett': 5,
    'Anil': 6,
    'Alex': 7,
    'Preston': 8,
    'Mark': 9
  }
};

function getActualFinish(season, owner, dbRank) {
  const norm = normalizeManager(owner);
  if (CANONICAL_FINISHES[season]?.[norm] !== undefined) {
    return CANONICAL_FINISHES[season][norm];
  }
  if (dbRank && dbRank >= 1 && dbRank <= 9) return dbRank;
  return CANONICAL_FINISHES[2027]?.[norm] || dbRank || '-';
}

function getRotoBadgeStyle(pts) {
  if (pts >= 8.0) return { bg: 'rgba(16, 185, 129, 0.2)', text: '#10b981', border: 'rgba(16, 185, 129, 0.45)' };
  if (pts >= 6.0) return { bg: 'rgba(3, 218, 198, 0.18)', text: '#03dac6', border: 'rgba(3, 218, 198, 0.4)' };
  if (pts >= 4.0) return { bg: 'rgba(251, 191, 36, 0.18)', text: '#fbbf24', border: 'rgba(251, 191, 36, 0.4)' };
  if (pts >= 2.5) return { bg: 'rgba(249, 115, 22, 0.18)', text: '#f97316', border: 'rgba(249, 115, 22, 0.4)' };
  return { bg: 'rgba(244, 63, 94, 0.18)', text: '#f43f5e', border: 'rgba(244, 63, 94, 0.4)' };
}

function formatRotoStat(key, val) {
  if (val === undefined || val === null || isNaN(val)) return '-';
  if (key === 'OBP') return (val < 1 ? val.toFixed(3).replace(/^0/, '') : val.toFixed(3));
  if (key === 'ERA' || key === 'WHIP') return val >= 90 ? 'N/A' : val.toFixed(2);
  if (key === 'QS') return val.toFixed(1);
  return String(Math.round(val));
}

// Helper to compute keeper cost from rank based on Keeper Costs tab
function calculateKeeperCostFromRank(rank) {
  if (!rank || rank > 150) return 0;
  if (rank === 1) return 40;
  if (rank === 2) return 36;
  if (rank === 3) return 33;
  if (rank === 4) return 31;
  if (rank === 5) return 30;
  if (rank === 6) return 29;
  if (rank === 7) return 28;
  if (rank === 8) return 27;
  if (rank === 9) return 26;
  if (rank === 10) return 25;
  if (rank <= 14) return 23;
  if (rank <= 16) return 22;
  if (rank <= 19) return 21;
  if (rank <= 22) return 20;
  if (rank <= 25) return 19;
  if (rank <= 28) return 18;
  if (rank <= 31) return 17;
  if (rank <= 35) return 16;
  if (rank <= 40) return 15;
  if (rank <= 45) return 14;
  if (rank <= 50) return 13;
  if (rank <= 55) return 12;
  if (rank <= 65) return 11;
  if (rank <= 75) return 9;
  if (rank <= 85) return 8;
  if (rank <= 95) return 7;
  if (rank <= 110) return 5;
  if (rank <= 125) return 4;
  if (rank <= 140) return 3;
  if (rank <= 150) return 2;
  return 0;
}

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

  const [activeTab, setActiveTab] = useState(initialTab || 'matrix'); // 'matrix' | 'rosters' | 'simulator' | 'planner' | 'settings' | 'calculations'

  const compBuyPrices = useMemo(() => getCompBuyPrices(seasonYear), [seasonYear]);
  const compSellPrices = useMemo(() => getCompSellPrices(seasonYear), [seasonYear]);
  const keeperSlots = useMemo(() => getKeeperSlots(seasonYear), [seasonYear]);

  useEffect(() => {
    if (initialTab) {
      setActiveTab(initialTab);
    }
  }, [initialTab]);

  const [selectedOwner, setSelectedOwner] = useState(currentUser || 'Daniel');
  const [keeperOwnerFilter, setKeeperOwnerFilter] = useState('ALL');

  // Keeper Summary & Roto sub-view states
  const [summaryViewMode, setSummaryViewMode] = useState('both'); // 'roto' | 'ledger' | 'both'
  const [rotoDisplayMode, setRotoDisplayMode] = useState('both'); // 'both' | 'points' | 'stats'
  const [rotoSortCol, setRotoSortCol] = useState('total');
  const [rotoSortDir, setRotoSortDir] = useState('desc');
  const [expandedRotoOwner, setExpandedRotoOwner] = useState(null);

  // Simulation state for the interactive comp pick simulator
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
    // 1. Load from default / prior keepers (2026)
    const pKeepers = Array.isArray(priorKeepers) ? priorKeepers : (priorKeepers?.keepers || []);
    pKeepers.forEach(k => {
      if (k.espn_player_id) map.set(String(k.espn_player_id), parseFloat(k.cost) || 0);
      if (k.player_name) map.set(k.player_name.toLowerCase().trim(), parseFloat(k.cost) || 0);
    });
    // Pre-count names to detect duplicates in pool
    const nameCounts = new Map();
    (players || []).forEach(p => {
      const pName = (p.Player || p.name || p.full_name || '').toLowerCase().trim();
      if (pName) nameCounts.set(pName, (nameCounts.get(pName) || 0) + 1);
    });

    // 2. Load from players array (Hefty Keeper Price)
    (players || []).forEach(p => {
      const pid = String(p['ESPN PlayerID'] || p.id || '');
      const pName = (p.Player || p.name || p.full_name || '').toLowerCase().trim();
      const rawPrice = p['Hefty Keeper Price'];
      if (rawPrice !== undefined && rawPrice !== null && rawPrice !== '') {
        const val = parseFloat(rawPrice);
        if (!isNaN(val)) {
          if (pid && !map.has(pid)) map.set(pid, val);
          // Only map by name if name is unique in pool to avoid duplicate cross-contamination
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

  // Real-world picks for the simulated owner
  const realOwnerCompPicks = useMemo(() => {
    const list = Array.isArray(compPicks) ? compPicks : (compPicks?.comp_picks || []);
    return list.filter(p => normalizeManager(p.owner) === normalizeManager(simulatedOwner));
  }, [compPicks, simulatedOwner]);

  // Sync simulator defaults from real world when owner changes
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
    // Sort each owner's keepers by slot
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

  // Sort team budgets sequentially by canonical finish rank (1 through 9)
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
        R: 0,
        HR: 0,
        RBI: 0,
        SB: 0,
        AB: 0,
        totOBPNum: 0,
        K: 0,
        QS: 0,
        SVHD: 0,
        IP: 0,
        totERANum: 0,
        totWHIPNum: 0
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
            R: r,
            HR: hr,
            RBI: rbi,
            SB: sb,
            OBP: obp,
            K: kCount,
            QS: qs,
            SVHD: svhd,
            ERA: era,
            WHIP: whip,
            IP: ip,
            AB: ab
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

    // Default sort by total points
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

  const sortedRotoTeams = useMemo(() => {
    if (!keeperRotoData?.teams) return [];
    const list = [...keeperRotoData.teams];
    const col = rotoSortCol;
    const isAsc = rotoSortDir === 'asc';

    return list.sort((a, b) => {
      let valA, valB;
      if (col === 'rank') {
        valA = a.rank; valB = b.rank;
      } else if (col === 'owner') {
        return isAsc ? a.owner.localeCompare(b.owner) : b.owner.localeCompare(a.owner);
      } else if (col === 'total') {
        valA = a.totalPoints; valB = b.totalPoints;
      } else if (col === 'batting') {
        valA = a.battingPoints; valB = b.battingPoints;
      } else if (col === 'pitching') {
        valA = a.pitchingPoints; valB = b.pitchingPoints;
      } else if (a.points[col] !== undefined) {
        valA = a.points[col]; valB = b.points[col];
      } else {
        valA = a.totalPoints; valB = b.totalPoints;
      }
      return isAsc ? valA - valB : valB - valA;
    });
  }, [keeperRotoData, rotoSortCol, rotoSortDir]);

  const handleRotoSort = (col) => {
    if (rotoSortCol === col) {
      setRotoSortDir(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setRotoSortCol(col);
      if (col === 'rank' || col === 'owner' || col === 'ERA' || col === 'WHIP') {
        setRotoSortDir('asc');
      } else {
        setRotoSortDir('desc');
      }
    }
  };

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

    // Compute forfeited latest rounds (starting from 32 down)
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
    // Ensure all keeper slots (1 through 6 in 2027, 1 through 5 in 2026) are present for planning
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

      // If slot is empty in originalKeepers and not replaced, find the next highest-cost rostered player
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

  // Selected owner's total roster count
  const plannerOwnerRosterCount = useMemo(() => {
    return (players || []).filter(p => normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner)).length;
  }, [players, plannerOwner]);

  // Candidates for What-If planner based on scope and search
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

    const isTargetMe = profile?.owner_name?.toLowerCase() === plannerOwner?.toLowerCase() ||
      (profile?.owner_name === 'Dan' && plannerOwner === 'Daniel') ||
      (profile?.owner_name === 'Daniel' && plannerOwner === 'Dan');

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

      // Clean existing keepers for this owner in this season to avoid orphaned slots
      await supabase
        .from('draft_keepers')
        .delete()
        .eq('season_year', seasonYear)
        .eq('owner', plannerOwner);

      const { error: keepersErr } = await supabase
        .from('draft_keepers')
        .insert(keeperRows);

      if (keepersErr) throw keepersErr;

      // Update draft_team_budgets
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

    const isTargetMe = profile?.owner_name?.toLowerCase() === simulatedOwner?.toLowerCase() ||
      (profile?.owner_name === 'Dan' && simulatedOwner === 'Daniel') ||
      (profile?.owner_name === 'Daniel' && simulatedOwner === 'Dan');

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

      // 1. Delete existing BOUGHT and OFFSET_LOST picks for this owner in this season
      await supabase
        .from('draft_compensation_picks')
        .delete()
        .eq('season_year', seasonYear)
        .eq('owner', simulatedOwner)
        .in('action_type', ['BOUGHT', 'OFFSET_LOST']);

      // 2. Insert new bought & offset picks
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

      // 3. Update draft_team_budgets
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

  // Commissioner: Handle Save League Settings (Deadline & Base Budget)
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

  // Commissioner / Admin: Handle Save Manual Punitive / Award Adjustments
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
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '12px 16px',
        background: '#1a1a1a',
        borderRadius: '8px',
        border: '1px solid #2e2e2e',
        gap: '12px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <span style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span>💎</span> Keepers, Compensation Picks & Budgets
          </span>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <button
              onClick={() => onSeasonYearChange && onSeasonYearChange(2027)}
              style={{
                background: seasonYear === 2027 ? '#03dac6' : 'rgba(255, 255, 255, 0.05)',
                color: seasonYear === 2027 ? '#000' : '#888',
                border: `1px solid ${seasonYear === 2027 ? '#03dac6' : '#333'}`,
                borderRadius: '6px',
                padding: '3px 8px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              🚀 2027 Prep
            </button>
            <button
              onClick={() => onSeasonYearChange && onSeasonYearChange(2026)}
              style={{
                background: seasonYear === 2026 ? '#bb86fc' : 'rgba(255, 255, 255, 0.05)',
                color: seasonYear === 2026 ? '#000' : '#888',
                border: `1px solid ${seasonYear === 2026 ? '#bb86fc' : '#333'}`,
                borderRadius: '6px',
                padding: '3px 8px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              🏛️ 2026 Archive
            </button>
          </div>
        </div>

        {/* Sub-Tab Buttons */}
        <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '3px', border: '1px solid #333', gap: '4px' }}>
          <button
            onClick={() => setActiveTab('matrix')}
            style={{
              background: activeTab === 'matrix' ? '#03dac6' : 'transparent',
              color: activeTab === 'matrix' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            📋 Keeper Summary
          </button>
          <button
            onClick={() => setActiveTab('rosters')}
            style={{
              background: activeTab === 'rosters' ? '#ffb74d' : 'transparent',
              color: activeTab === 'rosters' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            💎 Keeper Rosters ({keepers.length})
          </button>
          <button
            onClick={() => { setActiveTab('simulator'); resetSimulatorToReal(); }}
            style={{
              background: activeTab === 'simulator' ? '#bb86fc' : 'transparent',
              color: activeTab === 'simulator' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            🎲 Comp Pick Submission
          </button>
          <button
            onClick={() => setActiveTab('planner')}
            style={{
              background: activeTab === 'planner' ? '#4caf50' : 'transparent',
              color: activeTab === 'planner' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            📋 Keeper Submission
          </button>
          <button
            onClick={() => setActiveTab('calculations')}
            style={{
              background: activeTab === 'calculations' ? '#00e676' : 'transparent',
              color: activeTab === 'calculations' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            🏷️ Keeper Prices
          </button>
          <button
            onClick={() => setActiveTab('settings')}
            style={{
              background: activeTab === 'settings' ? '#f59e0b' : 'transparent',
              color: activeTab === 'settings' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            👑 Commish Settings
          </button>
        </div>
      </div>

      {/* Official Keeper Deadline Notification Banner */}
      {leagueSettings?.keeper_lock_deadline && (
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '10px 16px',
          borderRadius: '8px',
          fontSize: '12px',
          background: isDeadlinePassed ? 'rgba(239, 68, 68, 0.12)' : 'rgba(59, 130, 246, 0.12)',
          border: isDeadlinePassed ? '1px solid rgba(239, 68, 68, 0.35)' : '1px solid rgba(59, 130, 246, 0.35)',
          color: isDeadlinePassed ? '#fca5a5' : '#93c5fd',
          flexWrap: 'wrap',
          gap: '8px'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '16px' }}>{isDeadlinePassed ? '🔒' : '⏰'}</span>
            <span>
              <strong>Official Keeper Deadline:</strong> {new Date(leagueSettings.keeper_lock_deadline).toLocaleString()}
              {isDeadlinePassed ? ' (Deadline has passed — keeper rosters are locked)' : ' (All keeper selections lock at this time)'}
            </span>
          </div>
          {isCommissioner && (
            <span style={{
              background: 'rgba(245, 158, 11, 0.2)',
              color: '#fbbf24',
              border: '1px solid rgba(245, 158, 11, 0.4)',
              padding: '2px 8px',
              borderRadius: '4px',
              fontSize: '10px',
              fontWeight: 'bold'
            }}>
              {governanceTitle === 'Commissioner' ? '👑' : '⚡'} {governanceTitle} Override Rights Active
            </span>
          )}
        </div>
      )}

      {/* SUB-TAB 1: KEEPER SUMMARY & ROTO MATRIX */}
      {activeTab === 'matrix' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Sub-view switcher bar within Keeper Summary */}
          <div style={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: '12px',
            background: '#161616',
            padding: '12px 16px',
            borderRadius: '8px',
            border: '1px solid #2a2a2a'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
              <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Summary View:
              </span>
              <div style={{ display: 'flex', background: '#202020', borderRadius: '6px', padding: '3px', border: '1px solid #333', gap: '4px' }}>
                <button
                  onClick={() => setSummaryViewMode('roto')}
                  style={{
                    background: summaryViewMode === 'roto' ? '#10b981' : 'transparent',
                    color: summaryViewMode === 'roto' ? '#000' : '#aaa',
                    border: 'none',
                    borderRadius: '4px',
                    padding: '6px 14px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease'
                  }}
                >
                  🏆 Keeper Roto Standings
                </button>
                <button
                  onClick={() => setSummaryViewMode('ledger')}
                  style={{
                    background: summaryViewMode === 'ledger' ? '#03dac6' : 'transparent',
                    color: summaryViewMode === 'ledger' ? '#000' : '#aaa',
                    border: 'none',
                    borderRadius: '4px',
                    padding: '6px 14px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease'
                  }}
                >
                  💼 Budget & Pick Ledger
                </button>
                <button
                  onClick={() => setSummaryViewMode('both')}
                  style={{
                    background: summaryViewMode === 'both' ? '#bb86fc' : 'transparent',
                    color: summaryViewMode === 'both' ? '#000' : '#aaa',
                    border: 'none',
                    borderRadius: '4px',
                    padding: '6px 14px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease'
                  }}
                >
                  📊 All-in-One Overview
                </button>
              </div>
            </div>

            {/* Display Mode Toggle for Roto */}
            {(summaryViewMode === 'roto' || summaryViewMode === 'both') && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ fontSize: '11px', color: '#777' }}>Detail:</span>
                <div style={{ display: 'flex', background: '#202020', borderRadius: '4px', padding: '2px', border: '1px solid #333' }}>
                  <button
                    onClick={() => setRotoDisplayMode('both')}
                    style={{
                      background: rotoDisplayMode === 'both' ? '#333' : 'transparent',
                      color: rotoDisplayMode === 'both' ? '#fff' : '#888',
                      border: 'none',
                      borderRadius: '3px',
                      padding: '4px 10px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    Pts + Stats
                  </button>
                  <button
                    onClick={() => setRotoDisplayMode('points')}
                    style={{
                      background: rotoDisplayMode === 'points' ? '#333' : 'transparent',
                      color: rotoDisplayMode === 'points' ? '#fff' : '#888',
                      border: 'none',
                      borderRadius: '3px',
                      padding: '4px 10px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    Points Only
                  </button>
                  <button
                    onClick={() => setRotoDisplayMode('stats')}
                    style={{
                      background: rotoDisplayMode === 'stats' ? '#333' : 'transparent',
                      color: rotoDisplayMode === 'stats' ? '#fff' : '#888',
                      border: 'none',
                      borderRadius: '3px',
                      padding: '4px 10px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    Raw Stats
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* SECTION A: KEEPER ROTO STANDINGS */}
          {(summaryViewMode === 'roto' || summaryViewMode === 'both') && (
            <div style={{
              background: '#181818',
              borderRadius: '8px',
              border: '1px solid #2a2a2a',
              overflow: 'hidden',
              display: 'flex',
              flexDirection: 'column',
              gap: '12px',
              padding: '16px'
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px' }}>
                <div>
                  <h3 style={{ margin: 0, fontSize: '16px', fontWeight: 'bold', color: '#fff', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🏆</span>
                    <span>{seasonYear} Keeper Projected Roto Standings</span>
                  </h3>
                  <div style={{ fontSize: '12px', color: '#888', marginTop: '2px' }}>
                    10-category rotisserie standings computed from currently selected keepers ({seasonYear >= 2027 ? 6 : 5} per owner). Max 90.0 pts (45.0 Batting / 45.0 Pitching).
                  </div>
                </div>
                <div style={{ fontSize: '11px', color: '#aaa', background: 'rgba(255,255,255,0.04)', padding: '4px 10px', borderRadius: '6px', border: '1px solid #333' }}>
                  Click any column header to sort • Click row to expand roster
                </div>
              </div>

              {/* Category Leaders Pill Bar */}
              {keeperRotoData?.leaders && (
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  flexWrap: 'wrap',
                  background: '#121212',
                  padding: '8px 12px',
                  borderRadius: '6px',
                  border: '1px solid #242424'
                }}>
                  <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', marginRight: '4px' }}>Leaders:</span>
                  <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                    🏃 R: <strong>{keeperRotoData.leaders.R?.owner}</strong> ({formatRotoStat('R', keeperRotoData.leaders.R?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                    💥 HR: <strong>{keeperRotoData.leaders.HR?.owner}</strong> ({formatRotoStat('HR', keeperRotoData.leaders.HR?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                    🎯 RBI: <strong>{keeperRotoData.leaders.RBI?.owner}</strong> ({formatRotoStat('RBI', keeperRotoData.leaders.RBI?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                    ⚡ SB: <strong>{keeperRotoData.leaders.SB?.owner}</strong> ({formatRotoStat('SB', keeperRotoData.leaders.SB?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(16, 185, 129, 0.25)' }}>
                    👁️ OBP: <strong>{keeperRotoData.leaders.OBP?.owner}</strong> ({formatRotoStat('OBP', keeperRotoData.leaders.OBP?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                    🔥 K: <strong>{keeperRotoData.leaders.K?.owner}</strong> ({formatRotoStat('K', keeperRotoData.leaders.K?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                    ⭐ QS: <strong>{keeperRotoData.leaders.QS?.owner}</strong> ({formatRotoStat('QS', keeperRotoData.leaders.QS?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                    🛡️ SV+HD: <strong>{keeperRotoData.leaders.SVHD?.owner}</strong> ({formatRotoStat('SVHD', keeperRotoData.leaders.SVHD?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                    🎯 ERA: <strong>{keeperRotoData.leaders.ERA?.owner}</strong> ({formatRotoStat('ERA', keeperRotoData.leaders.ERA?.val)})
                  </span>
                  <span style={{ fontSize: '11px', background: 'rgba(187, 134, 252, 0.1)', color: '#bb86fc', padding: '2px 8px', borderRadius: '4px', border: '1px solid rgba(187, 134, 252, 0.25)' }}>
                    🔒 WHIP: <strong>{keeperRotoData.leaders.WHIP?.owner}</strong> ({formatRotoStat('WHIP', keeperRotoData.leaders.WHIP?.val)})
                  </span>
                </div>
              )}

              {/* Roto Table */}
              <div style={{ overflowX: 'auto', borderRadius: '6px', border: '1px solid #292929' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'center' }}>
                  <thead>
                    {/* Top Category Groupings */}
                    <tr style={{ background: '#121212', borderBottom: '1px solid #2d2d2d', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                      <th colSpan={5} style={{ padding: '6px 10px', textAlign: 'left', color: '#888' }}>
                        Team Standings
                      </th>
                      <th colSpan={5} style={{ padding: '6px 10px', color: '#10b981', borderLeft: '1px solid #333', borderRight: '1px solid #333', background: 'rgba(16, 185, 129, 0.05)' }}>
                        ⚾ Batting Categories
                      </th>
                      <th colSpan={5} style={{ padding: '6px 10px', color: '#bb86fc', background: 'rgba(187, 134, 252, 0.05)' }}>
                        🎯 Pitching Categories
                      </th>
                    </tr>

                    {/* Column Headers */}
                    <tr style={{ background: '#1c1c1c', color: '#aaa', borderBottom: '2px solid #333', userSelect: 'none' }}>
                      <th
                        onClick={() => handleRotoSort('rank')}
                        style={{ padding: '10px 8px', width: '50px', cursor: 'pointer', textAlign: 'center' }}
                      >
                        Rank {rotoSortCol === 'rank' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                      </th>
                      <th
                        onClick={() => handleRotoSort('owner')}
                        style={{ padding: '10px 14px', textAlign: 'left', minWidth: '120px', cursor: 'pointer' }}
                      >
                        Owner {rotoSortCol === 'owner' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                      </th>
                      <th
                        onClick={() => handleRotoSort('total')}
                        style={{ padding: '10px 12px', cursor: 'pointer', color: '#03dac6', minWidth: '85px' }}
                      >
                        Total Pts {rotoSortCol === 'total' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                      </th>
                      <th
                        onClick={() => handleRotoSort('batting')}
                        style={{ padding: '10px 10px', cursor: 'pointer', color: '#10b981', minWidth: '65px' }}
                      >
                        Bat Pts {rotoSortCol === 'batting' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                      </th>
                      <th
                        onClick={() => handleRotoSort('pitching')}
                        style={{ padding: '10px 10px', cursor: 'pointer', color: '#bb86fc', borderRight: '1px solid #333', minWidth: '65px' }}
                      >
                        Pit Pts {rotoSortCol === 'pitching' ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                      </th>

                      {/* Batting Columns */}
                      {['R', 'HR', 'RBI', 'SB', 'OBP'].map(cat => (
                        <th
                          key={cat}
                          onClick={() => handleRotoSort(cat)}
                          style={{
                            padding: '10px 8px',
                            cursor: 'pointer',
                            color: '#10b981',
                            background: rotoSortCol === cat ? 'rgba(16, 185, 129, 0.15)' : 'rgba(16, 185, 129, 0.03)',
                            minWidth: rotoDisplayMode === 'both' ? '68px' : '55px'
                          }}
                        >
                          {cat} {rotoSortCol === cat ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                        </th>
                      ))}

                      {/* Pitching Columns */}
                      {[
                        { key: 'K', label: 'K' },
                        { key: 'QS', label: 'QS' },
                        { key: 'SVHD', label: 'SV+HD' },
                        { key: 'ERA', label: 'ERA' },
                        { key: 'WHIP', label: 'WHIP' }
                      ].map(cat => (
                        <th
                          key={cat.key}
                          onClick={() => handleRotoSort(cat.key)}
                          style={{
                            padding: '10px 8px',
                            cursor: 'pointer',
                            color: '#bb86fc',
                            background: rotoSortCol === cat.key ? 'rgba(187, 134, 252, 0.15)' : 'rgba(187, 134, 252, 0.03)',
                            minWidth: rotoDisplayMode === 'both' ? '68px' : '55px'
                          }}
                        >
                          {cat.label} {rotoSortCol === cat.key ? (rotoSortDir === 'asc' ? '▲' : '▼') : ''}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedRotoTeams.map((t, idx) => {
                      const isExpanded = expandedRotoOwner === t.owner;
                      const isUser = t.owner === 'Daniel';
                      const rankBadge = t.rank === 1 ? '🥇' : t.rank === 2 ? '🥈' : t.rank === 3 ? '🥉' : `#${t.rank}`;

                      return (
                        <React.Fragment key={t.owner}>
                          <tr
                            onClick={() => setExpandedRotoOwner(isExpanded ? null : t.owner)}
                            style={{
                              borderBottom: '1px solid #262626',
                              background: isUser ? 'rgba(3, 218, 198, 0.07)' : idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)',
                              cursor: 'pointer',
                              transition: 'background 0.12s ease'
                            }}
                          >
                            <td style={{ padding: '10px 8px', fontWeight: 'bold', fontSize: '13px', color: t.rank <= 3 ? '#fff' : '#888' }}>
                              {rankBadge}
                            </td>
                            <td style={{ padding: '10px 14px', textAlign: 'left' }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <span style={{ fontWeight: 'bold', color: '#fff', fontSize: '13px' }}>
                                  {t.owner}
                                </span>
                                {isUser && (
                                  <span style={{ fontSize: '9px', background: '#03dac6', color: '#000', padding: '1px 5px', borderRadius: '3px', fontWeight: 'bold' }}>
                                    YOU
                                  </span>
                                )}
                                <span style={{ fontSize: '10px', color: '#666', marginLeft: 'auto' }}>
                                  {isExpanded ? '▲' : '▼'}
                                </span>
                              </div>
                              <div style={{ fontSize: '10px', color: '#777', marginTop: '1px' }}>
                                {t.keeperCount} {t.keeperCount === 1 ? 'Keeper' : 'Keepers'}
                              </div>
                            </td>

                            {/* Total Points */}
                            <td style={{ padding: '10px 12px' }}>
                              <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#03dac6' }}>
                                {t.totalPoints.toFixed(1)}
                              </div>
                              <div style={{
                                width: '100%',
                                height: '4px',
                                background: '#333',
                                borderRadius: '2px',
                                overflow: 'hidden',
                                marginTop: '3px'
                              }}>
                                <div style={{
                                  width: `${Math.min(100, (t.totalPoints / 90) * 100)}%`,
                                  height: '100%',
                                  background: 'linear-gradient(90deg, #10b981, #03dac6)'
                                }} />
                              </div>
                            </td>

                            {/* Batting Points */}
                            <td style={{ padding: '10px 10px', fontWeight: 'bold', color: '#10b981' }}>
                              {t.battingPoints.toFixed(1)}
                            </td>

                            {/* Pitching Points */}
                            <td style={{ padding: '10px 10px', fontWeight: 'bold', color: '#bb86fc', borderRight: '1px solid #333' }}>
                              {t.pitchingPoints.toFixed(1)}
                            </td>

                            {/* 5 Batting Categories */}
                            {['R', 'HR', 'RBI', 'SB', 'OBP'].map(cat => {
                              const pts = t.points[cat];
                              const raw = t.rawStats[cat];
                              const bStyle = getRotoBadgeStyle(pts);

                              return (
                                <td key={cat} style={{ padding: '8px 6px' }}>
                                  {rotoDisplayMode !== 'stats' && (
                                    <div style={{
                                      display: 'inline-block',
                                      background: bStyle.bg,
                                      color: bStyle.text,
                                      border: `1px solid ${bStyle.border}`,
                                      borderRadius: '4px',
                                      padding: '1px 6px',
                                      fontSize: '11px',
                                      fontWeight: 'bold',
                                      minWidth: '28px'
                                    }}>
                                      {pts.toFixed(1)}
                                    </div>
                                  )}
                                  {rotoDisplayMode !== 'points' && (
                                    <div style={{ fontSize: '10px', color: '#aaa', marginTop: rotoDisplayMode === 'both' ? '2px' : '0' }}>
                                      {formatRotoStat(cat, raw)}
                                    </div>
                                  )}
                                </td>
                              );
                            })}

                            {/* 5 Pitching Categories */}
                            {[
                              { key: 'K', label: 'K' },
                              { key: 'QS', label: 'QS' },
                              { key: 'SVHD', label: 'SV+HD' },
                              { key: 'ERA', label: 'ERA' },
                              { key: 'WHIP', label: 'WHIP' }
                            ].map(cat => {
                              const pts = t.points[cat.key];
                              const raw = t.rawStats[cat.key];
                              const bStyle = getRotoBadgeStyle(pts);

                              return (
                                <td key={cat.key} style={{ padding: '8px 6px' }}>
                                  {rotoDisplayMode !== 'stats' && (
                                    <div style={{
                                      display: 'inline-block',
                                      background: bStyle.bg,
                                      color: bStyle.text,
                                      border: `1px solid ${bStyle.border}`,
                                      borderRadius: '4px',
                                      padding: '1px 6px',
                                      fontSize: '11px',
                                      fontWeight: 'bold',
                                      minWidth: '28px'
                                    }}>
                                      {pts.toFixed(1)}
                                    </div>
                                  )}
                                  {rotoDisplayMode !== 'points' && (
                                    <div style={{ fontSize: '10px', color: '#aaa', marginTop: rotoDisplayMode === 'both' ? '2px' : '0' }}>
                                      {formatRotoStat(cat.key, raw)}
                                    </div>
                                  )}
                                </td>
                              );
                            })}
                          </tr>

                          {/* Expanded Keepers Detail Row */}
                          {isExpanded && (
                            <tr style={{ background: '#121212', borderBottom: '1px solid #333' }}>
                              <td colSpan={15} style={{ padding: '14px 16px', textAlign: 'left' }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                                  <span style={{ fontSize: '12px', fontWeight: 'bold', color: '#03dac6' }}>
                                    💎 {t.owner}'s Kept Players ({t.keepers.length})
                                  </span>
                                  <span style={{ fontSize: '11px', color: '#777' }}>
                                    Total Spend: ${t.keepers.reduce((s, k) => s + (k.cost || 0), 0)}
                                  </span>
                                </div>
                                <div style={{ overflowX: 'auto' }}>
                                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
                                    <thead>
                                      <tr style={{ background: '#1c1c1c', color: '#888', borderBottom: '1px solid #333' }}>
                                        <th style={{ padding: '6px 8px', textAlign: 'center', width: '35px' }}>Slot</th>
                                        <th style={{ padding: '6px 10px', textAlign: 'left' }}>Player</th>
                                        <th style={{ padding: '6px 8px', textAlign: 'center' }}>Pos</th>
                                        <th style={{ padding: '6px 8px', textAlign: 'center' }}>MLB</th>
                                        <th style={{ padding: '6px 8px', textAlign: 'center' }}>Rank</th>
                                        <th style={{ padding: '6px 8px', textAlign: 'center' }}>Cost</th>
                                        <th style={{ padding: '6px 8px', textAlign: 'center' }}>Token</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>R</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>HR</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>RBI</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>SB</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#10b981' }}>OBP</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>K</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>QS</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>SV+HD</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>ERA</th>
                                        <th style={{ padding: '6px 6px', textAlign: 'center', color: '#bb86fc' }}>WHIP</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {t.keepers.map(k => (
                                        <tr key={k.player_name || k.keeper_slot} style={{ borderBottom: '1px solid #222' }}>
                                          <td style={{ padding: '6px 8px', textAlign: 'center', color: '#888' }}>#{k.keeper_slot}</td>
                                          <td style={{ padding: '6px 10px', fontWeight: 'bold', color: '#fff' }}>{k.player_name}</td>
                                          <td style={{ padding: '6px 8px', textAlign: 'center', color: '#aaa' }}>{k.position || '-'}</td>
                                          <td style={{ padding: '6px 8px', textAlign: 'center', color: '#aaa' }}>{k.mlb_team || '-'}</td>
                                          <td style={{ padding: '6px 8px', textAlign: 'center', color: '#888' }}>#{k.rank}</td>
                                          <td style={{ padding: '6px 8px', textAlign: 'center', fontWeight: 'bold', color: '#ffb74d' }}>${k.cost}</td>
                                          <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                                            {k.token_applied ? (
                                              <span style={{ background: 'rgba(16, 185, 129, 0.2)', color: '#10b981', padding: '1px 6px', borderRadius: '4px', fontSize: '10px', fontWeight: 'bold' }}>
                                                🎫 Midpoint
                                              </span>
                                            ) : (
                                              <span style={{ color: '#555' }}>-</span>
                                            )}
                                          </td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.R > 0 ? k.stats.R : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.HR > 0 ? k.stats.HR : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.RBI > 0 ? k.stats.RBI : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.SB > 0 ? k.stats.SB : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.OBP > 0 ? formatRotoStat('OBP', k.stats.OBP) : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.K > 0 ? k.stats.K : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.QS > 0 ? k.stats.QS : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.SVHD > 0 ? k.stats.SVHD : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.ERA > 0 && k.stats.IP > 0 ? formatRotoStat('ERA', k.stats.ERA) : '-'}</td>
                                          <td style={{ padding: '6px 6px', textAlign: 'center', color: '#aaa' }}>{k.stats?.WHIP > 0 && k.stats.IP > 0 ? formatRotoStat('WHIP', k.stats.WHIP) : '-'}</td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              </td>
                            </tr>
                          )}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* SECTION B: 9 OWNER KEEPER & BUDGET CARDS */}
          {(summaryViewMode === 'ledger' || summaryViewMode === 'both') && (
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
              gap: '12px'
            }}>
              {sortedTeamBudgets.map(b => {
                const picks = compPicksByOwner[b.owner] || { bought: [], lost: [], sold: [] };
                const ownerKeepers = keepersByOwner[b.owner] || [];
                const tokenKeeper = ownerKeepers.find(k => k.token_applied);
                const isSelected = selectedOwner === b.owner;
                const actualFinish = getActualFinish(seasonYear, b.owner, b.finish_rank);

                return (
                  <div
                    key={b.owner}
                    onClick={() => setSelectedOwner(b.owner)}
                    style={{
                      background: isSelected ? '#22272e' : '#181818',
                      border: isSelected ? '1px solid #03dac6' : '1px solid #2a2a2a',
                      borderRadius: '8px',
                      padding: '14px',
                      cursor: 'pointer',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff' }}>{b.owner}</span>
                        <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold' }}>Fin #{actualFinish}</span>
                      </div>
                      <span style={{
                        fontSize: '11px',
                        fontWeight: 'bold',
                        padding: '2px 8px',
                        borderRadius: '10px',
                        background: b.final_budget < 0 ? 'rgba(244, 67, 54, 0.2)' : 'rgba(76, 175, 80, 0.2)',
                        color: b.final_budget < 0 ? '#f44336' : '#4caf50'
                      }}>
                        Rem: ${b.final_budget}
                      </span>
                    </div>

                    {/* Budget breakdown progress bar */}
                    <div style={{ display: 'flex', height: '6px', borderRadius: '3px', overflow: 'hidden', background: '#333', marginBottom: '10px' }}>
                      <div style={{ width: `${Math.min(100, (b.keeper_spend / b.base_budget) * 100)}%`, background: '#ffb74d' }} title={`Keepers: $${b.keeper_spend}`} />
                      <div style={{ width: `${Math.min(100, (b.comp_pick_spend / b.base_budget) * 100)}%`, background: '#bb86fc' }} title={`Comp Picks: $${b.comp_pick_spend}`} />
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px', fontSize: '11px', color: '#aaa', marginBottom: '10px' }}>
                      <div>Base Budget: <strong style={{ color: '#fff' }}>${b.base_budget}</strong></div>
                      <div>Keeper Spend: <strong style={{ color: '#ffb74d' }}>${b.keeper_spend}</strong></div>
                      <div>Comp Pick Spend: <strong style={{ color: '#bb86fc' }}>${b.comp_pick_spend}</strong></div>
                      <div>Net Draft Picks: <strong style={{ color: b.net_picks > 0 ? '#03dac6' : '#fff' }}>{b.net_picks > 0 ? `+${b.net_picks}` : b.net_picks}</strong></div>
                    </div>

                    {/* Bought & Offset Pills */}
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                      {picks.bought.map(p => (
                        <span key={p.round_num} style={{
                          background: 'rgba(187, 134, 252, 0.15)',
                          border: '1px solid rgba(187, 134, 252, 0.4)',
                          color: '#bb86fc',
                          fontSize: '10px',
                          padding: '1px 6px',
                          borderRadius: '4px'
                        }}>
                          +Rd {p.round_num} (${p.cost_or_income})
                        </span>
                      ))}
                      {picks.lost.map(p => (
                        <span key={p.round_num} style={{
                          background: 'rgba(244, 67, 54, 0.15)',
                          border: '1px solid rgba(244, 67, 54, 0.4)',
                          color: '#f44336',
                          fontSize: '10px',
                          padding: '1px 6px',
                          borderRadius: '4px'
                        }}>
                          -Rd {p.round_num} (Offset)
                        </span>
                      ))}
                      {picks.bought.length === 0 && picks.lost.length === 0 && (
                        <span style={{ fontSize: '10px', color: '#666', fontStyle: 'italic' }}>No comp picks traded</span>
                      )}
                    </div>

                    {/* Token Status Badge for 2027 */}
                    {seasonYear === 2027 && (
                      <div style={{ marginTop: '10px', paddingTop: '8px', borderTop: '1px solid #2a2a2a', fontSize: '11px' }}>
                        {tokenKeeper ? (
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <span style={{ color: '#10b981', display: 'flex', alignItems: 'center', gap: '4px', fontWeight: 'bold' }}>
                              <span>🎫</span>
                              <span>Token: {tokenKeeper.player_name}</span>
                            </span>
                            <span style={{ color: '#03dac6', fontSize: '10px', fontWeight: 'bold' }}>
                              Midpoint: ${tokenKeeper.cost}
                            </span>
                          </div>
                        ) : (
                          <span style={{ color: '#888', fontStyle: 'italic', display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span>🎫</span>
                            <span>Midpoint Token: Unused (1 Available)</span>
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* SECTION C: COMPLETE BUDGET & PICK LEDGER TABLE */}
          {(summaryViewMode === 'ledger' || summaryViewMode === 'both') && (
            <div style={{
              background: '#181818',
              borderRadius: '8px',
              border: '1px solid #2a2a2a',
              overflow: 'hidden'
            }}>
              <div style={{ padding: '12px 16px', borderBottom: '1px solid #2a2a2a', fontWeight: 'bold', color: '#fff', fontSize: '13px' }}>
                📊 Complete {seasonYear} Budget & Compensation Pick Ledger
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'left' }}>
                  <thead>
                    <tr style={{ background: '#1f1f1f', color: '#888', borderBottom: '1px solid #333' }}>
                      <th style={{ padding: '10px 14px' }}>Finish</th>
                      <th style={{ padding: '10px 14px' }}>Owner</th>
                      <th style={{ padding: '10px 14px' }}>Base Budget</th>
                      <th style={{ padding: '10px 14px' }}>Keeper Spend</th>
                      <th style={{ padding: '10px 14px' }}>Comp Pick Spend</th>
                      <th style={{ padding: '10px 14px' }}>Comp Pick Income</th>
                      <th style={{ padding: '10px 14px' }}>Final Remaining</th>
                      <th style={{ padding: '10px 14px' }}>Purchased Picks</th>
                      <th style={{ padding: '10px 14px' }}>Offset Picks</th>
                      <th style={{ padding: '10px 14px' }}>Net Picks</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedTeamBudgets.map((b, idx) => {
                      const picks = compPicksByOwner[b.owner] || { bought: [], lost: [], sold: [] };
                      const actualFinish = getActualFinish(seasonYear, b.owner, b.finish_rank);

                      return (
                        <tr
                          key={b.owner}
                          style={{
                            borderBottom: '1px solid #262626',
                            background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)'
                          }}
                        >
                          <td style={{ padding: '10px 14px', color: '#fff', fontWeight: 'bold' }}>#{actualFinish}</td>
                          <td style={{ padding: '10px 14px', fontWeight: 'bold', color: '#fff' }}>{b.owner}</td>
                          <td style={{ padding: '10px 14px', color: '#03dac6' }}>${b.base_budget}</td>
                          <td style={{ padding: '10px 14px', color: '#ffb74d' }}>${b.keeper_spend}</td>
                          <td style={{ padding: '10px 14px', color: '#bb86fc' }}>${b.comp_pick_spend}</td>
                          <td style={{ padding: '10px 14px', color: '#4caf50' }}>${b.comp_pick_income}</td>
                          <td style={{ padding: '10px 14px', fontWeight: 'bold', color: b.final_budget < 0 ? '#f44336' : '#fff' }}>
                            ${b.final_budget}
                          </td>
                          <td style={{ padding: '10px 14px' }}>
                            {picks.bought.map(p => `Rd ${p.round_num} ($${p.cost_or_income})`).join(', ') || 'None'}
                          </td>
                          <td style={{ padding: '10px 14px', color: '#f44336' }}>
                            {picks.lost.map(p => `Rd ${p.round_num}`).join(', ') || 'None'}
                          </td>
                          <td style={{ padding: '10px 14px', fontWeight: 'bold', color: b.net_picks > 0 ? '#03dac6' : '#fff' }}>
                            {b.net_picks > 0 ? `+${b.net_picks}` : b.net_picks}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* SUB-TAB 2: KEEPER ROSTERS */}
      {activeTab === 'rosters' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Filter Bar */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span style={{ fontSize: '13px', color: '#888' }}>Filter Owner:</span>
            <select
              value={keeperOwnerFilter}
              onChange={e => setKeeperOwnerFilter(e.target.value)}
              style={{
                background: '#222',
                color: '#fff',
                border: '1px solid #444',
                borderRadius: '4px',
                padding: '6px 12px',
                fontSize: '12px'
              }}
            >
              <option value="ALL">All Owners ({seasonYear >= 2027 ? 54 : 45} Keepers)</option>
              {DRAFT_MANAGERS.map(m => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </div>

          {/* Grid of Owner Keeper Rosters */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))',
            gap: '14px'
          }}>
            {DRAFT_MANAGERS.filter(m => keeperOwnerFilter === 'ALL' || keeperOwnerFilter === m).map(owner => {
              const ownerKeepers = keepersByOwner[owner] || [];
              const totalCost = ownerKeepers.reduce((sum, k) => sum + (k.cost || 0), 0);
              const validRanks = ownerKeepers.map(k => k.rank).filter(Boolean);
              const avgRank = validRanks.length ? (validRanks.reduce((s, r) => s + r, 0) / validRanks.length).toFixed(1) : 0;

              return (
                <div
                  key={owner}
                  style={{
                    background: '#181818',
                    border: '1px solid #2a2a2a',
                    borderRadius: '8px',
                    padding: '14px',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '10px'
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid #2a2a2a', paddingBottom: '8px' }}>
                    <span style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff' }}>{owner}</span>
                    <div style={{ display: 'flex', gap: '8px', fontSize: '11px' }}>
                      <span style={{ color: '#ffb74d' }}>Spend: <strong>${totalCost}</strong></span>
                      <span style={{ color: '#888' }}>Avg Rank: <strong>{avgRank}</strong></span>
                    </div>
                  </div>

                  {/* 5 Keepers */}
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    {ownerKeepers.map(k => {
                      const isHigh = k.cost >= 30;
                      const isMid = k.cost >= 15 && k.cost < 30;
                      const isSleeper = k.cost === 0;

                      return (
                        <div
                          key={k.keeper_slot}
                          onClick={() => onPlayerClick && onPlayerClick(k.espn_player_id)}
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                            background: '#222',
                            padding: '8px 10px',
                            borderRadius: '6px',
                            border: k.token_applied ? '1px solid #059669' : '1px solid #333',
                            cursor: onPlayerClick ? 'pointer' : 'default',
                            transition: 'background 0.15s ease'
                          }}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span style={{
                              fontSize: '11px',
                              fontWeight: 'bold',
                              color: '#666',
                              width: '18px'
                            }}>
                              #{k.keeper_slot}
                            </span>
                            <div>
                              <div style={{ fontWeight: 'bold', fontSize: '13px', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <span>{k.player_name}</span>
                                {k.token_applied && (
                                  <span style={{
                                    background: 'rgba(16, 185, 129, 0.2)',
                                    color: '#10b981',
                                    border: '1px solid #059669',
                                    fontSize: '10px',
                                    padding: '1px 5px',
                                    borderRadius: '4px',
                                    fontWeight: 'bold'
                                  }}>
                                    🎫 Midpoint Token
                                  </span>
                                )}
                              </div>
                              <div style={{ fontSize: '11px', color: '#888' }}>
                                {k.position || 'N/A'} • {k.mlb_team || 'MLB'} • Rank {k.rank || 'N/A'}
                                {k.token_applied && k.prior_cost != null && k.new_cost != null && (
                                  <span style={{ color: '#03dac6', marginLeft: '6px' }}>
                                    (Prior: ${k.prior_cost} | 2027: ${k.new_cost} | Midpoint: ${k.cost})
                                  </span>
                                )}
                              </div>
                            </div>
                          </div>

                          <div style={{ textAlign: 'right' }}>
                            <span style={{
                              fontWeight: 'bold',
                              fontSize: '13px',
                              padding: '2px 8px',
                              borderRadius: '4px',
                              background: isHigh ? 'rgba(255, 183, 77, 0.2)' : isMid ? 'rgba(3, 218, 198, 0.2)' : isSleeper ? 'rgba(187, 134, 252, 0.2)' : 'rgba(76, 175, 80, 0.2)',
                              color: isHigh ? '#ffb74d' : isMid ? '#03dac6' : isSleeper ? '#bb86fc' : '#4caf50',
                              border: `1px solid ${isHigh ? 'rgba(255, 183, 77, 0.4)' : isMid ? 'rgba(3, 218, 198, 0.4)' : isSleeper ? 'rgba(187, 134, 252, 0.4)' : 'rgba(76, 175, 80, 0.4)'}`
                            }}>
                              ${k.cost}
                            </span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* SUB-TAB 3: INTERACTIVE COMPENSATION PICK SIMULATOR */}
      {activeTab === 'simulator' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Simulator Controls & Manager Selector */}
          <div style={{
            background: '#1e1e1e',
            padding: '14px 16px',
            borderRadius: '8px',
            border: '1px solid #333',
            display: 'flex',
            flexWrap: 'wrap',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: '12px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#fff' }}>Simulate For Manager:</span>
              <select
                value={simulatedOwner}
                onChange={e => {
                  setSimulatedOwner(e.target.value);
                  // Reset simulator to that owner's real picks
                  const bought = {};
                  const sold = {};
                  compPicks.filter(p => p.owner === e.target.value).forEach(p => {
                    if (p.action_type === 'BOUGHT') bought[p.round_num] = true;
                    if (p.action_type === 'SOLD') sold[p.round_num] = true;
                  });
                  setSimBoughtRounds(bought);
                  setSimSoldRounds(sold);
                }}
                style={{
                  background: '#111',
                  color: '#fff',
                  border: '1px solid #444',
                  borderRadius: '4px',
                  padding: '6px 12px',
                  fontSize: '13px',
                  fontWeight: 'bold'
                }}
              >
                {DRAFT_MANAGERS.map(m => (
                  <option key={m} value={m}>{m}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <button
                onClick={resetSimulatorToReal}
                style={{
                  background: '#333',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '6px 12px',
                  fontSize: '11px',
                  cursor: 'pointer'
                }}
              >
                🔄 Reset Picks
              </button>

              <button
                onClick={handleSaveCompPicks}
                disabled={savingCompPicks || simCalculations.simulatedRemaining < 0}
                style={{
                  background: simCalculations.simulatedRemaining < 0
                    ? '#444'
                    : isCommissioner && simulatedOwner !== profile?.owner_name
                      ? '#d97706'
                      : '#7c3aed',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '6px 14px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: (savingCompPicks || simCalculations.simulatedRemaining < 0) ? 'not-allowed' : 'pointer',
                  opacity: (savingCompPicks || simCalculations.simulatedRemaining < 0) ? 0.6 : 1,
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.3)'
                }}
              >
                {savingCompPicks
                  ? 'Saving...'
                  : isCommissioner && simulatedOwner !== profile?.owner_name
                    ? `👑 Commish Save for ${simulatedOwner}`
                    : '💾 Save & Submit Consolation Picks'}
              </button>
            </div>
          </div>

          {/* Real-time Calculation Dashboard */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
            gap: '10px'
          }}>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Base Budget</div>
              <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#03dac6', marginTop: '4px' }}>
                ${simCalculations.baseBudget}
              </div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Keeper Spend</div>
              <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#ffb74d', marginTop: '4px' }}>
                ${simCalculations.keeperSpend}
              </div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Comp Spend (Simulated)</div>
              <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#bb86fc', marginTop: '4px' }}>
                ${simCalculations.spend}
              </div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Comp Income (Simulated)</div>
              <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#4caf50', marginTop: '4px' }}>
                +${simCalculations.income}
              </div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Simulated Remaining</div>
              <div style={{
                fontSize: '18px',
                fontWeight: 'bold',
                color: simCalculations.simulatedRemaining < 0 ? '#f44336' : '#03dac6',
                marginTop: '4px'
              }}>
                ${simCalculations.simulatedRemaining}
              </div>
            </div>
          </div>

          {/* Pick Impact Analysis Box */}
          <div style={{
            background: '#16191f',
            border: '1px solid #233446',
            borderRadius: '8px',
            padding: '14px',
            display: 'flex',
            flexDirection: 'column',
            gap: '8px'
          }}>
            <span style={{ fontWeight: 'bold', fontSize: '13px', color: '#03dac6' }}>
              🎯 Simulated Draft Pick Order & Roster Impact:
            </span>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'center', fontSize: '12px' }}>
              <span style={{ color: '#aaa' }}>Added Picks (End of Round):</span>
              {simCalculations.boughtList.map(b => (
                <span key={b.round} style={{
                  background: 'rgba(3, 218, 198, 0.15)',
                  border: '1px solid #03dac6',
                  color: '#03dac6',
                  padding: '2px 8px',
                  borderRadius: '4px',
                  fontWeight: 'bold'
                }}>
                  🎟️ Round {b.round} (+1 pick, -${b.cost})
                </span>
              ))}
              {simCalculations.boughtList.length === 0 && <span style={{ color: '#666' }}>None</span>}
            </div>

            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'center', fontSize: '12px' }}>
              <span style={{ color: '#aaa' }}>Forfeited Picks (Offset from Draft End):</span>
              {simCalculations.offsetRounds.map(r => (
                <span key={r} style={{
                  background: 'rgba(244, 67, 54, 0.15)',
                  border: '1px solid #f44336',
                  color: '#f44336',
                  padding: '2px 8px',
                  borderRadius: '4px',
                  fontWeight: 'bold'
                }}>
                  🚫 Round {r} (Offset/Lost)
                </span>
              ))}
              {simCalculations.offsetRounds.length === 0 && <span style={{ color: '#666' }}>None</span>}
            </div>

            <div style={{ fontSize: '11px', color: '#888', borderTop: '1px solid #222', paddingTop: '6px', marginTop: '4px' }}>
              ⚖️ <strong>Roster Rule:</strong> Fixed roster size of 32 players ({seasonYear >= 2027 ? '6 keepers + 26 drafted players' : '5 keepers + 27 drafted players'}). Buying earlier picks automatically drops late-round selections.
            </div>
          </div>

          {/* Interactive Buy Board */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            padding: '14px'
          }}>
            <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', marginBottom: '10px' }}>
              🛍️ Purchase Additional Draft Picks ({seasonYear >= 2027 ? 'Rounds 7–21' : 'Rounds 6–20'})
            </div>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
              gap: '8px'
            }}>
              {Object.keys(compBuyPrices).map(rStr => {
                const r = parseInt(rStr, 10);
                const cost = compBuyPrices[r];
                const isBought = !!simBoughtRounds[r];

                return (
                  <button
                    key={r}
                    onClick={() => {
                      setSimBoughtRounds(prev => ({
                        ...prev,
                        [r]: !prev[r]
                      }));
                    }}
                    style={{
                      background: isBought ? 'rgba(3, 218, 198, 0.2)' : '#222',
                      border: isBought ? '1px solid #03dac6' : '1px solid #333',
                      borderRadius: '6px',
                      padding: '10px 8px',
                      cursor: 'pointer',
                      textAlign: 'center',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    <div style={{ fontWeight: 'bold', fontSize: '13px', color: isBought ? '#03dac6' : '#fff' }}>
                      Round {r}
                    </div>
                    <div style={{ fontSize: '12px', color: '#ffb74d', marginTop: '2px' }}>
                      Cost: ${cost}
                    </div>
                    <div style={{
                      fontSize: '10px',
                      fontWeight: 'bold',
                      color: isBought ? '#03dac6' : '#888',
                      marginTop: '4px'
                    }}>
                      {isBought ? '✓ PURCHASED' : '+ BUY PICK'}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>

          {/* Interactive Sell Board */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            padding: '14px'
          }}>
            <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', marginBottom: '10px' }}>
              💵 Sell Draft Picks for Budget Income ({seasonYear >= 2027 ? 'Rounds 7–10' : 'Rounds 6–9'})
            </div>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
              gap: '8px'
            }}>
              {Object.keys(compSellPrices).map(rStr => {
                const r = parseInt(rStr, 10);
                const income = compSellPrices[r];
                const isSold = !!simSoldRounds[r];

                return (
                  <button
                    key={r}
                    onClick={() => {
                      setSimSoldRounds(prev => ({
                        ...prev,
                        [r]: !prev[r]
                      }));
                    }}
                    style={{
                      background: isSold ? 'rgba(76, 175, 80, 0.2)' : '#222',
                      border: isSold ? '1px solid #4caf50' : '1px solid #333',
                      borderRadius: '6px',
                      padding: '10px 8px',
                      cursor: 'pointer',
                      textAlign: 'center',
                      transition: 'all 0.15s ease'
                    }}
                  >
                    <div style={{ fontWeight: 'bold', fontSize: '13px', color: isSold ? '#4caf50' : '#fff' }}>
                      Round {r}
                    </div>
                    <div style={{ fontSize: '12px', color: '#4caf50', marginTop: '2px' }}>
                      Gain: +${income}
                    </div>
                    <div style={{
                      fontSize: '10px',
                      fontWeight: 'bold',
                      color: isSold ? '#4caf50' : '#888',
                      marginTop: '4px'
                    }}>
                      {isSold ? '✓ SOLD FOR CASH' : '- SELL PICK'}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* SUB-TAB 4: KEEPER SUBMISSION */}
      {activeTab === 'planner' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Planner Controls */}
          <div style={{
            background: '#1e1e1e',
            padding: '14px 16px',
            borderRadius: '8px',
            border: '1px solid #333',
            display: 'flex',
            flexWrap: 'wrap',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: '12px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#fff' }}>Submitting For Manager:</span>
              <select
                value={plannerOwner}
                onChange={e => {
                  setPlannerOwner(e.target.value);
                  setReplacedKeepers({});
                }}
                style={{
                  background: '#111',
                  color: '#fff',
                  border: '1px solid #444',
                  borderRadius: '4px',
                  padding: '6px 12px',
                  fontSize: '13px',
                  fontWeight: 'bold'
                }}
              >
                {DRAFT_MANAGERS.map(m => (
                  <option key={m} value={m}>{m}</option>
                ))}
              </select>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <button
                onClick={() => setReplacedKeepers({})}
                style={{
                  background: '#333',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '6px 12px',
                  fontSize: '11px',
                  cursor: 'pointer'
                }}
              >
                🔄 Reset Keepers
              </button>

              <button
                onClick={handleSaveKeepers}
                disabled={savingKeepers || (isDeadlinePassed && !isCommissioner)}
                style={{
                  background: (isDeadlinePassed && !isCommissioner)
                    ? '#444'
                    : isCommissioner && plannerOwner !== profile?.owner_name
                      ? '#d97706'
                      : '#059669',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '6px 14px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: (isDeadlinePassed && !isCommissioner || savingKeepers) ? 'not-allowed' : 'pointer',
                  opacity: (isDeadlinePassed && !isCommissioner || savingKeepers) ? 0.6 : 1,
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.3)'
                }}
              >
                {savingKeepers
                  ? 'Saving...'
                  : isDeadlinePassed && !isCommissioner
                    ? '🔒 Keepers Locked'
                    : isCommissioner && plannerOwner !== profile?.owner_name
                      ? `👑 Commish Save for ${plannerOwner}`
                      : '💾 Save & Submit Official Keepers'}
              </button>
            </div>
          </div>

          {/* Keeper Submission Overview Dashboard */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
            gap: '10px'
          }}>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Keeper Spend</div>
              <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#ffb74d', marginTop: '4px' }}>
                ${plannerData.totalCost}
              </div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Cap: ${plannerData.baseBudget}</div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Remaining for Draft/Comp</div>
              <div style={{
                fontSize: '20px',
                fontWeight: 'bold',
                color: plannerData.remainingBudget < 0 ? '#f44336' : '#03dac6',
                marginTop: '4px'
              }}>
                ${plannerData.remainingBudget}
              </div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>
                {plannerData.remainingBudget < 0 ? '⚠️ OVER BUDGET' : 'Under Cap'}
              </div>
            </div>
            <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
              <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Average Keeper Rank</div>
              <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#bb86fc', marginTop: '4px' }}>
                {plannerData.avgRank}
              </div>
              <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>Across {keeperSlots.length} Keepers</div>
            </div>
            {seasonYear === 2027 && (
              <div style={{ background: '#1c1c1c', padding: '12px', borderRadius: '6px', border: '1px solid #333' }}>
                <div style={{ fontSize: '11px', color: '#888', textTransform: 'uppercase' }}>Keeper Discount Token</div>
                <div style={{ fontSize: '20px', fontWeight: 'bold', color: plannerData.tokenSlot ? '#10b981' : '#aaa', marginTop: '4px' }}>
                  {plannerData.tokenSlot ? `Slot #${plannerData.tokenSlot}` : '1 Available'}
                </div>
                <div style={{ fontSize: '11px', color: '#666', marginTop: '2px' }}>
                  {plannerData.tokenSlot ? `Saved $${plannerData.totalTokenSavings}` : 'Pays midpoint of 2026 & 2027 (rounded down)'}
                </div>
              </div>
            )}
          </div>

          {/* 5 Keepers with Swap Options */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
            gap: '12px'
          }}>
            {plannerData.keepers.map(k => (
              <div
                key={k.keeper_slot}
                style={{
                  background: '#181818',
                  border: k.token_applied ? '1px solid #10b981' : k.isReplaced ? '1px solid #4caf50' : '1px solid #2e2e2e',
                  borderRadius: '8px',
                  padding: '12px',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '8px'
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontSize: '12px', fontWeight: 'bold', color: '#888' }}>
                    Keeper Slot #{k.keeper_slot}
                  </span>
                  <div style={{ display: 'flex', gap: '4px' }}>
                    {k.token_applied && (
                      <span style={{
                        background: 'rgba(16, 185, 129, 0.2)',
                        color: '#10b981',
                        fontSize: '10px',
                        padding: '1px 6px',
                        borderRadius: '4px',
                        fontWeight: 'bold'
                      }}>
                        🎫 MIDPOINT TOKEN
                      </span>
                    )}
                    {k.isReplaced && (
                      <span style={{
                        background: 'rgba(76, 175, 80, 0.2)',
                        color: '#4caf50',
                        fontSize: '10px',
                        padding: '1px 6px',
                        borderRadius: '4px',
                        fontWeight: 'bold'
                      }}>
                        TEST SWAP
                      </span>
                    )}
                  </div>
                </div>

                <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff' }}>
                  {k.player_name}
                </div>
                <div style={{ fontSize: '11px', color: '#aaa' }}>
                  {k.position || 'N/A'} • {k.mlb_team || 'MLB'} • Rank: {k.rank || 'N/A'}
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '4px' }}>
                  <div>
                    <span style={{ fontSize: '13px', fontWeight: 'bold', color: '#ffb74d' }}>
                      Cost: ${k.cost}
                    </span>
                    {k.token_applied && (
                      <span style={{
                        marginLeft: '6px',
                        fontSize: '10px',
                        fontWeight: 'bold',
                        color: '#10b981',
                        background: 'rgba(16, 185, 129, 0.15)',
                        padding: '1px 5px',
                        borderRadius: '4px',
                        border: '1px solid rgba(16, 185, 129, 0.3)'
                      }}>
                        -${k.token_savings}
                      </span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                    {seasonYear === 2027 && !k.isEmpty && (
                      <button
                        onClick={() => setTokenSlot(prev => prev === k.keeper_slot ? null : k.keeper_slot)}
                        style={{
                          background: k.token_applied ? 'rgba(16, 185, 129, 0.25)' : '#262626',
                          color: k.token_applied ? '#6ee7b7' : '#bbb',
                          border: k.token_applied ? '1px solid #10b981' : '1px solid #444',
                          borderRadius: '4px',
                          padding: '3px 8px',
                          fontSize: '11px',
                          fontWeight: 'bold',
                          cursor: 'pointer',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '4px',
                          transition: 'all 0.15s ease'
                        }}
                        title={k.token_applied ? 'Click to remove token' : 'Apply 1 token to pay midpoint of prior year price and 2027 price (rounded down to whole dollar)'}
                      >
                        <span>{k.token_applied ? '✅ Active' : '🎫 Token'}</span>
                      </button>
                    )}
                    {k.isReplaced && (
                      <button
                        onClick={() => {
                          setReplacedKeepers(prev => {
                            const copy = { ...prev };
                            delete copy[k.keeper_slot];
                            return copy;
                          });
                        }}
                        style={{
                          background: 'transparent',
                          border: 'none',
                          color: '#f44336',
                          fontSize: '11px',
                          cursor: 'pointer',
                          textDecoration: 'underline'
                        }}
                      >
                        Revert
                      </button>
                    )}
                  </div>
                </div>
                {k.token_applied && (
                  <div style={{ fontSize: '10px', color: '#03dac6', marginTop: '2px', background: '#111', padding: '4px 6px', borderRadius: '4px' }}>
                    Prior (2026): <strong>${k.prior_cost ?? k.cost}</strong> • Standard (2027): <strong>${k.new_cost ?? k.cost}</strong> • You Pay Midpoint: <strong>${k.cost}</strong>
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Search & Select Player Pool to Swap */}
          <div style={{
            background: '#181818',
            borderRadius: '8px',
            border: '1px solid #2a2a2a',
            padding: '16px',
            display: 'flex',
            flexDirection: 'column',
            gap: '12px'
          }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: '8px' }}>
              <div>
                <div style={{ fontWeight: 'bold', fontSize: '14px', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <span>🎯</span>
                  <span>Swap Into Keepers: Select From Roster or Search Pool</span>
                </div>
                <div style={{ fontSize: '11px', color: '#888', marginTop: '2px' }}>
                  Defaulting to <strong>{plannerOwner}</strong>'s current roster. Pick any player and click [Slot 1] – [Slot 5] to test swapping them into your keepers.
                </div>
              </div>

              {/* Scope filter pills */}
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                <button
                  type="button"
                  onClick={() => setPlannerScope('roster')}
                  style={{
                    background: plannerScope === 'roster' ? '#059669' : '#262626',
                    color: plannerScope === 'roster' ? '#fff' : '#aaa',
                    border: plannerScope === 'roster' ? '1px solid #10b981' : '1px solid #3e3e3e',
                    borderRadius: '6px',
                    padding: '5px 10px',
                    fontSize: '11px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px',
                    transition: 'all 0.15s ease'
                  }}
                >
                  <span>👤</span>
                  <span>{plannerOwner}'s Current Roster ({plannerOwnerRosterCount})</span>
                </button>

                <button
                  type="button"
                  onClick={() => setPlannerScope('roster_fa')}
                  style={{
                    background: plannerScope === 'roster_fa' ? '#0284c7' : '#262626',
                    color: plannerScope === 'roster_fa' ? '#fff' : '#aaa',
                    border: plannerScope === 'roster_fa' ? '1px solid #38bdf8' : '1px solid #3e3e3e',
                    borderRadius: '6px',
                    padding: '5px 10px',
                    fontSize: '11px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px',
                    transition: 'all 0.15s ease'
                  }}
                >
                  <span>🆓</span>
                  <span>Roster + Free Agents</span>
                </button>

                <button
                  type="button"
                  onClick={() => setPlannerScope('all')}
                  style={{
                    background: plannerScope === 'all' ? '#7c3aed' : '#262626',
                    color: plannerScope === 'all' ? '#fff' : '#aaa',
                    border: plannerScope === 'all' ? '1px solid #a78bfa' : '1px solid #3e3e3e',
                    borderRadius: '6px',
                    padding: '5px 10px',
                    fontSize: '11px',
                    fontWeight: 'bold',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px',
                    transition: 'all 0.15s ease'
                  }}
                >
                  <span>🌐</span>
                  <span>All Players ({players.length})</span>
                </button>
              </div>
            </div>

            {/* Filter Search Input */}
            <div style={{ position: 'relative', width: '100%' }}>
              <input
                type="text"
                placeholder={
                  plannerScope === 'roster'
                    ? `Filter ${plannerOwner}'s roster by player name, position (SP, OF, SS), or MLB team...`
                    : 'Search player pool by name, position (e.g. SP, OF, C), or MLB team (e.g. LAD, NYY)...'
                }
                value={plannerSearch}
                onChange={e => setPlannerSearch(e.target.value)}
                style={{
                  width: '100%',
                  background: '#111',
                  color: '#fff',
                  border: '1px solid #444',
                  borderRadius: '6px',
                  padding: '9px 36px 9px 12px',
                  fontSize: '13px',
                  boxSizing: 'border-box',
                  outline: 'none'
                }}
              />
              {plannerSearch && (
                <button
                  type="button"
                  onClick={() => setPlannerSearch('')}
                  style={{
                    position: 'absolute',
                    right: '10px',
                    top: '50%',
                    transform: 'translateY(-50%)',
                    background: 'transparent',
                    border: 'none',
                    color: '#888',
                    fontSize: '14px',
                    cursor: 'pointer',
                    padding: '2px 6px'
                  }}
                  title="Clear search filter"
                >
                  ✕
                </button>
              )}
            </div>

            {/* Candidate List Counter & Status */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '11px', color: '#888', px: '2px' }}>
              <span>
                {plannerCandidates.length === 0
                  ? 'No matching players found'
                  : `Showing ${Math.min(plannerCandidates.length, 60)} of ${plannerCandidates.length} ${plannerScope === 'roster' ? `${plannerOwner} rostered` : ''} players`}
              </span>
              <span style={{ color: '#666' }}>
                💡 Click any slot button to preview keeper budget impact
              </span>
            </div>

            {/* Candidate List */}
            <div style={{
              display: 'flex',
              flexDirection: 'column',
              gap: '6px',
              maxHeight: '360px',
              overflowY: 'auto',
              paddingRight: '4px'
            }}>
              {plannerCandidates.length === 0 ? (
                <div style={{
                  padding: '30px 16px',
                  textAlign: 'center',
                  color: '#777',
                  fontSize: '13px',
                  background: '#141414',
                  borderRadius: '6px',
                  border: '1px dashed #333'
                }}>
                  {plannerSearch.trim()
                    ? `No players found matching "${plannerSearch}" in ${plannerScope === 'roster' ? `${plannerOwner}'s roster` : plannerScope === 'roster_fa' ? 'roster + free agents' : 'player pool'}.`
                    : `No players available in selected scope.`}
                </div>
              ) : (
                plannerCandidates.slice(0, 60).map(p => {
                  const pid = String(p.id || p.espn_player_id || p['ESPN PlayerID'] || '');
                  const pName = p.name || p.Player || p.full_name || 'Player';
                  const rank = p.rank || 999;
                  const cost = p.cost !== undefined ? p.cost : calculateKeeperCostFromRank(rank);
                  const isCurrentManagerRoster = normalizeManager(p.rosterOwner) === normalizeManager(plannerOwner);
                  const isFreeAgent = !p.rosterOwner || p.rosterOwner === 'Available' || p.rosterOwner === 'Free Agent' || p.isFreeAgent;

                  // Check if player is currently in one of the keeper slots
                  const currentKeeperSlot = keeperSlots.find(slotNum => {
                    const slotKeeper = plannerData.keepers.find(k => k.keeper_slot === slotNum);
                    if (!slotKeeper || slotKeeper.isEmpty) return false;
                    const skId = String(slotKeeper.espn_player_id || '');
                    const skName = (slotKeeper.player_name || '').toLowerCase().trim();
                    return (pid && pid === skId) || (skName && skName === pName.toLowerCase().trim());
                  });

                  return (
                    <div
                      key={`${pid || pName}_${p.position}`}
                      style={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        background: currentKeeperSlot ? '#1c261e' : '#202020',
                        padding: '8px 12px',
                        borderRadius: '6px',
                        border: currentKeeperSlot ? '1px solid #059669' : '1px solid #303030',
                        gap: '8px',
                        transition: 'background 0.15s ease'
                      }}
                    >
                      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '8px' }}>
                        <strong style={{ color: '#fff', fontSize: '13px' }}>
                          {pName}
                        </strong>

                        {/* Position & Team badge */}
                        <span style={{
                          background: '#2d2d2d',
                          color: '#ccc',
                          fontSize: '10px',
                          padding: '2px 6px',
                          borderRadius: '4px',
                          fontWeight: '600'
                        }}>
                          {p.position || p.Position || 'UTIL'} • {p.team || p.Team || 'MLB'}
                        </span>

                        {/* Rank & Cost badge */}
                        <span style={{
                          background: '#332914',
                          color: '#fbbf24',
                          border: '1px solid #78350f',
                          fontSize: '10px',
                          padding: '2px 6px',
                          borderRadius: '4px',
                          fontWeight: 'bold'
                        }}>
                          Rank #{rank} • ${cost}
                        </span>

                        {seasonYear === 2027 && (
                          <span style={{
                            background: '#132e27',
                            color: '#6ee7b7',
                            border: '1px solid #065f46',
                            fontSize: '10px',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            fontWeight: 'bold'
                          }} title="If your 1 keeper token is applied to this player">
                            Midpoint: ${Math.floor((getPriorCost(pid, pName, cost) + cost) / 2)}
                          </span>
                        )}

                        {/* Ownership badge */}
                        {isCurrentManagerRoster ? (
                          <span style={{
                            background: '#064e3b',
                            color: '#6ee7b7',
                            border: '1px solid #047857',
                            fontSize: '10px',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            fontWeight: '600'
                          }}>
                            👤 {plannerOwner}'s Roster
                          </span>
                        ) : isFreeAgent ? (
                          <span style={{
                            background: '#0c4a6e',
                            color: '#7dd3fc',
                            border: '1px solid #0284c7',
                            fontSize: '10px',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            fontWeight: '600'
                          }}>
                            🆓 Free Agent
                          </span>
                        ) : (
                          <span style={{
                            background: '#1e293b',
                            color: '#94a3b8',
                            border: '1px solid #334155',
                            fontSize: '10px',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            fontWeight: '600'
                          }}>
                            🔒 {p.rosterOwner}
                          </span>
                        )}

                        {/* Already Kept Indicator */}
                        {currentKeeperSlot && (
                          <span style={{
                            background: '#059669',
                            color: '#fff',
                            fontSize: '10px',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            fontWeight: 'bold'
                          }}>
                            ✓ Slot {currentKeeperSlot}
                          </span>
                        )}
                      </div>

                      {/* Swap Buttons */}
                      <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                        {keeperSlots.map(slot => {
                          const isThisSlot = currentKeeperSlot === slot;
                          return (
                            <button
                              key={slot}
                              type="button"
                              onClick={() => {
                                setReplacedKeepers(prev => ({
                                  ...prev,
                                  [slot]: p
                                }));
                              }}
                              style={{
                                background: isThisSlot ? '#059669' : '#2b2b2b',
                                color: isThisSlot ? '#ffffff' : '#38bdf8',
                                border: isThisSlot ? '1px solid #10b981' : '1px solid #444',
                                borderRadius: '4px',
                                padding: '4px 8px',
                                fontSize: '11px',
                                fontWeight: 'bold',
                                cursor: 'pointer',
                                transition: 'all 0.15s ease'
                              }}
                              title={`Swap ${pName} into Keeper Slot ${slot}`}
                            >
                              {isThisSlot ? `✓ Slot ${slot}` : `Slot ${slot}`}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>
      )}

      {/* SUB-TAB 5: COMMISSIONER SETTINGS & MANUAL ADJUSTMENTS (DAN & ADRIAN ONLY) */}
      {activeTab === 'settings' && (
        !isCommissioner ? (
          <div style={{
            background: '#1a1a1a',
            borderRadius: '8px',
            border: '1px solid #333',
            padding: '40px 20px',
            textAlign: 'center',
            maxWidth: '500px',
            margin: '20px auto'
          }}>
            <div style={{ fontSize: '36px', marginBottom: '12px' }}>🔒</div>
            <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#fff' }}>Commissioner & Admin Access Only</div>
            <div style={{ fontSize: '12px', color: '#888', marginTop: '8px', lineHeight: '1.5' }}>
              Setting the official keeper deadline, base budget, and applying punitive or award adjustments to individual owners is restricted to Commissioner (<strong>Adrian</strong>) and Admin (<strong>Dan</strong>).
            </div>
            {user ? (
              <div style={{ fontSize: '11px', color: '#818cf8', marginTop: '16px', paddingTop: '12px', borderTop: '1px solid #282828' }}>
                Logged in as <strong>{profile?.owner_name || user.email}</strong> (Owner)
              </div>
            ) : (
              <div style={{ fontSize: '11px', color: '#666', marginTop: '16px', paddingTop: '12px', borderTop: '1px solid #282828' }}>
                Log in with Discord via the top menu to verify commissioner status.
              </div>
            )}
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            {/* Header banner */}
            <div style={{
              background: 'rgba(245, 158, 11, 0.12)',
              border: '1px solid rgba(245, 158, 11, 0.35)',
              borderRadius: '8px',
              padding: '14px 18px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '12px',
              flexWrap: 'wrap'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <span style={{ fontSize: '20px' }}>👑</span>
                <div>
                  <div style={{ fontSize: '14px', fontWeight: 'bold', color: '#fbbf24' }}>
                    Commissioner Offseason Control Suite
                  </div>
                  <div style={{ fontSize: '12px', color: '#cbd5e1' }}>
                    Authorized for <strong>Dan</strong> and <strong>Adrian</strong>. Changes take effect across the entire league.
                  </div>
                </div>
              </div>
              <span style={{
                background: '#f59e0b',
                color: '#000',
                padding: '3px 8px',
                borderRadius: '4px',
                fontSize: '11px',
                fontWeight: '900',
                textTransform: 'uppercase',
                letterSpacing: '0.5px'
              }}>
                Full Admin Rights
              </span>
            </div>

            {/* Section 1: Global Settings (Deadline & Base Budget) */}
            <div style={{
              background: '#181818',
              borderRadius: '8px',
              border: '1px solid #2a2a2a',
              padding: '18px',
              display: 'flex',
              flexDirection: 'column',
              gap: '16px'
            }}>
              <div style={{ fontSize: '14px', fontWeight: 'bold', color: '#fff', borderBottom: '1px solid #282828', paddingBottom: '8px' }}>
                ⏱️ Offseason Schedule & Budget Baseline
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px' }}>
                <div>
                  <label style={{ display: 'block', fontSize: '12px', fontWeight: 'bold', color: '#aaa', marginBottom: '6px' }}>
                    Official Keeper Locking Deadline:
                  </label>
                  <input
                    type="datetime-local"
                    value={deadlineInput}
                    onChange={e => setDeadlineInput(e.target.value)}
                    style={{
                      width: '100%',
                      background: '#111',
                      border: '1px solid #444',
                      borderRadius: '6px',
                      padding: '8px 12px',
                      color: '#fff',
                      fontSize: '13px'
                    }}
                  />
                  <div style={{ fontSize: '11px', color: '#666', marginTop: '4px' }}>
                    After this timestamp, all non-commissioner keeper selections are locked.
                  </div>
                </div>

                <div>
                  <label style={{ display: 'block', fontSize: '12px', fontWeight: 'bold', color: '#aaa', marginBottom: '6px' }}>
                    League Baseline Draft Budget ($):
                  </label>
                  <input
                    type="number"
                    value={baselineBudgetInput}
                    onChange={e => setBaselineBudgetInput(e.target.value)}
                    style={{
                      width: '100%',
                      background: '#111',
                      border: '1px solid #444',
                      borderRadius: '6px',
                      padding: '8px 12px',
                      color: '#fff',
                      fontSize: '13px'
                    }}
                  />
                  <div style={{ fontSize: '11px', color: '#666', marginTop: '4px' }}>
                    Standard draft budget baseline allocated to all 9 managers (default: $100).
                  </div>
                </div>
              </div>

              <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: '8px' }}>
                <button
                  onClick={handleSaveLeagueSettings}
                  disabled={savingSettings}
                  style={{
                    background: '#f59e0b',
                    color: '#000',
                    border: 'none',
                    borderRadius: '6px',
                    padding: '8px 16px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: savingSettings ? 'not-allowed' : 'pointer',
                    opacity: savingSettings ? 0.6 : 1,
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px'
                  }}
                >
                  {savingSettings ? 'Saving...' : '💾 Save Global Settings'}
                </button>
              </div>
            </div>

            {/* Section 2: Owner Manual Adjustments (Punitive Penalties & Awards) */}
            <div style={{
              background: '#181818',
              borderRadius: '8px',
              border: '1px solid #2a2a2a',
              overflow: 'hidden'
            }}>
              <div style={{
                padding: '16px 18px',
                borderBottom: '1px solid #282828',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: '10px'
              }}>
                <div>
                  <div style={{ fontSize: '14px', fontWeight: 'bold', color: '#fff' }}>
                    ⚖️ Owner Punitive & Award Manual Budget Adjustments
                  </div>
                  <div style={{ fontSize: '11px', color: '#888', marginTop: '2px' }}>
                    Specify positive adjustments (awards/bonuses) or negative adjustments (fines/penalties) for each owner.
                  </div>
                </div>

                <button
                  onClick={handleSaveManualAdjustments}
                  disabled={savingAdjustments}
                  style={{
                    background: '#059669',
                    color: '#fff',
                    border: 'none',
                    borderRadius: '6px',
                    padding: '8px 18px',
                    fontSize: '12px',
                    fontWeight: 'bold',
                    cursor: savingAdjustments ? 'not-allowed' : 'pointer',
                    opacity: savingAdjustments ? 0.6 : 1,
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    boxShadow: '0 2px 4px rgba(0,0,0,0.3)'
                  }}
                >
                  {savingAdjustments ? 'Saving...' : '💾 Save All Budget Adjustments'}
                </button>
              </div>

              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'left' }}>
                  <thead>
                    <tr style={{ background: '#111', color: '#888', borderBottom: '1px solid #333' }}>
                      <th style={{ padding: '10px 14px' }}>Owner</th>
                      <th style={{ padding: '10px 12px', textAlign: 'center' }}>Base</th>
                      <th style={{ padding: '10px 12px', textAlign: 'center' }}>Keepers</th>
                      <th style={{ padding: '10px 12px', textAlign: 'center' }}>Comp Net</th>
                      <th style={{ padding: '10px 12px', minWidth: '130px' }}>Manual Adj ($)</th>
                      <th style={{ padding: '10px 12px', minWidth: '220px' }}>Adjustment Reason / Notes</th>
                      <th style={{ padding: '10px 14px', textAlign: 'right' }}>Calculated Final</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teamBudgets.map((b, idx) => {
                      const formVal = adjustmentsForm[b.owner] || { adjustment: b.manual_adjustment || 0, notes: b.adjustment_notes || '' };
                      const adjNum = parseFloat(formVal.adjustment) || 0;
                      const calculatedFinal = b.base_budget - (b.keeper_spend || 0) - (b.comp_pick_spend || 0) + (b.comp_pick_income || 0) + adjNum;

                      return (
                        <tr key={b.owner} style={{
                          borderBottom: '1px solid #222',
                          background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)'
                        }}>
                          <td style={{ padding: '10px 14px', fontWeight: 'bold', color: '#fff' }}>
                            {b.owner}
                            <span style={{ fontSize: '10px', color: '#666', marginLeft: '6px' }}>Team {b.team_id}</span>
                          </td>
                          <td style={{ padding: '10px 12px', textAlign: 'center', color: '#aaa' }}>
                            ${b.base_budget}
                          </td>
                          <td style={{ padding: '10px 12px', textAlign: 'center', color: '#ffb74d' }}>
                            ${b.keeper_spend || 0}
                          </td>
                          <td style={{ padding: '10px 12px', textAlign: 'center', color: '#bb86fc' }}>
                            ${((b.comp_pick_income || 0) - (b.comp_pick_spend || 0))}
                          </td>
                          <td style={{ padding: '10px 12px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                              <span style={{ color: adjNum > 0 ? '#4caf50' : adjNum < 0 ? '#f44336' : '#666', fontWeight: 'bold' }}>$</span>
                              <input
                                type="number"
                                value={formVal.adjustment}
                                onChange={e => {
                                  const val = e.target.value;
                                  setAdjustmentsForm(prev => ({
                                    ...prev,
                                    [b.owner]: {
                                      ...prev[b.owner],
                                      adjustment: val
                                    }
                                  }));
                                }}
                                placeholder="0"
                                style={{
                                  width: '80px',
                                  background: '#111',
                                  border: adjNum !== 0 ? (adjNum > 0 ? '1px solid #4caf50' : '1px solid #f44336') : '1px solid #333',
                                  borderRadius: '4px',
                                  padding: '4px 8px',
                                  color: adjNum > 0 ? '#4caf50' : adjNum < 0 ? '#f44336' : '#fff',
                                  fontSize: '12px',
                                  fontWeight: 'bold'
                                }}
                              />
                            </div>
                          </td>
                          <td style={{ padding: '10px 12px' }}>
                            <input
                              type="text"
                              value={formVal.notes}
                              onChange={e => {
                                const val = e.target.value;
                                setAdjustmentsForm(prev => ({
                                  ...prev,
                                  [b.owner]: {
                                    ...prev[b.owner],
                                    notes: val
                                  }
                                }));
                              }}
                              placeholder="e.g. Late fee fine (-$5) or Toilet bowl prize (+$5)"
                              style={{
                                width: '100%',
                                background: '#111',
                                border: '1px solid #333',
                                borderRadius: '4px',
                                padding: '4px 8px',
                                color: '#ddd',
                                fontSize: '11px'
                              }}
                            />
                          </td>
                          <td style={{ padding: '10px 14px', textAlign: 'right' }}>
                            <span style={{
                              fontWeight: 'bold',
                              fontSize: '13px',
                              color: calculatedFinal < 0 ? '#f44336' : '#4caf50',
                              padding: '2px 8px',
                              borderRadius: '4px',
                              background: calculatedFinal < 0 ? 'rgba(244, 67, 54, 0.15)' : 'rgba(76, 175, 80, 0.15)'
                            }}>
                              ${calculatedFinal}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )
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
