import { useState, useMemo, useEffect } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, calculateBatterValue, calculatePitcherValue } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';

// In-memory cache for full season MLB totals across re-renders
const mlbSeasonDataCache = new Map();

function cleanPlayerName(name) {
  if (!name) return '';
  return name
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

export default function OwnerDisparitiesView({ allStats, selectedSeason = 2026, onPlayerClick, onOwnerClick }) {
  // Mode switcher: 'rostered_vs_unrostered' (new) | 'manager_vs_manager' (existing)
  const [disparityMode, setDisparityMode] = useState('rostered_vs_unrostered');

  // Common filters
  const [tab, setTab] = useState('batters'); // 'batters' | 'pitchers'
  const [minVolume, setMinVolume] = useState(15); // 15 PA for batters, 5 IP for pitchers
  const [searchQuery, setSearchQuery] = useState('');

  // Manager vs Manager specific filters
  const [selectedOwnerId, setSelectedOwnerId] = useState('all');

  // Rostered vs Unrostered specific filters
  const [rosteredSortBy, setRosteredSortBy] = useState('disparity_desc'); 
  // 'disparity_desc' | 'disparity_asc' | 'unrostered_vol' | 'unrostered_stat' | 'rostered_vol'
  const [requireBothStates, setRequireBothStates] = useState(true);

  // Completed games cutoff: For active 2026 season, synchronize through yesterday's completed games
  // to avoid attributing in-progress or same-day unfinalized games to unrostered performance.
  const cutoffDate = useMemo(() => {
    if (selectedSeason < 2026) return null;
    const now = new Date();
    const y = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
    return `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, '0')}-${String(y.getDate()).padStart(2, '0')}`;
  }, [selectedSeason]);

  // MLB Season Stats state
  const [mlbSplits, setMlbSplits] = useState({ batters: [], pitchers: [] });
  const [loadingMlb, setLoadingMlb] = useState(false);
  const [mlbError, setMlbError] = useState(null);

  // Fetch MLB season data
  useEffect(() => {
    let isCancelled = false;

    async function loadMlbSeason() {
      const cacheKey = `mlb_season_${selectedSeason}_${cutoffDate || 'full'}`;
      if (mlbSeasonDataCache.has(cacheKey)) {
        setMlbSplits(mlbSeasonDataCache.get(cacheKey));
        return;
      }

      setLoadingMlb(true);
      setMlbError(null);

      try {
        const hittingUrl = cutoffDate
          ? `https://statsapi.mlb.com/api/v1/stats?stats=byDateRange&season=${selectedSeason}&startDate=2026-03-25&endDate=${cutoffDate}&group=hitting&limit=1500&playerPool=all`
          : `https://statsapi.mlb.com/api/v1/stats?stats=season&season=${selectedSeason}&group=hitting&limit=1500&playerPool=all`;
        const pitchingUrl = cutoffDate
          ? `https://statsapi.mlb.com/api/v1/stats?stats=byDateRange&season=${selectedSeason}&startDate=2026-03-25&endDate=${cutoffDate}&group=pitching&limit=1500&playerPool=all`
          : `https://statsapi.mlb.com/api/v1/stats?stats=season&season=${selectedSeason}&group=pitching&limit=1500&playerPool=all`;

        const [batRes, pitRes] = await Promise.all([
          fetch(hittingUrl),
          fetch(pitchingUrl)
        ]);

        if (!batRes.ok || !pitRes.ok) {
          throw new Error('Failed to fetch MLB season stats from MLB Stats API');
        }

        const [batJson, pitJson] = await Promise.all([batRes.json(), pitRes.json()]);
        const data = {
          batters: batJson.stats?.[0]?.splits || [],
          pitchers: pitJson.stats?.[0]?.splits || []
        };

        mlbSeasonDataCache.set(cacheKey, data);
        if (!isCancelled) {
          setMlbSplits(data);
          setLoadingMlb(false);
        }
      } catch (err) {
        console.error('Error loading MLB season splits:', err);
        if (!isCancelled) {
          setMlbError(err.message || 'Error loading MLB data');
          setLoadingMlb(false);
        }
      }
    }

    loadMlbSeason();
    return () => { isCancelled = true; };
  }, [selectedSeason, cutoffDate]);

  const handleTabChange = (newTab) => {
    setTab(newTab);
    setMinVolume(newTab === 'batters' ? 15 : 5);
  };

  // -------------------------------------------------------------
  // 1. Group records by player & determine positions
  // -------------------------------------------------------------
  const playersData = useMemo(() => {
    const map = {};

    allStats.forEach(r => {
      // Ignore Ghost Team (99)
      if (r.team_id === 99 || r.team_id === '99') return;

      // In Rostered vs. Unrostered mode, exclude in-progress games from today
      if (disparityMode === 'rostered_vs_unrostered' && cutoffDate) {
        const gameDate = getDateFromPeriodId(r.scoring_period_id, selectedSeason);
        if (gameDate > cutoffDate) return;
      }

      const mapKey = r.player_id || r.full_name;
      if (!map[mapKey]) {
        map[mapKey] = {
          id: r.player_id,
          name: r.full_name,
          teamRecords: {},
          allRecords: [],
          isPitcher: false,
        };
      }

      const p = map[mapKey];
      p.allRecords.push(r);

      if (!p.teamRecords[r.team_id]) {
        p.teamRecords[r.team_id] = [];
      }
      p.teamRecords[r.team_id].push(r);

      const PITCHER_SLOTS = new Set([13, 14, 15]);
      if (PITCHER_SLOTS.has(r.lineup_slot_id)) {
        p.isPitcher = true;
      }
      const s = r.stats || {};
      if (parseFloat(s.IP) > 0 || parseFloat(s['34']) > 0 || parseFloat(s.K) > 0 || parseFloat(s['48']) > 0) {
        p.isPitcher = true;
      }
    });

    return Object.values(map);
  }, [allStats, cutoffDate, disparityMode, selectedSeason]);

  // -------------------------------------------------------------
  // 2. Rostered vs Unrostered Disparity Engine
  // -------------------------------------------------------------
  const mlbLookup = useMemo(() => {
    const batLookup = new Map();
    const pitLookup = new Map();

    (mlbSplits.batters || []).forEach(s => {
      if (s.player?.fullName) {
        batLookup.set(cleanPlayerName(s.player.fullName), s.stat);
      }
    });

    (mlbSplits.pitchers || []).forEach(s => {
      if (s.player?.fullName) {
        pitLookup.set(cleanPlayerName(s.player.fullName), s.stat);
      }
    });

    return { batLookup, pitLookup };
  }, [mlbSplits]);

  const rosteredDisparities = useMemo(() => {
    const isBattersTab = tab === 'batters';
    const results = [];

    playersData.forEach(p => {
      if (isBattersTab && p.isPitcher) return;
      if (!isBattersTab && !p.isPitcher) return;

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        if (!p.name.toLowerCase().includes(q)) return;
      }

      const cleanName = cleanPlayerName(p.name);
      const mlbStat = isBattersTab 
        ? mlbLookup.batLookup.get(cleanName)
        : mlbLookup.pitLookup.get(cleanName);

      // Aggregate total rostered stats (both starter and bench)
      const rosStats = aggregateStats(p.allRecords, { includeAll: true });
      const rosGames = p.allRecords.length;

      // Determine teams that owned this player
      const teamStints = Object.entries(p.teamRecords).map(([tid, recs]) => ({
        teamId: parseInt(tid),
        team: TEAMS[tid] || { name: `Team ${tid}`, owner: '' },
        games: recs.length,
      })).sort((a, b) => b.games - a.games);

      if (isBattersTab) {
        const mlbPa = mlbStat?.plateAppearances || 0;
        const rosPa = parseFloat(rosStats.PA) || 0;
        const rosHr = parseFloat(rosStats.HR) || 0;
        const rosR = parseFloat(rosStats.R) || 0;
        const rosRbi = parseFloat(rosStats.RBI) || 0;
        const rosSb = parseFloat(rosStats.SB) || 0;
        const rosObp = parseFloat(rosStats.OBP) || 0;
        const rosValue = calculateBatterValue(rosStats);

        const unrosPa = Math.max(0, mlbPa - rosPa);
        const unrosHr = Math.max(0, (mlbStat?.homeRuns || 0) - rosHr);
        const unrosR = Math.max(0, (mlbStat?.runs || 0) - rosR);
        const unrosRbi = Math.max(0, (mlbStat?.rbi || 0) - rosRbi);
        const unrosSb = Math.max(0, (mlbStat?.stolenBases || 0) - rosSb);
        const unrosH = Math.max(0, (mlbStat?.hits || 0) - (parseFloat(rosStats.H) || 0));
        const unrosBb = Math.max(0, (mlbStat?.baseOnBalls || 0) - (parseFloat(rosStats.BB) || 0));
        const unrosHbp = Math.max(0, (mlbStat?.hitByPitch || 0) - (parseFloat(rosStats.HBP) || 0));
        const unrosObp = unrosPa > 0 ? Math.min(1.0, (unrosH + unrosBb + unrosHbp) / unrosPa) : 0;
        const unrosGames = Math.max(0, (mlbStat?.gamesPlayed || 0) - rosGames);

        const unrosStats = {
          PA: unrosPa,
          R: unrosR,
          HR: unrosHr,
          RBI: unrosRbi,
          SB: unrosSb,
          OBP: unrosObp,
        };

        const unrosValue = unrosPa > 0 ? calculateBatterValue(unrosStats) : 0;
        const disparity = rosValue - unrosValue;

        // Qualification checks
        if (rosPa < minVolume) return;
        if (requireBothStates && unrosPa < minVolume) return;

        results.push({
          id: p.id,
          name: p.name,
          teamStints,
          isPitcher: false,
          hasMlbData: !!mlbStat,
          rostered: {
            games: rosGames,
            volume: rosPa,
            volUnit: 'PA',
            value: rosValue,
            stats: rosStats,
            summaryStr: `${rosHr} HR, ${rosRbi} RBI, ${rosR} R, ${rosSb} SB, ${rosObp.toFixed(3).replace(/^0/, '')} OBP`,
          },
          unrostered: {
            games: unrosGames,
            volume: unrosPa,
            volUnit: 'PA',
            value: unrosValue,
            stats: unrosStats,
            summaryStr: `${unrosHr} HR, ${unrosRbi} RBI, ${unrosR} R, ${unrosSb} SB, ${unrosObp > 0 ? unrosObp.toFixed(3).replace(/^0/, '') : '.000'} OBP`,
          },
          disparity,
          keyCountingDiff: {
            statName: 'HR',
            rosVal: rosHr,
            unrosVal: unrosHr,
            diff: rosHr - unrosHr,
          },
        });
      } else {
        // Pitchers
        const mlbOuts = mlbStat?.outs || mlbStat?.outsPitched;
        const mlbIp = mlbOuts ? mlbOuts / 3.0 : (parseFloat(mlbStat?.inningsPitched) || 0);
        const rosIp = parseFloat(rosStats.IP) || 0;
        const rosEr = parseFloat(rosStats.ER) || 0;
        const rosK = parseFloat(rosStats.K) || 0;
        const rosSvHd = parseFloat(rosStats['SV+HDs']) || 0;
        const rosQs = parseFloat(rosStats.QS) || 0;
        const rosValue = calculatePitcherValue(rosStats);
        const rosEra = parseFloat(rosStats.ERA) || 0;
        const rosWhip = parseFloat(rosStats.WHIP) || 0;

        const unrosIp = Math.max(0, mlbIp - rosIp);
        const unrosEr = Math.max(0, (mlbStat?.earnedRuns || 0) - rosEr);
        const unrosK = Math.max(0, (mlbStat?.strikeOuts || 0) - rosK);
        const mlbSv = mlbStat?.saves || 0;
        const mlbHd = mlbStat?.holds || 0;
        const unrosSvHd = Math.max(0, (mlbSv + mlbHd) - rosSvHd);
        const unrosH = Math.max(0, (mlbStat?.hits || 0) - (parseFloat(rosStats.H_Allowed) || 0));
        const unrosBb = Math.max(0, (mlbStat?.baseOnBalls || 0) - (parseFloat(rosStats.BB_Allowed) || 0));
        const unrosEra = unrosIp > 0 ? (unrosEr * 9) / unrosIp : 0;
        const unrosWhip = unrosIp > 0 ? (unrosBb + unrosH) / unrosIp : 0;
        const unrosGames = Math.max(0, (mlbStat?.gamesPlayed || 0) - rosGames);

        const unrosStats = {
          IP: unrosIp,
          ER: unrosEr,
          K: unrosK,
          'SV+HDs': unrosSvHd,
          QS: 0,
          ERA: unrosEra,
          WHIP: unrosWhip,
        };

        const unrosValue = unrosIp > 0 ? calculatePitcherValue(unrosStats) : 0;
        const disparity = rosValue - unrosValue;

        // Qualification checks
        if (rosIp < minVolume) return;
        if (requireBothStates && unrosIp < minVolume) return;

        results.push({
          id: p.id,
          name: p.name,
          teamStints,
          isPitcher: true,
          hasMlbData: !!mlbStat,
          rostered: {
            games: rosGames,
            volume: rosIp,
            volUnit: 'IP',
            value: rosValue,
            stats: rosStats,
            summaryStr: `${rosIp.toFixed(1)} IP, ${rosK} K, ${rosEra.toFixed(2)} ERA, ${rosWhip.toFixed(2)} WHIP, ${rosQs} QS, ${rosSvHd} SV+HD`,
          },
          unrostered: {
            games: unrosGames,
            volume: unrosIp,
            volUnit: 'IP',
            value: unrosValue,
            stats: unrosStats,
            summaryStr: `${unrosIp.toFixed(1)} IP, ${unrosK} K, ${unrosEra > 0 ? unrosEra.toFixed(2) : '0.00'} ERA, ${unrosWhip > 0 ? unrosWhip.toFixed(2) : '0.00'} WHIP, ${unrosSvHd} SV+HD`,
          },
          disparity,
          keyCountingDiff: {
            statName: 'K',
            rosVal: rosK,
            unrosVal: unrosK,
            diff: rosK - unrosK,
          },
        });
      }
    });

    // Sort according to selection
    return results.sort((a, b) => {
      if (rosteredSortBy === 'disparity_desc') return b.disparity - a.disparity;
      if (rosteredSortBy === 'disparity_asc') return a.disparity - b.disparity;
      if (rosteredSortBy === 'unrostered_vol') return b.unrostered.volume - a.unrostered.volume;
      if (rosteredSortBy === 'unrostered_stat') {
        const statKey = isBattersTab ? 'HR' : 'K';
        return (b.unrostered.stats[statKey] || 0) - (a.unrostered.stats[statKey] || 0);
      }
      if (rosteredSortBy === 'rostered_vol') return b.rostered.volume - a.rostered.volume;
      return b.disparity - a.disparity;
    });
  }, [playersData, mlbLookup, tab, searchQuery, minVolume, requireBothStates, rosteredSortBy]);

  // -------------------------------------------------------------
  // 3. KPI Highlights for Rostered vs Unrostered
  // -------------------------------------------------------------
  const rosteredKpis = useMemo(() => {
    if (rosteredDisparities.length === 0) return null;
    const isBatters = tab === 'batters';

    // 1. Top Rostered Phenom (highest positive disparity)
    const topPhenom = [...rosteredDisparities].sort((a, b) => b.disparity - a.disparity)[0];

    // 2. Waiver Wire Regret (highest negative disparity / better on waivers)
    const topRegret = [...rosteredDisparities].sort((a, b) => a.disparity - b.disparity)[0];

    // 3. Most Unrostered Counting Stat (HR for batters, K for pitchers)
    const primaryCat = isBatters ? 'HR' : 'K';
    const topCounting = [...rosteredDisparities].sort((a, b) => (b.unrostered.stats[primaryCat] || 0) - (a.unrostered.stats[primaryCat] || 0))[0];

    // 4. Most Unrostered Secondary Stat (SB for batters, SV+HD for pitchers)
    const secondaryCat = isBatters ? 'SB' : 'SV+HDs';
    const topSecondary = [...rosteredDisparities].sort((a, b) => (b.unrostered.stats[secondaryCat] || 0) - (a.unrostered.stats[secondaryCat] || 0))[0];

    return {
      topPhenom,
      topRegret,
      topCounting: {
        player: topCounting,
        cat: primaryCat,
        val: topCounting?.unrostered.stats[primaryCat] || 0,
      },
      topSecondary: {
        player: topSecondary,
        cat: isBatters ? 'SB' : 'SV+HD',
        val: topSecondary?.unrostered.stats[secondaryCat] || 0,
      },
    };
  }, [rosteredDisparities, tab]);

  // -------------------------------------------------------------
  // 4. Manager vs Manager Disparities (Original View)
  // -------------------------------------------------------------
  const managerDisparities = useMemo(() => {
    const isBattersTab = tab === 'batters';
    let results = [];

    playersData.forEach(p => {
      if (isBattersTab && p.isPitcher) return;
      if (!isBattersTab && !p.isPitcher) return;

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        if (!p.name.toLowerCase().includes(q)) return;
      }

      const teamStints = [];
      Object.keys(p.teamRecords).forEach(teamId => {
        const records = p.teamRecords[teamId];
        const stats = aggregateStats(records);
        const volume = isBattersTab ? parseFloat(stats.PA) || 0 : parseFloat(stats.IP) || 0;

        if (volume >= minVolume) {
          const value = isBattersTab ? calculateBatterValue(stats) : calculatePitcherValue(stats);
          teamStints.push({
            teamId: parseInt(teamId),
            team: TEAMS[teamId] || { name: 'Unknown', owner: '' },
            volume,
            value,
            stats,
          });
        }
      });

      if (teamStints.length >= 2) {
        teamStints.sort((a, b) => b.value - a.value);
        const bestStint = teamStints[0];
        const worstStint = teamStints[teamStints.length - 1];

        results.push({
          id: p.id,
          name: p.name,
          bestStint,
          worstStint,
          disparity: bestStint.value - worstStint.value,
        });
      }
    });

    if (selectedOwnerId !== 'all') {
      const oid = parseInt(selectedOwnerId);
      results = results.filter(r => r.bestStint.teamId === oid || r.worstStint.teamId === oid);
    }

    return results.sort((a, b) => b.disparity - a.disparity);
  }, [playersData, tab, minVolume, selectedOwnerId, searchQuery]);

  return (
    <div className="space-y-6">
      {/* Header & Mode Switcher */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-gray-200 pb-5">
        <div>
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-black text-gray-900 tracking-tight">Performance Disparities</h2>
            <span className="px-2.5 py-0.5 text-xs font-bold bg-blue-100 text-blue-800 rounded-full border border-blue-200">
              {selectedSeason} Season
            </span>
            {cutoffDate && disparityMode === 'rostered_vs_unrostered' && (
              <span 
                className="px-2.5 py-0.5 text-xs font-semibold bg-emerald-50 text-emerald-800 rounded-full border border-emerald-200"
                title="Synchronized with completed MLB games through yesterday to prevent in-progress games from skewing unrostered stats"
              >
                Synced through {cutoffDate}
              </span>
            )}
          </div>
          <p className="text-sm text-gray-500 mt-1">
            {disparityMode === 'rostered_vs_unrostered' 
              ? 'Analyze which players performed best on fantasy rosters vs. while unrostered on the waiver wire.'
              : 'Analyze players who were rostered by multiple fantasy teams and had large performance swings between stints.'}
          </p>
        </div>

        {/* Top-Level Mode Selector */}
        <div className="flex items-center bg-gray-100 p-1.5 rounded-xl border border-gray-200 shadow-xs self-start md:self-auto">
          <button
            onClick={() => setDisparityMode('rostered_vs_unrostered')}
            className={`flex items-center gap-2 px-4 py-2 text-xs md:text-sm font-bold rounded-lg transition-all ${
              disparityMode === 'rostered_vs_unrostered'
                ? 'bg-white text-blue-700 shadow-sm border border-gray-200/60'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <span>⚖️</span>
            <span>Rostered vs. Unrostered</span>
          </button>
          <button
            onClick={() => setDisparityMode('manager_vs_manager')}
            className={`flex items-center gap-2 px-4 py-2 text-xs md:text-sm font-bold rounded-lg transition-all ${
              disparityMode === 'manager_vs_manager'
                ? 'bg-white text-blue-700 shadow-sm border border-gray-200/60'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <span>🔄</span>
            <span>Manager vs. Manager</span>
          </button>
        </div>
      </div>

      {/* KPI Cards (Rostered vs Unrostered Mode) */}
      {disparityMode === 'rostered_vs_unrostered' && rosteredKpis && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* Card 1: Top Rostered Phenom */}
          <div className="bg-gradient-to-br from-emerald-50 to-emerald-100/50 border border-emerald-200 rounded-xl p-4 shadow-xs">
            <div className="flex items-center justify-between">
              <span className="text-xs font-bold uppercase tracking-wider text-emerald-800">Top Rostered Phenom</span>
              <span className="text-lg">🚀</span>
            </div>
            <div className="mt-2">
              <button
                onClick={() => rosteredKpis.topPhenom && onPlayerClick(rosteredKpis.topPhenom.id, rosteredKpis.topPhenom.name)}
                className="font-extrabold text-base text-gray-900 hover:text-emerald-700 transition-colors text-left truncate block max-w-full"
              >
                {rosteredKpis.topPhenom?.name || 'N/A'}
              </button>
              <div className="flex items-baseline gap-2 mt-1">
                <span className="font-mono text-xl font-black text-emerald-700">
                  +{rosteredKpis.topPhenom?.disparity.toFixed(2)}
                </span>
                <span className="text-xs text-emerald-900 font-medium">Disparity</span>
              </div>
              <div className="text-[11px] text-emerald-800/80 mt-1 truncate">
                Rostered: {rosteredKpis.topPhenom?.rostered.value.toFixed(2)} | Wire: {rosteredKpis.topPhenom?.unrostered.value.toFixed(2)}
              </div>
            </div>
          </div>

          {/* Card 2: Top Wire Regret */}
          <div className="bg-gradient-to-br from-rose-50 to-rose-100/50 border border-rose-200 rounded-xl p-4 shadow-xs">
            <div className="flex items-center justify-between">
              <span className="text-xs font-bold uppercase tracking-wider text-rose-800">Waiver Wire Regret</span>
              <span className="text-lg">🤦</span>
            </div>
            <div className="mt-2">
              <button
                onClick={() => rosteredKpis.topRegret && onPlayerClick(rosteredKpis.topRegret.id, rosteredKpis.topRegret.name)}
                className="font-extrabold text-base text-gray-900 hover:text-rose-700 transition-colors text-left truncate block max-w-full"
              >
                {rosteredKpis.topRegret?.name || 'N/A'}
              </button>
              <div className="flex items-baseline gap-2 mt-1">
                <span className="font-mono text-xl font-black text-rose-700">
                  {rosteredKpis.topRegret?.disparity.toFixed(2)}
                </span>
                <span className="text-xs text-rose-900 font-medium">Better on Wire</span>
              </div>
              <div className="text-[11px] text-rose-800/80 mt-1 truncate">
                Rostered: {rosteredKpis.topRegret?.rostered.value.toFixed(2)} | Wire: {rosteredKpis.topRegret?.unrostered.value.toFixed(2)}
              </div>
            </div>
          </div>

          {/* Card 3: Top Unrostered Primary Counting Stat */}
          <div className="bg-gradient-to-br from-amber-50 to-amber-100/50 border border-amber-200 rounded-xl p-4 shadow-xs">
            <div className="flex items-center justify-between">
              <span className="text-xs font-bold uppercase tracking-wider text-amber-800">
                Most Unrostered {rosteredKpis.topCounting.cat}s
              </span>
              <span className="text-lg">{tab === 'batters' ? '💣' : '🎯'}</span>
            </div>
            <div className="mt-2">
              <button
                onClick={() => rosteredKpis.topCounting.player && onPlayerClick(rosteredKpis.topCounting.player.id, rosteredKpis.topCounting.player.name)}
                className="font-extrabold text-base text-gray-900 hover:text-amber-700 transition-colors text-left truncate block max-w-full"
              >
                {rosteredKpis.topCounting.player?.name || 'N/A'}
              </button>
              <div className="flex items-baseline gap-2 mt-1">
                <span className="font-mono text-xl font-black text-amber-800">
                  {rosteredKpis.topCounting.val} {rosteredKpis.topCounting.cat}
                </span>
                <span className="text-xs text-amber-900 font-medium">left on wire</span>
              </div>
              <div className="text-[11px] text-amber-800/80 mt-1 truncate">
                vs {rosteredKpis.topCounting.player?.rostered.stats[rosteredKpis.topCounting.cat] || 0} {rosteredKpis.topCounting.cat} while rostered
              </div>
            </div>
          </div>

          {/* Card 4: Top Unrostered Secondary Stat */}
          <div className="bg-gradient-to-br from-indigo-50 to-indigo-100/50 border border-indigo-200 rounded-xl p-4 shadow-xs">
            <div className="flex items-center justify-between">
              <span className="text-xs font-bold uppercase tracking-wider text-indigo-800">
                Most Unrostered {rosteredKpis.topSecondary.cat}
              </span>
              <span className="text-lg">{tab === 'batters' ? '⚡' : '🛡️'}</span>
            </div>
            <div className="mt-2">
              <button
                onClick={() => rosteredKpis.topSecondary.player && onPlayerClick(rosteredKpis.topSecondary.player.id, rosteredKpis.topSecondary.player.name)}
                className="font-extrabold text-base text-gray-900 hover:text-indigo-700 transition-colors text-left truncate block max-w-full"
              >
                {rosteredKpis.topSecondary.player?.name || 'N/A'}
              </button>
              <div className="flex items-baseline gap-2 mt-1">
                <span className="font-mono text-xl font-black text-indigo-800">
                  {rosteredKpis.topSecondary.val} {rosteredKpis.topSecondary.cat}
                </span>
                <span className="text-xs text-indigo-900 font-medium">left on wire</span>
              </div>
              <div className="text-[11px] text-indigo-800/80 mt-1 truncate">
                vs {rosteredKpis.topSecondary.player?.rostered.stats[tab === 'batters' ? 'SB' : 'SV+HDs'] || 0} while rostered
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Filter Bar */}
      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-xs flex flex-wrap items-center justify-between gap-4">
        {/* Left: Position Switcher & Search */}
        <div className="flex items-center gap-3 flex-wrap">
          {/* Batters / Pitchers */}
          <div className="flex bg-gray-100 p-1 rounded-lg border border-gray-200">
            <button
              onClick={() => handleTabChange('batters')}
              className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${
                tab === 'batters' ? 'bg-white text-blue-700 shadow-xs' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Batters
            </button>
            <button
              onClick={() => handleTabChange('pitchers')}
              className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${
                tab === 'pitchers' ? 'bg-white text-blue-700 shadow-xs' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Pitchers
            </button>
          </div>

          {/* Search Input */}
          <div className="relative">
            <input
              type="text"
              placeholder="Search player..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-8 pr-3 py-1.5 text-xs rounded-lg border border-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-400 w-44"
            />
            <span className="absolute left-2.5 top-1.5 text-gray-400 text-xs">🔍</span>
          </div>
        </div>

        {/* Right: Mode-Specific Filters */}
        <div className="flex items-center gap-3 flex-wrap">
          {disparityMode === 'rostered_vs_unrostered' ? (
            <>
              {/* Sort By Dropdown */}
              <div className="flex items-center gap-1.5">
                <label className="text-xs text-gray-600 font-bold">Sort:</label>
                <select
                  value={rosteredSortBy}
                  onChange={(e) => setRosteredSortBy(e.target.value)}
                  className="text-xs border border-gray-300 rounded-lg px-2.5 py-1.5 bg-white text-gray-800 font-semibold focus:outline-none focus:ring-2 focus:ring-blue-400"
                >
                  <option value="disparity_desc">🚀 Highest Disparity (Better Rostered)</option>
                  <option value="disparity_asc">🤦 Highest Regret (Better on Wire)</option>
                  <option value="unrostered_vol">💎 Most Unrostered {tab === 'batters' ? 'PA' : 'IP'}</option>
                  <option value="unrostered_stat">💣 Most Unrostered {tab === 'batters' ? 'HRs' : 'Ks'}</option>
                  <option value="rostered_vol">🛡️ Most Rostered {tab === 'batters' ? 'PA' : 'IP'}</option>
                </select>
              </div>

              {/* Requirement Checkbox */}
              <label className="flex items-center gap-1.5 text-xs text-gray-700 cursor-pointer select-none font-medium">
                <input
                  type="checkbox"
                  checked={requireBothStates}
                  onChange={(e) => setRequireBothStates(e.target.checked)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 w-3.5 h-3.5"
                />
                <span>Both Rostered & Unrostered</span>
              </label>

              {/* Min Volume */}
              <div className="flex items-center gap-1.5">
                <label className="text-xs text-gray-600 font-bold">
                  Min {tab === 'batters' ? 'PA' : 'IP'}:
                </label>
                <input
                  type="number"
                  min="1"
                  value={minVolume}
                  onChange={(e) => setMinVolume(Math.max(1, Number(e.target.value)))}
                  className="w-16 text-xs border border-gray-300 rounded-lg px-2 py-1 font-mono text-center focus:outline-none focus:ring-2 focus:ring-blue-400"
                />
              </div>
            </>
          ) : (
            <>
              {/* Involved Owner Filter */}
              <div className="flex items-center gap-1.5">
                <label className="text-xs text-gray-600 font-bold">Involved Owner:</label>
                <select
                  value={selectedOwnerId}
                  onChange={(e) => setSelectedOwnerId(e.target.value)}
                  className="text-xs border border-gray-300 rounded-lg px-2.5 py-1.5 bg-white text-gray-800 font-semibold focus:outline-none focus:ring-2 focus:ring-blue-400 min-w-[130px]"
                >
                  <option value="all">All Owners</option>
                  {Object.values(TEAMS).filter(t => t.id !== 99).map(t => (
                    <option key={t.id} value={t.id}>{t.name}</option>
                  ))}
                </select>
              </div>

              {/* Min Volume */}
              <div className="flex items-center gap-1.5">
                <label className="text-xs text-gray-600 font-bold">
                  Min {tab === 'batters' ? 'PA' : 'IP'} / Owner:
                </label>
                <input
                  type="number"
                  min="1"
                  value={minVolume}
                  onChange={(e) => setMinVolume(Math.max(1, Number(e.target.value)))}
                  className="w-16 text-xs border border-gray-300 rounded-lg px-2 py-1 font-mono text-center focus:outline-none focus:ring-2 focus:ring-blue-400"
                />
              </div>
            </>
          )}
        </div>
      </div>

      {/* Loading & Error Indicators */}
      {loadingMlb && disparityMode === 'rostered_vs_unrostered' && (
        <div className="bg-blue-50/80 border border-blue-200 text-blue-800 rounded-xl p-4 flex items-center justify-between text-xs animate-pulse">
          <div className="flex items-center gap-2">
            <span className="inline-block w-3 h-3 rounded-full bg-blue-600 animate-ping"></span>
            <span className="font-semibold">Loading full-season MLB statistics from MLB Stats API to calculate unrostered production...</span>
          </div>
          <span className="font-mono text-[11px] text-blue-600">Syncing MLB season data</span>
        </div>
      )}

      {mlbError && disparityMode === 'rostered_vs_unrostered' && (
        <div className="bg-amber-50 border border-amber-200 text-amber-800 rounded-xl p-3 text-xs flex items-center gap-2">
          <span>⚠️</span>
          <span>Notice: Could not sync some MLB totals ({mlbError}). Showing available rostered numbers.</span>
        </div>
      )}

      {/* View 1: Rostered vs. Unrostered Table */}
      {disparityMode === 'rostered_vs_unrostered' && (
        rosteredDisparities.length === 0 ? (
          <div className="text-center py-16 bg-white rounded-xl shadow-xs border border-gray-200 text-gray-400">
            <div className="text-3xl mb-2">⚖️</div>
            <p className="font-semibold text-gray-600">No players found matching current filters.</p>
            <p className="text-xs text-gray-400 mt-1">Try lowering the minimum volume requirement or unchecking "Both Rostered & Unrostered".</p>
          </div>
        ) : (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse text-xs">
                <thead>
                  <tr className="bg-gray-100 border-b border-gray-200 text-[11px] uppercase tracking-wider text-gray-600">
                    <th className="p-3.5 font-black">Player & Fantasy Roster</th>
                    <th className="p-3.5 font-black bg-blue-50/80 text-blue-900 border-l border-blue-200">
                      Rostered Performance
                    </th>
                    <th className="p-3.5 font-black bg-slate-100 text-slate-800 border-l border-gray-300">
                      Unrostered / Free Agent
                    </th>
                    <th className="p-3.5 font-black text-center bg-gray-100 border-l border-gray-200">
                      Performance Disparity
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {rosteredDisparities.map((row) => {
                    const isPositive = row.disparity >= 0;
                    return (
                      <tr key={row.id || row.name} className="hover:bg-blue-50/20 transition-colors">
                        {/* Player Info */}
                        <td className="p-3.5 align-middle max-w-[220px]">
                          <div className="flex flex-col">
                            <button
                              onClick={() => onPlayerClick(row.id, row.name)}
                              className="font-black text-sm text-blue-600 hover:text-blue-800 text-left transition-colors truncate"
                            >
                              {row.name}
                            </button>
                            <div className="flex items-center gap-1.5 mt-1.5 flex-wrap">
                              {row.teamStints.map(stint => (
                                <span
                                  key={stint.teamId}
                                  onClick={() => onOwnerClick(stint.team)}
                                  className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-gray-100 hover:bg-gray-200 text-gray-700 text-[10px] font-semibold cursor-pointer transition-colors"
                                  title={`${stint.team.name} (${stint.games} games)`}
                                >
                                  <TeamAvatar team={stint.team} size="xs" />
                                  <span className="truncate max-w-[80px]">{stint.team.name}</span>
                                  <span className="text-gray-400 text-[9px]">({stint.games}G)</span>
                                </span>
                              ))}
                            </div>
                          </div>
                        </td>

                        {/* Rostered Stint */}
                        <td className="p-3.5 align-middle border-l border-blue-100 bg-blue-50/30">
                          <div className="flex items-center justify-between gap-3">
                            <div className="space-y-0.5">
                              <div className="font-semibold text-gray-900 text-xs">
                                {row.rostered.summaryStr}
                              </div>
                              <div className="text-[10px] text-blue-800 font-bold flex items-center gap-1.5">
                                <span className="bg-blue-100 text-blue-900 px-1.5 py-0.2 rounded font-mono">
                                  {row.rostered.volume.toFixed(1)} {row.rostered.volUnit}
                                </span>
                                <span>across {row.rostered.games} games</span>
                              </div>
                            </div>
                            <div className="text-right">
                              <div className="font-mono font-black text-sm text-blue-700">
                                {row.rostered.value.toFixed(2)}
                              </div>
                              <div className="text-[9px] uppercase font-bold text-gray-400 tracking-wider">
                                Value Rate
                              </div>
                            </div>
                          </div>
                        </td>

                        {/* Unrostered Stint */}
                        <td className="p-3.5 align-middle border-l border-gray-200 bg-slate-50/50">
                          <div className="flex items-center justify-between gap-3">
                            <div className="space-y-0.5">
                              <div className="font-semibold text-gray-800 text-xs">
                                {row.unrostered.volume > 0 ? row.unrostered.summaryStr : 'No MLB activity while unrostered'}
                              </div>
                              <div className="text-[10px] text-gray-500 font-medium flex items-center gap-1.5">
                                {row.unrostered.volume > 0 ? (
                                  <>
                                    <span className="bg-slate-200 text-slate-800 px-1.5 py-0.2 rounded font-mono font-bold">
                                      {row.unrostered.volume.toFixed(1)} {row.unrostered.volUnit}
                                    </span>
                                    <span>across {row.unrostered.games} games</span>
                                  </>
                                ) : (
                                  <span className="italic text-gray-400">100% of season spent rostered</span>
                                )}
                              </div>
                            </div>
                            <div className="text-right">
                              <div className="font-mono font-black text-sm text-gray-700">
                                {row.unrostered.volume > 0 ? row.unrostered.value.toFixed(2) : '-'}
                              </div>
                              <div className="text-[9px] uppercase font-bold text-gray-400 tracking-wider">
                                Value Rate
                              </div>
                            </div>
                          </div>
                        </td>

                        {/* Disparity Badge */}
                        <td className="p-3.5 align-middle text-center border-l border-gray-200">
                          <div className="flex flex-col items-center justify-center gap-1">
                            <div className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-full font-mono text-xs font-black shadow-2xs ${
                              isPositive 
                                ? 'bg-emerald-100 text-emerald-800 border border-emerald-300' 
                                : 'bg-rose-100 text-rose-800 border border-rose-300'
                            }`}>
                              <span>{isPositive ? '▲' : '▼'}</span>
                              <span>{isPositive ? `+${row.disparity.toFixed(2)}` : row.disparity.toFixed(2)}</span>
                            </div>
                            <span className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
                              {isPositive ? 'Better Rostered' : 'Better on Wire'}
                            </span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )
      )}

      {/* View 2: Manager vs. Manager Table (Original) */}
      {disparityMode === 'manager_vs_manager' && (
        managerDisparities.length === 0 ? (
          <div className="text-center py-16 bg-white rounded-xl shadow-xs border border-gray-200 text-gray-400">
            <div className="text-3xl mb-2">🔄</div>
            <p className="font-semibold text-gray-600">No players found with multiple qualified stints.</p>
            <p className="text-xs text-gray-400 mt-1">Try lowering the minimum volume requirement.</p>
          </div>
        ) : (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <table className="w-full text-left border-collapse text-xs">
              <thead>
                <tr className="bg-gray-100 border-b border-gray-200 text-[11px] uppercase tracking-wider text-gray-500">
                  <th className="p-4 font-bold">Player</th>
                  <th className="p-4 font-bold text-center border-l border-gray-200">Best Stint</th>
                  <th className="p-4 font-bold text-center border-l border-gray-200">Worst Stint</th>
                  <th className="p-4 font-bold text-center border-l border-gray-200 bg-blue-50 text-blue-800">Disparity</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {managerDisparities.map((row, i) => (
                  <tr key={i} className="hover:bg-gray-50 transition-colors">
                    {/* Player */}
                    <td className="p-4 align-middle">
                      <button 
                        onClick={() => onPlayerClick(row.id, row.name)}
                        className="font-bold text-blue-600 hover:text-blue-800 transition-colors text-left text-sm"
                      >
                        {row.name}
                      </button>
                    </td>

                    {/* Best Stint */}
                    <td className="p-4 align-middle border-l border-gray-100">
                      <div className="flex items-center justify-between gap-4">
                        <div 
                          onClick={() => onOwnerClick(row.bestStint.team)}
                          className="flex items-center gap-2 cursor-pointer hover:opacity-80"
                        >
                          <TeamAvatar team={row.bestStint.team} size="sm" />
                          <div>
                            <div className="font-bold text-xs text-gray-900 leading-none">{row.bestStint.team.name}</div>
                            <div className="text-[10px] text-gray-500 mt-0.5">{row.bestStint.team.owner}</div>
                          </div>
                        </div>
                        <div className="text-right">
                          <div className="font-mono font-black text-sm text-green-600">{row.bestStint.value.toFixed(2)}</div>
                          <div className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">{row.bestStint.volume.toFixed(1)} {tab === 'batters' ? 'PA' : 'IP'}</div>
                        </div>
                      </div>
                    </td>

                    {/* Worst Stint */}
                    <td className="p-4 align-middle border-l border-gray-100">
                      <div className="flex items-center justify-between gap-4">
                        <div 
                          onClick={() => onOwnerClick(row.worstStint.team)}
                          className="flex items-center gap-2 cursor-pointer hover:opacity-80"
                        >
                          <TeamAvatar team={row.worstStint.team} size="sm" />
                          <div>
                            <div className="font-bold text-xs text-gray-900 leading-none">{row.worstStint.team.name}</div>
                            <div className="text-[10px] text-gray-500 mt-0.5">{row.worstStint.team.owner}</div>
                          </div>
                        </div>
                        <div className="text-right">
                          <div className="font-mono font-black text-sm text-red-600">{row.worstStint.value.toFixed(2)}</div>
                          <div className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">{row.worstStint.volume.toFixed(1)} {tab === 'batters' ? 'PA' : 'IP'}</div>
                        </div>
                      </div>
                    </td>

                    {/* Disparity */}
                    <td className="p-4 align-middle text-center border-l border-gray-100 bg-blue-50/30">
                      <div className="font-mono text-lg font-black text-gray-800">
                        +{row.disparity.toFixed(2)}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      )}
    </div>
  );
}
