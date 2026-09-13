import { useEffect, useMemo, useState } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, SCORING_CATS } from '../utils/scoring';
import {
  calculateEraAdjustedStat,
  formatHighlightVal,
  RATE_STATS,
  ALL_COUNTING_STATS,
  ROSTER_CAPACITIES,
  MLB_LEAGUE_AVERAGES,
  getMlbMultiplier,
} from '../utils/eraAdjustments';
import TeamAvatar from '../components/TeamAvatar';
import DayRosterModal from '../components/DayRosterModal';

const STAT_OPTIONS = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

const VOLUME_THRESHOLDS = {
  OBP:  { key: 'PA', min: 10, label: 'PA' },
  ERA:  { key: 'IP', min: 3,  label: 'IP' },
  WHIP: { key: 'IP', min: 3,  label: 'IP' },
};

const AVAILABLE_SEASONS = Array.from({ length: 2026 - 2012 + 1 }, (_, i) => 2012 + i);

const BAT_STATS = new Set(['R', 'HR', 'RBI', 'SB', 'OBP']);

export default function HighlightsView({
  allStats,
  allSeasonData = {},
  selectedSeason = 2026,
  onDownloadAll,
  downloadAllProgress,
  onLoadSeason,
}) {
  const [selectedStat, setSelectedStat] = useState('HR');
  const [selectedTeamId, setSelectedTeamId] = useState('all');
  const [selectedDay, setSelectedDay] = useState(null);
  const [fromYear, setFromYear] = useState(selectedSeason);
  const [toYear, setToYear] = useState(selectedSeason);
  const [loadingSeasonYears, setLoadingSeasonYears] = useState([]);

  // Era adjustment states
  const [adjustRoster, setAdjustRoster] = useState(false);
  const [adjustMlb, setAdjustMlb] = useState(false);
  const [showMethodologyModal, setShowMethodologyModal] = useState(false);

  // Close methodology modal on Escape key
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') setShowMethodologyModal(false);
    };
    if (showMethodologyModal) {
      window.addEventListener('keydown', handleKeyDown);
      return () => window.removeEventListener('keydown', handleKeyDown);
    }
  }, [showMethodologyModal]);

  // Reset range whenever the parent season changes.
  useEffect(() => {
    setFromYear(selectedSeason);
    setToYear(selectedSeason);
  }, [selectedSeason]);

  const teamIds = useMemo(
    () => Object.keys(TEAMS).filter(id => parseInt(id) !== 99),
    []
  );

  const yearsInRange = useMemo(() => {
    const years = [];
    for (let y = fromYear; y <= toYear; y++) years.push(y);
    return years;
  }, [fromYear, toYear]);

  const isYearLoaded = (y) => {
    if (allSeasonData[y]?.length > 0) return true;
    if (y === selectedSeason && allStats?.length > 0) return true;
    return false;
  };

  const loadedYearsInRange = useMemo(
    () => yearsInRange.filter(isYearLoaded),
    [yearsInRange, allSeasonData, selectedSeason, allStats] // eslint-disable-line react-hooks/exhaustive-deps
  );

  const missingYears = useMemo(
    () => yearsInRange.filter(y => !isYearLoaded(y)),
    [yearsInRange, allSeasonData, selectedSeason, allStats] // eslint-disable-line react-hooks/exhaustive-deps
  );

  // Auto-load missing seasons when a focused range (<= 2 seasons missing) is selected
  useEffect(() => {
    if (!onLoadSeason || missingYears.length === 0) return;
    if (missingYears.length <= 2) {
      let isMounted = true;
      setLoadingSeasonYears(prev => Array.from(new Set([...prev, ...missingYears])));
      Promise.all(missingYears.map(y => onLoadSeason(y)))
        .finally(() => {
          if (isMounted) {
            setLoadingSeasonYears(prev => prev.filter(y => !missingYears.includes(y)));
          }
        });
      return () => {
        isMounted = false;
      };
    }
  }, [missingYears, onLoadSeason]);

  const handleLoadMissing = async () => {
    if (!onLoadSeason || missingYears.length === 0) return;
    setLoadingSeasonYears(prev => Array.from(new Set([...prev, ...missingYears])));
    for (const y of missingYears) {
      await onLoadSeason(y);
    }
    setLoadingSeasonYears([]);
  };

  // Combine records from all loaded seasons in the selected range.
  const combinedStats = useMemo(() => {
    const records = [];
    yearsInRange.forEach(y => {
      if (allSeasonData[y]?.length > 0) {
        records.push(...allSeasonData[y]);
      } else if (y === selectedSeason && allStats?.length > 0) {
        records.push(...allStats);
      }
    });
    return records;
  }, [yearsInRange, allStats, allSeasonData, selectedSeason]);

  // Remove exact duplicates that can arise from multiple CSV uploads to Supabase.
  // Key: season + team + period + player — if all four match, keep only the first.
  const dedupedStats = useMemo(() => {
    const seen = new Set();
    return combinedStats.filter(r => {
      const yr = r.season_year || selectedSeason || 2026;
      const k = `${yr}__${r.team_id}__${r.scoring_period_id}__${r.player_id ?? r.full_name}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }, [combinedStats, selectedSeason]);

  // Group records by team + day across all included seasons.
  const teamDayRecords = useMemo(() => {
    const groups = {};
    dedupedStats.forEach(r => {
      if (r.lineup_slot_id === 16 || r.lineup_slot_id === 17) return;
      const yr = r.season_year || selectedSeason || 2026;
      const key = `${yr}__${r.team_id}__${r.scoring_period_id}`;
      if (!groups[key]) {
        groups[key] = {
          key,
          teamId: r.team_id,
          teamName: TEAMS[r.team_id]?.name || `Team ${r.team_id}`,
          period: r.scoring_period_id,
          season_year: yr,
          date: getDateFromPeriodId(r.scoring_period_id, yr),
          records: [],
        };
      }
      groups[key].records.push(r);
    });

    return Object.values(groups).map(g => {
      const stats = aggregateStats(g.records);
      const activeBattersCount = g.records.filter(r => ![13, 14, 15, 16, 17].includes(r.lineup_slot_id)).length;
      const activePitchersCount = g.records.filter(r => [13, 14, 15].includes(r.lineup_slot_id)).length;
      return {
        ...g,
        stats,
        activeBattersCount,
        activePitchersCount,
      };
    });
  }, [dedupedStats, selectedSeason]);

  const volumeConfig = VOLUME_THRESHOLDS[selectedStat] ?? null;
  const isCountingStat = ALL_COUNTING_STATS.has(selectedStat);
  const isAnyAdjusted = isCountingStat && (adjustRoster || adjustMlb);

  const { best, worst } = useMemo(() => {
    let records = teamDayRecords;

    if (selectedTeamId !== 'all') {
      records = records.filter(r => String(r.teamId) === String(selectedTeamId));
    }

    const isBatStat = BAT_STATS.has(selectedStat);
    if (!isBatStat) {
      records = records.filter(r => (r.stats.IP || 0) > 0);
    }

    if (volumeConfig) {
      records = records.filter(r => (parseFloat(r.stats[volumeConfig.key]) || 0) >= volumeConfig.min);
    }

    const higherIsBetter = SCORING_CATS[selectedStat]?.type !== 'low';
    const tiebreakKey = isBatStat ? 'PA' : 'IP';

    const volTiebreak = (a, b) =>
      (parseFloat(b.stats[tiebreakKey]) || 0) - (parseFloat(a.stats[tiebreakKey]) || 0);

    // Rate stats (ERA, WHIP, OBP): rank by marginal impact —
    // "which day, if removed, would shift the overall aggregate the most?"
    if (RATE_STATS.has(selectedStat)) {
      // Build aggregate numerators/denominators across all filtered records
      const totalIP   = records.reduce((s, r) => s + (r.stats.IP          || 0), 0);
      const totalER   = records.reduce((s, r) => s + (r.stats.ER          || 0), 0);
      const totalBBH  = records.reduce((s, r) => s + (r.stats.BB_Allowed  || 0) + (r.stats.H_Allowed || 0), 0);
      const totalOBPn = records.reduce((s, r) => s + (r.stats.OBP_num     || 0), 0);
      const totalPA   = records.reduce((s, r) => s + (r.stats.PA          || 0), 0);

      // impact = (stat without this day) − (overall stat)
      // contribution = how much this day HELPED the stat (positive = helped)
      const getContribution = (r) => {
        if (selectedStat === 'ERA') {
          const newIP = totalIP - (r.stats.IP || 0);
          if (newIP <= 0) return 0;
          const overall = totalIP > 0 ? (totalER * 9) / totalIP : 0;
          const without = ((totalER - (r.stats.ER || 0)) * 9) / newIP;
          return without - overall; // ERA↑ when removed → day helped (positive contribution)
        }
        if (selectedStat === 'WHIP') {
          const newIP = totalIP - (r.stats.IP || 0);
          if (newIP <= 0) return 0;
          const overall = totalIP > 0 ? totalBBH / totalIP : 0;
          const without = (totalBBH - (r.stats.BB_Allowed || 0) - (r.stats.H_Allowed || 0)) / newIP;
          return without - overall; // WHIP↑ when removed → day helped (positive contribution)
        }
        // OBP
        const newPA = totalPA - (r.stats.PA || 0);
        if (newPA <= 0) return 0;
        const overall = totalPA > 0 ? totalOBPn / totalPA : 0;
        const without = (totalOBPn - (r.stats.OBP_num || 0)) / newPA;
        return overall - without; // OBP↓ when removed → day helped (positive contribution)
      };

      const withContrib = records.map(r => ({
        ...r,
        currentStatRaw: parseFloat(r.stats[selectedStat]) || 0,
        currentStatVal: parseFloat(r.stats[selectedStat]) || 0,
        adjInfo: { isAdjusted: false, totalMultiplier: 1.0 },
        c: getContribution(r)
      }));

      const sortedBest  = [...withContrib].sort((a, b) =>
        Math.abs(b.c - a.c) > 0.0001 ? b.c - a.c : volTiebreak(a, b)
      );
      const sortedWorst = [...withContrib].sort((a, b) =>
        Math.abs(a.c - b.c) > 0.0001 ? a.c - b.c : volTiebreak(a, b)
      );

      return {
        best:  sortedBest.slice(0, 10),
        worst: sortedWorst.slice(0, 10),
      };
    }

    // Counting stats: attach era-adjusted values and sort by adjusted total
    const recordsWithAdj = records.map(r => {
      const rawVal = parseFloat(r.stats[selectedStat]) || 0;
      const yr = r.season_year || selectedSeason || 2026;
      const adjInfo = calculateEraAdjustedStat(selectedStat, rawVal, yr, {
        adjustRoster,
        adjustMlb,
      });
      return {
        ...r,
        currentStatRaw: rawVal,
        currentStatVal: adjInfo.adjustedVal,
        adjInfo,
      };
    });

    const sortBestFirst = [...recordsWithAdj].sort((a, b) => {
      const va = a.currentStatVal;
      const vb = b.currentStatVal;
      if (Math.abs(va - vb) > 0.0001) return higherIsBetter ? vb - va : va - vb;
      return volTiebreak(a, b);
    });

    const sortWorstFirst = [...recordsWithAdj].sort((a, b) => {
      const va = a.currentStatVal;
      const vb = b.currentStatVal;
      if (Math.abs(va - vb) > 0.0001) return higherIsBetter ? va - vb : vb - va;
      return volTiebreak(a, b);
    });

    return {
      best:  sortBestFirst.slice(0, 10),
      worst: sortWorstFirst.slice(0, 10),
    };
  }, [teamDayRecords, selectedStat, selectedTeamId, volumeConfig, adjustRoster, adjustMlb, selectedSeason]);

  const catLabel  = SCORING_CATS[selectedStat]?.label || selectedStat;
  const volKey    = BAT_STATS.has(selectedStat) ? 'PA' : 'IP';
  const volLabel  = BAT_STATS.has(selectedStat) ? 'PA' : 'IP';
  const isRange   = fromYear !== toYear;
  const rangeLabel = isRange ? `${fromYear}–${toYear}` : String(fromYear);

  const selectCls = "border border-gray-200 rounded-lg px-3 py-2 text-sm font-semibold bg-white text-gray-700 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400";

  const HighlightTable = ({ title, records, accent }) => (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className={`px-5 py-3 border-b border-gray-200 ${accent} flex items-center justify-between`}>
        <h3 className="text-sm font-bold uppercase tracking-wider">{title}</h3>
        {isAnyAdjusted && (
          <span className="text-[10px] font-mono font-bold bg-white/70 px-2 py-0.5 rounded border border-current">
            Sorted by Adjusted {catLabel}
          </span>
        )}
      </div>
      <table className="min-w-full text-sm">
        <thead className="bg-gray-50 border-b border-gray-200 text-xs text-gray-500 uppercase tracking-wider">
          <tr>
            <th className="px-4 py-2 text-left w-8">#</th>
            <th className="px-4 py-2 text-left">Team</th>
            <th className="px-4 py-2 text-center">Date</th>
            <th className="px-4 py-2 text-center text-gray-400">{volLabel}</th>
            <th className="px-4 py-2 text-center font-bold text-blue-600">
              <div className="flex items-center justify-center gap-1">
                <span>{catLabel}</span>
                {isAnyAdjusted && (
                  <span className="text-[10px] text-amber-600 font-bold">(Adj)</span>
                )}
              </div>
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {loadingSeasonYears.length > 0 && records.length === 0 ? (
            <tr>
              <td colSpan={5} className="px-4 py-8 text-center text-gray-400 italic">
                <div className="flex items-center justify-center gap-2">
                  <svg className="animate-spin h-4 w-4 text-blue-500" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
                  </svg>
                  <span>Loading highlights data…</span>
                </div>
              </td>
            </tr>
          ) : records.length === 0 ? (
            <tr>
              <td colSpan={5} className="px-4 py-8 text-center text-gray-400 italic">
                No records meet the minimum volume threshold.
              </td>
            </tr>
          ) : (
            records.map((r, i) => {
              const isAdjusted = r.adjInfo?.isAdjusted;
              const tooltip = isAdjusted
                ? `${r.teamName} (${r.date})\nRaw ${selectedStat}: ${Math.round(r.currentStatRaw)}\n` +
                  (adjustRoster ? `• Roster Multiplier: ${r.adjInfo.rosterMultiplier.toFixed(3)}x (${r.season_year <= 2025 ? '13/9' : '16/12'} starters vs 2026 16/12)\n` : '') +
                  (adjustMlb ? `• MLB Era Multiplier: ${r.adjInfo.mlbMultiplier.toFixed(3)}x (MLB ${r.season_year} environment vs 2026)\n` : '') +
                  `• Combined Factor: ${r.adjInfo.totalMultiplier.toFixed(3)}x → ${r.currentStatVal.toFixed(2)} ${selectedStat}`
                : `${r.teamName} (${r.date}) — ${r.currentStatRaw ?? r.stats[selectedStat]} ${selectedStat}`;

              return (
                <tr
                  key={`${r.key}-${i}`}
                  onClick={() => setSelectedDay(r)}
                  title={tooltip}
                  className="hover:bg-blue-50 cursor-pointer transition-colors group"
                >
                  <td className="px-4 py-3 text-gray-300 font-mono text-xs">{i + 1}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2">
                      <TeamAvatar team={{ id: r.teamId, name: r.teamName }} size="sm" />
                      <span className="font-semibold text-gray-900 group-hover:text-blue-700">{r.teamName}</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-center font-mono text-xs text-gray-500">{r.date}</td>
                  <td className="px-4 py-3 text-center font-mono text-xs text-gray-400">
                    {formatHighlightVal(r.stats[volKey], volKey)}
                  </td>
                  <td className="px-4 py-3 text-center font-mono font-bold">
                    {isAdjusted ? (
                      <div className="flex flex-col items-center justify-center">
                        <span className="text-amber-600 font-black text-sm">
                          {formatHighlightVal(r.currentStatVal, selectedStat, true)}
                        </span>
                        <span className="text-[10px] text-gray-400 font-normal">
                          raw: {Math.round(r.currentStatRaw)}
                        </span>
                      </div>
                    ) : (
                      <span className="text-gray-900">
                        {formatHighlightVal(r.stats[selectedStat], selectedStat)}
                      </span>
                    )}
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
    </div>
  );

  return (
    <div className="space-y-6">
      {/* Top Header & Standard Filters */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h2 className="text-2xl font-black text-gray-900">Highlights — {rangeLabel}</h2>

        <div className="flex gap-3 flex-wrap items-center">
          {volumeConfig && (
            <span className="text-xs text-gray-400 italic">
              Min. {volumeConfig.min} {volumeConfig.label} required
            </span>
          )}

          {/* Download all seasons */}
          <button
            onClick={onDownloadAll}
            disabled={!!downloadAllProgress}
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm font-semibold bg-white text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors cursor-pointer"
            title="Download all seasons into the browser cache for cross-season highlights"
          >
            {downloadAllProgress
              ? `${downloadAllProgress.done}/${downloadAllProgress.total}…`
              : 'Download All Seasons'}
          </button>

          {/* Year range */}
          <div className="flex items-center gap-2">
            <select
              value={fromYear}
              onChange={e => {
                const y = parseInt(e.target.value);
                setFromYear(y);
                if (toYear < y) setToYear(y);
              }}
              className={selectCls}
            >
              {AVAILABLE_SEASONS.map(y => (
                <option key={y} value={y}>{y}</option>
              ))}
            </select>
            <span className="text-gray-400 text-sm font-semibold">–</span>
            <select
              value={toYear}
              onChange={e => {
                const y = parseInt(e.target.value);
                setToYear(y);
                if (fromYear > y) setFromYear(y);
              }}
              className={selectCls}
            >
              {AVAILABLE_SEASONS.map(y => (
                <option key={y} value={y}>{y}</option>
              ))}
            </select>
          </div>

          <select
            value={selectedStat}
            onChange={e => setSelectedStat(e.target.value)}
            className={selectCls}
          >
            {STAT_OPTIONS.map(s => (
              <option key={s} value={s}>{SCORING_CATS[s]?.label || s}</option>
            ))}
          </select>

          <select
            value={selectedTeamId}
            onChange={e => setSelectedTeamId(e.target.value)}
            className={selectCls}
          >
            <option value="all">All Teams</option>
            {teamIds.map(id => (
              <option key={id} value={id}>{TEAMS[id].name}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Era Adjustments Control Bar */}
      <div className="bg-gradient-to-r from-slate-900 via-slate-800 to-indigo-950 text-white rounded-2xl p-4 shadow-md border border-slate-700/60 flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-amber-500/20 border border-amber-500/40 flex items-center justify-center text-xl shrink-0">
            ⚡
          </div>
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <h3 className="text-sm font-black tracking-wide text-white">Era Adjustments</h3>
              {isCountingStat && (
                <span className={`text-[10px] font-black uppercase px-2 py-0.5 rounded-full border ${
                  adjustRoster && adjustMlb
                    ? 'bg-gradient-to-r from-amber-500 to-indigo-500 text-white border-white/20 shadow-xs'
                    : adjustRoster
                    ? 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                    : adjustMlb
                    ? 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40'
                    : 'bg-slate-800 text-slate-400 border-slate-700'
                }`}>
                  {adjustRoster && adjustMlb
                    ? '⚡ Full Era-Adjusted'
                    : adjustRoster
                    ? '👥 Roster-Adjusted'
                    : adjustMlb
                    ? '⚾ MLB Era-Adjusted'
                    : 'Raw Stats'}
                </span>
              )}
            </div>
            <p className="text-xs text-slate-300 mt-0.5">
              {isCountingStat
                ? 'Normalize counting totals for league starter slot expansions & MLB league-wide environment shifts.'
                : 'Rate statistics (ERA, WHIP, OBP) are per-inning/PA rates. Era adjustments apply to counting categories.'}
            </p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2 shrink-0">
          {/* Roster Size Toggle */}
          <button
            onClick={() => setAdjustRoster(!adjustRoster)}
            disabled={!isCountingStat}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition-all flex items-center gap-1.5 cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed border ${
              adjustRoster && isCountingStat
                ? 'bg-amber-500 hover:bg-amber-400 text-slate-950 border-amber-400 shadow-md shadow-amber-500/20'
                : 'bg-slate-800/90 hover:bg-slate-700 text-slate-300 border-slate-700'
            }`}
            title="Normalizes historical seasons (13 batters / 9 pitchers in 2018–2025) to the modern 2026 format (16 batters / 12 pitchers)"
          >
            <span>👥</span>
            <span>Roster Size Adj</span>
            {adjustRoster && isCountingStat && <span className="text-[10px] bg-slate-950/20 px-1 py-0.2 rounded font-black">ON</span>}
          </button>

          {/* MLB Environment Toggle */}
          <button
            onClick={() => setAdjustMlb(!adjustMlb)}
            disabled={!isCountingStat}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition-all flex items-center gap-1.5 cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed border ${
              adjustMlb && isCountingStat
                ? 'bg-indigo-500 hover:bg-indigo-400 text-white border-indigo-400 shadow-md shadow-indigo-500/20'
                : 'bg-slate-800/90 hover:bg-slate-700 text-slate-300 border-slate-700'
            }`}
            title="Normalizes for MLB-wide shifts over time (juiced ball HR spikes, modern SB explosion from rules changes, declining starter innings for QS)"
          >
            <span>⚾</span>
            <span>MLB Era Adj</span>
            {adjustMlb && isCountingStat && <span className="text-[10px] bg-white/20 px-1 py-0.2 rounded font-black">ON</span>}
          </button>

          {/* Methodology Modal Trigger */}
          <button
            onClick={() => setShowMethodologyModal(true)}
            className="px-2.5 py-1.5 rounded-xl text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 transition-colors flex items-center gap-1 cursor-pointer"
            title="View mathematical methodology, baseline formulas, and season multipliers"
          >
            <span>ℹ️</span>
            <span>Methodology</span>
          </button>
        </div>
      </div>

      {/* Download all / loading / missing seasons banner */}
      {downloadAllProgress ? (
        <div className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-3 text-sm text-blue-800 flex items-center gap-3">
          <svg className="animate-spin h-4 w-4 text-blue-600 shrink-0" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
          </svg>
          <span>
            Downloading season data… {downloadAllProgress.done} / {downloadAllProgress.total} complete
            {downloadAllProgress.current && ` (loading ${downloadAllProgress.current})`}
          </span>
        </div>
      ) : loadingSeasonYears.length > 0 ? (
        <div className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-3 text-sm text-blue-800 flex items-center gap-3">
          <svg className="animate-spin h-4 w-4 text-blue-600 shrink-0" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
          </svg>
          <span>
            Loading season data for {loadingSeasonYears.join(', ')}…
          </span>
        </div>
      ) : missingYears.length > 0 ? (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 text-sm text-yellow-800 flex items-center justify-between gap-4">
          <span>
            <span className="font-semibold">{loadedYearsInRange.length} of {yearsInRange.length} seasons loaded.</span>
            {' '}Missing: {missingYears.join(', ')}.
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={handleLoadMissing}
              className="shrink-0 bg-yellow-700 hover:bg-yellow-800 text-white text-xs font-bold px-3 py-1.5 rounded-lg transition-colors cursor-pointer"
            >
              Load {missingYears.length === 1 ? missingYears[0] : 'Missing Seasons'}
            </button>
            <button
              onClick={onDownloadAll}
              className="shrink-0 bg-white border border-yellow-400 hover:bg-yellow-100 text-yellow-800 text-xs font-bold px-3 py-1.5 rounded-lg transition-colors cursor-pointer"
            >
              Download All
            </button>
          </div>
        </div>
      ) : null}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <HighlightTable
          title={`Best ${catLabel} Days`}
          records={best}
          accent="bg-green-50 text-green-800"
        />
        <HighlightTable
          title={`Worst ${catLabel} Days`}
          records={worst}
          accent="bg-red-50 text-red-700"
        />
      </div>

      {/* Roster detail modal */}
      {selectedDay && (
        <DayRosterModal
          teamDayRecord={selectedDay}
          highlightStat={selectedStat}
          adjustRoster={adjustRoster}
          adjustMlb={adjustMlb}
          onClose={() => setSelectedDay(null)}
        />
      )}

      {/* Era Adjustments Methodology Modal */}
      {showMethodologyModal && (
        <div
          className="fixed inset-0 bg-black/75 flex items-center justify-center z-[90] p-4 backdrop-blur-xs animate-fade-in"
          onClick={e => e.target === e.currentTarget && setShowMethodologyModal(false)}
        >
          <div className="bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl w-full max-w-3xl max-h-[90vh] flex flex-col overflow-hidden text-slate-200">
            {/* Modal Header */}
            <div className="bg-slate-950 px-6 py-4 border-b border-slate-800 flex items-center justify-between shrink-0">
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 rounded-lg bg-amber-500/20 text-amber-300 flex items-center justify-center text-lg font-black">
                  ⚡
                </div>
                <div>
                  <h3 className="text-base font-black text-white">Era-Adjustment Methodology & Multipliers</h3>
                  <p className="text-xs text-slate-400">Sabermetric standards for fair multi-era comparison</p>
                </div>
              </div>
              <button
                onClick={() => setShowMethodologyModal(false)}
                className="text-slate-400 hover:text-white text-2xl font-black leading-none cursor-pointer"
              >
                &times;
              </button>
            </div>

            {/* Modal Content */}
            <div className="p-6 overflow-y-auto space-y-6 text-xs leading-relaxed">
              {/* Section 1: Roster Size */}
              <div className="bg-slate-950/80 rounded-xl p-4 border border-slate-800 space-y-3">
                <div className="flex items-center gap-2 text-amber-400 font-bold text-sm">
                  <span>👥</span>
                  <h4>1. League Roster Capacity Normalization</h4>
                </div>
                <p className="text-slate-300">
                  Counting statistics accumulate higher totals when owners start more players per day. Our league maintained a <strong>22-starter format (13 Batters / 9 Pitchers)</strong> from 2018 through 2025, expanding in 2026 to a <strong>28-starter format (16 Batters / 12 Pitchers)</strong>:
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1">
                  <div className="bg-slate-900 p-3 rounded-lg border border-slate-800">
                    <span className="font-bold text-white block text-sm">Batting Counting Stats</span>
                    <span className="text-[11px] text-slate-400 block mb-2">Runs, Home Runs, RBIs, Stolen Bases</span>
                    <div className="font-mono text-amber-300 text-xs">
                      2018–2025 Multiplier: <strong>16 / 13 ≈ 1.231x</strong>
                    </div>
                    <div className="font-mono text-slate-400 text-xs mt-0.5">
                      2026 Multiplier: <strong>1.000x</strong> (Baseline)
                    </div>
                  </div>
                  <div className="bg-slate-900 p-3 rounded-lg border border-slate-800">
                    <span className="font-bold text-white block text-sm">Pitching Counting Stats</span>
                    <span className="text-[11px] text-slate-400 block mb-2">Strikeouts, Quality Starts, SV+HDs</span>
                    <div className="font-mono text-amber-300 text-xs">
                      2018–2025 Multiplier: <strong>12 / 9 = 1.333x</strong>
                    </div>
                    <div className="font-mono text-slate-400 text-xs mt-0.5">
                      2026 Multiplier: <strong>1.000x</strong> (Baseline)
                    </div>
                  </div>
                </div>
                <p className="text-[11px] text-slate-400 italic">
                  Example: Mark&apos;s 51 K day on the final day of 2025 (achieved with 9 active pitcher slots) normalizes to <strong>68.0 Strikeouts</strong> under the 12-pitcher format.
                </p>
              </div>

              {/* Section 2: MLB Environment */}
              <div className="bg-slate-950/80 rounded-xl p-4 border border-slate-800 space-y-3">
                <div className="flex items-center gap-2 text-indigo-400 font-bold text-sm">
                  <span>⚾</span>
                  <h4>2. MLB Environment / Era Scarcity Adjustment</h4>
                </div>
                <p className="text-slate-300">
                  Major League Baseball environments shift over time due to ball construction, rule changes, and bullpen management. The MLB multiplier scales each season to the 2026 standard:
                </p>
                <div className="bg-slate-900 p-2.5 rounded-lg border border-slate-800 font-mono text-[11px] text-indigo-300 text-center">
                  Multiplier = MLB Average (2026 Baseline) ÷ MLB Average (Season)
                </div>
                <div className="overflow-x-auto rounded-lg border border-slate-800 mt-2">
                  <table className="w-full text-left border-collapse text-[11px]">
                    <thead className="bg-slate-900 text-slate-400 uppercase font-bold text-[10px]">
                      <tr>
                        <th className="py-2 px-2.5">Season</th>
                        <th className="py-2 px-2">HR/G</th>
                        <th className="py-2 px-2">SB/G</th>
                        <th className="py-2 px-2">QS%</th>
                        <th className="py-2 px-2">K/G</th>
                        <th className="py-2 px-2">R/G</th>
                        <th className="py-2 px-3">Era Context</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/80 font-mono">
                      {[2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026].map(y => {
                        const s = MLB_LEAGUE_AVERAGES.seasons[y];
                        const hrMult = getMlbMultiplier('HR', y).toFixed(2);
                        const sbMult = getMlbMultiplier('SB', y).toFixed(2);
                        return (
                          <tr key={y} className={`hover:bg-slate-800/40 ${y === 2026 ? 'bg-indigo-950/40 font-bold text-indigo-200' : ''}`}>
                            <td className="py-1.5 px-2.5 font-bold text-white">{y}</td>
                            <td className="py-1.5 px-2">{s.HR} <span className="text-slate-400 font-normal">({hrMult}x)</span></td>
                            <td className="py-1.5 px-2">{s.SB} <span className="text-slate-400 font-normal">({sbMult}x)</span></td>
                            <td className="py-1.5 px-2">{Math.round(s.QS * 100)}%</td>
                            <td className="py-1.5 px-2">{s.K}</td>
                            <td className="py-1.5 px-2">{s.R}</td>
                            <td className="py-1.5 px-3 font-sans text-[10px] text-slate-400">
                              {y === 2019 && 'Juiced Ball peak (HRs deflated by 0.83x)'}
                              {y === 2021 && 'SB drought (SBs boosted by 1.48x)'}
                              {y === 2022 && 'Deadened Ball (HRs boosted by 1.07x)'}
                              {y === 2023 && 'Rule changes (larger bases, pitch clock)'}
                              {y === 2026 && 'Current standard baseline'}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Section 3: Combined Multiplier */}
              <div className="bg-slate-950/80 rounded-xl p-4 border border-slate-800 space-y-2">
                <div className="flex items-center gap-2 text-emerald-400 font-bold text-sm">
                  <span>✨</span>
                  <h4>3. Combined Multiplier Formula</h4>
                </div>
                <p className="text-slate-300">
                  When both toggles are enabled, the multipliers compose multiplicatively:
                </p>
                <div className="bg-slate-900 p-2.5 rounded-lg border border-slate-800 font-mono text-[11px] text-emerald-300 text-center">
                  Adjusted Stat = Raw Stat × Roster Multiplier × MLB Multiplier
                </div>
              </div>
            </div>

            {/* Modal Footer */}
            <div className="bg-slate-950 px-6 py-3 border-t border-slate-800 flex justify-end shrink-0">
              <button
                onClick={() => setShowMethodologyModal(false)}
                className="px-4 py-2 rounded-xl bg-blue-600 hover:bg-blue-500 text-white font-bold text-xs transition cursor-pointer"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
