import { useEffect, useMemo, useState } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { aggregateStats, SCORING_CATS } from '../utils/scoring';
import TeamAvatar from '../components/TeamAvatar';
import DayRosterModal from '../components/DayRosterModal';

const STAT_OPTIONS = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];
const RATE_STATS = new Set(['ERA', 'WHIP', 'OBP']);

const VOLUME_THRESHOLDS = {
  OBP:  { key: 'PA', min: 10, label: 'PA' },
  ERA:  { key: 'IP', min: 3,  label: 'IP' },
  WHIP: { key: 'IP', min: 3,  label: 'IP' },
};

const AVAILABLE_SEASONS = Array.from({ length: 2026 - 2012 + 1 }, (_, i) => 2012 + i);

function formatVal(val, cat) {
  const n = parseFloat(val);
  if (isNaN(n) || val === undefined || val === null) return '-';
  if (cat === 'ERA') return n.toFixed(2);
  if (cat === 'OBP') return n.toFixed(4).replace(/^0/, '');
  if (SCORING_CATS[cat]?.isRate) return n.toFixed(3).replace(/^0/, '');
  if (cat === 'IP') return `${Math.floor(n)}.${Math.round((n % 1) * 3)}`;
  return Math.round(n);
}

const BAT_STATS = new Set(['R', 'HR', 'RBI', 'SB', 'OBP']);

export default function HighlightsView({ allStats, allSeasonData = {}, selectedSeason = 2026, onDownloadAll, downloadAllProgress }) {
  const [selectedStat, setSelectedStat] = useState('HR');
  const [selectedTeamId, setSelectedTeamId] = useState('all');
  const [selectedDay, setSelectedDay] = useState(null);
  const [fromYear, setFromYear] = useState(selectedSeason);
  const [toYear, setToYear] = useState(selectedSeason);

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

  const loadedYearsInRange = useMemo(
    () => yearsInRange.filter(y => allSeasonData[y]?.length > 0),
    [yearsInRange, allSeasonData]
  );

  const missingYears = yearsInRange.filter(y => !allSeasonData[y]?.length);

  // Combine records from all loaded seasons in the selected range.
  const combinedStats = useMemo(() => {
    if (fromYear === toYear) return allStats;
    const records = [];
    yearsInRange.forEach(y => {
      if (allSeasonData[y]) records.push(...allSeasonData[y]);
    });
    return records.length > 0 ? records : allStats;
  }, [fromYear, toYear, yearsInRange, allStats, allSeasonData]);

  // Remove exact duplicates that can arise from multiple CSV uploads to Supabase.
  // Key: season + team + period + player — if all four match, keep only the first.
  const dedupedStats = useMemo(() => {
    const seen = new Set();
    return combinedStats.filter(r => {
      const k = `${r.season_year}__${r.team_id}__${r.scoring_period_id}__${r.player_id ?? r.full_name}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }, [combinedStats]);

  // Group records by team + day across all included seasons.
  const teamDayRecords = useMemo(() => {
    const groups = {};
    dedupedStats.forEach(r => {
      if (r.lineup_slot_id === 16 || r.lineup_slot_id === 17) return;
      const key = `${r.season_year || 2026}__${r.team_id}__${r.scoring_period_id}`;
      if (!groups[key]) {
        groups[key] = {
          key,
          teamId: r.team_id,
          teamName: TEAMS[r.team_id]?.name || `Team ${r.team_id}`,
          period: r.scoring_period_id,
          season_year: r.season_year || 2026,
          date: getDateFromPeriodId(r.scoring_period_id, r.season_year || 2026),
          records: [],
        };
      }
      groups[key].records.push(r);
    });

    return Object.values(groups).map(g => ({
      ...g,
      stats: aggregateStats(g.records),
    }));
  }, [dedupedStats]);

  const volumeConfig = VOLUME_THRESHOLDS[selectedStat] ?? null;

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

      const withContrib = records.map(r => ({ r, c: getContribution(r) }));

      const sortedBest  = [...withContrib].sort((a, b) =>
        Math.abs(b.c - a.c) > 0.0001 ? b.c - a.c : volTiebreak(a.r, b.r)
      );
      const sortedWorst = [...withContrib].sort((a, b) =>
        Math.abs(a.c - b.c) > 0.0001 ? a.c - b.c : volTiebreak(a.r, b.r)
      );

      return {
        best:  sortedBest.slice(0, 10).map(x => x.r),
        worst: sortedWorst.slice(0, 10).map(x => x.r),
      };
    }

    // Counting stats: sort by raw value
    const sortBestFirst = [...records].sort((a, b) => {
      const va = parseFloat(a.stats[selectedStat]) || 0;
      const vb = parseFloat(b.stats[selectedStat]) || 0;
      if (Math.abs(va - vb) > 0.0001) return higherIsBetter ? vb - va : va - vb;
      return volTiebreak(a, b);
    });

    const sortWorstFirst = [...records].sort((a, b) => {
      const va = parseFloat(a.stats[selectedStat]) || 0;
      const vb = parseFloat(b.stats[selectedStat]) || 0;
      if (Math.abs(va - vb) > 0.0001) return higherIsBetter ? va - vb : vb - va;
      return volTiebreak(a, b);
    });

    return {
      best:  sortBestFirst.slice(0, 10),
      worst: sortWorstFirst.slice(0, 10),
    };
  }, [teamDayRecords, selectedStat, selectedTeamId, volumeConfig]);

  const catLabel  = SCORING_CATS[selectedStat]?.label || selectedStat;
  const volKey    = BAT_STATS.has(selectedStat) ? 'PA' : 'IP';
  const volLabel  = BAT_STATS.has(selectedStat) ? 'PA' : 'IP';
  const isRange   = fromYear !== toYear;
  const rangeLabel = isRange ? `${fromYear}–${toYear}` : String(fromYear);

  const selectCls = "border border-gray-200 rounded-lg px-3 py-2 text-sm font-semibold bg-white text-gray-700 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400";

  const HighlightTable = ({ title, records, accent }) => (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className={`px-5 py-3 border-b border-gray-200 ${accent}`}>
        <h3 className="text-sm font-bold uppercase tracking-wider">{title}</h3>
      </div>
      <table className="min-w-full text-sm">
        <thead className="bg-gray-50 border-b border-gray-200 text-xs text-gray-500 uppercase tracking-wider">
          <tr>
            <th className="px-4 py-2 text-left w-8">#</th>
            <th className="px-4 py-2 text-left">Team</th>
            <th className="px-4 py-2 text-center">Date</th>
            <th className="px-4 py-2 text-center text-gray-400">{volLabel}</th>
            <th className="px-4 py-2 text-center font-bold text-blue-600">{catLabel}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {records.length === 0 ? (
            <tr>
              <td colSpan={5} className="px-4 py-8 text-center text-gray-400 italic">
                No records meet the minimum volume threshold.
              </td>
            </tr>
          ) : (
            records.map((r, i) => (
              <tr
                key={`${r.key}-${i}`}
                onClick={() => setSelectedDay(r)}
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
                  {formatVal(r.stats[volKey], volKey)}
                </td>
                <td className="px-4 py-3 text-center font-mono font-bold text-gray-900">
                  {formatVal(r.stats[selectedStat], selectedStat)}
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );

  return (
    <div className="space-y-6">
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
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm font-semibold bg-white text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
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

      {/* Download all / missing seasons banner */}
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
      ) : isRange && missingYears.length > 0 ? (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 text-sm text-yellow-800 flex items-center justify-between gap-4">
          <span>
            <span className="font-semibold">{loadedYearsInRange.length} of {yearsInRange.length} seasons loaded.</span>
            {' '}Missing: {missingYears.join(', ')}.
          </span>
          <button
            onClick={onDownloadAll}
            className="shrink-0 bg-yellow-700 hover:bg-yellow-800 text-white text-xs font-bold px-3 py-1.5 rounded-lg transition-colors"
          >
            Download All
          </button>
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

      {selectedDay && (
        <DayRosterModal
          teamDayRecord={selectedDay}
          highlightStat={selectedStat}
          onClose={() => setSelectedDay(null)}
        />
      )}
    </div>
  );
}
