// src/views/history/tabs/HistoryHighlightsTab.jsx
import React from 'react';
import TeamAvatar from '../../../components/TeamAvatar';
import {
  CATEGORIES,
  LEGACY_CATEGORIES,
  CAT_ICONS,
  formatRawStat,
  formatDiffStat,
  formatMeanStat,
} from '../services/historyFinishesService';
import { RATE_STATS } from '../../../utils/eraAdjustments';

export default function HistoryHighlightsTab({
  highlightsData,
  adjustRoster,
  setAdjustRoster,
  adjustMlb,
  setAdjustMlb,
  exclude2020Lowlights,
  setExclude2020Lowlights,
  highlightFilter,
  setHighlightFilter,
  highlightViewMode,
  setHighlightViewMode,
  marginsGapType,
  setMarginsGapType,
  selectedStatCategory,
  setSelectedStatCategory,
  displayedStatCategories,
}) {
  const isDisplayingLegacy = LEGACY_CATEGORIES.some(c => c.key === selectedStatCategory);

  const renderTotalsCategoryCard = (cat, isLegacy = false) => {
    const record = isLegacy
      ? highlightsData.legacyStatRecords?.[cat.key]
      : highlightsData.categoryStatRecords?.[cat.key];
    if (!record) return null;
    const { highs, lows, totalTracked } = record;
    const icon = CAT_ICONS[cat.key] || '⚾';
    const isBat = cat.type === 'bat';
    const showHighs = highlightFilter === 'all' || highlightFilter === 'highlights';
    const showLows = highlightFilter === 'all' || highlightFilter === 'lowlights';
    const twoCols = showHighs && showLows;

    return (
      <div
        key={`stat-rec-${cat.key}`}
        className="bg-white border border-gray-200 rounded-3xl p-5 sm:p-6 shadow-sm space-y-4 hover:shadow-md transition"
      >
        {/* Card Header */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
          <div className="flex items-center gap-2.5">
            <span className="text-2xl">{icon}</span>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                  isLegacy
                    ? 'bg-purple-100 text-purple-800'
                    : isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                }`}>
                  {isLegacy ? 'Legacy' : isBat ? 'Batting' : 'Pitching'}
                </span>
              </div>
              <div className="text-[11px] text-gray-400 mt-0.5 flex flex-wrap items-center gap-2">
                <span>{cat.higherIsBetter ? '▲ Higher is better' : '▼ Lower is better'}</span>
                <span>•</span>
                <span>{totalTracked} team-seasons tracked</span>
                <span>•</span>
                <span className="font-mono text-gray-500">
                  {cat.activeYears[0] === cat.activeYears[1] ? `Season: ${cat.activeYears[0]}` : `Active: ${cat.activeYears[0]}–${cat.activeYears[1]}`}
                </span>
                {cat.legacyNote && (
                  <>
                    <span>•</span>
                    <span className="text-amber-700 font-semibold bg-amber-50 px-1.5 py-0.5 rounded border border-amber-200">
                      ℹ️ {cat.legacyNote}
                    </span>
                  </>
                )}
              </div>
            </div>
          </div>

          {isLegacy ? (
            <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-purple-100 text-purple-800 border border-purple-300 self-start sm:self-auto">
              <span>🏛️</span>
              <span>Discontinued</span>
            </span>
          ) : (
            (adjustRoster || adjustMlb) && !RATE_STATS.has(cat.key) && (
              <span className="inline-flex items-center gap-1 text-[10px] font-black px-2.5 py-1 rounded-lg bg-amber-500/10 text-amber-800 border border-amber-300/60 self-start sm:self-auto">
                <span>⚡</span>
                <span>Era Adjusted</span>
              </span>
            )
          )}
        </div>

        {/* Highs & Lows Columns */}
        <div className={`grid grid-cols-1 ${twoCols ? 'lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100' : ''} gap-6`}>
          {/* HIGHS / BEST COLUMN */}
          {showHighs && (
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1">
                <span className="text-xs font-black text-emerald-800 flex items-center gap-1.5">
                  <span>🥇</span>
                  <span>All-Time Highs (Best)</span>
                </span>
                <span className="text-[10px] font-mono font-bold text-emerald-600">Top 5</span>
              </div>

              <div className="divide-y divide-gray-50 text-xs">
                {highs.map((rec, i) => {
                  const isGold = i === 0;
                  const isSilver = i === 1;
                  const isBronze = i === 2;
                  let dispVal = '';
                  let subRaw = null;
                  if (!isLegacy && rec.isAdjusted && !RATE_STATS.has(cat.key)) {
                    dispVal = cat.key === 'QS' || cat.key === 'SVHLD'
                      ? rec.adjustedVal.toFixed(1)
                      : Math.round(rec.adjustedVal).toLocaleString();
                    subRaw = `raw: ${formatRawStat(cat.key, rec.rawVal)}`;
                  } else {
                    dispVal = formatRawStat(cat.key, rec.rawVal);
                  }

                  return (
                    <div
                      key={`high-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                      className="py-2.5 px-2 flex items-center justify-between hover:bg-emerald-50/50 rounded-xl transition gap-3"
                    >
                      <div className="flex items-center gap-2.5 min-w-0">
                        <span className={`w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 ${
                          isGold ? 'bg-amber-400 text-amber-950 ring-1 ring-amber-300' :
                          isSilver ? 'bg-slate-300 text-slate-900' :
                          isBronze ? 'bg-amber-700 text-amber-100' :
                          'bg-gray-100 text-gray-600'
                        }`}>
                          {isGold ? '🥇' : isSilver ? '🥈' : isBronze ? '🥉' : i + 1}
                        </span>
                        <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                          <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                            <span>{rec.owner}</span>
                            <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                            {rec.place === 1 && <span className="text-amber-500 text-[11px]">👑</span>}
                          </div>
                          <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                            • {rec.teamName}
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center gap-2.5 shrink-0 pl-2">
                        <div className="text-right font-mono">
                          <div className="font-black text-emerald-700 text-xs sm:text-sm">
                            {dispVal}
                          </div>
                          {subRaw && (
                            <div className="text-[9px] text-gray-400">
                              {subRaw}
                            </div>
                          )}
                        </div>
                        {rec.pctVsAvg !== null && (
                          <span
                            className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-emerald-50 text-emerald-700 border border-emerald-200/80 shrink-0 font-mono"
                            title={`Season average in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)} (${formatDiffStat(cat.key, rec.diffVsAvg)} vs avg)`}
                          >
                            <span>+{rec.pctVsAvg.toFixed(1)}%</span>
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* LOWS / WORST COLUMN */}
          {showLows && (
            <div className={`space-y-2 ${twoCols ? 'lg:pl-6 pt-4 lg:pt-0' : ''}`}>
              <div className="flex items-center justify-between px-1">
                <span className="text-xs font-black text-rose-800 flex items-center gap-1.5">
                  <span>📉</span>
                  <span>All-Time Lows (Toughest)</span>
                </span>
                <span className="text-[10px] font-mono font-bold text-rose-600">Bottom 5</span>
              </div>

              <div className="divide-y divide-gray-50 text-xs">
                {lows.map((rec, i) => {
                  let dispVal = '';
                  let subRaw = null;
                  if (!isLegacy && rec.isAdjusted && !RATE_STATS.has(cat.key)) {
                    dispVal = cat.key === 'QS' || cat.key === 'SVHLD'
                      ? rec.adjustedVal.toFixed(1)
                      : Math.round(rec.adjustedVal).toLocaleString();
                    subRaw = `raw: ${formatRawStat(cat.key, rec.rawVal)}`;
                  } else {
                    dispVal = formatRawStat(cat.key, rec.rawVal);
                  }

                  return (
                    <div
                      key={`low-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                      className="py-2.5 px-2 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition gap-3"
                    >
                      <div className="flex items-center gap-2.5 min-w-0">
                        <span className="w-5 h-5 rounded-lg bg-gray-100 text-gray-500 font-mono font-black text-[10px] flex items-center justify-center shrink-0">
                          {i + 1}
                        </span>
                        <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                          <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                            <span>{rec.owner}</span>
                            <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                          </div>
                          <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                            • {rec.teamName}
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center gap-2.5 shrink-0 pl-2">
                        <div className="text-right font-mono">
                          <div className="font-black text-rose-600 text-xs sm:text-sm">
                            {dispVal}
                          </div>
                          {subRaw && (
                            <div className="text-[9px] text-gray-400">
                              {subRaw}
                            </div>
                          )}
                        </div>
                        {rec.pctVsAvg !== null && (
                          <span
                            className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[11px] font-black bg-rose-50 text-rose-700 border border-rose-200/80 shrink-0 font-mono"
                            title={`Season average in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)} (${formatDiffStat(cat.key, rec.diffVsAvg)} vs avg)`}
                          >
                            <span>-{Math.abs(rec.pctVsAvg).toFixed(1)}%</span>
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
      </div>
    );
  };

  const renderRelativeCategoryCard = (cat, isLegacy = false) => {
    const record = highlightsData.relativeDominanceRecords?.[cat.key];
    if (!record) return null;
    const { highs, lows, totalTracked } = record;
    const icon = CAT_ICONS[cat.key] || '⚾';
    const isBat = cat.type === 'bat';
    const showHighs = highlightFilter === 'all' || highlightFilter === 'highlights';
    const showLows = highlightFilter === 'all' || highlightFilter === 'lowlights';
    const twoCols = showHighs && showLows;

    return (
      <div
        key={`rel-rec-${cat.key}`}
        className="bg-white border border-gray-200 rounded-3xl p-5 sm:p-6 shadow-sm space-y-4 hover:shadow-md transition"
      >
        {/* Card Header */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
          <div className="flex items-center gap-2.5">
            <span className="text-2xl">{icon}</span>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                  isLegacy
                    ? 'bg-purple-100 text-purple-800'
                    : isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                }`}>
                  {isLegacy ? 'Legacy' : isBat ? 'Batting' : 'Pitching'}
                </span>
                <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-indigo-50 text-indigo-700 border border-indigo-200/60 font-mono">
                  % Above/Below Season Avg
                </span>
              </div>
              <div className="text-[11px] text-gray-400 mt-0.5 flex flex-wrap items-center gap-2">
                <span>Ranks normalized deviation from that season's league mean</span>
                <span>•</span>
                <span>{totalTracked} team-seasons tracked</span>
                {cat.legacyNote && (
                  <>
                    <span>•</span>
                    <span className="text-amber-700 font-semibold bg-amber-50 px-1.5 py-0.5 rounded border border-amber-200">
                      ℹ️ {cat.legacyNote}
                    </span>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Highs & Lows Columns */}
        <div className={`grid grid-cols-1 ${twoCols ? 'lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100' : ''} gap-6`}>
          {/* HIGHS / RELATIVE DOMINANCE COLUMN */}
          {showHighs && (
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1">
                <span className="text-xs font-black text-indigo-900 flex items-center gap-1.5">
                  <span>🚀</span>
                  <span>Greatest Dominance (% Above League Avg)</span>
                </span>
                <span className="text-[10px] font-mono font-bold text-indigo-600">Top 5</span>
              </div>

              <div className="divide-y divide-gray-50 text-xs">
                {highs.map((rec, i) => {
                  const isGold = i === 0;
                  const isSilver = i === 1;
                  const isBronze = i === 2;
                  const rawDisp = formatRawStat(cat.key, rec.rawVal);

                  return (
                    <div
                      key={`rel-high-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                      className="py-2.5 px-2 flex items-center justify-between hover:bg-indigo-50/50 rounded-xl transition gap-3"
                    >
                      <div className="flex items-center gap-2.5 min-w-0">
                        <span className={`w-5 h-5 rounded-lg flex items-center justify-center font-mono font-black text-[10px] shrink-0 ${
                          isGold ? 'bg-amber-400 text-amber-950 ring-1 ring-amber-300' :
                          isSilver ? 'bg-slate-300 text-slate-900' :
                          isBronze ? 'bg-amber-700 text-amber-100' :
                          'bg-gray-100 text-gray-600'
                        }`}>
                          {isGold ? '🥇' : isSilver ? '🥈' : isBronze ? '🥉' : i + 1}
                        </span>
                        <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                          <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                            <span>{rec.owner}</span>
                            <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                            {rec.place === 1 && <span className="text-amber-500 text-[11px]">👑</span>}
                          </div>
                          <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                            • {rec.teamName}
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center gap-3 shrink-0 pl-2">
                        <div className="text-right font-mono">
                          <div className="text-xs text-gray-500">
                            {rawDisp}
                          </div>
                          <div className="text-[9px] text-gray-400">
                            avg: {formatMeanStat(cat.key, rec.seasonMean)}
                          </div>
                        </div>
                        <span
                          className="inline-flex items-center gap-0.5 px-2.5 py-1 rounded-lg text-xs font-black bg-indigo-600 text-white shadow-xs shrink-0 font-mono"
                          title={`Season mean in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)} (${formatDiffStat(cat.key, rec.diffVsAvg)} diff)`}
                        >
                          <span>+{rec.pctVsAvg?.toFixed(1)}%</span>
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* LOWS / RELATIVE DEFICITS COLUMN */}
          {showLows && (
            <div className={`space-y-2 ${twoCols ? 'lg:pl-6 pt-4 lg:pt-0' : ''}`}>
              <div className="flex items-center justify-between px-1">
                <span className="text-xs font-black text-rose-900 flex items-center gap-1.5">
                  <span>⚓</span>
                  <span>Deepest Deficits (% Below League Avg)</span>
                </span>
                <span className="text-[10px] font-mono font-bold text-rose-600">Bottom 5</span>
              </div>

              <div className="divide-y divide-gray-50 text-xs">
                {lows.map((rec, i) => {
                  const rawDisp = formatRawStat(cat.key, rec.rawVal);

                  return (
                    <div
                      key={`rel-low-${cat.key}-${rec.year}-${rec.owner}-${i}`}
                      className="py-2.5 px-2 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition gap-3"
                    >
                      <div className="flex items-center gap-2.5 min-w-0">
                        <span className="w-5 h-5 rounded-lg bg-gray-100 text-gray-500 font-mono font-black text-[10px] flex items-center justify-center shrink-0">
                          {i + 1}
                        </span>
                        <div className="min-w-0 flex items-center gap-1.5 flex-wrap sm:flex-nowrap">
                          <div className="font-black text-gray-900 text-xs sm:text-sm whitespace-nowrap flex items-center gap-1">
                            <span>{rec.owner}</span>
                            <span className="text-gray-400 font-normal font-mono text-xs">({rec.year})</span>
                          </div>
                          <div className="text-[11px] text-gray-400 truncate max-w-[140px] sm:max-w-[220px] hidden sm:inline">
                            • {rec.teamName}
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center gap-3 shrink-0 pl-2">
                        <div className="text-right font-mono">
                          <div className="text-xs text-gray-500">
                            {rawDisp}
                          </div>
                          <div className="text-[9px] text-gray-400">
                            avg: {formatMeanStat(cat.key, rec.seasonMean)}
                          </div>
                        </div>
                        <span
                          className="inline-flex items-center gap-0.5 px-2.5 py-1 rounded-lg text-xs font-black bg-rose-600 text-white shadow-xs shrink-0 font-mono"
                          title={`Season mean in ${rec.year}: ${formatMeanStat(cat.key, rec.seasonMean)} (${formatDiffStat(cat.key, rec.diffVsAvg)} diff)`}
                        >
                          <span>-{Math.abs(rec.pctVsAvg || 0).toFixed(1)}%</span>
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  const renderMarginsCategoryCard = (cat) => {
    const record = highlightsData.categoryGapsRecords?.[cat.key];
    if (!record) return null;
    const { gaps12, gapsLast, totalSeasons } = record;
    const icon = CAT_ICONS[cat.key] || '⚾';
    const isBat = cat.type === 'bat';
    const show12 = marginsGapType === 'all' || marginsGapType === '1st_2nd';
    const showLast = marginsGapType === 'all' || marginsGapType === 'last_2nd_last';
    const twoCols = show12 && showLast;

    return (
      <div
        key={`margin-rec-${cat.key}`}
        className="bg-white border border-gray-200 rounded-3xl p-5 sm:p-6 shadow-sm space-y-4 hover:shadow-md transition"
      >
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-gray-100 pb-3">
          <div className="flex items-center gap-2.5">
            <span className="text-2xl">{icon}</span>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-base font-black text-gray-900">{cat.name}</h3>
                <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                  isBat ? 'bg-amber-100 text-amber-800' : 'bg-indigo-100 text-indigo-800'
                }`}>
                  {isBat ? 'Batting' : 'Pitching'}
                </span>
                <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-amber-50 text-amber-900 border border-amber-300 font-mono">
                  Competitive Chasms
                </span>
              </div>
              <div className="text-[11px] text-gray-400 mt-0.5 flex flex-wrap items-center gap-2">
                <span>Evaluates the gap between 1st & 2nd place, and the deficit between Last & 2nd-to-Last</span>
                <span>•</span>
                <span>{totalSeasons} seasons evaluated</span>
              </div>
            </div>
          </div>
        </div>

        <div className={`grid grid-cols-1 ${twoCols ? 'lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-gray-100' : ''} gap-6`}>
          {show12 && (
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1">
                <span className="text-xs font-black text-amber-900 flex items-center gap-1.5">
                  <span>🥇</span>
                  <span>Largest 1st vs 2nd Runaway Titles</span>
                </span>
                <span className="text-[10px] font-mono font-bold text-amber-700">Top 5</span>
              </div>

              <div className="divide-y divide-gray-50 text-xs">
                {gaps12.map((m, i) => (
                  <div
                    key={`gap12-${cat.key}-${m.year}-${i}`}
                    className="py-2.5 px-2 flex items-center justify-between hover:bg-amber-50/50 rounded-xl transition gap-3"
                  >
                    <div className="flex items-center gap-2.5 min-w-0">
                      <span className="w-5 h-5 rounded-lg bg-amber-100 text-amber-950 font-mono font-black text-[10px] flex items-center justify-center shrink-0">
                        {i + 1}
                      </span>
                      <div className="min-w-0">
                        <div className="font-black text-gray-900 text-xs sm:text-sm flex items-center gap-1">
                          <span>{m.first.owner}</span>
                          <span className="text-gray-400 font-normal font-mono text-xs">({m.year})</span>
                          <span className="text-[11px] text-emerald-600 font-bold">vs {m.second.owner}</span>
                        </div>
                        <div className="text-[11px] text-gray-500 truncate max-w-[220px]">
                          1st: <strong className="text-gray-900 font-mono">{formatRawStat(cat.key, m.first.rawVal)}</strong> • 2nd: <span className="font-mono">{formatRawStat(cat.key, m.second.rawVal)}</span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-2.5 shrink-0 pl-2">
                      <div className="text-right font-mono">
                        <div className="font-black text-amber-800 text-xs sm:text-sm">
                          +{formatDiffStat(cat.key, m.gap12)}
                        </div>
                        <div className="text-[9px] text-gray-400">
                          lead margin
                        </div>
                      </div>
                      <span className="inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-black bg-amber-500 text-white shrink-0 font-mono">
                        +{m.pctGap12.toFixed(1)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {showLast && (
            <div className={`space-y-2 ${twoCols ? 'lg:pl-6 pt-4 lg:pt-0' : ''}`}>
              <div className="flex items-center justify-between px-1">
                <span className="text-xs font-black text-rose-900 flex items-center gap-1.5">
                  <span>🕳️</span>
                  <span>Largest Last vs 2nd-to-Last Basement Abyss</span>
                </span>
                <span className="text-[10px] font-mono font-bold text-rose-700">Bottom 5</span>
              </div>

              <div className="divide-y divide-gray-50 text-xs">
                {gapsLast.map((m, i) => (
                  <div
                    key={`gaplast-${cat.key}-${m.year}-${i}`}
                    className="py-2.5 px-2 flex items-center justify-between hover:bg-rose-50/50 rounded-xl transition gap-3"
                  >
                    <div className="flex items-center gap-2.5 min-w-0">
                      <span className="w-5 h-5 rounded-lg bg-rose-100 text-rose-950 font-mono font-black text-[10px] flex items-center justify-center shrink-0">
                        {i + 1}
                      </span>
                      <div className="min-w-0">
                        <div className="font-black text-gray-900 text-xs sm:text-sm flex items-center gap-1">
                          <span>{m.last.owner}</span>
                          <span className="text-gray-400 font-normal font-mono text-xs">({m.year})</span>
                          <span className="text-[11px] text-rose-600 font-bold">trailing {m.secondLast.owner}</span>
                        </div>
                        <div className="text-[11px] text-gray-500 truncate max-w-[220px]">
                          Last: <strong className="text-rose-900 font-mono">{formatRawStat(cat.key, m.last.rawVal)}</strong> • 2nd-to-Last: <span className="font-mono">{formatRawStat(cat.key, m.secondLast.rawVal)}</span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-2.5 shrink-0 pl-2">
                      <div className="text-right font-mono">
                        <div className="font-black text-rose-700 text-xs sm:text-sm">
                          -{formatDiffStat(cat.key, m.gapLast)}
                        </div>
                        <div className="text-[9px] text-gray-400">
                          abyss deficit
                        </div>
                      </div>
                      <span className="inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-black bg-rose-600 text-white shrink-0 font-mono">
                        -{Math.abs(m.pctGapLast).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-8">
      {/* Era & Roster Controls Header */}
      <div className="bg-gradient-to-r from-amber-500/10 via-amber-500/5 to-transparent border border-amber-300/60 rounded-3xl p-6 shadow-sm flex flex-col md:flex-row items-center justify-between gap-6">
        <div>
          <h2 className="text-xl font-black text-gray-900 flex items-center gap-2">
            <span>⭐</span>
            <span>Era & Roster Adjusted Finishes Engine</span>
          </h2>
          <p className="text-xs text-gray-600 mt-1 max-w-2xl">
            Compare historical dominance across different league sizes (8, 9, 10 teams), roster expansions (22 active starters to 28), and varying MLB ball environments.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-2 bg-white px-3.5 py-2 rounded-2xl border border-gray-300 shadow-xs cursor-pointer hover:border-amber-400 transition text-xs font-black text-gray-800">
            <input
              type="checkbox"
              checked={adjustRoster}
              onChange={e => setAdjustRoster(e.target.checked)}
              className="rounded text-amber-600 focus:ring-amber-500 cursor-pointer"
            />
            <span>Roster Size Adjusted (22➔28)</span>
          </label>

          <label className="flex items-center gap-2 bg-white px-3.5 py-2 rounded-2xl border border-gray-300 shadow-xs cursor-pointer hover:border-amber-400 transition text-xs font-black text-gray-800">
            <input
              type="checkbox"
              checked={adjustMlb}
              onChange={e => setAdjustMlb(e.target.checked)}
              className="rounded text-amber-600 focus:ring-amber-500 cursor-pointer"
            />
            <span>MLB Era Adjusted</span>
          </label>

          <label className="flex items-center gap-2 bg-white px-3.5 py-2 rounded-2xl border border-gray-300 shadow-xs cursor-pointer hover:border-rose-400 transition text-xs font-black text-gray-800" title="Exclude the 60-game COVID-shortened 2020 season from all-time lowlights & counting lows">
            <input
              type="checkbox"
              checked={exclude2020Lowlights}
              onChange={e => setExclude2020Lowlights(e.target.checked)}
              className="rounded text-rose-600 focus:ring-rose-500 cursor-pointer"
            />
            <span className="flex items-center gap-1.5">
              <span>🦠</span>
              <span>Exclude 2020 (COVID)</span>
            </span>
          </label>

          {/* View filter */}
          <div className="flex rounded-xl bg-gray-100 p-1 border border-gray-200 text-xs font-black">
            <button
              onClick={() => setHighlightFilter('all')}
              className={`px-3 py-1 rounded-lg transition ${highlightFilter === 'all' ? 'bg-white text-gray-900 shadow-xs' : 'text-gray-500'}`}
            >
              All
            </button>
            <button
              onClick={() => setHighlightFilter('highlights')}
              className={`px-3 py-1 rounded-lg transition ${highlightFilter === 'highlights' ? 'bg-amber-500 text-white shadow-xs' : 'text-gray-500'}`}
            >
              ⭐ Highlights
            </button>
            <button
              onClick={() => setHighlightFilter('lowlights')}
              className={`px-3 py-1 rounded-lg transition ${highlightFilter === 'lowlights' ? 'bg-rose-500 text-white shadow-xs' : 'text-gray-500'}`}
            >
              ⚠️ Lowlights
            </button>
          </div>
        </div>
      </div>

      {/* ALL-TIME STAT RECORDS (HIGHS, LOWS, RELATIVE DOMINANCE & CATEGORY MARGINS) */}
      <div className="space-y-6">
        <div className="flex flex-col gap-4 border-b border-gray-200 pb-4">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-2 text-sm font-black text-gray-900 uppercase tracking-wider">
                <span>{highlightViewMode === 'totals' ? '📊' : highlightViewMode === 'relative' ? '📈' : '⚡'}</span>
                <span>
                  {highlightViewMode === 'totals' && 'All-Time Category Stat Records (Highs & Lows)'}
                  {highlightViewMode === 'relative' && 'Relative Dominance & Deficits (% vs Season League Average)'}
                  {highlightViewMode === 'margins' && 'Historical Category Margins & Gaps (Blowouts & Punts)'}
                </span>
                <span className="text-xs text-gray-500 font-normal lowercase">
                  {highlightViewMode === 'totals' && (adjustRoster || adjustMlb ? '(Era & roster adjusted)' : '(Raw single-season totals)')}
                  {highlightViewMode === 'relative' && '(Standardized across eras)'}
                  {highlightViewMode === 'margins' && '(1st vs 2nd Leads & Last vs 2nd-Last Deficits)'}
                </span>
              </div>
              <p className="text-xs text-gray-500 mt-1 max-w-3xl">
                {highlightViewMode === 'totals' && (
                  <>All-time single-season benchmarks across all 10 roto categories. Evaluates active seasons for each category (OBP: 2013+, QS: 2014+, SV+H: 2019+). Each record displays its comparative margin against that season's league mean.</>
                )}
                {highlightViewMode === 'relative' && (
                  <>Ranks single-season performances by percentage above (or below) that season's league mean. Standardizes performance across high and low offensive eras to highlight historically unprecedented dominance or basement collapses.</>
                )}
                {highlightViewMode === 'margins' && (
                  <>Measures single-season competitive chasms: runaway category titles (largest margins between 1st and 2nd place) and the basement abyss (largest deficits between last and second-to-last place).</>
                )}
              </p>
            </div>

            {/* Sub-view Switcher */}
            <div className="flex flex-wrap items-center gap-1.5 p-1 bg-gray-100 rounded-2xl border border-gray-200 text-xs font-black self-start md:self-auto shrink-0">
              <button
                type="button"
                onClick={() => setHighlightViewMode('totals')}
                className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${
                  highlightViewMode === 'totals'
                    ? 'bg-white text-gray-900 shadow-xs ring-1 ring-black/5'
                    : 'text-gray-500 hover:text-gray-900'
                }`}
              >
                <span>🏆</span>
                <span>All-Time Totals</span>
              </button>
              <button
                type="button"
                onClick={() => setHighlightViewMode('relative')}
                className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${
                  highlightViewMode === 'relative'
                    ? 'bg-white text-indigo-950 shadow-xs ring-1 ring-black/5'
                    : 'text-gray-500 hover:text-gray-900'
                }`}
              >
                <span>📈</span>
                <span>Relative (% vs Avg)</span>
              </button>
              <button
                type="button"
                onClick={() => setHighlightViewMode('margins')}
                className={`px-3 py-1.5 rounded-xl transition flex items-center gap-1.5 cursor-pointer ${
                  highlightViewMode === 'margins'
                    ? 'bg-white text-amber-950 shadow-xs ring-1 ring-black/5'
                    : 'text-gray-500 hover:text-gray-900'
                }`}
              >
                <span>⚡</span>
                <span>Margins & Gaps</span>
              </button>
            </div>
          </div>

          {/* Second row: Margins Gap Type Switcher & Category filter pills */}
          <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-3 pt-1">
            {highlightViewMode === 'margins' ? (
              <div className="flex items-center gap-1.5 p-1 bg-amber-50 rounded-xl border border-amber-200 text-xs font-bold self-start shrink-0">
                <button
                  type="button"
                  onClick={() => setMarginsGapType('all')}
                  className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                    marginsGapType === 'all'
                      ? 'bg-amber-600 text-white shadow-xs'
                      : 'text-amber-900 hover:bg-amber-100'
                  }`}
                >
                  All Margins
                </button>
                <button
                  type="button"
                  onClick={() => setMarginsGapType('1st_2nd')}
                  className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                    marginsGapType === '1st_2nd'
                      ? 'bg-amber-600 text-white shadow-xs'
                      : 'text-amber-900 hover:bg-amber-100'
                  }`}
                >
                  🥇 1st vs 2nd Leads
                </button>
                <button
                  type="button"
                  onClick={() => setMarginsGapType('last_2nd_last')}
                  className={`px-2.5 py-1 rounded-lg transition cursor-pointer ${
                    marginsGapType === 'last_2nd_last'
                      ? 'bg-rose-600 text-white shadow-xs'
                      : 'text-rose-900 hover:bg-rose-100'
                  }`}
                >
                  ⚠️ Last vs 2nd-Last Deficits
                </button>
              </div>
            ) : (
              <div className="text-xs text-gray-400 font-medium">
                Filter by stat category:
              </div>
            )}

            {/* Category filter pills */}
            <div className="flex flex-wrap items-center gap-1.5 overflow-x-auto py-1">
              <button
                type="button"
                onClick={() => setSelectedStatCategory('ALL')}
                className={`px-2.5 py-1 rounded-lg text-xs font-black transition cursor-pointer ${
                  selectedStatCategory === 'ALL'
                    ? 'bg-blue-600 text-white shadow-xs'
                    : 'bg-white text-gray-700 border border-gray-200 hover:bg-gray-100'
                }`}
              >
                All Stats
              </button>
              {CATEGORIES.map(cat => (
                <button
                  key={cat.key}
                  type="button"
                  onClick={() => setSelectedStatCategory(cat.key)}
                  className={`px-2 py-1 rounded-lg text-xs font-bold transition cursor-pointer flex items-center gap-1 ${
                    selectedStatCategory === cat.key
                      ? (cat.type === 'bat' ? 'bg-amber-600 text-white shadow-xs' : 'bg-indigo-600 text-white shadow-xs')
                      : 'bg-white text-gray-700 border border-gray-200 hover:bg-gray-100'
                  }`}
                >
                  <span>{CAT_ICONS[cat.key]}</span>
                  <span>{cat.label}</span>
                </button>
              ))}

              {/* Legacy Category Pills */}
              <div className="hidden sm:inline-block w-px h-5 bg-gray-300 mx-1"></div>
              {LEGACY_CATEGORIES.map(cat => (
                <button
                  key={cat.key}
                  type="button"
                  onClick={() => setSelectedStatCategory(cat.key)}
                  className={`px-2 py-1 rounded-lg text-xs font-bold transition cursor-pointer flex items-center gap-1 ${
                    selectedStatCategory === cat.key
                      ? 'bg-purple-700 text-white shadow-xs'
                      : 'bg-purple-50 text-purple-800 border border-purple-200 hover:bg-purple-100'
                  }`}
                  title={`Legacy Category: ${cat.name} (${cat.activeYears[0]}${cat.activeYears[0] !== cat.activeYears[1] ? '–' + cat.activeYears[1] : ''})`}
                >
                  <span>{CAT_ICONS[cat.key]}</span>
                  <span>{cat.label} ({cat.activeYears[0] === cat.activeYears[1] ? `'${String(cat.activeYears[0]).slice(2)}` : `'${String(cat.activeYears[0]).slice(2)}–'${String(cat.activeYears[1]).slice(2)}`})</span>
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* SUB-VIEW 1: ALL-TIME TOTALS */}
        {highlightViewMode === 'totals' && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 gap-6">
              {displayedStatCategories.map(cat => renderTotalsCategoryCard(cat, isDisplayingLegacy))}
            </div>

            {selectedStatCategory === 'ALL' && (
              <div className="space-y-4 pt-6 border-t border-gray-200">
                <div className="flex items-center gap-2">
                  <span className="text-lg">🏛️</span>
                  <h3 className="text-sm font-black text-purple-950 uppercase tracking-wider">
                    Discontinued & Legacy Categories
                  </h3>
                  <span className="text-xs text-gray-400 font-normal">
                    (Pre-Modern Scoring Formats)
                  </span>
                </div>
                <div className="grid grid-cols-1 gap-6">
                  {LEGACY_CATEGORIES.map(cat => renderTotalsCategoryCard(cat, true))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* SUB-VIEW 2: RELATIVE DOMINANCE (% vs Season Average) */}
        {highlightViewMode === 'relative' && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 gap-6">
              {displayedStatCategories.map(cat => renderRelativeCategoryCard(cat, isDisplayingLegacy))}
            </div>

            {selectedStatCategory === 'ALL' && (
              <div className="space-y-4 pt-6 border-t border-gray-200">
                <div className="flex items-center gap-2">
                  <span className="text-lg">🏛️</span>
                  <h3 className="text-sm font-black text-purple-950 uppercase tracking-wider">
                    Discontinued & Legacy Categories
                  </h3>
                  <span className="text-xs text-gray-400 font-normal">
                    (Standardized Relative Dominance)
                  </span>
                </div>
                <div className="grid grid-cols-1 gap-6">
                  {LEGACY_CATEGORIES.map(cat => renderRelativeCategoryCard(cat, true))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* SUB-VIEW 3: CATEGORY MARGINS & COMPETITIVE GAPS */}
        {highlightViewMode === 'margins' && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 gap-6">
              {(!isDisplayingLegacy ? displayedStatCategories : []).map(cat => renderMarginsCategoryCard(cat))}
            </div>
          </div>
        )}
      </div>

      {/* HIGHLIGHTS CARDS SECTION (CHAMPIONS, BLOWOUTS, HITTING, PITCHING) */}
      {(highlightFilter === 'all' || highlightFilter === 'highlights') && (
        <div className="space-y-6">
          <div className="flex items-center gap-2 text-sm font-black text-amber-900 uppercase tracking-wider">
            <span>🏆</span>
            <span>All-Time Highlights & Mastery (Historical Best Performances)</span>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* 1. Highest Scoring Finishes */}
            <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
                  <span>🥇</span>
                  <span>Highest Roto Point Totals in History</span>
                </h3>
                <span className="text-[11px] text-gray-400 font-mono">
                  {adjustRoster || adjustMlb ? 'Era Adjusted Pts' : 'Raw Roto Pts'}
                </span>
              </div>
              <div className="divide-y divide-gray-100 text-xs">
                {highlightsData.topScoring.map((rec, i) => (
                  <div key={`high-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-amber-50/40 px-2 rounded-xl transition">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                      <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                      <div>
                        <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                        <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                      </div>
                    </div>
                    <div className="text-right font-mono font-black text-blue-700 text-sm">
                      {rec.isAdjusted ? rec.adjustedPoints.toFixed(1) : rec.points.toFixed(1)} pts
                      {rec.isAdjusted && (
                        <span className="block text-[10px] text-gray-400 font-normal">raw: {rec.points.toFixed(1)}</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* 2. Largest Championship Blowouts */}
            <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
                  <span>🚀</span>
                  <span>Largest Championship Blowouts</span>
                </h3>
                <span className="text-[11px] text-gray-400 font-mono">Point Margin over 2nd Place</span>
              </div>
              <div className="divide-y divide-gray-100 text-xs">
                {highlightsData.blowouts.map((b, i) => (
                  <div key={`blowout-${b.year}`} className="py-2.5 flex items-center justify-between hover:bg-amber-50/40 px-2 rounded-xl transition">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                      <div>
                        <div className="font-black text-gray-900">{b.year} Champion: {b.champ}</div>
                        <div className="text-[10px] text-gray-500">Runner-up: {b.runnerUp} ({b.runnerUpPoints} pts)</div>
                      </div>
                    </div>
                    <div className="text-right font-mono font-black text-emerald-600 text-sm">
                      +{b.margin.toFixed(1)} pts
                      <span className="block text-[10px] text-gray-400 font-normal">Total: {b.champPoints} pts</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* 3. Offensive Masterclasses (Hitting Points) */}
            <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <h3 className="text-sm font-black text-amber-900 flex items-center gap-2">
                  <span>🏏</span>
                  <span>Offensive Masterclasses (Top Hitting Pts)</span>
                </h3>
                <span className="text-[11px] text-gray-400 font-mono">50 Pt Max</span>
              </div>
              <div className="divide-y divide-gray-100 text-xs">
                {highlightsData.topHitting.map((rec, i) => (
                  <div key={`hit-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-amber-50/40 px-2 rounded-xl transition">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                      <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                      <div>
                        <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                        <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                      </div>
                    </div>
                    <div className="text-right font-mono font-black text-amber-700 text-sm">
                      {rec.hittingPoints} pts
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* 4. Pitching Masterclasses (Pitching Points) */}
            <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-3">
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <h3 className="text-sm font-black text-indigo-900 flex items-center gap-2">
                  <span>🎯</span>
                  <span>Pitching Masterclasses (Top Pitching Pts)</span>
                </h3>
                <span className="text-[11px] text-gray-400 font-mono">50 Pt Max</span>
              </div>
              <div className="divide-y divide-gray-100 text-xs">
                {highlightsData.topPitching.map((rec, i) => (
                  <div key={`pitch-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-indigo-50/40 px-2 rounded-xl transition">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                      <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                      <div>
                        <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                        <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                      </div>
                    </div>
                    <div className="text-right font-mono font-black text-indigo-700 text-sm">
                      {rec.pitchingPoints} pts
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* LOWLIGHTS SECTION */}
      {(highlightFilter === 'all' || highlightFilter === 'lowlights') && (
        <div className="space-y-6 pt-4">
          <div className="flex items-center gap-2 text-sm font-black text-rose-900 uppercase tracking-wider">
            <span>⚠️</span>
            <span>All-Time Lowlights (Toughest Historical Campaigns)</span>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* 1. Lowest Scoring Finishes */}
            <div className="bg-white border border-rose-200 rounded-3xl p-5 shadow-sm space-y-3">
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <h3 className="text-sm font-black text-rose-900 flex items-center gap-2">
                  <span>📉</span>
                  <span>Lowest Roto Point Finishes in History</span>
                </h3>
                <span 
                  onClick={() => setExclude2020Lowlights(!exclude2020Lowlights)}
                  className="text-[11px] text-gray-400 font-mono cursor-pointer hover:text-gray-600 transition"
                >
                  {exclude2020Lowlights ? 'Excl. 2020 (COVID)' : 'All Seasons'}
                </span>
              </div>
              <div className="divide-y divide-gray-100 text-xs">
                {highlightsData.lowestScoring.map((rec, i) => (
                  <div key={`low-${rec.year}-${rec.owner}`} className="py-2.5 flex items-center justify-between hover:bg-rose-50/40 px-2 rounded-xl transition">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                      <TeamAvatar team={{ owner: rec.owner }} size="xs" />
                      <div>
                        <div className="font-black text-gray-900">{rec.owner} ({rec.year})</div>
                        <div className="text-[10px] text-gray-500 truncate max-w-[180px]">{rec.teamName}</div>
                      </div>
                    </div>
                    <div className="text-right font-mono font-black text-rose-600 text-sm">
                      {rec.points.toFixed(1)} pts
                      <span className="block text-[10px] text-gray-400 font-normal">#{rec.place} finish</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* 2. Championship Hangovers */}
            <div className="bg-white border border-rose-200 rounded-3xl p-5 shadow-sm space-y-3">
              <div className="flex items-center justify-between border-b border-gray-100 pb-3">
                <h3 className="text-sm font-black text-rose-900 flex items-center gap-2">
                  <span>🛌</span>
                  <span>Championship Hangovers (Title Defenses)</span>
                </h3>
                <span className="text-[11px] text-gray-400 font-mono">Rank Drop Following Championship</span>
              </div>
              <div className="divide-y divide-gray-100 text-xs">
                {highlightsData.hangovers.map((h, i) => (
                  <div key={`hang-${h.champYear}-${h.owner}`} className="py-2.5 flex items-center justify-between hover:bg-rose-50/40 px-2 rounded-xl transition">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-black text-gray-400 w-4">{i + 1}.</span>
                      <div>
                        <div className="font-black text-gray-900">
                          {h.owner}: {h.champYear} Champ ➔ {h.nextYear} (#{h.nextPlace})
                        </div>
                        <div className="text-[10px] text-gray-500 truncate max-w-[200px]">{h.nextTeam}</div>
                      </div>
                    </div>
                    <div className="text-right font-mono font-black text-rose-600 text-sm">
                      ▼ {h.rankDrop} spots
                      <span className="block text-[10px] text-gray-400 font-normal">{h.champPoints} ➔ {h.nextPoints} pts</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
