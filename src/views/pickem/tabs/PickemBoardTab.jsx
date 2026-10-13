/**
 * @file PickemBoardTab.jsx
 * @description Tab 1: Leaderboard podium, complete standings table with prize allocations,
 * and master pick comparison matrix with category filtering.
 */

import React from 'react';
import { evaluateLivePick, getCategoryLabel } from '../services/pickemScoringService';

export default function PickemBoardTab({
  selectedSeason,
  isInProgress,
  showLiveProjections,
  isUpcoming,
  isSummaryEra,
  activeStandings = [],
  seasonOwners = [],
  filteredQuestions = [],
  pickMatrix = {},
  gridCategoryFilter = 'ALL',
  setGridCategoryFilter,
  liveProjectionsData,
  setActiveTab,
}) {
  return (
    <div className="space-y-8 animate-fade-in">
      {/* PODIUM & STANDINGS CARD */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-800 gap-2">
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-xl font-bold text-white flex items-center gap-2">
                <span>🏆</span> {selectedSeason} Standings & Budget Prizes
              </h2>
              {isInProgress && showLiveProjections && (
                <span className="px-2.5 py-0.5 rounded-full text-[11px] font-black bg-amber-400 text-slate-950 uppercase tracking-wider animate-pulse">
                  ⚡ Live Interim Projection
                </span>
              )}
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              {isInProgress && showLiveProjections
                ? 'Projected standings calculated from current live MLB records, division leaders, wild cards, and FanGraphs projected fWAR for awards.'
                : "Top 4 finishers earn additional auction draft dollars for next season's draft: 1st (+$4), 2nd (+$3), 3rd (+$2), 4th (+$1). Ties award the full amount for that place."}
            </p>
          </div>
          {isUpcoming && selectedSeason === 2027 && (
            <span className="text-xs bg-emerald-950 text-emerald-300 px-3 py-1 rounded-full border border-emerald-800/60 font-semibold self-start">
              Picks open – scores pending season results
            </span>
          )}
        </div>

        {activeStandings.length > 0 ? (
          <>
            {/* TOP 3 PODIUM */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 my-6">
              {/* 2nd Place */}
              <div className="order-2 md:order-1 bg-gradient-to-b from-slate-800/60 to-slate-900/90 border border-slate-700/60 rounded-xl p-4 text-center flex flex-col items-center justify-center relative">
                <span className="text-3xl mb-1">🥈</span>
                <span className="text-xs uppercase font-bold tracking-widest text-slate-400">2nd Place</span>
                <div className="text-lg font-black text-white mt-1">
                  {activeStandings[1]?.owner_name || '—'}
                </div>
                <div className="text-2xl font-black text-indigo-300 mt-0.5">
                  {activeStandings[1]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                </div>
                {(activeStandings[1]?.budget_awarded || 0) > 0 && (
                  <span className="mt-2 text-xs font-extrabold px-2.5 py-0.5 rounded-full bg-emerald-950 border border-emerald-500/40 text-emerald-300">
                    +${activeStandings[1].budget_awarded} Draft Budget
                  </span>
                )}
                {activeStandings[1]?.total_hits > 0 && (
                  <span className="text-[10px] text-slate-400 mt-1">
                    {activeStandings[1].total_hits} active hits
                  </span>
                )}
              </div>

              {/* 1st Place (Leader) */}
              <div className="order-1 md:order-2 bg-gradient-to-b from-amber-950/40 via-slate-800/90 to-slate-900 border-2 border-amber-500/60 rounded-xl p-5 text-center flex flex-col items-center justify-center relative shadow-lg shadow-amber-950/20 transform md:-translate-y-2">
                <div className="absolute -top-3 bg-amber-500 text-slate-950 text-[10px] uppercase font-black px-3 py-0.5 rounded-full tracking-wider shadow">
                  {isInProgress && showLiveProjections ? 'CURRENT LEADER' : 'CHAMPION'}
                </div>
                <span className="text-4xl mb-1 mt-1">🥇</span>
                <span className="text-xs uppercase font-bold tracking-widest text-amber-400">1st Place</span>
                <div className="text-xl font-black text-white mt-1">
                  {activeStandings[0]?.owner_name || '—'}
                </div>
                <div className="text-3xl font-black text-amber-300 mt-0.5">
                  {activeStandings[0]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                </div>
                {(activeStandings[0]?.budget_awarded || 0) > 0 && (
                  <span className="mt-2 text-xs font-black px-3 py-1 rounded-full bg-amber-400 text-slate-950 shadow">
                    +${activeStandings[0].budget_awarded} Draft Budget
                  </span>
                )}
                {activeStandings[0]?.total_hits > 0 && (
                  <span className="text-[10px] text-amber-200/80 mt-1 font-medium">
                    {activeStandings[0].total_hits} active hits
                  </span>
                )}
              </div>

              {/* 3rd Place */}
              <div className="order-3 bg-gradient-to-b from-amber-950/20 to-slate-900/90 border border-amber-900/40 rounded-xl p-4 text-center flex flex-col items-center justify-center relative">
                <span className="text-3xl mb-1">🥉</span>
                <span className="text-xs uppercase font-bold tracking-widest text-amber-600">3rd Place</span>
                <div className="text-lg font-black text-white mt-1">
                  {activeStandings[2]?.owner_name || '—'}
                </div>
                <div className="text-2xl font-black text-indigo-300 mt-0.5">
                  {activeStandings[2]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                </div>
                {(activeStandings[2]?.budget_awarded || 0) > 0 && (
                  <span className="mt-2 text-xs font-extrabold px-2.5 py-0.5 rounded-full bg-emerald-950 border border-emerald-500/40 text-emerald-300">
                    +${activeStandings[2].budget_awarded} Draft Budget
                  </span>
                )}
                {activeStandings[2]?.total_hits > 0 && (
                  <span className="text-[10px] text-slate-400 mt-1">
                    {activeStandings[2].total_hits} active hits
                  </span>
                )}
              </div>
            </div>

            {/* FULL STANDINGS TABLE */}
            <div className="overflow-x-auto mt-4">
              <table className="w-full text-left text-sm border-collapse">
                <thead>
                  <tr className="border-b border-slate-800 text-xs uppercase font-bold text-slate-400">
                    <th className="py-2.5 px-3">Rank</th>
                    <th className="py-2.5 px-3">Owner</th>
                    <th className="py-2.5 px-3 text-right">
                      {isInProgress && showLiveProjections ? 'Projected Pts' : 'Total Points'}
                    </th>
                    <th className="py-2.5 px-3 text-center">Prize Money</th>
                    {isSummaryEra ? (
                      <>
                        <th className="py-2.5 px-3 text-right text-xs">NL Play</th>
                        <th className="py-2.5 px-3 text-right text-xs">AL Play</th>
                        <th className="py-2.5 px-3 text-right text-xs">Postseason</th>
                        <th className="py-2.5 px-3 text-right text-xs">NL Award</th>
                        <th className="py-2.5 px-3 text-right text-xs">AL Award</th>
                        <th className="py-2.5 px-3 text-right text-xs">Wins/Loss</th>
                      </>
                    ) : isInProgress && showLiveProjections ? (
                      <th className="py-2.5 px-3 text-slate-400">Current Active Hits (YTD Standings & WAR)</th>
                    ) : (
                      <th className="py-2.5 px-3 text-slate-400">Performance Status</th>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {activeStandings.map((sc) => {
                    const place = sc.place || 99;
                    const isPodium = place <= 3;
                    const budget = sc.budget_awarded || 0;
                    const cats = sc.category_scores || {};

                    return (
                      <tr
                        key={sc.id || sc.owner_name}
                        className={`hover:bg-slate-800/40 transition-colors ${
                          place === 1 ? 'bg-amber-950/10' : isPodium ? 'bg-indigo-950/10' : ''
                        }`}
                      >
                        <td className="py-2.5 px-3 font-extrabold">
                          {place === 1 ? '🥇 1' : place === 2 ? '🥈 2' : place === 3 ? '🥉 3' : `#${place}`}
                        </td>
                        <td className="py-2.5 px-3 font-bold text-white flex items-center gap-2">
                          <span>{sc.owner_name}</span>
                          {sc.team_id && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-indigo-300 font-normal">
                              Team {sc.team_id}
                            </span>
                          )}
                        </td>
                        <td className="py-2.5 px-3 text-right font-black text-indigo-300 text-base">
                          {sc.total_points ?? 0}
                        </td>
                        <td className="py-2.5 px-3 text-center">
                          {budget > 0 ? (
                            <span className="px-2.5 py-1 rounded-full text-xs font-black bg-emerald-950/80 border border-emerald-500/50 text-emerald-300 inline-block shadow-xs">
                              +${budget}
                            </span>
                          ) : (
                            <span className="text-slate-600 text-xs font-medium">—</span>
                          )}
                        </td>

                        {isSummaryEra ? (
                          <>
                            <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.nl_playoffs ?? cats['NL Playoffs'] ?? '—'}</td>
                            <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.al_playoffs ?? cats['AL Playoffs'] ?? '—'}</td>
                            <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.playoff_result ?? cats['Playoff Result'] ?? '—'}</td>
                            <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.nl_award ?? cats['NL Award'] ?? '—'}</td>
                            <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.al_award ?? cats['AL Award'] ?? '—'}</td>
                            <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats['most_wins/losses'] ?? cats['Most Wins/Losses'] ?? '—'}</td>
                          </>
                        ) : isInProgress && showLiveProjections ? (
                          <td className="py-2.5 px-3 text-xs">
                            <div className="flex flex-wrap items-center gap-1.5 max-w-lg">
                              {(sc.hits || []).slice(0, 5).map((h, i) => (
                                <span
                                  key={i}
                                  className={`px-2 py-0.5 rounded text-[11px] font-semibold ${
                                    h.hit_type === 'EXACT'
                                      ? 'bg-emerald-950/80 border border-emerald-500/40 text-emerald-300'
                                      : 'bg-sky-950/80 border border-sky-500/40 text-sky-300'
                                  }`}
                                  title={`${h.question_label}: ${h.pick_value}`}
                                >
                                  {h.pick_value} (+{h.points})
                                </span>
                              ))}
                              {(sc.hits || []).length > 5 && (
                                <span className="text-[10px] text-slate-400 font-bold">
                                  +{(sc.hits || []).length - 5} more
                                </span>
                              )}
                            </div>
                          </td>
                        ) : (
                          <td className="py-2.5 px-3 text-xs text-slate-400">
                            {place === 1 ? '🏆 Champion' : place <= 4 ? '✨ In the Money' : 'Out of the Money'}
                          </td>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <div className="text-center py-10 text-slate-400">
            <p className="text-sm">No scores recorded yet for {selectedSeason}.</p>
            <button
              onClick={() => setActiveTab?.('entry')}
              className="mt-3 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold rounded-lg transition-colors cursor-pointer"
            >
              Submit picks for this season →
            </button>
          </div>
        )}
      </div>

      {/* MASTER PICK MATRIX (GRID) */}
      {!isSummaryEra && (
        <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
          <div className="flex flex-col md:flex-row md:items-center justify-between pb-4 border-b border-slate-800 gap-3">
            <div>
              <h2 className="text-xl font-bold text-white flex items-center gap-2">
                <span>📋</span> {selectedSeason} Master Pick Matrix
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">
                {isInProgress && showLiveProjections
                  ? 'Real-time side-by-side evaluation against live 2026 MLB division leaders and FanGraphs projected fWAR leaders.'
                  : "Side-by-side comparison of every owner's predictions and official outcomes."}
              </p>
            </div>

            {/* Filter by category */}
            <div className="flex items-center gap-1.5 overflow-x-auto text-xs">
              {['ALL', 'division', 'wild_card', 'playoff_result', 'award', 'extremes'].map(cat => (
                <button
                  key={cat}
                  onClick={() => setGridCategoryFilter(cat)}
                  className={`px-2.5 py-1 rounded-lg font-medium whitespace-nowrap transition-colors cursor-pointer ${
                    gridCategoryFilter === cat
                      ? 'bg-indigo-600 text-white font-bold'
                      : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
                  }`}
                >
                  {cat === 'ALL' ? 'All Questions' : getCategoryLabel(cat)}
                </button>
              ))}
            </div>
          </div>

          {/* Pick Matrix Table */}
          <div className="overflow-x-auto mt-4 max-h-[750px] overflow-y-auto scrollbar-thin">
            <table className="w-full text-left text-xs border-collapse">
              <thead className="sticky top-0 bg-slate-950 z-20 shadow-md">
                <tr className="border-b border-slate-800 text-slate-300 font-bold uppercase tracking-wider">
                  <th className="py-3 px-3 min-w-[200px]">Question</th>
                  <th className="py-3 px-3 min-w-[160px] bg-slate-900/90 text-emerald-300 border-x border-slate-800">
                    {isInProgress && showLiveProjections ? '⚡ YTD Leader (MLB / WAR)' : 'Actual Outcome'}
                  </th>
                  {seasonOwners.map(o => (
                    <th key={o.name} className="py-3 px-3 min-w-[140px] text-center">
                      <span className="font-extrabold text-white text-sm block">{o.name}</span>
                      {o.team_id && <span className="text-[10px] text-slate-400 font-normal">Team {o.team_id}</span>}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {filteredQuestions.map(q => {
                  const outcome = q.correct_answer || q.actual_answer;
                  const hasActual = outcome && outcome.trim() !== '';

                  // Live info for 2026
                  const liveCat = liveProjectionsData?.categories?.[q.question_key];

                  return (
                    <tr key={q.id} className="hover:bg-slate-800/30 transition-colors">
                      {/* Question Column */}
                      <td className="py-2.5 px-3">
                        <div className="font-bold text-white text-sm">{q.question_label || q.question_text}</div>
                        <div className="flex items-center gap-2 mt-0.5">
                          <span className="text-[10px] px-1.5 py-0.2 rounded bg-indigo-950/80 border border-indigo-700/50 text-indigo-300 font-semibold">
                            {getCategoryLabel(q.category)}
                          </span>
                          <span className="text-[10px] text-slate-400 font-medium">
                            {q.points_exact || 3} pts
                          </span>
                        </div>
                      </td>

                      {/* Actual Answer / Live Leader Column */}
                      <td className="py-2.5 px-3 bg-slate-950/40 border-x border-slate-800">
                        {hasActual ? (
                          <span className="font-bold text-emerald-300 bg-emerald-950/80 px-2 py-1 rounded border border-emerald-600/40 block text-center">
                            {outcome}
                          </span>
                        ) : isInProgress && showLiveProjections && liveCat ? (
                          <div className="text-center space-y-0.5">
                            <div className="font-bold text-amber-300 text-xs flex items-center justify-center gap-1">
                              <span>⚡</span>
                              <span>{liveCat.leader}</span>
                            </div>
                            <div className="text-[10px] text-slate-400 font-mono">
                              {liveCat.stat}
                            </div>
                          </div>
                        ) : (
                          <span className="text-slate-600 italic block text-center">Pending</span>
                        )}
                      </td>

                      {/* Owner Pick Columns */}
                      {seasonOwners.map(o => {
                        const pick = pickMatrix[q.id]?.[o.name.toLowerCase()];
                        const pickVal = pick?.pick_value;

                        if (!pickVal) {
                          return (
                            <td key={o.name} className="py-2.5 px-3 text-center text-slate-600 italic">
                              —
                            </td>
                          );
                        }

                        const isExact = pick?.is_correct;
                        const pts = pick?.points_awarded ?? pick?.points_earned ?? 0;
                        const isCrossover = !isExact && pts > 0;

                        // If live in progress mode
                        const liveEval = isInProgress && showLiveProjections
                          ? evaluateLivePick(q.question_key, pickVal, liveProjectionsData)
                          : null;

                        return (
                          <td key={o.name} className="py-2.5 px-3 text-center">
                            {liveEval ? (
                              <div
                                className={`p-2 rounded-lg border transition-all text-xs ${
                                  liveEval.status === 'EXACT'
                                    ? 'bg-emerald-950/80 border-emerald-500/60 text-emerald-200 font-bold shadow-xs'
                                    : liveEval.status === 'CROSSOVER'
                                    ? 'bg-sky-950/80 border-sky-500/60 text-sky-200 font-semibold'
                                    : liveEval.status === 'CONTENDER'
                                    ? 'bg-amber-950/40 border-amber-500/40 text-amber-200'
                                    : 'bg-slate-900/60 border-slate-800 text-slate-400'
                                }`}
                              >
                                <div>{pickVal}</div>
                                <div
                                  className={`text-[10px] font-black mt-0.5 ${
                                    liveEval.status === 'EXACT'
                                      ? 'text-emerald-400'
                                      : liveEval.status === 'CROSSOVER'
                                      ? 'text-sky-400'
                                      : liveEval.status === 'CONTENDER'
                                      ? 'text-amber-400'
                                      : 'text-slate-500'
                                  }`}
                                >
                                  {liveEval.label}
                                </div>
                              </div>
                            ) : (
                              <div
                                className={`p-2 rounded-lg border transition-all text-xs ${
                                  isExact
                                    ? 'bg-emerald-950/70 border-emerald-500/50 text-emerald-200 font-bold shadow-xs'
                                    : isCrossover
                                    ? 'bg-sky-950/70 border-sky-500/50 text-sky-200 font-semibold'
                                    : hasActual
                                    ? 'bg-slate-950/40 border-slate-800 text-slate-400 line-through opacity-75'
                                    : 'bg-slate-800/40 border-slate-700/60 text-slate-200 font-medium'
                                }`}
                              >
                                <div>{pickVal}</div>
                                {isExact && (
                                  <div className="text-[10px] text-emerald-400 font-black mt-0.5">
                                    ✓ +{pts} pts
                                  </div>
                                )}
                                {isCrossover && (
                                  <div className="text-[10px] text-sky-400 font-black mt-0.5">
                                    ✦ +{pts} pts (Playoff Spot)
                                  </div>
                                )}
                              </div>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
