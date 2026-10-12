// src/views/history/tabs/HistoryStandingsTab.jsx
import React from 'react';
import TeamAvatar from '../../../components/TeamAvatar';
import {
  AVAILABLE_SEASONS,
  CATEGORIES,
  getDisplayRawStat,
} from '../services/historyFinishesService';

export default function HistoryStandingsTab({
  selectedYear,
  setSelectedYear,
  selectedOwner,
  setSelectedOwner,
  allOwners,
  currentCategoriesOnly,
  setCurrentCategoriesOnly,
  showRawStats,
  setShowRawStats,
  adjustRoster,
  setAdjustRoster,
  adjustMlb,
  setAdjustMlb,
  selectedSeasonChampion,
  sortedFinishes,
  handleSort,
  onOwnerClick,
  getOwnerTeamObj,
}) {
  return (
    <div className="space-y-6">
      {/* Filter Bar */}
      <div className="bg-white border border-gray-200 rounded-2xl p-4 shadow-sm flex flex-wrap items-center justify-between gap-4">
        <div className="flex flex-wrap items-center gap-3">
          {/* Year Selector */}
          <div className="flex items-center gap-2">
            <span className="text-xs font-black text-gray-500 uppercase">Season:</span>
            <select
              value={selectedYear}
              onChange={e => {
                const yr = e.target.value;
                setSelectedYear(yr);
                if (yr === 'ALL') {
                  setShowRawStats(true);
                }
                if (yr !== 'ALL' && parseInt(yr, 10) < 2019 && currentCategoriesOnly) {
                  setCurrentCategoriesOnly(false);
                }
              }}
              className="bg-gray-50 border border-gray-300 text-gray-800 text-xs font-black rounded-xl px-3 py-2 focus:ring-2 focus:ring-blue-500 cursor-pointer shadow-xs"
            >
              {AVAILABLE_SEASONS.filter(y => !currentCategoriesOnly || y === 'ALL' || y >= 2019).map(y => (
                <option key={y} value={y}>
                  {y === 'ALL' 
                    ? (currentCategoriesOnly ? '🌟 Modern Era (2019–2026)' : '🌟 All Seasons (2012–2026)')
                    : y === 2026 ? '⚾ 2026 (Regular Season)' : `🏛️ ${y} Season`}
                </option>
              ))}
            </select>
          </div>

          {/* Owner Filter */}
          <div className="flex items-center gap-2">
            <span className="text-xs font-black text-gray-500 uppercase">Owner:</span>
            <select
              value={selectedOwner}
              onChange={e => setSelectedOwner(e.target.value)}
              className="bg-gray-50 border border-gray-300 text-gray-800 text-xs font-black rounded-xl px-3 py-2 focus:ring-2 focus:ring-blue-500 cursor-pointer shadow-xs"
            >
              <option value="ALL">👥 All Owners</option>
              {allOwners.map(o => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
        </div>

        {/* Standings Controls & Toggles */}
        <div className="flex flex-wrap items-center gap-2.5">
          {/* Current Categories Only Toggle */}
          <button
            type="button"
            onClick={() => {
              setCurrentCategoriesOnly(prev => {
                const next = !prev;
                if (next && selectedYear !== 'ALL' && parseInt(selectedYear, 10) < 2019) {
                  setSelectedYear('ALL');
                }
                return next;
              });
            }}
            className={`px-3 py-1.5 rounded-xl text-xs font-black flex items-center gap-1.5 transition cursor-pointer shadow-xs ${
              currentCategoriesOnly
                ? 'bg-emerald-600 text-white shadow-md ring-1 ring-emerald-400'
                : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'
            }`}
            title="Only include seasons (2019–2026) using the current 10 stat categories (OBP, QS, SV+H)"
          >
            <span>🎯</span>
            <span>{currentCategoriesOnly ? 'Current Categories Only (2019–2026)' : 'Current Categories (2019+)'}</span>
          </button>

          <button
            type="button"
            onClick={() => setShowRawStats(prev => !prev)}
            className={`px-3 py-1.5 rounded-xl text-xs font-black flex items-center gap-1.5 transition cursor-pointer shadow-xs ${
              showRawStats
                ? 'bg-blue-600 text-white shadow-md ring-1 ring-blue-400'
                : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'
            }`}
            title="Toggle displaying the underlying raw statistics underneath the roto point ranks"
          >
            <span>🔢</span>
            <span>{showRawStats ? 'Hide Raw Stats' : 'Show Raw Stats'}</span>
          </button>

          <label className="flex items-center gap-1.5 text-xs font-bold text-gray-600 cursor-pointer bg-gray-50 px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-100 transition">
            <input
              type="checkbox"
              checked={adjustRoster}
              onChange={e => setAdjustRoster(e.target.checked)}
              className="rounded text-blue-600 focus:ring-blue-500 cursor-pointer"
            />
            <span>Roster Adjusted (22➔28)</span>
          </label>
          <label className="flex items-center gap-1.5 text-xs font-bold text-gray-600 cursor-pointer bg-gray-50 px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-100 transition">
            <input
              type="checkbox"
              checked={adjustMlb}
              onChange={e => setAdjustMlb(e.target.checked)}
              className="rounded text-blue-600 focus:ring-blue-500 cursor-pointer"
            />
            <span>MLB Era Adjusted</span>
          </label>
        </div>
      </div>

      {/* Season Hero Spotlight (when a single season is selected) */}
      {selectedSeasonChampion && (
        <div className="bg-gradient-to-br from-amber-500/10 via-amber-500/5 to-transparent border border-amber-400/40 rounded-3xl p-6 shadow-md flex flex-col md:flex-row items-center justify-between gap-6">
          <div className="flex items-center gap-4">
            <div className="w-16 h-16 rounded-2xl bg-amber-500/20 border-2 border-amber-400 flex items-center justify-center text-3xl shadow-inner">
              🏆
            </div>
            <div>
              <div className="text-xs font-black text-amber-700 uppercase tracking-wider">
                {selectedSeasonChampion.year} League Champion
              </div>
              <div className="text-2xl font-black text-gray-900 flex items-center gap-2">
                <span>{selectedSeasonChampion.champ.owner}</span>
                <span className="text-base text-gray-500 font-normal">({selectedSeasonChampion.champ.teamName})</span>
              </div>
              <div className="text-xs text-gray-600 mt-1">
                Recorded <strong className="text-amber-800 font-black">{selectedSeasonChampion.champ.points} roto points</strong> across {selectedSeasonChampion.totalTeams} teams
                {selectedSeasonChampion.margin && (
                  <span> • Won by <strong className="text-emerald-700 font-bold">+{selectedSeasonChampion.margin} pts</strong></span>
                )}
              </div>
            </div>
          </div>

          {/* Podium summary */}
          <div className="flex items-center gap-4 text-xs font-bold">
            {selectedSeasonChampion.runnerUp && (
              <div className="bg-white/80 border border-gray-200 rounded-xl px-3 py-2 shadow-xs">
                <span className="text-gray-400 uppercase text-[10px] block font-black">🥈 Runner-Up</span>
                <span className="text-gray-800 font-black">{selectedSeasonChampion.runnerUp.owner}</span>
                <span className="text-gray-500 ml-1">({selectedSeasonChampion.runnerUp.points} pts)</span>
              </div>
            )}
            {selectedSeasonChampion.thirdPlace && (
              <div className="bg-white/80 border border-gray-200 rounded-xl px-3 py-2 shadow-xs">
                <span className="text-gray-400 uppercase text-[10px] block font-black">🥉 3rd Place</span>
                <span className="text-gray-800 font-black">{selectedSeasonChampion.thirdPlace.owner}</span>
                <span className="text-gray-500 ml-1">({selectedSeasonChampion.thirdPlace.points} pts)</span>
              </div>
            )}
          </div>
        </div>
      )}

      {/* 2026 In-Progress Notice */}
      {selectedYear === '2026' && (
        <div className="bg-gradient-to-br from-blue-500/10 via-indigo-500/5 to-transparent border border-blue-400/30 rounded-3xl p-6 shadow-sm flex items-center gap-4">
          <div className="w-14 h-14 rounded-2xl bg-blue-500/20 border-2 border-blue-400 flex items-center justify-center text-3xl shadow-inner shrink-0">
            ⚾
          </div>
          <div>
            <div className="text-xs font-black text-blue-700 uppercase tracking-wider flex items-center gap-2">
              <span>2026 Regular Season In Progress</span>
              <span className="inline-block px-2 py-0.5 rounded-full bg-blue-100 text-blue-800 text-[10px] font-bold">Active Season</span>
            </div>
            <div className="text-lg font-black text-gray-900 mt-0.5">
              Standings reflect Week 23 regular season table.
            </div>
            <div className="text-xs text-gray-600 mt-1">
              The 2026 League Champion will be crowned upon completion of the postseason playoffs.
            </div>
          </div>
        </div>
      )}

      {/* Standings Table */}
      <div className="bg-white border border-gray-200 rounded-3xl shadow-xl overflow-hidden">
        <div className="p-4 sm:p-5 border-b border-gray-100 flex items-center justify-between bg-gray-50/70">
          <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
            <span>📋</span>
            <span>Standings & Category Breakdown</span>
            <span className="text-xs text-gray-500 font-normal">
              ({sortedFinishes.length} finishes found {currentCategoriesOnly && '• 2019–2026 current format'})
            </span>
          </h2>
          <div className="text-xs text-gray-500 hidden sm:block">
            Click headers to sort • Click owner to view profile
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full text-xs text-left">
            <thead className="bg-gray-100/90 text-gray-700 font-black uppercase text-[10px] tracking-wider border-b border-gray-200 select-none">
              <tr>
                <th onClick={() => handleSort('year')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition">Year</th>
                <th onClick={() => handleSort('place')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition">Place</th>
                <th onClick={() => handleSort('owner')} className="py-3 px-4 cursor-pointer hover:bg-gray-200 transition">Owner & Team</th>
                <th onClick={() => handleSort('points')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition text-right bg-blue-50/60 text-blue-900">
                  {adjustRoster || adjustMlb ? 'Adj Pts' : 'Roto Pts'}
                </th>
                <th onClick={() => handleSort('hitting')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition text-center">Hit Pts</th>
                <th onClick={() => handleSort('pitching')} className="py-3 px-3 cursor-pointer hover:bg-gray-200 transition text-center">Pitch Pts</th>
                {CATEGORIES.map(cat => {
                  let legacySub = null;
                  if (!currentCategoriesOnly) {
                    if (cat.key === 'OBP') legacySub = "BA in '12";
                    else if (cat.key === 'QS') legacySub = "W in '12–'13";
                    else if (cat.key === 'SVHLD') legacySub = "SV in '12–'18";
                  }

                  return (
                    <th
                      key={cat.key}
                      onClick={() => handleSort(cat.key)}
                      className={`py-3 px-2 text-center cursor-pointer hover:bg-gray-200 transition ${
                        cat.type === 'bat' ? 'bg-amber-50/40 text-amber-900' : 'bg-indigo-50/40 text-indigo-900'
                      }`}
                      title={cat.name + (cat.legacyNote ? ` • ${cat.legacyNote}` : '')}
                    >
                      <div>{cat.label}</div>
                      {showRawStats && (
                        <div className="text-[8px] font-normal text-gray-500 font-sans normal-case tracking-normal mt-0.5">
                          {legacySub ? (
                            <span className="text-amber-700 font-bold" title={cat.legacyNote}>{legacySub}</span>
                          ) : (
                            'pts / raw'
                          )}
                        </div>
                      )}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {sortedFinishes.map((row, idx) => {
                const isChamp = row.place === 1 && row.year !== 2026;
                const isPodium = row.place <= 3 && row.year !== 2026;
                const dispPoints = row.isAdjusted ? row.adjustedPoints.toFixed(1) : row.points.toFixed(1);

                // Split bar percentage
                const totalCatPts = (row.hittingPoints + row.pitchingPoints) || 1;
                const hitPct = Math.round((row.hittingPoints / totalCatPts) * 100);

                return (
                  <tr
                    key={`${row.year}-${row.owner}-${idx}`}
                    className={`transition-colors hover:bg-blue-50/40 ${
                      isChamp ? 'bg-amber-50/25 font-bold' : isPodium ? 'bg-slate-50/50' : ''
                    }`}
                  >
                    {/* Year */}
                    <td className="py-3 px-3 font-mono font-bold text-gray-500">
                      {row.year}
                    </td>

                    {/* Place Badge */}
                    <td className="py-3 px-3 font-black">
                      <span className={`inline-flex items-center justify-center w-7 h-7 rounded-xl text-xs font-black shadow-xs ${
                        isChamp ? 'bg-amber-400 text-amber-950 ring-2 ring-amber-300' :
                        (isPodium && row.place === 2) ? 'bg-slate-300 text-slate-900' :
                        (isPodium && row.place === 3) ? 'bg-amber-700 text-amber-100' :
                        'bg-gray-100 text-gray-600'
                      }`}>
                        {isChamp ? '🥇' : (isPodium && row.place === 2) ? '🥈' : (isPodium && row.place === 3) ? '🥉' : `#${row.place}`}
                      </span>
                    </td>

                    {/* Owner & Team */}
                    <td className="py-3 px-4">
                      <div
                        onClick={() => onOwnerClick && onOwnerClick(getOwnerTeamObj(row.owner))}
                        className="flex items-center gap-2.5 cursor-pointer group"
                      >
                        <TeamAvatar team={{ owner: row.owner }} size="sm" className="group-hover:scale-105 transition-transform" />
                        <div>
                          <div className="font-black text-gray-900 group-hover:text-blue-600 transition flex items-center gap-1.5">
                            <span>{row.owner}</span>
                            {isChamp && <span className="text-[10px] text-amber-500">🏆</span>}
                          </div>
                          <div className="text-[11px] text-gray-500 truncate max-w-[180px] sm:max-w-[240px]">
                            {row.teamName}
                          </div>
                        </div>
                      </div>
                    </td>

                    {/* Points (Total Roto) */}
                    <td className="py-3 px-3 text-right font-mono font-black text-sm text-blue-700 bg-blue-50/40">
                      {dispPoints}
                      {row.isAdjusted && (
                        <span className="block text-[10px] text-gray-400 font-normal">raw: {row.points.toFixed(1)}</span>
                      )}
                    </td>

                    {/* Hitting Points */}
                    <td className="py-3 px-3 text-center font-mono font-bold text-amber-800">
                      {row.hittingPoints}
                    </td>

                    {/* Pitching Points */}
                    <td className="py-3 px-3 text-center font-mono font-bold text-indigo-800">
                      {row.pitchingPoints}
                      {/* Mini visual split bar */}
                      <div className="w-12 mx-auto mt-1 h-1 rounded-full bg-gray-200 overflow-hidden flex" title={`${hitPct}% Hit / ${100-hitPct}% Pitch`}>
                        <div className="bg-amber-500 h-full" style={{ width: `${hitPct}%` }}></div>
                        <div className="bg-indigo-500 h-full" style={{ width: `${100-hitPct}%` }}></div>
                      </div>
                    </td>

                    {/* 10 Category Breakdown */}
                    {CATEGORIES.map(cat => {
                      const val = row.categoryRanks[cat.key];
                      const maxCatVal = Math.max(...sortedFinishes.filter(f => f.year === row.year).map(f => f.categoryRanks[cat.key] || 0));
                      const isLeader = val && val === maxCatVal && val > 0;
                      const { text: rawFormatted, badge, title } = getDisplayRawStat(cat.key, row);
                      return (
                        <td
                          key={cat.key}
                          className={`py-2 px-2 text-center font-mono ${
                            isLeader ? 'bg-emerald-100/70 text-emerald-950 font-black' : 'text-gray-700'
                          }`}
                        >
                          <div className="font-bold text-xs">{val !== undefined ? val : '-'}</div>
                          {showRawStats && (
                            <div
                              className="text-[10px] font-mono font-medium text-slate-500 mt-0.5 whitespace-nowrap flex items-center justify-center gap-1"
                              title={title || undefined}
                            >
                              <span>{rawFormatted}</span>
                              {badge && (
                                <span className="text-[9px] px-1 py-0.2 rounded bg-amber-100 text-amber-900 font-sans font-bold border border-amber-300">
                                  {badge}
                                </span>
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
    </div>
  );
}
