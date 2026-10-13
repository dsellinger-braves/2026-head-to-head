import React from 'react';
import { getDateFromPeriodId } from '../../../schedule';

export default function SimulatorRosterWorkbench({
  filteredPlayers = [],
  playerFilter,
  handleFilterTabClick,
  sortBy,
  setSortBy,
  searchQuery,
  setSearchQuery,
  excludedPlayerIds,
  excludedOutingKeys,
  expandedPlayerIds,
  togglePlayerExclusion,
  toggleOutingExclusion,
  toggleAccordion,
  selectedSeason,
  onPlayerClick
}) {
  return (
    <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4">
      {/* Filter / Sort bar */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-gray-100 pb-3">
        <div>
          <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
            <span>📋</span>
            <span>Roster & Outing Exclusion Workbench</span>
          </h3>
          <p className="text-xs text-gray-400 mt-0.5">
            Uncheck a player to drop them entirely, or expand to drop individual games/spot-starts.
          </p>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {/* Position filter pill */}
          <div className="flex items-center gap-1 bg-gray-100 p-1 rounded-xl text-xs">
            <button
              type="button"
              onClick={() => handleFilterTabClick('pitchers')}
              className={`px-2.5 py-1 font-bold rounded-lg transition cursor-pointer ${
                playerFilter === 'pitchers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'
              }`}
            >
              Pitchers
            </button>
            <button
              type="button"
              onClick={() => handleFilterTabClick('streamers')}
              className={`px-2.5 py-1 font-bold rounded-lg transition cursor-pointer ${
                playerFilter === 'streamers' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'
              }`}
            >
              Streamers (≤4)
            </button>
            <button
              type="button"
              onClick={() => handleFilterTabClick('batters')}
              className={`px-2.5 py-1 font-bold rounded-lg transition cursor-pointer ${
                playerFilter === 'batters' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'
              }`}
            >
              Batters
            </button>
            <button
              type="button"
              onClick={() => handleFilterTabClick('all')}
              className={`px-2.5 py-1 font-bold rounded-lg transition cursor-pointer ${
                playerFilter === 'all' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600'
              }`}
            >
              All
            </button>
          </div>

          {/* Sort by dropdown */}
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="px-2.5 py-1 text-xs border border-gray-200 rounded-xl bg-gray-50 focus:bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 font-bold text-gray-700 cursor-pointer"
          >
            {playerFilter === 'batters' ? (
              <>
                <option value="obp_drag">Sort: Worst OBP Drag</option>
                <option value="lowest_obp">Sort: Lowest OBP (Min 15 PA)</option>
                <option value="pa">Sort: Plate Appearances</option>
                <option value="apps">Sort: Appearances</option>
                <option value="name">Sort: Name</option>
              </>
            ) : (
              <>
                <option value="era_damage">Sort: ERA Damage</option>
                <option value="er">Sort: Most ER</option>
                <option value="ip">Sort: Innings Pitched</option>
                <option value="apps">Sort: Appearances</option>
                <option value="name">Sort: Name</option>
              </>
            )}
          </select>

          {/* Search */}
          <input
            type="text"
            placeholder="Search player..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="px-2.5 py-1 text-xs border border-gray-200 rounded-xl bg-gray-50 focus:bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 w-36"
          />
        </div>
      </div>

      {/* Players List Table */}
      <div className="divide-y divide-gray-100 text-xs">
        {filteredPlayers.length === 0 ? (
          <div className="py-8 text-center text-gray-400">
            No active players found matching the current filters.
          </div>
        ) : (
          filteredPlayers.map(p => {
            const isFullyExcluded = excludedPlayerIds.has(p.playerId);
            const isExpanded = expandedPlayerIds.has(p.playerId);

            const excludedOutingsCount = p.outings.filter(o => 
              isFullyExcluded || excludedOutingKeys.has(`${p.playerId}_${o.scoringPeriodId}`)
            ).length;

            const isPartiallyExcluded = !isFullyExcluded && excludedOutingsCount > 0;

            return (
              <div
                key={p.playerId}
                className={`transition-colors ${
                  isFullyExcluded ? 'bg-rose-50/50' : isPartiallyExcluded ? 'bg-amber-50/40' : 'hover:bg-gray-50/60'
                }`}
              >
                {/* Player Summary Row */}
                <div className="py-3 px-2 flex items-center justify-between gap-3">
                  <div className="flex items-center gap-3 min-w-0">
                    <input
                      type="checkbox"
                      checked={isFullyExcluded}
                      onChange={() => togglePlayerExclusion(p.playerId)}
                      className="w-4 h-4 rounded text-blue-600 focus:ring-blue-500 cursor-pointer"
                      title={isFullyExcluded ? 'Include player' : 'Exclude player from simulation'}
                    />

                    <div className="min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span
                          onClick={() => onPlayerClick && onPlayerClick(p.playerId)}
                          className={`font-black text-sm cursor-pointer hover:underline ${
                            isFullyExcluded ? 'line-through text-gray-400' : 'text-gray-900'
                          }`}
                        >
                          {p.fullName}
                        </span>

                        <span className={`px-2 py-0.5 rounded-full text-[10px] font-black ${
                          p.isPitcher ? 'bg-indigo-100 text-indigo-800' : 'bg-amber-100 text-amber-800'
                        }`}>
                          {p.isPitcher ? 'Pitcher' : 'Batter'}
                        </span>

                        <span className="text-gray-400 text-[11px]">
                          {p.activeAppearances} active app{p.activeAppearances === 1 ? '' : 's'}
                        </span>

                        {/* Damage Badge for high ERA */}
                        {p.isPitcher && p.eraDamage > 1.5 && (
                          <span className="inline-flex items-center gap-1 text-[10px] font-black px-2 py-0.5 rounded-md bg-rose-100 text-rose-800 border border-rose-200">
                            <span>⚠️</span>
                            <span>+{p.eraDamage.toFixed(1)} ER Burden</span>
                          </span>
                        )}

                        {/* Drag Badge for negative OBP impact */}
                        {p.isBatter && p.obpDrag >= 4.0 && (
                          <span className="inline-flex items-center gap-1 text-[10px] font-black px-2 py-0.5 rounded-md bg-orange-100 text-orange-800 border border-orange-200">
                            <span>📉</span>
                            <span>+{p.obpDrag.toFixed(1)} OB Drag</span>
                          </span>
                        )}

                        {isPartiallyExcluded && (
                          <span className="text-[10px] font-bold px-2 py-0.5 rounded bg-amber-100 text-amber-800">
                            {excludedOutingsCount} of {p.activeAppearances} outings excluded
                          </span>
                        )}

                        {isFullyExcluded && (
                          <span className="text-[10px] font-black px-2 py-0.5 rounded bg-rose-100 text-rose-800">
                            Excluded
                          </span>
                        )}
                      </div>

                      {/* Player Stat Line */}
                      <div className="text-[11px] text-gray-500 font-mono mt-0.5 flex flex-wrap items-center gap-2">
                        {p.isPitcher ? (
                          <>
                            <span>{p.ip.toFixed(1)} IP</span>
                            <span>•</span>
                            <span className={p.era > 5.0 ? 'font-black text-rose-700' : 'font-bold text-gray-700'}>
                              {p.era !== null ? p.era.toFixed(2) : '-'} ERA
                            </span>
                            <span>•</span>
                            <span>{p.whip !== null ? p.whip.toFixed(2) : '-'} WHIP</span>
                            <span>•</span>
                            <span>{p.er} ER</span>
                            <span>•</span>
                            <span>{p.k} K</span>
                            <span>•</span>
                            <span>{p.qs} QS</span>
                            <span>•</span>
                            <span>{p.svHd} SV+H</span>
                          </>
                        ) : (
                          <>
                            <span>{p.pa} PA</span>
                            <span>•</span>
                            <span className={p.obp !== null && p.obp < 0.280 ? 'font-black text-rose-700' : 'font-bold text-gray-700'}>
                              {p.obp !== null ? p.obp.toFixed(3) : '-'} OBP
                            </span>
                            <span>•</span>
                            <span>{p.r} R</span>
                            <span>•</span>
                            <span>{p.hr} HR</span>
                            <span>•</span>
                            <span>{p.rbi} RBI</span>
                            <span>•</span>
                            <span>{p.sb} SB</span>
                            <span>•</span>
                            <span className="text-gray-400">({p.h}/{p.ab})</span>
                          </>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Accordion expand button for outing-level drilldown */}
                  <div className="flex items-center gap-2 shrink-0">
                    <button
                      type="button"
                      onClick={() => toggleAccordion(p.playerId)}
                      className="px-2.5 py-1 rounded-lg text-xs font-bold text-gray-600 bg-gray-100 hover:bg-gray-200 transition flex items-center gap-1 cursor-pointer"
                    >
                      <span>{isExpanded ? 'Hide Starts' : `Starts (${p.outings.length})`}</span>
                      <span className="text-[10px]">{isExpanded ? '▲' : '▼'}</span>
                    </button>
                  </div>
                </div>

                {/* Outing-Level Drilldown Accordion */}
                {isExpanded && (
                  <div className="bg-gray-50/80 p-3 rounded-2xl border border-gray-200/80 m-2 space-y-2 text-xs">
                    <div className="flex items-center justify-between text-[11px] font-bold text-gray-500 px-1 border-b border-gray-200 pb-1">
                      <span>Daily Active Starts for {p.fullName}</span>
                      <span className="font-normal text-gray-400">Uncheck a specific box to drop that start alone</span>
                    </div>

                    <div className="space-y-1">
                      {p.outings.map((out, idx) => {
                        const outingKey = `${p.playerId}_${out.scoringPeriodId}`;
                        const isOutingExcluded = isFullyExcluded || excludedOutingKeys.has(outingKey);
                        const dateStr = getDateFromPeriodId(out.scoringPeriodId, selectedSeason);
                        const isPitcherMeltdown = out.isPitcher && out.er >= 4;
                        const isBatterCollar = out.isBatter && out.ab >= 2 && out.h === 0 && out.bb === 0;

                        return (
                          <div
                            key={`out-${out.recordId}-${idx}`}
                            className={`p-2 rounded-xl flex items-center justify-between gap-2 border transition ${
                              isOutingExcluded
                                ? 'bg-rose-100/60 border-rose-300 text-rose-950'
                                : isPitcherMeltdown || isBatterCollar
                                ? 'bg-amber-50/80 border-amber-200 text-gray-800'
                                : 'bg-white border-gray-200 text-gray-800'
                            }`}
                          >
                            <div className="flex items-center gap-2.5 min-w-0">
                              <input
                                type="checkbox"
                                checked={!isOutingExcluded}
                                disabled={isFullyExcluded}
                                onChange={() => toggleOutingExclusion(p.playerId, out.scoringPeriodId)}
                                className="w-3.5 h-3.5 rounded text-blue-600 cursor-pointer"
                                title={isOutingExcluded ? 'Include this start' : 'Exclude this start'}
                              />
                              <span className="font-mono font-bold text-xs bg-gray-200/70 px-1.5 py-0.5 rounded">
                                {dateStr}
                              </span>
                              {out.isPitcher ? (
                                <div className="flex items-center gap-1.5 flex-wrap font-mono">
                                  <span className="font-black text-gray-900">{out.ip.toFixed(1)} IP</span>
                                  <span>•</span>
                                  <span className={out.er >= 4 ? 'font-black text-rose-700' : 'text-gray-700'}>
                                    {out.er} ER
                                  </span>
                                  <span>•</span>
                                  <span>{out.era !== null ? out.era.toFixed(1) : '-'} ERA</span>
                                  <span>•</span>
                                  <span>{out.k} K</span>
                                  <span>•</span>
                                  <span>{out.bbAll} BB, {out.hAll} H</span>
                                  {out.qs > 0 && <span className="text-[10px] font-bold text-emerald-700">QS</span>}
                                  {out.er >= 5 && <span className="text-[10px] font-black text-rose-600">💥 Meltdown</span>}
                                </div>
                              ) : (
                                <div className="flex items-center gap-1.5 flex-wrap font-mono">
                                  <span className="font-black text-gray-900">{out.h}/{out.ab}</span>
                                  <span>•</span>
                                  <span>{out.r} R</span>
                                  <span>•</span>
                                  <span>{out.hr} HR</span>
                                  <span>•</span>
                                  <span>{out.rbi} RBI</span>
                                  <span>•</span>
                                  <span>{out.sb} SB</span>
                                  <span>•</span>
                                  <span>{out.bb} BB</span>
                                  {isBatterCollar && (
                                    <span className="text-[10px] font-black text-cyan-800 bg-cyan-100 px-1.5 py-0.2 rounded">
                                      ❄️ 0-fer Collar
                                    </span>
                                  )}
                                </div>
                              )}
                            </div>

                            <div className="text-right shrink-0">
                              {isOutingExcluded && (
                                <span className="text-[10px] font-black text-rose-700 bg-rose-200/80 px-2 py-0.5 rounded">
                                  Excluded
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
            );
          })
        )}
      </div>
    </div>
  );
}
