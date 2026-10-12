// src/views/history/tabs/FranchiseHallOfFameTab.jsx
import React from 'react';
import TeamAvatar from '../../../components/TeamAvatar';
import leagueContextData from '../../../data/league_context.json';

const MATRIX_SEASONS = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026];

export default function FranchiseHallOfFameTab({
  sortedFranchiseRecords,
  franchiseRecords,
  hallSortField,
  hallSortDirection,
  hallSortLabel,
  handleHallSort,
  onOwnerClick,
  getOwnerTeamObj,
}) {
  return (
    <div className="space-y-8">
      {/* 1. All-Time Leaderboard Table */}
      <div className="bg-white border border-gray-200 rounded-3xl shadow-xl overflow-hidden">
        <div className="p-5 border-b border-gray-100 bg-gray-50/70 flex items-center justify-between">
          <div>
            <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>🏛️</span>
              <span>All-Time Franchise Career Leaderboard</span>
            </h2>
            <p className="text-xs text-gray-500 mt-0.5">Cumulative statistics across all 14 seasons (2012–2025)</p>
          </div>
          <span className="text-xs font-mono font-bold text-gray-500">
            Sorted by <span className="text-blue-600 font-black">{hallSortLabel}</span> ({hallSortDirection === 'desc' ? 'High to Low' : 'Low to High'}) • Click headers to sort
          </span>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full text-xs text-left select-none">
            <thead className="bg-gray-100/90 text-gray-700 font-black uppercase text-[10px] tracking-wider border-b border-gray-200">
              <tr>
                <th className="py-3 px-3">#</th>
                <th
                  onClick={() => handleHallSort('owner')}
                  className={`py-3 px-4 cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'owner' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center gap-1">
                    <span>Owner</span>
                    {hallSortField === 'owner' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('titles')}
                  className={`py-3 px-3 text-center cursor-pointer hover:bg-amber-100 transition ${
                    hallSortField === 'titles' ? 'bg-amber-100/90 text-amber-950 font-black' : 'bg-amber-50/80 text-amber-950'
                  }`}
                >
                  <div className="flex items-center justify-center gap-1">
                    <span>🏆 Titles</span>
                    {hallSortField === 'titles' && (
                      <span className="text-amber-900 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('podiums')}
                  className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'podiums' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-center gap-1">
                    <span>Podiums (1-3)</span>
                    {hallSortField === 'podiums' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('seasons')}
                  className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'seasons' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-center gap-1">
                    <span>Seasons</span>
                    {hallSortField === 'seasons' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('avgPlace')}
                  className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'avgPlace' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-center gap-1">
                    <span>Avg Finish</span>
                    {hallSortField === 'avgPlace' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('bestPlace')}
                  className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'bestPlace' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-center gap-1">
                    <span>Best</span>
                    {hallSortField === 'bestPlace' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('worstPlace')}
                  className={`py-3 px-3 text-center cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'worstPlace' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-center gap-1">
                    <span>Worst</span>
                    {hallSortField === 'worstPlace' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('totalPoints')}
                  className={`py-3 px-4 text-right cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'totalPoints' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-end gap-1">
                    <span>All-Time Pts</span>
                    {hallSortField === 'totalPoints' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
                <th
                  onClick={() => handleHallSort('ptsPerSeason')}
                  className={`py-3 px-3 text-right cursor-pointer hover:bg-gray-200 transition ${
                    hallSortField === 'ptsPerSeason' ? 'bg-blue-50 text-blue-900' : ''
                  }`}
                >
                  <div className="flex items-center justify-end gap-1">
                    <span>Pts/Season</span>
                    {hallSortField === 'ptsPerSeason' && (
                      <span className="text-blue-600 font-black">{hallSortDirection === 'asc' ? '▲' : '▼'}</span>
                    )}
                  </div>
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {sortedFranchiseRecords.map((f, i) => (
                <tr
                  key={f.owner}
                  className={`hover:bg-blue-50/40 transition-colors ${
                    f.titles > 0 ? 'font-bold' : ''
                  }`}
                >
                  <td className="py-3 px-3 font-mono font-black text-gray-400">{i + 1}</td>
                  <td className="py-3 px-4">
                    <div
                      onClick={() => onOwnerClick && onOwnerClick(getOwnerTeamObj(f.owner))}
                      className="flex items-center gap-2.5 cursor-pointer group"
                    >
                      <TeamAvatar team={{ owner: f.owner }} size="sm" className="group-hover:scale-105 transition-transform" />
                      <div>
                        <div className="font-black text-gray-900 group-hover:text-blue-600 transition flex items-center gap-1.5">
                          <span>{f.owner}</span>
                          {f.titles >= 2 && <span className="text-xs">👑</span>}
                        </div>
                        {f.titleYears.length > 0 && (
                          <div className="text-[10px] text-amber-600 font-mono">
                            Champs: {f.titleYears.join(', ')}
                          </div>
                        )}
                      </div>
                    </div>
                  </td>

                  {/* Championships */}
                  <td className="py-3 px-3 text-center font-black text-amber-900 bg-amber-50/40 text-sm">
                    {f.titles > 0 ? `${f.titles} 🏆` : '-'}
                  </td>

                  {/* Podiums */}
                  <td className="py-3 px-3 text-center font-black text-gray-800">
                    {f.podiums}
                  </td>

                  {/* Seasons */}
                  <td className="py-3 px-3 text-center font-mono text-gray-600">
                    {f.seasons}
                  </td>

                  {/* Average Finish */}
                  <td className="py-3 px-3 text-center font-mono font-black text-blue-700">
                    #{f.avgPlace}
                  </td>

                  {/* Best / Worst */}
                  <td className="py-3 px-3 text-center font-mono text-emerald-700">#{f.bestPlace}</td>
                  <td className="py-3 px-3 text-center font-mono text-gray-500">#{f.worstPlace}</td>

                  {/* All-Time Points */}
                  <td className="py-3 px-4 text-right font-mono font-black text-gray-900">
                    {f.totalPoints.toFixed(1)}
                  </td>

                  {/* Pts per season */}
                  <td className="py-3 px-3 text-right font-mono text-gray-600">
                    {f.ptsPerSeason}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* 2. 14-Year Placement Matrix Grid */}
      <div className="bg-white border border-gray-200 rounded-3xl shadow-xl overflow-hidden">
        <div className="p-5 border-b border-gray-100 bg-gray-50/70 flex items-center justify-between">
          <div>
            <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
              <span>📅</span>
              <span>14-Year Placement Finish Matrix (2012–2026)</span>
            </h2>
            <p className="text-xs text-gray-500 mt-0.5">Complete historical track record by owner and year</p>
          </div>
          <div className="flex items-center gap-2 text-[10px] font-bold">
            <span className="inline-block w-3 h-3 rounded bg-amber-400"></span> 1st
            <span className="inline-block w-3 h-3 rounded bg-slate-300"></span> 2nd
            <span className="inline-block w-3 h-3 rounded bg-amber-700"></span> 3rd
            <span className="inline-block w-3 h-3 rounded bg-blue-100"></span> 4-5
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full text-xs text-center border-collapse">
            <thead className="bg-gray-100/90 text-gray-700 font-black uppercase text-[10px] tracking-wider border-b border-gray-200">
              <tr>
                <th className="py-3 px-3 text-left">Owner</th>
                {MATRIX_SEASONS.map(yr => (
                  <th key={yr} className="py-3 px-2 font-mono">{String(yr).slice(2)}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {franchiseRecords.map(f => (
                <tr key={`matrix-${f.owner}`} className="hover:bg-gray-50/80 transition-colors">
                  <td className="py-2.5 px-3 text-left font-black text-gray-900 flex items-center gap-2">
                    <TeamAvatar team={{ owner: f.owner }} size="xs" />
                    <span>{f.owner}</span>
                  </td>
                  {MATRIX_SEASONS.map(yr => {
                    const item = f.finishesByYear[yr];
                    if (!item) {
                      return <td key={yr} className="py-2.5 px-2 text-gray-300 font-mono">-</td>;
                    }
                    const p = item.place;
                    const badgeColor =
                      p === 1 ? 'bg-amber-400 text-amber-950 font-black ring-1 ring-amber-300' :
                      p === 2 ? 'bg-slate-300 text-slate-900 font-bold' :
                      p === 3 ? 'bg-amber-700 text-amber-100 font-bold' :
                      p <= 5 ? 'bg-blue-100 text-blue-900 font-semibold' :
                      'bg-gray-100 text-gray-600';

                    return (
                      <td key={yr} className="py-2 px-1">
                        <span
                          className={`inline-flex items-center justify-center w-6 h-6 rounded-lg text-[11px] font-mono shadow-2xs ${badgeColor}`}
                          title={`${yr}: #${p} place (${item.points} pts) - ${item.teamName}`}
                        >
                          {p}
                        </span>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* 3. Championship Roll of Honor Timeline */}
      <div className="bg-white border border-gray-200 rounded-3xl p-6 shadow-xl space-y-4">
        <h2 className="text-base font-black text-gray-900 flex items-center gap-2">
          <span>👑</span>
          <span>Championship Roll of Honor</span>
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
          {leagueContextData?.historical_finishes_summary?.championship_roll_of_honor?.map(c => (
            <div
              key={c.year}
              className="bg-gradient-to-br from-amber-500/10 via-slate-50 to-white border border-amber-300/50 rounded-2xl p-4 shadow-xs hover:shadow-md transition space-y-2"
            >
              <div className="flex items-center justify-between text-xs font-black">
                <span className="text-amber-800">{c.year} Champion</span>
                <span className="text-xs">🏆</span>
              </div>
              <div className="text-base font-black text-gray-900">{c.champion}</div>
              <div className="text-[11px] text-gray-500 italic truncate">{c.champion_team_name}</div>
              <div className="pt-2 border-t border-gray-100 text-[11px] text-gray-600 flex justify-between">
                <span>{c.champion_points} pts</span>
                <span className="text-gray-400">2nd: {c.runner_up} ({c.runner_up_points})</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
