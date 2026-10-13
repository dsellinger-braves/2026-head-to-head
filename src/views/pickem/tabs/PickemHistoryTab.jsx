import React from 'react';

export default function PickemHistoryTab({ hallOfFame = [] }) {
  return (
    <div className="space-y-6">
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
        <div className="border-b border-slate-800 pb-4">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <span>🏛️</span> All-Time Pick'em Dynasty Leaderboard (2016 – 2026)
          </h2>
          <p className="text-xs text-slate-400 mt-1">
            Cumulative championships, total auction budget cash won, and scoring records over 10 seasons of MLB forecasting.
          </p>
        </div>

        <div className="overflow-x-auto mt-6">
          <table className="w-full text-left text-sm border-collapse">
            <thead>
              <tr className="border-b border-slate-800 text-xs uppercase font-bold text-slate-400">
                <th className="py-3 px-3">Owner</th>
                <th className="py-3 px-3 text-center">🏆 Titles</th>
                <th className="py-3 px-3 text-center">🏅 Top 3</th>
                <th className="py-3 px-3 text-center">💰 Total Budget Won</th>
                <th className="py-3 px-3 text-right">Total Points</th>
                <th className="py-3 px-3 text-right">Seasons Played</th>
                <th className="py-3 px-3 text-right">Personal Best</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60">
              {hallOfFame.map((h, idx) => (
                <tr
                  key={h.name}
                  className={`hover:bg-slate-800/40 transition-colors ${
                    idx === 0 ? 'bg-amber-950/20' : ''
                  }`}
                >
                  <td className="py-3 px-3 font-bold text-white flex items-center gap-2">
                    {idx === 0 && <span className="text-amber-400">👑</span>}
                    <span>{h.name}</span>
                  </td>
                  <td className="py-3 px-3 text-center">
                    {h.titles > 0 ? (
                      <span className="px-2.5 py-1 rounded-full text-xs font-black bg-amber-950/80 border border-amber-500/50 text-amber-300">
                        {h.titles} {h.titles === 1 ? 'Championship' : 'Championships'}
                      </span>
                    ) : (
                      <span className="text-slate-600 text-xs">—</span>
                    )}
                  </td>
                  <td className="py-3 px-3 text-center text-slate-300 font-bold">
                    {h.top3}
                  </td>
                  <td className="py-3 px-3 text-center">
                    {h.totalPrizes > 0 ? (
                      <span className="px-2.5 py-1 rounded-full text-xs font-black bg-emerald-950/80 border border-emerald-500/50 text-emerald-300">
                        +${h.totalPrizes}
                      </span>
                    ) : (
                      <span className="text-slate-600 text-xs">$0</span>
                    )}
                  </td>
                  <td className="py-3 px-3 text-right font-black text-indigo-300">
                    {h.totalPoints}
                  </td>
                  <td className="py-3 px-3 text-right text-slate-400 font-mono text-xs">
                    {h.seasonsCount}
                  </td>
                  <td className="py-3 px-3 text-right text-slate-300 text-xs font-mono">
                    {h.bestScore} pts <span className="text-slate-500">('{h.bestYear?.toString().slice(2)}')</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
