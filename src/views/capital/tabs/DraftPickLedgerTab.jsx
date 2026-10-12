import React from 'react';
import { normalizeOwner } from '../../../constants/league';


export default function DraftPickLedgerTab({
  roundFilter,
  setRoundFilter,
  DRAFT_OWNERS,
  rounds,
  computedPicks
}) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="text-xs font-black uppercase tracking-wider text-slate-400">Rounds Filter:</span>
          <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold">
            {[
              { id: 'ALL', label: 'All (R7–32)' },
              { id: 'EARLY', label: 'Early (R7–15)' },
              { id: 'MID', label: 'Mid (R16–23)' },
              { id: 'LATE', label: 'Late (R24–32)' },
            ].map(f => (
              <button
                key={f.id}
                onClick={() => setRoundFilter(f.id)}
                className={`px-2.5 py-1 rounded-md transition-colors cursor-pointer ${
                  roundFilter === f.id
                    ? 'bg-slate-800 text-teal-400 shadow-xs'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                {f.label}
              </button>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-4 text-xs">
          <span className="inline-flex items-center gap-1.5 text-slate-400">
            <span className="w-2.5 h-2.5 rounded-full bg-slate-700 border border-slate-600"></span> Original Pick
          </span>
          <span className="inline-flex items-center gap-1.5 text-amber-300 font-bold">
            <span className="w-2.5 h-2.5 rounded-full bg-amber-500 border border-amber-400"></span> Traded / Acquired Pick
          </span>
        </div>
      </div>

      <div className="overflow-x-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-xs border-collapse">
          <thead>
            <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
              <th className="py-3 px-3 text-center w-20">Round</th>
              {DRAFT_OWNERS.map(owner => (
                <th key={owner} className="py-3 px-3 text-center min-w-[110px]">
                  <div className="font-extrabold text-slate-200">{owner}</div>
                  <div className="text-[10px] text-slate-400 font-semibold lowercase">slot</div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
            {rounds.map(roundNum => (
              <tr key={roundNum} className="hover:bg-slate-800/40 transition-colors">
                <td className="py-2.5 px-3 text-center font-black text-slate-400 bg-slate-950/40">
                  R{roundNum}
                </td>
                {DRAFT_OWNERS.map(slotOwner => {
                  const pick = computedPicks.find(p => p.round === roundNum && p.originalOwner === slotOwner);
                  if (!pick) return <td key={slotOwner} className="py-2.5 px-2 text-center text-slate-600">-</td>;

                  const isTraded = pick.isTraded;
                  return (
                    <td
                      key={slotOwner}
                      className={`py-2 px-2 text-center transition-colors ${
                        isTraded
                          ? 'bg-amber-500/10 hover:bg-amber-500/20'
                          : 'hover:bg-slate-800/30'
                      }`}
                    >
                      <div className="inline-flex flex-col items-center">
                        <span className={`px-2 py-0.5 rounded-md text-xs font-bold ${
                          isTraded
                            ? 'bg-amber-500/30 text-amber-300 border border-amber-500/40 shadow-xs'
                            : 'text-slate-300'
                        }`}>
                          {pick.currentOwner}
                        </span>
                        {isTraded && (
                          <span className="text-[9px] text-amber-400/80 font-medium mt-0.5">
                            via {(() => {
                              const s = pick.tradeDetails?.sending_owner || pick.originalOwner;
                              return normalizeOwner(s, 'draft') || 'Trade';
                            })()}
                          </span>
                        )}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
