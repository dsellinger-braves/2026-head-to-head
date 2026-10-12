import React from 'react';

export default function Historical2026BoardTab({
  roundFilter,
  setRoundFilter,
  DRAFT_OWNERS,
  compPicks,
  keepers
}) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
            2026 Ground Truth Draft Board Architecture
          </h2>
          <p className="text-xs text-slate-400 mt-0.5">
            Rounds 1–5 are Keepers; Rounds 6–20 include purchased Comp Picks; Rounds 27–32 show Offset picks.
          </p>
        </div>
        <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold">
          {['ALL', 'KEEPERS', 'EARLY', 'MID', 'LATE'].map(rf => (
            <button
              key={rf}
              onClick={() => setRoundFilter(rf)}
              className={`px-2.5 py-1 rounded-md transition-colors cursor-pointer ${
                roundFilter === rf
                  ? 'bg-slate-800 text-emerald-400 shadow-xs'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              {rf === 'ALL' ? 'All (R1–32)' : rf === 'KEEPERS' ? 'Keepers (R1–5)' : rf === 'EARLY' ? 'Early (R6–14)' : rf === 'MID' ? 'Mid (R15–23)' : 'Late (R24–32)'}
            </button>
          ))}
        </div>
      </div>

      <div className="overflow-x-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-xs border-collapse min-w-[950px]">
          <thead>
            <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
              <th className="py-3 px-3 text-center w-20">Round</th>
              {DRAFT_OWNERS.map(owner => (
                <th key={owner} className="py-3 px-3 text-center">
                  <div className="text-slate-200 font-extrabold">{owner}</div>
                </th>
              ))}
              <th className="py-3 px-3 text-center w-36">Comp Pick (End)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
            {Array.from({ length: 32 }, (_, i) => i + 1)
              .filter(rNum => {
                if (roundFilter === 'KEEPERS') return rNum <= 5;
                if (roundFilter === 'EARLY') return rNum >= 6 && rNum <= 14;
                if (roundFilter === 'MID') return rNum >= 15 && rNum <= 23;
                if (roundFilter === 'LATE') return rNum >= 24 && rNum <= 32;
                return true;
              })
              .map(rNum => {
                const isKeeperRound = rNum <= 5;
                const compPicksThisRound = (compPicks || []).filter(
                  cp => cp.round_num === rNum &&
                    (cp.action_type === 'COMP_BOUGHT' || cp.action_type === 'BOUGHT') &&
                    (!cp.season_year || cp.season_year === 2026 || cp.season_year === '2026')
                );

                return (
                  <tr key={rNum} className="hover:bg-slate-800/40 transition-colors">
                    <td className="py-2 px-3 text-center font-black text-slate-400 bg-slate-950/40">
                      R{rNum}
                      {isKeeperRound && <span className="block text-[9px] text-amber-400">Keeper</span>}
                    </td>
                    {DRAFT_OWNERS.map(owner => {
                      if (isKeeperRound) {
                        const keeperObj = (keepers || []).find(
                          k => k.round_num === rNum &&
                            (k.manager === owner || (owner === 'Daniel' && k.manager === 'Dan'))
                        );
                        return (
                          <td key={owner} className="py-2 px-2 text-center bg-amber-950/10">
                            {keeperObj ? (
                              <div className="text-[11px] font-bold text-amber-300">
                                {keeperObj.player_name || keeperObj.player}
                              </div>
                            ) : (
                              <span className="text-slate-600 text-[10px]">Keeper Slot</span>
                            )}
                          </td>
                        );
                      }
                      return (
                        <td key={owner} className="py-2 px-2 text-center text-slate-300 text-xs">
                          {owner}
                        </td>
                      );
                    })}
                    <td className="py-2 px-3 text-center bg-emerald-950/10">
                      {compPicksThisRound.length > 0 ? (
                        <div className="flex flex-col gap-1 items-center">
                          {compPicksThisRound.map((cp, idx) => (
                            <span
                              key={idx}
                              className="px-2 py-0.5 rounded text-[10px] font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/40"
                            >
                              {cp.owner_name || cp.manager} (+Comp)
                            </span>
                          ))}
                        </div>
                      ) : (
                        <span className="text-slate-700 text-[11px]">-</span>
                      )}
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
