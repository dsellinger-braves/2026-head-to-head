import React from 'react';

export default function OwnerPickLedgersTab({
  DRAFT_OWNERS,
  selectedOwner,
  setSelectedOwner,
  ownerStats
}) {
  return (
    <div className="space-y-6">
      {/* Owner Selector Pills */}
      <div className="flex flex-wrap items-center gap-2">
        {DRAFT_OWNERS.map(owner => (
          <button
            key={owner}
            onClick={() => setSelectedOwner(owner)}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition-all cursor-pointer border ${
              selectedOwner === owner
                ? 'bg-amber-500 text-slate-950 border-amber-400 shadow-md font-black'
                : 'bg-slate-900 text-slate-400 border-slate-800 hover:text-white hover:border-slate-700'
            }`}
          >
            {owner} ({ownerStats[owner]?.total ?? 26})
          </button>
        ))}
      </div>

      {/* Ledger Breakdown Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
          <div className="text-xs font-bold text-slate-400 uppercase tracking-wider">Total Owned 2027 Picks</div>
          <div className="text-3xl font-black text-white mt-1">{ownerStats[selectedOwner]?.total ?? 26}</div>
          <div className="text-xs text-slate-500 mt-1">26 base rounds in draft (R7–32)</div>
        </div>
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
          <div className="text-xs font-bold text-slate-400 uppercase tracking-wider">Acquired from Others</div>
          <div className="text-3xl font-black text-emerald-400 mt-1">{ownerStats[selectedOwner]?.acquired.length ?? 0}</div>
          <div className="text-xs text-slate-500 mt-1">Incoming pick assets</div>
        </div>
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
          <div className="text-xs font-bold text-slate-400 uppercase tracking-wider">Traded Away to Others</div>
          <div className="text-3xl font-black text-rose-400 mt-1">{ownerStats[selectedOwner]?.tradedAway.length ?? 0}</div>
          <div className="text-xs text-slate-500 mt-1">Outgoing pick assets</div>
        </div>
      </div>

      {/* Complete Pick Inventory Table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
        <h3 className="text-sm font-black uppercase tracking-wider text-slate-300">
          {selectedOwner}'s 2027 Draft Pick Inventory
        </h3>
        <div className="overflow-x-auto rounded-xl border border-slate-800">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                <th className="py-2.5 px-4 w-24">Round</th>
                <th className="py-2.5 px-4 w-32">Status</th>
                <th className="py-2.5 px-4 w-40">Original Owner</th>
                <th className="py-2.5 px-4">Trade Context</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
              {ownerStats[selectedOwner]?.ownedPicks.map((pick, idx) => (
                <tr key={idx} className="hover:bg-slate-800/40 transition-colors">
                  <td className="py-2.5 px-4 font-black text-slate-300">Round {pick.round}</td>
                  <td className="py-2.5 px-4">
                    {pick.originalOwner === selectedOwner ? (
                      <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700">
                        ORIGINAL
                      </span>
                    ) : (
                      <span className="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-amber-500/20 text-amber-300 border border-amber-500/40">
                        ACQUIRED
                      </span>
                    )}
                  </td>
                  <td className="py-2.5 px-4 font-bold text-white">{pick.originalOwner}</td>
                  <td className="py-2.5 px-4 text-slate-400">
                    {pick.isTraded
                      ? `Acquired via Trade #${pick.tradeDetails?.trade_id} (${pick.tradeDetails?.trade_date})${pick.tradeDetails?.notes ? ' • ' + pick.tradeDetails.notes : ''}`
                      : 'Own natural slot'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Traded Away Table if any */}
      {(ownerStats[selectedOwner]?.tradedAway.length ?? 0) > 0 && (
        <div className="bg-slate-900 border border-rose-900/40 rounded-2xl p-5 shadow-xl space-y-4">
          <h3 className="text-sm font-black uppercase tracking-wider text-rose-400">
            Picks {selectedOwner} Has Traded Away
          </h3>
          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-2.5 px-4 w-24">Round</th>
                  <th className="py-2.5 px-4 w-32">Status</th>
                  <th className="py-2.5 px-4 w-40">Now Owned By</th>
                  <th className="py-2.5 px-4">Trade Context</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-rose-900/30 font-medium text-slate-200">
                {ownerStats[selectedOwner]?.tradedAway.map((pick, idx) => (
                  <tr key={`away-${idx}`} className="hover:bg-rose-950/20 transition-colors">
                    <td className="py-2.5 px-4 font-black text-rose-400 line-through">
                      Round {pick.round}
                    </td>
                    <td className="py-2.5 px-4">
                      <span className="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-rose-500/20 text-rose-300 border border-rose-500/40">
                        TRADED AWAY
                      </span>
                    </td>
                    <td className="py-2.5 px-4 font-bold text-white">
                      {pick.currentOwner}
                    </td>
                    <td className="py-2.5 px-4 text-rose-300/80">
                      Traded to <strong>{pick.currentOwner}</strong> via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                      {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
