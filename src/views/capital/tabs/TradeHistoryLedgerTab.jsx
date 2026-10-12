import React from 'react';

export default function TradeHistoryLedgerTab({
  assetTradesList,
  isCommissioner,
  isAdrian,
  handleDeleteExecutedTrade
}) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
            Draft Asset & Player Trades ({assetTradesList.length} Transactions)
          </h2>
          <span className="text-xs text-slate-500">
            Synchronized historical ledger from Supabase (draft_asset_trades)
          </span>
        </div>
        {isCommissioner && (
          <span className="text-[10px] font-bold px-2.5 py-1 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40">
            {isAdrian ? '👑 Commissioner Mode: Delete & Modify Actions Enabled' : '⚡ Admin Mode: Delete & Modify Actions Enabled'}
          </span>
        )}
      </div>

      <div className="overflow-x-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-xs border-collapse">
          <thead>
            <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
              <th className="py-3 px-4 w-28">Date</th>
              <th className="py-3 px-3 text-center w-20">Trade #</th>
              <th className="py-3 px-4 w-32">Sending Owner</th>
              <th className="py-3 px-4 w-32">Receiving Owner</th>
              <th className="py-3 px-4 w-52">Asset Traded</th>
              <th className="py-3 px-4">Notes & Package Details</th>
              {isCommissioner && <th className="py-3 px-3 text-right w-24">{isAdrian ? 'Commish' : 'Admin'}</th>}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
            {assetTradesList.map((t, idx) => (
              <tr key={`history-${idx}`} className="hover:bg-slate-800/40 transition-colors">
                <td className="py-3 px-4 font-mono text-slate-400 text-[11px]">
                  {t.trade_date}
                </td>
                <td className="py-3 px-3 text-center">
                  <span className="px-2 py-0.5 bg-slate-800 text-teal-300 rounded font-black text-[11px]">
                    #{t.trade_id}
                  </span>
                </td>
                <td className="py-3 px-4 font-bold text-rose-400">
                  {t.sending_owner}
                </td>
                <td className="py-3 px-4 font-bold text-emerald-400">
                  {t.receiving_owner}
                </td>
                <td className="py-3 px-4">
                  {t.asset_type === 'Player' ? (
                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-sky-500/15 border border-sky-500/40 text-sky-300 font-bold text-xs">
                      <span>👤</span>
                      <span>{t.asset_name}</span>
                      <span className="text-[10px] text-sky-400/80">(Player)</span>
                    </span>
                  ) : t.asset_type === 'Budget' ? (
                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-emerald-500/15 border border-emerald-500/40 text-emerald-300 font-bold text-xs">
                      <span>💵</span>
                      <span>{t.asset_name}</span>
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-500/15 border border-amber-500/40 text-amber-300 font-bold text-xs">
                      <span>🎟️</span>
                      <span>{t.asset_name}</span>
                      {t.round_num && <span className="text-[10px] text-amber-400/80">(R{t.round_num})</span>}
                    </span>
                  )}
                </td>
                <td className="py-3 px-4 text-slate-300">
                  {t.notes || `Trade #${t.trade_id}`}
                </td>
                {isCommissioner && (
                  <td className="py-3 px-3 text-right">
                    {t.id && (
                      <button
                        onClick={() => handleDeleteExecutedTrade(t)}
                        className="px-2 py-1 rounded bg-rose-950/60 border border-rose-500/40 text-rose-300 hover:bg-rose-900 text-[10px] font-bold cursor-pointer transition-all"
                        title="Delete this record from draft_asset_trades"
                      >
                        🗑️ Delete
                      </button>
                    )}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
