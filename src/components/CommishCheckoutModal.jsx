// src/components/CommishCheckoutModal.jsx
import React, { useState } from 'react';
import { useAuth } from '../context/useAuth';

export default function CommishCheckoutModal({ isOpen, onClose }) {
  const { profile, checkoutCommissionerPowers } = useAuth();
  const [reason, setReason] = useState('');
  const [loading, setLoading] = useState(false);

  if (!isOpen) return null;

  const quickReasons = [
    '🤝 Trade Review & Approval',
    '⚙️ Offseason Draft Maintenance',
    '🛠️ Roster / Keeper Adjustment',
    '📋 General Administrative Review'
  ];

  const handleCheckout = async (e) => {
    e.preventDefault();
    setLoading(true);
    try {
      await checkoutCommissionerPowers(reason || 'Administrative maintenance');
      onClose();
    } catch (err) {
      console.error('Failed to checkout commish powers:', err);
      alert('Error activating commissioner powers: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-fade-in">
      <div className="bg-slate-900 border border-amber-500/40 rounded-2xl max-w-md w-full shadow-2xl overflow-hidden ring-1 ring-amber-500/30">
        <div className="bg-gradient-to-r from-amber-600/30 to-slate-900 px-6 py-4 border-b border-amber-500/30 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-2xl">🛡️</span>
            <div>
              <h3 className="text-base font-black text-white">Check Out Commissioner Powers</h3>
              <p className="text-[11px] text-amber-300/80">Break-glass access for {profile?.owner_name || 'Commissioner'}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white text-lg font-bold cursor-pointer"
          >
            ✕
          </button>
        </div>

        <form onSubmit={handleCheckout} className="p-6 space-y-4">
          <div className="bg-amber-950/40 border border-amber-500/30 rounded-xl p-3.5 text-xs text-amber-200/90 leading-relaxed space-y-1.5">
            <p className="font-bold flex items-center gap-1.5 text-amber-300">
              <span>📢</span>
              <span>Discord Notification & Audit Notice</span>
            </p>
            <p>
              Activating commissioner powers allows you to approve/edit trades, override rosters, and manage draft capital.
            </p>
            <p className="text-[11px] text-amber-400/80">
              An alert will be posted to the <strong className="text-amber-200">#league-news</strong> Discord channel announcing your checkout, and all administrative actions will be recorded in the league audit log.
            </p>
          </div>

          <div>
            <label className="block text-xs font-bold text-slate-300 mb-1.5">
              Stated Reason / Purpose:
            </label>
            <input
              type="text"
              placeholder="e.g. Reviewing trade proposal, draft board adjustment..."
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              className="w-full bg-slate-950 border border-slate-700 rounded-xl px-3.5 py-2.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-amber-400"
              autoFocus
            />
          </div>

          <div className="space-y-1">
            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Quick Presets:</span>
            <div className="flex flex-wrap gap-1.5">
              {quickReasons.map(r => (
                <button
                  key={r}
                  type="button"
                  onClick={() => setReason(r)}
                  className="px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-[11px] font-medium transition-colors cursor-pointer"
                >
                  {r}
                </button>
              ))}
            </div>
          </div>

          <div className="flex items-center justify-end gap-3 pt-3 border-t border-slate-800">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 rounded-xl text-xs font-bold text-slate-400 hover:text-white cursor-pointer"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="px-5 py-2 rounded-xl bg-amber-500 hover:bg-amber-400 text-slate-950 font-black text-xs shadow-lg shadow-amber-500/20 cursor-pointer disabled:opacity-50 transition-all flex items-center gap-1.5"
            >
              <span>{loading ? 'Activating...' : '🛡️ Activate Commish Powers'}</span>
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
