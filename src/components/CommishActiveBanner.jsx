// src/components/CommishActiveBanner.jsx
import React, { useState } from 'react';
import { useAuth } from '../context/useAuth';

export default function CommishActiveBanner() {
  const { isCommissioner, profile, commishCheckoutReason, relinquishCommissionerPowers, effectiveOwner, isActingAsOther } = useAuth();
  const [relinquishing, setRelinquishing] = useState(false);

  if (!isCommissioner) return null;

  const handleRelinquish = async () => {
    setRelinquishing(true);
    try {
      await relinquishCommissionerPowers();
    } catch (err) {
      console.error('Error relinquishing commissioner powers:', err);
    } finally {
      setRelinquishing(false);
    }
  };

  return (
    <div className="bg-gradient-to-r from-amber-600 via-amber-500 to-amber-600 text-slate-950 font-bold px-4 py-2 border-b border-amber-400 shadow-md flex flex-wrap items-center justify-between gap-3 text-xs">
      <div className="flex items-center gap-2">
        <span className="text-base animate-pulse">🛡️</span>
        <div>
          <span className="font-black uppercase tracking-wider">Commissioner Powers Active:</span>{' '}
          <span>{profile?.owner_name || 'Commissioner'}</span>
          {isActingAsOther && (
            <span className="ml-2 px-2 py-0.5 rounded-full bg-slate-950 text-amber-300 text-[10px] font-black">
              Acting as {effectiveOwner}
            </span>
          )}
          {commishCheckoutReason && (
            <span className="ml-2 text-slate-900 font-normal italic">
              — &quot;{commishCheckoutReason}&quot;
            </span>
          )}
        </div>
      </div>

      <div className="flex items-center gap-2">
        <span className="text-[11px] text-amber-950 font-semibold hidden md:inline">
          Actions logged to audit registry & #league-news
        </span>
        <button
          onClick={handleRelinquish}
          disabled={relinquishing}
          className="px-3 py-1 rounded-lg bg-slate-950 hover:bg-slate-900 text-amber-300 hover:text-white text-xs font-black shadow-sm transition-all cursor-pointer disabled:opacity-50"
          title="Relinquish administrative powers and return to regular owner mode"
        >
          {relinquishing ? 'Checking In...' : '🔒 Relinquish Powers (Check In)'}
        </button>
      </div>
    </div>
  );
}
