// src/views/history/modals/EraAdjustmentModal.jsx
import React, { useEffect } from 'react';

export default function EraAdjustmentModal({ isOpen, onClose }) {
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') onClose();
    };
    if (isOpen) {
      window.addEventListener('keydown', handleKeyDown);
      return () => window.removeEventListener('keydown', handleKeyDown);
    }
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 overflow-y-auto">
      <div 
        className="bg-white border border-gray-200 rounded-3xl p-6 sm:p-8 max-w-2xl w-full shadow-2xl space-y-6 relative animate-scale-up"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-5 right-5 text-gray-400 hover:text-gray-700 text-lg font-black transition cursor-pointer p-1"
          aria-label="Close modal"
        >
          ✕
        </button>

        <div className="flex items-center gap-3 border-b border-gray-100 pb-4">
          <div className="w-10 h-10 rounded-2xl bg-amber-500/10 text-amber-600 flex items-center justify-center text-xl font-black">
            ⚖️
          </div>
          <div>
            <h3 className="text-xl font-black text-gray-900">Historical Era & Roster Adjustments</h3>
            <p className="text-xs text-gray-500">Mathematical methodology and normalization principles</p>
          </div>
        </div>

        <div className="space-y-4 text-xs sm:text-sm text-gray-700 leading-relaxed">
          <div className="bg-amber-50/60 border border-amber-200/80 rounded-2xl p-4 space-y-2">
            <h4 className="font-black text-amber-900 flex items-center gap-2">
              <span>1.</span>
              <span>Roster Capacity Expansion (22 starters vs 28 starters)</span>
            </h4>
            <p>
              From <strong>2012 through 2025</strong>, the league used a 22-man active starter lineup (13 active batters and 9 active pitchers). In <strong>2026</strong>, active lineups expanded to 28 starters (16 batters and 12 pitchers).
            </p>
            <p className="font-mono text-xs text-amber-800">
              Batting Multiplier: 16 / 13 = 1.231× | Pitching Multiplier: 12 / 9 = 1.333×
            </p>
          </div>

          <div className="bg-blue-50/60 border border-blue-200/80 rounded-2xl p-4 space-y-2">
            <h4 className="font-black text-blue-900 flex items-center gap-2">
              <span>2.</span>
              <span>League Size & Roto Point Normalization</span>
            </h4>
            <p>
              League size has varied: 8 teams in 2013 (max 80 pts), 9 teams in 2012 and 2026 (max 90 pts), and 10 teams from 2014 to 2025 (max 100 pts). To compare championships equitably, we calculate a <strong>Dominance Index (% of Max Points Available)</strong> scaled to a 100-point universal standard.
            </p>
          </div>

          <div className="bg-emerald-50/60 border border-emerald-200/80 rounded-2xl p-4 space-y-2">
            <h4 className="font-black text-emerald-900 flex items-center gap-2">
              <span>3.</span>
              <span>MLB League-Wide Run Environment Normalization</span>
            </h4>
            <p>
              MLB run environments have shifted dramatically over the past 14 years: the 2019 juiced-ball spike (1.39 HR/game), the 2022 deadened-ball slump (1.07 HR/game), and the 2023–2026 pitch clock and enlarged bases. The MLB toggle scales counts using official MLB league averages relative to the 2026 baseline.
            </p>
          </div>
        </div>

        <div className="pt-2 flex justify-end">
          <button
            onClick={onClose}
            className="px-6 py-2.5 rounded-xl bg-blue-600 text-white font-bold text-xs hover:bg-blue-700 transition cursor-pointer shadow-md"
          >
            Got It
          </button>
        </div>
      </div>
    </div>
  );
}
