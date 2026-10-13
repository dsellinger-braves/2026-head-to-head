/**
 * @file PickemMethodologyModal.jsx
 * @description Modal explaining in-progress live standings, point allocations, and
 * sabermetric projection methodology for the Pick'em Championship.
 */

import React, { useEffect } from 'react';

export default function PickemMethodologyModal({ isOpen, onClose }) {
  useEffect(() => {
    if (!isOpen) return;
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div
      onClick={(e) => e.target === e.currentTarget && onClose()}
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-xs p-4 animate-fade-in"
      role="dialog"
      aria-modal="true"
    >
      <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-xl w-full p-6 shadow-2xl relative space-y-4">
        <div className="flex items-center justify-between border-b border-slate-800 pb-3">
          <h3 className="text-lg font-bold text-white flex items-center gap-2">
            <span>⚡</span> In-Progress Projections Methodology
          </h3>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white text-lg font-bold cursor-pointer transition-colors"
            aria-label="Close Modal"
          >
            ✕
          </button>
        </div>

        <div className="text-xs text-slate-300 space-y-3 leading-relaxed">
          <p>
            To track which predictions are working out while the MLB season is actively underway, our system combines live official standings with sabermetric projections:
          </p>
          <ul className="space-y-2 list-disc pl-4 text-slate-300">
            <li>
              <strong className="text-white">Division Winners (3 pts):</strong> Evaluated against current 1st-place teams in each division via the official MLB Stats API.
            </li>
            <li>
              <strong className="text-white">Wild Card & Playoff Crossovers (2 pts):</strong> Teams holding playoff seeds in AL and NL (division winners and wild card seeds 1–3). If an owner predicted a team for a playoff spot (division or wild card) and that team qualifies for any playoff spot in that league, they earn 2 points if not an exact match.
            </li>
            <li>
              <strong className="text-white">Pennants (5 pts) & World Series (7 pts):</strong> Current top seed in each league and best overall record in baseball.
            </li>
            <li>
              <strong className="text-white">Player Awards Placeholder (4 pts):</strong> Because awards are officially announced in November, we use <span className="text-amber-300 font-bold">Projected Full-Season fWAR</span> (combining FanGraphs actual YTD fWAR + Rest-of-Season projected fWAR). The #1 player in each league's projected fWAR is the current placeholder leader.
            </li>
            <li>
              <strong className="text-white">Extreme Win / Loss Totals (3 pts):</strong> Teams holding the most total wins and most total losses in Major League Baseball.
            </li>
          </ul>
          <p className="text-[11px] text-slate-400 pt-2 border-t border-slate-800">
            Data sources: Official MLB Stats API (<code className="text-indigo-300">statsapi.mlb.com</code>) and FanGraphs Depth Charts / ROS Leaderboards API.
          </p>
        </div>

        <div className="pt-2 text-right">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs rounded-lg cursor-pointer transition-colors"
          >
            Got It
          </button>
        </div>
      </div>
    </div>
  );
}
