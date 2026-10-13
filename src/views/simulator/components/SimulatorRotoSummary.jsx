import React from 'react';
import { SCORING_CATS, getStatMeta } from '../../../utils/scoring';
import {
  BATTING_CATS,
  PITCHING_CATS,
  formatCatNum,
  formatDelta
} from '../services/simulatorEngine';

export default function SimulatorRotoSummary({
  activeTeamObj,
  leagueStandingsComparison,
  baseRoto = { total: 0 },
  simRoto = { total: 0 },
  baseTeamStats = {},
  simTeamStats = {},
  categoriesUp = 0,
  categoriesDown = 0,
  categoriesEven = 0,
  battingPointsDelta = 0,
  pitchingPointsDelta = 0
}) {
  const rankDiff = leagueStandingsComparison.rankDelta;
  const rotoPointsDiff = simRoto.total - baseRoto.total;

  return (
    <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-5">
      {/* High-Level Impact Bar */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4 border-b border-gray-100 pb-4">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xl">📊</span>
            <h3 className="text-lg font-black text-gray-900">
              Full Rotisserie Summary ({activeTeamObj.name})
            </h3>
          </div>
          <p className="text-xs text-gray-500 mt-0.5">
            Instant snapshot across all 10 rotisserie categories under the counterfactual simulation.
          </p>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {/* Standings Rank Card */}
          <div className="bg-purple-50/80 border border-purple-200 rounded-2xl px-3.5 py-2 flex items-center gap-3">
            <div>
              <div className="text-[10px] font-bold text-purple-700 uppercase tracking-wide">League Rank</div>
              <div className="text-xl font-black text-purple-950 font-mono">
                {leagueStandingsComparison.simRank}
                <span className="text-xs font-bold text-purple-600 ml-0.5">
                  {leagueStandingsComparison.simRank === 1 ? 'st 👑' :
                   leagueStandingsComparison.simRank === 2 ? 'nd' :
                   leagueStandingsComparison.simRank === 3 ? 'rd' : 'th'}
                </span>
              </div>
            </div>
            <span className={`text-xs font-black font-mono px-2 py-0.5 rounded-lg ${
              rankDiff > 0 ? 'bg-emerald-100 text-emerald-800' :
              rankDiff < 0 ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-600'
            }`}>
              {rankDiff > 0 ? `▲ +${rankDiff}` : rankDiff < 0 ? `▼ ${rankDiff}` : 'Even'}
            </span>
          </div>

          {/* Total Points Card */}
          <div className="bg-blue-50/80 border border-blue-200 rounded-2xl px-3.5 py-2 flex items-center gap-3">
            <div>
              <div className="text-[10px] font-bold text-blue-700 uppercase tracking-wide">Total Roto Pts</div>
              <div className="text-xl font-black text-blue-950 font-mono">
                {simRoto.total.toFixed(1)}
                <span className="text-xs text-gray-400 font-normal ml-1">/ 90</span>
              </div>
            </div>
            <span className={`text-xs font-black font-mono px-2 py-0.5 rounded-lg ${
              rotoPointsDiff > 0.001 ? 'bg-emerald-100 text-emerald-800' :
              rotoPointsDiff < -0.001 ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-600'
            }`}>
              {rotoPointsDiff > 0 ? '+' : ''}{rotoPointsDiff.toFixed(1)} pts
            </span>
          </div>

          {/* Shift Breakdown Pills */}
          <div className="flex items-center gap-1.5 bg-gray-50 border border-gray-200 rounded-2xl p-2 text-xs font-bold font-mono">
            <span className="px-2 py-1 bg-emerald-100/80 text-emerald-800 rounded-lg" title="Categories gained">
              🟢 {categoriesUp} Up
            </span>
            <span className="px-2 py-1 bg-rose-100/80 text-rose-800 rounded-lg" title="Categories lost">
              🔴 {categoriesDown} Down
            </span>
            <span className="px-2 py-1 bg-gray-200/80 text-gray-700 rounded-lg" title="Categories unchanged">
              ⚪ {categoriesEven} Even
            </span>
          </div>
        </div>
      </div>

      {/* 10 Category Grid Layout */}
      <div className="space-y-4">
        {/* Batting Header & 5 Cards */}
        <div>
          <div className="flex items-center justify-between text-xs font-black uppercase text-amber-800 tracking-wider mb-2">
            <div className="flex items-center gap-1.5">
              <span>⚾</span>
              <span>Batting Categories (5)</span>
            </div>
            <span className={`font-mono text-[11px] px-2 py-0.5 rounded ${
              battingPointsDelta > 0.001 ? 'bg-emerald-100 text-emerald-800' :
              battingPointsDelta < -0.001 ? 'bg-rose-100 text-rose-800' : 'text-gray-400'
            }`}>
              Net: {battingPointsDelta > 0 ? '+' : ''}{battingPointsDelta.toFixed(1)} Pts
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
            {BATTING_CATS.map(catKey => {
              const meta = getStatMeta(catKey);
              const isLow = SCORING_CATS[catKey]?.type === 'low';

              const baseValRaw = baseTeamStats[`${catKey}_raw`] ?? baseTeamStats[catKey];
              const simValRaw = simTeamStats[`${catKey}_raw`] ?? simTeamStats[catKey];

              const baseNum = parseFloat(baseValRaw) || 0;
              const simNum = parseFloat(simValRaw) || 0;
              const diff = simNum - baseNum;

              const basePts = baseRoto[catKey] ?? 0;
              const simPts = simRoto[catKey] ?? 0;
              const ptsDiff = simPts - basePts;

              const isBeneficial = isLow ? diff < -0.0001 : diff > 0.0001;
              const isDetrimental = isLow ? diff > 0.0001 : diff < -0.0001;

              return (
                <div
                  key={`top-cat-${catKey}`}
                  className={`p-3 rounded-2xl border transition-all ${
                    ptsDiff > 0.001
                      ? 'bg-emerald-50/70 border-emerald-300 ring-1 ring-emerald-200'
                      : ptsDiff < -0.001
                      ? 'bg-rose-50/70 border-rose-300 ring-1 ring-rose-200'
                      : 'bg-slate-50/70 border-gray-200'
                  }`}
                >
                  <div className="flex items-center justify-between text-[11px] font-bold text-gray-500 mb-1">
                    <span className="text-gray-900 font-black">{meta.label}</span>
                    <span className="text-[10px] text-gray-400">{catKey === 'OBP' ? 'Rate' : 'Counting'}</span>
                  </div>

                  <div className="flex items-baseline justify-between mt-1">
                    <div>
                      <div className="text-lg font-black text-gray-900 font-mono">
                        {formatCatNum(catKey, simNum)}
                      </div>
                      <div className="text-[10px] text-gray-400 font-mono">
                        Base: {formatCatNum(catKey, baseNum)}
                      </div>
                    </div>

                    <div className="text-right">
                      <span className={`inline-block text-[10px] font-black font-mono px-1.5 py-0.5 rounded ${
                        isBeneficial ? 'bg-emerald-100 text-emerald-800' :
                        isDetrimental ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-500'
                      }`}>
                        {formatDelta(catKey, diff)}
                      </span>
                    </div>
                  </div>

                  <div className="mt-2 pt-2 border-t border-gray-200/60 flex items-center justify-between text-[11px] font-mono">
                    <span className="font-bold text-blue-900">{simPts.toFixed(1)} pts</span>
                    <span className={`font-black text-[10px] px-1.5 py-0.2 rounded ${
                      ptsDiff > 0.001 ? 'bg-emerald-200 text-emerald-900 font-bold' :
                      ptsDiff < -0.001 ? 'bg-rose-200 text-rose-900 font-bold' : 'text-gray-400'
                    }`}>
                      {ptsDiff > 0 ? `+${ptsDiff.toFixed(1)}` : ptsDiff < 0 ? ptsDiff.toFixed(1) : 'Even'}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Pitching Header & 5 Cards */}
        <div>
          <div className="flex items-center justify-between text-xs font-black uppercase text-indigo-800 tracking-wider mb-2">
            <div className="flex items-center gap-1.5">
              <span>🎯</span>
              <span>Pitching Categories (5)</span>
            </div>
            <span className={`font-mono text-[11px] px-2 py-0.5 rounded ${
              pitchingPointsDelta > 0.001 ? 'bg-emerald-100 text-emerald-800' :
              pitchingPointsDelta < -0.001 ? 'bg-rose-100 text-rose-800' : 'text-gray-400'
            }`}>
              Net: {pitchingPointsDelta > 0 ? '+' : ''}{pitchingPointsDelta.toFixed(1)} Pts
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
            {PITCHING_CATS.map(catKey => {
              const meta = getStatMeta(catKey);
              const isLow = SCORING_CATS[catKey]?.type === 'low';

              const baseValRaw = baseTeamStats[`${catKey}_raw`] ?? baseTeamStats[catKey];
              const simValRaw = simTeamStats[`${catKey}_raw`] ?? simTeamStats[catKey];

              const baseNum = parseFloat(baseValRaw) || 0;
              const simNum = parseFloat(simValRaw) || 0;
              const diff = simNum - baseNum;

              const basePts = baseRoto[catKey] ?? 0;
              const simPts = simRoto[catKey] ?? 0;
              const ptsDiff = simPts - basePts;

              const isBeneficial = isLow ? diff < -0.0001 : diff > 0.0001;
              const isDetrimental = isLow ? diff > 0.0001 : diff < -0.0001;

              return (
                <div
                  key={`top-cat-${catKey}`}
                  className={`p-3 rounded-2xl border transition-all ${
                    ptsDiff > 0.001
                      ? 'bg-emerald-50/70 border-emerald-300 ring-1 ring-emerald-200'
                      : ptsDiff < -0.001
                      ? 'bg-rose-50/70 border-rose-300 ring-1 ring-rose-200'
                      : 'bg-slate-50/70 border-gray-200'
                  }`}
                >
                  <div className="flex items-center justify-between text-[11px] font-bold text-gray-500 mb-1">
                    <span className="text-gray-900 font-black">{meta.label}</span>
                    <span className="text-[10px] text-gray-400">{isLow ? 'Lower' : 'Higher'}</span>
                  </div>

                  <div className="flex items-baseline justify-between mt-1">
                    <div>
                      <div className="text-lg font-black text-gray-900 font-mono">
                        {formatCatNum(catKey, simNum)}
                      </div>
                      <div className="text-[10px] text-gray-400 font-mono">
                        Base: {formatCatNum(catKey, baseNum)}
                      </div>
                    </div>

                    <div className="text-right">
                      <span className={`inline-block text-[10px] font-black font-mono px-1.5 py-0.5 rounded ${
                        isBeneficial ? 'bg-emerald-100 text-emerald-800' :
                        isDetrimental ? 'bg-rose-100 text-rose-800' : 'bg-gray-100 text-gray-500'
                      }`}>
                        {formatDelta(catKey, diff)}
                      </span>
                    </div>
                  </div>

                  <div className="mt-2 pt-2 border-t border-gray-200/60 flex items-center justify-between text-[11px] font-mono">
                    <span className="font-bold text-blue-900">{simPts.toFixed(1)} pts</span>
                    <span className={`font-black text-[10px] px-1.5 py-0.2 rounded ${
                      ptsDiff > 0.001 ? 'bg-emerald-200 text-emerald-900 font-bold' :
                      ptsDiff < -0.001 ? 'bg-rose-200 text-rose-900 font-bold' : 'text-gray-400'
                    }`}>
                      {ptsDiff > 0 ? `+${ptsDiff.toFixed(1)}` : ptsDiff < 0 ? ptsDiff.toFixed(1) : 'Even'}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
