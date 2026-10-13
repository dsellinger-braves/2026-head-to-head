import React from 'react';
import TeamAvatar from '../../../components/TeamAvatar';

export default function SimulatorStandingsRipple({
  leagueStandingsComparison,
  simulationResults,
  baselineRotoPoints,
  onOwnerClick
}) {
  return (
    <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-4">
      <div className="flex items-center justify-between border-b border-gray-100 pb-3">
        <div>
          <h3 className="text-base font-black text-gray-900 flex items-center gap-2">
            <span>🏆</span>
            <span>Full League Rotisserie Standings (Ripple Effect)</span>
          </h3>
          <p className="text-xs text-gray-400 mt-0.5">
            See how this simulated adjustment impacts every team in the league.
          </p>
        </div>
        <span className="text-xs font-mono font-bold text-gray-500 bg-gray-100 px-2.5 py-1 rounded-lg">
          9 Teams
        </span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-xs">
        {/* Baseline Standings Column */}
        <div className="space-y-2 border border-gray-100 rounded-2xl p-3 bg-gray-50/50">
          <div className="font-bold text-gray-500 uppercase tracking-wider text-[11px] px-1">
            Actual Current Standings
          </div>
          <div className="divide-y divide-gray-100">
            {leagueStandingsComparison.baseStandings.map((t, idx) => (
              <div
                key={`base-st-${t.id}`}
                onClick={() => onOwnerClick && onOwnerClick(t)}
                className={`py-2 px-2 flex items-center justify-between rounded-xl cursor-pointer hover:bg-blue-50/50 transition ${
                  t.isTarget ? 'bg-blue-100/70 font-black text-blue-950 ring-1 ring-blue-300' : ''
                }`}
              >
                <div className="flex items-center gap-2 min-w-0">
                  <span className="w-5 font-mono font-bold text-gray-400">{idx + 1}.</span>
                  <TeamAvatar team={t} size="xs" />
                  <span className="truncate">{t.name}</span>
                </div>
                <span className="font-mono font-bold">{t.rotoTotal.toFixed(1)} pts</span>
              </div>
            ))}
          </div>
        </div>

        {/* Simulated Standings Column */}
        <div className="space-y-2 border border-emerald-200 rounded-2xl p-3 bg-emerald-50/30">
          <div className="font-bold text-emerald-800 uppercase tracking-wider text-[11px] px-1 flex items-center justify-between">
            <span>Simulated Standings</span>
            <span className="text-[10px] font-mono">Counterfactual</span>
          </div>
          <div className="divide-y divide-emerald-100/60">
            {leagueStandingsComparison.simStandings.map((t, idx) => {
              const diff = (simulationResults.simulatedRotoPoints[t.id]?.total || 0) - (baselineRotoPoints[t.id]?.total || 0);
              return (
                <div
                  key={`sim-st-${t.id}`}
                  className={`py-2 px-2 flex items-center justify-between rounded-xl ${
                    t.isTarget ? 'bg-emerald-200/60 font-black text-emerald-950 ring-1 ring-emerald-400' : ''
                  }`}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="w-5 font-mono font-bold text-emerald-700">{idx + 1}.</span>
                    <TeamAvatar team={t} size="xs" />
                    <span className="truncate">{t.name}</span>
                  </div>
                  <div className="flex items-center gap-2 font-mono">
                    <span className="font-bold">{t.rotoTotal.toFixed(1)} pts</span>
                    {Math.abs(diff) > 0.001 && (
                      <span className={`text-[10px] font-bold px-1.5 py-0.2 rounded ${
                        diff > 0 ? 'bg-emerald-100 text-emerald-800' : 'bg-rose-100 text-rose-800'
                      }`}>
                        {diff > 0 ? '+' : ''}{diff.toFixed(1)}
                      </span>
                    )}
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
