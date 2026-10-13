import React from 'react';

export default function SimulatorHeader({
  selectedTeamId,
  setSelectedTeamId,
  humanTeams = [],
  handleClearAllExclusions,
  handleExcludeTop3EraKillers,
  handleExcludeSubInningMeltdowns,
  handleExcludeTop3ObpAnchors,
  handleExcludeHitlessSlumpDays,
  totalExclusionsCount = 0,
  excludedPlayerIdsCount = 0,
  excludedOutingKeysCount = 0
}) {
  return (
    <div className="bg-gradient-to-r from-slate-900 via-indigo-950 to-blue-950 text-white p-6 rounded-3xl shadow-lg border border-slate-800 space-y-4">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2.5">
            <span className="text-3xl">🧪</span>
            <div>
              <h2 className="text-2xl font-black tracking-tight">What-If Player Impact Simulator</h2>
              <p className="text-xs text-indigo-200 mt-0.5">
                Model team statistics and league standings without specific players or disaster spot-starts.
              </p>
            </div>
          </div>
        </div>

        {/* Team Switcher */}
        <div className="flex items-center gap-2 bg-white/10 backdrop-blur p-1.5 rounded-2xl border border-white/10 self-start md:self-auto">
          <span className="text-xs font-bold text-indigo-200 pl-2">Team:</span>
          <select
            value={selectedTeamId}
            onChange={(e) => {
              setSelectedTeamId(Number(e.target.value));
              handleClearAllExclusions();
            }}
            className="bg-slate-900/90 text-white font-black text-xs px-3 py-1.5 rounded-xl border border-indigo-400/40 focus:outline-none focus:ring-2 focus:ring-blue-400 cursor-pointer"
          >
            {humanTeams.map(t => (
              <option key={t.id} value={t.id}>
                {t.name} ({t.owner})
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Quick Exclusions Status & Action Presets */}
      <div className="flex flex-wrap items-center justify-between gap-3 pt-3 border-t border-white/10 text-xs">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-bold text-gray-300">Quick Presets:</span>
          <button
            onClick={handleExcludeTop3EraKillers}
            className="px-2.5 py-1 rounded-lg bg-rose-500/20 text-rose-300 border border-rose-500/40 hover:bg-rose-500/30 transition font-bold cursor-pointer"
            title="Exclude top 3 pitchers causing highest ERA inflation"
          >
            💥 Top 3 ERA Killers
          </button>
          <button
            onClick={handleExcludeSubInningMeltdowns}
            className="px-2.5 py-1 rounded-lg bg-amber-500/20 text-amber-300 border border-amber-500/40 hover:bg-amber-500/30 transition font-bold cursor-pointer"
            title="Exclude all pitching outings with < 1 IP and >= 2 ER"
          >
            🛑 Pitching Meltdowns (&lt;1 IP)
          </button>
          <button
            onClick={handleExcludeTop3ObpAnchors}
            className="px-2.5 py-1 rounded-lg bg-orange-500/20 text-orange-300 border border-orange-500/40 hover:bg-orange-500/30 transition font-bold cursor-pointer"
            title="Exclude top 3 batters with highest OBP drag"
          >
            📉 Top 3 OBP Anchors
          </button>
          <button
            onClick={handleExcludeHitlessSlumpDays}
            className="px-2.5 py-1 rounded-lg bg-cyan-500/20 text-cyan-300 border border-cyan-500/40 hover:bg-cyan-500/30 transition font-bold cursor-pointer"
            title="Exclude all batting outings with 0 H, 0 BB, and at least 3 AB"
          >
            ❄️ 0-fer Slump Days (0 H, 0 BB)
          </button>
          {totalExclusionsCount > 0 && (
            <button
              onClick={handleClearAllExclusions}
              className="px-2.5 py-1 rounded-lg bg-gray-700/80 text-gray-200 hover:bg-gray-700 transition font-bold cursor-pointer"
            >
              🔄 Reset All ({totalExclusionsCount})
            </button>
          )}
        </div>

        <div className="font-mono text-indigo-200">
          Active Exclusions: <span className="font-black text-white">{excludedPlayerIdsCount}</span> players, <span className="font-black text-white">{excludedOutingKeysCount}</span> single outings
        </div>
      </div>
    </div>
  );
}
