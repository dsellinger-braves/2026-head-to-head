import React from 'react';

export default function SimulatorReplacementControls({
  simulationResults,
  pitcherReplacementMode,
  setPitcherReplacementMode,
  customPitcherRates,
  setCustomPitcherRates,
  batterReplacementMode,
  setBatterReplacementMode,
  customBatterRates,
  setCustomBatterRates,
  leagueAverages
}) {
  const vacatedIp = simulationResults.vacated?.vacatedIp || 0;
  const vacatedPa = simulationResults.vacated?.vacatedPa || 0;

  return (
    <div className="bg-white border border-gray-200 rounded-3xl p-5 shadow-sm space-y-5">
      <div className="border-b border-gray-100 pb-3">
        <h3 className="text-sm font-black text-gray-900 flex items-center gap-2">
          <span>⚖️</span>
          <span>Replacement Player Simulation Modes</span>
        </h3>
        <p className="text-xs text-gray-500 mt-0.5">
          Configure independent replacement assumptions for vacated pitching innings ({vacatedIp.toFixed(1)} IP) and batting plate appearances ({vacatedPa} PA).
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 text-xs">
        {/* Pitcher Replacement Section */}
        <div className="space-y-3 p-4 rounded-2xl bg-indigo-50/40 border border-indigo-100">
          <div className="flex items-center justify-between">
            <span className="font-black text-indigo-950 flex items-center gap-1.5">
              <span>🎯</span>
              <span>Pitcher Replacement Strategy</span>
            </span>
            <span className="font-mono text-indigo-700 font-bold text-[11px]">
              {vacatedIp.toFixed(1)} IP Vacated
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-1 bg-white p-1 rounded-xl border border-indigo-200/60">
            <button
              type="button"
              onClick={() => setPitcherReplacementMode('empty')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                pitcherReplacementMode === 'empty' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              🚫 Empty Slot
            </button>
            <button
              type="button"
              onClick={() => setPitcherReplacementMode('league_avg')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                pitcherReplacementMode === 'league_avg' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              📊 League Avg
            </button>
            <button
              type="button"
              onClick={() => setPitcherReplacementMode('replacement_level')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                pitcherReplacementMode === 'replacement_level' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ⚖️ Waiver Wire
            </button>
            <button
              type="button"
              onClick={() => setPitcherReplacementMode('custom')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                pitcherReplacementMode === 'custom' ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ✏️ Custom Rates
            </button>
          </div>

          <div className="text-gray-600 text-[11px]">
            {pitcherReplacementMode === 'empty' && (
              <p>Pure omission: leaves vacated slots blank. 0 IP or ER added back.</p>
            )}
            {pitcherReplacementMode === 'league_avg' && (
              <p>
                Replaces vacated IP at 2026 league mean: <span className="font-mono font-bold text-gray-900">{leagueAverages.era.toFixed(2)} ERA</span>, <span className="font-mono font-bold text-gray-900">{leagueAverages.whip.toFixed(2)} WHIP</span>, <span className="font-mono font-bold text-gray-900">{leagueAverages.k9.toFixed(1)} K/9</span>.
              </p>
            )}
            {pitcherReplacementMode === 'replacement_level' && (
              <p>
                Replaces vacated IP at standard waiver streamer rates: <span className="font-mono font-bold text-gray-900">4.65 ERA</span>, <span className="font-mono font-bold text-gray-900">1.38 WHIP</span>, <span className="font-mono font-bold text-gray-900">7.5 K/9</span>.
              </p>
            )}
            {pitcherReplacementMode === 'custom' && (
              <div className="grid grid-cols-3 gap-2 pt-1">
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">ERA</label>
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    max="15"
                    value={customPitcherRates.era}
                    onChange={(e) => setCustomPitcherRates({ ...customPitcherRates, era: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-indigo-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">WHIP</label>
                  <input
                    type="number"
                    step="0.05"
                    min="0"
                    max="3"
                    value={customPitcherRates.whip}
                    onChange={(e) => setCustomPitcherRates({ ...customPitcherRates, whip: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-indigo-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">K/9</label>
                  <input
                    type="number"
                    step="0.5"
                    min="0"
                    max="20"
                    value={customPitcherRates.k9}
                    onChange={(e) => setCustomPitcherRates({ ...customPitcherRates, k9: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-indigo-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Batter Replacement Section */}
        <div className="space-y-3 p-4 rounded-2xl bg-amber-50/40 border border-amber-100">
          <div className="flex items-center justify-between">
            <span className="font-black text-amber-950 flex items-center gap-1.5">
              <span>⚾</span>
              <span>Batter Replacement Strategy & Archetypes</span>
            </span>
            <span className="font-mono text-amber-700 font-bold text-[11px]">
              {vacatedPa} PA Vacated
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-1 bg-white p-1 rounded-xl border border-amber-200/60">
            <button
              type="button"
              onClick={() => setBatterReplacementMode('empty')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'empty' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              🚫 Empty Slot
            </button>
            <button
              type="button"
              onClick={() => setBatterReplacementMode('league_avg')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'league_avg' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              📊 League Avg
            </button>
            <button
              type="button"
              onClick={() => setBatterReplacementMode('replacement_level')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'replacement_level' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ⚖️ Waiver Bat
            </button>
            <button
              type="button"
              onClick={() => setBatterReplacementMode('power_slugger')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'power_slugger' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              💥 Slugger
            </button>
            <button
              type="button"
              onClick={() => setBatterReplacementMode('speedster')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'speedster' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ⚡ Speedster
            </button>
            <button
              type="button"
              onClick={() => setBatterReplacementMode('contact')}
              className={`px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'contact' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              🛡️ High OBP
            </button>
            <button
              type="button"
              onClick={() => setBatterReplacementMode('custom')}
              className={`col-span-2 px-2 py-1.5 font-bold rounded-lg transition text-[11px] cursor-pointer ${
                batterReplacementMode === 'custom' ? 'bg-amber-600 text-white shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              ✏️ Custom Batter Profile
            </button>
          </div>

          <div className="text-gray-600 text-[11px]">
            {batterReplacementMode === 'empty' && (
              <p>Pure omission: models leaving the active hitting slot empty or benched (0 stats added).</p>
            )}
            {batterReplacementMode === 'league_avg' && (
              <p>
                Replaces vacated PA across all categories at 2026 league rates: <span className="font-mono font-bold text-gray-900">.{Math.round(leagueAverages.obp * 1000)} OBP</span>, ~{Math.round(leagueAverages.hrPerPa * 600)} HR, ~{Math.round(leagueAverages.rPerPa * 600)} R, ~{Math.round(leagueAverages.rbiPerPa * 600)} RBI, ~{Math.round(leagueAverages.sbPerPa * 600)} SB per 600 PA.
              </p>
            )}
            {batterReplacementMode === 'replacement_level' && (
              <p>
                Replaces vacated PA with typical waiver hitter rates: <span className="font-mono font-bold text-gray-900">.300 OBP</span>, 14 HR, 50 R, 50 RBI, 5 SB per 600 PA.
              </p>
            )}
            {batterReplacementMode === 'power_slugger' && (
              <p>
                Replaces vacated PA with corner slugger rates: <span className="font-mono font-bold text-gray-900">.330 OBP</span>, 28 HR, 75 R, 85 RBI, 2 SB per 600 PA.
              </p>
            )}
            {batterReplacementMode === 'speedster' && (
              <p>
                Replaces vacated PA with leadoff speedster rates: <span className="font-mono font-bold text-gray-900">.335 OBP</span>, 8 HR, 80 R, 35 RBI, 35 SB per 600 PA.
              </p>
            )}
            {batterReplacementMode === 'contact' && (
              <p>
                Replaces vacated PA with on-base specialist rates: <span className="font-mono font-bold text-gray-900">.365 OBP</span>, 12 HR, 70 R, 55 RBI, 10 SB per 600 PA.
              </p>
            )}
            {batterReplacementMode === 'custom' && (
              <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 pt-1">
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">OBP</label>
                  <input
                    type="number"
                    step="0.005"
                    min="0.100"
                    max="0.500"
                    value={customBatterRates.obp}
                    onChange={(e) => setCustomBatterRates({ ...customBatterRates, obp: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">HR / 600</label>
                  <input
                    type="number"
                    step="1"
                    min="0"
                    max="70"
                    value={customBatterRates.hrPer600}
                    onChange={(e) => setCustomBatterRates({ ...customBatterRates, hrPer600: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">R / 600</label>
                  <input
                    type="number"
                    step="1"
                    min="0"
                    max="150"
                    value={customBatterRates.rPer600}
                    onChange={(e) => setCustomBatterRates({ ...customBatterRates, rPer600: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">RBI / 600</label>
                  <input
                    type="number"
                    step="1"
                    min="0"
                    max="150"
                    value={customBatterRates.rbiPer600}
                    onChange={(e) => setCustomBatterRates({ ...customBatterRates, rbiPer600: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-bold text-gray-500 mb-0.5">SB / 600</label>
                  <input
                    type="number"
                    step="1"
                    min="0"
                    max="100"
                    value={customBatterRates.sbPer600}
                    onChange={(e) => setCustomBatterRates({ ...customBatterRates, sbPer600: parseFloat(e.target.value) || 0 })}
                    className="w-full bg-white border border-amber-200 rounded px-2 py-1 font-mono font-bold"
                  />
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
