// src/components/keepers/tabs/CompSimulatorTab.jsx
import React, { useMemo } from 'react';

// Expected Value (xWAR) and Player Archetype Calibration per Round
const ROUND_VALUATIONS = {
  6: { war: 3.8, archetype: 'Elite SP1 / Franchise Bat', examples: 'Bobby Witt Jr., Tarik Skubal, Chris Sale' },
  7: { war: 3.2, archetype: 'High-K SP2 / Everyday All-Star', examples: 'Cole Ragans, Ketel Marte, George Kirby' },
  8: { war: 2.8, archetype: 'Solid Mid-Rotation / Top-50 Regular', examples: 'Hunter Brown, Joe Ryan, Mookie Betts' },
  9: { war: 2.5, archetype: 'High-Upside Arm / Power Bat', examples: 'Tanner Bibee, Jackson Merrill, Bryson Stott' },
  10: { war: 2.2, archetype: 'Solid Everyday Starter', examples: 'Bryan Woo, Christian Walker, Lane Thomas' },
  11: { war: 2.0, archetype: 'Everyday Regular / SP3', examples: 'Maikel Garcia, Gavin Stone, Cristopher Sanchez' },
  12: { war: 1.9, archetype: 'Reliable Regular / High-K Arm', examples: 'Brandon Lowe, Bailey Ober, MacKenzie Gore' },
  13: { war: 1.8, archetype: 'Category Specialist (Power/Speed)', examples: 'Ceddanne Rafaela, Brice Turang, Ryan Pepiot' },
  14: { war: 1.7, archetype: 'High-Floor Regular / Setup Arm', examples: 'Jake Burger, Reese Olson, Nick Lodolo' },
  15: { war: 1.6, archetype: 'Bullpen Anchor / Closer / Platoon Bat', examples: 'Jeff Hoffman, Jason Foley, Mark Vientos' },
  16: { war: 1.4, archetype: 'High-Leverage Setup / Category Depth', examples: 'Hunter Gaddis, Bryan Abreu, Matt Wallner' },
  17: { war: 1.3, archetype: 'Bullpen Depth / Platoon Slugger', examples: 'Griffin Jax, Lucas Erceg, Tyler O\'Neill' },
  18: { war: 1.2, archetype: 'Middle Reliever / Bench Specialist', examples: 'Chad Green, David Robertson, Kerry Carpenter' },
  19: { war: 1.0, archetype: 'Speculative Flyer / High-Upside Stash', examples: 'Ben Rice, Spencer Schwellenbach, Cade Smith' },
  20: { war: 0.8, archetype: 'End-Game Depth / Speculative Arm', examples: 'Porter Hodge, Justin Martinez, Parker Meadows' },
  21: { war: 0.7, archetype: 'End-Game Reserve Flyer', examples: 'Orion Kerkering, Lawrence Butler, Joey Ortiz' }
};

const REPLACEMENT_LEVEL_WAR = 0.15; // Average expected WAR of late-round picks (R27-32)

export default function CompSimulatorTab({
  simulatedOwner,
  setSimulatedOwner,
  compPicks,
  setSimBoughtRounds,
  setSimSoldRounds,
  DRAFT_MANAGERS,
  resetSimulatorToReal,
  handleSaveCompPicks,
  savingCompPicks,
  simCalculations,
  isCommissioner,
  profile,
  seasonYear,
  compBuyPrices,
  simBoughtRounds,
  compSellPrices,
  simSoldRounds
}) {
  // Calculate Net Talent Differential (Added xWAR vs Forfeited xWAR)
  const talentAnalysis = useMemo(() => {
    let addedWar = 0;
    const boughtRounds = simCalculations.boughtList || [];
    boughtRounds.forEach(b => {
      const val = ROUND_VALUATIONS[b.round];
      addedWar += val ? val.war : 1.0;
    });

    const forfeitedCount = (simCalculations.offsetRounds || []).length;
    const forfeitedWar = forfeitedCount * REPLACEMENT_LEVEL_WAR;
    const netWarDelta = addedWar - forfeitedWar;
    const totalCost = simCalculations.spend || 0;
    const warPerDollar = totalCost > 0 ? (addedWar / totalCost).toFixed(2) : '0.00';

    return {
      addedWar: parseFloat(addedWar.toFixed(1)),
      forfeitedWar: parseFloat(forfeitedWar.toFixed(1)),
      netWarDelta: parseFloat(netWarDelta.toFixed(1)),
      warPerDollar
    };
  }, [simCalculations]);

  return (
    <div className="space-y-6">
      {/* Simulator Controls & Manager Selector */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-wrap justify-between items-center gap-4">
        <div className="flex items-center gap-3">
          <span className="text-2xl">🎟️</span>
          <div>
            <h2 className="text-lg font-black text-white tracking-tight">
              Compensation Pick Valuation & Purchase Calculator
            </h2>
            <p className="text-slate-400 text-xs">
              Evaluate expected WAR return, cost efficiency, and late-round pick offset trade-offs
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold text-slate-400">Manager:</span>
            <select
              value={simulatedOwner}
              onChange={e => {
                const target = e.target.value;
                setSimulatedOwner(target);
                // Reset simulator to target owner's real picks
                const bought = {};
                const sold = {};
                compPicks.filter(p => p.owner === target).forEach(p => {
                  if (p.action_type === 'BOUGHT') bought[p.round_num] = true;
                  if (p.action_type === 'SOLD') sold[p.round_num] = true;
                });
                setSimBoughtRounds(bought);
                setSimSoldRounds(sold);
              }}
              className="bg-slate-950 text-white border border-slate-700 font-black text-xs rounded-xl px-3 py-1.5 focus:outline-none focus:border-teal-400"
            >
              {DRAFT_MANAGERS.map(m => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </div>

          <button
            onClick={resetSimulatorToReal}
            className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl text-xs font-bold transition cursor-pointer"
            title="Reset to official current picks"
          >
            🔄 Reset
          </button>

          <button
            onClick={handleSaveCompPicks}
            disabled={savingCompPicks || simCalculations.simulatedRemaining < 0}
            className={`px-4 py-1.5 rounded-xl text-xs font-black shadow-lg transition cursor-pointer flex items-center gap-1.5 ${
              simCalculations.simulatedRemaining < 0
                ? 'bg-slate-800 text-slate-500 cursor-not-allowed opacity-50'
                : isCommissioner && simulatedOwner !== profile?.owner_name
                ? 'bg-amber-600 hover:bg-amber-500 text-white'
                : 'bg-teal-500 hover:bg-teal-400 text-slate-950 ring-1 ring-teal-300'
            }`}
          >
            {savingCompPicks
              ? 'Saving...'
              : isCommissioner && simulatedOwner !== profile?.owner_name
              ? `👑 Commish Save for ${simulatedOwner}`
              : '💾 Submit Compensation Picks'}
          </button>
        </div>
      </div>

      {/* Real-Time Financial & Budget Ledgers */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm">
          <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Base Budget</div>
          <div className="text-xl font-black text-teal-400 mt-1">${simCalculations.baseBudget}</div>
          <div className="text-[10px] text-slate-500 mt-0.5">$100 ± finish & trades</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm">
          <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Keeper Spend</div>
          <div className="text-xl font-black text-amber-400 mt-1">${simCalculations.keeperSpend}</div>
          <div className="text-[10px] text-slate-500 mt-0.5">Rounds 1–{seasonYear >= 2027 ? 6 : 5} locked</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm">
          <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Comp Spend</div>
          <div className="text-xl font-black text-purple-400 mt-1">${simCalculations.spend}</div>
          <div className="text-[10px] text-slate-500 mt-0.5">{simCalculations.boughtList.length} pick(s) bought</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-sm">
          <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Comp Income</div>
          <div className="text-xl font-black text-emerald-400 mt-1">+${simCalculations.income}</div>
          <div className="text-[10px] text-slate-500 mt-0.5">{Object.values(simSoldRounds).filter(Boolean).length} pick(s) sold</div>
        </div>

        <div className={`rounded-2xl p-4 shadow-sm border ${
          simCalculations.simulatedRemaining < 0
            ? 'bg-rose-950/40 border-rose-500/60'
            : 'bg-slate-900 border-slate-800'
        }`}>
          <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Remaining Budget</div>
          <div className={`text-xl font-black mt-1 ${
            simCalculations.simulatedRemaining < 0 ? 'text-rose-400' : 'text-teal-300'
          }`}>
            ${simCalculations.simulatedRemaining}
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">
            {simCalculations.simulatedRemaining === 0 ? 'Fully optimized ($0 cash)' : simCalculations.simulatedRemaining > 0 ? 'Surplus cash available' : 'Budget Deficit!'}
          </div>
        </div>
      </div>

      {/* Valuation & Talent Efficiency Diagnostic Box */}
      <div className="bg-gradient-to-r from-slate-900 via-indigo-950/40 to-slate-900 border border-indigo-500/30 rounded-3xl p-5 shadow-xl space-y-4">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 border-b border-indigo-500/20 pb-3">
          <div className="flex items-center gap-2">
            <span className="text-xl">📈</span>
            <div>
              <h3 className="font-black text-sm text-indigo-200">
                Talent Expected Value (xWAR) & Roster Differential
              </h3>
              <p className="text-[11px] text-slate-400">
                Net gain from adding early/mid compensation rounds versus forfeiting late draft picks
              </p>
            </div>
          </div>

          <div className="flex items-center gap-4 text-xs font-bold">
            <div className="text-right">
              <span className="text-slate-400 block text-[10px] uppercase">Net Talent Gain</span>
              <span className="font-mono font-black text-emerald-400 text-base">
                +{talentAnalysis.netWarDelta} xWAR
              </span>
            </div>
            <div className="text-right">
              <span className="text-slate-400 block text-[10px] uppercase">Capital Efficiency</span>
              <span className="font-mono font-black text-cyan-300 text-base">
                {talentAnalysis.warPerDollar} WAR/$
              </span>
            </div>
          </div>
        </div>

        {/* Detailed Differential Breakdown */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
          <div className="bg-slate-950/70 p-3.5 rounded-2xl border border-slate-800 space-y-2">
            <div className="flex items-center justify-between font-black text-teal-300">
              <span>🎟️ Added Picks (Inserted at Round End):</span>
              <span className="font-mono text-emerald-400">+{talentAnalysis.addedWar} WAR</span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {simCalculations.boughtList.length === 0 ? (
                <span className="text-slate-500 italic">No compensation picks added.</span>
              ) : (
                simCalculations.boughtList.map(b => {
                  const val = ROUND_VALUATIONS[b.round];
                  return (
                    <span
                      key={b.round}
                      className="px-2.5 py-1 bg-teal-950/80 text-teal-300 border border-teal-700/60 rounded-xl font-bold text-[11px] flex items-center gap-1.5"
                    >
                      <span>Round {b.round}</span>
                      <span className="text-[10px] text-slate-400 font-mono">(-${b.cost})</span>
                      <span className="text-[10px] text-emerald-400 font-mono font-black">~{val?.war} WAR</span>
                    </span>
                  );
                })
              )}
            </div>
          </div>

          <div className="bg-slate-950/70 p-3.5 rounded-2xl border border-slate-800 space-y-2">
            <div className="flex items-center justify-between font-black text-rose-300">
              <span>🚫 Forfeited Late Picks (End-of-Draft Offsets):</span>
              <span className="font-mono text-rose-400">-{talentAnalysis.forfeitedWar} WAR</span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {simCalculations.offsetRounds.length === 0 ? (
                <span className="text-slate-500 italic">No picks forfeited (standard 32-player roster).</span>
              ) : (
                simCalculations.offsetRounds.map(r => (
                  <span
                    key={r}
                    className="px-2.5 py-1 bg-rose-950/60 text-rose-300 border border-rose-800/60 rounded-xl font-bold text-[11px] flex items-center gap-1.5"
                  >
                    <span>Round {r}</span>
                    <span className="text-[10px] text-slate-400 font-mono">(Lost)</span>
                    <span className="text-[10px] text-slate-400 font-mono">~{REPLACEMENT_LEVEL_WAR} WAR</span>
                  </span>
                ))
              )}
            </div>
          </div>
        </div>

        <div className="text-[11px] text-slate-400 flex items-center justify-between pt-1">
          <span>
            ⚖️ <strong>Roster Rule:</strong> Fixed 32-man rosters. Every purchased pick in Rounds 6–20 offsets a late pick from the tail of the draft (Rounds 27–32).
          </span>
          <span className="text-indigo-300 font-bold">
            Average Gain: ~{(talentAnalysis.netWarDelta / Math.max(1, simCalculations.boughtList.length)).toFixed(2)} WAR / pick
          </span>
        </div>
      </div>

      {/* Interactive Purchase Board */}
      <div className="bg-slate-900 border border-slate-800 rounded-3xl p-6 shadow-xl space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
          <div>
            <h3 className="font-black text-base text-white flex items-center gap-2">
              <span>🛍️</span>
              <span>Purchase Additional Draft Picks ({seasonYear >= 2027 ? 'Rounds 7–21' : 'Rounds 6–20'})</span>
            </h3>
            <p className="text-xs text-slate-400">
              Click any round card to toggle purchasing. Compare cost against projected WAR return and talent tier.
            </p>
          </div>
          <span className="text-xs font-bold text-teal-400 bg-teal-950/60 px-3 py-1 rounded-xl border border-teal-800/60">
            Graduated Cost Scale: $15 down to $1
          </span>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {Object.keys(compBuyPrices).map(rStr => {
            const r = parseInt(rStr, 10);
            const cost = compBuyPrices[r];
            const isBought = !!simBoughtRounds[r];
            const val = ROUND_VALUATIONS[r] || { war: 1.0, archetype: 'Contributor', examples: '' };
            const efficiency = (val.war / cost).toFixed(2);
            const isSweetSpot = r === 15 || r === 19 || r === 20;

            return (
              <div
                key={r}
                onClick={() => {
                  setSimBoughtRounds(prev => ({
                    ...prev,
                    [r]: !prev[r]
                  }));
                }}
                className={`p-3.5 rounded-2xl border transition-all cursor-pointer relative flex flex-col justify-between ${
                  isBought
                    ? 'bg-teal-950/40 border-teal-400 ring-2 ring-teal-400/30 shadow-lg scale-[1.02]'
                    : 'bg-slate-950/80 border-slate-800 hover:border-slate-700 hover:bg-slate-900/60'
                }`}
              >
                {isSweetSpot && (
                  <span className="absolute -top-2 -right-1 px-2 py-0.2 bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 font-black text-[9px] rounded-full shadow-md uppercase tracking-wider">
                    {r === 15 ? '🔥 Sweet Spot' : '⚡ High ROI'}
                  </span>
                )}

                <div>
                  <div className="flex items-center justify-between">
                    <span className="font-black text-sm text-white">Round {r}</span>
                    <span className="font-mono font-black text-amber-400 text-xs">${cost}</span>
                  </div>

                  <div className="mt-2 space-y-1">
                    <div className="flex items-center justify-between text-[11px]">
                      <span className="text-slate-400">Expected WAR:</span>
                      <span className="font-mono font-bold text-emerald-400">~{val.war}</span>
                    </div>
                    <div className="flex items-center justify-between text-[11px]">
                      <span className="text-slate-400">Cost Ratio:</span>
                      <span className="font-mono font-bold text-cyan-300">{efficiency} WAR/$</span>
                    </div>
                  </div>

                  <div className="mt-2.5 pt-2 border-t border-slate-800/80">
                    <div className="text-[10px] font-bold text-slate-300 truncate" title={val.archetype}>
                      {val.archetype}
                    </div>
                    <div className="text-[9px] text-slate-500 truncate mt-0.5" title={val.examples}>
                      {val.examples}
                    </div>
                  </div>
                </div>

                <div className={`mt-3 py-1 text-center font-black text-[10px] rounded-xl transition ${
                  isBought
                    ? 'bg-teal-500 text-slate-950'
                    : 'bg-slate-900 text-slate-400 hover:text-white'
                }`}>
                  {isBought ? '✓ PURCHASED' : '+ ADD COMP PICK'}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Interactive Sell Board */}
      <div className="bg-slate-900 border border-slate-800 rounded-3xl p-6 shadow-xl space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
          <div>
            <h3 className="font-black text-base text-white flex items-center gap-2">
              <span>💵</span>
              <span>Sell Draft Picks for Budget Income ({seasonYear >= 2027 ? 'Rounds 7–10' : 'Rounds 6–9'})</span>
            </h3>
            <p className="text-xs text-slate-400">
              Forfeit an early pick to generate budget cash and finance high-cost elite keepers (e.g., Ohtani $40, Judge $31).
            </p>
          </div>
          <span className="text-xs font-bold text-emerald-400 bg-emerald-950/60 px-3 py-1 rounded-xl border border-emerald-800/60">
            Cash-In Scale: +$8, +$4, +$2, +$1
          </span>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-3">
          {Object.keys(compSellPrices).map(rStr => {
            const r = parseInt(rStr, 10);
            const income = compSellPrices[r];
            const isSold = !!simSoldRounds[r];
            const val = ROUND_VALUATIONS[r];

            return (
              <div
                key={r}
                onClick={() => {
                  setSimSoldRounds(prev => ({
                    ...prev,
                    [r]: !prev[r]
                  }));
                }}
                className={`p-4 rounded-2xl border transition-all cursor-pointer flex flex-col justify-between ${
                  isSold
                    ? 'bg-emerald-950/40 border-emerald-400 ring-2 ring-emerald-400/30 shadow-lg scale-[1.02]'
                    : 'bg-slate-950/80 border-slate-800 hover:border-slate-700 hover:bg-slate-900/60'
                }`}
              >
                <div>
                  <div className="flex items-center justify-between">
                    <span className="font-black text-sm text-white">Round {r}</span>
                    <span className="font-mono font-black text-emerald-400 text-sm">+${income}</span>
                  </div>

                  <p className="text-[11px] text-slate-400 mt-2">
                    Trades away your Round {r} selection (~{val?.war} WAR) to inject +${income} cash into keeper budget.
                  </p>
                </div>

                <div className={`mt-3 py-1 text-center font-black text-[10px] rounded-xl transition ${
                  isSold
                    ? 'bg-emerald-500 text-slate-950'
                    : 'bg-slate-900 text-slate-400 hover:text-white'
                }`}>
                  {isSold ? '✓ SOLD FOR CASH' : '- SELL FOR CASH'}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
