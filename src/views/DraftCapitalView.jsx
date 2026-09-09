// src/views/DraftCapitalView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import defaultDraftAssetTrades from '../data/draftAssetTrades2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';

const DRAFT_OWNERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"];

function compute2027DraftPicks(draftTrades = []) {
  const owners = [...DRAFT_OWNERS].sort();
  const picks = [];

  // Base draft order: 27 rounds (Rounds 6 through 32), 1 pick per owner per round (243 total picks)
  for (let round = 6; round <= 32; round++) {
    owners.forEach(owner => {
      picks.push({
        round,
        originalOwner: owner,
        currentOwner: owner,
        isTraded: false,
        tradeDetails: null,
      });
    });
  }

  // Filter for draft asset trades
  const assetTrades = (draftTrades || []).filter(t =>
    t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget'
  );

  assetTrades.forEach(trade => {
    const round = trade.round_num;
    if (!round) return;
    const sending = trade.sending_owner === 'Dan' ? 'Daniel' : trade.sending_owner;
    const receiving = trade.receiving_owner === 'Dan' ? 'Daniel' : trade.receiving_owner;

    // Find the pick in this round currently owned by 'sending'
    const pick = picks.find(p => p.round === round && p.currentOwner === sending);
    if (pick) {
      pick.currentOwner = receiving;
      pick.isTraded = true;
      pick.tradeDetails = trade;
    }
  });

  return picks;
}

export default function DraftCapitalView({ currentUser = 'Daniel' }) {
  const [draftTrades, setDraftTrades] = useState(defaultDraftAssetTrades);
  const [compPicks, setCompPicks] = useState(defaultCompPicks);
  const [keepers, setKeepers] = useState(defaultKeepers);
  const [loading, setLoading] = useState(true);

  const [activeSubTab, setActiveSubTab] = useState('board'); // 'board' | 'ledgers' | 'history' | '2026board'
  const [selectedOwner, setSelectedOwner] = useState(currentUser);
  const [roundFilter, setRoundFilter] = useState('ALL');

  useEffect(() => {
    async function loadData() {
      setLoading(true);
      try {
        const [tradesRes, compRes, keepersRes] = await Promise.all([
          supabase.from('draft_asset_trades').select('*').order('trade_id', { ascending: true }),
          supabase.from('draft_compensation_picks').select('*').order('round_num', { ascending: true }),
          supabase.from('draft_keepers').select('*').order('team_id', { ascending: true }),
        ]);

        if (tradesRes.data?.length > 0) setDraftTrades(tradesRes.data);
        if (compRes.data?.length > 0) setCompPicks(compRes.data);
        if (keepersRes.data?.length > 0) setKeepers(keepersRes.data);
      } catch (err) {
        console.warn('Using local fallback for draft assets:', err);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  const computedPicks = useMemo(() => {
    return compute2027DraftPicks(draftTrades);
  }, [draftTrades]);

  // Compute owner statistics
  const ownerStats = useMemo(() => {
    const stats = {};
    DRAFT_OWNERS.forEach(owner => {
      const ownedPicks = computedPicks.filter(p => p.currentOwner === owner);
      const acquired = computedPicks.filter(p => p.currentOwner === owner && p.originalOwner !== owner);
      const tradedAway = computedPicks.filter(p => p.originalOwner === owner && p.currentOwner !== owner);
      stats[owner] = {
        owner,
        total: ownedPicks.length,
        diff: ownedPicks.length - 27,
        acquired,
        tradedAway,
        ownedPicks,
      };
    });
    return stats;
  }, [computedPicks]);

  // Distinct trades involving draft assets
  const assetTradesList = useMemo(() => {
    return (draftTrades || []).filter(t =>
      t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget'
    );
  }, [draftTrades]);

  // Filtered rounds for round-by-round board
  const rounds = useMemo(() => {
    const list = [];
    for (let r = 6; r <= 32; r++) list.push(r);
    if (roundFilter === 'ALL') return list;
    if (roundFilter === 'EARLY') return list.filter(r => r <= 14);
    if (roundFilter === 'MID') return list.filter(r => r >= 15 && r <= 23);
    if (roundFilter === 'LATE') return list.filter(r => r >= 24);
    return list;
  }, [roundFilter]);

  if (loading) {
    return (
      <div className="py-24 text-center">
        <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-teal-500 mb-3"></div>
        <div className="text-slate-400 text-sm font-semibold">Loading draft capital & future picks...</div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header Banner */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div className="absolute -top-12 -right-12 w-64 h-64 bg-amber-500/10 rounded-full blur-3xl pointer-events-none" />
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 relative z-10">
          <div>
            <div className="flex items-center gap-3">
              <span className="text-3xl">🎟️</span>
              <div>
                <h1 className="text-2xl font-black text-white tracking-tight">Draft Capital & Pick Board</h1>
                <p className="text-slate-400 text-xs mt-0.5">
                  Future 2027 draft assets, trade ledgers, compensation offsets, and historical 2026 draft structure
                </p>
              </div>
            </div>
          </div>

          {/* Subtabs */}
          <div className="flex flex-wrap items-center bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-bold">
            <button
              onClick={() => setActiveSubTab('board')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'board'
                  ? 'bg-teal-500 text-slate-950 shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              📊 2027 Traded Board
            </button>
            <button
              onClick={() => setActiveSubTab('ledgers')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'ledgers'
                  ? 'bg-amber-500 text-slate-950 shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              📑 Owner Ledgers
            </button>
            <button
              onClick={() => setActiveSubTab('history')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'history'
                  ? 'bg-purple-500 text-white shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              📜 Trade History ({assetTradesList.length})
            </button>
            <button
              onClick={() => setActiveSubTab('2026board')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === '2026board'
                  ? 'bg-blue-600 text-white shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              🏛️ 2026 Ground Truth
            </button>
          </div>
        </div>

        {/* Quick Manager Capital Summary Cards */}
        <div className="grid grid-cols-3 sm:grid-cols-5 md:grid-cols-9 gap-2 mt-6 pt-4 border-t border-slate-800">
          {DRAFT_OWNERS.map(owner => {
            const stat = ownerStats[owner];
            const diff = stat?.diff || 0;
            const isMe = owner === currentUser;
            return (
              <button
                key={owner}
                onClick={() => {
                  setSelectedOwner(owner);
                  setActiveSubTab('ledgers');
                }}
                className={`p-2 rounded-xl text-center transition-all cursor-pointer border ${
                  selectedOwner === owner && activeSubTab === 'ledgers'
                    ? 'bg-amber-500/20 border-amber-500/60 shadow-lg'
                    : isMe
                    ? 'bg-slate-800/80 border-slate-700 hover:border-slate-600'
                    : 'bg-slate-950/60 border-slate-800/60 hover:bg-slate-800/40'
                }`}
              >
                <div className="text-[11px] font-bold text-slate-300 truncate">{owner}</div>
                <div className="text-sm font-black text-white mt-0.5">{stat?.total ?? 27}</div>
                <div className={`text-[10px] font-extrabold ${diff > 0 ? 'text-emerald-400' : diff < 0 ? 'text-rose-400' : 'text-slate-500'}`}>
                  {diff > 0 ? `+${diff}` : diff === 0 ? '±0' : `${diff}`}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* SUBTAB 1: 2027 TRADED PICK BOARD */}
      {activeSubTab === 'board' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <span className="text-xs font-black uppercase tracking-wider text-slate-400">Rounds Filter:</span>
              <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold">
                {[
                  { id: 'ALL', label: 'All (R6–32)' },
                  { id: 'EARLY', label: 'Early (R6–14)' },
                  { id: 'MID', label: 'Mid (R15–23)' },
                  { id: 'LATE', label: 'Late (R24–32)' },
                ].map(f => (
                  <button
                    key={f.id}
                    onClick={() => setRoundFilter(f.id)}
                    className={`px-2.5 py-1 rounded-md transition-colors cursor-pointer ${
                      roundFilter === f.id
                        ? 'bg-slate-800 text-teal-400 shadow-xs'
                        : 'text-slate-400 hover:text-white'
                    }`}
                  >
                    {f.label}
                  </button>
                ))}
              </div>
            </div>

            <div className="flex items-center gap-4 text-xs">
              <span className="inline-flex items-center gap-1.5 text-slate-400">
                <span className="w-2.5 h-2.5 rounded-full bg-slate-700 border border-slate-600"></span> Original Pick
              </span>
              <span className="inline-flex items-center gap-1.5 text-amber-300 font-bold">
                <span className="w-2.5 h-2.5 rounded-full bg-amber-500 border border-amber-400"></span> Traded / Acquired Pick
              </span>
            </div>
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-3 text-center w-20">Round</th>
                  {DRAFT_OWNERS.map(owner => (
                    <th key={owner} className="py-3 px-3 text-center min-w-[110px]">
                      <div className="font-extrabold text-slate-200">{owner}</div>
                      <div className="text-[10px] text-slate-400 font-semibold lowercase">slot</div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {rounds.map(roundNum => (
                  <tr key={roundNum} className="hover:bg-slate-800/40 transition-colors">
                    <td className="py-2.5 px-3 text-center font-black text-slate-400 bg-slate-950/40">
                      R{roundNum}
                    </td>
                    {DRAFT_OWNERS.map(slotOwner => {
                      const pick = computedPicks.find(p => p.round === roundNum && p.originalOwner === slotOwner);
                      const isTraded = pick?.isTraded;
                      const currentOwner = pick?.currentOwner || slotOwner;

                      return (
                        <td key={slotOwner} className="py-2 px-2 text-center">
                          {isTraded ? (
                            <div
                              title={`Acquired by ${currentOwner} from ${slotOwner} (Trade #${pick?.tradeDetails?.trade_id || ''})`}
                              className="bg-amber-500/15 border border-amber-500/40 text-amber-300 rounded-lg p-1.5 transition-all shadow-xs"
                            >
                              <div className="font-black text-xs text-amber-200 flex items-center justify-center gap-1">
                                <span>🎟️</span>
                                <span>{currentOwner}</span>
                              </div>
                              <div className="text-[9px] text-amber-400/80 font-bold tracking-tight">
                                ex-{slotOwner} #{pick?.tradeDetails?.trade_id}
                              </div>
                            </div>
                          ) : (
                            <div className="bg-slate-950/70 border border-slate-800/80 text-slate-400 rounded-lg p-1.5">
                              <span className="font-semibold text-slate-400">{currentOwner}</span>
                            </div>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="bg-slate-950 border-t-2 border-slate-800 font-black text-xs">
                  <td className="py-3 px-3 text-center text-slate-400">Total</td>
                  {DRAFT_OWNERS.map(owner => {
                    const count = ownerStats[owner]?.total || 27;
                    const diff = ownerStats[owner]?.diff || 0;
                    return (
                      <td key={owner} className="py-3 px-3 text-center">
                        <span className="text-white font-black">{count}</span>{' '}
                        <span className={`text-[10px] ${diff > 0 ? 'text-emerald-400' : diff < 0 ? 'text-rose-400' : 'text-slate-500'}`}>
                          ({diff > 0 ? `+${diff}` : diff})
                        </span>
                      </td>
                    );
                  })}
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      )}

      {/* SUBTAB 2: OWNER PICK LEDGERS */}
      {activeSubTab === 'ledgers' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-6">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <span className="text-xs font-black uppercase tracking-wider text-slate-400">Select Manager:</span>
              <select
                value={selectedOwner}
                onChange={e => setSelectedOwner(e.target.value)}
                className="bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-sm font-bold text-white focus:outline-none focus:border-amber-500 cursor-pointer"
              >
                {DRAFT_OWNERS.map(o => (
                  <option key={o} value={o}>
                    {o} ({ownerStats[o]?.total || 27} picks • {ownerStats[o]?.diff >= 0 ? `+${ownerStats[o]?.diff}` : ownerStats[o]?.diff})
                  </option>
                ))}
              </select>
            </div>

            <div className="text-xs text-amber-400 font-bold bg-amber-500/10 border border-amber-500/20 px-3 py-1.5 rounded-lg">
              {selectedOwner} holds <strong className="text-white">{ownerStats[selectedOwner]?.total}</strong> total picks for 2027
            </div>
          </div>

          {/* Currently Owned Picks */}
          <div>
            <div className="text-xs font-black uppercase tracking-wider text-slate-400 mb-2">
              Currently Owned 2027 Picks ({ownerStats[selectedOwner]?.ownedPicks?.length || 0})
            </div>
            <div className="overflow-x-auto rounded-xl border border-slate-800">
              <table className="w-full text-left text-xs border-collapse">
                <thead>
                  <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                    <th className="py-3 px-4 w-28">Round</th>
                    <th className="py-3 px-4 w-32">Status</th>
                    <th className="py-3 px-4 w-36">Original Owner</th>
                    <th className="py-3 px-4">Trade Details & Rationale</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                  {ownerStats[selectedOwner]?.ownedPicks.map((pick, idx) => {
                    const isAcquired = pick.originalOwner !== selectedOwner;
                    return (
                      <tr key={`ledger-${selectedOwner}-${idx}`} className="hover:bg-slate-800/40 transition-colors">
                        <td className="py-2.5 px-4 font-black">
                          <span className={isAcquired ? 'text-amber-400' : 'text-teal-400'}>
                            Round {pick.round}
                          </span>
                        </td>
                        <td className="py-2.5 px-4">
                          <span
                            className={`px-2 py-0.5 rounded text-[10px] font-black uppercase ${
                              isAcquired
                                ? 'bg-amber-500/20 text-amber-300 border border-amber-500/40'
                                : 'bg-teal-500/20 text-teal-300 border border-teal-500/40'
                            }`}
                          >
                            {isAcquired ? '✓ ACQUIRED' : 'ORIGINAL'}
                          </span>
                        </td>
                        <td className="py-2.5 px-4 font-bold text-white">
                          {pick.originalOwner}
                        </td>
                        <td className="py-2.5 px-4 text-slate-300">
                          {isAcquired ? (
                            <span className="text-amber-300/90">
                              Acquired from <strong>{pick.originalOwner}</strong> via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                              {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                            </span>
                          ) : (
                            <span className="text-slate-500">Original slot selection</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Traded Away Picks */}
          {ownerStats[selectedOwner]?.tradedAway.length > 0 && (
            <div>
              <div className="text-xs font-black uppercase tracking-wider text-rose-400 mb-2">
                Original Picks Traded Away by {selectedOwner} ({ownerStats[selectedOwner]?.tradedAway.length})
              </div>
              <div className="overflow-x-auto rounded-xl border border-rose-900/40">
                <table className="w-full text-left text-xs border-collapse">
                  <thead>
                    <tr className="bg-slate-950 text-slate-400 border-b border-rose-900/40 uppercase tracking-wider font-black">
                      <th className="py-3 px-4 w-28">Round</th>
                      <th className="py-3 px-4 w-32">Status</th>
                      <th className="py-3 px-4 w-36">Current Owner</th>
                      <th className="py-3 px-4">Trade Details</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-rose-900/30 font-medium text-slate-200">
                    {ownerStats[selectedOwner]?.tradedAway.map((pick, idx) => (
                      <tr key={`away-${idx}`} className="hover:bg-rose-950/20 transition-colors">
                        <td className="py-2.5 px-4 font-black text-rose-400 line-through">
                          Round {pick.round}
                        </td>
                        <td className="py-2.5 px-4">
                          <span className="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-rose-500/20 text-rose-300 border border-rose-500/40">
                            TRADED AWAY
                          </span>
                        </td>
                        <td className="py-2.5 px-4 font-bold text-white">
                          {pick.currentOwner}
                        </td>
                        <td className="py-2.5 px-4 text-rose-300/80">
                          Traded to <strong>{pick.currentOwner}</strong> via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                          {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* SUBTAB 3: TRADE HISTORY LOG */}
      {activeSubTab === 'history' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
              Draft Asset Trades ({assetTradesList.length} Transactions)
            </h2>
            <span className="text-xs text-slate-500">
              Source: Google Sheets Trade Log synced to Supabase
            </span>
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-4 w-28">Date</th>
                  <th className="py-3 px-3 text-center w-20">Trade #</th>
                  <th className="py-3 px-4 w-32">Sending Owner</th>
                  <th className="py-3 px-4 w-32">Receiving Owner</th>
                  <th className="py-3 px-4 w-52">Draft Asset Traded</th>
                  <th className="py-3 px-4">Notes & Associated Player Moves</th>
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
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-500/15 border border-amber-500/40 text-amber-300 font-bold text-xs">
                        <span>🎟️</span>
                        <span>{t.asset_name}</span>
                        {t.round_num && <span className="text-[10px] text-amber-400/80">(R{t.round_num})</span>}
                      </span>
                    </td>
                    <td className="py-3 px-4 text-slate-300">
                      {t.notes || `Trade #${t.trade_id}`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* SUBTAB 4: 2026 GROUND TRUTH BOARD */}
      {activeSubTab === '2026board' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
                2026 Ground Truth Draft Board Architecture
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">
                Rounds 1–5 are Keepers; Rounds 6–20 include purchased Comp Picks; Rounds 27–32 show Offset picks.
              </p>
            </div>
            <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold">
              {['ALL', 'KEEPERS', 'EARLY', 'MID', 'LATE'].map(rf => (
                <button
                  key={rf}
                  onClick={() => setRoundFilter(rf)}
                  className={`px-2.5 py-1 rounded-md transition-colors cursor-pointer ${
                    roundFilter === rf
                      ? 'bg-slate-800 text-emerald-400 shadow-xs'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  {rf === 'ALL' ? 'All (R1–32)' : rf === 'KEEPERS' ? 'Keepers (R1–5)' : rf === 'EARLY' ? 'Early (R6–14)' : rf === 'MID' ? 'Mid (R15–23)' : 'Late (R24–32)'}
                </button>
              ))}
            </div>
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse min-w-[950px]">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-3 text-center w-20">Round</th>
                  {DRAFT_OWNERS.map(owner => (
                    <th key={owner} className="py-3 px-3 text-center">
                      <div className="text-slate-200 font-extrabold">{owner}</div>
                    </th>
                  ))}
                  <th className="py-3 px-3 text-center w-36">Comp Pick (End)</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {Array.from({ length: 32 }, (_, i) => i + 1)
                  .filter(rNum => {
                    if (roundFilter === 'KEEPERS') return rNum <= 5;
                    if (roundFilter === 'EARLY') return rNum >= 6 && rNum <= 14;
                    if (roundFilter === 'MID') return rNum >= 15 && rNum <= 23;
                    if (roundFilter === 'LATE') return rNum >= 24 && rNum <= 32;
                    return true;
                  })
                  .map(rNum => {
                    const isKeeperRound = rNum <= 5;
                    const compPicksThisRound = (compPicks || []).filter(
                      cp => cp.round_num === rNum && cp.action_type === 'COMP_BOUGHT'
                    );

                    return (
                      <tr key={rNum} className="hover:bg-slate-800/40 transition-colors">
                        <td className="py-2 px-3 text-center font-black text-slate-400 bg-slate-950/40">
                          R{rNum}
                          {isKeeperRound && <span className="block text-[9px] text-amber-400">Keeper</span>}
                        </td>
                        {DRAFT_OWNERS.map(owner => {
                          if (isKeeperRound) {
                            const keeperObj = (keepers || []).find(
                              k => k.owner === owner && k.keeper_slot === rNum
                            );
                            return (
                              <td key={owner} className="py-2 px-2 text-center">
                                {keeperObj ? (
                                  <div className="bg-amber-500/15 border border-amber-500/30 rounded-lg p-1 text-center">
                                    <div className="font-bold text-[11px] text-amber-200 truncate">
                                      {keeperObj.player_name}
                                    </div>
                                    <div className="text-[9px] text-amber-400/90 font-semibold">
                                      ${keeperObj.cost} • #{keeperObj.rank}
                                    </div>
                                  </div>
                                ) : (
                                  <span className="text-slate-600">-</span>
                                )}
                              </td>
                            );
                          }

                          const isOffset = (compPicks || []).some(
                            cp => cp.owner === owner && cp.round_num === rNum && cp.action_type === 'OFFSET_LOST'
                          );

                          return (
                            <td key={owner} className="py-2 px-2 text-center">
                              {isOffset ? (
                                <div className="bg-rose-500/15 border border-rose-500/40 rounded-lg p-1 text-center">
                                  <div className="text-[10px] font-bold text-rose-400 line-through">Pick Slot</div>
                                  <div className="text-[9px] text-rose-300 font-black">🚫 Offset</div>
                                </div>
                              ) : (
                                <div className="bg-slate-950/70 border border-slate-800 rounded-lg p-1 text-center">
                                  <span className="text-[11px] text-slate-400">Standard</span>
                                </div>
                              )}
                            </td>
                          );
                        })}

                        {/* End of round compensation picks */}
                        <td className="py-2 px-2 text-center">
                          {compPicksThisRound.length > 0 ? (
                            <div className="flex flex-col gap-1">
                              {compPicksThisRound.map(cp => (
                                <span
                                  key={cp.id || `${cp.owner}-${cp.round_num}`}
                                  className="px-1.5 py-0.5 rounded bg-purple-500/20 border border-purple-500/40 text-purple-300 font-bold text-[10px]"
                                >
                                  🎟️ {cp.owner} (${cp.cost_or_income})
                                </span>
                              ))}
                            </div>
                          ) : (
                            <span className="text-slate-600 text-[11px]">-</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
