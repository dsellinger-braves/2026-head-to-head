// src/views/DraftPrepView.jsx
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { supabase } from '../supabaseClient';
import defaultKeepers2026 from '../data/keeperInput2026.json';
import defaultKeepers2027 from '../data/keeperInput2027.json';
import defaultCalculations from '../data/keeperCalculations.json';
import { useAuth } from '../context/useAuth';
import { DRAFT_OWNERS, enrichPlayerWithCalculations, getEffectiveRank } from './draft/utils/draftCalculations';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../utils/headshotUtils';
import TierBoard from '../components/draft/TierBoard';
import {
  loadUserTiers,
  saveUserTiers,
  assignPlayerTier,
  togglePlayerTag,
  setPlayerNote,
  loadUserQueue,
  saveUserQueue,
  generateAutoTiers
} from '../utils/tierStorageService';

export default function DraftPrepView({
  currentUser = 'Daniel',
  seasonYear = 2027,
  onSeasonYearChange,
  onPlayerClick,
  onNavigate
}) {
  const { effectiveOwner } = useAuth();
  const activeOwner = effectiveOwner || currentUser || 'Daniel';

  const [selectedSeason, setSelectedSeason] = useState(seasonYear || 2027);
  const [selectedOwner, setSelectedOwner] = useState(activeOwner);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  // Tier state loaded from tierStorageService
  const [tierState, setTierState] = useState(() => loadUserTiers(selectedSeason, selectedOwner));
  const [queue, setQueue] = useState(() => loadUserQueue());
  const [isQueueOpen, setIsQueueOpen] = useState(true);

  // Sync selectedOwner when effectiveOwner updates
  useEffect(() => {
    if (effectiveOwner) {
      setSelectedOwner(effectiveOwner);
    }
  }, [effectiveOwner]);

  // Sync season when parent prop updates
  useEffect(() => {
    if (seasonYear && seasonYear !== selectedSeason) {
      setSelectedSeason(seasonYear);
    }
  }, [seasonYear, selectedSeason]);

  // Reload tier configuration when owner or season changes
  useEffect(() => {
    const loaded = loadUserTiers(selectedSeason, selectedOwner);
    setTierState(loaded);
  }, [selectedSeason, selectedOwner]);

  // Sync queue state with external updates
  useEffect(() => {
    const handleQueueUpdate = (e) => {
      if (e?.detail) setQueue(e.detail);
    };
    window.addEventListener('draft_queue_updated', handleQueueUpdate);
    return () => window.removeEventListener('draft_queue_updated', handleQueueUpdate);
  }, []);

  // Fetch players and keepers
  useEffect(() => {
    let isMounted = true;
    async function fetchData() {
      setLoading(true);
      try {
        const defaultK = selectedSeason >= 2027
          ? (defaultKeepers2027?.keepers || defaultKeepers2027 || [])
          : (defaultKeepers2026?.keepers || defaultKeepers2026 || []);

        const [kRes, p1, p2, p3, p4] = await Promise.all([
          supabase.from('draft_keepers').select('*').eq('season_year', selectedSeason),
          supabase.from('player-pool').select('*').range(0, 999),
          supabase.from('player-pool').select('*').range(1000, 1999),
          supabase.from('player-pool').select('*').range(2000, 2999),
          supabase.from('player-pool').select('*').range(3000, 3999)
        ]);

        const loadedKeepers = (kRes?.data && kRes.data.length > 0) ? kRes.data : defaultK;

        const rawPool = [
          ...(p1?.data || []),
          ...(p2?.data || []),
          ...(p3?.data || []),
          ...(p4?.data || [])
        ];

        const baseList = rawPool.length > 0
          ? rawPool
          : (defaultCalculations?.players || []).map(cp => ({
              'ESPN PlayerID': String(cp.espn_player_id || cp.player_id || ''),
              id: cp.player_id,
              Player: cp.player_name,
              Position: cp.position || (cp.is_pitcher ? (cp.pitcher_role || 'SP') : 'DH'),
              Team: cp.team || 'MLB',
              Availability: 'Available'
            }));

        // Map keepers
        const keeperMap = new Map();
        loadedKeepers.forEach(k => {
          const rawOwner = k.owner || k.owner_name || k.team_owner || '';
          const owner = (rawOwner === 'Dan' || rawOwner === 'dsellinger') ? 'Daniel' : rawOwner;
          const pid = String(k.espn_player_id || k.player_id || '');
          if (pid) keeperMap.set(pid, { owner, slot: k.keeper_slot, cost: k.cost });
        });

        const enriched = baseList.map(p => {
          const ep = enrichPlayerWithCalculations(p, selectedSeason);
          const pid = String(ep['ESPN PlayerID'] || ep.espn_player_id || ep.id || '');
          const kInfo = keeperMap.get(pid);
          return {
            ...ep,
            Availability: kInfo ? kInfo.owner : 'Available',
            isKeeper: !!kInfo,
            keeperOwner: kInfo ? kInfo.owner : null,
            keeperSlot: kInfo ? kInfo.slot : null,
            keeperCost: kInfo ? kInfo.cost : null
          };
        });

        // Sort by effective rank
        enriched.sort((a, b) => {
          const rankA = getEffectiveRank(a) || 999;
          const rankB = getEffectiveRank(b) || 999;
          return rankA - rankB;
        });

        if (isMounted) {
          setPlayers(enriched);

          // If no custom player tier assignments exist yet, auto-populate from ranks
          const currentTiers = loadUserTiers(selectedSeason, selectedOwner);
          if (!currentTiers.playerTiers || Object.keys(currentTiers.playerTiers).length === 0) {
            const autoAssigned = generateAutoTiers(enriched, currentTiers.tiers);
            const newState = { ...currentTiers, playerTiers: autoAssigned };
            setTierState(newState);
            saveUserTiers(selectedSeason, selectedOwner, newState);
          }
        }
      } catch (err) {
        console.error('Error loading draft prep players:', err);
      } finally {
        if (isMounted) setLoading(false);
      }
    }

    fetchData();
    return () => { isMounted = false; };
  }, [selectedSeason, selectedOwner]);

  // Locked keepers for the selected manager
  const ownerKeepers = useMemo(() => {
    return players.filter(p => p.isKeeper && p.keeperOwner === selectedOwner);
  }, [players, selectedOwner]);

  // Aggregate stats banked from keepers
  const keeperBankedStats = useMemo(() => {
    let hr = 0; let sb = 0; let k = 0; let qs = 0; let r = 0; let rbi = 0;
    ownerKeepers.forEach(p => {
      hr += parseFloat(p.ZIPSHR || p.ESPNHR || 0) || 0;
      sb += parseFloat(p.ZIPSSB || p.ESPNSB || 0) || 0;
      k += parseFloat(p.ZIPSK || p.ESPNK || 0) || 0;
      qs += parseFloat(p.ZIPSQS || p.ESPNQS || 0) || 0;
      r += parseFloat(p.ZIPSR || p.ESPNR || 0) || 0;
      rbi += parseFloat(p.ZIPSRBI || p.ESPNRBI || 0) || 0;
    });
    return { hr: Math.round(hr), sb: Math.round(sb), k: Math.round(k), qs: Math.round(qs), r: Math.round(r), rbi: Math.round(rbi) };
  }, [ownerKeepers]);

  // Calculate upcoming snake draft picks for this manager
  const ownerSnakePicks = useMemo(() => {
    const list = [];
    // Start at Round 6 (pick 46) for 2026, or Round 7 for 2027
    const startRound = selectedSeason >= 2027 ? 7 : 6;
    const ownersList = [...DRAFT_OWNERS];
    let overall = (startRound - 1) * 9 + 1;

    for (let r = startRound; r <= 22; r++) {
      const order = r % 2 === 1 ? [...ownersList] : [...ownersList].reverse();
      order.forEach((owner, idx) => {
        if (owner === selectedOwner) {
          list.push({
            overallPick: overall,
            round: r,
            pickInRound: idx + 1
          });
        }
        overall++;
      });
    }
    return list;
  }, [selectedOwner, selectedSeason]);

  // Tier Mutation Handlers
  const handleMovePlayerToTier = useCallback((playerId, targetTierId) => {
    const updated = assignPlayerTier(selectedSeason, selectedOwner, playerId, targetTierId);
    if (updated) setTierState({ ...updated });
  }, [selectedSeason, selectedOwner]);

  const handleToggleTag = useCallback((playerId, tagId) => {
    const updated = togglePlayerTag(selectedSeason, selectedOwner, playerId, tagId);
    if (updated) setTierState({ ...updated });
  }, [selectedSeason, selectedOwner]);

  const handleUpdateNote = useCallback((playerId, noteText) => {
    const updated = setPlayerNote(selectedSeason, selectedOwner, playerId, noteText);
    if (updated) setTierState({ ...updated });
  }, [selectedSeason, selectedOwner]);

  const handleAddTier = useCallback((newTier) => {
    const updated = {
      ...tierState,
      tiers: [...tierState.tiers, newTier]
    };
    setTierState(updated);
    saveUserTiers(selectedSeason, selectedOwner, updated);
  }, [tierState, selectedSeason, selectedOwner]);

  const handleDeleteTier = useCallback((tierIdToDelete) => {
    const fallbackTier = tierState.tiers.find(t => t.id !== tierIdToDelete)?.id || 'tier-5';
    const updatedPlayerTiers = { ...tierState.playerTiers };
    Object.keys(updatedPlayerTiers).forEach(pid => {
      if (updatedPlayerTiers[pid] === tierIdToDelete) {
        updatedPlayerTiers[pid] = fallbackTier;
      }
    });
    const updated = {
      ...tierState,
      tiers: tierState.tiers.filter(t => t.id !== tierIdToDelete),
      playerTiers: updatedPlayerTiers
    };
    setTierState(updated);
    saveUserTiers(selectedSeason, selectedOwner, updated);
  }, [tierState, selectedSeason, selectedOwner]);

  const handleResetToAutoTiers = useCallback(() => {
    if (!window.confirm('Reset all player tier placements to consensus rankings? Your custom tags and notes will be preserved.')) {
      return;
    }
    const autoAssigned = generateAutoTiers(players, tierState.tiers);
    const updated = { ...tierState, playerTiers: autoAssigned };
    setTierState(updated);
    saveUserTiers(selectedSeason, selectedOwner, updated);
  }, [players, tierState, selectedSeason, selectedOwner]);

  // Queue Handlers
  const handleAddToQueue = useCallback((player) => {
    const pid = String(player['ESPN PlayerID'] || player.espn_player_id || player.id || '');
    if (queue.some(p => String(p['ESPN PlayerID'] || p.espn_player_id || p.id) === pid)) return;
    const next = [...queue, player];
    setQueue(next);
    saveUserQueue(next);
  }, [queue]);

  const handleRemoveFromQueue = useCallback((playerId) => {
    const pid = String(playerId);
    const next = queue.filter(p => String(p['ESPN PlayerID'] || p.espn_player_id || p.id) !== pid);
    setQueue(next);
    saveUserQueue(next);
  }, [queue]);

  const handleMoveQueueItem = useCallback((fromIndex, toIndex) => {
    if (toIndex < 0 || toIndex >= queue.length) return;
    const next = [...queue];
    const [moved] = next.splice(fromIndex, 1);
    next.splice(toIndex, 0, moved);
    setQueue(next);
    saveUserQueue(next);
  }, [queue]);

  const handleClearQueue = useCallback(() => {
    if (window.confirm('Clear your entire draft queue?')) {
      setQueue([]);
      saveUserQueue([]);
    }
  }, []);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-4 md:p-8 space-y-6">
      {/* Header Banner */}
      <div className="bg-slate-900 border border-slate-800 rounded-3xl p-6 shadow-2xl space-y-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <span className="text-4xl">📋</span>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-2xl md:text-3xl font-black text-white tracking-tight">
                  Draft Prep & Strategy Board
                </h1>
                <span className="px-2.5 py-0.5 rounded-full text-xs font-black bg-teal-500/20 text-teal-300 border border-teal-500/40">
                  Pre-Draft War Room
                </span>
              </div>
              <p className="text-slate-400 text-xs mt-1">
                Customize positional tiers, assign targets, add private scouting notes, and build your draft night queue outside the lobby.
              </p>
            </div>
          </div>

          {/* Quick Action Navigation */}
          <div className="flex items-center gap-2 flex-wrap">
            <button
              onClick={() => {
                if (onNavigate) onNavigate('draft');
                else window.location.hash = '#/draft/lobby';
              }}
              className="px-4 py-2 bg-amber-500 hover:bg-amber-400 text-slate-950 font-black text-xs rounded-xl shadow-lg transition cursor-pointer flex items-center gap-1.5 ring-1 ring-amber-300"
            >
              <span>🎯</span>
              <span>Enter Live Draft Lobby</span>
            </button>

            <button
              onClick={() => {
                if (onNavigate) onNavigate('capital');
                else window.location.hash = '#/draft/traded-board';
              }}
              className="px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 font-bold text-xs rounded-xl transition cursor-pointer flex items-center gap-1.5"
            >
              <span>🎟️</span>
              <span>Draft Capital</span>
            </button>

            <button
              onClick={() => {
                if (onNavigate) onNavigate('keepers');
                else window.location.hash = '#/keepers/summary';
              }}
              className="px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 font-bold text-xs rounded-xl transition cursor-pointer flex items-center gap-1.5"
            >
              <span>💎</span>
              <span>Keepers & Budget</span>
            </button>
          </div>
        </div>

        {/* Manager Selector & Season Switcher */}
        <div className="flex flex-wrap items-center justify-between gap-4 pt-4 border-t border-slate-800">
          <div className="flex items-center gap-3">
            <span className="text-xs font-bold text-slate-400">Prepping Board For:</span>
            <select
              value={selectedOwner}
              onChange={e => setSelectedOwner(e.target.value)}
              className="bg-slate-950 text-white border border-slate-700 font-black text-xs rounded-xl px-3 py-1.5 focus:outline-none focus:border-teal-400"
            >
              {DRAFT_OWNERS.map(owner => (
                <option key={owner} value={owner}>
                  {owner} {owner === activeOwner ? '⭐ (You)' : ''}
                </option>
              ))}
            </select>

            {/* Season Switcher */}
            <div className="flex bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-bold">
              <button
                onClick={() => {
                  setSelectedSeason(2027);
                  if (onSeasonYearChange) onSeasonYearChange(2027);
                }}
                className={`px-3 py-1 rounded-lg transition-colors cursor-pointer ${
                  selectedSeason === 2027 ? 'bg-purple-600 text-white font-black' : 'text-slate-400 hover:text-white'
                }`}
              >
                🚀 2027 Prep
              </button>
              <button
                onClick={() => {
                  setSelectedSeason(2026);
                  if (onSeasonYearChange) onSeasonYearChange(2026);
                }}
                className={`px-3 py-1 rounded-lg transition-colors cursor-pointer ${
                  selectedSeason === 2026 ? 'bg-teal-500 text-slate-950 font-black' : 'text-slate-400 hover:text-white'
                }`}
              >
                🏛️ 2026 Archive
              </button>
            </div>
          </div>

          {/* Tools & Reset */}
          <div className="flex items-center gap-2">
            <button
              onClick={handleResetToAutoTiers}
              className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-bold rounded-xl transition cursor-pointer"
              title="Reset tier placements to default rankings"
            >
              🔄 Auto-Tier by Rank
            </button>

            <button
              onClick={() => setIsQueueOpen(prev => !prev)}
              className={`px-3 py-1.5 rounded-xl text-xs font-black transition cursor-pointer flex items-center gap-1.5 ${
                isQueueOpen
                  ? 'bg-amber-500 text-slate-950 shadow-md'
                  : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
              }`}
            >
              <span>★</span>
              <span>Draft Queue ({queue.length})</span>
            </button>
          </div>
        </div>

        {/* Manager's Locked Keeper Summary Card */}
        <div className="bg-slate-950/80 border border-slate-800/80 rounded-2xl p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <span className="text-xs font-black text-teal-400 uppercase tracking-wider">
                🔒 Locked Keepers ({ownerKeepers.length} / {selectedSeason >= 2027 ? 6 : 5} Slots)
              </span>
              <span className="text-[10px] text-slate-500">Rounds 1–{selectedSeason >= 2027 ? 6 : 5} Locked</span>
            </div>
            <div className="flex flex-wrap gap-2 pt-1">
              {ownerKeepers.length === 0 ? (
                <span className="text-xs text-slate-500 italic">No keepers locked yet for {selectedOwner}.</span>
              ) : (
                ownerKeepers.map(k => (
                  <span
                    key={k.id || k.Player}
                    className="px-2.5 py-1 bg-slate-900 border border-slate-700 rounded-xl text-xs font-bold text-white flex items-center gap-1.5"
                  >
                    <span>{k.Player}</span>
                    <span className="text-[10px] text-teal-400 font-mono">({k.Position})</span>
                    {k.keeperCost !== undefined && (
                      <span className="text-[10px] text-amber-400 font-mono font-bold">${k.keeperCost}</span>
                    )}
                  </span>
                ))
              )}
            </div>
          </div>

          {/* Stats Banked From Keepers */}
          {ownerKeepers.length > 0 && (
            <div className="flex items-center gap-4 bg-slate-900/60 px-4 py-2 rounded-xl border border-slate-800 flex-shrink-0 text-xs">
              <div className="text-center">
                <div className="text-[10px] text-slate-500 font-bold uppercase">Banked HR</div>
                <div className="font-mono font-black text-emerald-400 text-sm">{keeperBankedStats.hr}</div>
              </div>
              <div className="text-center">
                <div className="text-[10px] text-slate-500 font-bold uppercase">Banked SB</div>
                <div className="font-mono font-black text-cyan-400 text-sm">{keeperBankedStats.sb}</div>
              </div>
              <div className="text-center">
                <div className="text-[10px] text-slate-500 font-bold uppercase">Banked K</div>
                <div className="font-mono font-black text-purple-400 text-sm">{keeperBankedStats.k}</div>
              </div>
              <div className="text-center">
                <div className="text-[10px] text-slate-500 font-bold uppercase">Banked QS</div>
                <div className="font-mono font-black text-amber-400 text-sm">{keeperBankedStats.qs}</div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Main Workspace: Tier Board + Slide-out Queue */}
      <div className="flex gap-6 items-start">
        {/* Main Tier Board */}
        <div className="flex-1 min-w-0">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-20 space-y-3">
              <div className="w-10 h-10 border-4 border-slate-800 border-t-teal-400 rounded-full animate-spin" />
              <p className="text-xs font-bold text-slate-400 uppercase tracking-widest animate-pulse">
                Loading Player Pool & Custom Tiers...
              </p>
            </div>
          ) : (
            <TierBoard
              players={players}
              tiers={tierState.tiers}
              playerTiers={tierState.playerTiers}
              playerTags={tierState.playerTags}
              playerNotes={tierState.playerNotes}
              queue={queue}
              onMovePlayerToTier={handleMovePlayerToTier}
              onToggleTag={handleToggleTag}
              onUpdateNote={handleUpdateNote}
              onAddToQueue={handleAddToQueue}
              onRemoveFromQueue={handleRemoveFromQueue}
              onOpenPlayerModal={onPlayerClick}
              onAddTier={handleAddTier}
              onDeleteTier={handleDeleteTier}
            />
          )}
        </div>

        {/* Collapsible Personal Draft Queue Sidebar */}
        {isQueueOpen && (
          <div className="w-80 lg:w-96 bg-slate-900 border border-slate-800 rounded-3xl p-5 shadow-2xl flex-shrink-0 space-y-4 sticky top-6 animate-fade-in max-h-[85vh] flex flex-col">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <div>
                <h3 className="font-black text-sm text-white flex items-center gap-1.5">
                  <span>★</span>
                  <span>Draft Night Queue</span>
                </h3>
                <p className="text-[11px] text-slate-400 mt-0.5">
                  Auto-picks fall back to this queue on the clock
                </p>
              </div>
              <div className="flex items-center gap-1">
                {queue.length > 0 && (
                  <button
                    onClick={handleClearQueue}
                    className="text-[10px] text-slate-500 hover:text-rose-400 p-1 cursor-pointer font-bold"
                    title="Clear queue"
                  >
                    Clear
                  </button>
                )}
                <button
                  onClick={() => setIsQueueOpen(false)}
                  className="text-slate-500 hover:text-white p-1 text-xs cursor-pointer"
                >
                  ✕
                </button>
              </div>
            </div>

            {/* Queue List */}
            <div className="flex-1 overflow-y-auto space-y-2 pr-1">
              {queue.length === 0 ? (
                <div className="text-center py-12 px-4 border-2 border-dashed border-slate-800 rounded-2xl">
                  <span className="text-3xl block mb-2">⭐</span>
                  <p className="text-xs font-bold text-slate-400">Queue is empty</p>
                  <p className="text-[11px] text-slate-600 mt-1">
                    Click the ★ star icon on any player card on the board to add them to your queue.
                  </p>
                </div>
              ) : (
                queue.map((p, idx) => {
                  const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
                  const assignedSnakePick = ownerSnakePicks[idx];

                  return (
                    <div
                      key={pid}
                      className="bg-slate-950 border border-slate-800 rounded-xl p-2.5 flex items-center justify-between gap-2 hover:border-slate-700 transition"
                    >
                      <div className="flex items-center gap-2 min-w-0">
                        {/* Queue Order / Projected Pick Badge */}
                        <div className="text-center flex-shrink-0 w-8">
                          <span className="font-black text-amber-400 text-xs">#{idx + 1}</span>
                          {assignedSnakePick && (
                            <span className="block text-[8px] text-slate-500 font-mono">
                              R{assignedSnakePick.round}
                            </span>
                          )}
                        </div>

                        <div className="w-8 h-8 rounded-full overflow-hidden bg-slate-800 flex-shrink-0 border border-slate-700">
                          <img
                            src={getPlayerHeadshotUrl(pid)}
                            alt={p.Player}
                            className="w-full h-full object-cover"
                            onError={(e) => handleHeadshotError(e, p)}
                            referrerPolicy="no-referrer"
                            loading="lazy"
                          />
                        </div>

                        <div className="min-w-0">
                          <div
                            onClick={() => onPlayerClick && onPlayerClick(p)}
                            className="font-black text-xs text-white truncate cursor-pointer hover:text-teal-300"
                          >
                            {p.Player || p.player_name}
                          </div>
                          <div className="text-[10px] text-slate-400 truncate">
                            {p.Position} • {p.Team}
                            {assignedSnakePick && (
                              <span className="text-teal-400 ml-1 font-mono">
                                (Pick #{assignedSnakePick.overallPick})
                              </span>
                            )}
                          </div>
                        </div>
                      </div>

                      {/* Reorder Buttons & Remove */}
                      <div className="flex items-center gap-0.5 flex-shrink-0">
                        <button
                          disabled={idx === 0}
                          onClick={() => handleMoveQueueItem(idx, idx - 1)}
                          className="p-1 text-slate-500 hover:text-white disabled:opacity-20 cursor-pointer disabled:cursor-not-allowed text-xs"
                          title="Move Up"
                        >
                          ▲
                        </button>
                        <button
                          disabled={idx === queue.length - 1}
                          onClick={() => handleMoveQueueItem(idx, idx + 1)}
                          className="p-1 text-slate-500 hover:text-white disabled:opacity-20 cursor-pointer disabled:cursor-not-allowed text-xs"
                          title="Move Down"
                        >
                          ▼
                        </button>
                        <button
                          onClick={() => handleRemoveFromQueue(pid)}
                          className="p-1 text-slate-600 hover:text-rose-400 cursor-pointer text-xs ml-1"
                          title="Remove from queue"
                        >
                          ✕
                        </button>
                      </div>
                    </div>
                  );
                })
              )}
            </div>

            {/* Footer Summary */}
            <div className="border-t border-slate-800 pt-3 text-[11px] text-slate-400 flex items-center justify-between">
              <span>{queue.length} target{queue.length === 1 ? '' : 's'} queued</span>
              <span className="text-teal-400 font-bold">Auto-saved to lobby</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
