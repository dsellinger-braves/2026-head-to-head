// src/components/draft/TierBoard.jsx
import React, { useState, useMemo } from 'react';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../../utils/headshotUtils';
import { getEffectiveRank } from '../../views/draft/utils/draftCalculations';
import { TAG_OPTIONS } from '../../utils/tierStorageService';

function renderPlayerStatBadges(p) {
  const pos = (p.Position || '').toUpperCase();
  const isPitcher = pos.includes('SP') || pos.includes('RP') || p.Pitcher;

  if (isPitcher) {
    const era = p.ZIPSERA || p.ESPNERA || '—';
    const whip = p.ZIPSWHIP || p.ESPNWHIP || '—';
    const k = p.ZIPSK || p.ESPNK || '—';
    const qs = p.ZIPSQS || p.ESPNQS || '—';
    const svHd = p['ZIPSSV+HDs'] || p['ESPNSV+HDs'] || p.ZIPSSV || null;

    return (
      <div className="flex items-center gap-1.5 flex-wrap font-mono text-[10px]">
        <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
          ERA <strong className="text-white">{era}</strong>
        </span>
        <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
          WHIP <strong className="text-white">{whip}</strong>
        </span>
        <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
          K <strong className="text-teal-300">{k}</strong>
        </span>
        <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
          QS <strong className="text-amber-300">{qs}</strong>
        </span>
        {svHd && svHd !== '—' && Number(svHd) > 0 && (
          <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
            SV+HD <strong className="text-purple-300">{svHd}</strong>
          </span>
        )}
      </div>
    );
  }

  const hr = p.ZIPSHR || p.ESPNHR || '—';
  const rbi = p.ZIPSRBI || p.ESPNRBI || '—';
  const r = p.ZIPSR || p.ESPNR || '—';
  const sb = p.ZIPSSB || p.ESPNSB || '—';
  const obp = p.ZIPSOBP || p.ESPNOBP || '—';
  const avg = p.ZIPSAVG || p.ESPNAVG || null;

  return (
    <div className="flex items-center gap-1.5 flex-wrap font-mono text-[10px]">
      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
        HR <strong className="text-rose-300">{hr}</strong>
      </span>
      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
        RBI <strong className="text-white">{rbi}</strong>
      </span>
      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
        R <strong className="text-white">{r}</strong>
      </span>
      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
        SB <strong className="text-cyan-300">{sb}</strong>
      </span>
      <span className="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-300">
        {obp !== '—' ? 'OBP' : 'AVG'} <strong className="text-emerald-300">{obp !== '—' ? obp : avg || '—'}</strong>
      </span>
    </div>
  );
}

export default function TierBoard({
  players = [],
  tiers = [],
  playerTiers = {},
  playerTags = {},
  playerNotes = {},
  queue = [],
  onMovePlayerToTier,
  onToggleTag,
  onUpdateNote,
  onAddToQueue,
  onRemoveFromQueue,
  onOpenPlayerModal,
  onAddTier,
  onDeleteTier,
  isLiveDraft = false,
  isMyTurn = false,
  onDraftPlayer = null
}) {
  const [selectedPos, setSelectedPos] = useState('ALL');
  const [searchQuery, setSearchQuery] = useState('');
  const [hideKept, setHideKept] = useState(true);
  const [activeEditingNotePid, setActiveEditingNotePid] = useState(null);
  const [editingNoteText, setEditingNoteText] = useState('');
  const [draggedOverTierId, setDraggedOverTierId] = useState(null);
  const [isAddingTier, setIsAddingTier] = useState(false);
  const [newTierName, setNewTierName] = useState('');
  const [newTierColor, setNewTierColor] = useState('#03dac6');
  const [tierLimits, setTierLimits] = useState({});

  // Filter players based on pos, search, and availability
  const filteredPlayers = useMemo(() => {
    return players.filter(p => {
      // 1. Availability / Kept filter
      if (hideKept && (p.isKeeper || p.Availability !== 'Available')) {
        return false;
      }

      // 2. Position filter
      const pos = (p.Position || '').toUpperCase();
      if (selectedPos === 'HITTERS') {
        if (pos.includes('SP') || pos.includes('RP') || p.Pitcher) return false;
      } else if (selectedPos === 'PITCHERS') {
        if (!pos.includes('SP') && !pos.includes('RP') && !p.Pitcher) return false;
      } else if (selectedPos !== 'ALL') {
        if (!pos.includes(selectedPos)) return false;
      }

      // 3. Search query
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase().trim();
        const name = (p.Player || p.player_name || p.name || '').toLowerCase();
        const team = (p.Team || p.team || '').toLowerCase();
        const note = (playerNotes[String(p['ESPN PlayerID'] || p.id || '')] || '').toLowerCase();
        if (!name.includes(q) && !team.includes(q) && !pos.toLowerCase().includes(q) && !note.includes(q)) {
          return false;
        }
      }

      return true;
    });
  }, [players, selectedPos, searchQuery, hideKept, playerNotes]);

  // Compute True Snake Rank (for available players)
  const trueSnakeRankMap = useMemo(() => {
    const map = new Map();
    let currentSnakeRank = 1;
    players.forEach(p => {
      const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
      if (!p.isKeeper && p.Availability === 'Available') {
        map.set(pid, currentSnakeRank++);
      }
    });
    return map;
  }, [players]);

  // Group filtered players by tier
  const playersByTier = useMemo(() => {
    const map = {};
    tiers.forEach(t => { map[t.id] = []; });
    const fallbackTierId = tiers[tiers.length - 1]?.id || 'tier-5';

    filteredPlayers.forEach(p => {
      const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
      const assignedTierId = playerTiers[pid] || fallbackTierId;
      if (map[assignedTierId]) {
        map[assignedTierId].push(p);
      } else {
        if (!map[fallbackTierId]) map[fallbackTierId] = [];
        map[fallbackTierId].push(p);
      }
    });

    // Sort players within each tier by effective rank
    Object.keys(map).forEach(tId => {
      map[tId].sort((a, b) => {
        const rankA = getEffectiveRank(a) || 999;
        const rankB = getEffectiveRank(b) || 999;
        return rankA - rankB;
      });
    });

    return map;
  }, [filteredPlayers, tiers, playerTiers]);

  // Drag and drop handlers
  const handleDragStart = (e, player) => {
    const pid = String(player['ESPN PlayerID'] || player.espn_player_id || player.id || '');
    e.dataTransfer.setData('text/plain', pid);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e, tierId) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    if (draggedOverTierId !== tierId) {
      setDraggedOverTierId(tierId);
    }
  };

  const handleDragLeave = (e, tierId) => {
    if (draggedOverTierId === tierId) {
      setDraggedOverTierId(null);
    }
  };

  const handleDrop = (e, targetTierId) => {
    e.preventDefault();
    setDraggedOverTierId(null);
    const pid = e.dataTransfer.getData('text/plain');
    if (pid && onMovePlayerToTier) {
      onMovePlayerToTier(pid, targetTierId);
    }
  };

  const handleNoteSave = (pid) => {
    if (onUpdateNote) {
      onUpdateNote(pid, editingNoteText);
    }
    setActiveEditingNotePid(null);
    setEditingNoteText('');
  };

  const handleCreateTierSubmit = (e) => {
    e.preventDefault();
    if (!newTierName.trim() || !onAddTier) return;
    const newId = `tier-${Date.now()}`;
    onAddTier({
      id: newId,
      name: newTierName.trim(),
      color: newTierColor || '#03dac6',
      description: 'Custom user-defined tier'
    });
    setNewTierName('');
    setIsAddingTier(false);
  };

  const handleShowMorePlayers = (tierId) => {
    setTierLimits(prev => ({
      ...prev,
      [tierId]: (prev[tierId] || 40) + 50
    }));
  };

  const POSITIONS = ['ALL', 'HITTERS', 'PITCHERS', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP'];

  return (
    <div className="flex flex-col h-full space-y-4 min-w-0">
      {/* Top Filter & Action Bar */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl flex flex-wrap items-center justify-between gap-3">
        {/* Positional Tabs */}
        <div className="flex flex-wrap items-center gap-1 bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-bold">
          {POSITIONS.map(pos => (
            <button
              key={pos}
              onClick={() => setSelectedPos(pos)}
              className={`px-3 py-1.5 rounded-lg transition-colors cursor-pointer ${
                selectedPos === pos
                  ? 'bg-teal-500 text-slate-950 shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              {pos}
            </button>
          ))}
        </div>

        {/* Search & Toggles */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="relative">
            <span className="absolute left-3 top-2 text-slate-500 text-xs">🔍</span>
            <input
              type="text"
              placeholder="Search player, team, note..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              className="pl-8 pr-3 py-1.5 bg-slate-950 border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-teal-400 w-52"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                className="absolute right-2.5 top-2 text-slate-500 hover:text-white text-xs cursor-pointer"
              >
                ✕
              </button>
            )}
          </div>

          <label className="flex items-center gap-2 text-xs font-bold text-slate-300 cursor-pointer bg-slate-950 px-3 py-1.5 rounded-xl border border-slate-800">
            <input
              type="checkbox"
              checked={hideKept}
              onChange={e => setHideKept(e.target.checked)}
              className="accent-teal-400 cursor-pointer"
            />
            <span>Hide Kept / Rostered</span>
          </label>

          {onAddTier && (
            <button
              onClick={() => setIsAddingTier(prev => !prev)}
              className="px-3 py-1.5 bg-purple-600/30 hover:bg-purple-600/50 text-purple-300 border border-purple-500/40 rounded-xl text-xs font-black transition cursor-pointer flex items-center gap-1.5"
            >
              <span>➕</span>
              <span>Add Custom Tier</span>
            </button>
          )}
        </div>
      </div>

      {/* Add Custom Tier Modal / Input Staging */}
      {isAddingTier && (
        <form onSubmit={handleCreateTierSubmit} className="bg-slate-900 border border-purple-500/40 rounded-2xl p-4 shadow-xl flex items-center gap-3 animate-fade-in flex-wrap">
          <span className="text-sm font-black text-purple-300">New Tier:</span>
          <input
            type="text"
            placeholder="Tier Name (e.g. High-Stakes Aces, Speed Sleepers)"
            value={newTierName}
            onChange={e => setNewTierName(e.target.value)}
            className="flex-1 min-w-[200px] px-3 py-1.5 bg-slate-950 border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-purple-400"
            autoFocus
          />
          <input
            type="color"
            value={newTierColor}
            onChange={e => setNewTierColor(e.target.value)}
            className="w-8 h-8 rounded-lg cursor-pointer bg-transparent border-0"
            title="Choose Tier Color"
          />
          <button
            type="submit"
            className="px-4 py-1.5 bg-purple-600 text-white rounded-xl text-xs font-black hover:bg-purple-500 transition cursor-pointer"
          >
            Save Tier
          </button>
          <button
            type="button"
            onClick={() => setIsAddingTier(false)}
            className="px-3 py-1.5 bg-slate-800 text-slate-400 rounded-xl text-xs hover:text-white transition cursor-pointer"
          >
            Cancel
          </button>
        </form>
      )}

      {/* Spacious Horizontal Kanban Lanes */}
      <div className="flex gap-4 items-start overflow-x-auto pb-6 pt-1 min-w-0 flex-1">
        {tiers.map((tier, tierIdx) => {
          const tierPlayers = playersByTier[tier.id] || [];
          const isOver = draggedOverTierId === tier.id;
          const displayLimit = tierLimits[tier.id] || 40;
          const visiblePlayers = tierPlayers.slice(0, displayLimit);
          const hasMore = tierPlayers.length > displayLimit;

          return (
            <div
              key={tier.id}
              onDragOver={(e) => handleDragOver(e, tier.id)}
              onDragLeave={(e) => handleDragLeave(e, tier.id)}
              onDrop={(e) => handleDrop(e, tier.id)}
              className={`w-[325px] sm:w-[340px] flex-shrink-0 bg-slate-900/95 border rounded-2xl flex flex-col max-h-[820px] transition-all duration-150 shadow-xl ${
                isOver
                  ? 'border-teal-400 ring-2 ring-teal-400/40 bg-slate-800/90 scale-[1.01]'
                  : 'border-slate-800 hover:border-slate-700'
              }`}
            >
              {/* Tier Header */}
              <div
                className="p-3.5 border-b border-slate-800 rounded-t-2xl flex items-center justify-between"
                style={{ borderTop: `4px solid ${tier.color || '#03dac6'}` }}
              >
                <div className="min-w-0 pr-2">
                  <div className="flex items-center gap-2">
                    <span className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: tier.color }} />
                    <h3 className="font-black text-sm text-white tracking-tight break-words">{tier.name}</h3>
                  </div>
                  <div className="text-[11px] text-slate-400 mt-1 flex items-center gap-2 flex-wrap">
                    <span className="px-1.5 py-0.2 bg-slate-800 text-slate-300 rounded font-bold">
                      {tierPlayers.length} player{tierPlayers.length === 1 ? '' : 's'}
                    </span>
                    {tierPlayers.length > 0 && (
                      <span className="text-slate-400 font-mono text-[10px]">
                        Avg #{Math.round(tierPlayers.reduce((acc, p) => acc + (getEffectiveRank(p) || 100), 0) / tierPlayers.length)}
                      </span>
                    )}
                  </div>
                </div>

                {onDeleteTier && tiers.length > 1 && (
                  <button
                    onClick={() => {
                      if (window.confirm(`Delete tier "${tier.name}"? Players will move to Tier 5.`)) {
                        onDeleteTier(tier.id);
                      }
                    }}
                    className="text-slate-600 hover:text-rose-400 text-xs p-1.5 transition cursor-pointer flex-shrink-0"
                    title="Delete custom tier"
                  >
                    🗑️
                  </button>
                )}
              </div>

              {/* Player Cards Container */}
              <div className="p-2.5 space-y-2.5 flex-1 overflow-y-auto">
                {tierPlayers.length === 0 ? (
                  <div className="h-32 border-2 border-dashed border-slate-800/80 rounded-xl flex items-center justify-center text-slate-600 text-xs font-bold text-center p-4">
                    Drag players here or use ▲/▼ buttons
                  </div>
                ) : (
                  visiblePlayers.map(p => {
                    const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
                    const isQueued = queue.some(q => String(q['ESPN PlayerID'] || q.id || q.espn_player_id) === pid);
                    const trueSnakeRank = trueSnakeRankMap.get(pid);
                    const effectiveRank = getEffectiveRank(p);
                    const activeTags = playerTags[pid] || [];
                    const currentNote = playerNotes[pid] || '';
                    const isEditingThisNote = activeEditingNotePid === pid;

                    return (
                      <div
                        key={pid}
                        draggable
                        onDragStart={(e) => handleDragStart(e, p)}
                        className={`bg-slate-950 border rounded-xl p-3 shadow-md transition-all hover:border-slate-600 cursor-grab active:cursor-grabbing ${
                          activeTags.includes('fade')
                            ? 'opacity-65 border-rose-900/60 bg-rose-950/20'
                            : activeTags.includes('target')
                            ? 'border-emerald-500/70 ring-1 ring-emerald-500/30'
                            : 'border-slate-800'
                        }`}
                      >
                        {/* Header: Photo, Name, Team, Positions, Actions */}
                        <div className="flex items-start justify-between gap-2">
                          <div className="flex items-center gap-2.5 min-w-0">
                            <div className="w-10 h-10 rounded-full overflow-hidden bg-slate-800 flex-shrink-0 border border-slate-700">
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
                                onClick={() => onOpenPlayerModal && onOpenPlayerModal(p)}
                                className="font-black text-sm text-white hover:text-teal-300 cursor-pointer leading-tight break-words"
                                title="View player scouting card"
                              >
                                {p.Player || p.player_name}
                              </div>
                              <div className="text-[11px] text-slate-400 flex items-center gap-1.5 flex-wrap mt-0.5">
                                <span className="font-bold text-teal-300 bg-teal-950/60 px-1.5 py-0.2 rounded border border-teal-800/50">
                                  {p.Position}
                                </span>
                                <span className="text-slate-300 font-medium">{p.Team}</span>
                                {trueSnakeRank && (
                                  <span className="px-1.5 py-0.2 bg-slate-900 text-teal-300 border border-teal-800/80 rounded font-black text-[10px]">
                                    🐍 Pick #{trueSnakeRank}
                                  </span>
                                )}
                              </div>
                            </div>
                          </div>

                          {/* Quick Actions: Queue & Pick */}
                          <div className="flex items-center gap-1 flex-shrink-0">
                            {isLiveDraft && isMyTurn && onDraftPlayer && (
                              <button
                                onClick={() => onDraftPlayer(p)}
                                className="px-2.5 py-1 bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-black text-[11px] rounded transition cursor-pointer shadow-sm"
                              >
                                DRAFT
                              </button>
                            )}

                            <button
                              onClick={() => {
                                if (isQueued) {
                                  onRemoveFromQueue && onRemoveFromQueue(pid);
                                } else {
                                  onAddToQueue && onAddToQueue(p);
                                }
                              }}
                              className={`p-1.5 rounded-lg text-xs transition cursor-pointer ${
                                isQueued
                                  ? 'bg-amber-400 text-slate-950 font-black shadow-xs'
                                  : 'text-slate-500 hover:text-amber-400 hover:bg-slate-800'
                              }`}
                              title={isQueued ? 'Remove from Queue' : 'Add to Queue'}
                            >
                              ★
                            </button>
                          </div>
                        </div>

                        {/* High-Contrast Projections Chip Bar */}
                        <div className="mt-2.5 bg-slate-900/90 p-1.5 rounded-lg border border-slate-800/80">
                          {renderPlayerStatBadges(p)}
                        </div>

                        {/* Priority Tags Row */}
                        <div className="flex items-center gap-1 mt-2.5 flex-wrap">
                          {TAG_OPTIONS.map(tag => {
                            const isTagged = activeTags.includes(tag.id);
                            return (
                              <button
                                key={tag.id}
                                onClick={() => onToggleTag && onToggleTag(pid, tag.id)}
                                className={`px-1.5 py-0.5 rounded text-[10px] font-bold transition cursor-pointer border ${
                                  isTagged
                                    ? 'border-teal-400 shadow-xs'
                                    : 'border-transparent text-slate-500 hover:text-slate-300 bg-slate-900/50'
                                }`}
                                style={{
                                  backgroundColor: isTagged ? tag.bg : undefined,
                                  color: isTagged ? tag.color : undefined
                                }}
                                title={tag.label}
                              >
                                <span>{tag.icon}</span>
                                {isTagged && <span className="ml-0.5 text-[9px]">{tag.label.split(' ')[0]}</span>}
                              </button>
                            );
                          })}

                          {/* Note Button */}
                          <button
                            onClick={() => {
                              if (isEditingThisNote) {
                                setActiveEditingNotePid(null);
                              } else {
                                setActiveEditingNotePid(pid);
                                setEditingNoteText(currentNote);
                              }
                            }}
                            className={`px-1.5 py-0.5 rounded text-[10px] font-bold transition cursor-pointer border ${
                              currentNote
                                ? 'bg-indigo-950/70 text-indigo-300 border-indigo-700/70'
                                : 'text-slate-500 hover:text-slate-300 border-transparent bg-slate-900/50'
                            }`}
                            title={currentNote ? `Note: ${currentNote}` : 'Add private scouting note'}
                          >
                            📝 {currentNote ? 'Note' : '+'}
                          </button>
                        </div>

                        {/* Inline Note Editor */}
                        {isEditingThisNote && (
                          <div className="mt-2.5 pt-2 border-t border-slate-800 space-y-1.5 animate-fade-in">
                            <textarea
                              rows={2}
                              value={editingNoteText}
                              onChange={e => setEditingNoteText(e.target.value)}
                              placeholder="Private scouting note (e.g. Target at turn, injury risk)..."
                              className="w-full bg-slate-900 border border-slate-700 rounded-lg p-2 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-teal-400 resize-none"
                            />
                            <div className="flex items-center justify-end gap-1.5">
                              <button
                                onClick={() => handleNoteSave(pid)}
                                className="px-2.5 py-1 bg-teal-500 text-slate-950 font-black text-[10px] rounded hover:bg-teal-400 transition cursor-pointer"
                              >
                                Save Note
                              </button>
                              <button
                                onClick={() => setActiveEditingNotePid(null)}
                                className="px-2 py-1 bg-slate-800 text-slate-400 text-[10px] rounded hover:text-white transition cursor-pointer"
                              >
                                Cancel
                              </button>
                            </div>
                          </div>
                        )}

                        {/* Saved Note Display if not editing */}
                        {!isEditingThisNote && currentNote && (
                          <div className="mt-2 text-[11px] text-indigo-300 bg-indigo-950/40 border border-indigo-900/50 rounded-lg p-1.5 italic line-clamp-2">
                            💡 {currentNote}
                          </div>
                        )}

                        {/* Tier Quick-Shift Arrows */}
                        <div className="flex items-center justify-between text-[10px] text-slate-500 mt-2.5 pt-1.5 border-t border-slate-900">
                          <button
                            disabled={tierIdx === 0}
                            onClick={() => onMovePlayerToTier && onMovePlayerToTier(pid, tiers[tierIdx - 1]?.id)}
                            className="hover:text-teal-400 disabled:opacity-20 cursor-pointer disabled:cursor-not-allowed font-medium"
                            title="Move to higher tier"
                          >
                            ▲ Up Tier
                          </button>
                          <span className="text-[10px] text-slate-500 font-mono font-bold">
                            Consensus #{effectiveRank || '--'}
                          </span>
                          <button
                            disabled={tierIdx === tiers.length - 1}
                            onClick={() => onMovePlayerToTier && onMovePlayerToTier(pid, tiers[tierIdx + 1]?.id)}
                            className="hover:text-amber-400 disabled:opacity-20 cursor-pointer disabled:cursor-not-allowed font-medium"
                            title="Move to lower tier"
                          >
                            ▼ Down Tier
                          </button>
                        </div>
                      </div>
                    );
                  })
                )}

                {/* Show More Players Button */}
                {hasMore && (
                  <button
                    onClick={() => handleShowMorePlayers(tier.id)}
                    className="w-full py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-xs font-bold text-slate-300 transition cursor-pointer text-center"
                  >
                    ⚡ Show {tierPlayers.length - displayLimit} More Players
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
