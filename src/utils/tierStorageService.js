// src/utils/tierStorageService.js
import { normalizeOwner } from '../constants/league';

export const DEFAULT_TIERS = [
  { id: 'tier-1', name: 'Tier 1: Franchise Anchors', color: '#10b981', description: 'Elite studs / First-round snake talent' },
  { id: 'tier-2', name: 'Tier 2: Cornerstone Starters', color: '#03dac6', description: 'All-Star caliber contributors & SP2s' },
  { id: 'tier-3', name: 'Tier 3: Solid Everyday Starters', color: '#38bdf8', description: 'Dependable mid-rotation arms & steady power' },
  { id: 'tier-4', name: 'Tier 4: Upside Sleepers & Specialists', color: '#fbbf24', description: 'Speed, SV+HD anchors, high-K targets' },
  { id: 'tier-5', name: 'Tier 5: Late Flyers & Depth', color: '#94a3b8', description: 'Speculative end-game flyers & depth' }
];

export const TAG_OPTIONS = [
  { id: 'target', label: 'Priority Target', icon: '🎯', color: '#10b981', bg: 'rgba(16, 185, 129, 0.2)' },
  { id: 'speed', label: 'Speed Need', icon: '⚡', color: '#06b6d4', bg: 'rgba(6, 182, 212, 0.2)' },
  { id: 'floor', label: 'High Floor', icon: '🛡️', color: '#3b82f6', bg: 'rgba(59, 130, 246, 0.2)' },
  { id: 'sleeper', label: 'Sleeper / Value', icon: '💣', color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.2)' },
  { id: 'fade', label: 'Do Not Draft / Fade', icon: '🚫', color: '#ef4444', bg: 'rgba(239, 68, 68, 0.2)' }
];

const tiersMemoryCache = new Map();

function getTiersStorageKey(seasonYear, owner) {
  const norm = normalizeOwner(owner || 'Daniel', 'draft') || 'Daniel';
  return `hefty_draft_tiers_${seasonYear || 2026}_${norm}`;
}

/**
 * Loads user tier assignments, custom tier definitions, tags, and notes with in-memory caching.
 */
export function loadUserTiers(seasonYear, owner) {
  const key = getTiersStorageKey(seasonYear, owner);
  if (tiersMemoryCache.has(key)) {
    return tiersMemoryCache.get(key);
  }
  try {
    const raw = typeof localStorage !== 'undefined' ? localStorage.getItem(key) : null;
    let res;
    if (!raw) {
      res = {
        tiers: [...DEFAULT_TIERS],
        playerTiers: {}, // { [playerId]: tierId }
        playerTags: {},  // { [playerId]: ['target', 'speed'] }
        playerNotes: {}  // { [playerId]: 'private note text' }
      };
    } else {
      const parsed = JSON.parse(raw);
      res = {
        tiers: (parsed.tiers && parsed.tiers.length > 0) ? parsed.tiers : [...DEFAULT_TIERS],
        playerTiers: parsed.playerTiers || {},
        playerTags: parsed.playerTags || {},
        playerNotes: parsed.playerNotes || {}
      };
    }
    tiersMemoryCache.set(key, res);
    return res;
  } catch (err) {
    console.error(`Error loading draft tiers from ${key}:`, err);
    return {
      tiers: [...DEFAULT_TIERS],
      playerTiers: {},
      playerTags: {},
      playerNotes: {}
    };
  }
}

/**
 * Saves user tier state and broadcasts update event for live draft lobby synchronization.
 */
export function saveUserTiers(seasonYear, owner, data) {
  const key = getTiersStorageKey(seasonYear, owner);
  try {
    tiersMemoryCache.set(key, data);
    if (typeof localStorage !== 'undefined') {
      localStorage.setItem(key, JSON.stringify(data));
    }
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('hefty_tiers_updated', {
        detail: { seasonYear, owner: normalizeOwner(owner, 'draft'), data }
      }));
    }
    return true;
  } catch (err) {
    console.error(`Error saving draft tiers to ${key}:`, err);
    return false;
  }
}

/**
 * Assigns a player to a specific tier.
 */
export function assignPlayerTier(seasonYear, owner, playerId, tierId) {
  if (!playerId) return;
  const current = loadUserTiers(seasonYear, owner);
  const pid = String(playerId);
  if (!tierId) {
    delete current.playerTiers[pid];
  } else {
    current.playerTiers[pid] = tierId;
  }
  saveUserTiers(seasonYear, owner, current);
  return current;
}

/**
 * Toggles a tag (e.g. 'target', 'speed', 'fade') on a player.
 */
export function togglePlayerTag(seasonYear, owner, playerId, tagId) {
  if (!playerId || !tagId) return;
  const current = loadUserTiers(seasonYear, owner);
  const pid = String(playerId);
  const existingTags = current.playerTags[pid] || [];
  if (existingTags.includes(tagId)) {
    current.playerTags[pid] = existingTags.filter(t => t !== tagId);
  } else {
    current.playerTags[pid] = [...existingTags, tagId];
  }
  saveUserTiers(seasonYear, owner, current);
  return current;
}

/**
 * Updates a scouting note for a player.
 */
export function setPlayerNote(seasonYear, owner, playerId, noteText) {
  if (!playerId) return;
  const current = loadUserTiers(seasonYear, owner);
  const pid = String(playerId);
  if (!noteText || !noteText.trim()) {
    delete current.playerNotes[pid];
  } else {
    current.playerNotes[pid] = noteText.trim();
  }
  saveUserTiers(seasonYear, owner, current);
  return current;
}

/**
 * Loads the active draft queue (synchronized with DraftRoomView).
 */
export function loadUserQueue() {
  try {
    const raw = localStorage.getItem('draft_queue');
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

/**
 * Saves the active draft queue and notifies open tabs / draft lobby.
 */
export function saveUserQueue(queue) {
  try {
    localStorage.setItem('draft_queue', JSON.stringify(queue || []));
    if (typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('draft_queue_updated', {
        detail: queue || []
      }));
    }
  } catch (err) {
    console.error('Error saving draft queue:', err);
  }
}

/**
 * Automatically assigns default tiers based on effective single-season rank and position.
 */
export function generateAutoTiers(players = [], defaultTiers = DEFAULT_TIERS) {
  const playerTiers = {};
  const t1 = defaultTiers[0]?.id || 'tier-1';
  const t2 = defaultTiers[1]?.id || 'tier-2';
  const t3 = defaultTiers[2]?.id || 'tier-3';
  const t4 = defaultTiers[3]?.id || 'tier-4';
  const t5 = defaultTiers[4]?.id || 'tier-5';

  players.forEach((p, idx) => {
    const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
    if (!pid) return;

    if (idx < 12) playerTiers[pid] = t1;
    else if (idx < 35) playerTiers[pid] = t2;
    else if (idx < 75) playerTiers[pid] = t3;
    else if (idx < 140) playerTiers[pid] = t4;
    else playerTiers[pid] = t5;
  });

  return playerTiers;
}
