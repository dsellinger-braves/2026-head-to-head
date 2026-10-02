import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { supabase, DEFAULT_SUPABASE_URL, DEFAULT_SUPABASE_ANON_KEY } from '../supabaseClient';
import defaultDraftAssetTrades from '../data/draftAssetTrades2026.json';
import defaultTeamBudgets from '../data/teamBudgets2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import defaultDraft2026 from '../data/draft2026.json';
import KeepersBudgetsPanel from '../components/KeepersBudgetsPanel';
import HistoricalDraftView from './HistoricalDraftView';
import { getPlayerHeadshotUrl, handleHeadshotError, updateGlobalPlayerLookup } from '../utils/headshotUtils';

function getFriendlyOwnerName(raw) {
  if (!raw && raw !== 0) return 'Unknown';
  const str = String(raw).trim();
  const match = str.match(/^team\s*(\d+)$/i);
  const id = match ? parseInt(match[1], 10) : (isNaN(parseInt(str, 10)) ? null : parseInt(str, 10));
  
  if (id !== null) {
    if (id === 1) return 'Tim';
    if (id === 2) return 'Adrian';
    if (id === 3) return 'Garrett';
    if (id === 5) return 'Daniel';
    if (id === 6 || id === 10) return 'Anil';
    if (id === 8) return 'Alex';
    if (id === 11) return 'Owens';
    if (id === 12) return 'Will';
    if (id === 13) return 'Mark';
    if (id === 9 || id === 14) return 'Preston';
  }
  
  const lower = str.toLowerCase();
  if (lower === 'dan' || lower === 'dsellinger') return 'Daniel';
  if (lower === 'timothy') return 'Tim';
  if (lower === 'owens' || lower === 'team owens') return 'Owens';
  if (lower === 'joseph mattingly') return 'Joseph';
  return str;
}

const mappedDraft2026History = (defaultDraft2026 || []).map(p => ({
  Year: '2026',
  year: 2026,
  Round: String(p.round || ''),
  round: p.round,
  Pick_Overall: String(p.overall_pick),
  overall_pick: p.overall_pick,
  Player_Name: p.player_name,
  player_name: p.player_name,
  Player: p.player_name,
  Team_ID: p.team_owner || p.team_id,
  Owner: p.team_owner,
  owner: p.team_owner,
  team_id: p.team_id,
  player_id: String(p.player_id || ''),
  'ESPN PlayerID': String(p.player_id || ''),
  Keeper: p.is_keeper ? 'True' : 'False',
  is_keeper: Boolean(p.is_keeper),
  cost: p.cost || 0
}));

// --- CONFIGURATION ---
const GEMINI_API_KEY = import.meta.env.VITE_GEMINI_API_KEY || 'AIzaSyDQ0eRBz6jSsORZrnG19jR5mzmd0QE0DWg';
const GEMINI_MODEL = 'gemini-2.5-flash';

// Google Cloud Storage for static player data
const GCS_BUCKET = "https://storage.googleapis.com/fantasy-draft-2026";

// IndexedDB cache for draft assets (matching fantasy-draft cache)
const DRAFT_DB_NAME = 'fantasy-draft-cache';
const DRAFT_DB_VERSION = 1;
const DRAFT_STORE_NAME = 'data';
let draftDbInstance = null;

async function getDraftDb() {
  if (draftDbInstance) return draftDbInstance;
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DRAFT_DB_NAME, DRAFT_DB_VERSION);
    req.onerror = () => reject(req.error);
    req.onsuccess = () => {
      draftDbInstance = req.result;
      resolve(draftDbInstance);
    };
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains(DRAFT_STORE_NAME)) {
        db.createObjectStore(DRAFT_STORE_NAME);
      }
    };
  });
}

async function draftDbGet(key) {
  try {
    const db = await getDraftDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(DRAFT_STORE_NAME, 'readonly');
      const store = tx.objectStore(DRAFT_STORE_NAME);
      const req = store.get(key);
      req.onerror = () => reject(req.error);
      req.onsuccess = () => resolve(req.result || null);
    });
  } catch (err) {
    console.error('draftDbGet error:', err);
    return null;
  }
}

async function draftDbSet(key, value) {
  try {
    const db = await getDraftDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(DRAFT_STORE_NAME, 'readwrite');
      const store = tx.objectStore(DRAFT_STORE_NAME);
      const req = store.put(value, key);
      req.onerror = () => reject(req.error);
      req.onsuccess = () => resolve();
    });
  } catch (err) {
    console.error('draftDbSet error:', err);
  }
}

// Cached fetch function for GCS data with 1-hour TTL
async function fetchFromGCS(filename, cacheKey) {
  try {
    const cached = await draftDbGet(cacheKey);
    if (cached) {
      const { data, timestamp } = cached;
      if (Date.now() - timestamp < 3600000) {
        console.log(`📦 Using cached ${filename} (IndexedDB)`);
        return data;
      }
    }
    console.log(`🌐 Fetching ${filename} from GCS...`);
    const res = await fetch(`${GCS_BUCKET}/${filename}`);
    if (res.status === 404) {
      console.warn(`⚠️ File not found: ${filename} (this may be expected if file hasn't been created yet)`);
      return [];
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    await draftDbSet(cacheKey, { data, timestamp: Date.now() });
    console.log(`✅ Cached ${filename} (${data?.length || 'N/A'} records)`);
    return data;
  } catch (err) {
    console.error(`❌ Error fetching ${filename}:`, err);
    try {
      const stale = await draftDbGet(cacheKey);
      if (stale && stale.data) {
        console.log(`📦 Using stale cache for ${filename}`);
        return stale.data;
      }
    } catch {
      /* ignore cache lookup error on network failure */
    }
    return [];
  }
}

// Edge function caller for FanGraphs projections
async function fetchFanGraphs(mode) {
  try {
    console.log(`⚾ Fetching fresh ${mode} stats from FanGraphs...`);
    const { data, error } = await supabase.functions.invoke('fangraphs', {
      body: { mode }
    });
    if (error) throw error;
    return data?.players || [];
  } catch (err) {
    console.error(`FanGraphs Fetch Error (${mode}):`, err);
    return [];
  }
}

// Edge function caller for live ESPN player profiles & rankings
async function callESPNProxy(body) {
  try {
    const res = await fetch(`${DEFAULT_SUPABASE_URL}/functions/v1/espn-proxy`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DEFAULT_SUPABASE_ANON_KEY}`
      },
      body: JSON.stringify(body)
    });
    if (!res.ok) {
      console.error(`ESPN Proxy HTTP Error: ${res.status}`);
      const errText = await res.text();
      console.error('Response:', errText);
      throw new Error(`ESPN Proxy error: ${res.status}`);
    }
    return await res.json();
  } catch (err) {
    console.error('callESPNProxy Error:', err);
    throw err;
  }
}

async function fetchAllEspnPlayers() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 120000);
  try {
    console.log('📡 Fetching all players from ESPN...');
    const res = await callESPNProxy({ mode: 'player_info' });
    clearTimeout(timer);
    if (res?.players) {
      console.log(`✅ Fetched ${res.players.length} players from ESPN`);
      return res.players;
    } else {
      console.warn('⚠️ No players returned from ESPN');
      return [];
    }
  } catch (err) {
    clearTimeout(timer);
    if (err.name === 'AbortError') {
      console.warn('⚠️ ESPN Proxy request timed out after 2 minutes.');
    } else {
      console.error('❌ ESPN Proxy Error:', err.message);
    }
    return [];
  }
}

function normalizeName(str) {
  return str
    ? str
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[.,'-]/g, '')
        .replace(/\s+(jr|sr|ii|iii|iv)$/i, '')
        .replace(/\s+/g, ' ')
        .trim()
    : '';
}

function roundNumber(val, decimals = 3) {
  if (val == null) return null;
  const n = parseFloat(val);
  return isNaN(n) ? null : parseFloat(n.toFixed(decimals));
}

function mergeEspnData(playerPool, espnData) {
  if (!playerPool || !espnData || espnData.length === 0) return playerPool;
  const espnMap = new Map();
  espnData.forEach(p => {
    const pid = p.player_id || p['ESPN PlayerID'];
    if (pid) espnMap.set(String(pid), p);
  });
  console.log(`📺 ESPN merge: ${espnMap.size} records available`);
  let matched = 0;
  const merged = playerPool.map(p => {
    const pid = String(p['ESPN PlayerID']);
    const e = espnMap.get(pid);
    if (e) {
      matched++;
      return {
        ...p,
        Position: e.eligiblePositions || p.Position,
        ADP: e.averageDraftPosition ?? p.ADP,
        averageDraftPosition: e.averageDraftPosition,
        ADPChange: e.averageDraftPositionPercentChange,
        averageDraftPositionPercentChange: e.averageDraftPositionPercentChange,
        'Percent Owned': e.percentOwned ?? p['Percent Owned'],
        percentOwned: e.percentOwned,
        OwnershipChange: e.percentChange,
        percentChange: e.percentChange,
        'ESPN ROTO Rank': e.rotoRank,
        rotoRank: e.rotoRank,
        'ESPN Single Season Ranking': e.rotoRank,
        injuryStatus: e.injuryStatus || e.injured_status,
        injured: e.injured,
        seasonOutlook: e.seasonOutlook,
        ESPNPA: e.ESPN_PA ?? p.ESPNPA,
        ESPNHR: e.ESPN_HR ?? p.ESPNHR,
        ESPNR: e.ESPN_R ?? p.ESPNR,
        ESPNRBI: e.ESPN_RBI ?? p.ESPNRBI,
        ESPNSB: e.ESPN_SB ?? p.ESPNSB,
        ESPNOBP: e.ESPN_OBP ?? p.ESPNOBP,
        ESPNIP: e.ESPN_IP ?? p.ESPNIP,
        ESPNK: e.ESPN_K ?? p.ESPNK,
        ESPNERA: e.ESPN_ERA ?? p.ESPNERA,
        ESPNWHIP: e.ESPN_WHIP ?? p.ESPNWHIP,
        ESPNQS: e.ESPN_QS ?? p.ESPNQS,
        'ESPNSV+HDs': (e.ESPN_SV ?? 0) + (e.ESPN_HD ?? 0) || p['ESPNSV+HDs'],
        stats2025: e.stats2025
      };
    }
    return p;
  });
  console.log(`📺 ESPN merge complete: ${matched} players matched`);
  return merged;
}

function mergeZipsData(playerPool, battersZips = [], pitchersZips = []) {
  if (!playerPool || playerPool.length === 0) {
    console.warn('No players to merge ZiPS data into');
    return playerPool;
  }

  const getName = p => p.Name || p.PlayerName || p.name || p.playername || '';
  const getMlbamId = p => p.xMLBAMID || p.XMLBAMID || p.mlbamid || p.MLBAMID || p.mlbam_id || '';

  const batterIdMap = new Map();
  const pitcherIdMap = new Map();
  const batterNameMap = new Map();
  const pitcherNameMap = new Map();
  const ambiguousBatters = new Set();
  const ambiguousPitchers = new Set();

  battersZips.forEach(p => {
    const mid = getMlbamId(p);
    if (mid) batterIdMap.set(String(mid), p);
    const name = getName(p);
    if (name) {
      const clean = normalizeName(name);
      if (batterNameMap.has(clean)) {
        ambiguousBatters.add(clean);
        batterNameMap.delete(clean);
      } else if (!ambiguousBatters.has(clean)) {
        batterNameMap.set(clean, p);
      }
    }
  });

  pitchersZips.forEach(p => {
    const mid = getMlbamId(p);
    if (mid) pitcherIdMap.set(String(mid), p);
    const name = getName(p);
    if (name) {
      const clean = normalizeName(name);
      if (pitcherNameMap.has(clean)) {
        ambiguousPitchers.add(clean);
        pitcherNameMap.delete(clean);
      } else if (!ambiguousPitchers.has(clean)) {
        pitcherNameMap.set(clean, p);
      }
    }
  });

  console.log(`📊 ZiPS merge maps: ${batterIdMap.size} batters by ID, ${batterNameMap.size} by name (${ambiguousBatters.size} ambiguous names skipped: ${[...ambiguousBatters].join(', ') || 'none'})`);
  console.log(`📊 ZiPS merge maps: ${pitcherIdMap.size} pitchers by ID, ${pitcherNameMap.size} by name (${ambiguousPitchers.size} ambiguous names skipped: ${[...ambiguousPitchers].join(', ') || 'none'})`);

  if (batterIdMap.size > 0) {
    const sampleIds = Array.from(batterIdMap.keys()).slice(0, 3);
    console.log(`📊 Sample ZiPS batting IDs: ${sampleIds.join(', ')}`);
  }
  if (playerPool.length > 0) {
    const samplePoolIds = playerPool.slice(0, 3).map(p => p.MLBAMID || p.mlbamid || 'N/A');
    console.log(`📊 Sample player pool IDs: ${samplePoolIds.join(', ')}`);
  }

  const poolNameCounts = new Map();
  playerPool.forEach(p => {
    const clean = normalizeName(p.Player || p.Name || '');
    if (clean) poolNameCounts.set(clean, (poolNameCounts.get(clean) || 0) + 1);
  });
  const poolAmbiguous = new Set([...poolNameCounts.entries()].filter(([, count]) => count > 1).map(([name]) => name));
  if (poolAmbiguous.size > 0) {
    console.log(`📊 Pool-ambiguous names (name fallback disabled for these): ${[...poolAmbiguous].join(', ')}`);
  }

  let bMatched = 0;
  let pMatched = 0;
  let nameFallbackCount = 0;

  const merged = playerPool.map(player => {
    const mlbRaw = String(player.MLBAMID || player.mlbamid || '').trim();
    const mlbamId = mlbRaw === '' || mlbRaw === '0' ? null : mlbRaw;
    const cleanName = normalizeName(player.Player || player.Name || '');
    const canFallback = !poolAmbiguous.has(cleanName) && !ambiguousBatters.has(cleanName) && !ambiguousPitchers.has(cleanName);

    let bData = null;
    if (mlbamId) bData = batterIdMap.get(mlbamId);
    if (!bData && cleanName && canFallback) {
      bData = batterNameMap.get(cleanName);
      if (bData) nameFallbackCount++;
    }

    let pData = null;
    if (mlbamId) pData = pitcherIdMap.get(mlbamId);
    if (!pData && cleanName && canFallback) {
      pData = pitcherNameMap.get(cleanName);
      if (pData) nameFallbackCount++;
    }

    const res = { ...player };

    if (bData) {
      bMatched++;
      res.ZIPSPA = bData.PA ?? player.ZIPSPA;
      res.ZIPSAB = bData.AB ?? player.ZIPSAB;
      res.ZIPSH = bData.H ?? player.ZIPSH;
      res.ZIPSHR = bData.HR ?? player.ZIPSHR;
      res.ZIPSR = bData.R ?? player.ZIPSR;
      res.ZIPSRBI = bData.RBI ?? player.ZIPSRBI;
      res.ZIPSSB = bData.SB ?? player.ZIPSSB;
      res.ZIPSCS = bData.CS ?? player.ZIPSCS;
      res.ZIPSBB = bData.BB ?? player.ZIPSBB;
      res.ZIPSK = bData.SO ?? bData.K ?? player.ZIPSK;
      res.ZIPSSO = bData.SO ?? bData.K ?? player.ZIPSSO;
      res.ZIPSOBP = roundNumber(bData.OBP) ?? player.ZIPSOBP;
      res.ZIPSSLG = roundNumber(bData.SLG) ?? player.ZIPSSLG;
      res.ZIPSAVG = roundNumber(bData.AVG) ?? player.ZIPSAVG;
      res.ZIPSwOBA = roundNumber(bData.wOBA) ?? player.ZIPSwOBA;
      res.ZIPSwRC = bData.wRC ?? player.ZIPSwRC;
      res.ZIPSwRCplus = bData['wRC+'] ?? player.ZIPSwRCplus;
      res.ZIPSWAR = bData.WAR ?? player.ZIPSWAR;
      res.ZIPS2B = bData['2B'] ?? player.ZIPS2B;
      res.ZIPS3B = bData['3B'] ?? player.ZIPS3B;
    }

    if (pData) {
      pMatched++;
      res.ZIPSW = pData.W ?? player.ZIPSW;
      res.ZIPSL = pData.L ?? player.ZIPSL;
      res.ZIPSG = pData.G ?? player.ZIPSG;
      res.ZIPSGS = pData.GS ?? player.ZIPSGS;
      res.ZIPSIP = pData.IP ?? player.ZIPSIP;
      res.ZIPSK = pData.SO ?? pData.K ?? res.ZIPSK ?? player.ZIPSK;
      res.ZIPSSO = pData.SO ?? pData.K ?? player.ZIPSSO;
      res.ZIPSBB = pData.BB ?? res.ZIPSBB ?? player.ZIPSBB;
      res.ZIPSERA = roundNumber(pData.ERA, 2) ?? player.ZIPSERA;
      res.ZIPSWHIP = roundNumber(pData.WHIP, 2) ?? player.ZIPSWHIP;
      res.ZIPSFIP = roundNumber(pData.FIP, 2) ?? player.ZIPSFIP;
      res.ZIPSxFIP = roundNumber(pData.xFIP, 2) ?? player.ZIPSxFIP;
      res.ZIPSKper9 = roundNumber(pData['K/9'], 2) ?? player.ZIPSKper9;
      res.ZIPSBBper9 = roundNumber(pData['BB/9'], 2) ?? player.ZIPSBBper9;
      res.ZIPSSV = pData.SV ?? player.ZIPSSV ?? 0;
      res.ZIPSHD = pData.HLD ?? pData.HD ?? player.ZIPSHD ?? 0;
      res['ZIPSSV+HDs'] = (res.ZIPSSV || 0) + (res.ZIPSHD || 0);
      if (pData.QS === undefined) {
        if (pData.GS && pData.IP) {
          res.ZIPSQS = Math.round(pData.GS * 0.45);
        }
      } else {
        res.ZIPSQS = pData.QS;
      }
      res.ZIPSWAR_pit = pData.WAR ?? player.ZIPSWAR_pit;
    }

    return res;
  });

  console.log(`📊 ZiPS merge complete: ${bMatched} batters, ${pMatched} pitchers matched (${nameFallbackCount} via name fallback)`);
  return merged;
}

// --- CONSTANTS ---
export const DRAFT_OWNERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"].sort();

const ROSTER_SLOTS = [
  { id: 'C1', label: 'C', eligible: ['C'] },
  { id: 'C2', label: 'C', eligible: ['C'] },
  { id: '1B', label: '1B', eligible: ['1B'] },
  { id: '2B', label: '2B', eligible: ['2B'] },
  { id: '3B', label: '3B', eligible: ['3B'] },
  { id: 'SS', label: 'SS', eligible: ['SS'] },
  { id: 'OF1', label: 'OF', eligible: ['OF'] },
  { id: 'OF2', label: 'OF', eligible: ['OF'] },
  { id: 'OF3', label: 'OF', eligible: ['OF'] },
  { id: 'OF4', label: 'OF', eligible: ['OF'] },
  { id: 'OF5', label: 'OF', eligible: ['OF'] },
  { id: 'DH', label: 'DH', eligible: ['DH', 'C', '1B', '2B', '3B', 'SS', 'OF'] },
  { id: 'SP1', label: 'SP', eligible: ['SP'] },
  { id: 'SP2', label: 'SP', eligible: ['SP'] },
  { id: 'SP3', label: 'SP', eligible: ['SP'] },
  { id: 'SP4', label: 'SP', eligible: ['SP'] },
  { id: 'RP1', label: 'RP', eligible: ['RP'] },
  { id: 'RP2', label: 'RP', eligible: ['RP'] },
  { id: 'BN1', label: 'Bench', eligible: ['ALL'] },
  { id: 'BN2', label: 'Bench', eligible: ['ALL'] },
  { id: 'BN3', label: 'Bench', eligible: ['ALL'] }
];

function generateDefaultDraftOrder() {
  const order = [];
  let overall = 1;
  for (let round = 1; round <= 21; round++) {
    const roundOwners = round % 2 === 1 ? [...DRAFT_OWNERS] : [...DRAFT_OWNERS].reverse();
    roundOwners.forEach((owner, idx) => {
      order.push({
        'Overall Pick': overall,
        'Round': round,
        'Pick': idx + 1,
        'Owner': owner,
        'ESPN PlayerID': null,
        'Selection': null
      });
      overall++;
    });
  }
  return order;
}

// --- GEMINI INTEGRATION & AI PICK ANALYSIS ---
const GEMINI_MODELS = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash'];

async function callGemini(prompt) {
  const apiKey = import.meta.env.VITE_GEMINI_API_KEY || (typeof GEMINI_API_KEY !== 'undefined' ? GEMINI_API_KEY : '');
  if (!apiKey || apiKey.startsWith('AIzaSyDQ0eRBz6jSsORZrnG19jR5mzmd0QE0DWg')) {
    return null; // Stale fallback key bypass
  }

  for (const model of GEMINI_MODELS) {
    try {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{
            parts: [{ text: prompt }]
          }]
        })
      });
      if (response.ok) {
        const data = await response.json();
        if (data.candidates && data.candidates[0]?.content?.parts?.[0]?.text) {
          return data.candidates[0].content.parts[0].text;
        }
      }
    } catch (err) {
      console.warn(`Gemini fetch error for ${model}:`, err);
    }
  }
  return null;
}

function formatPlayerStats(player) {
  const isPitcher = player.Position?.includes('SP') || player.Position?.includes('RP');
  if (isPitcher) {
    return `ERA: ${player.ZIPSERA || 'N/A'}, WHIP: ${player.ZIPSWHIP || 'N/A'}, K: ${player.ZIPSK || 'N/A'}, QS: ${player.ZIPSQS || 'N/A'}, SV+H: ${player['ZIPSSV+HDs'] || 'N/A'}`;
  }
  return `R: ${player.ZIPSR || 'N/A'}, HR: ${player.ZIPSHR || 'N/A'}, RBI: ${player.ZIPSRBI || 'N/A'}, SB: ${player.ZIPSSB || 'N/A'}, OBP: ${player.ZIPSOBP || 'N/A'}`;
}

function getEffectiveRank(player) {
  if (!player) return null;
  const r = player['Hefty Keeper Rank'] ?? 
            player['Hefty Single Season Rank'] ?? 
            player['ESPN Keeper Rank'] ?? 
            player['ESPN Single Season Rank'] ?? 
            player['ESPN ROTO Rank'] ?? 
            player.ADP ?? 
            player.rank;
  const num = parseFloat(r);
  return isNaN(num) || num <= 0 ? null : Math.round(num);
}

function getEffectivePrice(player) {
  if (!player) return null;
  const p = player['Hefty Keeper Price'] ?? 
            player['Hefty Single Season Price'] ?? 
            player['ESPN Price'] ?? 
            player.Value ?? 
            player.price;
  const num = parseFloat(p);
  return isNaN(num) || num <= 0 ? null : Math.round(num);
}

function determinePickAngle(player, pickNum, rank, surplus, teamContext) {
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const isReliever = (player.Position || '').includes('RP') || (player.Position || '').includes('CL');
  const posCounts = teamContext?.positionCounts || {};
  const spCount = posCounts['SP'] || 0;
  const sb = parseFloat(player.ZIPSSB || player.ESPNSB || 0);
  const hr = parseFloat(player.ZIPSHR || player.ESPNHR || 0);
  const k = parseFloat(player.ZIPSK || player.ESPNK || 0);
  const sv = parseFloat(player['ZIPSSV+HDs'] || player['ESPNSV+HDs'] || 0);

  // 1. Extreme board displacement
  if (surplus !== null && surplus >= 12) return 'SURPLUS_STEAL';
  if (surplus !== null && surplus <= -14) return 'AGGRESSIVE_REACH';

  // 2. Specialized role or skill profile
  if (isReliever || sv >= 16) return 'BULLPEN_CLOSER';
  if (isPitcher && spCount >= 2) return 'ROTATION_HEAVY';
  if (sb >= 28) return 'SPEED_SPECIALIST';
  if (hr >= 30) return 'POWER_SLUGGER';
  if (isPitcher && k >= 190) return 'K_MACHINE';

  // 3. Draft stages
  if (pickNum <= 18) return 'CORNERSTONE_FOUNDATION';
  if (pickNum >= 140) return 'LATE_ROUND_FLYER';

  // 4. Moderate surplus/deficit
  if (surplus !== null && surplus >= 6) return 'VALUE_PICK';
  if (surplus !== null && surplus <= -6) return 'PRIORITY_TARGET';

  return 'BALANCED_FIT';
}

function generateLocalDraftCommentary(player, owner, pickNum, teamContext = {}) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const angle = determinePickAngle(player, pickNum, rank, surplus, teamContext);

  const hr = player.ZIPSHR || player.ESPNHR;
  const rbi = player.ZIPSRBI || player.ESPNRBI;
  const sb = player.ZIPSSB || player.ESPNSB;
  const obp = player.ZIPSOBP || player.ESPNOBP;
  const obpStr = obp ? `.${String(obp).replace('0.', '').slice(0, 3)}` : null;
  const k = player.ZIPSK || player.ESPNK;
  const era = player.ZIPSERA || player.ESPNERA;
  const whip = player.ZIPSWHIP || player.ESPNWHIP;
  const qs = player.ZIPSQS || player.ESPNQS;
  const sv = player['ZIPSSV+HDs'] || player['ESPNSV+HDs'];

  let headline = '';
  let narrative = '';
  const v = pickNum % 3;

  switch (angle) {
    case 'SURPLUS_STEAL': {
      headline = `🔥 <b>Draft Steal (+${surplus} Surplus)</b>`;
      const variants = [
        `Tremendous board patience pays off for <b>${owner}</b>. Securing a consensus #${rank} talent at pick #${pickNum} captures massive value for their roster.`,
        `The room let <b>${player.Player}</b> slide too far. <b>${owner}</b> scoops up a projected ${isPitcher ? `${k || 170} K / ${era || '3.50'} ERA profile` : `${hr || 25} HR / ${obpStr || '.350'} OBP engine`} at a +${surplus} pick discount.`,
        `Pure profit for <b>${owner}</b> here at #${pickNum}. Getting a $${price || 20} projected asset this late represents one of the cleanest value plays of the draft so far.`
      ];
      narrative = variants[v];
      break;
    }

    case 'AGGRESSIVE_REACH': {
      const reach = Math.abs(surplus);
      headline = `⚡ <b>Aggressive Target (-${reach} Reach)</b>`;
      const variants = [
        `<b>${owner}</b> refuses to wait, leaping ${reach} spots ahead of consensus board rank to secure <b>${player.Player}</b>. Clearly prioritizing ${isPitcher ? `arm talent (${k || 150}+ Ks)` : `offensive punch (${hr || 20}+ HR)`} before this tier evaporates.`,
        `Flag planted early. <b>${owner}</b> pulls the trigger on <b>${player.Player}</b> well above his #${rank || 'late'} ranking, prioritizing roster fit over market consensus.`,
        `Bold maneuvering at pick #${pickNum}. <b>${owner}</b> pays a premium to lock down ${player.Position}, betting the projections underestimate his upside.`
      ];
      narrative = variants[v];
      break;
    }

    case 'BULLPEN_CLOSER': {
      headline = `🎯 <b>High-Leverage Bullpen</b>`;
      const variants = [
        `<b>${owner}</b> tackles high-leverage relief, picking up <b>${player.Player}</b> for late-inning lockdown. Projected for ${sv || '20+'} SV+HDs with swing-and-miss stuff (${k || '70'} Ks).`,
        `Relief scarcity strikes and <b>${owner}</b> responds. <b>${player.Player}</b> gives this bullpen a proven weapon for the SV+HD and ratio categories (${era || '3.10'} ERA).`,
        `A targeted bullpen acquisition at #${pickNum}. <b>${owner}</b> secures ${sv ? `${sv} projected SV+HDs` : 'late-inning leverage'} to stay competitive in weekly relief counts.`
      ];
      narrative = variants[v];
      break;
    }

    case 'ROTATION_HEAVY': {
      headline = `⚾ <b>Rotation Stacking</b>`;
      const variants = [
        `<b>${owner}</b> continues stacking starting pitching. <b>${player.Player}</b> brings ${qs || 14} projected QS and a ${era || '3.60'} ERA to an already formidable mound group.`,
        `Mound dominance remains the clear blueprint for <b>${owner}</b>. Adding <b>${player.Player}</b> gives them another high-volume starter projected for ${k || 160} Ks.`,
        `Another starting arm in the fold for <b>${owner}</b>. Betting heavily on starting pitching volume to carry the QS and strikeout categories week in and week out.`
      ];
      narrative = variants[v];
      break;
    }

    case 'SPEED_SPECIALIST': {
      headline = `💨 <b>Elite Speed Impact</b>`;
      const variants = [
        `Sprint speed comes off the board. <b>${owner}</b> secures a game-breaker on the basepaths in <b>${player.Player}</b>, whose projected ${sb || 30} steals can swing weekly matchups alone.`,
        `A dedicated category play for <b>${owner}</b>. Adding ${sb || 25}+ stolen base potential gives this roster immense flexibility in weekly matchup planning.`,
        `Speed is scarce and <b>${owner}</b> wasn't going to get left behind. <b>${player.Player}</b> pairs ${sb || 25} SB upside with a steady ${obpStr || '.340'} OBP.`
      ];
      narrative = variants[v];
      break;
    }

    case 'POWER_SLUGGER': {
      headline = `💥 <b>Middle-of-Order Power</b>`;
      const variants = [
        `Pure offensive thumping for <b>${owner}</b>. <b>${player.Player}</b> is forecasted for ${hr || 28} homers and ${rbi || 90} RBIs, giving this lineup a formidable run-production base.`,
        `Run production dialed in. <b>${owner}</b> adds <b>${player.Player}</b> to pace the power columns, projecting for ${hr || 25}+ HRs and heavy extra-base hit damage.`,
        `<b>${owner}</b> adds serious pop at pick #${pickNum}. <b>${player.Player}</b> projects to deliver ${hr || 26} HR with strong run totals in the middle of a productive lineup.`
      ];
      narrative = variants[v];
      break;
    }

    case 'K_MACHINE': {
      headline = `🔥 <b>Strikeout Punch</b>`;
      const variants = [
        `Whiff-rate premium for <b>${owner}</b>. <b>${player.Player}</b> generates elite swing-and-miss stuff, projected for ${k || 190} strikeouts across ${qs || 15} quality starts.`,
        `<b>${owner}</b> dials up the strikeout punch with <b>${player.Player}</b>. High-ceiling stuff on the mound that elevates weekly K totals immediately.`,
        `Missing bats at a high clip. <b>${owner}</b> nabs <b>${player.Player}</b> to fortify strikeouts (${k || 180}+ K proj) and keep WHIP in check (${whip || '1.15'}).`
      ];
      narrative = variants[v];
      break;
    }

    case 'CORNERSTONE_FOUNDATION': {
      headline = `👑 <b>Early Anchor Pick</b>`;
      const variants = [
        `<b>${owner}</b> lays a premier cornerstone in round ${Math.ceil(pickNum / 10)}. <b>${player.Player}</b> provides an elite baseline of ${isPitcher ? `${k || 190} Ks and ${era || '3.20'} ERA` : `${hr || 28} HR and .${String(obp || '360').replace('0.', '')} OBP`} to build around.`,
        `A foundational piece for <b>${owner}</b> at pick #${pickNum}. <b>${player.Player}</b> delivers high-floor excellence and marquee category contribution right out of the gate.`,
        `Starting strong: <b>${owner}</b> secures <b>${player.Player}</b> to anchor the top of their draft board with elite projected production.`
      ];
      narrative = variants[v];
      break;
    }

    case 'LATE_ROUND_FLYER': {
      headline = `🎲 <b>Late-Round Upside</b>`;
      const variants = [
        `Calculated risk at pick #${pickNum}. <b>${owner}</b> takes a flyer on <b>${player.Player}</b>, who offers intriguing upside if playing time breaks his way.`,
        `Late-draft upside hunting for <b>${owner}</b>. At this stage of the draft, <b>${player.Player}</b>'s skill profile (${isPitcher ? `${k || 120} K upside` : `${hr || 18} HR power`}) is well worth the roster spot.`,
        `High-ceiling play late. <b>${owner}</b> adds <b>${player.Player}</b> at #${pickNum} to round out roster flexibility before the final rounds conclude.`
      ];
      narrative = variants[v];
      break;
    }

    case 'VALUE_PICK': {
      headline = `📈 <b>Above-Average Value (+${surplus})</b>`;
      const variants = [
        `Smart draft board reading by <b>${owner}</b>. <b>${player.Player}</b> slips past his #${rank} projection to deliver a favorable ${surplus}-pick surplus at #${pickNum}.`,
        `Consensus value in the bag. <b>${owner}</b> capitalizes on <b>${player.Player}</b> sliding ${surplus} spots past expected board rank for a steady ${player.Position} addition.`,
        `Efficient acquisition at #${pickNum}. <b>${owner}</b> collects positive equity on <b>${player.Player}</b> without having to force the issue.`
      ];
      narrative = variants[v];
      break;
    }

    case 'PRIORITY_TARGET': {
      headline = `⚡ <b>Targeted Priority (-${Math.abs(surplus)})</b>`;
      const variants = [
        `<b>${owner}</b> didn't want to risk losing <b>${player.Player}</b>, pulling him ${Math.abs(surplus)} spots early to ensure ${player.Position} coverage.`,
        `Target locked. <b>${owner}</b> steps ahead of the curve at pick #${pickNum} to bring in <b>${player.Player}</b> ahead of market consensus.`,
        `Proactive roster construction. <b>${owner}</b> pays a modest reach price to secure <b>${player.Player}</b>'s specific statistical profile.`
      ];
      narrative = variants[v];
      break;
    }

    case 'BALANCED_FIT':
    default: {
      headline = `🎯 <b>On-Market Selection</b>`;
      const variants = [
        `Right on script at pick #${pickNum}. <b>${owner}</b> selects <b>${player.Player}</b> directly in line with board valuation, addressing ${player.Position} with projected ${isPitcher ? `${era || '3.70'} ERA / ${k || 140} K` : `${hr || 20} HR / ${rbi || 70} RBI`}.`,
        `Clean, disciplined pick by <b>${owner}</b>. <b>${player.Player}</b> comes off the board at fair market value to provide steady ${player.Position} production.`,
        `<b>${owner}</b> stays balanced at #${pickNum}, picking up <b>${player.Player}</b> to keep weekly counting categories and ratios on pace.`
      ];
      narrative = variants[v];
      break;
    }
  }

  const footer = `<br><br><span style="color:#888; font-size:11px;">Rank: #${rank || '—'} • Proj Value: $${price || '—'} • Pos: ${player.Position} • Team: ${player.Team || 'MLB'}</span>`;

  return `${headline}<br><br>${narrative}${footer}`;
}

async function generateDraftCommentary(player, owner, pickNum, teamContext = {}, season = 2027) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const angle = determinePickAngle(player, pickNum, rank, surplus, teamContext);
  const statLine = formatPlayerStats(player);

  const posBreakdown = typeof teamContext === 'object' && teamContext?.positionCounts
    ? Object.entries(teamContext.positionCounts).map(([pos, c]) => `${pos}: ${c}`).join(', ')
    : (typeof teamContext === 'string' ? teamContext : 'None yet');

  const recentPicksStr = typeof teamContext === 'object' && teamContext?.recentPicks?.length
    ? teamContext.recentPicks.join(' ➔ ')
    : 'None yet';

  const surplusDesc = surplus !== null
    ? (surplus >= 0 ? `+${surplus} spots past consensus rank (value slide)` : `${surplus} spots ahead of consensus rank (reach)`)
    : 'In line with consensus';

  const prompt = `You are a sharp, analytical fantasy baseball war-room commentator providing instant reaction for the ${season} draft.
League Format: Head-to-Head Each Category (R, HR, RBI, SB, OBP | K, QS, ERA, WHIP, SV+HD).

SELECTION:
- Manager: ${owner}
- Player: ${player.Player} (${player.Position} - ${player.Team || 'MLB'})
- Pick: #${pickNum}
- Board Rank: ${rank ? `#${rank}` : 'Unranked'}
- Projected Value: ${price ? `$${price}` : 'N/A'}
- Surplus vs Pick: ${surplusDesc}
- Player Projections: ${statLine}

MANAGER ROSTER CONTEXT:
- Picks so far: ${teamContext?.rosterCount || 0}
- Current positions rostered: ${posBreakdown}
- Recent picks: ${recentPicksStr}

ANALYTICAL ANGLE: [${angle}]

STRICT NEGATIVE CONSTRAINTS - BANNED CLICHÉS (DO NOT USE ANY OF THESE UNDER ANY CIRCUMSTANCES):
- DO NOT say "solidifies their depth" or "adds depth"
- DO NOT say "immediate category firepower" or "firepower"
- DO NOT say "championship push" or "title aspirations" or "championship contention"
- DO NOT say "anchors the staff" or "anchors the rotation"
- DO NOT say "locks in [Player] at Pick [X]"
- DO NOT say "makes a statement" or "puts the league on notice"
- DO NOT say "only time will tell" or "time will tell"
- DO NOT say "high-risk, high-reward"
- DO NOT say "fills a crucial void/need"
- DO NOT say "fitting their [archetype] identity"
- DO NOT say "potent bat" or "live arm"

INSTRUCTIONS:
1. Write 2 to 3 concise, punchy sentences (strictly under 65 words).
2. Focus on specific numbers (e.g. 30+ HR, sub-3.30 ERA, 25 SB, or +15 pick surplus) and tactical roster construction.
3. Sound like a knowledgeable, sharp fantasy analyst in an athletic war room.
4. Format with clean HTML: Use <b> tags for emphasis. Do NOT include markdown code blocks.
5. End with:
<br><br><span style="color:#888; font-size:11px;">Rank: #${rank || '—'} • Proj Value: $${price || '—'} • Pos: ${player.Position} • MLB: ${player.Team || 'MLB'}</span>`;

  const aiResponse = await callGemini(prompt);
  if (aiResponse && aiResponse !== 'Analysis unavailable') {
    const cleaned = aiResponse.replace(/^```html\s*/i, '').replace(/```\s*$/i, '').trim();
    return cleaned;
  }
  return generateLocalDraftCommentary(player, owner, pickNum, teamContext);
}

// --- AUDIO SYSTEM ---
let currentTtsAudio = null;

function prepareSpokenScript(htmlText) {
  if (!htmlText) return '';
  // Strip HTML tags
  let text = htmlText.replace(/<[^>]*>/g, ' ');
  // Remove metadata footers (Rank, Proj Value, Pos, Team, etc.)
  text = text.replace(/Rank:\s*#?\d*.*?Team:\s*[\w\s]*/gi, ' ');
  text = text.replace(/Board:\s*#?\d*.*?MLB:\s*[\w\s]*/gi, ' ');
  // Remove emojis
  text = text.replace(/[\u{1F300}-\u{1FAFF}]|[\u{2600}-\u{27BF}]/gu, ' ');
  // Expand fantasy abbreviations so pronunciation sounds like an athletic sports broadcaster
  text = text
    .replace(/\bHRs?\b/g, 'home runs')
    .replace(/\bRBIs?\b/g, 'RBIs')
    .replace(/\bSB\b/g, 'stolen bases')
    .replace(/\bOBP\b/g, 'on-base percentage')
    .replace(/\bERA\b/g, 'E-R-A')
    .replace(/\bWHIP\b/g, 'whip')
    .replace(/\bKs\b/g, 'strikeouts')
    .replace(/\bQS\b/g, 'quality starts')
    .replace(/\bSV\+HDs?\b/g, 'saves plus holds')
    .replace(/#(\d+)/g, 'number $1')
    .replace(/\$(\d+)/g, '$1 dollars')
    .replace(/\s+/g, ' ')
    .trim();
  return text;
}

async function playPodcastTTS(htmlText, voiceName = 'en-US-Journey-D') {
  const spokenText = prepareSpokenScript(htmlText);
  if (!spokenText || spokenText.length < 5) return;

  // Stop any currently playing TTS audio or speech synthesis
  if (currentTtsAudio) {
    try {
      currentTtsAudio.pause();
      currentTtsAudio.currentTime = 0;
    } catch {
      // ignore
    }
    currentTtsAudio = null;
  }
  if (typeof window !== 'undefined' && 'speechSynthesis' in window) {
    window.speechSynthesis.cancel();
  }

  // 1. Google Cloud Text-to-Speech API with Google's premier Journey Podcast voice
  const apiKey = import.meta.env.VITE_GOOGLE_TTS_API_KEY || import.meta.env.VITE_GEMINI_API_KEY || '';
  if (apiKey && !apiKey.startsWith('AIzaSyDQ0eRBz6jSsORZrnG19jR5mzmd0QE0DWg')) {
    try {
      const url = `https://texttospeech.googleapis.com/v1/text:synthesize?key=${apiKey}`;
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          input: { text: spokenText },
          voice: {
            languageCode: 'en-US',
            name: voiceName
          },
          audioConfig: {
            audioEncoding: 'MP3',
            speakingRate: 1.05,
            pitch: 0.0
          }
        })
      });

      if (response.ok) {
        const data = await response.json();
        if (data.audioContent) {
          const audio = new Audio(`data:audio/mp3;base64,${data.audioContent}`);
          currentTtsAudio = audio;
          await audio.play();
          return;
        }
      }
    } catch (err) {
      console.warn('Google Cloud TTS API error, falling back to Web Speech:', err);
    }
  }

  // 2. High-fidelity Web Speech fallback (utilizing Google US English or best available browser voice)
  if (typeof window !== 'undefined' && 'speechSynthesis' in window) {
    try {
      const utterance = new SpeechSynthesisUtterance(spokenText);
      utterance.rate = 1.05;
      utterance.pitch = 1.0;
      
      const voices = window.speechSynthesis.getVoices();
      const preferredVoice = voices.find(v => 
        v.lang.startsWith('en') && (v.name.includes('Google') || v.name.includes('Natural') || v.name.includes('Samantha') || v.name.includes('Daniel'))
      ) || voices.find(v => v.lang.startsWith('en'));

      if (preferredVoice) {
        utterance.voice = preferredVoice;
      }
      window.speechSynthesis.speak(utterance);
    } catch (speechErr) {
      console.warn('Web speech synthesis failed:', speechErr);
    }
  }
}

function playOwnerSound(ownerName) {
  if (!ownerName) return;
  try {
    const rawBase = import.meta.env.BASE_URL || '/';
    const base = rawBase.endsWith('/') ? rawBase : `${rawBase}/`;
    const audioPath = `${base}audio/owners/${ownerName.toLowerCase()}.mp3`;
    const audio = new Audio(audioPath);
    audio.play().catch(err => {
      console.log('Audio playback blocked or file not found:', err);
    });
  } catch (error) {
    console.error('Error playing sound:', error);
  }
}

// --- INJURY HELPER ---
function getInjuryIndicator(playerId, playerInfoArray) {
  if (!playerInfoArray || !playerInfoArray.length) return null;
  const pId = String(playerId);
  const info = playerInfoArray.find(i => String(i.player_id) === pId);
  if (!info || !info.injured_status || info.injured_status === 'ACTIVE') return null;

  const status = info.injured_status.toUpperCase();
  let color = '#ff9800';
  let displayText = status;

  if (status === 'DAY_TO_DAY' || status === 'DTD') {
    color = '#ffc107';
    displayText = 'DTD';
  } else if (status.includes('IL') || status === 'OUT') {
    color = '#f44336';
    if (status === 'SEVEN_DAY_IL') displayText = 'IL-7';
    else if (status === 'TEN_DAY_IL') displayText = 'IL-10';
    else if (status === 'FIFTEEN_DAY_IL') displayText = 'IL-15';
    else if (status === 'SIXTY_DAY_IL') displayText = 'IL-60';
    else displayText = 'IL';
  }

  return { color, status: displayText };
}

// Helper to fetch live player news from ESPN (syndicated from RotoWire) with IndexedDB caching & fallback
async function fetchPlayerNews(playerId) {
  if (!playerId) return [];
  const cacheKey = `espn_player_news_${playerId}`;
  try {
    const cached = await draftDbGet(cacheKey);
    if (cached && cached.data) {
      const { data, timestamp } = cached;
      // 30 minute cache TTL for fresh news
      if (Date.now() - timestamp < 1800000 && Array.isArray(data) && data.length > 0) {
        return data;
      }
    }
  } catch (err) {
    console.warn(`Cache read error for player news (${playerId}):`, err);
  }

  // 1. Fetch live from ESPN Fantasy News API (official public endpoint with RotoWire syndication)
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 6000);
    const res = await fetch(`https://site.api.espn.com/apis/fantasy/v2/games/flb/news/players?playerId=${encodeURIComponent(playerId)}`, {
      signal: controller.signal
    });
    clearTimeout(timeoutId);

    if (res.ok) {
      const json = await res.json();
      if (json && Array.isArray(json.feed) && json.feed.length > 0) {
        const items = json.feed.map(item => ({
          player_id: String(item.playerId || playerId),
          headline: item.headline || '',
          story: item.story || item.description || '',
          lastModified: item.lastModified || item.published || item.categorized || new Date().toISOString(),
          type: item.type || 'RotoWire',
          source: item.type || 'RotoWire'
        }));
        await draftDbSet(cacheKey, { data: items, timestamp: Date.now() });
        return items;
      }
    }
  } catch (err) {
    console.warn(`Live ESPN/RotoWire news fetch failed for player ${playerId}:`, err);
  }

  // 2. Fallback: Check stale IndexedDB cache if available
  try {
    const stale = await draftDbGet(cacheKey);
    if (stale && Array.isArray(stale.data) && stale.data.length > 0) {
      return stale.data;
    }
  } catch {
    /* ignore stale cache lookup error */
  }

  // 3. Fallback: Query static GCS player-news archive
  try {
    const gcsNews = await fetchFromGCS('player-news.json', 'gcs_player_news');
    if (Array.isArray(gcsNews)) {
      const matched = gcsNews
        .filter(n => String(n.player_id).trim() === String(playerId).trim())
        .sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified))
        .slice(0, 10)
        .map(n => ({
          ...n,
          type: n.type || 'RotoWire Archive'
        }));
      return matched;
    }
  } catch (err) {
    console.warn(`Fallback GCS news failed for player ${playerId}:`, err);
  }

  return [];
}

// --- PLAYER MODAL ---
function PlayerModal({
  player,
  onClose,
  onOpenInDepthModal,
  players = [],
  historicalFinish: propHistoricalFinish = [],
  draftHistory: propDraftHistory = [],
  savantBatters: propSavantBatters = [],
  savantPitchers: propSavantPitchers = [],
  battersZips: propBattersZips = [],
  pitchersZips: propPitchersZips = [],
  isMobile = false
}) {
  const [playerNews, setPlayerNews] = useState([]);
  const [newsLoading, setNewsLoading] = useState(false);
  const [playerInfo, setPlayerInfo] = useState(null);
  const [externalLinks, setExternalLinks] = useState(null);
  const [internalDraftHistory, setInternalDraftHistory] = useState([]);
  const [internalHistoricalFinish, setInternalHistoricalFinish] = useState([]);
  const [internalSavantBatters, setInternalSavantBatters] = useState([]);
  const [internalSavantPitchers, setInternalSavantPitchers] = useState([]);
  const [mlbAge, setMlbAge] = useState(null);
  const [relPos, setRelPos] = useState(player?.Position?.split('/')[0]?.trim() || 'Overall');
  const [loading, setLoading] = useState(true);

  // Sync relPos when player changes
  useEffect(() => {
    if (player?.Position) {
      setRelPos(player.Position.split('/')[0]?.trim() || 'Overall');
    }
  }, [player]);

  const effectiveDraftHistory = propDraftHistory.length > 0 ? propDraftHistory : internalDraftHistory;
  const effectiveHistoricalFinish = propHistoricalFinish.length > 0 ? propHistoricalFinish : internalHistoricalFinish;
  const effectiveSavantBatters = propSavantBatters.length > 0 ? propSavantBatters : internalSavantBatters;
  const effectiveSavantPitchers = propSavantPitchers.length > 0 ? propSavantPitchers : internalSavantPitchers;

  const isPitcherPlayer = Boolean(
    player?.Position &&
    (player.Position.includes('SP') || player.Position.includes('RP') || player.Position.includes('P'))
  );

  const isTwoWayPlayer = Boolean(
    isPitcherPlayer &&
    player?.Position &&
    (player.Position.includes('DH') ||
      player.Position.includes('OF') ||
      player.Position.includes('1B') ||
      player.Position.includes('2B') ||
      player.Position.includes('3B') ||
      player.Position.includes('SS') ||
      player.Position.includes('C'))
  );

  const playerId = String(player?.['ESPN PlayerID'] || '').trim();
  const mlbamId = String(player?.MLBAMID || '').trim();

  useEffect(() => {
    async function fetchPlayerData() {
      if (!player) return;
      setLoading(true);
      setNewsLoading(true);

      // Fetch live ESPN / RotoWire news in parallel
      if (playerId) {
        fetchPlayerNews(playerId)
          .then((news) => {
            setPlayerNews(news || []);
          })
          .catch((err) => {
            console.warn(`Error loading news for player ${playerId}:`, err);
            setPlayerNews([]);
          })
          .finally(() => {
            setNewsLoading(false);
          });
      } else {
        setPlayerNews([]);
        setNewsLoading(false);
      }

      try {
        const promises = [
          fetchFromGCS('player-info.json', 'gcs_player_info'),
          fetchFromGCS('player-links.json', 'gcs_player_links')
        ];

        const needDraftHistory = propDraftHistory.length === 0;
        const needHistoricalFinish = propHistoricalFinish.length === 0;
        const needSavantBat = propSavantBatters.length === 0;
        const needSavantPitch = propSavantPitchers.length === 0;

        if (needDraftHistory) promises.push(fetchFromGCS('draft-history.json', 'gcs_draft_history_v2'));
        if (needHistoricalFinish) promises.push(fetchFromGCS('historical-finish.json', 'gcs_league_history'));
        if (needSavantBat) promises.push(fetchFromGCS('savant-batting.json', 'gcs_bat_savant_2025'));
        if (needSavantPitch) promises.push(fetchFromGCS('savant-pitching.json', 'gcs_pitch_savant_2025'));

        const results = await Promise.allSettled(promises);
        let rIdx = 0;

        const infoData = results[rIdx++].status === 'fulfilled' ? results[rIdx - 1].value || [] : [];
        const linksData = results[rIdx++].status === 'fulfilled' ? results[rIdx - 1].value || [] : [];

        if (needDraftHistory) {
          const dHist = results[rIdx++].status === 'fulfilled' ? results[rIdx - 1].value || [] : [];
          const has2026 = dHist.some(d => String(d.Year || d.year) === '2026');
          const mergedHist = has2026 ? dHist : [...mappedDraft2026History, ...dHist];
          setInternalDraftHistory(mergedHist);
        }
        if (needHistoricalFinish) {
          const hFinish = results[rIdx++].status === 'fulfilled' ? results[rIdx - 1].value || [] : [];
          setInternalHistoricalFinish(hFinish);
        }
        if (needSavantBat) {
          const sBat = results[rIdx++].status === 'fulfilled' ? results[rIdx - 1].value || [] : [];
          setInternalSavantBatters(sBat);
        }
        if (needSavantPitch) {
          const sPitch = results[rIdx++].status === 'fulfilled' ? results[rIdx - 1].value || [] : [];
          setInternalSavantPitchers(sPitch);
        }

        const playerInfoFiltered = (infoData || []).find(i => String(i.player_id).trim() === playerId);
        const playerLinksFiltered = (linksData || []).find(l => {
          const lId = String(l.player_id || l['ESPN PlayerID'] || l.espn_player_id || '').trim();
          return lId === playerId;
        });

        setPlayerInfo(playerInfoFiltered || null);
        setExternalLinks(playerLinksFiltered || null);

        if (mlbamId) {
          fetch(`https://statsapi.mlb.com/api/v1/people/${mlbamId}`)
            .then(res => res.json())
            .then(data => {
              if (data?.people?.[0]?.currentAge) {
                setMlbAge(data.people[0].currentAge);
              }
            })
            .catch(() => {});
        }
      } catch (error) {
        console.error('Error fetching player modal data:', error);
      } finally {
        setLoading(false);
      }
    }

    fetchPlayerData();
  }, [player, playerId, mlbamId, propDraftHistory.length, propHistoricalFinish.length, propSavantBatters.length, propSavantPitchers.length]);

  // Close modal on ESC key press
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  if (!player) return null;

  const getInjuryColor = (status) => {
    if (!status) return null;
    const s = status.toUpperCase();
    if (s === 'ACTIVE') return '#4caf50';
    if (s === 'DAY_TO_DAY' || s === 'DTD') return '#ffc107';
    if (s.includes('IL') || s === 'OUT') return '#f44336';
    return '#ff9800';
  };

  const getInjuryDisplayText = (status) => {
    if (!status) return 'Unknown';
    const s = status.toUpperCase();
    if (s === 'ACTIVE') return 'Healthy';
    if (s === 'DAY_TO_DAY') return 'DTD';
    if (s === 'SEVEN_DAY_IL') return 'IL-7';
    if (s === 'TEN_DAY_IL') return 'IL-10';
    if (s === 'FIFTEEN_DAY_IL') return 'IL-15';
    if (s === 'SIXTY_DAY_IL') return 'IL-60';
    return status;
  };

  const injuryStatus = playerInfo?.injured_status || playerInfo?.injuryStatus;
  const injuryColor = injuryStatus ? getInjuryColor(injuryStatus) : null;
  const injuryDisplay = injuryStatus ? getInjuryDisplayText(injuryStatus) : null;

  const isChampion = (year, owner) => {
    if (!effectiveHistoricalFinish || !effectiveHistoricalFinish.length || !year || !owner) return false;
    const yStr = String(year).trim();
    const oStr = getFriendlyOwnerName(owner).trim().toLowerCase();
    return !!effectiveHistoricalFinish.find(f => {
      const fYear = String(f.Year || f.season_year || '').trim();
      const fOwner = getFriendlyOwnerName(f.Owner || f.team_owner || f.team_id || f.Team_ID || '').trim().toLowerCase();
      const fRank = String(f['Final Rank'] || f.final_place || f.rank || '').trim();
      return fYear === yStr && fOwner === oStr && fRank === '1';
    });
  };

  let hist = effectiveDraftHistory || [];
  if (!hist.some(d => String(d.Year || d.year) === '2026') && mappedDraft2026History.length > 0) {
    hist = [...mappedDraft2026History, ...hist];
  }
  const playerDraftHistory = hist
    .filter(h => {
      const hPid = String(h.player_id || h['ESPN PlayerID'] || h.espn_player_id || '').trim();
      const hMlb = String(h.MLBAMID || h.mlbamid || '').trim();
      const hName = String(h.Player_Name || h.Player || h.player_name || '').trim().toLowerCase();
      const pName = String(player.Player || player.player_name || '').trim().toLowerCase();
      if (playerId && hPid && playerId === hPid) return true;
      if (mlbamId && hMlb && mlbamId === hMlb) return true;
      if (pName && hName && pName === hName) return true;
      return false;
    })
    .sort((a, b) => (parseInt(b.Year || b.year) || 0) - (parseInt(a.Year || a.year) || 0));

  const savantPool = isPitcherPlayer ? effectiveSavantPitchers : effectiveSavantBatters;
  const playerSavant = (savantPool || []).find(item => {
    const pId = String(item.player_id).trim();
    return (mlbamId && pId === mlbamId) || (playerId && pId === playerId);
  });

  const statcastMetrics = isPitcherPlayer
    ? [
        { label: 'xwOBA', key: 'xwoba', desc: false },
        { label: 'xBA', key: 'xba', desc: false },
        { label: 'xSLG', key: 'xslg', desc: false },
        { label: 'K %', key: 'k_percent', desc: true },
        { label: 'BB %', key: 'bb_percent', desc: false },
        { label: 'Whiff %', key: 'whiff_percent', desc: true }
      ]
    : [
        { label: 'xwOBA', key: 'xwoba', desc: true },
        { label: 'xBA', key: 'xba', desc: true },
        { label: 'xSLG', key: 'xslg', desc: true },
        { label: 'K %', key: 'k_percent', desc: false },
        { label: 'BB %', key: 'bb_percent', desc: true }
      ];

  const calculatePercentile = (metricKey, higherIsBetter = true) => {
    if (!playerSavant || !savantPool || savantPool.length === 0) return undefined;
    const pVal = parseFloat(playerSavant[metricKey]);
    if (isNaN(pVal)) return undefined;

    const validEntries = savantPool
      .map(item => parseFloat(item[metricKey]))
      .filter(val => !isNaN(val) && val !== 0);

    if (validEntries.length === 0) return undefined;

    let count = 0;
    validEntries.forEach(val => {
      if (higherIsBetter && pVal > val) count++;
      if (!higherIsBetter && pVal < val) count++;
    });

    return Math.round((count / validEntries.length) * 100);
  };

  const getPercentileColor = (pct) => {
    if (pct == null) return '#444';
    if (pct >= 90) return '#d32f2f';
    if (pct >= 75) return '#f44336';
    if (pct >= 60) return '#ff9800';
    if (pct >= 40) return '#b0bec5';
    if (pct >= 25) return '#42a5f5';
    return '#1565c0';
  };

  const stats2025 = playerInfo?.stats2025;
  const d2025 = playerDraftHistory.find(d => String(d.Year || d.year) === '2025');

  const get2025BattingStat = (key) => {
    if (stats2025 && stats2025[key] !== undefined && stats2025[key] !== null) return stats2025[key];
    const bZips = (propBattersZips || []).find(b => String(b.MLBAMID || '').trim() === mlbamId);
    if (bZips && bZips[key] !== undefined && bZips[key] !== null) return bZips[key];
    if (d2025 && d2025[key] !== undefined && d2025[key] !== null) return d2025[key];
    return '-';
  };

  const get2025PitchingStat = (key) => {
    if (stats2025 && stats2025[key] !== undefined && stats2025[key] !== null) return stats2025[key];
    const pZips = (propPitchersZips || []).find(p => String(p.MLBAMID || '').trim() === mlbamId);
    if (pZips && pZips[key] !== undefined && pZips[key] !== null) return pZips[key];
    if (d2025 && d2025[key] !== undefined && d2025[key] !== null) return d2025[key];
    return '-';
  };

  const statTableStyles = {
    th: {
      padding: '8px',
      textAlign: 'center',
      color: '#888',
      borderBottom: '1px solid #444',
      fontSize: '11px',
      textTransform: 'uppercase'
    },
    td: {
      padding: '8px',
      textAlign: 'center',
      color: '#fff',
      borderBottom: '1px solid #333'
    }
  };

  return (
    <div style={styles.modalOverlay} onClick={onClose}>
      <div
        style={{
          ...styles.modalContent,
          maxWidth: '1280px',
          width: '95%',
          maxHeight: '90vh'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div style={styles.modalHeader}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '18px', flexWrap: 'wrap' }}>
            <div style={{
              width: '80px',
              height: '80px',
              borderRadius: '50%',
              border: '3px solid #bb86fc',
              backgroundColor: '#222',
              overflow: 'hidden',
              flexShrink: 0,
              boxShadow: '0 4px 14px rgba(187, 134, 252, 0.35)'
            }}>
              <img
                src={getPlayerHeadshotUrl(player)}
                alt={player.Player}
                style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                onError={(e) => handleHeadshotError(e, player)}
                referrerPolicy="no-referrer"
              />
            </div>
            <div>
              <h2 style={{ margin: 0, color: '#fff', fontSize: '28px' }}>
                {player.Player}
              </h2>
              <div style={{ color: '#888', fontSize: '15px', marginTop: '5px', display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
                <span>{playerInfo?.eligiblePositions || player.Position} • {player.Team}</span>
                <span>• ADP: {playerInfo?.averageDraftPosition?.toFixed(1) || player.ADP || 'N/A'}</span>
                {playerInfo?.averageDraftPositionPercentChange && Math.abs(playerInfo.averageDraftPositionPercentChange) > 0.1 && (
                  <span style={{
                    color: playerInfo.averageDraftPositionPercentChange > 0 ? '#f44336' : '#4caf50',
                    fontSize: '12px'
                  }}>
                    ({playerInfo.averageDraftPositionPercentChange > 0 ? '↓' : '↑'}{Math.abs(playerInfo.averageDraftPositionPercentChange).toFixed(1)}%)
                  </span>
                )}
                <span>• Owned: {playerInfo?.percentOwned?.toFixed(1) || player['Percent Owned'] || 'N/A'}%</span>
                {mlbAge && <span>• Age: {mlbAge}</span>}
                {injuryStatus && injuryDisplay !== 'Healthy' && (
                  <span style={{
                    padding: '3px 10px',
                    borderRadius: '4px',
                    background: injuryColor,
                    color: '#fff',
                    fontWeight: 'bold',
                    fontSize: '12px'
                  }}>
                    {injuryDisplay}
                  </span>
                )}
              </div>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            {onOpenInDepthModal && (
              <button
                onClick={() => {
                  onClose();
                  onOpenInDepthModal(player['ESPN PlayerID'], player.Player);
                }}
                style={{
                  background: '#2563eb',
                  color: '#fff',
                  border: 'none',
                  padding: '6px 12px',
                  borderRadius: '6px',
                  fontSize: '12px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
                title="View full historical matchup box scores and career logs"
              >
                📊 In-Season Log
              </button>
            )}
            <button onClick={onClose} style={styles.modalCloseBtn}>✕</button>
          </div>
        </div>

        {loading ? (
          <div style={{ textAlign: 'center', padding: '60px', color: '#888' }}>
            Loading player data...
          </div>
        ) : (
          <div style={{
            display: 'flex',
            flexDirection: isMobile ? 'column' : 'row',
            flexGrow: 1,
            overflow: 'hidden'
          }}>
            {/* Main Left Content */}
            <div style={{ flex: 1, padding: '20px 24px', overflowY: 'auto' }}>
              {/* 2026 Projections & 2025 Stats Table */}
              <div style={styles.modalSection}>
                <h3 style={styles.modalSectionTitle}>
                  2026 Projections & 2025 Stats
                  {playerInfo?.stats2025 && (
                    <span style={{ fontSize: '11px', color: '#4caf50', marginLeft: '10px', fontWeight: 'normal' }}>
                      ✓ Live from ESPN
                    </span>
                  )}
                </h3>

                {(!isPitcherPlayer || isTwoWayPlayer) && (
                  <div style={{ marginBottom: isTwoWayPlayer ? '18px' : '0' }}>
                    {isTwoWayPlayer && (
                      <div style={{ color: '#fff', fontWeight: 'bold', marginBottom: '5px', fontSize: '12px', borderBottom: '1px solid #444', paddingBottom: '2px' }}>
                        HITTING
                      </div>
                    )}
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                      <thead>
                        <tr style={{ background: '#333' }}>
                          <th style={statTableStyles.th}>Source</th>
                          <th style={statTableStyles.th}>PA</th>
                          <th style={statTableStyles.th}>R</th>
                          <th style={statTableStyles.th}>HR</th>
                          <th style={statTableStyles.th}>RBI</th>
                          <th style={statTableStyles.th}>SB</th>
                          <th style={statTableStyles.th}>OBP</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr style={{ background: 'rgba(255, 255, 255, 0.08)', borderBottom: '1px solid #555' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#aaa' }}>2025</td>
                          <td style={statTableStyles.td}>{get2025BattingStat('PA') || get2025BattingStat('AB')}</td>
                          <td style={statTableStyles.td}>{get2025BattingStat('R')}</td>
                          <td style={statTableStyles.td}>{get2025BattingStat('HR')}</td>
                          <td style={statTableStyles.td}>{get2025BattingStat('RBI')}</td>
                          <td style={statTableStyles.td}>{get2025BattingStat('SB')}</td>
                          <td style={statTableStyles.td}>{get2025BattingStat('OBP')}</td>
                        </tr>
                        <tr style={{ background: 'rgba(187, 134, 252, 0.1)', borderBottom: '1px solid #333' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#bb86fc' }}>ZiPS</td>
                          <td style={statTableStyles.td}>{player.ZIPSPA || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSR || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSHR || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSRBI || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSSB || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSOBP ? parseFloat(player.ZIPSOBP).toFixed(3) : '-'}</td>
                        </tr>
                        <tr style={{ background: 'rgba(3, 218, 198, 0.1)' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#03dac6' }}>ESPN</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_PA || player.ESPNPA || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_R || player.ESPNR || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_HR || player.ESPNHR || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_RBI || player.ESPNRBI || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_SB || player.ESPNSB || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_OBP ? parseFloat(playerInfo.ESPN_OBP).toFixed(3) : (player.ESPNOBP || '-')}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}

                {(isPitcherPlayer || isTwoWayPlayer) && (
                  <div>
                    {isTwoWayPlayer && (
                      <div style={{ color: '#fff', fontWeight: 'bold', marginBottom: '5px', fontSize: '12px', borderBottom: '1px solid #444', paddingBottom: '2px', marginTop: '14px' }}>
                        PITCHING
                      </div>
                    )}
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                      <thead>
                        <tr style={{ background: '#333' }}>
                          <th style={statTableStyles.th}>Source</th>
                          <th style={statTableStyles.th}>IP</th>
                          <th style={statTableStyles.th}>K</th>
                          <th style={statTableStyles.th}>QS</th>
                          <th style={statTableStyles.th}>ERA</th>
                          <th style={statTableStyles.th}>WHIP</th>
                          <th style={statTableStyles.th}>SV+HD</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr style={{ background: 'rgba(255, 255, 255, 0.08)', borderBottom: '1px solid #555' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#aaa' }}>2025</td>
                          <td style={statTableStyles.td}>{get2025PitchingStat('IP')}</td>
                          <td style={statTableStyles.td}>{get2025PitchingStat('K') || get2025PitchingStat('SO')}</td>
                          <td style={statTableStyles.td}>{get2025PitchingStat('QS')}</td>
                          <td style={statTableStyles.td}>{get2025PitchingStat('ERA')}</td>
                          <td style={statTableStyles.td}>{get2025PitchingStat('WHIP')}</td>
                          <td style={statTableStyles.td}>
                            {get2025PitchingStat('SV+HD') !== '-'
                              ? get2025PitchingStat('SV+HD')
                              : (get2025PitchingStat('SV') !== '-' || get2025PitchingStat('HD') !== '-')
                                ? ((parseInt(get2025PitchingStat('SV')) || 0) + (parseInt(get2025PitchingStat('HD')) || 0))
                                : '-'}
                          </td>
                        </tr>
                        <tr style={{ background: 'rgba(187, 134, 252, 0.1)', borderBottom: '1px solid #333' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#bb86fc' }}>ZiPS</td>
                          <td style={statTableStyles.td}>{player.ZIPSIP || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSK || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSQS || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSERA ? parseFloat(player.ZIPSERA).toFixed(2) : '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSWHIP ? parseFloat(player.ZIPSWHIP).toFixed(2) : '-'}</td>
                          <td style={statTableStyles.td}>{player['ZIPSSV+HDs'] || '-'}</td>
                        </tr>
                        <tr style={{ background: 'rgba(3, 218, 198, 0.1)' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#03dac6' }}>ESPN</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_IP ? (playerInfo.ESPN_IP / 3).toFixed(1) : (player.ESPNIP || '-')}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_K || player.ESPNK || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_QS || player.ESPNQS || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_ERA ? parseFloat(playerInfo.ESPN_ERA).toFixed(2) : (player.ESPNERA ? parseFloat(player.ESPNERA).toFixed(2) : '-')}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_WHIP ? parseFloat(playerInfo.ESPN_WHIP).toFixed(2) : (player.ESPNWHIP ? parseFloat(player.ESPNWHIP).toFixed(2) : '-')}</td>
                          <td style={statTableStyles.td}>
                            {playerInfo?.ESPN_SV !== undefined && playerInfo?.ESPN_HD !== undefined
                              ? (parseFloat(playerInfo.ESPN_SV || 0) + parseFloat(playerInfo.ESPN_HD || 0))
                              : (player['ESPNSV+HDs'] || '-')}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {/* Season Outlook */}
              {playerInfo && (playerInfo.seasonOutlook || playerInfo.season_outlook) && (
                <div style={{ ...styles.modalSection, borderLeft: '4px solid var(--accent)' }}>
                  <h3 style={styles.modalSectionTitle}>2026 Season Outlook</h3>
                  <p
                    style={{ margin: 0, color: '#ccc', fontSize: '14px', lineHeight: '1.6' }}
                    dangerouslySetInnerHTML={{ __html: playerInfo.seasonOutlook || playerInfo.season_outlook }}
                  />
                </div>
              )}

              {/* Recent News */}
              <div style={styles.modalSection}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
                  <h3 style={{ ...styles.modalSectionTitle, margin: 0 }}>Recent News</h3>
                  <span style={{ fontSize: '11px', color: '#9ca3af', display: 'flex', alignItems: 'center', gap: '6px' }}>
                    <span style={{
                      display: 'inline-block',
                      width: '7px',
                      height: '7px',
                      borderRadius: '50%',
                      background: '#10b981',
                      boxShadow: '0 0 6px #10b981'
                    }} />
                    Live RotoWire / ESPN
                  </span>
                </div>
                {newsLoading ? (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '16px 0', color: '#9ca3af', fontSize: '13px' }}>
                    <span className="inline-block animate-spin rounded-full h-4 w-4 border-2 border-emerald-400 border-t-transparent" />
                    Fetching latest player updates from RotoWire...
                  </div>
                ) : playerNews.length === 0 ? (
                  <p style={{ color: '#666', fontSize: '14px', fontStyle: 'italic', margin: 0 }}>
                    No recent news available
                  </p>
                ) : (
                  <div style={{ maxHeight: '240px', overflowY: 'auto', paddingRight: '4px' }}>
                    {playerNews.map((news, idx) => (
                      <div key={idx} style={styles.newsItem}>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '4px' }}>
                          <span style={{ color: '#888', fontSize: '12px' }}>
                            {new Date(news.lastModified).toLocaleDateString('en-US', {
                              month: 'short',
                              day: 'numeric',
                              year: 'numeric'
                            })}
                          </span>
                          <span style={{
                            fontSize: '10px',
                            fontWeight: '700',
                            textTransform: 'uppercase',
                            letterSpacing: '0.05em',
                            padding: '2px 6px',
                            borderRadius: '4px',
                            background: news.type?.toLowerCase().includes('archive') ? 'rgba(156, 163, 175, 0.15)' : 'rgba(3, 218, 198, 0.12)',
                            color: news.type?.toLowerCase().includes('archive') ? '#9ca3af' : '#03dac6',
                            border: news.type?.toLowerCase().includes('archive') ? '1px solid rgba(156, 163, 175, 0.3)' : '1px solid rgba(3, 218, 198, 0.3)'
                          }}>
                            {news.type || 'RotoWire'}
                          </span>
                        </div>
                        <div style={{ color: '#fff', fontSize: '14px', fontWeight: 'bold', marginBottom: '6px', lineHeight: '1.4' }}>
                          {news.headline}
                        </div>
                        <div
                          style={{ color: '#ccc', fontSize: '13px', lineHeight: '1.5' }}
                          dangerouslySetInnerHTML={{
                            __html: (news.story || '')
                              .replace(/<photo\d*>/gi, '')
                              .replace(/<\/photo\d*>/gi, '')
                              .replace(/<p>\s*<\/p>/gi, '')
                              .replace(/<a /gi, '<a target="_blank" rel="noopener noreferrer" style="color:#03dac6;text-decoration:none;" ')
                          }}
                        />
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* League History */}
              <div style={{ ...styles.modalSection, borderLeft: '4px solid #ff9800' }}>
                <h3 style={styles.modalSectionTitle}>League History (2012-2026)</h3>
                {playerDraftHistory.length === 0 ? (
                  <p style={{ color: '#666', fontSize: '14px', fontStyle: 'italic', margin: 0 }}>
                    No previous draft history in this league
                  </p>
                ) : (
                  <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))',
                    gap: '8px'
                  }}>
                    {playerDraftHistory.map((entry, idx) => {
                      const rawOwner = entry.Owner || entry.owner || entry.Team_ID || entry.team_id || 'Unknown';
                      const owner = getFriendlyOwnerName(rawOwner);
                      const year = entry.Year || entry.year;
                      const champ = isChampion(year, owner);
                      const isKeeper = String(entry.Keeper).toLowerCase() === 'true' || entry.Keeper === true;

                      let cardBg = '#333';
                      if (isKeeper && champ) {
                        cardBg = 'linear-gradient(90deg, #1b5e20 0%, #4a4a2a 100%)';
                      } else if (isKeeper) {
                        cardBg = 'linear-gradient(135deg, #1b5e20 0%, #333 100%)';
                      } else if (champ) {
                        cardBg = 'linear-gradient(135deg, #4a4a2a 0%, #333 100%)';
                      }

                      const borderStyle = champ
                        ? '1px solid gold'
                        : (isKeeper ? '1px solid #4caf50' : 'none');

                      const roundStr = isKeeper
                        ? 'Keeper'
                        : `Rd ${entry.Round || entry.round || '—'}`;
                      const costVal = entry.cost || entry.Bid_Amount || entry.bid_amount;
                      const costStr = costVal && String(costVal) !== '0'
                        ? ` ($${costVal})`
                        : '';

                      return (
                        <div
                          key={idx}
                          style={{
                            background: cardBg,
                            padding: '8px',
                            borderRadius: '6px',
                            textAlign: 'center',
                            border: borderStyle,
                            boxShadow: champ ? '0 0 10px rgba(255, 215, 0, 0.35)' : 'none'
                          }}
                        >
                          <div style={{ color: '#ff9800', fontWeight: 'bold', fontSize: '13px' }}>
                            {year}
                          </div>
                          <div style={{
                            color: champ ? 'gold' : '#fff',
                            fontSize: '12px',
                            fontWeight: champ ? 'bold' : 'normal',
                            marginTop: '2px'
                          }}>
                            {owner} {champ && '🏆'}
                          </div>
                          <div style={{ color: '#888', fontSize: '11px', marginTop: '2px' }}>
                            {roundStr}{costStr}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* External Resources */}
              <div style={styles.modalSection}>
                <h3 style={styles.modalSectionTitle}>External Resources</h3>
                <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                  {externalLinks?.baseball_reference_url && (
                    <a
                      href={externalLinks.baseball_reference_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={styles.externalLink}
                    >
                      Baseball Reference
                    </a>
                  )}
                  {externalLinks?.fangraphs_url && (
                    <a
                      href={externalLinks.fangraphs_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={styles.externalLink}
                    >
                      FanGraphs
                    </a>
                  )}
                  {externalLinks?.baseball_savant_url && (
                    <a
                      href={externalLinks.baseball_savant_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={styles.externalLink}
                    >
                      Baseball Savant
                    </a>
                  )}
                  {externalLinks?.espn_url && (
                    <a
                      href={externalLinks.espn_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={styles.externalLink}
                    >
                      ESPN
                    </a>
                  )}
                  {!externalLinks && (
                    <p style={{ color: '#666', fontSize: '14px', fontStyle: 'italic', margin: 0 }}>
                      No external links available
                    </p>
                  )}
                </div>
              </div>
            </div>

            {/* Right Sidebar */}
            <div style={{
              width: isMobile ? '100%' : '320px',
              background: '#181818',
              padding: '20px',
              borderLeft: isMobile ? 'none' : '1px solid #333',
              borderTop: isMobile ? '1px solid #333' : 'none',
              overflowY: 'auto',
              flexShrink: 0
            }}>
              {/* Rankings Section */}
              {(() => {
                const heftySS = player['Hefty Single Season Rank'];
                const heftyKeeper = player['Hefty Keeper Rank'];
                const espnSS = player['ESPN Single Season Rank'] || player['ESPN ROTO Rank'];
                const espnKeeper = player['ESPN Keeper Rank'];

                if (![heftySS, heftyKeeper, espnSS, espnKeeper].some(v => v != null && v !== '')) {
                  return null;
                }

                const getRankColor = (rank) => {
                  const val = parseInt(rank);
                  if (isNaN(val)) return '#555';
                  if (val <= 12) return `hsl(${Math.round(120 - (val - 1) * 5)}, 70%, 45%)`;
                  if (val <= 30) return `hsl(${Math.round(60 - (val - 12) * 1.5)}, 70%, 45%)`;
                  return '#e57373';
                };

                const RankBadge = ({ label, rank, accent }) => {
                  const val = parseInt(rank);
                  const displayRank = isNaN(val) ? '—' : `#${val}`;
                  const rankColor = isNaN(val) ? '#444' : getRankColor(val);
                  return (
                    <div style={{
                      flex: 1,
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      gap: '4px',
                      padding: '10px 6px',
                      background: '#252525',
                      borderRadius: '6px',
                      borderTop: `3px solid ${accent}`
                    }}>
                      <div style={{ fontSize: '18px', fontWeight: 800, color: rankColor, lineHeight: 1 }}>
                        {displayRank}
                      </div>
                      <div style={{ fontSize: '10px', color: '#888', textAlign: 'center', lineHeight: 1.3 }}>
                        {label}
                      </div>
                    </div>
                  );
                };

                const heftyVal = parseInt(heftySS);
                const espnVal = parseInt(espnSS);
                const hasDiff = !isNaN(heftyVal) && !isNaN(espnVal);
                const diff = heftyVal - espnVal;

                return (
                  <div style={{ marginBottom: '20px' }}>
                    <div style={{
                      fontSize: '11px',
                      fontWeight: 'bold',
                      color: '#888',
                      textTransform: 'uppercase',
                      letterSpacing: '1px',
                      marginBottom: '8px'
                    }}>
                      Rankings
                    </div>

                    <div style={{ fontSize: '10px', color: '#666', textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: '5px' }}>
                      Single Season
                    </div>
                    <div style={{ display: 'flex', gap: '8px', marginBottom: '10px' }}>
                      <RankBadge label="Hefty" rank={heftySS} accent="#bb86fc" />
                      <RankBadge label="ESPN" rank={espnSS} accent="#f4891f" />
                    </div>

                    <div style={{ fontSize: '10px', color: '#666', textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: '5px' }}>
                      Keeper
                    </div>
                    <div style={{ display: 'flex', gap: '8px', marginBottom: '4px' }}>
                      <RankBadge label="Hefty" rank={heftyKeeper} accent="#bb86fc" />
                      <RankBadge label="ESPN" rank={espnKeeper} accent="#f4891f" />
                    </div>

                    {hasDiff && (
                      Math.abs(diff) < 3 ? (
                        <div style={{ fontSize: '10px', color: '#4caf50', textAlign: 'center', marginTop: '6px' }}>
                          ✓ Systems agree
                        </div>
                      ) : (
                        <div style={{
                          fontSize: '10px',
                          color: diff < 0 ? '#bb86fc' : '#f4891f',
                          textAlign: 'center',
                          marginTop: '6px'
                        }}>
                          {diff < 0
                            ? `Hefty ranks ${Math.abs(diff)} spots higher than ESPN`
                            : `ESPN ranks ${Math.abs(diff)} spots higher than Hefty`}
                        </div>
                      )
                    )}
                    <div style={{ marginTop: '16px', borderTop: '1px solid #2a2a2a' }} />
                  </div>
                );
              })()}

              {/* 2025 Statcast Percentiles Section */}
              <div style={{ marginBottom: '20px' }}>
                <h3 style={{
                  margin: '0 0 14px 0',
                  color: '#03dac6',
                  fontSize: '13px',
                  fontWeight: 'bold',
                  textTransform: 'uppercase',
                  letterSpacing: '1px',
                  textAlign: 'center'
                }}>
                  2025 Statcast Percentiles
                </h3>
                {playerSavant ? (
                  <div>
                    {statcastMetrics.map(m => {
                      const pct = calculatePercentile(m.key, m.desc);
                      const rawVal = playerSavant[m.key];
                      return (
                        <div key={m.key} style={{ display: 'flex', alignItems: 'center', marginBottom: '8px', fontSize: '12px' }}>
                          <div style={{ width: '80px', color: '#ccc', textAlign: 'right', paddingRight: '8px' }}>
                            {m.label}
                          </div>
                          <div style={{
                            flexGrow: 1,
                            background: '#333',
                            height: '16px',
                            borderRadius: '4px',
                            overflow: 'hidden',
                            position: 'relative',
                            display: 'flex',
                            alignItems: 'center'
                          }}>
                            {/* 50% guideline */}
                            <div style={{ position: 'absolute', left: '50%', width: '1px', height: '100%', background: '#555', zIndex: 0 }} />
                            {pct !== undefined && (
                              <div style={{
                                width: `${pct}%`,
                                height: '100%',
                                background: getPercentileColor(pct),
                                transition: 'width 0.5s ease',
                                zIndex: 1
                              }} />
                            )}
                            <div style={{
                              position: 'absolute',
                              width: '100%',
                              textAlign: 'center',
                              color: '#fff',
                              fontSize: '10px',
                              fontWeight: 'bold',
                              textShadow: '0 0 3px rgba(0,0,0,0.8)',
                              zIndex: 2
                            }}>
                              {pct === undefined ? '' : pct}
                            </div>
                          </div>
                          <div style={{
                            width: '45px',
                            textAlign: 'left',
                            paddingLeft: '8px',
                            color: '#ccc',
                            fontSize: '11px',
                            fontWeight: 'bold'
                          }}>
                            {rawVal ?? '-'}
                          </div>
                        </div>
                      );
                    })}
                    <div style={{ marginTop: '14px', textAlign: 'center', fontSize: '10px', color: '#666', lineHeight: 1.4 }}>
                      * Bar shows Percentile (100=Best)<br />
                      * Number on right is Raw Value
                    </div>
                  </div>
                ) : (
                  <div style={{ textAlign: 'center', color: '#666', marginTop: '20px', fontSize: '12px', fontStyle: 'italic' }}>
                    No 2025 Statcast data found.
                  </div>
                )}
                <div style={{ marginTop: '16px', borderTop: '1px solid #2a2a2a' }} />
              </div>

              {/* Relative Value Chart */}
              {(() => {
                const allPool = players && players.length ? players : [];
                if (!allPool.length || !player) return null;

                const filtered = allPool.filter(p => {
                  const pPos = p.Position || '';
                  let match = false;
                  if (relPos === 'Overall') match = true;
                  else match = pPos.includes(relPos);
                  const pr = parseFloat(p['Projected PR']);
                  return match && !isNaN(pr);
                }).sort((a, b) => (parseFloat(b['Projected PR']) || 0) - (parseFloat(a['Projected PR']) || 0));

                const limit = relPos === 'Overall' ? 200 : 50;
                const viewData = filtered.slice(0, limit);
                const maxVal = Math.max(...viewData.map(p => parseFloat(p['Projected PR']) || 0), 10);
                const currentId = String(player['ESPN PlayerID']).trim();
                const playerPositions = (player.Position || '').split('/').map(p => p.trim()).filter(Boolean);

                return (
                  <div style={{ background: '#252525', borderRadius: '8px', padding: '12px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                      <div style={{ fontSize: '11px', fontWeight: 'bold', color: '#03dac6', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                        Relative Value
                      </div>
                      <select
                        value={relPos}
                        onChange={(e) => setRelPos(e.target.value)}
                        style={{
                          background: '#333',
                          color: '#fff',
                          border: 'none',
                          fontSize: '11px',
                          padding: '2px 6px',
                          borderRadius: '4px',
                          cursor: 'pointer'
                        }}
                      >
                        {playerPositions.map(pos => (
                          <option key={pos} value={pos}>{pos}</option>
                        ))}
                        <option value="Overall">Overall</option>
                      </select>
                    </div>

                    <div style={{ display: 'flex', alignItems: 'flex-end', height: '60px', gap: '1px' }}>
                      {viewData.map(p => {
                        const isThis = String(p['ESPN PlayerID']).trim() === currentId;
                        const pr = parseFloat(p['Projected PR']) || 0;
                        const heightPct = Math.max((pr / maxVal) * 100, 5);
                        return (
                          <div
                            key={p['ESPN PlayerID'] || p.Player}
                            title={`${p.Player}: ${pr.toFixed(1)}`}
                            style={{
                              flex: 1,
                              height: `${heightPct}%`,
                              background: isThis ? '#ff9800' : '#03dac6',
                              borderRadius: '1px 1px 0 0',
                              border: isThis ? '1px solid #fff' : 'none'
                            }}
                          />
                        );
                      })}
                    </div>
                    <div style={{ marginTop: '6px', fontSize: '10px', color: '#888', textAlign: 'center' }}>
                      {relPos === 'Overall' ? 'Top 200 Players' : `Top 50 ${relPos}s`} (Orange = This Player)
                    </div>
                  </div>
                );
              })()}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// --- HELPER: Clickable player name ---
function PlayerNameButton({ player, onClick, style = {} }) {
  return (
    <span
      onClick={(e) => {
        e.stopPropagation();
        onClick(player);
      }}
      style={{
        cursor: 'pointer',
        textDecoration: 'none',
        transition: 'opacity 0.2s',
        ...style
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.opacity = '0.8';
        e.currentTarget.style.textDecoration = 'underline';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.opacity = '1';
        e.currentTarget.style.textDecoration = 'none';
      }}
    >
      {player.Player}
    </span>
  );
}

const OWNER_AVATARS = {
  Dan: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/dan.png',
  Daniel: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/dan.png',
  Alex: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/alex.png',
  Adrian: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/adrian.png',
  Garrett: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/garrett.png',
  Mark: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/mark.png',
  Preston: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/preston.png',
  Tim: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/tim.png',
  Will: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/will.png',
  Anil: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/anil.png',
  default: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/default.png'
};

const DEFAULT_OWNER_PROFILES = {
  Adrian: { archetype: { name: 'Value Hunter', emoji: '🎯' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.25 }, mid: { pitcherRate: 0.35 }, late: { pitcherRate: 0.3 } } } },
  Alex: { archetype: { name: 'Pitching Hoarder', emoji: '⚾' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.5 }, mid: { pitcherRate: 0.4 }, late: { pitcherRate: 0.35 } } } },
  Anil: { archetype: { name: 'Hitting Focused', emoji: '🏏' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.15 }, mid: { pitcherRate: 0.25 }, late: { pitcherRate: 0.35 } } } },
  Daniel: { archetype: { name: 'Ace Hunter', emoji: '👑' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.4 }, mid: { pitcherRate: 0.3 }, late: { pitcherRate: 0.3 } } } },
  Dan: { archetype: { name: 'Ace Hunter', emoji: '👑' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.4 }, mid: { pitcherRate: 0.3 }, late: { pitcherRate: 0.3 } } } },
  Garrett: { archetype: { name: 'Bold Gambler', emoji: '🎲' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.3 }, mid: { pitcherRate: 0.35 }, late: { pitcherRate: 0.35 } } } },
  Mark: { archetype: { name: 'Value Hunter', emoji: '🎯' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.3 }, mid: { pitcherRate: 0.3 }, late: { pitcherRate: 0.3 } } } },
  Preston: { archetype: { name: 'Pitching Hoarder', emoji: '⚾' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.45 }, mid: { pitcherRate: 0.4 }, late: { pitcherRate: 0.3 } } } },
  Tim: { archetype: { name: 'Hitting Focused', emoji: '🏏' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.2 }, mid: { pitcherRate: 0.25 }, late: { pitcherRate: 0.35 } } } },
  Will: { archetype: { name: 'Ace Hunter', emoji: '👑' }, tendencies: { positionPreferences: { early: { pitcherRate: 0.35 }, mid: { pitcherRate: 0.3 }, late: { pitcherRate: 0.3 } } } }
};

// --- MODE SELECTION MODAL ---
function ModeSelectionModal({ onSelectMode, roomSeason = 2027, onSeasonChange }) {
  const modes = [
    {
      id: 'live',
      label: '🎯 Live Draft',
      desc: 'Sync with the real draft board. Your picks write to Supabase and update everyone in real time.',
      color: '#03dac6'
    },
    {
      id: 'multitest',
      label: '🤝 Multiplayer Test',
      desc: 'Shared board over Supabase. Multiple people can test together. Includes a Reset button to wipe all picks.',
      color: '#ff9800'
    },
    {
      id: 'mockdraft',
      label: '🤖 Mock Draft',
      desc: 'Fully local, no Supabase writes. The AI auto-picks for every other owner using their historical tendencies. Perfect for solo prep.',
      color: '#bb86fc'
    },
    {
      id: 'test',
      label: '🧪 Solo Test',
      desc: 'Local board, no Supabase writes. You pick for every slot yourself — useful for UI testing.',
      color: '#ffc107'
    },
    {
      id: 'mobile',
      label: '📱 Mobile',
      desc: 'Streamlined view for phones. Queue and draft from a small screen while away from your desk.',
      color: '#4caf50'
    },
    {
      id: 'host',
      label: '🎙️ Host / TV',
      desc: 'Big-screen broadcast view. Shows pick card, AI commentary, and countdown. Meant for a shared display.',
      color: '#cf6679'
    }
  ];

  return (
    <div style={{
      position: 'fixed',
      inset: 0,
      background: 'rgba(0,0,0,0.95)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 1000,
      padding: '20px'
    }}>
      <div style={{
        background: '#1e1e1e',
        borderRadius: '16px',
        padding: '36px 32px',
        textAlign: 'center',
        border: '2px solid #bb86fc',
        boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
        width: '100%',
        maxWidth: '720px'
      }}>
        <div style={{ fontSize: '38px', marginBottom: '6px' }}>⚾</div>
        
        {/* Quick season toggle */}
        <div style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', background: '#111', padding: '4px 10px', borderRadius: '20px', border: '1px solid #333', marginBottom: '14px' }}>
          <button
            onClick={() => onSeasonChange && onSeasonChange(2027)}
            style={{
              background: roomSeason === 2027 ? '#bb86fc' : 'transparent',
              color: roomSeason === 2027 ? '#000' : '#888',
              border: 'none',
              borderRadius: '14px',
              padding: '4px 12px',
              fontSize: '12px',
              fontWeight: 800,
              cursor: 'pointer'
            }}
          >
            🚀 2027 Draft (Active Prep)
          </button>
          <button
            onClick={() => onSeasonChange && onSeasonChange(2026)}
            style={{
              background: roomSeason === 2026 ? '#03dac6' : 'transparent',
              color: roomSeason === 2026 ? '#000' : '#888',
              border: 'none',
              borderRadius: '14px',
              padding: '4px 12px',
              fontSize: '12px',
              fontWeight: 800,
              cursor: 'pointer'
            }}
          >
            🏛️ 2026 Draft (Archive)
          </button>
        </div>

        <h1 style={{ fontSize: '28px', color: '#bb86fc', margin: '0 0 6px', fontWeight: 800, letterSpacing: '1px' }}>
          Hefty War Room {roomSeason}
        </h1>
        <p style={{ color: '#888', fontSize: '14px', margin: '0 0 28px' }}>
          {roomSeason === 2027 
            ? '2027 Draft Prep & War Room with 2026 trade continuity'
            : '2026 Official Completed Draft Archive & Historical Rosters'}
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
          {modes.map(mode => (
            <button
              key={mode.id}
              onClick={() => onSelectMode(mode.id)}
              style={{
                background: `${mode.color}18`,
                border: `2px solid ${mode.color}55`,
                borderRadius: '12px',
                padding: '16px 18px',
                cursor: 'pointer',
                textAlign: 'left',
                transition: 'all 0.15s ease',
                color: '#e0e0e0'
              }}
              onMouseEnter={e => {
                e.currentTarget.style.background = `${mode.color}30`;
                e.currentTarget.style.borderColor = mode.color;
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = `0 8px 24px ${mode.color}33`;
              }}
              onMouseLeave={e => {
                e.currentTarget.style.background = `${mode.color}18`;
                e.currentTarget.style.borderColor = `${mode.color}55`;
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = 'none';
              }}
            >
              <div style={{ fontSize: '15px', fontWeight: 700, color: mode.color, marginBottom: '6px' }}>
                {mode.label}
              </div>
              <div style={{ fontSize: '12px', color: '#999', lineHeight: '1.4' }}>
                {mode.desc}
              </div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

// --- LOGIN MODAL ---
function LoginModal({ owners, onLogin, roomSeason = 2027 }) {
  const [selectedOwner, setSelectedOwner] = useState(owners[0] || "");

  return (
    <div style={styles.loginOverlay}>
      <div style={styles.loginBox}>
        <h1 style={{ fontSize: '36px', marginBottom: '10px', color: 'var(--accent)' }}>
          ⚾ Hefty War Room {roomSeason}
        </h1>
        <p style={{ marginBottom: '30px', color: '#888' }}>Select your team to enter the draft</p>
        <div>
          <select 
            value={selectedOwner} 
            onChange={(e) => setSelectedOwner(e.target.value)}
            style={styles.loginSelect}
          >
            {owners.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
        </div>
        <button 
          onClick={() => onLogin(selectedOwner)}
          style={styles.loginButton}
        >
          Enter Draft Room
        </button>
      </div>
    </div>
  );
}

// --- COUNTDOWN TIMER ---
function CountdownTimer({ pickStartTime }) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const elapsed = Math.floor((now - pickStartTime) / 1000);
  const secondsLeft = Math.max(0, 60 - elapsed);
  const isWarning = secondsLeft <= 10;

  return (
    <div style={{
      ...styles.clock,
      color: isWarning ? '#ff0000' : 'var(--alert)',
      animation: isWarning ? 'pulse 1s infinite' : 'none'
    }}>
      {secondsLeft}s
    </div>
  );
}

// --- TICKER ---
function Ticker({ recentPicks, players }) {
  if (recentPicks.length === 0) return null;

  return (
    <div style={styles.tickerContainer}>
      <div style={styles.tickerContent}>
        {recentPicks.slice(-10).reverse().map(pick => {
          const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
          return (
            <span key={pick['Overall Pick']} style={styles.tickerItem}>
              #{pick['Overall Pick']} {pick.Owner}: {player ? player.Player : 'Unknown'} ({player?.Position || '?'})
            </span>
          );
        })}
      </div>
    </div>
  );
}

// --- ON DECK SIDEBAR ---
function OnDeckSidebar({ upcomingPicks, currentUser }) {
  return (
    <div style={styles.sidebar}>
      <h3 style={{ color: 'var(--highlight)', margin: '0 0 15px 0', fontSize: '18px', textTransform: 'uppercase', letterSpacing: '1px' }}>
        On Deck
      </h3>
      {upcomingPicks.length === 0 ? (
        <div style={{ color: '#666', fontStyle: 'italic' }}>Draft complete</div>
      ) : (
        upcomingPicks.map(p => {
          const isUser = p.Owner === currentUser;
          return (
            <div 
              key={p['Overall Pick']} 
              style={{
                ...styles.deckItem,
                background: isUser ? '#2e2540' : '#222',
                borderLeft: isUser ? '4px solid #bb86fc' : 'none',
                boxShadow: isUser ? '0 0 10px rgba(187, 134, 252, 0.2)' : 'none'
              }}
            >
              <div>
                <span style={{ color: isUser ? '#bb86fc' : '#888', fontWeight: isUser ? 'bold' : 'normal', marginRight: '8px' }}>
                  #{p['Overall Pick']}
                </span>
                <span style={{ color: isUser ? '#fff' : '#ccc', fontWeight: isUser ? 'bold' : 'normal' }}>
                  {p.Owner}
                </span>
              </div>
              <div style={{ fontSize: '12px', color: '#666' }}>
                R{p.Round} P{p.Pick}
              </div>
            </div>
          );
        })
      )}
    </div>
  );
}

// --- TEAM NEEDS SUMMARY ---
function TeamNeedsSummary({ myPicks, players }) {
  const needs = useMemo(() => {
    const counts = {
      C: 0, '1B': 0, '2B': 0, '3B': 0, SS: 0, OF: 0, DH: 0, SP: 0, RP: 0, Bench: 0
    };
    
    myPicks.forEach(pick => {
      const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
      if (!player) return;
      const pos = player.Position || '';
      if (pos.includes('C')) counts.C++;
      else if (pos.includes('1B')) counts['1B']++;
      else if (pos.includes('2B')) counts['2B']++;
      else if (pos.includes('3B')) counts['3B']++;
      else if (pos.includes('SS')) counts.SS++;
      else if (pos.includes('OF')) counts.OF++;
      else if (pos.includes('SP')) counts.SP++;
      else if (pos.includes('RP')) counts.RP++;
      else counts.Bench++;
    });
    
    return counts;
  }, [myPicks, players]);

  return (
    <div style={styles.infoWidget}>
      <div style={styles.infoWidgetTitle}>Team Roster Breakdown</div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '8px', fontSize: '12px' }}>
        {Object.entries(needs).map(([pos, count]) => (
          <div key={pos} style={{ textAlign: 'center', background: '#2a2a2a', padding: '6px', borderRadius: '4px' }}>
            <div style={{ color: '#888', fontSize: '10px' }}>{pos}</div>
            <div style={{ color: count > 0 ? '#03dac6' : '#666', fontWeight: 'bold', fontSize: '14px' }}>
              {count}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// --- RECENT ACTIVITY WIDGET ---
function RecentActivityWidget({ recentPicks, players, onPlayerClick, playerInfo }) {
  const lastFive = recentPicks.slice(-5).reverse();

  return (
    <div style={styles.infoWidget}>
      <div style={styles.infoWidgetTitle}>Recent Picks</div>
      {lastFive.length === 0 ? (
        <div style={{ color: '#666', fontSize: '12px', fontStyle: 'italic' }}>No picks yet</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
          {lastFive.map(pick => {
            const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
            const injury = player ? getInjuryIndicator(player['ESPN PlayerID'], playerInfo) : null;
            return (
              <div key={pick['Overall Pick']} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '12px' }}>
                <div>
                  <span style={{ color: '#888', marginRight: '6px' }}>#{pick['Overall Pick']}</span>
                  <span style={{ color: 'var(--highlight)', fontWeight: 'bold' }}>{pick.Owner}</span>
                </div>
                {player ? (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                    <div style={{ width: '22px', height: '22px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #444' }}>
                      <img
                        src={getPlayerHeadshotUrl(player)}
                        alt=""
                        style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                        onError={(e) => handleHeadshotError(e, player)}
                        referrerPolicy="no-referrer"
                        loading="lazy"
                      />
                    </div>
                    <PlayerNameButton 
                      player={player} 
                      onClick={onPlayerClick}
                      style={{ color: injury ? injury.color : '#fff' }}
                    />
                    <span style={{ color: '#888', fontSize: '10px' }}>({player.Position})</span>
                  </div>
                ) : (
                  <span style={{ color: '#666' }}>--</span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// --- QUEUE PREVIEW WIDGET ---
function QueuePreviewWidget({ queue, onPlayerClick, playerInfo }) {
  return (
    <div style={styles.infoWidget}>
      <div style={styles.infoWidgetTitle}>Queue ({queue.length})</div>
      {queue.length === 0 ? (
        <div style={{ color: '#666', fontSize: '12px', fontStyle: 'italic' }}>Queue is empty. Star players from the pool to add them.</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', maxHeight: '100px', overflowY: 'auto' }}>
          {queue.slice(0, 5).map(player => {
            const injury = getInjuryIndicator(player['ESPN PlayerID'], playerInfo);
            return (
              <div key={player['ESPN PlayerID']} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <div style={{ width: '22px', height: '22px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #444' }}>
                    <img
                      src={getPlayerHeadshotUrl(player)}
                      alt=""
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                      onError={(e) => handleHeadshotError(e, player)}
                      referrerPolicy="no-referrer"
                      loading="lazy"
                    />
                  </div>
                  <PlayerNameButton 
                    player={player} 
                    onClick={onPlayerClick}
                    style={{ color: injury ? injury.color : '#fff', fontWeight: 'bold' }}
                  />
                </div>
                <span style={{ color: '#888', fontSize: '10px' }}>{player.Position} • {player.Team}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// --- PLAYER POOL PANEL ---
function PlayerPoolPanel({ players, onDraft, isMyTurn, queue, onAddToQueue, onRemoveFromQueue, draftMode, testModePicks, allPicks, onPlayerClick, playerInfo, isMobile = false }) {
  const [sortConfig, setSortConfig] = useState({ key: 'Hefty Keeper Rank', direction: 'asc' });
  const [filterPos, setFilterPos] = useState('');
  const [searchText, setSearchText] = useState('');

  const sortedPlayers = useMemo(() => {
    const activePicks = (draftMode === 'test' || draftMode === 'mockdraft') && testModePicks?.length > 0
      ? testModePicks
      : (allPicks || []);
    const draftedSet = new Set(activePicks.map(p => String(p['ESPN PlayerID'])).filter(id => id && id !== 'null' && id !== 'undefined'));

    let filtered = [...players].filter(p => {
      const pid = String(p['ESPN PlayerID']);
      if (draftedSet.has(pid)) return false;
      return p.Availability === 'Available' || !p.Availability;
    });
    
    if (searchText) {
      filtered = filtered.filter(p => 
        p.Player?.toLowerCase().includes(searchText.toLowerCase()) ||
        p.Team?.toLowerCase().includes(searchText.toLowerCase())
      );
    }
    
    if (filterPos) {
      filtered = filtered.filter(p => p.Position?.includes(filterPos));
    }

    const getPlayerRank = (p) => {
      const r = p['Hefty Keeper Rank'] ?? p['Hefty Single Season Rank'] ?? p['ESPN Keeper Rank'] ?? p.rank;
      const num = parseFloat(r);
      return isNaN(num) || num <= 0 ? 9999 : num;
    };

    const getPlayerPrice = (p) => {
      const pr = p['Hefty Keeper Price'] ?? p['Hefty Single Season Price'] ?? p['ESPN Price'] ?? p.price;
      const num = parseFloat(pr);
      return isNaN(num) ? -999 : num;
    };
    
    filtered.sort((a, b) => {
      let comparison = 0;
      if (sortConfig.key === 'Hefty Keeper Rank' || sortConfig.key === 'rank') {
        comparison = getPlayerRank(a) - getPlayerRank(b);
      } else if (sortConfig.key === 'Hefty Keeper Price' || sortConfig.key === 'price') {
        comparison = getPlayerPrice(a) - getPlayerPrice(b);
      } else if (sortConfig.key === 'ZIPSERA' || sortConfig.key === 'ZIPSWHIP') {
        const aVal = parseFloat(a[sortConfig.key]);
        const bVal = parseFloat(b[sortConfig.key]);
        const aSafe = isNaN(aVal) || aVal <= 0 ? 999 : aVal;
        const bSafe = isNaN(bVal) || bVal <= 0 ? 999 : bVal;
        comparison = aSafe - bSafe;
      } else {
        const aVal = a[sortConfig.key] ?? '';
        const bVal = b[sortConfig.key] ?? '';
        const aNum = parseFloat(aVal);
        const bNum = parseFloat(bVal);
        if (!isNaN(aNum) && !isNaN(bNum)) {
          comparison = aNum - bNum;
        } else {
          comparison = String(aVal).localeCompare(String(bVal));
        }
      }
      
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
    
    return filtered;
  }, [players, sortConfig, filterPos, searchText, draftMode, testModePicks, allPicks]);

  const DESC_FIRST_KEYS = useMemo(() => new Set([
    'Hefty Keeper Price',
    'ZIPSR',
    'ZIPSHR',
    'ZIPSRBI',
    'ZIPSSB',
    'ZIPSOBP',
    'ZIPSK',
    'ZIPSQS',
    'ZIPSSV+HDs'
  ]), []);

  const requestSort = (key) => {
    setSortConfig(prev => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === 'asc' ? 'desc' : 'asc'
        };
      }
      const initialDirection = DESC_FIRST_KEYS.has(key) ? 'desc' : 'asc';
      return { key, direction: initialDirection };
    });
  };

  const getSortIcon = (key) => {
    if (sortConfig.key !== key) return '';
    return sortConfig.direction === 'asc' ? ' ▲' : ' ▼';
  };

  const isQueued = (playerId) => queue.some(p => String(p['ESPN PlayerID']) === String(playerId));

  return (
    <div style={{ display: 'flex', gridColumn: '1 / -1', gap: '20px', height: '100%' }}>
      {/* Main Pool */}
      <div style={{ ...styles.wrColumn, flex: '3' }}>
        <div style={styles.wrHeader}>
          <span>Available Players {draftMode === 'test' && <span style={{ color: '#ffc107', fontSize: '14px' }}>(TEST MODE)</span>}{draftMode === 'mockdraft' && <span style={{ color: '#bb86fc', fontSize: '14px' }}>(MOCK DRAFT)</span>}</span>
          <div style={{ display: 'flex', gap: '10px' }}>
            <select 
              value={filterPos} 
              onChange={e => setFilterPos(e.target.value)}
              style={styles.filterControl}
            >
              <option value="">All Pos</option>
              <option value="C">C</option>
              <option value="1B">1B</option>
              <option value="2B">2B</option>
              <option value="3B">3B</option>
              <option value="SS">SS</option>
              <option value="OF">OF</option>
              <option value="SP">SP</option>
              <option value="RP">RP</option>
            </select>
            <input 
              type="text"
              placeholder="Search player or team..."
              value={searchText}
              onChange={e => setSearchText(e.target.value)}
              style={styles.filterControl}
            />
          </div>
        </div>
        
        <div style={styles.poolTableContainer}>
          <table style={styles.table}>
            <thead style={styles.tableHead}>
              <tr>
                <th style={styles.th} width="75">Action</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Player')}>Player{getSortIcon('Player')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Position')}>Pos{getSortIcon('Position')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Team')}>Team{getSortIcon('Team')}</th>
                <th style={{ ...styles.th, color: '#ffc107', cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Hefty Keeper Price')}>Hefty ${getSortIcon('Hefty Keeper Price')}</th>
                <th style={{ ...styles.th, color: '#90caf9', cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('Hefty Keeper Rank')}>Rank{getSortIcon('Hefty Keeper Rank')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSR')}>R{getSortIcon('ZIPSR')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSHR')}>HR{getSortIcon('ZIPSHR')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSRBI')}>RBI{getSortIcon('ZIPSRBI')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSSB')}>SB{getSortIcon('ZIPSSB')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSOBP')}>OBP{getSortIcon('ZIPSOBP')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSK')}>K{getSortIcon('ZIPSK')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSQS')}>QS{getSortIcon('ZIPSQS')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSERA')}>ERA{getSortIcon('ZIPSERA')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSWHIP')}>WHIP{getSortIcon('ZIPSWHIP')}</th>
                <th style={{ ...styles.th, cursor: 'pointer', userSelect: 'none' }} onClick={() => requestSort('ZIPSSV+HDs')}>SV+H{getSortIcon('ZIPSSV+HDs')}</th>
              </tr>
            </thead>
            <tbody>
              {sortedPlayers.slice(0, 200).map(p => {
                const queued = isQueued(p['ESPN PlayerID']);
                const isPitcher = p.Position?.includes('SP') || p.Position?.includes('RP');
                const injury = getInjuryIndicator(p['ESPN PlayerID'], playerInfo);
                const heftyPrice = p['Hefty Keeper Price'] ?? p['Hefty Single Season Price'];
                const heftyRank = p['Hefty Keeper Rank'] ?? p['Hefty Single Season Rank'];
                
                return (
                  <tr key={p['ESPN PlayerID']} style={styles.tableRow}>
                    <td style={styles.td}>
                      {isMyTurn && (
                        <button onClick={() => onDraft(p)} style={styles.btnDraft}>
                          DRAFT
                        </button>
                      )}
                      <button 
                        onClick={() => queued ? onRemoveFromQueue(p['ESPN PlayerID']) : onAddToQueue(p)}
                        style={queued ? styles.btnStarActive : styles.btnStar}
                        title={queued ? "Remove from queue" : "Add to queue"}
                      >
                        {queued ? '★' : '☆'}
                      </button>
                    </td>
                    <td style={styles.td}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <div style={{
                          width: '26px',
                          height: '26px',
                          borderRadius: '50%',
                          overflow: 'hidden',
                          background: '#222',
                          flexShrink: 0,
                          border: '1px solid #444'
                        }}>
                          <img
                            src={getPlayerHeadshotUrl(p)}
                            alt=""
                            style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                            onError={(e) => handleHeadshotError(e, p)}
                            referrerPolicy="no-referrer"
                            loading="lazy"
                          />
                        </div>
                        <PlayerNameButton 
                          player={p} 
                          onClick={onPlayerClick}
                          style={{ color: injury ? injury.color : '#fff', fontWeight: 'bold' }}
                        />
                        {injury && (
                          <span style={{
                            marginLeft: '4px',
                            padding: '2px 6px',
                            borderRadius: '3px',
                            background: injury.color,
                            color: '#000',
                            fontSize: '10px',
                            fontWeight: 'bold'
                          }}>
                            {injury.status}
                          </span>
                        )}
                      </div>
                    </td>
                    <td style={styles.td}>{p.Position}</td>
                    <td style={styles.td}>{p.Team}</td>
                    <td style={{ ...styles.td, textAlign: 'right', fontWeight: 'bold', color: heftyPrice ? '#ffc107' : '#666' }}>
                      {heftyPrice !== undefined && heftyPrice !== null && heftyPrice !== '' ? `$${heftyPrice}` : '-'}
                    </td>
                    <td style={{ ...styles.td, textAlign: 'center', color: '#90caf9', fontSize: '11px', fontWeight: '600' }}>
                      {heftyRank ? `#${heftyRank}` : '-'}
                    </td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSR || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSHR || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSRBI || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSSB || '-')}</td>
                    <td style={styles.td}>{isPitcher ? '-' : (p.ZIPSOBP || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSK || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSQS || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSERA || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p.ZIPSWHIP || '-')}</td>
                    <td style={styles.td}>{!isPitcher ? '-' : (p['ZIPSSV+HDs'] || '-')}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Queue */}
      {!isMobile && (
        <div style={{ ...styles.wrColumn, flex: '1' }}>
          <div style={styles.wrHeader}>My Queue ({queue.length})</div>
          <div style={{ overflowY: 'auto', flexGrow: 1 }}>
            {queue.length === 0 ? (
              <div style={{ padding: '20px', color: '#666', textAlign: 'center', fontSize: '13px' }}>
                Click ☆ next to any player in the pool to queue them up.
              </div>
            ) : (
              queue.map(p => (
                <div key={p['ESPN PlayerID']} style={styles.queueItem}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <div style={{ width: '28px', height: '28px', borderRadius: '50%', overflow: 'hidden', background: '#222', flexShrink: 0, border: '1px solid #444' }}>
                      <img
                        src={getPlayerHeadshotUrl(p)}
                        alt=""
                        style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                        onError={(e) => handleHeadshotError(e, p)}
                        referrerPolicy="no-referrer"
                        loading="lazy"
                      />
                    </div>
                    <div>
                      <div style={{ fontWeight: 'bold' }}>{p.Player}</div>
                      <div style={{ fontSize: '12px', color: '#888' }}>{p.Position} - {p.Team}</div>
                    </div>
                  </div>
                  <button 
                    onClick={() => onRemoveFromQueue(p['ESPN PlayerID'])}
                    style={styles.btnStarActive}
                    title="Remove"
                  >
                    ✕
                  </button>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// --- ROSTER MANAGER PANEL ---
function RosterManagerPanel({ allPicks, players, currentUser }) {
  const [selectedOwner, setSelectedOwner] = useState(currentUser || DRAFT_OWNERS[0]);
  const [assignments, setAssignments] = useState({});

  const ownerPicks = allPicks.filter(p => p.Owner === selectedOwner && p['ESPN PlayerID']);
  const ownerPlayers = ownerPicks.map(pick => 
    players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']))
  ).filter(Boolean);

  const slottedPlayerIds = new Set(
    Object.values(assignments).filter(Boolean).map(p => String(p['ESPN PlayerID']))
  );

  const unslottedPlayers = ownerPlayers.filter(p => !slottedPlayerIds.has(String(p['ESPN PlayerID'])));

  const totals = useMemo(() => {
    const slots = Object.values(assignments).filter(Boolean);
    const batters = slots.filter(p => !p.Position?.includes('SP') && !p.Position?.includes('RP'));
    const pitchers = slots.filter(p => p.Position?.includes('SP') || p.Position?.includes('RP'));
    
    return {
      r: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSR) || 0), 0),
      hr: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSHR) || 0), 0),
      rbi: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSRBI) || 0), 0),
      sb: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSSB) || 0), 0),
      obp: batters.length > 0 ? 
        batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSOBP) || 0), 0) / batters.length : 0,
      k: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSK) || 0), 0),
      qs: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSQS) || 0), 0),
      era: pitchers.length > 0 ?
        pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSERA) || 0), 0) / pitchers.length : 0,
      whip: pitchers.length > 0 ?
        pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSWHIP) || 0), 0) / pitchers.length : 0,
      sv: pitchers.reduce((sum, p) => sum + (parseFloat(p['ZIPSSV+HDs']) || 0), 0)
    };
  }, [assignments]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <span>Roster Manager</span>
        <div style={{ fontSize: '14px', color: '#ccc', display: 'flex', alignItems: 'center', gap: '8px' }}>
          Viewing: 
          <select 
            value={selectedOwner}
            onChange={e => setSelectedOwner(e.target.value)}
            style={styles.filterControl}
          >
            {DRAFT_OWNERS.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
        </div>
      </div>

      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th} width="60">Slot</th>
              <th style={styles.th} width="260">Player</th>
              <th style={styles.th}>R</th>
              <th style={styles.th}>HR</th>
              <th style={styles.th}>RBI</th>
              <th style={styles.th}>SB</th>
              <th style={styles.th}>OBP</th>
              <th style={styles.th}>K</th>
              <th style={styles.th}>QS</th>
              <th style={styles.th}>ERA</th>
              <th style={styles.th}>WHIP</th>
              <th style={styles.th}>SV+H</th>
            </tr>
          </thead>
          <tbody>
            {ROSTER_SLOTS.map(slot => {
              const player = assignments[slot.id];
              const isPitcher = player?.Position?.includes('SP') || player?.Position?.includes('RP');
              
              const eligiblePlayers = ownerPlayers.filter(p => {
                if (player && String(p['ESPN PlayerID']) === String(player['ESPN PlayerID'])) return true;
                if (slottedPlayerIds.has(String(p['ESPN PlayerID']))) return false;
                const pos = p.Position || '';
                return slot.eligible.includes('ALL') || slot.eligible.some(e => pos.includes(e));
              });
              
              return (
                <tr key={slot.id} style={styles.tableRow}>
                  <td style={{ ...styles.td, color: '#888', fontWeight: 'bold' }}>{slot.label}</td>
                  <td style={styles.td}>
                    <select 
                      style={styles.rosterSelect}
                      value={player ? player['ESPN PlayerID'] : ''}
                      onChange={(e) => {
                        if (e.target.value === '') {
                          setAssignments(prev => {
                            const newAssignments = { ...prev };
                            delete newAssignments[slot.id];
                            return newAssignments;
                          });
                        } else {
                          const newPlayer = ownerPlayers.find(p => 
                            String(p['ESPN PlayerID']) === String(e.target.value)
                          );
                          if (newPlayer) {
                            setAssignments(prev => ({ ...prev, [slot.id]: newPlayer }));
                          }
                        }
                      }}
                    >
                      <option value="">-- Empty --</option>
                      {eligiblePlayers.map(p => (
                        <option key={p['ESPN PlayerID']} value={p['ESPN PlayerID']}>
                          {p.Player} ({p.Position})
                        </option>
                      ))}
                    </select>
                  </td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSR || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSHR || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSRBI || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? Math.round(player.ZIPSSB || 0) : '-'}</td>
                  <td style={styles.td}>{player && !isPitcher ? parseFloat(player.ZIPSOBP || 0).toFixed(3) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? Math.round(player.ZIPSK || 0) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? Math.round(player.ZIPSQS || 0) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? parseFloat(player.ZIPSERA || 0).toFixed(2) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? parseFloat(player.ZIPSWHIP || 0).toFixed(3) : '-'}</td>
                  <td style={styles.td}>{player && isPitcher ? Math.round(player['ZIPSSV+HDs'] || 0) : '-'}</td>
                </tr>
              );
            })}
          </tbody>
          <tfoot>
            <tr style={{ background: '#444', fontWeight: 'bold', color: '#fff' }}>
              <td colSpan="2" style={{ ...styles.td, textAlign: 'right', paddingRight: '10px' }}>
                TOTALS:
              </td>
              <td style={styles.td}>{Math.round(totals.r)}</td>
              <td style={styles.td}>{Math.round(totals.hr)}</td>
              <td style={styles.td}>{Math.round(totals.rbi)}</td>
              <td style={styles.td}>{Math.round(totals.sb)}</td>
              <td style={styles.td}>{totals.obp.toFixed(3)}</td>
              <td style={styles.td}>{Math.round(totals.k)}</td>
              <td style={styles.td}>{Math.round(totals.qs)}</td>
              <td style={styles.td}>{totals.era.toFixed(2)}</td>
              <td style={styles.td}>{totals.whip.toFixed(3)}</td>
              <td style={styles.td}>{Math.round(totals.sv)}</td>
            </tr>
          </tfoot>
        </table>
        
        {/* Unslotted Players */}
        {unslottedPlayers.length > 0 && (
          <div style={{
            marginTop: '20px',
            padding: '15px',
            background: '#222',
            borderRadius: '8px',
            borderLeft: '4px solid #ffc107'
          }}>
            <div style={{ 
              fontWeight: 'bold', 
              color: 'var(--highlight)', 
              marginBottom: '10px',
              fontSize: '14px'
            }}>
              Unslotted Players ({unslottedPlayers.length})
            </div>
            <div style={{ 
              color: '#ccc', 
              fontSize: '13px',
              lineHeight: '1.8'
            }}>
              {unslottedPlayers.map((p, idx) => (
                <span key={p['ESPN PlayerID']}>
                  <strong style={{ color: '#fff' }}>{p.Player}</strong>
                  <span style={{ color: '#888' }}> ({p.Position})</span>
                  {idx < unslottedPlayers.length - 1 && ', '}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// --- 2027 DRAFT CAPITAL CALCULATOR ---
function compute2027DraftPicks(draftTrades = []) {
  const owners = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"].sort();
  const picks = [];

  // Base draft order: 26 rounds (Rounds 7 through 32), 1 pick per owner per round (234 total picks)
  for (let round = 7; round <= 32; round++) {
    owners.forEach(owner => {
      picks.push({
        round,
        originalOwner: owner,
        currentOwner: owner,
        isTraded: false,
        tradeDetails: null
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

// --- 2027 DRAFT ORDER GENERATOR ---
function generate2027DraftOrder(draftTrades = [], keepers2027 = [], compPicks2027 = []) {
  const fullOrder = [];
  let overall = 1;

  // Rounds 1-6: Keepers (54 picks across 9 owners)
  for (let r = 1; r <= 6; r++) {
    const roundOwners = r % 2 === 1 ? [...DRAFT_OWNERS] : [...DRAFT_OWNERS].reverse();
    roundOwners.forEach((owner, idx) => {
      // Find keeper assigned to this owner and slot if submitted
      const keeper = (keepers2027 || []).find(k => {
        const rawOwner = k.owner || k.owner_name || k.team_owner || '';
        const oName = (rawOwner === 'Dan' || rawOwner === 'dsellinger') ? 'Daniel' : rawOwner;
        return oName.toLowerCase() === owner.toLowerCase() && (Number(k.keeper_slot) === r || (!k.keeper_slot && idx === 0));
      });

      const espnId = keeper ? String(keeper.espn_player_id || keeper.player_id || '') : null;

      fullOrder.push({
        'Overall Pick': overall,
        Round: r,
        Pick: idx + 1,
        'Raw Pick Number': overall,
        'Original Owner': owner,
        Owner: owner,
        'Pick Traded?': 'N',
        'Comp Pick?': null,
        'Number Pick for Owner': `${owner}${r - 1}`,
        'ESPN PlayerID': espnId || null,
        Selection: keeper ? (keeper.player_name || keeper.Player) : null,
        isKeeper: true
      });
      overall++;
    });
  }

  // Rounds 7-32: Drafted rounds with 2026/2027 trades applied
  const basePicks = compute2027DraftPicks(draftTrades);
  const ownerPickCounters = {};
  DRAFT_OWNERS.forEach(o => { ownerPickCounters[o] = 1; });

  for (let r = 7; r <= 32; r++) {
    const roundPicks = basePicks.filter(p => p.round === r);
    const roundCompPicks = (compPicks2027 || []).filter(cp => cp.round_num === r);

    let pickInRound = 1;
    roundPicks.forEach(p => {
      const owner = p.currentOwner;
      const count = ownerPickCounters[owner] || 1;
      ownerPickCounters[owner] = count + 1;

      fullOrder.push({
        'Overall Pick': overall,
        Round: r,
        Pick: pickInRound,
        'Raw Pick Number': overall,
        'Original Owner': p.originalOwner,
        Owner: p.currentOwner,
        'Pick Traded?': p.isTraded ? 'Y' : 'N',
        'Comp Pick?': null,
        'Number Pick for Owner': `${owner}${count}`,
        'ESPN PlayerID': null,
        Selection: null,
        tradeDetails: p.tradeDetails
      });
      pickInRound++;
      overall++;
    });

    roundCompPicks.forEach(cp => {
      const owner = cp.owner_name === 'Dan' ? 'Daniel' : cp.owner_name;
      const count = ownerPickCounters[owner] || 1;
      ownerPickCounters[owner] = count + 1;

      fullOrder.push({
        'Overall Pick': overall,
        Round: r,
        Pick: pickInRound,
        'Raw Pick Number': overall,
        'Original Owner': 'Comp Pick',
        Owner: owner,
        'Pick Traded?': 'N',
        'Comp Pick?': cp.comp_type || 'Y - Awarded',
        'Number Pick for Owner': `${owner}${count}`,
        'ESPN PlayerID': null,
        Selection: null
      });
      pickInRound++;
      overall++;
    });
  }

  return fullOrder;
}

// --- MY PICKS PANEL ---
function MyPicksPanel({ allPicks, players, currentUser, draftTrades = [], roomSeason = 2027 }) {
  const [localSeasonView, setLocalSeasonView] = useState(null);
  const seasonView = localSeasonView || String(roomSeason || '2027');
  const setSeasonView = setLocalSeasonView;

  const userPicks = allPicks.filter(p => p.Owner === currentUser);
  const filledPicks = userPicks.filter(p => p['ESPN PlayerID']);

  // 2027 Picks calculation
  const computed2027Picks = useMemo(() => {
    return compute2027DraftPicks(draftTrades);
  }, [draftTrades]);

  const user2027Owned = useMemo(() => {
    return computed2027Picks.filter(p => p.currentOwner === currentUser);
  }, [computed2027Picks, currentUser]);

  const user2027TradedAway = useMemo(() => {
    return computed2027Picks.filter(p => p.originalOwner === currentUser && p.currentOwner !== currentUser);
  }, [computed2027Picks, currentUser]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          <span>Draft Picks ({currentUser})</span>
          <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
            <button
              onClick={() => setSeasonView('2026')}
              style={{
                background: seasonView === '2026' ? 'var(--highlight)' : 'transparent',
                color: seasonView === '2026' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              2026 Selections
            </button>
            <button
              onClick={() => setSeasonView('2027')}
              style={{
                background: seasonView === '2027' ? '#ffb74d' : 'transparent',
                color: seasonView === '2027' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 10px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              🎟️ 2027 Upcoming Draft Capital
            </button>
          </div>
        </div>

        <span style={{ fontSize: '13px', color: '#888' }}>
          {seasonView === '2026' ? (
            `${filledPicks.length} of ${userPicks.length} picks made`
          ) : (
            `${user2027Owned.length} Total Picks Held for 2027 Draft (${user2027Owned.length - 27 >= 0 ? `+${user2027Owned.length - 27}` : user2027Owned.length - 27} Net)`
          )}
        </span>
      </div>
      
      <div style={styles.poolTableContainer}>
        {seasonView === '2026' ? (
          <table style={styles.table}>
            <thead style={styles.tableHead}>
              <tr>
                <th style={styles.th}>Pick #</th>
                <th style={styles.th}>Round</th>
                <th style={styles.th}>Status</th>
                <th style={styles.th}>Player</th>
                <th style={styles.th}>Position</th>
                <th style={styles.th}>Team</th>
              </tr>
            </thead>
            <tbody>
              {userPicks.map(pick => {
                const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
                const isFilled = Boolean(player);
                
                return (
                  <tr key={pick['Overall Pick']} style={styles.tableRow}>
                    <td style={styles.td}>
                      <strong style={{ color: isFilled ? '#03dac6' : '#888' }}>
                        #{pick['Overall Pick']}
                      </strong>
                    </td>
                    <td style={styles.td}>{pick.Round}</td>
                    <td style={styles.td}>
                      <span style={{
                        padding: '4px 8px',
                        borderRadius: '4px',
                        fontSize: '11px',
                        fontWeight: 'bold',
                        background: isFilled ? '#03dac620' : '#88888820',
                        color: isFilled ? '#03dac6' : '#888'
                      }}>
                        {isFilled ? '✓ FILLED' : 'UPCOMING'}
                      </span>
                    </td>
                    <td style={styles.td}>
                      {player ? (
                        <strong style={{ color: '#fff' }}>{player.Player}</strong>
                      ) : (
                        <span style={{ color: '#666', fontStyle: 'italic' }}>Not yet selected</span>
                      )}
                    </td>
                    <td style={styles.td}>{player?.Position || '-'}</td>
                    <td style={styles.td}>{player?.Team || '-'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            <div>
              <div style={{ fontWeight: 'bold', color: '#ffb74d', marginBottom: '10px', fontSize: '13px' }}>
                Active Pick Inventory ({user2027Owned.length} picks owned for 2027)
              </div>
              <table style={styles.table}>
                <thead style={styles.tableHead}>
                  <tr>
                    <th style={styles.th}>Round</th>
                    <th style={styles.th}>Status</th>
                    <th style={styles.th}>Original Owner</th>
                    <th style={styles.th}>Trade Details / Notes</th>
                  </tr>
                </thead>
                <tbody>
                  {user2027Owned.map((pick, pIdx) => {
                    const isAcquired = pick.originalOwner !== currentUser;
                    return (
                      <tr key={`2027-owned-${pIdx}`} style={styles.tableRow}>
                        <td style={styles.td}>
                          <strong style={{ color: isAcquired ? '#ffb74d' : '#03dac6', fontSize: '13px' }}>
                            Round {pick.round}
                          </strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{
                            padding: '3px 8px',
                            borderRadius: '4px',
                            fontSize: '10px',
                            fontWeight: 'bold',
                            background: isAcquired ? '#ffb74d25' : '#03dac620',
                            color: isAcquired ? '#ffb74d' : '#03dac6',
                            border: isAcquired ? '1px solid #ffb74d50' : '1px solid #03dac650'
                          }}>
                            {isAcquired ? '✓ ACQUIRED' : 'ORIGINAL'}
                          </span>
                        </td>
                        <td style={styles.td}>
                          <span style={{ color: isAcquired ? '#fff' : '#aaa' }}>
                            {pick.originalOwner}
                          </span>
                        </td>
                        <td style={styles.td}>
                          {isAcquired ? (
                            <span style={{ color: '#ffcc80', fontSize: '11px' }}>
                              Acquired via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                              {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                            </span>
                          ) : (
                            <span style={{ color: '#666', fontSize: '11px' }}>Own draft pick</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {user2027TradedAway.length > 0 && (
              <div>
                <div style={{ fontWeight: 'bold', color: '#ff5252', marginBottom: '10px', fontSize: '13px' }}>
                  Picks Traded Away ({user2027TradedAway.length} picks)
                </div>
                <table style={styles.table}>
                  <thead style={styles.tableHead}>
                    <tr>
                      <th style={styles.th}>Round</th>
                      <th style={styles.th}>Status</th>
                      <th style={styles.th}>Traded To</th>
                      <th style={styles.th}>Trade Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {user2027TradedAway.map((pick, pIdx) => (
                      <tr key={`2027-away-${pIdx}`} style={{ ...styles.tableRow, opacity: 0.85 }}>
                        <td style={styles.td}>
                          <strong style={{ color: '#ff5252', textDecoration: 'line-through' }}>
                            Round {pick.round}
                          </strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{
                            padding: '3px 8px',
                            borderRadius: '4px',
                            fontSize: '10px',
                            fontWeight: 'bold',
                            background: '#ff525220',
                            color: '#ff5252',
                            border: '1px solid #ff525240'
                          }}>
                            TRADED AWAY
                          </span>
                        </td>
                        <td style={styles.td}>
                          <strong style={{ color: '#fff' }}>{pick.currentOwner}</strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{ color: '#ff8a80', fontSize: '11px' }}>
                            Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                            {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// --- DRAFT CAPITAL & FUTURE PICKS PANEL ---
function DraftCapitalPanel({ draftTrades = [], compPicks = [], keepers = [], currentUser }) {
  const [selectedOwner, setSelectedOwner] = useState(currentUser || DRAFT_OWNERS[0]);
  const [activeSubTab, setActiveSubTab] = useState('board'); // 'board' | 'ledgers' | 'history' | '2026board'
  const [roundFilter, setRoundFilter] = useState('ALL');

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
        diff: ownedPicks.length - 26,
        acquired,
        tradedAway,
        ownedPicks
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
    for (let r = 7; r <= 32; r++) list.push(r);
    if (roundFilter === 'ALL') return list;
    if (roundFilter === 'EARLY') return list.filter(r => r <= 15);
    if (roundFilter === 'MID') return list.filter(r => r >= 16 && r <= 23);
    if (roundFilter === 'LATE') return list.filter(r => r >= 24);
    return list;
  }, [roundFilter]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      {/* Header */}
      <div style={styles.wrHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          <span>🎟️ Draft Capital & Pick Board</span>
          <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
            <button
              onClick={() => setActiveSubTab('board')}
              style={{
                background: activeSubTab === 'board' ? '#03dac6' : 'transparent',
                color: activeSubTab === 'board' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              📊 2027 Traded Board
            </button>
            <button
              onClick={() => setActiveSubTab('ledgers')}
              style={{
                background: activeSubTab === 'ledgers' ? '#ffb74d' : 'transparent',
                color: activeSubTab === 'ledgers' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              📑 Owner Pick Ledgers
            </button>
            <button
              onClick={() => setActiveSubTab('history')}
              style={{
                background: activeSubTab === 'history' ? '#bb86fc' : 'transparent',
                color: activeSubTab === 'history' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              📜 Trade Log History ({assetTradesList.length})
            </button>
            <button
              onClick={() => setActiveSubTab('2026board')}
              style={{
                background: activeSubTab === '2026board' ? '#00e676' : 'transparent',
                color: activeSubTab === '2026board' ? '#000' : '#888',
                border: 'none',
                borderRadius: '4px',
                padding: '4px 12px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer'
              }}
            >
              💎 2026 Ground Truth Board
            </button>
          </div>
        </div>

        <span style={{ fontSize: '13px', color: '#aaa' }}>
          234 Total Draft Picks (Rounds 7–32 across 9 teams)
        </span>
      </div>

      <div style={styles.poolTableContainer}>
        {/* Owner Net Balance Cards */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(135px, 1fr))',
          gap: '8px',
          marginBottom: '20px'
        }}>
          {DRAFT_OWNERS.map(owner => {
            const stat = ownerStats[owner] || { total: 26, diff: 0, acquired: [], tradedAway: [] };
            const isSelected = selectedOwner === owner;
            const hasActivity = stat.acquired.length > 0 || stat.tradedAway.length > 0;

            return (
              <div
                key={owner}
                onClick={() => setSelectedOwner(owner)}
                style={{
                  background: isSelected ? '#2a2a2a' : '#1a1a1a',
                  border: isSelected ? '2px solid #03dac6' : '1px solid #333',
                  borderRadius: '8px',
                  padding: '10px 12px',
                  cursor: 'pointer',
                  transition: 'all 0.15s ease',
                  position: 'relative'
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
                  <span style={{ fontWeight: 'bold', fontSize: '13px', color: isSelected ? '#03dac6' : '#fff' }}>
                    {owner}
                  </span>
                  <span style={{
                    fontSize: '11px',
                    fontWeight: 'bold',
                    padding: '2px 6px',
                    borderRadius: '4px',
                    background: stat.diff > 0 ? '#00e67620' : stat.diff < 0 ? '#ff525220' : '#88888820',
                    color: stat.diff > 0 ? '#00e676' : stat.diff < 0 ? '#ff5252' : '#888'
                  }}>
                    {stat.diff > 0 ? `+${stat.diff}` : stat.diff < 0 ? `${stat.diff}` : 'Even'}
                  </span>
                </div>

                <div style={{ fontSize: '12px', color: '#ccc', fontWeight: 'bold' }}>
                  {stat.total} picks
                </div>

                {hasActivity ? (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '3px', marginTop: '6px' }}>
                    {stat.acquired.map((a, i) => (
                      <span
                        key={`acq-${i}`}
                        style={{
                          fontSize: '9px',
                          background: '#00e67625',
                          color: '#69f0ae',
                          padding: '1px 4px',
                          borderRadius: '3px',
                          fontWeight: 'bold'
                        }}
                        title={`Acquired Round ${a.round} from ${a.originalOwner}`}
                      >
                        +R{a.round}
                      </span>
                    ))}
                    {stat.tradedAway.map((t, i) => (
                      <span
                        key={`away-${i}`}
                        style={{
                          fontSize: '9px',
                          background: '#ff525225',
                          color: '#ff8a80',
                          padding: '1px 4px',
                          borderRadius: '3px',
                          fontWeight: 'bold'
                        }}
                        title={`Traded away Round ${t.round} to ${t.currentOwner}`}
                      >
                        -R{t.round}
                      </span>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: '10px', color: '#666', marginTop: '6px' }}>
                    All original picks
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Sub-tab Content */}
        {activeSubTab === 'board' && (
          <div>
            {/* Filter Bar */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  Filter Rounds:
                </span>
                {['ALL', 'EARLY', 'MID', 'LATE'].map(rf => (
                  <button
                    key={rf}
                    onClick={() => setRoundFilter(rf)}
                    style={{
                      background: roundFilter === rf ? '#333' : '#1e1e1e',
                      color: roundFilter === rf ? '#03dac6' : '#888',
                      border: roundFilter === rf ? '1px solid #03dac6' : '1px solid #333',
                      borderRadius: '4px',
                      padding: '3px 8px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    {rf === 'ALL' ? 'All (R7-32)' : rf === 'EARLY' ? 'Early (R7-15)' : rf === 'MID' ? 'Mid (R16-23)' : 'Late (R24-32)'}
                  </button>
                ))}
              </div>

              <span style={{ fontSize: '11px', color: '#ffb74d', fontStyle: 'italic' }}>
                * Amber cards indicate pick ownership transferred via approved trade.
              </span>
            </div>

            <div style={{ overflowX: 'auto' }}>
              <table style={{ ...styles.table, minWidth: '900px' }}>
                <thead style={styles.tableHead}>
                  <tr>
                    <th style={{ ...styles.th, width: '90px' }}>Round</th>
                    {DRAFT_OWNERS.map(o => (
                      <th key={o} style={{ ...styles.th, textAlign: 'center' }}>
                        {o}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rounds.map(roundNum => {
                    return (
                      <tr key={`round-${roundNum}`} style={styles.tableRow}>
                        <td style={{ ...styles.td, fontWeight: 'bold', color: '#03dac6', whiteSpace: 'nowrap' }}>
                          Round {roundNum}
                        </td>
                        {DRAFT_OWNERS.map(origOwner => {
                          const pick = computedPicks.find(p => p.round === roundNum && p.originalOwner === origOwner);
                          if (!pick) return <td key={origOwner} style={styles.td}>-</td>;

                          const isTraded = pick.isTraded;
                          return (
                            <td key={origOwner} style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                              {isTraded ? (
                                <div
                                  style={{
                                    background: 'linear-gradient(135deg, rgba(255,183,77,0.25), rgba(255,152,0,0.15))',
                                    border: '1px solid #ffb74d',
                                    borderRadius: '6px',
                                    padding: '4px 6px',
                                    display: 'inline-block',
                                    minWidth: '78px'
                                  }}
                                  title={`Trade #${pick.tradeDetails?.trade_id} (${pick.tradeDetails?.trade_date}): ${pick.tradeDetails?.asset_name} transferred from ${pick.originalOwner} to ${pick.currentOwner}`}
                                >
                                  <div style={{ fontWeight: 'bold', color: '#ffcc80', fontSize: '12px' }}>
                                    {pick.currentOwner}
                                  </div>
                                  <div style={{ fontSize: '9px', color: '#ffab40', textDecoration: 'line-through' }}>
                                    via {pick.originalOwner}
                                  </div>
                                </div>
                              ) : (
                                <span style={{ color: '#aaa', fontSize: '12px' }}>
                                  {pick.currentOwner}
                                </span>
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeSubTab === 'ledgers' && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  Select Manager:
                </span>
                <select
                  value={selectedOwner}
                  onChange={e => setSelectedOwner(e.target.value)}
                  style={styles.filterControl}
                >
                  {DRAFT_OWNERS.map(o => (
                    <option key={o} value={o}>{o} ({ownerStats[o]?.total || 26} picks)</option>
                  ))}
                </select>
              </div>

              <span style={{ color: '#03dac6', fontSize: '12px', fontWeight: 'bold' }}>
                {selectedOwner} holds {ownerStats[selectedOwner]?.total} picks for 2027
              </span>
            </div>

            <table style={styles.table}>
              <thead style={styles.tableHead}>
                <tr>
                  <th style={styles.th}>Round</th>
                  <th style={styles.th}>Status</th>
                  <th style={styles.th}>Original Owner</th>
                  <th style={styles.th}>Trade Details & Notes</th>
                </tr>
              </thead>
              <tbody>
                {ownerStats[selectedOwner]?.ownedPicks.map((pick, idx) => {
                  const isAcquired = pick.originalOwner !== selectedOwner;
                  return (
                    <tr key={`ledger-${selectedOwner}-${idx}`} style={styles.tableRow}>
                      <td style={styles.td}>
                        <strong style={{ color: isAcquired ? '#ffb74d' : '#03dac6', fontSize: '13px' }}>
                          Round {pick.round}
                        </strong>
                      </td>
                      <td style={styles.td}>
                        <span style={{
                          padding: '3px 8px',
                          borderRadius: '4px',
                          fontSize: '10px',
                          fontWeight: 'bold',
                          background: isAcquired ? '#ffb74d25' : '#03dac620',
                          color: isAcquired ? '#ffb74d' : '#03dac6',
                          border: isAcquired ? '1px solid #ffb74d50' : '1px solid #03dac650'
                        }}>
                          {isAcquired ? '✓ ACQUIRED' : 'ORIGINAL'}
                        </span>
                      </td>
                      <td style={styles.td}>
                        <span style={{ color: isAcquired ? '#fff' : '#aaa' }}>
                          {pick.originalOwner}
                        </span>
                      </td>
                      <td style={styles.td}>
                        {isAcquired ? (
                          <span style={{ color: '#ffcc80', fontSize: '12px' }}>
                            Acquired from {pick.originalOwner} via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                            {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                          </span>
                        ) : (
                          <span style={{ color: '#666', fontSize: '12px' }}>Original round selection</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>

            {ownerStats[selectedOwner]?.tradedAway.length > 0 && (
              <div style={{ marginTop: '25px' }}>
                <div style={{ fontWeight: 'bold', color: '#ff5252', marginBottom: '10px', fontSize: '13px' }}>
                  Original Picks Traded Away by {selectedOwner} ({ownerStats[selectedOwner]?.tradedAway.length})
                </div>
                <table style={styles.table}>
                  <thead style={styles.tableHead}>
                    <tr>
                      <th style={styles.th}>Round</th>
                      <th style={styles.th}>Status</th>
                      <th style={styles.th}>New Owner</th>
                      <th style={styles.th}>Trade Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ownerStats[selectedOwner]?.tradedAway.map((pick, idx) => (
                      <tr key={`away-${idx}`} style={{ ...styles.tableRow, opacity: 0.85 }}>
                        <td style={styles.td}>
                          <strong style={{ color: '#ff5252', textDecoration: 'line-through' }}>
                            Round {pick.round}
                          </strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{
                            padding: '3px 8px',
                            borderRadius: '4px',
                            fontSize: '10px',
                            fontWeight: 'bold',
                            background: '#ff525220',
                            color: '#ff5252',
                            border: '1px solid #ff525240'
                          }}>
                            TRADED AWAY
                          </span>
                        </td>
                        <td style={styles.td}>
                          <strong style={{ color: '#fff' }}>{pick.currentOwner}</strong>
                        </td>
                        <td style={styles.td}>
                          <span style={{ color: '#ff8a80', fontSize: '12px' }}>
                            Traded to {pick.currentOwner} via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                            {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {activeSubTab === 'history' && (
          <div>
            <table style={styles.table}>
              <thead style={styles.tableHead}>
                <tr>
                  <th style={styles.th}>Date</th>
                  <th style={styles.th}>Trade #</th>
                  <th style={styles.th}>Sending Owner</th>
                  <th style={styles.th}>Receiving Owner</th>
                  <th style={styles.th}>Draft Asset Traded</th>
                  <th style={styles.th}>Associated ESPN Move / Details</th>
                </tr>
              </thead>
              <tbody>
                {assetTradesList.map((t, idx) => (
                  <tr key={`history-${idx}`} style={styles.tableRow}>
                    <td style={{ ...styles.td, fontFamily: 'monospace', color: '#aaa', fontSize: '11px' }}>
                      {t.trade_date}
                    </td>
                    <td style={styles.td}>
                      <span style={{
                        padding: '2px 6px',
                        background: '#333',
                        color: '#03dac6',
                        borderRadius: '4px',
                        fontWeight: 'bold',
                        fontSize: '11px'
                      }}>
                        #{t.trade_id}
                      </span>
                    </td>
                    <td style={{ ...styles.td, fontWeight: 'bold', color: '#ff8a80' }}>
                      {t.sending_owner}
                    </td>
                    <td style={{ ...styles.td, fontWeight: 'bold', color: '#69f0ae' }}>
                      {t.receiving_owner}
                    </td>
                    <td style={styles.td}>
                      <span style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: '4px',
                        padding: '3px 8px',
                        borderRadius: '4px',
                        background: '#ffb74d25',
                        color: '#ffb74d',
                        border: '1px solid #ffb74d60',
                        fontWeight: 'bold',
                        fontSize: '11px'
                      }}>
                        <span>🎟️</span>
                        <span>{t.asset_name}</span>
                        {t.round_num && <span style={{ fontSize: '10px', color: '#ffa726' }}>(Round {t.round_num})</span>}
                      </span>
                    </td>
                    <td style={{ ...styles.td, color: '#ccc', fontSize: '12px' }}>
                      {t.notes || `Trade #${t.trade_id}`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {activeSubTab === '2026board' && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <span style={{ fontSize: '11px', color: '#888', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  Filter 2026 Board:
                </span>
                {['ALL', 'KEEPERS', 'EARLY', 'MID', 'LATE'].map(rf => (
                  <button
                    key={rf}
                    onClick={() => setRoundFilter(rf)}
                    style={{
                      background: roundFilter === rf ? '#333' : '#1e1e1e',
                      color: roundFilter === rf ? '#00e676' : '#888',
                      border: roundFilter === rf ? '1px solid #00e676' : '1px solid #333',
                      borderRadius: '4px',
                      padding: '3px 8px',
                      fontSize: '11px',
                      fontWeight: 'bold',
                      cursor: 'pointer'
                    }}
                  >
                    {rf === 'ALL' ? 'All (R1-32)' : rf === 'KEEPERS' ? 'Keepers (R1-5)' : rf === 'EARLY' ? 'Early (R6-14)' : rf === 'MID' ? 'Mid (R15-23)' : 'Late (R24-32)'}
                  </button>
                ))}
              </div>

              <span style={{ fontSize: '11px', color: '#03dac6', fontStyle: 'italic' }}>
                * Rounds 1–5 are Keepers; Rounds 6–20 include purchased Comp Picks; Rounds 27–32 show Offset picks.
              </span>
            </div>

            <div style={{ overflowX: 'auto' }}>
              <table style={{ ...styles.table, minWidth: '950px' }}>
                <thead style={styles.tableHead}>
                  <tr>
                    <th style={{ ...styles.th, width: '100px' }}>Round</th>
                    {DRAFT_OWNERS.map(o => (
                      <th key={o} style={{ ...styles.th, textAlign: 'center' }}>
                        {o}
                      </th>
                    ))}
                    <th style={{ ...styles.th, textAlign: 'center', width: '160px', color: '#bb86fc' }}>
                      End-of-Round Comp Picks
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {Array.from({ length: 32 }, (_, i) => i + 1)
                    .filter(r => {
                      if (roundFilter === 'ALL') return true;
                      if (roundFilter === 'KEEPERS') return r <= 5;
                      if (roundFilter === 'EARLY') return r >= 6 && r <= 14;
                      if (roundFilter === 'MID') return r >= 15 && r <= 23;
                      if (roundFilter === 'LATE') return r >= 24;
                      return true;
                    })
                    .map(rNum => {
                      const isKeeperRound = rNum <= 5;
                      const compPicksThisRound = (compPicks || []).filter(cp => cp.round_num === rNum && cp.action_type === 'BOUGHT');

                      return (
                        <tr key={`2026-round-${rNum}`} style={styles.tableRow}>
                          <td style={{ ...styles.td, fontWeight: 'bold', color: isKeeperRound ? '#ffb74d' : '#03dac6', whiteSpace: 'nowrap' }}>
                            {isKeeperRound ? `💎 Round ${rNum} (Keeper)` : `Round ${rNum}`}
                          </td>

                          {DRAFT_OWNERS.map(owner => {
                            if (isKeeperRound) {
                              const keeper = (keepers || []).find(k => k.owner === owner && k.keeper_slot === rNum);
                              return (
                                <td key={owner} style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                                  {keeper ? (
                                    <div style={{
                                      background: 'rgba(255, 183, 77, 0.15)',
                                      border: '1px solid rgba(255, 183, 77, 0.4)',
                                      borderRadius: '6px',
                                      padding: '4px 6px',
                                      display: 'inline-block',
                                      minWidth: '85px'
                                    }}>
                                      <div style={{ fontWeight: 'bold', fontSize: '11px', color: '#fff' }}>
                                        {keeper.player_name}
                                      </div>
                                      <div style={{ fontSize: '9px', color: '#ffb74d' }}>
                                        ${keeper.cost} • Rank {keeper.rank || 'N/A'}
                                      </div>
                                    </div>
                                  ) : (
                                    <span style={{ color: '#666' }}>-</span>
                                  )}
                                </td>
                              );
                            }

                            // Standard draft rounds (6 to 32)
                            const isOffset = (compPicks || []).some(cp => cp.owner === owner && cp.round_num === rNum && cp.action_type === 'OFFSET_LOST');

                            return (
                              <td key={owner} style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                                {isOffset ? (
                                  <div style={{
                                    background: 'rgba(244, 67, 54, 0.15)',
                                    border: '1px solid rgba(244, 67, 54, 0.4)',
                                    borderRadius: '6px',
                                    padding: '4px 6px',
                                    display: 'inline-block',
                                    minWidth: '85px'
                                  }}>
                                    <div style={{ fontSize: '11px', fontWeight: 'bold', color: '#f44336', textDecoration: 'line-through' }}>
                                      Pick Slot
                                    </div>
                                    <div style={{ fontSize: '9px', color: '#ff8a80', fontWeight: 'bold' }}>
                                      🚫 Offset (Comp Pick)
                                    </div>
                                  </div>
                                ) : (
                                  <div style={{
                                    background: '#1a1a1a',
                                    border: '1px solid #333',
                                    borderRadius: '6px',
                                    padding: '4px 6px',
                                    display: 'inline-block',
                                    minWidth: '75px'
                                  }}>
                                    <span style={{ fontSize: '11px', color: '#aaa' }}>Standard Pick</span>
                                  </div>
                                )}
                              </td>
                            );
                          })}

                          {/* End-of-round compensation picks column */}
                          <td style={{ ...styles.td, textAlign: 'center', padding: '6px 4px' }}>
                            {compPicksThisRound.length > 0 ? (
                              <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
                                {compPicksThisRound.map(cp => (
                                  <span key={cp.id || `${cp.owner}-${cp.round_num}`} style={{
                                    background: 'rgba(187, 134, 252, 0.2)',
                                    border: '1px solid #bb86fc',
                                    borderRadius: '4px',
                                    padding: '2px 6px',
                                    fontSize: '10px',
                                    fontWeight: 'bold',
                                    color: '#bb86fc'
                                  }}>
                                    🎟️ {cp.owner} (${cp.cost_or_income})
                                  </span>
                                ))}
                              </div>
                            ) : (
                              <span style={{ color: '#555', fontSize: '11px' }}>-</span>
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
    </div>
  );
}

// --- ANALYSIS HISTORY PANEL ---
function AnalysisHistoryPanel({ analysisHistory }) {
  const [filterOwner, setFilterOwner] = useState('');
  const [searchText, setSearchText] = useState('');

  const filteredHistory = useMemo(() => {
    return analysisHistory.filter(item => {
      const ownerMatch = !filterOwner || item.owner === filterOwner;
      const textMatch = !searchText || 
        item.commentary.toLowerCase().includes(searchText.toLowerCase()) ||
        item.playerName.toLowerCase().includes(searchText.toLowerCase());
      return ownerMatch && textMatch;
    });
  }, [analysisHistory, filterOwner, searchText]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <span>Analysis History ({analysisHistory.length} picks analyzed)</span>
        <div style={{ display: 'flex', gap: '10px' }}>
          <select 
            value={filterOwner}
            onChange={e => setFilterOwner(e.target.value)}
            style={styles.filterControl}
          >
            <option value="">All Owners</option>
            {DRAFT_OWNERS.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
          <input 
            type="text"
            placeholder="Search commentary..."
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
            style={styles.filterControl}
          />
        </div>
      </div>
      
      <div style={styles.poolTableContainer}>
        {filteredHistory.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '40px', color: '#666' }}>
            {analysisHistory.length === 0 ? 
              'No analyses generated yet. Make draft picks to see Gemini AI commentary!' :
              'No analyses match your filters.'
            }
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
            {filteredHistory.map((item, idx) => (
              <div key={idx} style={{
                background: '#222',
                padding: '15px',
                borderRadius: '8px',
                borderLeft: '4px solid var(--accent)'
              }}>
                <div style={{ 
                  display: 'flex', 
                  justifyContent: 'space-between', 
                  marginBottom: '10px',
                  paddingBottom: '10px',
                  borderBottom: '1px solid #333'
                }}>
                  <div>
                    <span style={{ color: '#888', fontSize: '12px' }}>
                      Pick #{item.pickNumber} • Round {item.round}
                    </span>
                    <div style={{ marginTop: '5px' }}>
                      <span style={{ color: 'var(--highlight)', fontWeight: 'bold', fontSize: '16px' }}>
                        {item.owner}
                      </span>
                      <span style={{ color: '#888', margin: '0 8px' }}>→</span>
                      <span style={{ color: '#fff', fontWeight: 'bold', fontSize: '16px' }}>
                        {item.playerName}
                      </span>
                      <span style={{ color: '#888', marginLeft: '8px', fontSize: '12px' }}>
                        {item.position} • {item.team}
                      </span>
                    </div>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '6px' }}>
                    <div style={{ color: '#666', fontSize: '11px', textAlign: 'right' }}>
                      {item.timestamp}
                    </div>
                    {item.commentary && (
                      <button
                        onClick={() => playPodcastTTS(item.commentary)}
                        style={{
                          background: 'rgba(187, 134, 252, 0.15)',
                          border: '1px solid #bb86fc',
                          color: '#bb86fc',
                          borderRadius: '4px',
                          padding: '2px 8px',
                          fontSize: '11px',
                          cursor: 'pointer',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '4px'
                        }}
                        title="Play podcast-style spoken analysis"
                      >
                        🔊 Listen
                      </button>
                    )}
                  </div>
                </div>
                <div 
                  style={{ 
                    color: '#ccc', 
                    fontSize: '13px', 
                    lineHeight: '1.6',
                    fontStyle: 'italic'
                  }}
                  dangerouslySetInnerHTML={{ __html: item.commentary }}
                />
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// --- DRAFT LOG PANEL ---
function DraftLogPanel({ allPicks, players, onPlayerClick }) {
  const [filterOwner, setFilterOwner] = useState('');
  const [filterPosition, setFilterPosition] = useState('');

  const completedPicks = allPicks.filter(p => p['ESPN PlayerID']);

  const filteredPicks = useMemo(() => {
    return completedPicks.filter(pick => {
      const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
      if (!player) return false;
      
      const ownerMatch = !filterOwner || pick.Owner === filterOwner;
      const posMatch = !filterPosition || player.Position?.includes(filterPosition);
      
      return ownerMatch && posMatch;
    });
  }, [completedPicks, players, filterOwner, filterPosition]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>
        <span>Draft Log ({completedPicks.length} picks completed)</span>
        <div style={{ display: 'flex', gap: '10px' }}>
          <select 
            value={filterOwner}
            onChange={e => setFilterOwner(e.target.value)}
            style={styles.filterControl}
          >
            <option value="">All Owners</option>
            {DRAFT_OWNERS.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
          <select 
            value={filterPosition}
            onChange={e => setFilterPosition(e.target.value)}
            style={styles.filterControl}
          >
            <option value="">All Positions</option>
            <option value="C">C</option>
            <option value="1B">1B</option>
            <option value="2B">2B</option>
            <option value="3B">3B</option>
            <option value="SS">SS</option>
            <option value="OF">OF</option>
            <option value="SP">SP</option>
            <option value="RP">RP</option>
          </select>
        </div>
      </div>
      
      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th}>Pick #</th>
              <th style={styles.th}>Round</th>
              <th style={styles.th}>Owner</th>
              <th style={styles.th}>Player</th>
              <th style={styles.th}>Pos</th>
              <th style={styles.th}>Team</th>
              <th style={styles.th}>R</th>
              <th style={styles.th}>HR</th>
              <th style={styles.th}>RBI</th>
              <th style={styles.th}>SB</th>
              <th style={styles.th}>OBP</th>
              <th style={styles.th}>K</th>
              <th style={styles.th}>QS</th>
              <th style={styles.th}>ERA</th>
              <th style={styles.th}>WHIP</th>
              <th style={styles.th}>SV+H</th>
            </tr>
          </thead>
          <tbody>
            {filteredPicks.map(pick => {
              const player = players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
              if (!player) return null;
              
              const isPitcher = player.Position?.includes('SP') || player.Position?.includes('RP');
              
              return (
                <tr key={pick['Overall Pick']} style={styles.tableRow}>
                  <td style={styles.td}>
                    <strong style={{ color: 'var(--highlight)' }}>#{pick['Overall Pick']}</strong>
                  </td>
                  <td style={styles.td}>{pick.Round}</td>
                  <td style={styles.td}>
                    <strong style={{ color: '#fff' }}>{pick.Owner}</strong>
                  </td>
                  <td style={styles.td}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <div style={{
                        width: '26px',
                        height: '26px',
                        borderRadius: '50%',
                        overflow: 'hidden',
                        background: '#222',
                        flexShrink: 0,
                        border: '1px solid #444'
                      }}>
                        <img
                          src={getPlayerHeadshotUrl(player)}
                          alt=""
                          style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                          onError={(e) => handleHeadshotError(e, player)}
                          referrerPolicy="no-referrer"
                          loading="lazy"
                        />
                      </div>
                      <PlayerNameButton 
                        player={player} 
                        onClick={onPlayerClick}
                        style={{ color: '#fff', fontWeight: 'bold' }}
                      />
                    </div>
                  </td>
                  <td style={styles.td}>{player.Position}</td>
                  <td style={styles.td}>{player.Team}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSR || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSHR || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSRBI || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSSB || '-')}</td>
                  <td style={styles.td}>{isPitcher ? '-' : (player.ZIPSOBP || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSK || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSQS || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSERA || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player.ZIPSWHIP || '-')}</td>
                  <td style={styles.td}>{!isPitcher ? '-' : (player['ZIPSSV+HDs'] || '-')}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- STANDINGS PANEL ---
function StandingsPanel({ allPicks, players }) {
  const standings = useMemo(() => {
    return DRAFT_OWNERS.map(owner => {
      const ownerPicks = allPicks.filter(p => p.Owner === owner && p['ESPN PlayerID']);
      const ownerPlayers = ownerPicks.map(pick => 
        players.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']))
      ).filter(Boolean);

      const batters = ownerPlayers.filter(p => !p.Position?.includes('SP') && !p.Position?.includes('RP'));
      const pitchers = ownerPlayers.filter(p => p.Position?.includes('SP') || p.Position?.includes('RP'));

      return {
        owner,
        picksCount: ownerPicks.length,
        r: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSR) || 0), 0),
        hr: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSHR) || 0), 0),
        rbi: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSRBI) || 0), 0),
        sb: batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSSB) || 0), 0),
        obp: batters.length > 0 ? 
          batters.reduce((sum, p) => sum + (parseFloat(p.ZIPSOBP) || 0), 0) / batters.length : 0,
        k: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSK) || 0), 0),
        qs: pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSQS) || 0), 0),
        era: pitchers.length > 0 ?
          pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSERA) || 0), 0) / pitchers.length : 0,
        whip: pitchers.length > 0 ?
          pitchers.reduce((sum, p) => sum + (parseFloat(p.ZIPSWHIP) || 0), 0) / pitchers.length : 0,
        sv: pitchers.reduce((sum, p) => sum + (parseFloat(p['ZIPSSV+HDs']) || 0), 0)
      };
    });
  }, [allPicks, players]);

  return (
    <div style={{ ...styles.wrColumn, width: '100%', gridColumn: '1 / -1' }}>
      <div style={styles.wrHeader}>Projected Standings from Drafted Rosters</div>
      <div style={styles.poolTableContainer}>
        <table style={styles.table}>
          <thead style={styles.tableHead}>
            <tr>
              <th style={styles.th}>Owner</th>
              <th style={styles.th}>Picks</th>
              <th style={styles.th}>Proj R</th>
              <th style={styles.th}>Proj HR</th>
              <th style={styles.th}>Proj RBI</th>
              <th style={styles.th}>Proj SB</th>
              <th style={styles.th}>Proj OBP</th>
              <th style={styles.th}>Proj K</th>
              <th style={styles.th}>Proj QS</th>
              <th style={styles.th}>Proj ERA</th>
              <th style={styles.th}>Proj WHIP</th>
              <th style={styles.th}>Proj SV+H</th>
            </tr>
          </thead>
          <tbody>
            {standings.map(s => (
              <tr key={s.owner} style={styles.tableRow}>
                <td style={{ ...styles.td, fontWeight: 'bold', color: 'var(--highlight)' }}>{s.owner}</td>
                <td style={styles.td}>{s.picksCount}</td>
                <td style={styles.td}>{Math.round(s.r)}</td>
                <td style={styles.td}>{Math.round(s.hr)}</td>
                <td style={styles.td}>{Math.round(s.rbi)}</td>
                <td style={styles.td}>{Math.round(s.sb)}</td>
                <td style={styles.td}>{s.obp.toFixed(3)}</td>
                <td style={styles.td}>{Math.round(s.k)}</td>
                <td style={styles.td}>{Math.round(s.qs)}</td>
                <td style={styles.td}>{s.era > 0 ? s.era.toFixed(2) : '-'}</td>
                <td style={styles.td}>{s.whip > 0 ? s.whip.toFixed(3) : '-'}</td>
                <td style={styles.td}>{Math.round(s.sv)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// --- MAIN DRAFT ROOM VIEW ---
export default function DraftRoomView({ 
  onOpenPlayerModal, 
  onSwitchView, 
  seasonYear = 2027, 
  onSeasonYearChange 
}) {
  const [roomSeason, setRoomSeason] = useState(seasonYear || 2027);
  const [draftMode, setDraftMode] = useState(() => localStorage.getItem('draftMode') || null);
  const [currentUser, setCurrentUser] = useState(() => localStorage.getItem('draftUser') || null);
  const [players, setPlayers] = useState([]);
  const [picks, setPicks] = useState([]);
  const [queue, setQueue] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem('draft_queue') || '[]');
    } catch {
      return [];
    }
  });
  const [pickStartTime, setPickStartTime] = useState(() => Date.now());
  const [activeTab, setActiveTab] = useState('Pool');
  const [draftTrades, setDraftTrades] = useState(defaultDraftAssetTrades || []);
  const [_teamBudgets, setTeamBudgets] = useState(defaultTeamBudgets?.budgets || []);
  const [_compPicks, setCompPicks] = useState(defaultCompPicks?.comp_picks || []);
  const [_keepers, setKeepers] = useState(defaultKeepers?.keepers || []);
  const [showDashboard, setShowDashboard] = useState(false);
  
  // Test mode state
  const [testModePicks, setTestModePicks] = useState([]);
  
  // AI Commentary state
  const [lastPickCommentary, setLastPickCommentary] = useState("Draft has not started.");
  const [generatingCommentary, setGeneratingCommentary] = useState(false);
  
  // Analysis history storage
  const [analysisHistory, setAnalysisHistory] = useState([]);
  
  // Player modal state
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [playerInfo, setPlayerInfo] = useState([]);

  // Static and merged draft data states
  const [_endingRoster, setEndingRoster] = useState([]);
  const [draftHistory, setDraftHistory] = useState([]);
  const [historicalFinish, setHistoricalFinish] = useState([]);
  const [battersZips, setBattersZips] = useState([]);
  const [pitchersZips, setPitchersZips] = useState([]);
  const [savantBatters, setSavantBatters] = useState([]);
  const [savantPitchers, setSavantPitchers] = useState([]);
  const [_espnPlayers, setEspnPlayers] = useState([]);

  // Audio state
  const [audioEnabled, setAudioEnabled] = useState(false);
  const audioEnabledRef = useRef(audioEnabled);
  useEffect(() => {
    audioEnabledRef.current = audioEnabled;
  }, [audioEnabled]);
  const lastAnnouncedPickRef = useRef(null);

  const [isRunningMock, setIsRunningMock] = useState(false);
  const [mockSpeed, setMockSpeed] = useState(1000);
  const [resetting, setResetting] = useState(false);

  // Sync roomSeason when parent seasonYear prop changes
  useEffect(() => {
    if (seasonYear && seasonYear !== roomSeason) {
      setRoomSeason(seasonYear);
      setTestModePicks([]);
      setAnalysisHistory([]);
      setLastPickCommentary(seasonYear === 2026 ? "Viewing 2026 completed draft archive." : "Draft has not started.");
    }
  }, [seasonYear, roomSeason]);

  const handleSeasonChange = (newYear) => {
    setRoomSeason(newYear);
    setTestModePicks([]);
    setAnalysisHistory([]);
    setLastPickCommentary(newYear === 2026 ? "Viewing 2026 completed draft archive." : "Draft has not started.");
    if (onSeasonYearChange) onSeasonYearChange(newYear);
  };

  const displayPicks = useMemo(() => {
    return (draftMode === 'test' || draftMode === 'mockdraft') && testModePicks.length > 0 ? testModePicks : picks;
  }, [draftMode, testModePicks, picks]);

  const displayPlayers = useMemo(() => {
    if (!players || players.length === 0) return [];
    if (roomSeason === 2027) {
      // For 2027 draft, the ONLY players that should show as currently rostered
      // are the presumed 2027 keeper picks. All other players must show as Available.
      const keeperMap = new Map();
      (_keepers || []).forEach(k => {
        const rawOwner = k.owner || k.owner_name || k.team_owner || '';
        const owner = (rawOwner === 'Dan' || rawOwner === 'dsellinger') ? 'Daniel' : rawOwner;
        const pid = String(k.espn_player_id || k.player_id || '');
        if (pid) keeperMap.set(pid, { owner, slot: k.keeper_slot });
      });

      return players.map(p => {
        const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '');
        const kInfo = keeperMap.get(pid);
        return {
          ...p,
          Availability: kInfo ? kInfo.owner : 'Available',
          isKeeper: !!kInfo,
          keeperOwner: kInfo ? kInfo.owner : null,
          keeperSlot: kInfo ? kInfo.slot : null
        };
      });
    }
    return players;
  }, [players, roomSeason, _keepers]);

  const playersRef = useRef(displayPlayers);
  useEffect(() => {
    playersRef.current = displayPlayers;
    updateGlobalPlayerLookup(displayPlayers);
  }, [displayPlayers]);

  const picksRef = useRef(displayPicks);
  useEffect(() => {
    picksRef.current = displayPicks;
  }, [displayPicks]);

  const currentPick = useMemo(() => {
    if (roomSeason === 2027) {
      // In 2027 draft prep, live drafting begins at pick 55 (picks 1-54 are 6 keeper rounds)
      return displayPicks.find(p => p['Overall Pick'] >= 55 && !p['ESPN PlayerID']);
    }
    return displayPicks.find(p => !p['ESPN PlayerID']);
  }, [displayPicks, roomSeason]);

  const currentPickId = currentPick?.['Overall Pick'];
  const currentPickOwner = currentPick?.Owner;

  // Sound effect when current pick changes
  useEffect(() => {
    if (currentPickOwner && audioEnabled && currentPickId) {
      if (currentPickId !== lastAnnouncedPickRef.current) {
        lastAnnouncedPickRef.current = currentPickId;
        playOwnerSound(currentPickOwner);
      }
    }
  }, [currentPickId, currentPickOwner, audioEnabled]);

  // Commentary callback - stable reference using playersRef
  const handleNewPick = useCallback(async (pick) => {
    const player = playersRef.current.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
    if (!player) return;

    // If pick already comes with commentary, display immediately without regenerating
    if (pick.ai_commentary) {
      setLastPickCommentary(pick.ai_commentary);
      const historyEntry = {
        pickNumber: pick['Overall Pick'],
        round: pick.Round,
        owner: pick.Owner,
        playerName: player.Player,
        position: player.Position,
        team: player.Team,
        commentary: pick.ai_commentary,
        timestamp: new Date().toLocaleTimeString()
      };
      setAnalysisHistory(prev => {
        if (prev.some(h => h.pickNumber === historyEntry.pickNumber)) return prev;
        return [...prev, historyEntry];
      });
      return;
    }
    
    setGeneratingCommentary(true);

    const currentPicks = picksRef.current || [];
    const currentPlayers = playersRef.current || [];

    // Find all previous picks by this owner
    const ownerPriorPicks = currentPicks.filter(p => 
      p.Owner === pick.Owner && 
      p['ESPN PlayerID'] && 
      p['Overall Pick'] !== pick['Overall Pick'] &&
      p['Overall Pick'] < (pick['Overall Pick'] || 999)
    );

    const posCounts = {};
    const recentPicks = [];
    ownerPriorPicks.forEach(p => {
      const pl = currentPlayers.find(x => String(x['ESPN PlayerID']) === String(p['ESPN PlayerID']));
      const pos = pl?.Position || p.Position || 'UTIL';
      const mainPos = pos.split(/[/,]/)[0].trim();
      posCounts[mainPos] = (posCounts[mainPos] || 0) + 1;
      const name = pl?.Player || p.Player;
      if (name) {
        recentPicks.push(`${name} (R${p.Round || '?'})`);
      }
    });

    const teamContext = {
      rosterCount: ownerPriorPicks.length,
      positionCounts: posCounts,
      recentPicks: recentPicks.slice(-3),
      totalTeamPicks: ownerPriorPicks.length + 1
    };
    
    const commentary = await generateDraftCommentary(
      player,
      pick.Owner,
      pick['Overall Pick'],
      teamContext,
      roomSeason
    );
    
    setLastPickCommentary(commentary);
    setGeneratingCommentary(false);

    // Podcast-style TTS voice analysis plays after the pick if audio is enabled
    if (audioEnabledRef.current) {
      setTimeout(() => {
        playPodcastTTS(commentary);
      }, 700);
    }
    
    const historyEntry = {
      pickNumber: pick['Overall Pick'],
      round: pick.Round,
      owner: pick.Owner,
      playerName: player.Player,
      position: player.Position,
      team: player.Team,
      commentary: commentary,
      timestamp: new Date().toLocaleTimeString()
    };
    setAnalysisHistory(prev => {
      if (prev.some(h => h.pickNumber === historyEntry.pickNumber)) return prev;
      return [...prev, historyEntry];
    });

    // Persist to Supabase draft_picks if in 2027 live draft
    if (roomSeason === 2027 && pick['Overall Pick']) {
      try {
        await supabase
          .from('draft_picks')
          .update({ ai_commentary: commentary })
          .eq('season_year', 2027)
          .eq('overall_pick', pick['Overall Pick']);
      } catch (err) {
        console.warn('Could not persist AI commentary to draft_picks:', err);
      }
    }
  }, [roomSeason]);

  const fetchDraftOrder = useCallback(async () => {
    try {
      if (roomSeason === 2027) {
        let trades = draftTrades;
        let keepers2027 = [];
        let compPicks2027 = [];
        let livePicks2027 = [];

        try {
          const [tradesRes, keepersRes, cpRes, liveRes] = await Promise.all([
            supabase.from('draft_asset_trades').select('*').order('trade_date', { ascending: true }),
            supabase.from('draft_keepers').select('*').eq('season_year', 2027).order('keeper_slot', { ascending: true }),
            supabase.from('draft_compensation_picks').select('*').eq('season_year', 2027).order('round_num', { ascending: true }),
            supabase.from('draft_picks').select('*').eq('season_year', 2027).order('overall_pick', { ascending: true })
          ]);
          if (tradesRes?.data) {
            trades = tradesRes.data;
            setDraftTrades(tradesRes.data);
          }
          if (keepersRes?.data) {
            keepers2027 = keepersRes.data;
            setKeepers(keepersRes.data);
          }
          if (cpRes?.data) {
            compPicks2027 = cpRes.data;
            setCompPicks(cpRes.data);
          }
          if (liveRes?.data) {
            livePicks2027 = liveRes.data;
            const existingComments = liveRes.data
              .filter(lp => lp.ai_commentary)
              .map(lp => ({
                pickNumber: lp.overall_pick,
                round: lp.round,
                owner: lp.team_owner,
                playerName: lp.player_name,
                position: lp.player_position || '',
                team: lp.player_team || '',
                commentary: lp.ai_commentary,
                timestamp: lp.picked_at ? new Date(lp.picked_at).toLocaleTimeString() : ''
              }));
            if (existingComments.length > 0) {
              setAnalysisHistory(prev => {
                const map = new Map(prev.map(item => [item.pickNumber, item]));
                existingComments.forEach(item => {
                  if (!map.has(item.pickNumber)) map.set(item.pickNumber, item);
                });
                return Array.from(map.values()).sort((a, b) => a.pickNumber - b.pickNumber);
              });
              setLastPickCommentary(existingComments[existingComments.length - 1].commentary);
            }
          }
        } catch (e) {
          console.warn('Supabase 2027 data fetch notice:', e);
        }

        const generated = generate2027DraftOrder(trades, keepers2027, compPicks2027);

        if (livePicks2027.length > 0) {
          const liveMap = new Map(livePicks2027.map(lp => [lp.overall_pick, lp]));
          const merged = generated.map(p => {
            const lp = liveMap.get(p['Overall Pick']);
            if (lp) {
              return {
                ...p,
                'ESPN PlayerID': lp.player_id,
                Selection: lp.player_name,
                ai_commentary: lp.ai_commentary
              };
            }
            return p;
          });
          setPicks(merged);
        } else {
          setPicks(generated);
        }
      } else {
        let loadedPicks = null;
        try {
          const { data, error } = await supabase
            .from('draft-order')
            .select('*')
            .order('Overall Pick', { ascending: true });
          if (!error && data && data.length > 0) {
            loadedPicks = data;
          }
        } catch (e) {
          console.warn('Supabase draft-order fetch error:', e);
        }

        if (loadedPicks && loadedPicks.length > 0) {
          setPicks(loadedPicks);
        } else if (roomSeason === 2026 && defaultDraft2026?.length > 0) {
          setPicks(defaultDraft2026.map(p => ({
            'Overall Pick': p.overall_pick,
            Round: p.round,
            Pick: p.pick,
            Owner: p.team_owner,
            'ESPN PlayerID': p.player_id,
            Selection: p.player_name,
            isKeeper: Boolean(p.is_keeper),
            is_keeper: Boolean(p.is_keeper)
          })));
        } else {
          setPicks(generateDefaultDraftOrder());
        }

        try {
          const [dtData, budgetsRes, cpRes, keepersRes] = await Promise.all([
            supabase.from('draft_asset_trades').select('*').order('trade_date', { ascending: true }),
            supabase.from('draft_team_budgets').select('*').order('finish_rank', { ascending: true }),
            supabase.from('draft_compensation_picks').select('*').order('round_num', { ascending: true }),
            supabase.from('draft_keepers').select('*').order('keeper_slot', { ascending: true })
          ]);
          if (dtData?.data && dtData.data.length > 0) setDraftTrades(dtData.data);
          if (budgetsRes?.data && budgetsRes.data.length > 0) setTeamBudgets(budgetsRes.data);
          if (cpRes?.data && cpRes.data.length > 0) setCompPicks(cpRes.data);
          if (keepersRes?.data && keepersRes.data.length > 0) setKeepers(keepersRes.data);
        } catch (e) {
          console.warn('Supabase supporting data fetch notice:', e);
        }
      }
    } catch (err) {
      console.warn('Draft order fetch error, fallback:', err);
      if (roomSeason === 2027) {
        setPicks(generate2027DraftOrder(draftTrades, [], []));
      } else if (roomSeason === 2026 && defaultDraft2026?.length > 0) {
        setPicks(defaultDraft2026.map(p => ({
          'Overall Pick': p.overall_pick,
          Round: p.round,
          Pick: p.pick,
          Owner: p.team_owner,
          'ESPN PlayerID': p.player_id,
          Selection: p.player_name,
          isKeeper: Boolean(p.is_keeper),
          is_keeper: Boolean(p.is_keeper)
        })));
      } else {
        setPicks(generateDefaultDraftOrder());
      }
    }
  }, [roomSeason, draftTrades]);

  const fetchStaticData = useCallback(async () => {
    console.log(`📦 Fetching static data...`);
    try {
      let pool = [];
      let ending = [];
      let history = [];
      let finishes = [];

      try {
        const [poolRes, endingRes, dhRes, hfRes, settingsRes] = await Promise.all([
          supabase.from('player-pool').select('*'),
          supabase.from('ending-roster').select('*'),
          supabase.from('draft_picks').select('*').order('season_year', { ascending: false }).order('overall_pick', { ascending: true }),
          supabase.from('historical_finishes').select('*').order('season_year', { ascending: false }),
          supabase.from('league_settings').select('*').limit(1)
        ]);

        pool = poolRes?.data || [];
        ending = endingRes?.data || [];
        if (dhRes?.data && dhRes.data.length > 0) {
          history = dhRes.data.map(p => ({
            Year: String(p.season_year),
            Round: String(p.round || ''),
            Pick_Overall: String(p.overall_pick),
            Player_Name: p.player_name,
            Team_ID: p.team_owner,
            Owner: p.team_owner,
            player_id: String(p.player_id || ''),
            Keeper: p.is_keeper ? 'True' : 'False'
          }));
        }
        if (hfRes?.data && hfRes.data.length > 0) {
          finishes = hfRes.data.map(f => ({
            Year: String(f.season_year),
            Owner: f.team_owner,
            "Final Rank": String(f.final_place || ''),
            Points: String(f.total_roto_points || ''),
            "Active Owner?": f.is_active_owner ? 'Y' : 'N',
            ...(f.category_ranks || {})
          }));
        }
        if (settingsRes?.data?.[0]?.current_season) {
          console.log(`⚾ Active League Season: ${settingsRes.data[0].current_season}`);
        }
      } catch (dbErr) {
        console.warn('Supabase warehouse query notice:', dbErr);
      }

      if (!pool.length) {
        pool = await fetchFromGCS('player-pool.json', 'gcs_player_pool') || [];
      }
      if (!history.length) {
        history = await fetchFromGCS('draft-history.json', 'gcs_draft_history_v2') || [];
      }
      const has2026 = history.some(d => String(d.Year || d.year) === '2026');
      if (!has2026 && mappedDraft2026History.length > 0) {
        history = [...mappedDraft2026History, ...history];
      }
      if (!finishes.length) {
        finishes = await fetchFromGCS('historical-finish.json', 'gcs_league_history') || [];
      }

      console.log(`✅ Loaded ${ending.length} ending roster entries`);
      console.log(`📦 Core data loaded: ${pool.length} players, ${history.length} draft history records, ${finishes.length} historical finish records`);

      const [battingZipsData, pitchingZipsData] = await Promise.all([
        fetchFanGraphs('batting'),
        fetchFanGraphs('pitching')
      ]);

      let savantBat = [];
      let savantPitch = [];
      const savantResults = await Promise.allSettled([
        fetchFromGCS('savant-batting.json', 'gcs_bat_savant_2025'),
        fetchFromGCS('savant-pitching.json', 'gcs_pitch_savant_2025')
      ]);
      if (savantResults[0].status === 'fulfilled') savantBat = savantResults[0].value || [];
      if (savantResults[1].status === 'fulfilled') savantPitch = savantResults[1].value || [];

      let espn = [];
      try {
        const cached = await draftDbGet('espn_player_info');
        if (cached) {
          const { data, timestamp } = cached;
          if (Date.now() - timestamp < 3600000) {
            espn = data;
            console.log(`📺 Using cached ESPN data`);
          }
        }
        if (espn.length === 0) {
          console.log(`📺 Fetching fresh ESPN data...`);
          espn = await fetchAllEspnPlayers();
          if (espn.length > 0) {
            await draftDbSet('espn_player_info', {
              data: espn,
              timestamp: Date.now()
            });
          }
        }
      } catch (err) {
        console.error('ESPN fetch error:', err);
      }

      let mergedPlayers = pool;
      if (espn.length > 0) {
        mergedPlayers = mergeEspnData(pool, espn);
      }
      if (battingZipsData.length > 0 || pitchingZipsData.length > 0) {
        console.log(`📊 Starting ZiPS merge: ${battingZipsData.length} batters, ${pitchingZipsData.length} pitchers`);
        mergedPlayers = mergeZipsData(mergedPlayers, battingZipsData, pitchingZipsData);
      }

      const mapWithMlbam = list => Array.isArray(list) ? list.map(item => ({
        ...item,
        MLBAMID: item.mlbamid || item.MLBAMID || item.playerid
      })) : [];

      setPlayers(mergedPlayers);
      setEndingRoster(ending);
      setDraftHistory(history);
      setHistoricalFinish(finishes);
      setBattersZips(mapWithMlbam(battingZipsData));
      setPitchersZips(mapWithMlbam(pitchingZipsData));
      setSavantBatters(savantBat);
      setSavantPitchers(savantPitch);
      if (espn.length > 0) {
        setPlayerInfo(espn);
        setEspnPlayers(espn);
      }
      console.log(`✅ All data loaded. Batters: ${battingZipsData.length}, Pitchers: ${pitchingZipsData.length}`);
    } catch (err) {
      console.error(`❌ Error fetching static data:`, err);
    }
  }, []);

  useEffect(() => {
    if (!draftMode) return;

    fetchStaticData();

    if (draftMode === 'live' || draftMode === 'multitest' || draftMode === 'mobile' || draftMode === 'host') {
      console.log(`🟢 Starting Polling for ${draftMode} mode (${roomSeason}) (Every 2s)...`);
      const poll = async () => {
        try {
          if (roomSeason === 2027) {
            const { data, error } = await supabase
              .from('draft_picks')
              .select('*')
              .eq('season_year', 2027)
              .order('overall_pick', { ascending: true });
            if (error) throw error;
            if (data) {
              setPicks(curr => {
                const liveMap = new Map(data.map(lp => [lp.overall_pick, lp]));
                const updated = curr.map(p => {
                  const lp = liveMap.get(p['Overall Pick']);
                  if (lp && !p['ESPN PlayerID']) {
                    return { ...p, 'ESPN PlayerID': lp.player_id, Selection: lp.player_name };
                  }
                  return p;
                });
                const currCount = curr.filter(p => p['ESPN PlayerID']).length;
                const newCount = updated.filter(p => p['ESPN PlayerID']).length;
                if (newCount > currCount) {
                  const latestNewPick = updated.filter(p => p['ESPN PlayerID']).slice(-1)[0];
                  if (latestNewPick) handleNewPick(latestNewPick);
                  return updated;
                }
                return curr;
              });
            }
          } else {
            const { data, error } = await supabase
              .from('draft-order')
              .select('*')
              .order('Overall Pick', { ascending: true });
            if (error) throw error;
            if (data && data.length > 0) {
              setPicks(curr => {
                const currPicked = curr.filter(p => p['ESPN PlayerID']).length;
                const newPicked = data.filter(p => p['ESPN PlayerID']).length;
                if (currPicked !== newPicked) {
                  const latestNewPick = data.filter(p => p['ESPN PlayerID']).slice(-1)[0];
                  if (latestNewPick) {
                    handleNewPick(latestNewPick);
                  }
                  return data;
                }
                return curr;
              });
            }
          }
        } catch (err) {
          console.error(`🔴 Polling Error:`, err.message);
        }
      };
      poll();
      const interval = setInterval(poll, 2000);
      return () => {
        console.log(`🛑 Stopping Polling`);
        clearInterval(interval);
      };
    } else if (draftMode === 'test' || draftMode === 'mockdraft') {
      fetchDraftOrder();
    }
  }, [draftMode, roomSeason, fetchStaticData, fetchDraftOrder, handleNewPick]);

  useEffect(() => {
    if (picks.length > 0) {
      const liveCutoff = roomSeason === 2027 ? 55 : 46;
      if (draftMode === 'test' && testModePicks.length === 0) {
        setTestModePicks(picks.map(p => p['Overall Pick'] >= liveCutoff ? {
          ...p,
          'ESPN PlayerID': null,
          Selection: null
        } : { ...p }));
      } else if (draftMode === 'mockdraft' && testModePicks.length === 0) {
        setTestModePicks(picks.map(p => p['Overall Pick'] >= liveCutoff ? {
          ...p,
          'ESPN PlayerID': null,
          Selection: null
        } : { ...p }));
      }
    }
  }, [draftMode, picks, testModePicks.length, roomSeason]);

  const handleModeSelect = (mode) => {
    localStorage.setItem('draftMode', mode);
    setDraftMode(mode);
    const liveCutoff = roomSeason === 2027 ? 55 : 46;
    if ((mode === 'test' || mode === 'mockdraft') && testModePicks.length === 0) {
      setTestModePicks(picks.map(p => p['Overall Pick'] >= liveCutoff ? {
        ...p,
        'ESPN PlayerID': null,
        Selection: null
      } : { ...p }));
    }
  };

  const handleLogin = (user) => {
    localStorage.setItem('draftUser', user);
    setCurrentUser(user);
  };

  const handleResetMode = () => {
    localStorage.removeItem('draftMode');
    setDraftMode(null);
  };

  const handleResetUser = () => {
    localStorage.removeItem('draftUser');
    setCurrentUser(null);
  };

  const stepMockPick = useCallback(() => {
    const current = currentPick;
    if (!current || current.Owner === currentUser) {
      setIsRunningMock(false);
      return;
    }

    const takenIds = new Set(displayPicks.map(p => String(p['ESPN PlayerID'])).filter(Boolean));
    const available = playersRef.current.filter(p => !takenIds.has(String(p['ESPN PlayerID'])));
    if (!available.length) return;

    const ownerRoster = displayPicks
      .filter(p => p.Owner === current.Owner && p['ESPN PlayerID'])
      .map(p => playersRef.current.find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID'])))
      .filter(Boolean);

    const pickNum = current['Overall Pick'];
    const windowOffset = pickNum <= 65 ? 20 : pickNum <= 120 ? 25 : pickNum <= 180 ? 35 : 60;

    const candidates = available.filter(p => {
      const adp = parseFloat(p.ADP);
      return isNaN(adp) || adp >= 900 ? pickNum > 150 : adp <= pickNum + windowOffset;
    });

    const poolToScore = candidates.length > 0 ? candidates : available.slice(0, 50);

    const slotLimits = { C: 1, '1B': 1, '2B': 1, '3B': 1, SS: 1, OF: 6, DH: 1, SP: 4, RP: 2 };
    const currentCounts = {};
    ownerRoster.forEach(rp => {
      Object.keys(slotLimits).forEach(slot => {
        if ((rp.Position || '').includes(slot)) {
          currentCounts[slot] = (currentCounts[slot] || 0) + 1;
        }
      });
    });

    const scored = poolToScore.map(player => {
      const pos = player.Position || '';
      const isSP = pos.includes('SP');
      const isRP = pos.includes('RP');
      const isP = isSP || isRP;

      const pr = parseFloat(player['Projected PR']) || 0;
      const valScore = Math.max(0, Math.min(100, (pr + 3) / 6 * 100));

      let needScore = 40;
      Object.entries(slotLimits).forEach(([slot, limit]) => {
        if (!pos.includes(slot)) return;
        const count = currentCounts[slot] || 0;
        const ratio = count / limit;
        const s = ratio === 0 ? 100 : ratio < 0.5 ? 85 : ratio < 1 ? 65 : ratio < 1.5 ? 35 : 10;
        needScore = Math.max(needScore, s);
      });

      const profile = DEFAULT_OWNER_PROFILES[current.Owner];
      let ownerScore = 50;
      if (profile?.tendencies) {
        const stage = pickNum <= 60 ? 'early' : pickNum <= 120 ? 'mid' : 'late';
        const pitcherRate = profile.tendencies.positionPreferences?.[stage]?.pitcherRate ?? 0.3;
        if (isP) {
          ownerScore = pitcherRate > 0.45 ? 75 : pitcherRate > 0.35 ? 62 : pitcherRate < 0.15 ? 30 : 38;
        } else {
          const hitRate = 1 - pitcherRate;
          ownerScore = hitRate > 0.8 ? 75 : hitRate > 0.65 ? 62 : hitRate < 0.5 ? 38 : 50;
        }

        const arch = profile.archetype?.name || '';
        if (arch === 'Ace Hunter' && isSP && pickNum <= 60) ownerScore = Math.min(95, ownerScore + 15);
        if (arch === 'Pitching Hoarder' && isP) ownerScore = Math.min(85, ownerScore + 10);
        if (arch === 'Value Hunter' && valScore > 70) ownerScore = Math.min(80, ownerScore + 10);
        if (arch === 'Bold Gambler' && (parseFloat(player.ADP) || 300) < pickNum - 15) ownerScore = Math.min(80, ownerScore + 12);
      }

      const totalScore = (valScore * 0.4 + needScore * 0.35 + ownerScore * 0.25) * (0.92 + Math.random() * 0.16);
      return { player, score: totalScore };
    }).sort((a, b) => b.score - a.score);

    const chosen = scored[0]?.player;
    if (!chosen) return;

    setPickStartTime(Date.now());
    setTestModePicks(prev => {
      const base = prev.length > 0 ? prev : displayPicks;
      return base.map(p => 
        p['Overall Pick'] === current['Overall Pick'] 
          ? { ...p, 'ESPN PlayerID': chosen['ESPN PlayerID'], Selection: chosen.Player }
          : p
      );
    });

    handleNewPick({
      ...current,
      'ESPN PlayerID': chosen['ESPN PlayerID'],
      Round: current.Round
    });
  }, [currentPick, displayPicks, currentUser, handleNewPick]);

  useEffect(() => {
    if (draftMode !== 'mockdraft' || !isRunningMock) return;

    if (!currentPick || currentPick.Owner === currentUser) {
      setIsRunningMock(false);
      return;
    }

    const timer = setTimeout(() => {
      stepMockPick();
    }, mockSpeed);

    return () => clearTimeout(timer);
  }, [draftMode, isRunningMock, mockSpeed, currentPick, currentUser, stepMockPick]);

  const handleResetDraft = async () => {
    const liveCutoff = roomSeason === 2027 ? 55 : 46;
    if (!window.confirm(`Are you sure you want to reset all ${roomSeason} draft picks? This will clear selections from pick ${liveCutoff} onwards.`)) return;
    setResetting(true);
    try {
      if (roomSeason === 2027) {
        const { error } = await supabase
          .from('draft_picks')
          .delete()
          .eq('season_year', 2027)
          .gte('overall_pick', liveCutoff);
        if (error) throw error;
      } else {
        const { error } = await supabase
          .from('draft-order')
          .update({ 'ESPN PlayerID': null, 'Selection': null })
          .gte('Overall Pick', liveCutoff);
        if (error) throw error;
      }
      setTestModePicks(prev => prev.map(p => p['Overall Pick'] >= liveCutoff ? { ...p, 'ESPN PlayerID': null, Selection: null } : p));
      setPicks(prev => prev.map(p => p['Overall Pick'] >= liveCutoff ? { ...p, 'ESPN PlayerID': null, Selection: null } : p));
      setLastPickCommentary(roomSeason === 2026 ? "Viewing 2026 completed draft archive." : "Draft has not started.");
      setAnalysisHistory([]);
      alert("Draft reset successfully!");
    } catch (e) {
      console.error("Reset error:", e);
      alert("Reset error: " + e.message);
    } finally {
      setResetting(false);
    }
  };

  const lastPick = displayPicks.filter(p => p['ESPN PlayerID']).slice(-1)[0];
  const lastPickPlayer = lastPick ? displayPlayers.find(p => String(p['ESPN PlayerID']) === String(lastPick['ESPN PlayerID'])) : null;
  
  const isMyTurn = (currentPick && currentUser && currentPick.Owner === currentUser) || draftMode === 'test' || draftMode === 'multitest';

  const upcomingPicks = useMemo(() => {
    const liveCutoff = roomSeason === 2027 ? 55 : 46;
    const currentIdx = displayPicks.findIndex(p => 
      roomSeason === 2027
        ? (p['Overall Pick'] >= liveCutoff && !p['ESPN PlayerID'])
        : !p['ESPN PlayerID']
    );
    if (currentIdx === -1) return [];
    return displayPicks.slice(currentIdx + 1, currentIdx + 21);
  }, [displayPicks, roomSeason]);

  const handleDraft = async (player) => {
    if (!isMyTurn && draftMode !== 'test' && draftMode !== 'mockdraft') return alert("Not your turn!");
    if (!window.confirm(`Draft ${player.Player}${draftMode === 'test' || draftMode === 'mockdraft' ? ' (Test Mode)' : ''}?`)) return;

    const newQueue = queue.filter(p => String(p['ESPN PlayerID']) !== String(player['ESPN PlayerID']));
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));

    if (draftMode === 'test' || draftMode === 'mockdraft') {
      setPickStartTime(Date.now());
      const basePicks = testModePicks.length > 0 ? testModePicks : picks;
      
      setTestModePicks(basePicks.map(p => 
        p['Overall Pick'] === currentPick['Overall Pick'] 
          ? { ...p, 'ESPN PlayerID': player['ESPN PlayerID'], 'Selection': player.Player }
          : p
      ));
      
      setPlayers(curr => curr.map(p => 
        String(p['ESPN PlayerID']) === String(player['ESPN PlayerID'])
          ? { ...p, Availability: currentPick.Owner }
          : p
      ));
      
      await handleNewPick({ ...currentPick, 'ESPN PlayerID': player['ESPN PlayerID'], Round: currentPick.Round });
    } else {
      if (roomSeason === 2027) {
        const { error } = await supabase
          .from('draft_picks')
          .upsert({
            season_year: 2027,
            round: currentPick.Round,
            pick: currentPick.Pick,
            overall_pick: currentPick['Overall Pick'],
            team_owner: currentPick.Owner,
            player_id: player['ESPN PlayerID'],
            player_name: player.Player,
            player_position: player.Position,
            player_team: player.Team,
            is_keeper: false,
            picked_at: new Date().toISOString()
          }, { onConflict: 'season_year,overall_pick' });
          
        if (error) {
          alert('Database Update Error: ' + error.message);
          return;
        }
        fetchDraftOrder();
        handleNewPick({
          ...currentPick,
          'ESPN PlayerID': player['ESPN PlayerID'],
          Round: currentPick.Round,
          Selection: player.Player
        });
      } else {
        const { error } = await supabase
          .from('draft-order')
          .update({ 
            'ESPN PlayerID': player['ESPN PlayerID'],
            'Selection': player.Player 
          })
          .eq('Overall Pick', currentPick['Overall Pick']);
          
        if (error) {
          alert('Database Update Error: ' + error.message);
          return;
        }
        
        fetchDraftOrder();
        handleNewPick({
          ...currentPick,
          'ESPN PlayerID': player['ESPN PlayerID'],
          Round: currentPick.Round,
          Selection: player.Player
        });
      }
    }
    
    setShowDashboard(false);
  };

  const addToQueue = (player) => {
    const newQueue = [...queue, player];
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));
  };

  const removeFromQueue = (playerId) => {
    const newQueue = queue.filter(p => String(p['ESPN PlayerID']) !== String(playerId));
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));
  };

  // --- HISTORICAL DRAFT SNAPSHOT (2026 ARCHIVE) ---
  if (roomSeason === 2026) {
    return (
      <>
        <HistoricalDraftView
          allPicks={displayPicks}
          players={displayPlayers}
          keepers={_keepers}
          compPicks={_compPicks}
          draftTrades={draftTrades}
          onSeasonChange={handleSeasonChange}
          onOpenPlayerModal={(id, name) => {
            const found = displayPlayers.find(p => String(p['ESPN PlayerID']) === String(id));
            if (found) setSelectedPlayer(found);
            else if (onOpenPlayerModal) onOpenPlayerModal(id, name);
          }}
          onSwitchView={onSwitchView}
          analysisHistory={analysisHistory}
        />
        {selectedPlayer && (
          <PlayerModal
            player={selectedPlayer}
            onClose={() => setSelectedPlayer(null)}
            onOpenInDepthModal={onOpenPlayerModal}
            players={displayPlayers}
            draftHistory={draftHistory}
            historicalFinish={historicalFinish}
            savantBatters={savantBatters}
            savantPitchers={savantPitchers}
            battersZips={battersZips}
            pitchersZips={pitchersZips}
          />
        )}
      </>
    );
  }

  if (!draftMode) {
    return (
      <ModeSelectionModal 
        onSelectMode={handleModeSelect} 
        roomSeason={roomSeason} 
        onSeasonChange={handleSeasonChange} 
      />
    );
  }

  if (!currentUser) {
    return <LoginModal owners={DRAFT_OWNERS} onLogin={handleLogin} roomSeason={roomSeason} />;
  }

  const myPicks = displayPicks.filter(p => p.Owner === currentUser && p['ESPN PlayerID']);
  const recentPicks = displayPicks.filter(p => p['ESPN PlayerID']);

  if (draftMode === 'host') {
    const elapsed = Math.floor((Date.now() - pickStartTime) / 1000);
    const secondsLeft = Math.max(0, 60 - elapsed);
    const isWarning = secondsLeft <= 30 && secondsLeft > 0;
    const isUrgent = secondsLeft <= 15 && secondsLeft > 0;
    const isExpired = secondsLeft === 0;

    const timerColor = isExpired ? '#f44336' : isUrgent ? '#ff5722' : isWarning ? '#ffc107' : '#03dac6';
    const timerBg = isExpired ? '#f44336' : isUrgent ? '#ff5722' : isWarning ? '#ffc107' : '#222';
    const timerTextColor = isExpired || isUrgent ? '#fff' : isWarning ? '#000' : '#03dac6';
    const timerClass = isExpired ? 'monospace timer-expired' : isUrgent ? 'monospace urgent-warning' : isWarning ? 'monospace timer-warning' : 'monospace';
    const headerBg = isExpired
      ? 'linear-gradient(180deg, #3d1a1a 0%, #2d1616 100%)'
      : isUrgent
      ? 'linear-gradient(180deg, #3d2a1a 0%, #2d1f16 100%)'
      : isWarning
      ? 'linear-gradient(180deg, #3d3a1a 0%, #2d2916 100%)'
      : 'linear-gradient(180deg, #1a1a2e 0%, #16213e 100%)';

    const isPitcher = lastPickPlayer ? (lastPickPlayer.Position || '').includes('SP') || (lastPickPlayer.Position || '').includes('RP') : false;

    return (
      <div className={`host-mode-container ${isExpired ? 'container-expired' : ''}`} style={{
        backgroundColor: '#000',
        color: '#e0e0e0',
        height: '100vh',
        overflow: 'hidden',
        display: 'grid',
        gridTemplateColumns: '60% 40%',
        gridTemplateRows: '90px 1fr 50px',
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
      }}>
        <style>{`
          .host-mode-container, .host-mode-container * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
          }
          .host-mode-container .monospace {
            font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Fira Mono', monospace !important;
          }
          @keyframes timerWarning { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.02); } }
          .timer-warning { animation: timerWarning 1s ease-in-out infinite !important; }
          @keyframes timerPulse { 0%, 100% { transform: scale(1); box-shadow: 0 0 20px rgba(244, 67, 54, 0.5); } 50% { transform: scale(1.05); box-shadow: 0 0 40px rgba(244, 67, 54, 0.8); } }
          @keyframes borderFlash { 0%, 100% { border-color: #f44336; } 50% { border-color: #ff8a80; } }
          @keyframes headerPulse { 0%, 100% { background: linear-gradient(180deg, #3d1a1a 0%, #2d1616 100%); } 50% { background: linear-gradient(180deg, #5d2a2a 0%, #3d1a1a 100%); } }
          @keyframes textFlash { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
          @keyframes urgentPulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.8; transform: scale(1.02); } }
          @keyframes redVignette { 0%, 100% { box-shadow: inset 0 0 150px 50px rgba(244, 67, 54, 0.3); } 50% { box-shadow: inset 0 0 200px 80px rgba(244, 67, 54, 0.5); } }
          @keyframes cardGlow { 0%, 100% { box-shadow: 0 0 30px rgba(244, 67, 54, 0.4), 0 0 60px rgba(244, 67, 54, 0.2); } 50% { box-shadow: 0 0 50px rgba(244, 67, 54, 0.6), 0 0 100px rgba(244, 67, 54, 0.3); } }
          @keyframes screenShake { 0%, 100% { transform: translateX(0); } 25% { transform: translateX(-2px); } 75% { transform: translateX(2px); } }
          .timer-expired { animation: timerPulse 0.8s ease-in-out infinite !important; }
          .header-expired { animation: headerPulse 1s ease-in-out infinite, borderFlash 0.5s ease-in-out infinite !important; }
          .times-up-flash { animation: textFlash 0.5s ease-in-out infinite !important; }
          .urgent-warning { animation: urgentPulse 0.5s ease-in-out infinite !important; }
          .container-expired { animation: screenShake 0.3s ease-in-out infinite !important; }
        `}</style>

        {/* Host Header */}
        <div className={isExpired ? 'header-expired' : ''} style={{
          gridColumn: '1 / -1',
          background: headerBg,
          borderBottom: `4px solid ${timerColor}`,
          display: 'flex',
          alignItems: 'center',
          padding: '0 25px',
          justifyContent: 'space-between',
          transition: 'all 0.3s ease'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#fff', letterSpacing: '2px' }}>
              ⚾ HEFTY WAR ROOM {roomSeason}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginLeft: '10px', paddingLeft: '15px', borderLeft: '2px solid #444', overflow: 'hidden' }}>
              {[currentPick, ...upcomingPicks].filter(Boolean).slice(0, 10).map((p, idx) => {
                const isCurrent = idx === 0;
                const avatar = OWNER_AVATARS[p.Owner] || OWNER_AVATARS.default;
                return (
                  <div key={p['Overall Pick']} style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    background: isCurrent ? 'rgba(3, 218, 198, 0.15)' : '#1a1a2e',
                    border: `1px solid ${isCurrent ? '#03dac6' : '#333'}`,
                    padding: '4px 10px',
                    borderRadius: '20px',
                    minWidth: '100px'
                  }}>
                    <div style={{ width: '24px', height: '24px', borderRadius: '50%', overflow: 'hidden', border: `2px solid ${isCurrent ? '#03dac6' : '#555'}` }}>
                      <img src={avatar} alt={p.Owner} style={{ width: '100%', height: '100%', objectFit: 'cover' }} onError={e => { e.target.style.display = 'none'; }} />
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                      <span style={{ fontSize: '9px', color: isCurrent ? '#03dac6' : '#888', fontWeight: 'bold' }}>#{p['Overall Pick']}</span>
                      <span style={{ fontSize: '11px', color: '#fff', fontWeight: 'bold', whiteSpace: 'nowrap' }}>{p.Owner}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            {isUrgent && !isExpired && (
              <div className="urgent-warning" style={{ background: '#ff5722', color: '#fff', padding: '6px 14px', borderRadius: '8px', fontSize: '14px', fontWeight: 'bold' }}>
                ⚠️ HURRY!
              </div>
            )}
            {isExpired && (
              <div className="times-up-flash" style={{ background: '#f44336', color: '#fff', padding: '6px 14px', borderRadius: '8px', fontSize: '15px', fontWeight: 'bold' }}>
                ⏰ TIME'S UP!
              </div>
            )}
            <div className={timerClass} style={{
              fontSize: '34px',
              background: timerBg,
              padding: '4px 16px',
              borderRadius: '8px',
              border: `3px solid ${timerColor}`,
              color: timerTextColor,
              minWidth: '90px',
              textAlign: 'center',
              fontWeight: 900
            }}>
              {secondsLeft}s
            </div>
            <button
              onClick={handleResetMode}
              style={{ background: 'transparent', color: '#888', border: '1px solid #555', padding: '6px 10px', borderRadius: '6px', fontSize: '12px', cursor: 'pointer' }}
            >
              Exit Host
            </button>
            {onSwitchView && (
              <button
                onClick={() => onSwitchView('summary')}
                style={{ background: '#1e3a8a', color: '#fff', border: '1px solid #3b82f6', padding: '6px 12px', borderRadius: '6px', fontSize: '12px', cursor: 'pointer', fontWeight: 'bold' }}
              >
                📊 League Site
              </button>
            )}
          </div>
        </div>

        {/* 60% Left Stage */}
        <div style={{
          gridColumn: '1 / 2',
          gridRow: '2 / 3',
          display: 'flex',
          flexDirection: 'column',
          padding: '24px',
          borderRight: '1px solid #333',
          background: isExpired ? 'radial-gradient(ellipse at center, #2d1a1a 0%, #1a0000 100%)' : 'radial-gradient(ellipse at center, #1a1a2e 0%, #000 100%)',
          gap: '16px',
          overflow: 'hidden'
        }}>
          <div style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            background: 'linear-gradient(135deg, #1e1e2f 0%, #252540 100%)',
            borderRadius: '16px',
            padding: '24px',
            border: '2px solid #333',
            position: 'relative'
          }}>
            <div style={{ fontSize: '16px', color: '#888', textTransform: 'uppercase', letterSpacing: '2px', textAlign: 'center', marginBottom: '16px' }}>
              {lastPick ? `Round ${lastPick.Round} • Pick ${lastPick['Overall Pick']}` : 'Waiting for first pick...'}
            </div>

            {lastPickPlayer ? (
              <div style={{ display: 'flex', width: '100%', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
                <div style={{ width: '120px', height: '120px', borderRadius: '16px', border: '3px solid #bb86fc', overflow: 'hidden', background: '#111', flexShrink: 0, boxShadow: '0 4px 20px rgba(187, 134, 252, 0.4)' }}>
                  <img
                    src={getPlayerHeadshotUrl(lastPickPlayer)}
                    alt={lastPickPlayer.Player}
                    style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                    onError={(e) => handleHeadshotError(e, lastPickPlayer)}
                  />
                </div>
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '0 16px' }}>
                  <div style={{ fontSize: '38px', fontWeight: '900', lineHeight: '1.1', color: '#fff', textAlign: 'center', marginBottom: '10px' }}>
                    {lastPickPlayer.Player}
                  </div>
                  <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginBottom: '14px' }}>
                    <span style={{ background: 'linear-gradient(135deg, #bb86fc 0%, #9c5dd9 100%)', padding: '4px 14px', borderRadius: '20px', fontSize: '14px', fontWeight: 'bold', color: '#fff' }}>
                      {lastPickPlayer.Position}
                    </span>
                    <span style={{ background: 'linear-gradient(135deg, #03dac6 0%, #00a896 100%)', padding: '4px 14px', borderRadius: '20px', fontSize: '14px', fontWeight: 'bold', color: '#000' }}>
                      {lastPickPlayer.Team}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: '10px' }}>
                    {isPitcher ? (
                      <>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>ERA</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSERA || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>WHIP</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSWHIP || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>K</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSK || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>QS</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSQS || '—'}</span>
                        </div>
                      </>
                    ) : (
                      <>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>HR</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSHR || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>RBI</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSRBI || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>SB</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSSB || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>OBP</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSOBP || '—'}</span>
                        </div>
                      </>
                    )}
                  </div>
                </div>
                <div style={{ width: '110px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
                  <div style={{ width: '90px', height: '90px', borderRadius: '50%', border: '3px solid #03dac6', overflow: 'hidden', background: '#111' }}>
                    <img
                      src={OWNER_AVATARS[lastPick?.Owner] || OWNER_AVATARS.default}
                      alt={lastPick?.Owner}
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                      onError={e => { e.target.style.display = 'none'; }}
                    />
                  </div>
                  <div style={{ textAlign: 'center', background: 'rgba(0,0,0,0.5)', padding: '4px 10px', borderRadius: '8px', border: '1px solid #03dac644' }}>
                    <div style={{ fontSize: '9px', color: '#03dac6', fontWeight: 'bold', textTransform: 'uppercase' }}>Selected By</div>
                    <div style={{ fontSize: '14px', color: '#fff', fontWeight: 'bold' }}>{lastPick?.Owner}</div>
                  </div>
                </div>
              </div>
            ) : (
              <div style={{ fontSize: '32px', color: '#444', fontWeight: 'bold', textAlign: 'center', padding: '40px 0' }}>
                WAR ROOM READY
              </div>
            )}

            {/* AI Commentary in Host Mode */}
            <div style={{
              flex: 1,
              minHeight: '110px',
              background: 'rgba(0,0,0,0.3)',
              padding: '16px',
              borderRadius: '12px',
              borderLeft: '5px solid #bb86fc',
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden'
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                <span style={{ fontSize: '12px', color: '#bb86fc', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  🎙️ HeftyMatic 3000 Instant Reaction
                </span>
                {lastPickCommentary && lastPickCommentary !== 'Draft has not started.' && !generatingCommentary && (
                  <button
                    onClick={() => playPodcastTTS(lastPickCommentary)}
                    style={{
                      background: 'rgba(187, 134, 252, 0.15)',
                      border: '1px solid #bb86fc',
                      color: '#bb86fc',
                      borderRadius: '6px',
                      padding: '2px 8px',
                      fontSize: '11px',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '4px'
                    }}
                    title="Play podcast-style spoken analysis"
                  >
                    🔊 Listen
                  </button>
                )}
              </div>
              <div style={{ flex: 1, overflowY: 'auto', fontSize: '15px', color: '#ddd', lineHeight: '1.5' }}>
                {generatingCommentary ? (
                  <span style={{ color: '#888', fontStyle: 'italic' }}>🤔 HeftyMatic 3000 is analyzing this pick...</span>
                ) : (
                  <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
                )}
              </div>
            </div>
          </div>
        </div>

        {/* 40% Right Stage */}
        <div style={{
          gridColumn: '2 / 3',
          gridRow: '2 / 3',
          background: '#111',
          padding: '20px',
          display: 'flex',
          flexDirection: 'column',
          gap: '15px',
          overflow: 'hidden'
        }}>
          <OnDeckSidebar upcomingPicks={upcomingPicks} currentUser={currentUser} />
          <div style={{ flex: 1, overflowY: 'auto' }}>
            <RecentActivityWidget
              recentPicks={recentPicks}
              players={displayPlayers}
              onPlayerClick={setSelectedPlayer}
              playerInfo={playerInfo}
            />
          </div>
        </div>

        {/* Bottom Ticker */}
        <div style={{ gridColumn: '1 / -1', gridRow: '3 / 4', borderTop: '2px solid #333' }}>
          <Ticker recentPicks={recentPicks} players={displayPlayers} />
        </div>

        {selectedPlayer && (
          <PlayerModal
            player={selectedPlayer}
            onClose={() => setSelectedPlayer(null)}
            onOpenInDepthModal={onOpenPlayerModal}
            players={displayPlayers}
            draftHistory={draftHistory}
            historicalFinish={historicalFinish}
            savantBatters={savantBatters}
            savantPitchers={savantPitchers}
            battersZips={battersZips}
            pitchersZips={pitchersZips}
          />
        )}
      </div>
    );
  }

  if (draftMode === 'mobile') {
    return (
      <div style={{
        ...styles.body,
        gridTemplateColumns: '1fr',
        gridTemplateRows: '50px 1fr 50px',
        height: '100vh'
      }}>
        {/* Mobile Header */}
        <div style={{
          gridColumn: '1 / -1',
          background: '#1f1f1f',
          borderBottom: '2px solid #bb86fc',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '0 15px',
          zIndex: 10
        }}>
          <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#bb86fc' }}>
            HWR '{String(roomSeason).slice(-2)}
          </div>
          <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
            <div style={{
              fontSize: '12px',
              fontWeight: 'bold',
              color: isMyTurn ? '#03dac6' : '#888',
              animation: isMyTurn ? 'pulse 1s infinite' : 'none'
            }}>
              {currentPick ? `${currentPick.Owner} (#${currentPick['Overall Pick']})` : 'COMPLETE'}
            </div>
            <CountdownTimer pickStartTime={pickStartTime} />
            <button
              onClick={handleResetMode}
              style={{ background: 'transparent', color: '#888', border: '1px solid #444', padding: '3px 6px', borderRadius: '4px', fontSize: '10px', cursor: 'pointer' }}
            >
              Mode
            </button>
            {onSwitchView && (
              <button
                onClick={() => onSwitchView('summary')}
                style={{ background: '#1e3a8a', color: '#fff', border: '1px solid #3b82f6', padding: '3px 8px', borderRadius: '4px', fontSize: '10px', cursor: 'pointer', fontWeight: 'bold' }}
              >
                League
              </button>
            )}
          </div>
        </div>

        {/* Mobile Content Area */}
        <div style={{ gridColumn: '1 / -1', overflow: 'hidden', background: '#121212', padding: '10px' }}>
          {activeTab === 'Pool' && (
            <PlayerPoolPanel
              players={displayPlayers}
              onDraft={handleDraft}
              isMyTurn={isMyTurn}
              queue={queue}
              onAddToQueue={addToQueue}
              onRemoveFromQueue={removeFromQueue}
              draftMode={draftMode}
              testModePicks={testModePicks}
              allPicks={displayPicks}
              onPlayerClick={setSelectedPlayer}
              playerInfo={playerInfo}
              isMobile={true}
            />
          )}
          {activeTab === 'Queue' && (
            <div style={{ height: '100%', overflowY: 'auto' }}>
              <div style={styles.wrHeader}>My Queue ({queue.length})</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {queue.length === 0 ? (
                  <div style={{ color: '#888', textAlign: 'center', padding: '20px' }}>No players in queue. Star players in the pool to add them here!</div>
                ) : (
                  queue.map(p => (
                    <div key={p['ESPN PlayerID']} style={{ ...styles.queueItem, background: '#222' }}>
                      <div>
                        <div style={{ color: '#fff', fontWeight: 'bold' }}>{p.Player}</div>
                        <div style={{ fontSize: '12px', color: '#888' }}>{p.Position} - {p.Team}</div>
                      </div>
                      <button onClick={() => removeFromQueue(p['ESPN PlayerID'])} style={styles.btnStarActive}>✕</button>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
          {activeTab === 'Roster' && (
            <RosterManagerPanel
              allPicks={displayPicks}
              players={displayPlayers}
              currentUser={currentUser}
            />
          )}
          {activeTab === 'Feed' && (
            <div style={{ height: '100%', overflowY: 'auto' }}>
              <div style={styles.wrHeader}>Draft Feed</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {recentPicks.slice().reverse().map(p => {
                  const playerObj = displayPlayers.find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID']));
                  return (
                    <div key={p['Overall Pick']} style={{
                      padding: '10px',
                      background: '#222',
                      borderRadius: '4px',
                      borderLeft: `4px solid ${p.Owner === currentUser ? '#03dac6' : '#555'}`
                    }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', color: '#888', fontSize: '11px' }}>
                        <span>Pick #{p['Overall Pick']}</span>
                        <span>{p.Owner}</span>
                      </div>
                      <div style={{ color: '#fff', fontWeight: 'bold', fontSize: '14px' }}>
                        {playerObj?.Player || p.Selection || 'Pick Made'}
                      </div>
                      <div style={{ color: '#aaa', fontSize: '12px' }}>
                        {playerObj?.Position} - {playerObj?.Team}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Mobile Bottom Nav */}
        <div style={{
          gridColumn: '1 / -1',
          background: '#2c2c2c',
          borderTop: '1px solid #444',
          display: 'flex',
          justifyContent: 'space-around',
          alignItems: 'center'
        }}>
          {[
            { id: 'Pool', label: 'Pool', icon: '📋' },
            { id: 'Queue', label: 'Queue', icon: '⭐' },
            { id: 'Roster', label: 'Roster', icon: '👥' },
            { id: 'Feed', label: 'Feed', icon: '📢' }
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              style={{
                background: 'none',
                border: 'none',
                color: activeTab === tab.id ? '#03dac6' : '#888',
                padding: '8px',
                fontSize: '11px',
                fontWeight: 'bold',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                cursor: 'pointer'
              }}
            >
              <span style={{ fontSize: '18px', marginBottom: '2px' }}>{tab.icon}</span>
              {tab.label}
            </button>
          ))}
        </div>

        {selectedPlayer && (
          <PlayerModal
            player={selectedPlayer}
            onClose={() => setSelectedPlayer(null)}
            onOpenInDepthModal={onOpenPlayerModal}
            players={displayPlayers}
            draftHistory={draftHistory}
            historicalFinish={historicalFinish}
            savantBatters={savantBatters}
            savantPitchers={savantPitchers}
            battersZips={battersZips}
            pitchersZips={pitchersZips}
            isMobile={true}
          />
        )}
      </div>
    );
  }

  return (
    <>
      <style>{`
        @keyframes scroll {
          0% { transform: translate3d(0, 0, 0); }
          100% { transform: translate3d(-100%, 0, 0); }
        }
        @keyframes pulse {
          0% { opacity: 1; }
          50% { opacity: 0.5; }
          100% { opacity: 1; }
        }
        :root {
          --bg-dark: #121212;
          --bg-card: #1e1e1e;
          --accent: #bb86fc;
          --text-main: #e0e0e0;
          --highlight: #03dac6;
          --alert: #cf6679;
        }
      `}</style>
      
      <div style={styles.body}>
        {/* Header */}
        <div style={styles.header}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            <div style={styles.leagueLogo}>
              ⚾ Hefty War Room {roomSeason}
            </div>

            {/* In-Room Season Switcher */}
            <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
              <button
                onClick={() => handleSeasonChange(2027)}
                style={{
                  background: roomSeason === 2027 ? '#bb86fc' : 'transparent',
                  color: roomSeason === 2027 ? '#000' : '#888',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '3px 9px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                🚀 2027 Draft
              </button>
              <button
                onClick={() => handleSeasonChange(2026)}
                style={{
                  background: roomSeason === 2026 ? '#03dac6' : 'transparent',
                  color: roomSeason === 2026 ? '#000' : '#888',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '3px 9px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                🏛️ 2026 Archive
              </button>
            </div>
            {draftMode === 'test' && (
              <span style={{ 
                fontSize: '12px', 
                background: '#ffc10722', 
                color: '#ffc107', 
                border: '1px solid #ffc107', 
                padding: '3px 8px', 
                borderRadius: '4px', 
                fontWeight: 'bold' 
              }}>
                [TEST]
              </span>
            )}
            {draftMode === 'multitest' && (
              <span style={{ 
                fontSize: '12px', 
                background: '#ff980022', 
                color: '#ff9800', 
                border: '1px solid #ff9800', 
                padding: '3px 8px', 
                borderRadius: '4px', 
                fontWeight: 'bold' 
              }}>
                [MULTIPLAYER TEST]
              </span>
            )}
            {draftMode === 'mockdraft' && (
              <span style={{ 
                fontSize: '12px', 
                background: '#bb86fc22', 
                color: '#bb86fc', 
                border: '1px solid #bb86fc', 
                padding: '3px 8px', 
                borderRadius: '4px', 
                fontWeight: 'bold' 
              }}>
                [MOCK DRAFT]
              </span>
            )}
            {draftMode === 'multitest' && (
              <button
                onClick={handleResetDraft}
                disabled={resetting}
                style={{
                  background: resetting ? '#666' : '#f44336',
                  color: '#fff',
                  border: 'none',
                  padding: '4px 10px',
                  borderRadius: '4px',
                  cursor: resetting ? 'wait' : 'pointer',
                  fontSize: '11px',
                  fontWeight: 'bold'
                }}
              >
                {resetting ? 'Resetting...' : '🔄 Reset Draft'}
              </button>
            )}
            <button
              onClick={handleResetMode}
              style={{
                background: 'transparent',
                color: '#888',
                border: '1px solid #444',
                padding: '4px 8px',
                borderRadius: '4px',
                fontSize: '11px',
                cursor: 'pointer'
              }}
              title="Switch between Live and Test Mode"
            >
              Switch Mode
            </button>
            <button
              onClick={handleResetUser}
              style={{
                background: 'transparent',
                color: '#888',
                border: '1px solid #444',
                padding: '4px 8px',
                borderRadius: '4px',
                fontSize: '11px',
                cursor: 'pointer'
              }}
              title="Change active team"
            >
              Switch Team ({currentUser})
            </button>
          </div>

          <div style={{ textAlign: 'center', color: '#fff' }}>
            <div style={{ fontSize: '11px', color: '#888', letterSpacing: '1px' }}>ON THE CLOCK</div>
            <div style={{ fontSize: '22px', color: 'var(--highlight)', fontWeight: 'bold' }}>
              {currentPick ? currentPick.Owner : 'DRAFT COMPLETE'}
            </div>
          </div>

          <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
            <button
              onClick={() => setAudioEnabled(!audioEnabled)}
              style={{
                background: audioEnabled ? '#4caf50' : '#444',
                color: '#fff',
                border: 'none',
                padding: '8px 14px',
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                gap: '6px'
              }}
              title="Toggle draft audio announcements"
            >
              <span>{audioEnabled ? '🔊' : '🔇'}</span>
              <span>Audio {audioEnabled ? 'ON' : 'OFF'}</span>
            </button>

            {draftMode === 'test' && (
              <button
                onClick={() => {
                  if (!audioEnabled) {
                    alert('Please enable audio first by clicking the Audio ON button');
                    return;
                  }
                  playOwnerSound(currentUser);
                }}
                style={{
                  background: '#ffc107',
                  color: '#000',
                  border: 'none',
                  padding: '8px 12px',
                  borderRadius: '6px',
                  cursor: 'pointer',
                  fontSize: '13px',
                  fontWeight: 'bold'
                }}
              >
                🧪 Test Audio
              </button>
            )}

            <CountdownTimer pickStartTime={pickStartTime} />

            {onSwitchView && (
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <button
                  onClick={() => onSwitchView('capital')}
                  style={{
                    background: '#2e1065',
                    color: '#d8b4fe',
                    border: '1px solid #7e22ce',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    fontSize: '13px',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    cursor: 'pointer'
                  }}
                  title="View 2027 Draft Capital & Traded Picks"
                >
                  <span>🎟️</span>
                  <span>Draft Capital</span>
                </button>
                <button
                  onClick={() => onSwitchView('keepers')}
                  style={{
                    background: '#064e3b',
                    color: '#6ee7b7',
                    border: '1px solid #059669',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    fontSize: '13px',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    cursor: 'pointer'
                  }}
                  title="View Keepers & Budgets Panel"
                >
                  <span>💎</span>
                  <span>Keepers & Budget</span>
                </button>
                <button
                  onClick={() => onSwitchView('weekly')}
                  style={{
                    background: '#1e3a8a',
                    color: '#fff',
                    border: '1px solid #3b82f6',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    fontSize: '13px',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    cursor: 'pointer'
                  }}
                  title="Return to Main League Site"
                >
                  <span>⚾</span>
                  <span>League Site</span>
                </button>
              </div>
            )}
          </div>
        </div>

        {/* War Room Drawer Toggle */}
        <button 
          style={styles.warRoomToggle}
          onClick={() => setShowDashboard(!showDashboard)}
        >
          {showDashboard ? '✕ CLOSE DRAWER' : '📋 OPEN WAR ROOM DASHBOARD'}
        </button>

        {/* Main Stage */}
        <div style={styles.mainStage}>
          <div style={styles.pickCard}>
            <div style={styles.pickMeta}>
              {lastPick ? `Round ${lastPick.Round} • Pick #${lastPick['Overall Pick']}` : 'Waiting for first pick...'}
            </div>
            <div style={styles.pickPlayer}>
              {lastPickPlayer ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '20px', flexWrap: 'wrap' }}>
                  <div style={{
                    width: '84px',
                    height: '84px',
                    borderRadius: '50%',
                    border: '3px solid #bb86fc',
                    backgroundColor: '#222',
                    overflow: 'hidden',
                    flexShrink: 0,
                    boxShadow: '0 4px 18px rgba(187, 134, 252, 0.4)'
                  }}>
                    <img
                      src={getPlayerHeadshotUrl(lastPickPlayer)}
                      alt={lastPickPlayer.Player}
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                      onError={(e) => handleHeadshotError(e, lastPickPlayer)}
                    />
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <PlayerNameButton 
                      player={lastPickPlayer} 
                      onClick={setSelectedPlayer}
                      style={{ color: '#fff', fontWeight: '700', fontSize: '52px' }}
                    />
                    {getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo) && (
                      <span style={{
                        marginLeft: '15px',
                        padding: '4px 10px',
                        borderRadius: '6px',
                        background: getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo).color,
                        color: '#000',
                        fontSize: '18px',
                        fontWeight: 'bold',
                        verticalAlign: 'middle'
                      }}>
                        {getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo).status}
                      </span>
                    )}
                  </div>
                </div>
              ) : (
                'WAITING ON CLOCK...'
              )}
            </div>
            <div style={styles.pickOwner}>
              {lastPick ? `Selected by ${lastPick.Owner}` : '--'}
            </div>
            <div style={styles.aiBox}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                <span style={{ fontSize: '11px', color: '#bb86fc', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  🎙️ Instant Reaction
                </span>
                {lastPickCommentary && lastPickCommentary !== 'Draft has not started.' && !generatingCommentary && (
                  <button
                    onClick={() => playPodcastTTS(lastPickCommentary)}
                    style={{
                      background: 'rgba(187, 134, 252, 0.15)',
                      border: '1px solid #bb86fc',
                      color: '#bb86fc',
                      borderRadius: '4px',
                      padding: '2px 6px',
                      fontSize: '10px',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '3px'
                    }}
                    title="Play podcast-style spoken analysis"
                  >
                    🔊 Listen
                  </button>
                )}
              </div>
              {generatingCommentary ? (
                <span>🤔 Gemini is analyzing this selection...</span>
              ) : (
                <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
              )}
            </div>
          </div>
        </div>

        {/* On Deck Sidebar */}
        <OnDeckSidebar upcomingPicks={upcomingPicks} currentUser={currentUser} />

        {/* Bottom Info Bar */}
        <div style={styles.infoBar}>
          <TeamNeedsSummary myPicks={myPicks} players={displayPlayers} />
          <RecentActivityWidget 
            recentPicks={recentPicks} 
            players={displayPlayers} 
            onPlayerClick={setSelectedPlayer}
            playerInfo={playerInfo}
          />
          <QueuePreviewWidget 
            queue={queue} 
            onPlayerClick={setSelectedPlayer} 
            playerInfo={playerInfo} 
          />
        </div>

        {/* Scrolling Ticker */}
        <Ticker recentPicks={recentPicks} players={displayPlayers} />

        {/* War Room Drawer / Panel */}
        {showDashboard && (
          <div style={styles.warRoomPanel}>
            <div style={styles.panelNav}>
              {['Pool', 'Roster', 'MyPicks', 'DraftLog', 'Analysis', 'Standings'].map(tab => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  style={activeTab === tab ? styles.navBtnActive : styles.navBtn}
                >
                  {tab === 'Pool' ? 'Draft Pool' : 
                   tab === 'Roster' ? 'Roster Manager' : 
                   tab === 'MyPicks' ? 'My Picks' :
                   tab === 'DraftLog' ? 'Draft Log' :
                   tab === 'Analysis' ? 'Analysis History' :
                   'Projected Standings'}
                </button>
              ))}
              <div style={{ marginLeft: 'auto', color: '#888', fontSize: '13px', display: 'flex', alignItems: 'center' }}>
                Logged in as: <span style={{ color: 'var(--highlight)', fontWeight: 'bold', marginLeft: '5px' }}>{currentUser}</span>
              </div>
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Pool' ? 'grid' : 'none' }}>
              <PlayerPoolPanel
                players={displayPlayers}
                onDraft={handleDraft}
                isMyTurn={isMyTurn}
                queue={queue}
                onAddToQueue={addToQueue}
                onRemoveFromQueue={removeFromQueue}
                draftMode={draftMode}
                testModePicks={testModePicks}
                allPicks={displayPicks}
                onPlayerClick={setSelectedPlayer}
                playerInfo={playerInfo}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Roster' ? 'grid' : 'none' }}>
              <RosterManagerPanel
                allPicks={displayPicks}
                players={displayPlayers}
                currentUser={currentUser}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'MyPicks' ? 'grid' : 'none' }}>
              <MyPicksPanel
                allPicks={displayPicks}
                players={displayPlayers}
                currentUser={currentUser}
                draftTrades={draftTrades}
                roomSeason={roomSeason}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'DraftLog' ? 'grid' : 'none' }}>
              <DraftLogPanel
                allPicks={displayPicks}
                players={displayPlayers}
                onPlayerClick={setSelectedPlayer}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Analysis' ? 'grid' : 'none' }}>
              <AnalysisHistoryPanel
                analysisHistory={analysisHistory}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Standings' ? 'grid' : 'none' }}>
              <StandingsPanel 
                allPicks={displayPicks}
                players={displayPlayers}
              />
            </div>
          </div>
        )}

        {/* Mock Draft Floating Controller */}
        {draftMode === 'mockdraft' && (
          <div style={{
            position: 'fixed',
            bottom: '55px',
            left: '50%',
            transform: 'translateX(-50%)',
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            background: '#1a1a2e',
            border: '1px solid #bb86fc44',
            borderRadius: '12px',
            padding: '8px 18px',
            zIndex: 150,
            boxShadow: '0 4px 20px rgba(0,0,0,0.6)'
          }}>
            <span style={{ color: '#bb86fc', fontSize: '11px', fontWeight: 'bold', letterSpacing: '1px' }}>
              🤖 MOCK DRAFT
            </span>
            <div style={{ width: '1px', height: '20px', background: '#444' }} />
            <button
              onClick={() => setIsRunningMock(r => !r)}
              style={{
                background: isRunningMock ? '#f44336' : '#4caf50',
                color: '#fff',
                border: 'none',
                padding: '6px 16px',
                borderRadius: '6px',
                cursor: 'pointer',
                fontWeight: 'bold',
                fontSize: '12px'
              }}
            >
              {isRunningMock ? '⏸ PAUSE' : '▶ RUN'}
            </button>
            {!isRunningMock && (
              <button
                onClick={stepMockPick}
                disabled={currentPick?.Owner === currentUser}
                title="Advance one pick"
                style={{
                  background: currentPick?.Owner === currentUser ? '#444' : '#ffc107',
                  color: currentPick?.Owner === currentUser ? '#666' : '#000',
                  border: 'none',
                  padding: '6px 14px',
                  borderRadius: '6px',
                  cursor: currentPick?.Owner === currentUser ? 'not-allowed' : 'pointer',
                  fontWeight: 'bold',
                  fontSize: '12px'
                }}
              >
                ⏭ STEP
              </button>
            )}
            <div style={{ width: '1px', height: '20px', background: '#444' }} />
            <span style={{ color: '#888', fontSize: '11px' }}>Speed:</span>
            {[
              { label: '🐢', title: 'Slow (3s)', val: 3000 },
              { label: '⚡', title: 'Fast (1s)', val: 1000 },
              { label: '🚀', title: 'Turbo (300ms)', val: 300 }
            ].map(s => (
              <button
                key={s.val}
                onClick={() => setMockSpeed(s.val)}
                title={s.title}
                style={{
                  background: mockSpeed === s.val ? '#bb86fc' : '#333',
                  color: mockSpeed === s.val ? '#000' : '#ccc',
                  border: 'none',
                  padding: '5px 10px',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '14px'
                }}
              >
                {s.label}
              </button>
            ))}
            <div style={{ width: '1px', height: '20px', background: '#444' }} />
            {currentPick?.Owner === currentUser ? (
              <span style={{ color: '#ffc107', fontWeight: 'bold', fontSize: '12px', animation: 'pulse 1s infinite' }}>
                ⭐ YOUR PICK!
              </span>
            ) : (
              <span style={{ color: '#888', fontSize: '12px' }}>
                On clock: <span style={{ color: '#03dac6', fontWeight: 'bold' }}>{currentPick?.Owner || '—'}</span>
              </span>
            )}
          </div>
        )}
      </div>
      
      {/* Player Detail Modal */}
      {selectedPlayer && (
        <PlayerModal 
          player={selectedPlayer} 
          onClose={() => setSelectedPlayer(null)}
          onOpenInDepthModal={onOpenPlayerModal}
          players={displayPlayers}
          draftHistory={draftHistory}
          historicalFinish={historicalFinish}
          savantBatters={savantBatters}
          savantPitchers={savantPitchers}
          battersZips={battersZips}
          pitchersZips={pitchersZips}
        />
      )}
    </>
  );
}

// --- STYLES ---
const styles = {
  body: {
    backgroundColor: '#121212',
    color: '#e0e0e0',
    fontFamily: "'Roboto Condensed', sans-serif",
    margin: 0,
    height: '100vh',
    overflow: 'hidden',
    display: 'grid',
    gridTemplateColumns: '1fr 300px',
    gridTemplateRows: '70px 1fr minmax(140px, auto) 45px'
  },
  header: {
    gridColumn: '1 / -1',
    background: '#1f1f1f',
    borderBottom: '2px solid #bb86fc',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0 25px',
    zIndex: 10
  },
  leagueLogo: {
    fontSize: '20px',
    fontWeight: 'bold',
    color: '#bb86fc',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  clock: {
    background: '#000',
    fontFamily: "'Orbitron', monospace",
    fontSize: '24px',
    padding: '4px 15px',
    border: '2px solid #333',
    borderRadius: '6px',
    userSelect: 'none',
    minWidth: '80px',
    textAlign: 'center'
  },
  warRoomToggle: {
    position: 'absolute',
    top: '80px',
    right: '320px',
    background: '#03dac6',
    color: '#000',
    border: 'none',
    padding: '8px 16px',
    borderRadius: '20px',
    cursor: 'pointer',
    fontWeight: 'bold',
    boxShadow: '0 4px 12px rgba(3, 218, 198, 0.4)',
    zIndex: 50,
    fontSize: '13px'
  },
  mainStage: {
    gridColumn: '1 / 2',
    gridRow: '2 / 3',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
    position: 'relative',
    padding: '20px'
  },
  pickCard: {
    background: 'linear-gradient(145deg, #252525, #1a1a1a)',
    border: '1px solid #333',
    borderRadius: '12px',
    padding: '30px',
    textAlign: 'center',
    width: '88%',
    maxWidth: '850px',
    boxShadow: '0 20px 50px rgba(0,0,0,0.5)'
  },
  pickMeta: {
    fontSize: '16px',
    color: '#888',
    marginBottom: '8px',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  pickPlayer: {
    fontSize: '56px',
    fontWeight: '700',
    color: '#fff',
    margin: '8px 0',
    lineHeight: '1.1',
    textShadow: '0 4px 10px rgba(0,0,0,0.5)'
  },
  pickOwner: {
    fontSize: '26px',
    color: '#03dac6',
    fontWeight: 'bold'
  },
  aiBox: {
    marginTop: '20px',
    background: 'rgba(187, 134, 252, 0.08)',
    borderLeft: '4px solid #bb86fc',
    padding: '14px 20px',
    textAlign: 'left',
    fontSize: '16px',
    lineHeight: '1.5',
    fontStyle: 'italic',
    color: '#ccc',
    maxHeight: '150px',
    overflowY: 'auto'
  },
  sidebar: {
    gridColumn: '2 / 3',
    gridRow: '2 / 3',
    background: '#181818',
    borderLeft: '1px solid #333',
    overflowY: 'auto',
    padding: '15px'
  },
  deckItem: {
    background: '#222',
    marginBottom: '8px',
    padding: '10px',
    borderRadius: '4px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  infoBar: {
    gridColumn: '1 / -1',
    gridRow: '3 / 4',
    display: 'grid',
    gridTemplateColumns: '1.2fr 1fr 1fr',
    gap: '12px',
    padding: '10px 20px',
    background: '#1a1a1a',
    borderTop: '1px solid #333'
  },
  infoWidget: {
    background: '#222',
    borderRadius: '8px',
    padding: '10px 14px',
    overflow: 'hidden'
  },
  infoWidgetTitle: {
    fontSize: '13px',
    fontWeight: 'bold',
    color: 'var(--highlight)',
    marginBottom: '8px',
    paddingBottom: '6px',
    borderBottom: '1px solid #333'
  },
  tickerContainer: {
    gridColumn: '1 / -1',
    gridRow: '4 / 5',
    background: '#bb86fc',
    color: '#000',
    overflow: 'hidden',
    display: 'flex',
    alignItems: 'center',
    whiteSpace: 'nowrap'
  },
  tickerContent: {
    display: 'inline-block',
    animation: 'scroll 40s linear infinite',
    paddingLeft: '100%',
    fontWeight: 'bold',
    fontSize: '16px'
  },
  tickerItem: {
    marginRight: '40px'
  },
  warRoomPanel: {
    display: 'grid',
    position: 'fixed',
    bottom: 0,
    left: 0,
    width: '100%',
    height: '82%',
    background: '#242424',
    borderTop: '4px solid #03dac6',
    zIndex: 100,
    padding: '16px 20px',
    boxSizing: 'border-box',
    gridTemplateRows: '40px 1fr',
    gap: '15px',
    boxShadow: '0 -10px 30px rgba(0,0,0,0.8)'
  },
  panelNav: {
    display: 'flex',
    gap: '10px',
    borderBottom: '1px solid #444',
    paddingBottom: '10px'
  },
  navBtn: {
    background: '#333',
    color: '#ccc',
    border: 'none',
    padding: '6px 14px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '13px'
  },
  navBtnActive: {
    background: '#03dac6',
    color: '#000',
    border: 'none',
    padding: '6px 14px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '13px'
  },
  panelContent: {
    height: '100%',
    overflow: 'hidden',
    gridTemplateColumns: '3fr 1fr',
    gap: '15px',
    paddingRight: '5px'
  },
  wrColumn: {
    display: 'flex',
    flexDirection: 'column',
    background: '#1a1a1a',
    padding: '12px 16px',
    borderRadius: '8px',
    height: '100%',
    overflow: 'hidden'
  },
  wrHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '10px',
    color: '#03dac6',
    fontSize: '16px',
    fontWeight: 'bold',
    borderBottom: '1px solid #444',
    paddingBottom: '6px',
    alignItems: 'center',
    flexWrap: 'wrap',
    gap: '10px'
  },
  poolTableContainer: {
    flexGrow: 1,
    overflowY: 'auto',
    overflowX: 'auto',
    marginTop: '6px'
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '12px',
    minWidth: '100%'
  },
  tableHead: {
    position: 'sticky',
    top: 0,
    background: '#2c2c2c',
    color: '#03dac6',
    zIndex: 10
  },
  th: {
    padding: '8px 6px',
    textAlign: 'left',
    cursor: 'pointer',
    borderBottom: '2px solid #555',
    userSelect: 'none',
    whiteSpace: 'nowrap'
  },
  td: {
    padding: '6px 6px',
    borderBottom: '1px solid #333',
    color: '#ccc',
    whiteSpace: 'nowrap'
  },
  tableRow: {
    transition: 'background 0.2s'
  },
  btnDraft: {
    background: '#03dac6',
    border: 'none',
    color: '#000',
    fontWeight: 'bold',
    padding: '3px 7px',
    cursor: 'pointer',
    borderRadius: '4px',
    fontSize: '11px',
    marginRight: '5px'
  },
  btnStar: {
    background: 'none',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: '14px'
  },
  btnStarActive: {
    background: 'none',
    border: 'none',
    color: 'gold',
    cursor: 'pointer',
    fontSize: '14px'
  },
  filterControl: {
    background: '#333',
    color: '#fff',
    border: '1px solid #555',
    padding: '4px 8px',
    borderRadius: '4px',
    fontSize: '12px'
  },
  rosterSelect: {
    background: '#333',
    color: '#fff',
    border: '1px solid #555',
    padding: '4px',
    width: '100%',
    borderRadius: '3px',
    cursor: 'pointer',
    fontSize: '12px'
  },
  queueItem: {
    padding: '8px',
    borderBottom: '1px solid #333',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    borderLeft: '3px solid #03dac6'
  },
  loginOverlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0,0,0,0.92)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 1000
  },
  loginBox: {
    background: '#1e1e1e',
    padding: '50px 40px',
    borderRadius: '12px',
    textAlign: 'center',
    border: '2px solid #bb86fc',
    boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
    maxWidth: '480px',
    width: '90%'
  },
  loginSelect: {
    fontSize: '16px',
    padding: '10px 14px',
    margin: '20px 0',
    width: '100%',
    borderRadius: '6px',
    border: '2px solid #444',
    background: '#333',
    color: '#fff'
  },
  loginButton: {
    fontSize: '16px',
    padding: '12px 24px',
    background: '#03dac6',
    color: '#000',
    border: 'none',
    borderRadius: '8px',
    cursor: 'pointer',
    fontWeight: 'bold',
    width: '100%'
  },
  modalOverlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0, 0, 0, 0.85)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 2000,
    backdropFilter: 'blur(4px)'
  },
  modalContent: {
    background: '#1e1e1e',
    borderRadius: '12px',
    width: '95%',
    maxWidth: '1280px',
    maxHeight: '90vh',
    overflow: 'hidden',
    boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
    border: '2px solid #bb86fc',
    display: 'flex',
    flexDirection: 'column'
  },
  modalHeader: {
    padding: '20px 25px',
    borderBottom: '2px solid #333',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    background: '#252525'
  },
  modalCloseBtn: {
    background: 'none',
    border: 'none',
    color: '#888',
    fontSize: '24px',
    cursor: 'pointer',
    padding: '0',
    width: '28px',
    height: '28px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: '4px'
  },
  modalBody: {
    padding: '20px 25px',
    overflowY: 'auto',
    flexGrow: 1
  },
  modalSection: {
    marginBottom: '20px',
    padding: '14px',
    background: '#252525',
    borderRadius: '8px',
    borderLeft: '4px solid var(--accent)'
  },
  modalSectionTitle: {
    margin: '0 0 10px 0',
    color: 'var(--highlight)',
    fontSize: '15px',
    fontWeight: 'bold',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  newsItem: {
    padding: '10px 12px',
    marginBottom: '8px',
    background: '#1a1a1a',
    borderRadius: '6px',
    borderLeft: '3px solid #03dac6'
  },
  externalLink: {
    display: 'inline-block',
    padding: '8px 16px',
    background: '#03dac6',
    color: '#000',
    textDecoration: 'none',
    borderRadius: '6px',
    fontWeight: 'bold',
    fontSize: '13px',
    transition: 'all 0.2s'
  }
};
