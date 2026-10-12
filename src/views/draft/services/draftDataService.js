import { supabase, DEFAULT_SUPABASE_URL, DEFAULT_SUPABASE_ANON_KEY } from '../../../supabaseClient';

// --- CONFIGURATION ---
const GCS_BUCKET = "https://storage.googleapis.com/fantasy-draft-2026";
const DRAFT_DB_NAME = 'fantasy-draft-cache';
const DRAFT_DB_VERSION = 1;
const DRAFT_STORE_NAME = 'data';
let draftDbInstance = null;

export async function getDraftDb() {
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

export async function draftDbGet(key) {
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

export async function draftDbSet(key, value) {
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
export async function fetchFromGCS(filename, cacheKey) {
  try {
    const cached = await draftDbGet(cacheKey);
    if (cached) {
      const { data, timestamp } = cached;
      if (Date.now() - timestamp < 3600000) {
        return data;
      }
    }
    const res = await fetch(`${GCS_BUCKET}/${filename}`);
    if (res.status === 404) {
      console.warn(`⚠️ File not found: ${filename} (this may be expected if file hasn't been created yet)`);
      return [];
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    await draftDbSet(cacheKey, { data, timestamp: Date.now() });
    return data;
  } catch (err) {
    console.error(`❌ Error fetching ${filename}:`, err);
    try {
      const stale = await draftDbGet(cacheKey);
      if (stale && stale.data) {
        return stale.data;
      }
    } catch {
      /* ignore cache lookup error on network failure */
    }
    return [];
  }
}

// Edge function caller for FanGraphs projections
export async function fetchFanGraphs(mode) {
  try {
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
export async function callESPNProxy(body) {
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

export async function fetchAllEspnPlayers() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 120000);
  try {
    const res = await callESPNProxy({ mode: 'player_info' });
    clearTimeout(timer);
    if (res?.players) {
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

// Helper to fetch live player news from ESPN (syndicated from RotoWire) with IndexedDB caching & fallback
export async function fetchPlayerNews(playerId) {
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
