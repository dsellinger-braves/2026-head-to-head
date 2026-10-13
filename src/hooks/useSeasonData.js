/**
 * @file useSeasonData.js
 * @description Custom hook orchestrating multi-season data fetching from Supabase,
 * IndexedDB client-side caching (idb-keyval), pagination batching, deduplication,
 * and background season loading.
 */

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { supabase } from '../supabaseClient';
import { get, set, del } from 'idb-keyval';
import { AVAILABLE_SEASONS } from '../routes';
import { LEAGUE_ID } from '../constants/league';

/**
 * Normalizes flat Supabase historical_data CSV records to match player_daily_stats schema.
 */
export function normalizeHistoricalRecord(row) {
  const stats = {};
  for (const [k, v] of Object.entries(row)) {
    // ESPN stat IDs are purely numeric column names ('5', '20', '34', …).
    if (/^\d+$/.test(k) && v !== null && v !== '' && v !== undefined) {
      stats[k] = v;
    }
  }
  return {
    season_year: row.season_year,
    team_id: row.team_id,
    scoring_period_id: row.scoring_period_id,
    player_id: row.id,
    full_name: row.fullName,
    lineup_slot_id: row.lineupSlotID != null ? parseInt(row.lineupSlotID, 10) : null,
    stats,
  };
}

export function useSeasonData(selectedSeason = 2026) {
  const [rawData, setRawData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [loadStatus, setLoadStatus] = useState('Initializing...');
  const [allSeasonData, setAllSeasonData] = useState({});
  const [downloadAllProgress, setDownloadAllProgress] = useState(null);
  const fetchingRef = useRef(false);

  /**
   * Fetches raw records for a specific season with multi-tier cache & paginated query.
   */
  const fetchRawSeason = useCallback(async (season, onProgress) => {
    if (season > 2026) {
      return [];
    }
    const cacheKey = `fantasy_data_v2_${season}`;
    try {
      const cachePromise = get(cacheKey);
      const cacheTimeout = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('IndexedDB read timeout')), 2500)
      );
      const cached = await Promise.race([cachePromise, cacheTimeout]);
      if (cached?.length > 0) return cached;
    } catch (cacheErr) {
      console.warn('Local cache read notice or timeout:', cacheErr);
    }

    const isHistorical = season < 2026;
    const tableName = isHistorical ? 'historical_data' : 'player_daily_stats';
    const pageSize = 1000;

    let countQuery = supabase
      .from(tableName)
      .select('*', { count: 'exact', head: true });

    if (!isHistorical) {
      countQuery = countQuery.eq('league_id', LEAGUE_ID);
    } else {
      countQuery = countQuery.eq('league_id', LEAGUE_ID).eq('season_year', season);
    }

    const { count, error: countErr } = await countQuery;
    if (countErr) throw countErr;

    const totalPages = Math.max(1, Math.ceil((count || 0) / pageSize));
    const allRecords = [];
    const batchSize = 6;

    for (let b = 0; b < totalPages; b += batchSize) {
      const promises = [];
      for (let p = b; p < Math.min(b + batchSize, totalPages); p++) {
        let q = supabase
          .from(tableName)
          .select('*')
          .range(p * pageSize, (p + 1) * pageSize - 1);

        if (!isHistorical) {
          q = q.eq('league_id', LEAGUE_ID).order('id', { ascending: true });
        } else {
          q = q.eq('league_id', LEAGUE_ID).eq('season_year', season).order('_db_id', { ascending: true });
        }
        promises.push(q);
      }

      const results = await Promise.all(promises);
      for (const res of results) {
        if (res.error) throw res.error;
        if (res.data?.length > 0) {
          allRecords.push(...res.data);
        }
      }
      onProgress?.(`Downloading ${season}… (${Math.min(allRecords.length, count || allRecords.length)} of ${count || allRecords.length} records)`);
    }

    const normalized = isHistorical
      ? allRecords.map(normalizeHistoricalRecord)
      : allRecords;

    // Remove duplicate rows: season + team + period + player + name
    const seen = new Set();
    const deduped = normalized.filter(r => {
      const k = `${r.season_year}__${r.team_id}__${r.scoring_period_id}__${r.player_id}__${r.full_name}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });

    try {
      await set(cacheKey, deduped);
    } catch (cacheWriteErr) {
      console.warn('Local cache write notice:', cacheWriteErr);
    }

    return deduped;
  }, []);

  const fetchAllData = useCallback(async (season) => {
    if (fetchingRef.current) return;
    fetchingRef.current = true;
    try {
      setLoading(true);
      setLoadStatus('Checking local cache...');
      const records = await fetchRawSeason(season, setLoadStatus);
      setRawData(records || []);
    } catch (err) {
      console.error('App Error:', err);
      try {
        const cachePromise = get(`fantasy_data_${season}`);
        const cacheTimeout = new Promise((_, reject) =>
          setTimeout(() => reject(new Error('IndexedDB recovery timeout')), 2000)
        );
        const cached = await Promise.race([cachePromise, cacheTimeout]);
        if (cached?.length > 0) {
          console.warn(`Recovered ${cached.length} records from local cache following server error`);
          setRawData(cached);
          return;
        }
      } catch (cacheErr) {
        console.error('Cache recovery failed:', cacheErr);
      }
      setLoadStatus('Notice: In-season statistics could not be loaded.');
    } finally {
      fetchingRef.current = false;
      setLoading(false);
    }
  }, [fetchRawSeason]);

  // Load a single season on-demand into allSeasonData without switching active view
  const loadSeason = useCallback(async (season) => {
    if (allSeasonData[season]?.length) return allSeasonData[season];
    try {
      const data = await fetchRawSeason(season);
      if (data?.length > 0) {
        setAllSeasonData(prev => ({ ...prev, [season]: data }));
        return data;
      }
    } catch (e) {
      console.error(`Failed to load season ${season}:`, e);
    }
    return [];
  }, [allSeasonData, fetchRawSeason]);

  // Download all historical seasons in background
  const downloadAllSeasons = useCallback(async () => {
    const missing = AVAILABLE_SEASONS.filter(y => y <= 2026 && !allSeasonData[y]?.length);
    if (!missing.length) return;
    setDownloadAllProgress({ done: 0, total: missing.length, current: null });
    for (let i = 0; i < missing.length; i++) {
      const year = missing[i];
      setDownloadAllProgress({ done: i, total: missing.length, current: year });
      try {
        const data = await fetchRawSeason(year);
        if (data?.length > 0) {
          setAllSeasonData(prev => ({ ...prev, [year]: data }));
        }
      } catch (e) {
        console.error(`Failed to load ${year}:`, e);
      }
    }
    setDownloadAllProgress(null);
  }, [allSeasonData, fetchRawSeason]);

  // Force-refresh active data by clearing client IndexedDB caches
  const refreshSeasonData = useCallback(async () => {
    await Promise.all(AVAILABLE_SEASONS.map(y => Promise.all([del(`fantasy_data_${y}`), del(`fantasy_data_v2_${y}`)])));
    setAllSeasonData({});
    await fetchAllData(selectedSeason);
  }, [selectedSeason, fetchAllData]);

  // Trigger fetch when selectedSeason changes
  useEffect(() => {
    fetchAllData(selectedSeason);
  }, [selectedSeason, fetchAllData]);

  // Accumulate loaded active season into allSeasonData
  useEffect(() => {
    if (rawData.length > 0) {
      setAllSeasonData(prev => ({ ...prev, [selectedSeason]: rawData }));
    }
  }, [rawData, selectedSeason]);

  // Latest scoring period records
  const todaysRecords = useMemo(() => {
    if (!rawData.length) return [];
    let maxPeriodId = -1;
    for (let i = 0; i < rawData.length; i++) {
      if (rawData[i].scoring_period_id > maxPeriodId) {
        maxPeriodId = rawData[i].scoring_period_id;
      }
    }
    return rawData.filter(r => r.scoring_period_id === maxPeriodId);
  }, [rawData]);

  return {
    rawData,
    setRawData,
    loading,
    loadStatus,
    allSeasonData,
    downloadAllProgress,
    todaysRecords,
    fetchAllData,
    fetchRawSeason,
    loadSeason,
    downloadAllSeasons,
    refreshSeasonData,
  };
}
