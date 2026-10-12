import { useEffect, useState } from 'react';
import styles from '../styles/draftRoomStyles';
import { getPlayerHeadshotUrl, handleHeadshotError } from '../../../utils/headshotUtils';
import { fetchPlayerNews, fetchFromGCS } from '../services/draftDataService';
import {
  normalizeName,
  getEffectiveQS,
  mappedDraft2026History,
  getFriendlyOwnerName
} from '../utils/draftCalculations';
import defaultCalculations from '../../../data/keeperCalculations.json';

export default function PlayerModal({
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
        { label: 'Exit Velo', key: 'exit_velocity_avg', desc: false },
        { label: 'Hard Hit %', key: 'hard_hit_percent', desc: false },
        { label: 'Barrel %', key: 'barrel_batted_rate', desc: false },
        { label: 'Whiff %', key: 'whiff_percent', desc: true },
        { label: 'K %', key: 'k_percent', desc: true },
        { label: 'BB %', key: 'bb_percent', desc: false }
      ]
    : [
        { label: 'xwOBA', key: 'xwoba', desc: true },
        { label: 'xBA', key: 'xba', desc: true },
        { label: 'xSLG', key: 'xslg', desc: true },
        { label: 'Exit Velo', key: 'exit_velocity_avg', desc: true },
        { label: 'Hard Hit %', key: 'hard_hit_percent', desc: true },
        { label: 'Barrel %', key: 'barrel_batted_rate', desc: true },
        { label: 'Whiff %', key: 'whiff_percent', desc: false },
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
                          <td style={statTableStyles.td}>{getEffectiveQS(player) || player.ZIPSQS || '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSERA ? parseFloat(player.ZIPSERA).toFixed(2) : '-'}</td>
                          <td style={statTableStyles.td}>{player.ZIPSWHIP ? parseFloat(player.ZIPSWHIP).toFixed(2) : '-'}</td>
                          <td style={statTableStyles.td}>{player['ZIPSSV+HDs'] || '-'}</td>
                        </tr>
                        <tr style={{ background: 'rgba(3, 218, 198, 0.1)' }}>
                          <td style={{ ...statTableStyles.td, fontWeight: 'bold', color: '#03dac6' }}>ESPN</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_IP ? (playerInfo.ESPN_IP / 3).toFixed(1) : (player.ESPNIP || '-')}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_K || player.ESPNK || '-'}</td>
                          <td style={statTableStyles.td}>{playerInfo?.ESPN_QS || player.ESPNQS || getEffectiveQS(player) || '-'}</td>
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
                const heftySS = player['Hefty Single Season Rank'] || player.overall_rank;
                const heftyKeeper = player['Hefty Keeper Rank'] || player.overall_rank;
                const espnSS = player['ESPN Single Season Rank'] || player['ESPN ROTO Rank'] || player.espn_rank;
                const espnKeeper = player['ESPN Keeper Rank'] || player.espn_rank;
                const heftyPrice = player['Hefty Keeper Price'] ?? player.overall_price ?? player['Hefty Single Season Price'] ?? player.price;
                const espnPrice = player['ESPN Price'] ?? player.espn_price;

                if (![heftySS, heftyKeeper, espnSS, espnKeeper, heftyPrice, espnPrice].some(v => v != null && v !== '')) {
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
                  const str = String(rank ?? '').trim();
                  const isPrice = str.startsWith('$');
                  const val = parseInt(str.replace(/[^0-9]/g, ''));
                  const displayRank = isNaN(val) ? '—' : (isPrice ? `$${val}` : `#${val}`);
                  const rankColor = isNaN(val) ? '#444' : (isPrice ? '#ffc107' : getRankColor(val));
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
                      Rankings & Valuations
                    </div>

                    {(heftyPrice !== undefined || espnPrice !== undefined) && (
                      <>
                        <div style={{ fontSize: '10px', color: '#666', textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: '5px' }}>
                          Auction Valuation
                        </div>
                        <div style={{ display: 'flex', gap: '8px', marginBottom: '10px' }}>
                          <RankBadge label="Hefty $" rank={heftyPrice !== undefined && heftyPrice !== null && heftyPrice !== '' ? `$${heftyPrice}` : '—'} accent="#ffc107" />
                          <RankBadge label="ESPN $" rank={espnPrice !== undefined && espnPrice !== null && espnPrice !== '' ? `$${espnPrice}` : '—'} accent="#f4891f" />
                        </div>
                      </>
                    )}

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

              {/* 3-Year Keeper Outlook & Category Profile Section */}
              {(() => {
                const playerCalc = (defaultCalculations?.players || []).find(cp => {
                  const cpPid = String(cp.espn_player_id || cp.player_id || '');
                  if (playerId && cpPid === playerId) return true;
                  if (mlbamId && String(cp.mlbam_id || cp.MLBAMID || '') === mlbamId) return true;
                  const cpName = normalizeName(cp.player_name || '');
                  const plName = normalizeName(player.Player || player.Name || '');
                  return cpName && plName && cpName === plName;
                });

                const y1Pr = playerCalc?.y1?.pr ?? (parseFloat(player['Projected PR']) || null);
                const y2Pr = playerCalc?.y2?.pr ?? null;
                const y3Pr = playerCalc?.y3?.pr ?? null;
                const surplus = playerCalc?.surplus_value ?? (player['Surplus Value'] !== undefined ? parseFloat(player['Surplus Value']) : null);

                // Category Badges
                const hr = parseFloat(player.ZIPSHR || player.ESPNHR || 0);
                const sb = parseFloat(player.ZIPSSB || player.ESPNSB || 0);
                const obp = parseFloat(player.ZIPSOBP || player.ESPNOBP || 0);
                const k = parseFloat(player.ZIPSK || player.ESPNK || 0);
                const qs = getEffectiveQS(player);
                const era = parseFloat(player.ZIPSERA || player.ESPNERA || 0);
                const whip = parseFloat(player.ZIPSWHIP || player.ESPNWHIP || 0);
                const svh = parseFloat(player['ZIPSSV+HDs'] || player['ESPNSV+HDs'] || 0);

                const badges = [];
                if (!isPitcherPlayer) {
                  if (hr >= 30) badges.push({ text: `🔥 ${Math.round(hr)} HR Power`, color: '#ef4444' });
                  if (sb >= 20) badges.push({ text: `💨 ${Math.round(sb)} SB Speed`, color: '#38bdf8' });
                  if (obp >= 0.365) badges.push({ text: `🎯 .${String(obp.toFixed(3)).replace('0.', '')} OBP Anchor`, color: '#10b981' });
                } else {
                  if (k >= 195) badges.push({ text: `👑 ${Math.round(k)} K Ace`, color: '#a855f7' });
                  if (qs >= 18) badges.push({ text: `💎 ${Math.round(qs)} Quality Starts`, color: '#03dac6' });
                  if (era > 0 && era <= 3.35) badges.push({ text: `🔒 ${era.toFixed(2)} ERA Anchor`, color: '#10b981' });
                  if (whip > 0 && whip <= 1.10) badges.push({ text: `🎯 ${whip.toFixed(2)} WHIP`, color: '#6366f1' });
                  if (svh >= 18) badges.push({ text: `🛡️ ${Math.round(svh)} SV+HD Anchor`, color: '#f59e0b' });
                }

                return (
                  <div style={{ marginBottom: '20px' }}>
                    {/* Category Strengths Badges */}
                    {badges.length > 0 && (
                      <div style={{ marginBottom: '14px' }}>
                        <div style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '6px' }}>
                          Category Profile
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                          {badges.map((b, i) => (
                            <span key={i} style={{
                              fontSize: '11px',
                              fontWeight: 'bold',
                              padding: '3px 8px',
                              borderRadius: '4px',
                              background: 'rgba(255, 255, 255, 0.05)',
                              border: `1px solid ${b.color}`,
                              color: b.color
                            }}>
                              {b.text}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* 3-Year Keeper Trajectory */}
                    {(y1Pr !== null || y2Pr !== null || y3Pr !== null) && (
                      <div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                          <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', textTransform: 'uppercase', letterSpacing: '1px' }}>
                            3-Year Keeper Outlook
                          </span>
                          {surplus !== null && !isNaN(surplus) && (
                            <span style={{
                              fontSize: '11px',
                              fontWeight: 'bold',
                              color: surplus >= 0 ? '#10b981' : '#f43f5e',
                              background: surplus >= 0 ? 'rgba(16, 185, 129, 0.12)' : 'rgba(244, 63, 94, 0.12)',
                              padding: '1px 6px',
                              borderRadius: '4px',
                              border: `1px solid ${surplus >= 0 ? 'rgba(16, 185, 129, 0.3)' : 'rgba(244, 63, 94, 0.3)'}`
                            }}>
                              {surplus >= 0 ? `+${surplus} Surplus` : `${surplus} Surplus`}
                            </span>
                          )}
                        </div>

                        <div style={{
                          display: 'grid',
                          gridTemplateColumns: '1fr 1fr 1fr',
                          gap: '6px',
                          background: '#252525',
                          padding: '8px',
                          borderRadius: '6px',
                          border: '1px solid #333'
                        }}>
                          <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '10px', color: '#888' }}>2026 (Y1)</div>
                            <div style={{ fontSize: '13px', fontWeight: 'bold', color: '#bb86fc' }}>
                              {y1Pr !== null ? `${y1Pr > 0 ? '+' : ''}${parseFloat(y1Pr).toFixed(1)} PR` : '—'}
                            </div>
                            {playerCalc?.y1?.rank && (
                              <div style={{ fontSize: '9px', color: '#aaa' }}>#{playerCalc.y1.rank}</div>
                            )}
                          </div>
                          <div style={{ textAlign: 'center', borderLeft: '1px solid #383838', borderRight: '1px solid #383838' }}>
                            <div style={{ fontSize: '10px', color: '#888' }}>2027 (Y2)</div>
                            <div style={{ fontSize: '13px', fontWeight: 'bold', color: '#03dac6' }}>
                              {y2Pr !== null ? `${y2Pr > 0 ? '+' : ''}${parseFloat(y2Pr).toFixed(1)} PR` : '—'}
                            </div>
                            {playerCalc?.y2?.rank && (
                              <div style={{ fontSize: '9px', color: '#aaa' }}>#{playerCalc.y2.rank}</div>
                            )}
                          </div>
                          <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '10px', color: '#888' }}>2028 (Y3)</div>
                            <div style={{ fontSize: '13px', fontWeight: 'bold', color: '#ffb74d' }}>
                              {y3Pr !== null ? `${y3Pr > 0 ? '+' : ''}${parseFloat(y3Pr).toFixed(1)} PR` : '—'}
                            </div>
                            {playerCalc?.y3?.rank && (
                              <div style={{ fontSize: '9px', color: '#aaa' }}>#{playerCalc.y3.rank}</div>
                            )}
                          </div>
                        </div>

                        {y1Pr !== null && y2Pr !== null && (
                          <div style={{ fontSize: '10px', color: '#888', marginTop: '6px', textAlign: 'center' }}>
                            {y2Pr >= y1Pr ? '🚀 Rising Dynasty Value' : (y1Pr - y3Pr > 4 ? '⏳ Near-Term Window Focus' : '🛡️ Consistent Multi-Year Core')}
                          </div>
                        )}
                      </div>
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
