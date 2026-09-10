import { useMemo, useState, useEffect, Fragment } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { SCORING_CATS, LINEUP_SLOTS, aggregateStats } from '../utils/scoring';
import { supabase } from '../supabaseClient';
import TeamAvatar from './TeamAvatar';

// In-memory cache for full MLB season stats to avoid redundant network calls
const overallStatsCache = new Map();

async function fetchPlayerOverallStats(playerName, isPitcher, seasonYear = 2026, cutoffDate = null) {
  const cacheKey = `${playerName}__${isPitcher ? 'P' : 'B'}__${seasonYear}__${cutoffDate || 'full'}`;
  if (overallStatsCache.has(cacheKey)) {
    return overallStatsCache.get(cacheKey);
  }

  try {
    // 1. First attempt: Official MLB Stats API (ultra-fast, official box scores)
    const searchRes = await fetch(`https://statsapi.mlb.com/api/v1/people/search?names=${encodeURIComponent(playerName)}`);
    if (searchRes.ok) {
      const searchJson = await searchRes.json();
      const person = searchJson.people?.[0];
      if (person?.id) {
        const group = isPitcher ? 'pitching' : 'hitting';
        const statsUrl = cutoffDate
          ? `https://statsapi.mlb.com/api/v1/people/${person.id}/stats?stats=byDateRange&season=${seasonYear}&startDate=2026-03-25&endDate=${cutoffDate}&group=${group}`
          : `https://statsapi.mlb.com/api/v1/people/${person.id}/stats?stats=season&season=${seasonYear}&group=${group}`;
        const statsRes = await fetch(statsUrl);
        if (statsRes.ok) {
          const statsJson = await statsRes.json();
          const stat = statsJson.stats?.[0]?.splits?.[0]?.stat;
          if (stat) {
            let res;
            if (isPitcher) {
              const outs = stat.outs || stat.outsPitched || 0;
              const ipFloat = outs ? outs / 3 : (parseFloat(stat.inningsPitched) || 0);
              res = {
                source: 'MLB Stats API',
                games: stat.gamesPlayed || 0,
                GS: stat.gamesStarted || 0,
                IP: ipFloat,
                ER: stat.earnedRuns || 0,
                K: stat.strikeOuts || 0,
                QS: stat.qualityStarts !== undefined ? stat.qualityStarts : 0,
                SV: stat.saves || 0,
                HD: stat.holds || 0,
                'SV+HDs': (stat.saves || 0) + (stat.holds || 0),
                H_Allowed: stat.hits || 0,
                BB_Allowed: stat.baseOnBalls || 0,
                ERA: parseFloat(stat.era) || 0,
                WHIP: parseFloat(stat.whip) || 0,
              };
            } else {
              res = {
                source: 'MLB Stats API',
                games: stat.gamesPlayed || 0,
                PA: stat.plateAppearances || 0,
                AB: stat.atBats || 0,
                H: stat.hits || 0,
                R: stat.runs || 0,
                HR: stat.homeRuns || 0,
                RBI: stat.rbi || 0,
                SB: stat.stolenBases || 0,
                BB: stat.baseOnBalls || 0,
                HBP: stat.hitByPitch || 0,
                OBP: parseFloat(stat.obp) || 0,
                AVG: parseFloat(stat.avg) || 0,
              };
            }
            overallStatsCache.set(cacheKey, res);
            return res;
          }
        }
      }
    }
  } catch (e) {
    console.warn('MLB Stats API lookup failed, trying fallback:', e);
  }

  // 2. Second attempt: FanGraphs Edge Function fallback
  try {
    const mode = isPitcher ? 'pitching' : 'batting';
    const { data } = await supabase.functions.invoke('fangraphs', { body: { mode } });
    if (data?.players?.length > 0) {
      const cleanName = playerName.toLowerCase().replace(/[^a-z]/g, '');
      const match = data.players.find(p => (p.PlayerName || '').toLowerCase().replace(/[^a-z]/g, '') === cleanName);
      if (match) {
        let res;
        if (isPitcher) {
          res = {
            source: 'FanGraphs',
            games: match.G || 0,
            GS: match.GS || 0,
            IP: parseFloat(match.IP) || 0,
            ER: match.ER || 0,
            K: match.SO || 0,
            QS: match.QS || 0,
            SV: match.SV || 0,
            HD: match.HLD || 0,
            'SV+HDs': (match.SV || 0) + (match.HLD || 0),
            H_Allowed: match.H || 0,
            BB_Allowed: match.BB || 0,
            ERA: parseFloat(match.ERA) || 0,
            WHIP: parseFloat(match.WHIP) || 0,
          };
        } else {
          res = {
            source: 'FanGraphs',
            games: match.G || 0,
            PA: match.PA || 0,
            AB: match.AB || 0,
            H: match.H || 0,
            R: match.R || 0,
            HR: match.HR || 0,
            RBI: match.RBI || 0,
            SB: match.SB || 0,
            BB: match.BB || 0,
            OBP: parseFloat(match.OBP) || 0,
            AVG: parseFloat(match.AVG) || 0,
          };
        }
        overallStatsCache.set(cacheKey, res);
        return res;
      }
    }
  } catch (fgErr) {
    console.warn('FanGraphs fallback failed:', fgErr);
  }

  overallStatsCache.set(cacheKey, null);
  return null;
}

export default function PlayerHistoryModal({ playerId, playerName, allStats, selectedSeason = 2026, onClose }) {
  const [selectedTeamId, setSelectedTeamId] = useState(null);
  const [gameLogSlotFilter, setGameLogSlotFilter] = useState('ALL'); // 'ALL' | 'STARTER' | 'BENCH'
  const [overallStats, setOverallStats] = useState(null);
  const [loadingOverall, setLoadingOverall] = useState(false);

  // 1. Helper: Determine if a record is an appearance (played on active roster or bench)
  const isActiveAppearance = (record) => {
    const s = record.stats || {};
    return (
      parseFloat(s['16']) > 0 || parseFloat(s.PA) > 0 ||
      parseFloat(s['0']) > 0 || parseFloat(s['2']) > 0 || parseFloat(s.AB) > 0 ||
      parseFloat(s['34']) > 0 || parseFloat(s.IP) > 0 || parseFloat(s.IP_OUTS) > 0 ||
      parseFloat(s['48']) > 0 || parseFloat(s.K) > 0 ||
      parseFloat(s['57']) > 0 || parseFloat(s.SV) > 0 ||
      parseFloat(s['60']) > 0 || parseFloat(s.HD) > 0 ||
      parseFloat(s['5']) > 0 || parseFloat(s.HR) > 0 ||
      parseFloat(s['20']) > 0 || parseFloat(s.R) > 0 ||
      parseFloat(s['21']) > 0 || parseFloat(s.RBI) > 0 ||
      parseFloat(s['23']) > 0 || parseFloat(s.SB) > 0 ||
      parseFloat(s['10']) > 0 || parseFloat(s.BB) > 0 ||
      parseFloat(s['45']) > 0 || parseFloat(s.ER) > 0
    );
  };

  // 2. Get records, filter, and sort
  const fullGameLog = useMemo(() => {
    return allStats
      .filter(r => r.player_id === playerId)
      .filter(isActiveAppearance)
      .sort((a, b) => b.scoring_period_id - a.scoring_period_id);
  }, [allStats, playerId]);

  // 3. Auto-detect Position
  const isPitcher = useMemo(() => {
    return fullGameLog.some(r => {
      const s = r.stats || {};
      return (
        (s.IP && parseFloat(s.IP) > 0) ||
        (s.IP_OUTS && s.IP_OUTS > 0) ||
        (s['34'] && parseFloat(s['34']) > 0) ||
        (s.K && parseFloat(s.K) > 0) ||
        (s['48'] && parseFloat(s['48']) > 0) ||
        (s.ER !== undefined && parseFloat(s.ER) > 0) ||
        (r.lineup_slot_id >= 13 && r.lineup_slot_id <= 15)
      );
    });
  }, [fullGameLog]);

  const displayMode = isPitcher ? 'pitching' : 'batting';
  
  const batCats = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP'];
  const pitchCats = ['IP', 'ER', 'K', 'QS', 'QS_PCT', 'SV+HDs', 'ERA', 'WHIP'];
  
  const displayCats = displayMode === 'batting' ? batCats : pitchCats;

  const seasonYear = selectedSeason || fullGameLog[0]?.season_year || 2026;

  // Completed games cutoff: For active 2026 season, synchronize through yesterday's completed games
  // to avoid attributing in-progress or same-day unfinalized games to unrostered performance.
  const cutoffDate = useMemo(() => {
    if (seasonYear < 2026) return null;
    const now = new Date();
    const y = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
    return `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, '0')}-${String(y.getDate()).padStart(2, '0')}`;
  }, [seasonYear]);

  // Fetch full-season MLB stats (FanGraphs / MLB Stats API) to calculate unrostered production
  useEffect(() => {
    let isCancelled = false;
    async function loadOverall() {
      if (!playerName) return;
      setLoadingOverall(true);
      const res = await fetchPlayerOverallStats(playerName, isPitcher, seasonYear, cutoffDate);
      if (!isCancelled) {
        setOverallStats(res);
        setLoadingOverall(false);
      }
    }
    loadOverall();
    return () => { isCancelled = true; };
  }, [playerName, isPitcher, seasonYear, cutoffDate]);

  // 4. Build Owner Summary with Starter vs Bench Splits
  const ownerSummary = useMemo(() => {
    const groups = {};
    
    fullGameLog.forEach(r => {
      const tid = r.team_id;
      if (!groups[tid]) {
        groups[tid] = {
          teamId: tid,
          teamName: TEAMS[tid]?.name || `Team ${tid}`,
          allRecords: [],
          activeRecords: [],
          benchRecords: [],
        };
      }
      groups[tid].allRecords.push(r);
      if (r.lineup_slot_id === 16 || r.lineup_slot_id === 17) {
        groups[tid].benchRecords.push(r);
      } else {
        groups[tid].activeRecords.push(r);
      }
    });

    return Object.values(groups).map(g => {
      const totalStats = aggregateStats(g.allRecords, { includeAll: true });
      const starterStats = aggregateStats(g.activeRecords);
      const benchStats = aggregateStats(g.benchRecords, { includeBenchOnly: true });

      return {
        teamId: g.teamId,
        teamName: g.teamName,
        total: {
          label: `${g.teamName}`,
          splitType: 'total',
          games: g.allRecords.length,
          stats: totalStats,
          records: g.allRecords,
        },
        starter: {
          label: `${g.teamName} (as a starter)`,
          splitType: 'starter',
          games: g.activeRecords.length,
          stats: starterStats,
          records: g.activeRecords,
        },
        bench: {
          label: `${g.teamName} (on bench)`,
          splitType: 'bench',
          games: g.benchRecords.length,
          stats: benchStats,
          records: g.benchRecords,
        },
      };
    }).sort((a, b) => b.total.games - a.total.games);
  }, [fullGameLog]);

  // 5. Calculate unrostered line: Total MLB Season Stats minus Total Rostered Stats
  const unrosteredRow = useMemo(() => {
    if (!overallStats) return null;

    // Filter fullGameLog to completed games up to cutoffDate
    const completedLog = (seasonYear === 2026 && cutoffDate)
      ? fullGameLog.filter(r => getDateFromPeriodId(r.scoring_period_id, seasonYear) <= cutoffDate)
      : fullGameLog;

    // Total rostered stats across all fantasy teams (active + bench)
    const totalRostered = aggregateStats(completedLog, { includeAll: true });
    const rosteredGames = completedLog.length;

    const unrosteredGames = Math.max(0, (overallStats.games || 0) - rosteredGames);

    if (isPitcher) {
      const mlbIp = overallStats.IP || 0;
      const rosIp = totalRostered.IP || 0;
      const unrosteredIp = Math.max(0, mlbIp - rosIp);
      const unrosteredEr = Math.max(0, (overallStats.ER || 0) - (totalRostered.ER || 0));
      const unrosteredK = Math.max(0, (overallStats.K || 0) - (totalRostered.K || 0));
      const unrosteredSvHd = Math.max(0, (overallStats['SV+HDs'] || 0) - (totalRostered['SV+HDs'] || 0));
      const unrosteredH = Math.max(0, (overallStats.H_Allowed || 0) - (totalRostered.H_Allowed || 0));
      const unrosteredBb = Math.max(0, (overallStats.BB_Allowed || 0) - (totalRostered.BB_Allowed || 0));
      const unrosteredGs = Math.max(0, (overallStats.GS || 0) - (totalRostered.GS || 0));
      const unrosteredQs = Math.max(0, (overallStats.QS || 0) - (totalRostered.QS || 0));

      const era = unrosteredIp > 0 ? ((unrosteredEr * 9) / unrosteredIp).toFixed(2) : '-';
      const whip = unrosteredIp > 0 ? ((unrosteredBb + unrosteredH) / unrosteredIp).toFixed(2) : '-';
      const qsPct = unrosteredGs > 0 ? ((unrosteredQs / unrosteredGs) * 100).toFixed(1) + '%' : '-';

      return {
        games: unrosteredGames,
        stats: {
          IP: unrosteredIp,
          ER: unrosteredEr,
          K: unrosteredK,
          QS: unrosteredQs,
          QS_PCT: qsPct,
          'SV+HDs': unrosteredSvHd,
          ERA: era,
          WHIP: whip,
        },
      };
    } else {
      const mlbPa = overallStats.PA || 0;
      const rosPa = totalRostered.PA || 0;
      const unrosteredPa = Math.max(0, mlbPa - rosPa);
      const unrosteredR = Math.max(0, (overallStats.R || 0) - (totalRostered.R || 0));
      const unrosteredHr = Math.max(0, (overallStats.HR || 0) - (totalRostered.HR || 0));
      const unrosteredRbi = Math.max(0, (overallStats.RBI || 0) - (totalRostered.RBI || 0));
      const unrosteredSb = Math.max(0, (overallStats.SB || 0) - (totalRostered.SB || 0));
      const unrosteredH = Math.max(0, (overallStats.H || 0) - (totalRostered.H || 0));
      const unrosteredBb = Math.max(0, (overallStats.BB || 0) - (totalRostered.BB || 0));
      const unrosteredHbp = Math.max(0, (overallStats.HBP || 0) - (totalRostered.HBP || 0));

      let obp = '-';
      if (unrosteredPa > 0) {
        const rawObp = Math.min(1.0, (unrosteredH + unrosteredBb + unrosteredHbp) / unrosteredPa);
        obp = rawObp.toFixed(3).replace(/^0/, '');
      }

      return {
        games: unrosteredGames,
        stats: {
          PA: unrosteredPa,
          R: unrosteredR,
          HR: unrosteredHr,
          RBI: unrosteredRbi,
          SB: unrosteredSb,
          OBP: obp,
        },
      };
    }
  }, [overallStats, fullGameLog, isPitcher, seasonYear, cutoffDate]);

  // 6. Helper for formatting
  const formatStat = (val, catKey, games = 1, statsObj = null) => {
    if (games === 0) return '-';
    if (val === undefined || val === null) return '-';
    if (catKey === 'IP') {
      const ip = parseFloat(val) || 0;
      if (ip === 0 && games === 0) return '-';
      return `${Math.floor(ip)}.${Math.round((ip % 1) * 3)}`;
    }
    if (catKey === 'ERA' || catKey === 'WHIP') {
      if (statsObj && (parseFloat(statsObj.IP) || 0) === 0) return '-';
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(2);
    }
    if (catKey === 'OBP') {
      if (statsObj && (parseFloat(statsObj.PA) || 0) === 0) return '-';
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(3).replace(/^0/, '');
    }
    if (catKey === 'QS_PCT') {
      if (statsObj && (parseFloat(statsObj.GS) || 0) === 0) return '-';
      const n = parseFloat(val);
      return isNaN(n) ? '-' : n.toFixed(1) + '%';
    }
    if (SCORING_CATS[catKey]?.isRate) {
      const num = parseFloat(val);
      return isNaN(num) ? '-' : num.toFixed(3).replace(/^0+/, '');
    }
    return val;
  };

  const getLabel = (col) => {
    if (col === 'QS_PCT') return 'QS%';
    return SCORING_CATS[col]?.label || col;
  };

  // 7. Filter for Detail View
  const teamAllRecords = useMemo(() => {
    if (!selectedTeamId) return [];
    return fullGameLog.filter(r => r.team_id === parseInt(selectedTeamId));
  }, [fullGameLog, selectedTeamId]);

  const activeRecords = useMemo(() => {
    return teamAllRecords.filter(r => {
      if (gameLogSlotFilter === 'STARTER') return r.lineup_slot_id !== 16 && r.lineup_slot_id !== 17;
      if (gameLogSlotFilter === 'BENCH') return r.lineup_slot_id === 16 || r.lineup_slot_id === 17;
      return true;
    });
  }, [teamAllRecords, gameLogSlotFilter]);
  
  const activeTeamName = selectedTeamId ? (TEAMS[selectedTeamId]?.name || 'Unknown') : '';

  // Close modal on ESC key press
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  return (
    <div 
      onClick={(e) => e.target === e.currentTarget && onClose()}
      className="fixed inset-0 bg-black/70 flex items-center justify-center z-[60] p-4 backdrop-blur-sm animate-fade-in"
    >
      <div className="bg-white rounded-lg shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden">
        
        {/* --- HEADER --- */}
        <div className="bg-blue-900 text-white p-4 flex justify-between items-center shadow-md shrink-0">
          <div className="flex items-center gap-4">
            {selectedTeamId && (
              <button 
                onClick={() => { setSelectedTeamId(null); setGameLogSlotFilter('ALL'); }}
                className="bg-blue-800 hover:bg-blue-700 text-white px-3 py-1 rounded-full text-sm font-semibold transition-colors flex items-center gap-1 cursor-pointer"
              >
                &larr; Back
              </button>
            )}
            <div>
              <h2 className="text-xl font-bold">{playerName}</h2>
              <div className="flex items-center gap-2 mt-0.5">
                <p className="text-blue-200 text-xs uppercase tracking-wider font-semibold">
                  {selectedTeamId ? `${activeTeamName} Game Log` : `Season Summary by Owner (${seasonYear})`}
                </p>
                {selectedTeamId && gameLogSlotFilter !== 'ALL' && (
                  <span className={`text-[10px] font-black uppercase px-1.5 py-0.5 rounded ${
                    gameLogSlotFilter === 'STARTER' ? 'bg-blue-500 text-white' : 'bg-amber-500 text-black'
                  }`}>
                    {gameLogSlotFilter === 'STARTER' ? 'Starters Only' : 'Bench Only'}
                  </span>
                )}
              </div>
            </div>
          </div>
          <button onClick={onClose} className="text-blue-300 hover:text-white text-3xl leading-none font-light cursor-pointer">&times;</button>
        </div>

        {/* --- CONTENT --- */}
        <div className="overflow-y-auto p-0 flex-1 bg-gray-50">
          
          {/* VIEW 1: OWNER SUMMARY */}
          {!selectedTeamId && (
            <table className="w-full text-sm border-collapse">
              <thead className="bg-gray-100 sticky top-0 shadow-sm z-10 text-xs text-gray-500 uppercase tracking-wider font-semibold">
                <tr>
                  <th className="p-3 text-left border-b border-gray-200">Owner & Role</th>
                  <th className="p-3 text-center border-b border-gray-200 w-16">Games</th>
                  {displayCats.map(c => (
                    <th key={c} className="p-3 text-center border-b border-gray-200 min-w-[50px]">
                      {getLabel(c)}
                    </th>
                  ))}
                  <th className="p-3 border-b border-gray-200 w-24"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 bg-white">
                {ownerSummary.length === 0 ? (
                  <tr>
                    <td colSpan={displayCats.length + 3} className="p-8 text-center text-gray-400 italic">
                      No games recorded this season on any fantasy team.
                    </td>
                  </tr>
                ) : (
                  ownerSummary.map(group => (
                    <Fragment key={group.teamId}>
                      {/* 1. Overall Combined Line */}
                      <tr 
                        onClick={() => { setSelectedTeamId(group.teamId); setGameLogSlotFilter('ALL'); }}
                        className="bg-gray-50 hover:bg-blue-50/80 cursor-pointer transition-colors border-t-2 border-gray-200 group"
                        title="Click to view all game logs for this owner"
                      >
                        <td className="p-3">
                          <div className="flex items-center gap-3">
                            <TeamAvatar team={{ id: group.teamId, name: group.teamName }} size="sm" />
                            <div className="flex items-center gap-2">
                              <span className="font-bold text-gray-900">{group.teamName}</span>
                              <span className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded bg-gray-200 text-gray-700">
                                Overall
                              </span>
                            </div>
                          </div>
                        </td>
                        <td className="p-3 text-center font-black text-gray-900 font-mono">{group.total.games}</td>
                        {displayCats.map(cat => (
                          <td key={cat} className="p-3 text-center text-gray-900 font-mono font-bold">
                            {formatStat(group.total.stats[cat], cat, group.total.games, group.total.stats)}
                          </td>
                        ))}
                        <td className="p-3 text-right text-xs font-semibold text-blue-600 opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap">
                          All Logs &rarr;
                        </td>
                      </tr>

                      {/* 2. Dan (as a starter) Split Line */}
                      <tr 
                        onClick={() => { setSelectedTeamId(group.teamId); setGameLogSlotFilter('STARTER'); }}
                        className="hover:bg-blue-50/50 cursor-pointer transition-colors border-b border-gray-100 group text-xs text-gray-700 bg-white"
                        title="Click to view starter game logs"
                      >
                        <td className="py-2.5 px-3 pl-9">
                          <div className="flex items-center gap-2">
                            <span className="text-gray-400 font-mono text-sm leading-none">↳</span>
                            <span className="font-medium text-gray-800">{group.teamName}</span>
                            <span className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded bg-blue-100 text-blue-800 border border-blue-200">
                              as a starter
                            </span>
                          </div>
                        </td>
                        <td className="py-2.5 px-3 text-center font-mono font-semibold text-gray-700">{group.starter.games}</td>
                        {displayCats.map(cat => (
                          <td key={cat} className="py-2.5 px-3 text-center font-mono text-gray-700">
                            {formatStat(group.starter.stats[cat], cat, group.starter.games, group.starter.stats)}
                          </td>
                        ))}
                        <td className="py-2.5 px-3 text-right text-[11px] font-semibold text-blue-500 opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap">
                          Starter Logs &rarr;
                        </td>
                      </tr>

                      {/* 3. Dan (on bench) Split Line */}
                      <tr 
                        onClick={() => { setSelectedTeamId(group.teamId); setGameLogSlotFilter('BENCH'); }}
                        className="hover:bg-amber-50/60 cursor-pointer transition-colors border-b border-gray-200/80 group text-xs text-gray-700 bg-amber-50/20"
                        title="Click to view bench game logs"
                      >
                        <td className="py-2.5 px-3 pl-9">
                          <div className="flex items-center gap-2">
                            <span className="text-gray-400 font-mono text-sm leading-none">↳</span>
                            <span className="font-medium text-gray-800">{group.teamName}</span>
                            <span className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded bg-amber-100 text-amber-900 border border-amber-300">
                              on bench
                            </span>
                          </div>
                        </td>
                        <td className="py-2.5 px-3 text-center font-mono font-semibold text-gray-700">{group.bench.games}</td>
                        {displayCats.map(cat => (
                          <td key={cat} className="py-2.5 px-3 text-center font-mono text-gray-700">
                            {formatStat(group.bench.stats[cat], cat, group.bench.games, group.bench.stats)}
                          </td>
                        ))}
                        <td className="py-2.5 px-3 text-right text-[11px] font-semibold text-amber-700 opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap">
                          Bench Logs &rarr;
                        </td>
                      </tr>
                    </Fragment>
                  ))
                )}

                {/* 4. Unrostered / Free Agent Split Line */}
                {loadingOverall ? (
                  <tr className="bg-slate-50/50 border-t-2 border-dashed border-gray-300 text-xs text-gray-400 animate-pulse">
                    <td className="p-3 pl-4">
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center text-xs text-slate-500 font-bold">
                          FA
                        </div>
                        <div>
                          <span className="font-semibold text-slate-600">Unrostered / Free Agent</span>
                          <span className="text-[11px] text-slate-400 ml-2">(calculating net stats from full season MLB data…)</span>
                        </div>
                      </div>
                    </td>
                    <td className="p-3 text-center font-mono text-gray-400">-</td>
                    {displayCats.map(c => (
                      <td key={c} className="p-3 text-center font-mono text-gray-400">-</td>
                    ))}
                    <td className="p-3"></td>
                  </tr>
                ) : unrosteredRow ? (
                  <tr className="bg-slate-50/80 hover:bg-slate-100/70 transition-colors border-t-2 border-dashed border-gray-300 text-xs text-gray-700">
                    <td className="p-3">
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded-full bg-slate-200 border border-slate-300 flex items-center justify-center text-xs font-black text-slate-600 shadow-xs">
                          FA
                        </div>
                        <div>
                          <div className="flex items-center gap-2">
                            <span className="font-bold text-gray-900">Unrostered / Free Agent</span>
                            <span className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded bg-slate-200 text-slate-700">
                              Waivers
                            </span>
                          </div>
                          <div className="text-[11px] text-gray-400">
                            Production while not on any fantasy roster ({overallStats?.source || 'MLB/FanGraphs'}{cutoffDate ? ` through ${cutoffDate}` : ''})
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="p-3 text-center font-mono font-bold text-gray-700">
                      {unrosteredRow.games}
                    </td>
                    {displayCats.map(cat => (
                      <td key={cat} className="p-3 text-center font-mono text-gray-700 font-semibold">
                        {formatStat(unrosteredRow.stats[cat], cat, unrosteredRow.games, unrosteredRow.stats)}
                      </td>
                    ))}
                    <td className="p-3 text-right text-xs text-gray-400 italic">
                      MLB Net
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          )}

          {/* VIEW 2: GAME LOG (Filtered) */}
          {selectedTeamId && (
            <div>
              {/* Filter pills bar */}
              <div className="bg-gray-100 px-4 py-2.5 flex items-center justify-between border-b border-gray-200 flex-wrap gap-2 text-xs">
                <span className="font-semibold text-gray-600">
                  Showing <strong className="text-gray-900">{activeRecords.length}</strong> of {teamAllRecords.length} appearances
                </span>
                <div className="flex items-center bg-gray-200 rounded-lg p-0.5 gap-1">
                  <button
                    onClick={() => setGameLogSlotFilter('ALL')}
                    className={`px-3 py-1 rounded text-xs font-bold transition-all cursor-pointer ${
                      gameLogSlotFilter === 'ALL' ? 'bg-white text-blue-700 shadow-xs' : 'text-gray-600 hover:text-gray-900'
                    }`}
                  >
                    All Games ({teamAllRecords.length})
                  </button>
                  <button
                    onClick={() => setGameLogSlotFilter('STARTER')}
                    className={`px-3 py-1 rounded text-xs font-bold transition-all cursor-pointer ${
                      gameLogSlotFilter === 'STARTER' ? 'bg-white text-blue-700 shadow-xs' : 'text-gray-600 hover:text-gray-900'
                    }`}
                  >
                    Starters ({teamAllRecords.filter(r => r.lineup_slot_id !== 16 && r.lineup_slot_id !== 17).length})
                  </button>
                  <button
                    onClick={() => setGameLogSlotFilter('BENCH')}
                    className={`px-3 py-1 rounded text-xs font-bold transition-all cursor-pointer ${
                      gameLogSlotFilter === 'BENCH' ? 'bg-white text-amber-800 shadow-xs' : 'text-gray-600 hover:text-gray-900'
                    }`}
                  >
                    Bench ({teamAllRecords.filter(r => r.lineup_slot_id === 16 || r.lineup_slot_id === 17).length})
                  </button>
                </div>
              </div>

              <table className="w-full text-sm border-collapse">
                <thead className="bg-gray-100 sticky top-0 shadow-sm z-10 text-xs text-gray-500 uppercase tracking-wider font-semibold">
                  <tr>
                    <th className="p-3 text-left border-b border-gray-200">Date</th>
                    <th className="p-3 text-center border-b border-gray-200">Slot</th>
                    {displayCats.map(c => (
                      <th key={c} className="p-3 text-center border-b border-gray-200 min-w-[50px]">
                        {getLabel(c)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 bg-white">
                  {activeRecords.length === 0 ? (
                    <tr>
                      <td colSpan={displayCats.length + 2} className="p-8 text-center text-gray-400 italic">
                        No games recorded for this filter.
                      </td>
                    </tr>
                  ) : (
                    activeRecords.map(record => {
                      const dateStr = getDateFromPeriodId(record.scoring_period_id, record.season_year);
                      const isBench = record.lineup_slot_id === 16;
                      const isIL = record.lineup_slot_id === 17;
                      const pos = LINEUP_SLOTS[record.lineup_slot_id] || (isBench ? 'Bench' : isIL ? 'IL' : 'BN');
                      
                      // Calculate single-game stats with includeAll: true so bench stats calculate
                      const singleStat = aggregateStats([record], { includeAll: true });

                      return (
                        <tr key={record.id || `${record.scoring_period_id}-${record.player_id}`} className="hover:bg-gray-50 transition-colors border-b border-gray-50 last:border-0">
                          <td className="p-3 text-gray-600 font-mono text-xs font-medium">{dateStr}</td>
                          <td className="p-3 text-center text-xs">
                            <span className={`px-2 py-0.5 rounded text-[10px] font-black uppercase ${
                              isIL
                                ? 'bg-rose-100 text-rose-800 border border-rose-200'
                                : isBench
                                ? 'bg-amber-100 text-amber-900 border border-amber-300 font-bold'
                                : 'bg-blue-50 text-blue-700 border border-blue-200'
                            }`}>
                              {pos}
                            </span>
                          </td>
                          {displayCats.map(cat => (
                            <td key={cat} className="p-3 text-center text-gray-700 font-mono text-xs">
                              {formatStat(singleStat[cat], cat, 1, singleStat)}
                            </td>
                          ))}
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}

        </div>
      </div>
    </div>
  );
}