// src/views/OwnerLandingView.jsx
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { TEAMS } from '../schedule';
import { aggregateStats, calculateRotoPoints, SCORING_CATS } from '../utils/scoring';
import { useAuth } from '../context/useAuth';
import TeamAvatar from '../components/TeamAvatar';

const ESPN_ROSTER_ENDPOINT = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/2026/segments/0/leagues/130215?view=mRoster&view=kona_player_info';
const ESPN_NEWS_ENDPOINT = 'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news';

const LEAGUE_MANAGERS = [
  { id: 1, name: 'Tim', title: 'Anti-lock Brake Systems' },
  { id: 2, name: 'Adrian', title: 'D0ng Demolishers' },
  { id: 3, name: 'Garrett', title: 'G-Men' },
  { id: 5, name: 'Daniel', title: 'Sellinger Sluggers' },
  { id: 6, name: 'Anil', title: 'Anil’s All-Stars' },
  { id: 8, name: 'Alex', title: 'Alex in Wonderland' },
  { id: 12, name: 'Will', title: 'Will-Power' },
  { id: 13, name: 'Mark', title: 'Mark’s Marauders' },
  { id: 14, name: 'Preston', title: 'Preston Press' },
];

export default function OwnerLandingView({
  allStats = [],
  processedWeeks = [],
  selectedSeason = 2026,
  onOwnerClick,
  onPlayerClick,
  onNavigate
}) {
  const { user, effectiveTeamId, effectiveOwner, isCommissioner, signInWithDiscord } = useAuth();

  // Active viewing team defaults to user's linked team, or Dan (Team 5), or manual switch
  const [activeTeamId, setActiveTeamId] = useState(() => {
    return effectiveTeamId || 5;
  });

  // Keep in sync if auth changes
  useEffect(() => {
    if (effectiveTeamId) {
      setActiveTeamId(effectiveTeamId);
    }
  }, [effectiveTeamId]);

  const activeTeam = useMemo(() => {
    return TEAMS[activeTeamId] || { id: activeTeamId, name: 'My Team', owner: 'Manager' };
  }, [activeTeamId]);

  // ESPN Live Data State
  const [espnLoading, setEspnLoading] = useState(true);
  const [rosterData, setRosterData] = useState([]);
  const [newsArticles, setNewsArticles] = useState([]);
  const [lastRefreshed, setLastRefreshed] = useState(null);

  // Fetch ESPN Live Rosters, PR 15, and News
  const fetchLiveEspnData = useCallback(async () => {
    setEspnLoading(true);
    try {
      // 1. Fetch ESPN League Rosters (contains PR 15 split & injuries)
      const rosterRes = await fetch(ESPN_ROSTER_ENDPOINT, { cache: 'no-store' });
      if (rosterRes.ok) {
        const leagueData = await rosterRes.json();
        const teamsList = leagueData.teams || [];
        const currentTeamData = teamsList.find(t => t.id === activeTeamId);
        if (currentTeamData && currentTeamData.roster?.entries) {
          setRosterData(currentTeamData.roster.entries);
        }
      }
    } catch (err) {
      console.warn('Could not fetch live ESPN roster data; using local records:', err);
    }

    try {
      // 2. Fetch ESPN MLB Breaking News
      const newsRes = await fetch(ESPN_NEWS_ENDPOINT);
      if (newsRes.ok) {
        const newsData = await newsRes.json();
        setNewsArticles(newsData.articles || []);
      }
    } catch (err) {
      console.warn('Could not fetch ESPN news:', err);
    } finally {
      setEspnLoading(false);
      setLastRefreshed(new Date());
    }
  }, [activeTeamId]);

  useEffect(() => {
    fetchLiveEspnData();
  }, [fetchLiveEspnData]);

  // --- 1. CURRENT H2H MATCHUP COMPUTATION ---
  const currentMatchupInfo = useMemo(() => {
    if (!processedWeeks || processedWeeks.length === 0) return null;

    // Pick active week: find week whose date range is current, or latest week with valid results
    const today = new Date().toISOString().slice(0, 10);
    let targetWeek = processedWeeks.find(w => today >= w.startDate && today <= w.endDate);

    if (!targetWeek) {
      // If beyond schedule or pre-season, take latest week that has non-empty matchups, else week 1
      const playedWeeks = processedWeeks.filter(w => {
        return w.matchups?.some(m => m.result && (m.result.homeScore > 0 || m.result.awayScore > 0));
      });
      targetWeek = playedWeeks.length > 0 ? playedWeeks[playedWeeks.length - 1] : processedWeeks[0];
    }

    if (!targetWeek || !targetWeek.matchups) return null;

    // Find the matchup for activeTeamId
    const m = targetWeek.matchups.find(item => {
      if (item.type === 'trio') {
        return item.teams?.some(t => t.id === activeTeamId) || item.teamIds?.includes(activeTeamId);
      }
      return item.homeTeam?.id === activeTeamId || item.awayTeam?.id === activeTeamId;
    });

    if (!m) return { weekName: targetWeek.name, weekId: targetWeek.weekId, matchup: null };

    const isHome = m.homeTeam?.id === activeTeamId;
    const myTeamObj = isHome ? m.homeTeam : m.awayTeam;
    const oppTeamObj = isHome ? m.awayTeam : m.homeTeam;
    const myStats = isHome ? m.homeStats : m.awayStats;
    const oppStats = isHome ? m.awayStats : m.homeStats;
    const myScore = isHome ? (m.result?.homeScore || 0) : (m.result?.awayScore || 0);
    const oppScore = isHome ? (m.result?.awayScore || 0) : (m.result?.homeScore || 0);
    const ties = m.result?.ties || 0;

    // Compute category-by-category status
    const categories = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];
    const catDetails = categories.map(cat => {
      const isLow = SCORING_CATS[cat]?.type === 'low';
      const myVal = myStats ? (myStats[`${cat}_raw`] !== undefined ? myStats[`${cat}_raw`] : myStats[cat]) : 0;
      const oppVal = oppStats ? (oppStats[`${cat}_raw`] !== undefined ? oppStats[`${cat}_raw`] : oppStats[cat]) : 0;
      const numMy = parseFloat(myVal) || 0;
      const numOpp = parseFloat(oppVal) || 0;

      let status = 'tied';
      if (Math.abs(numMy - numOpp) > 0.0001) {
        const winning = isLow ? numMy < numOpp : numMy > numOpp;
        status = winning ? 'win' : 'loss';
      }

      const formatVal = (v) => {
        if (v === undefined || v === null) return '-';
        if (cat === 'OBP') return (parseFloat(v) || 0).toFixed(3).replace(/^0/, '');
        if (cat === 'ERA' || cat === 'WHIP') return (parseFloat(v) || 0).toFixed(2);
        return Math.round(parseFloat(v) || 0);
      };

      return {
        cat,
        label: SCORING_CATS[cat]?.label || cat,
        myFormatted: formatVal(myVal),
        oppFormatted: formatVal(oppVal),
        myNum: numMy,
        oppNum: numOpp,
        status,
        isLow
      };
    });

    return {
      weekName: targetWeek.name,
      weekId: targetWeek.weekId,
      matchupId: m.matchupId,
      myTeam: myTeamObj,
      oppTeam: oppTeamObj,
      myScore,
      oppScore,
      ties,
      catDetails,
      isTrio: m.type === 'trio'
    };
  }, [processedWeeks, activeTeamId]);

  // --- 2. ROTO LEAGUE POSITION & GAPS ---
  const rotoStandings = useMemo(() => {
    if (!allStats || allStats.length === 0) return null;

    // Filter to real teams 1..14
    const humanTeamIds = [1, 2, 3, 5, 6, 8, 12, 13, 14];
    const teamStatsMap = {};
    humanTeamIds.forEach(id => {
      const records = allStats.filter(r => r.team_id === id);
      teamStatsMap[id] = aggregateStats(records);
    });

    const rotoResults = calculateRotoPoints(teamStatsMap);

    // Sort teams by total points descending
    const rankedTeams = Object.entries(rotoResults)
      .map(([idStr, pts]) => ({
        id: parseInt(idStr, 10),
        points: pts,
        total: pts.total || 0,
        teamInfo: TEAMS[idStr] || { name: `Team ${idStr}`, owner: `Owner ${idStr}` }
      }))
      .sort((a, b) => b.total - a.total);

    const myIndex = rankedTeams.findIndex(t => t.id === activeTeamId);
    if (myIndex === -1) return null;

    const myRank = myIndex + 1;
    const myTotal = rankedTeams[myIndex].total;
    const leaderTotal = rankedTeams[0].total;
    const pointsBehindLeader = myRank === 1 ? 0 : parseFloat((leaderTotal - myTotal).toFixed(1));
    const nextTeamBehind = myRank < rankedTeams.length ? rankedTeams[myIndex + 1] : null;
    const cushionOverNext = nextTeamBehind ? parseFloat((myTotal - nextTeamBehind.total).toFixed(1)) : null;

    // Identify category strengths & weaknesses
    const myCatPts = rankedTeams[myIndex].points;
    const cats = ['R', 'HR', 'RBI', 'SB', 'OBP', 'K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];
    const catRanks = cats.map(cat => {
      // Rank in this category across all 9
      const sortedByCat = [...rankedTeams].sort((a, b) => (b.points[cat] || 0) - (a.points[cat] || 0));
      const rank = sortedByCat.findIndex(t => t.id === activeTeamId) + 1;
      return {
        cat,
        label: SCORING_CATS[cat]?.label || cat,
        points: myCatPts[cat] || 0,
        rank
      };
    });

    const strengths = [...catRanks].sort((a, b) => b.points - a.points).slice(0, 3);
    const weaknesses = [...catRanks].sort((a, b) => a.points - b.points).slice(0, 3);

    return {
      rank: myRank,
      totalTeams: rankedTeams.length,
      totalPoints: myTotal,
      pointsBehindLeader,
      cushionOverNext,
      nextTeamBehind,
      strengths,
      weaknesses,
      leaderTeam: rankedTeams[0]
    };
  }, [allStats, activeTeamId]);

  // --- 3. HOT & COLD PLAYERS (ESPN PR 15) ---
  const { hotPlayers, coldPlayers, injuredPlayers, rosterPlayerNames } = useMemo(() => {
    if (!rosterData || rosterData.length === 0) {
      return { hotPlayers: [], coldPlayers: [], injuredPlayers: [], rosterPlayerNames: new Set() };
    }

    const injured = [];
    const scoredPlayers = [];
    const names = new Set();

    rosterData.forEach(entry => {
      const p = entry.playerPoolEntry?.player;
      if (!p) return;

      names.add(p.fullName?.toLowerCase());
      if (p.fullName) names.add(p.fullName);

      // Check Injury Status
      if (p.injured || (p.injuryStatus && p.injuryStatus !== 'ACTIVE')) {
        injured.push({
          id: p.id,
          name: p.fullName,
          position: p.defaultPositionId === 1 ? 'SP' : (p.defaultPositionId === 11 ? 'RP' : 'BAT'),
          injuryStatus: p.injuryStatus || 'DAY_TO_DAY',
          injured: true,
          team: p.proTeamId ? `MLB Team ${p.proTeamId}` : 'MLB'
        });
      }

      // Check 15-Day PR Split (`statSplitTypeId === 2`)
      const s15 = p.stats?.find(s => s.statSplitTypeId === 2);
      const st = s15?.stats || {};
      const isPitcher = p.defaultPositionId === 1 || p.defaultPositionId === 11 || (p.eligibleSlots && p.eligibleSlots.includes(13));

      let pr = 0;
      let statSummary = '';
      let hasActivity = false;

      if (!isPitcher) {
        pr = typeof st['19'] === 'number' ? st['19'] : 0;
        const ab = st['0'] || 0;
        const h = st['1'] || 0;
        const hr = st['5'] || 0;
        const rbi = st['21'] || 0;
        const sb = st['23'] || 0;
        if (ab > 0) {
          hasActivity = true;
          const avg = (h / ab).toFixed(3).replace(/^0/, '');
          statSummary = `${avg} AVG · ${hr} HR · ${rbi} RBI · ${sb} SB (${ab} AB)`;
        } else {
          statSummary = '0 ABs last 15d';
        }
      } else {
        const ip = (st['34'] || 0) / 3;
        const k = st['48'] || 0;
        const er = st['45'] || 0;
        const bb = st['39'] || 0;
        const h = st['37'] || 0;
        if (ip > 0) {
          hasActivity = true;
          const era = ((er * 9) / ip).toFixed(2);
          const whip = ((bb + h) / ip).toFixed(2);
          pr = (k * 1.0 + (st['63'] || 0) * 3 + (st['57'] || 0) * 4 + (st['60'] || 0) * 3 - er * 1.5 - bb * 0.5) / (ip / 5);
          statSummary = `${ip.toFixed(1)} IP · ${k} K · ${era} ERA · ${whip} WHIP`;
        } else {
          statSummary = '0 IP last 15d';
        }
      }

      if (hasActivity) {
        scoredPlayers.push({
          id: p.id,
          name: p.fullName,
          isPitcher,
          pr: parseFloat(pr.toFixed(2)),
          statSummary,
          position: isPitcher ? 'P' : 'BAT'
        });
      }
    });

    // Sort descending by PR
    scoredPlayers.sort((a, b) => b.pr - a.pr);

    const hot = scoredPlayers.slice(0, 4);
    const cold = scoredPlayers.slice(-4).reverse();

    return {
      hotPlayers: hot,
      coldPlayers: cold,
      injuredPlayers: injured,
      rosterPlayerNames: names
    };
  }, [rosterData]);

  // --- 4. RELEVANT PLAYER NEWS MATCHING ---
  const relevantNews = useMemo(() => {
    if (!newsArticles || newsArticles.length === 0) return [];

    const matchedArticles = [];
    const generalArticles = [];

    newsArticles.forEach(article => {
      const cats = article.categories || [];
      const mentionsRosterPlayer = cats.some(cat => {
        const athleteName = cat.athlete?.description || cat.description || '';
        return athleteName && rosterPlayerNames.has(athleteName.toLowerCase());
      }) || (article.headline && Array.from(rosterPlayerNames).some(n => typeof n === 'string' && article.headline.toLowerCase().includes(n.toLowerCase())));

      if (mentionsRosterPlayer) {
        matchedArticles.push({ ...article, isRosterPlayer: true });
      } else {
        generalArticles.push({ ...article, isRosterPlayer: false });
      }
    });

    // If we have matched articles, place them first; fill up to 4 articles total
    const combined = [...matchedArticles, ...generalArticles].slice(0, 4);
    return combined;
  }, [newsArticles, rosterPlayerNames]);

  return (
    <div className="space-y-8 animate-fade-in pb-12">
      {/* ========================================================= */}
      {/* 1. HERO MANAGER BANNER & TEAM SWITCHER */}
      {/* ========================================================= */}
      <div className="relative overflow-hidden bg-gradient-to-br from-slate-900 via-slate-900/95 to-slate-950 border border-slate-800 rounded-3xl p-6 sm:p-8 shadow-2xl backdrop-blur-xl">
        {/* Glowing backdrop flare */}
        <div className="absolute top-0 right-0 w-96 h-96 bg-blue-600/10 rounded-full blur-3xl pointer-events-none -mr-20 -mt-20" />
        <div className="absolute bottom-0 left-0 w-96 h-96 bg-indigo-600/10 rounded-full blur-3xl pointer-events-none -ml-20 -mb-20" />

        <div className="relative z-10 flex flex-col lg:flex-row lg:items-center justify-between gap-6">
          <div className="flex items-center gap-5">
            <TeamAvatar team={activeTeam} size="lg" />
            <div>
              <div className="flex items-center gap-2.5 flex-wrap">
                <span className="px-2.5 py-0.5 rounded-full text-xs font-black tracking-wider uppercase bg-blue-500/20 text-blue-400 border border-blue-500/30">
                  {selectedSeason} Season Dashboard
                </span>
                {user ? (
                  <span className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 flex items-center gap-1">
                    <span>🟢</span>
                    <span>{effectiveOwner ? `Logged in as ${effectiveOwner}` : 'Verified'}</span>
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={signInWithDiscord}
                    className="px-2.5 py-0.5 rounded-full text-xs font-bold bg-indigo-600/30 hover:bg-indigo-600/50 text-indigo-300 border border-indigo-500/40 transition cursor-pointer flex items-center gap-1"
                  >
                    <span>💬</span>
                    <span>Connect Discord</span>
                  </button>
                )}
              </div>
              <h1 className="text-3xl sm:text-4xl font-black text-white tracking-tight mt-1">
                {activeTeam.name}
              </h1>
              <p className="text-slate-400 text-sm font-medium flex items-center gap-2 mt-0.5">
                <span>Manager: <strong className="text-slate-200">{activeTeam.owner}</strong></span>
                <span>·</span>
                <span>Team #{activeTeam.id}</span>
                {lastRefreshed && (
                  <>
                    <span>·</span>
                    <span className="text-xs text-slate-500">Live ESPN sync active</span>
                  </>
                )}
              </p>
            </div>
          </div>

          {/* Quick Stat Highlights */}
          <div className="flex items-center gap-3 self-start lg:self-center flex-wrap">
            {rotoStandings && (
              <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl px-4 py-2.5 text-center min-w-[110px]">
                <div className="text-xs text-slate-400 font-bold uppercase tracking-wider">Roto Rank</div>
                <div className="text-2xl font-black text-emerald-400">
                  #{rotoStandings.rank}
                  <span className="text-xs text-slate-400 font-medium ml-1">of {rotoStandings.totalTeams}</span>
                </div>
              </div>
            )}
            {currentMatchupInfo && (
              <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl px-4 py-2.5 text-center min-w-[110px]">
                <div className="text-xs text-slate-400 font-bold uppercase tracking-wider">H2H Status</div>
                <div className={`text-2xl font-black ${currentMatchupInfo.myScore > currentMatchupInfo.oppScore ? 'text-blue-400' : currentMatchupInfo.myScore === currentMatchupInfo.oppScore ? 'text-amber-400' : 'text-rose-400'}`}>
                  {currentMatchupInfo.myScore} - {currentMatchupInfo.oppScore}
                  {currentMatchupInfo.ties > 0 && <span className="text-xs text-slate-400 ml-1">({currentMatchupInfo.ties}T)</span>}
                </div>
              </div>
            )}
            <button
              type="button"
              onClick={fetchLiveEspnData}
              disabled={espnLoading}
              className="p-3 bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 hover:text-white rounded-2xl transition cursor-pointer disabled:opacity-50"
              title="Refresh live ESPN stats"
            >
              <span className={`inline-block text-lg ${espnLoading ? 'animate-spin' : ''}`}>🔄</span>
            </button>
          </div>
        </div>

        {/* Interactive Manager Switcher Bar */}
        <div className="mt-6 pt-5 border-t border-slate-800/80">
          <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-2.5 flex items-center justify-between">
            <span>👤 Switch Active Team View</span>
            {isCommissioner && <span className="text-amber-400 text-[11px] font-black">👑 Commissioner Mode Active</span>}
          </div>
          <div className="flex items-center gap-1.5 overflow-x-auto pb-1 no-scrollbar">
            {LEAGUE_MANAGERS.map(m => {
              const isActive = m.id === activeTeamId;
              return (
                <button
                  key={m.id}
                  type="button"
                  onClick={() => setActiveTeamId(m.id)}
                  className={`px-3 py-1.5 rounded-xl text-xs font-black transition cursor-pointer whitespace-nowrap flex items-center gap-1.5 ${
                    isActive
                      ? 'bg-blue-600 text-white ring-2 ring-blue-400 shadow-md scale-105'
                      : 'bg-slate-800/60 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-700/50'
                  }`}
                >
                  <span>{m.name}</span>
                  {m.id === effectiveTeamId && <span className="text-[10px] bg-emerald-500 text-slate-950 font-black px-1 rounded-sm">YOU</span>}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* ========================================================= */}
      {/* 2. CORE PERFORMANCE GRID (H2H MATCHUP + ROTO POSITION) */}
      {/* ========================================================= */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* --- CARD A: CURRENT H2H MATCHUP --- */}
        <div className="bg-slate-900/90 border border-slate-800 rounded-3xl p-6 sm:p-7 shadow-xl backdrop-blur-md flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between gap-2 pb-4 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <span className="text-2xl">⚔️</span>
                <div>
                  <h2 className="text-lg font-black text-white tracking-tight">Current H2H Matchup</h2>
                  <p className="text-xs text-slate-400 font-medium">
                    {currentMatchupInfo?.weekName || 'Regular Season Matchup'}
                  </p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => onNavigate && onNavigate('weekly')}
                className="text-xs font-bold text-blue-400 hover:text-blue-300 transition flex items-center gap-1 cursor-pointer"
              >
                <span>Full Box Score</span>
                <span>➔</span>
              </button>
            </div>

            {currentMatchupInfo ? (
              <div className="mt-5 space-y-5">
                {/* Scoreboard summary tile */}
                <div className="bg-slate-950/60 border border-slate-800/80 rounded-2xl p-4 flex items-center justify-between gap-4">
                  <div
                    onClick={() => onOwnerClick && onOwnerClick(activeTeam)}
                    className="flex items-center gap-3 cursor-pointer hover:opacity-85 transition"
                  >
                    <TeamAvatar team={activeTeam} size="md" />
                    <div>
                      <div className="text-xs font-bold text-slate-400">YOU</div>
                      <div className="text-base font-black text-white">{activeTeam.name}</div>
                    </div>
                  </div>

                  <div className="text-center px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-700/60">
                    <div className="text-2xl font-black tracking-tight text-white">
                      <span className={currentMatchupInfo.myScore > currentMatchupInfo.oppScore ? 'text-emerald-400' : currentMatchupInfo.myScore < currentMatchupInfo.oppScore ? 'text-rose-400' : 'text-amber-400'}>
                        {currentMatchupInfo.myScore}
                      </span>
                      <span className="text-slate-500 mx-2">-</span>
                      <span className={currentMatchupInfo.oppScore > currentMatchupInfo.myScore ? 'text-emerald-400' : currentMatchupInfo.oppScore < currentMatchupInfo.myScore ? 'text-rose-400' : 'text-amber-400'}>
                        {currentMatchupInfo.oppScore}
                      </span>
                    </div>
                    <div className="text-[10px] uppercase font-black tracking-wider text-slate-400">
                      {currentMatchupInfo.myScore > currentMatchupInfo.oppScore
                        ? 'Ahead'
                        : currentMatchupInfo.myScore < currentMatchupInfo.oppScore
                        ? 'Trailing'
                        : 'Tied'}
                    </div>
                  </div>

                  <div
                    onClick={() => currentMatchupInfo.oppTeam && onOwnerClick && onOwnerClick(currentMatchupInfo.oppTeam)}
                    className="flex items-center gap-3 text-right cursor-pointer hover:opacity-85 transition"
                  >
                    <div>
                      <div className="text-xs font-bold text-slate-400">OPPONENT</div>
                      <div className="text-base font-black text-white">{currentMatchupInfo.oppTeam?.name}</div>
                    </div>
                    {currentMatchupInfo.oppTeam && <TeamAvatar team={currentMatchupInfo.oppTeam} size="md" />}
                  </div>
                </div>

                {/* Category by Category Status Bars */}
                <div className="space-y-2">
                  <div className="text-xs font-bold uppercase tracking-wider text-slate-400 flex justify-between px-1">
                    <span>Category</span>
                    <div className="flex gap-16">
                      <span>Your Stat</span>
                      <span>Opponent</span>
                    </div>
                  </div>
                  <div className="space-y-1.5">
                    {currentMatchupInfo.catDetails.map(c => {
                      const isWin = c.status === 'win';
                      const isLoss = c.status === 'loss';
                      return (
                        <div
                          key={c.cat}
                          className="flex items-center justify-between px-3 py-2 rounded-xl bg-slate-950/40 border border-slate-800/40 text-xs hover:border-slate-700 transition"
                        >
                          <div className="flex items-center gap-2 min-w-[90px]">
                            <span className={`w-2 h-2 rounded-full ${isWin ? 'bg-emerald-400' : isLoss ? 'bg-rose-400' : 'bg-slate-500'}`} />
                            <span className="font-bold text-slate-200">{c.cat}</span>
                          </div>

                          <div className="flex items-center gap-4">
                            <span className={`font-mono font-black min-w-[50px] text-right ${isWin ? 'text-emerald-400 font-extrabold' : 'text-slate-300'}`}>
                              {c.myFormatted}
                            </span>
                            <span className="text-slate-600">vs</span>
                            <span className={`font-mono min-w-[50px] text-left ${isLoss ? 'text-rose-400 font-extrabold' : 'text-slate-400'}`}>
                              {c.oppFormatted}
                            </span>
                          </div>

                          <div className="w-16 text-right font-black text-[11px]">
                            {isWin ? (
                              <span className="text-emerald-400 uppercase">Lead ✓</span>
                            ) : isLoss ? (
                              <span className="text-rose-400 uppercase">Trail ✗</span>
                            ) : (
                              <span className="text-slate-400 uppercase">Tie =</span>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            ) : (
              <div className="py-12 text-center text-slate-400 text-sm">
                No active matchup scheduled for this week.
              </div>
            )}
          </div>

          <div className="mt-6 pt-4 border-t border-slate-800/80">
            <button
              type="button"
              onClick={() => onNavigate && onNavigate('weekly')}
              className="w-full py-2.5 px-4 rounded-xl bg-blue-600 hover:bg-blue-500 text-white font-black text-xs transition cursor-pointer shadow-md flex items-center justify-center gap-2"
            >
              <span>Explore All Week Matchups & Box Scores</span>
              <span>➔</span>
            </button>
          </div>
        </div>

        {/* --- CARD B: ROTO LEAGUE POSITION & PACE --- */}
        <div className="bg-slate-900/90 border border-slate-800 rounded-3xl p-6 sm:p-7 shadow-xl backdrop-blur-md flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between gap-2 pb-4 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <span className="text-2xl">🏆</span>
                <div>
                  <h2 className="text-lg font-black text-white tracking-tight">Roto League Standings & Pace</h2>
                  <p className="text-xs text-slate-400 font-medium">10-Category Cumulative Roto Performance</p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => onNavigate && onNavigate('teams')}
                className="text-xs font-bold text-emerald-400 hover:text-emerald-300 transition flex items-center gap-1 cursor-pointer"
              >
                <span>Full Roto Matrix</span>
                <span>➔</span>
              </button>
            </div>

            {rotoStandings ? (
              <div className="mt-5 space-y-5">
                {/* Standing Hero Stat Tile */}
                <div className="bg-gradient-to-r from-emerald-950/40 via-slate-950/60 to-slate-950/40 border border-emerald-500/20 rounded-2xl p-5 flex items-center justify-between gap-4">
                  <div>
                    <div className="text-xs font-bold uppercase tracking-wider text-emerald-400">Current Position</div>
                    <div className="text-4xl font-black text-white mt-1">
                      #{rotoStandings.rank}
                      <span className="text-sm text-slate-400 font-medium ml-2">of {rotoStandings.totalTeams} Teams</span>
                    </div>
                    <div className="text-xs text-slate-400 mt-1">
                      {rotoStandings.rank === 1 ? (
                        <span className="text-emerald-400 font-bold">👑 Holding 1st Place!</span>
                      ) : (
                        <span>
                          <strong className="text-white">{rotoStandings.pointsBehindLeader} pts</strong> behind 1st ({rotoStandings.leaderTeam?.teamInfo?.name})
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="text-right">
                    <div className="text-xs font-bold uppercase tracking-wider text-slate-400">Total Points</div>
                    <div className="text-3xl font-black text-emerald-400 mt-1">{rotoStandings.totalPoints}</div>
                    {rotoStandings.cushionOverNext !== null && (
                      <div className="text-xs text-slate-400 mt-1">
                        +<strong className="text-emerald-300">{rotoStandings.cushionOverNext} pts</strong> cushion over #{rotoStandings.rank + 1}
                      </div>
                    )}
                  </div>
                </div>

                {/* Category Highlights: Strengths & Weaknesses */}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  {/* Strengths */}
                  <div className="bg-slate-950/50 border border-slate-800 rounded-2xl p-4 space-y-2.5">
                    <div className="text-xs font-black uppercase tracking-wider text-emerald-400 flex items-center gap-1.5">
                      <span>💎</span>
                      <span>Top Strengths</span>
                    </div>
                    <div className="space-y-2">
                      {rotoStandings.strengths.map(s => (
                        <div key={s.cat} className="flex items-center justify-between text-xs">
                          <span className="font-bold text-slate-200">{s.label}</span>
                          <span className="px-2 py-0.5 rounded-md bg-emerald-950/60 border border-emerald-700/40 text-emerald-300 font-black">
                            #{s.rank} ({s.points} pts)
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Vulnerabilities */}
                  <div className="bg-slate-950/50 border border-slate-800 rounded-2xl p-4 space-y-2.5">
                    <div className="text-xs font-black uppercase tracking-wider text-rose-400 flex items-center gap-1.5">
                      <span>🎯</span>
                      <span>Target Opportunities</span>
                    </div>
                    <div className="space-y-2">
                      {rotoStandings.weaknesses.map(w => (
                        <div key={w.cat} className="flex items-center justify-between text-xs">
                          <span className="font-bold text-slate-200">{w.label}</span>
                          <span className="px-2 py-0.5 rounded-md bg-rose-950/60 border border-rose-700/40 text-rose-300 font-black">
                            #{w.rank} ({w.points} pts)
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="py-12 text-center text-slate-400 text-sm">
                Roto standings calculation in progress...
              </div>
            )}
          </div>

          <div className="mt-6 pt-4 border-t border-slate-800/80">
            <button
              type="button"
              onClick={() => onNavigate && onNavigate('teams')}
              className="w-full py-2.5 px-4 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white font-black text-xs transition cursor-pointer shadow-md flex items-center justify-center gap-2"
            >
              <span>View Roto Gap Analysis & Pace Targets</span>
              <span>➔</span>
            </button>
          </div>
        </div>
      </div>

      {/* ========================================================= */}
      {/* 3. HOT & COLD PLAYERS (ESPN PR 15) */}
      {/* ========================================================= */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-3xl p-6 sm:p-7 shadow-xl backdrop-blur-md">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-5 border-b border-slate-800">
          <div className="flex items-center gap-3">
            <span className="text-2xl">🔥</span>
            <div>
              <h2 className="text-lg font-black text-white tracking-tight">Hot & Cold Roster Momentum</h2>
              <p className="text-xs text-slate-400 font-medium">
                Ranked using <strong>ESPN PR 15</strong> (Player Rating over the last 15 days)
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2 text-xs text-slate-400 font-medium">
            <span>Rostered on: <strong className="text-white">{activeTeam.name}</strong></span>
          </div>
        </div>

        <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* HOT PLAYERS */}
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-xs font-black uppercase tracking-wider text-amber-400">
              <span>🔥</span>
              <span>On Fire (Highest PR 15)</span>
            </div>

            {hotPlayers.length > 0 ? (
              <div className="space-y-2">
                {hotPlayers.map((p, idx) => (
                  <div
                    key={p.id}
                    onClick={() => onPlayerClick && onPlayerClick(p.id, p.name)}
                    className="group flex items-center justify-between p-3.5 rounded-2xl bg-gradient-to-r from-amber-950/20 via-slate-950/40 to-slate-950/60 border border-amber-500/20 hover:border-amber-400/50 transition cursor-pointer"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-7 h-7 rounded-xl bg-amber-500/20 text-amber-300 font-black text-xs flex items-center justify-center border border-amber-500/30">
                        {idx + 1}
                      </div>
                      <div>
                        <div className="text-sm font-black text-white group-hover:text-amber-300 transition flex items-center gap-1.5">
                          <span>{p.name}</span>
                          <span className="text-[10px] text-slate-400 font-bold">({p.position})</span>
                        </div>
                        <div className="text-xs text-slate-400 font-medium mt-0.5">
                          {p.statSummary}
                        </div>
                      </div>
                    </div>

                    <div className="text-right">
                      <span className="px-2.5 py-1 rounded-xl bg-amber-500/20 text-amber-400 border border-amber-500/30 font-black text-xs">
                        +{p.pr} PR
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="p-6 rounded-2xl bg-slate-950/40 border border-slate-800 text-center text-xs text-slate-400">
                {espnLoading ? 'Fetching live 15-day player ratings...' : 'No active player stats in the last 15 days.'}
              </div>
            )}
          </div>

          {/* COLD PLAYERS */}
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-xs font-black uppercase tracking-wider text-cyan-400">
              <span>❄️</span>
              <span>Slumping / Cold Stretch</span>
            </div>

            {coldPlayers.length > 0 ? (
              <div className="space-y-2">
                {coldPlayers.map((p, idx) => (
                  <div
                    key={p.id}
                    onClick={() => onPlayerClick && onPlayerClick(p.id, p.name)}
                    className="group flex items-center justify-between p-3.5 rounded-2xl bg-gradient-to-r from-cyan-950/20 via-slate-950/40 to-slate-950/60 border border-cyan-500/20 hover:border-cyan-400/50 transition cursor-pointer"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-7 h-7 rounded-xl bg-cyan-500/20 text-cyan-300 font-black text-xs flex items-center justify-center border border-cyan-500/30">
                        {idx + 1}
                      </div>
                      <div>
                        <div className="text-sm font-black text-white group-hover:text-cyan-300 transition flex items-center gap-1.5">
                          <span>{p.name}</span>
                          <span className="text-[10px] text-slate-400 font-bold">({p.position})</span>
                        </div>
                        <div className="text-xs text-slate-400 font-medium mt-0.5">
                          {p.statSummary}
                        </div>
                      </div>
                    </div>

                    <div className="text-right">
                      <span className={`px-2.5 py-1 rounded-xl font-black text-xs border ${p.pr < 0 ? 'bg-rose-500/20 text-rose-400 border-rose-500/30' : 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30'}`}>
                        {p.pr} PR
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="p-6 rounded-2xl bg-slate-950/40 border border-slate-800 text-center text-xs text-slate-400">
                {espnLoading ? 'Fetching live 15-day player ratings...' : 'No slumping players recorded.'}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ========================================================= */}
      {/* 4. INJURIES & ESPN PLAYER NEWS */}
      {/* ========================================================= */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* --- CARD C: INJURY ALERT CENTER --- */}
        <div className="bg-slate-900/90 border border-slate-800 rounded-3xl p-6 sm:p-7 shadow-xl backdrop-blur-md flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between gap-2 pb-4 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <span className="text-2xl">🚑</span>
                <div>
                  <h2 className="text-lg font-black text-white tracking-tight">Injury Alert Center</h2>
                  <p className="text-xs text-slate-400 font-medium">Real-time status for rostered players</p>
                </div>
              </div>
              <span className={`px-2.5 py-0.5 rounded-full text-xs font-black ${injuredPlayers.length > 0 ? 'bg-rose-500/20 text-rose-400 border border-rose-500/30' : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'}`}>
                {injuredPlayers.length} Injured
              </span>
            </div>

            <div className="mt-5 space-y-2.5">
              {injuredPlayers.length > 0 ? (
                injuredPlayers.map(p => {
                  const isSevere = p.injuryStatus.includes('SIXTY') || p.injuryStatus === 'OUT';
                  const isDayToDay = p.injuryStatus.includes('DAY_TO_DAY');
                  return (
                    <div
                      key={p.id}
                      onClick={() => onPlayerClick && onPlayerClick(p.id, p.name)}
                      className="flex items-center justify-between p-3.5 rounded-2xl bg-slate-950/60 border border-slate-800 hover:border-slate-700 transition cursor-pointer"
                    >
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded-xl bg-slate-800 flex items-center justify-center text-sm font-bold text-slate-300">
                          {p.position}
                        </div>
                        <div>
                          <div className="text-sm font-black text-white hover:text-blue-400 transition">
                            {p.name}
                          </div>
                          <div className="text-xs text-slate-400">
                            {p.team}
                          </div>
                        </div>
                      </div>

                      <div className="text-right">
                        <span className={`px-2.5 py-1 rounded-xl text-xs font-black uppercase tracking-wider border ${
                          isSevere
                            ? 'bg-rose-600/20 text-rose-400 border-rose-600/30'
                            : isDayToDay
                            ? 'bg-amber-600/20 text-amber-400 border-amber-600/30'
                            : 'bg-indigo-600/20 text-indigo-400 border-indigo-600/30'
                        }`}>
                          {p.injuryStatus.replace(/_/g, ' ')}
                        </span>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="py-12 text-center rounded-2xl bg-emerald-950/20 border border-emerald-500/20 p-6 space-y-2">
                  <div className="text-3xl">🎉</div>
                  <div className="text-base font-black text-emerald-400">All Rostered Players Healthy!</div>
                  <p className="text-xs text-slate-400 max-w-xs mx-auto">
                    No active IL or Day-to-Day designations currently assigned to this squad.
                  </p>
                </div>
              )}
            </div>
          </div>

          <div className="mt-6 pt-4 border-t border-slate-800/80 text-xs text-slate-500 flex items-center justify-between">
            <span>Synchronized with ESPN IL designations</span>
            <button
              type="button"
              onClick={() => onNavigate && onNavigate('transactions')}
              className="text-blue-400 hover:text-blue-300 font-bold transition cursor-pointer"
            >
              Waiver Wire ➔
            </button>
          </div>
        </div>

        {/* --- CARD D: ESPN PLAYER NEWS FEED --- */}
        <div className="bg-slate-900/90 border border-slate-800 rounded-3xl p-6 sm:p-7 shadow-xl backdrop-blur-md flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between gap-2 pb-4 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <span className="text-2xl">📰</span>
                <div>
                  <h2 className="text-lg font-black text-white tracking-tight">ESPN Player News</h2>
                  <p className="text-xs text-slate-400 font-medium">Headlines & reports relevant to your roster</p>
                </div>
              </div>
              <span className="text-[11px] font-bold text-slate-500">ESPN MLB Wire</span>
            </div>

            <div className="mt-5 space-y-3">
              {relevantNews.length > 0 ? (
                relevantNews.map(article => {
                  const imageUrl = article.images?.[0]?.url;
                  const dateStr = article.published ? new Date(article.published).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : '';
                  return (
                    <a
                      key={article.id || article.headline}
                      href={article.links?.web?.href || 'https://www.espn.com/mlb/'}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="group flex items-start gap-3 p-3 rounded-2xl bg-slate-950/60 border border-slate-800/80 hover:border-slate-700 transition block"
                    >
                      {imageUrl && (
                        <img
                          src={imageUrl}
                          alt=""
                          className="w-16 h-16 rounded-xl object-cover shrink-0 border border-slate-800"
                        />
                      )}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap mb-1">
                          {article.isRosterPlayer && (
                            <span className="px-2 py-0.5 rounded-md bg-blue-500/20 text-blue-400 border border-blue-500/30 text-[10px] font-black uppercase tracking-wider">
                              Rostered Player
                            </span>
                          )}
                          <span className="text-[11px] text-slate-500 font-medium">{dateStr}</span>
                        </div>
                        <h3 className="text-xs font-extrabold text-white group-hover:text-blue-400 transition line-clamp-2 leading-snug">
                          {article.headline}
                        </h3>
                        {article.description && article.description !== article.headline && (
                          <p className="text-[11px] text-slate-400 line-clamp-1 mt-0.5">
                            {article.description}
                          </p>
                        )}
                      </div>
                    </a>
                  );
                })
              ) : (
                <div className="py-12 text-center text-slate-400 text-xs">
                  {espnLoading ? 'Loading latest MLB player news...' : 'No news articles available.'}
                </div>
              )}
            </div>
          </div>

          <div className="mt-6 pt-4 border-t border-slate-800/80 flex items-center justify-between text-xs text-slate-500">
            <span>Powered by ESPN MLB API</span>
            <a
              href="https://www.espn.com/mlb/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-400 hover:text-blue-300 font-bold transition"
            >
              ESPN MLB Home ➔
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}
