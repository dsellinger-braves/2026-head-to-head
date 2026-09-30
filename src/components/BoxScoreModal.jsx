import { useState, useEffect, useMemo } from 'react';
import { CATEGORIES, LINEUP_SLOTS, aggregateStats } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

const BATTER_CATS = [
  { id: 'PA', label: 'PA', isCategory: false },
  { id: 'AB', label: 'AB', isCategory: false },
  { id: 'R', label: 'R', isCategory: true },
  { id: 'H', label: 'H', isCategory: false },
  { id: 'HR', label: 'HR', isCategory: true },
  { id: 'RBI', label: 'RBI', isCategory: true },
  { id: 'SB', label: 'SB', isCategory: true },
  { id: 'BB', label: 'BB', isCategory: false },
  { id: 'OBP', label: 'OBP', isCategory: true, isRate: true }
];

const PITCHER_CATS = [
  { id: 'IP', label: 'IP', isCategory: false },
  { id: 'H_Allowed', label: 'H', isCategory: false },
  { id: 'ER', label: 'ER', isCategory: false },
  { id: 'BB_Allowed', label: 'BB', isCategory: false },
  { id: 'K', label: 'K', isCategory: true },
  { id: 'QS', label: 'QS', isCategory: true },
  { id: 'SV+HDs', label: 'SV+HD', isCategory: true },
  { id: 'ERA', label: 'ERA', isCategory: true, isRate: true },
  { id: 'WHIP', label: 'WHIP', isCategory: true, isRate: true }
];

function formatDisplayVal(val, catId) {
  if (val === undefined || val === null) return '-';
  const n = parseFloat(val);
  if (isNaN(n)) return val;
  if (catId === 'IP') {
    return `${Math.floor(n)}.${Math.round((n % 1) * 3)}`;
  }
  if (catId === 'OBP') {
    return n.toFixed(4).replace(/^0/, '');
  }
  if (catId === 'ERA' || catId === 'WHIP') {
    return n.toFixed(2);
  }
  return Math.round(n);
}

export default function BoxScoreModal({ matchup, onClose, onPlayerClick }) {
  const [activeTab, setActiveTab] = useState('matchup'); // 'matchup' | 'players'
  const [selectedTeamId, setSelectedTeamId] = useState(null);
  const [showBench, setShowBench] = useState(false);

  const defaultTeamId = matchup?.type === 'trio' ? matchup?.teams?.[0]?.id : matchup?.homeTeam?.id;
  const currentTeamId = selectedTeamId ?? defaultTeamId;

  // Close modal on ESC key press
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  // Helper to extract team records for the selected team
  const selectedTeamRecords = useMemo(() => {
    if (!matchup) return [];
    if (matchup.type === 'trio') {
      return matchup.teamRecords?.[currentTeamId] || [];
    }
    if (currentTeamId === matchup.homeTeam?.id) {
      return matchup.homeRecords || [];
    }
    if (currentTeamId === matchup.awayTeam?.id) {
      return matchup.awayRecords || [];
    }
    return [];
  }, [matchup, currentTeamId]);

  // Process player-level detail for the selected team
  const { activeBatters, activePitchers, benchPlayers, teamTotals } = useMemo(() => {
    if (!selectedTeamRecords || selectedTeamRecords.length === 0) {
      return { activeBatters: [], activePitchers: [], benchPlayers: [], teamTotals: null };
    }

    const playerMap = {};
    for (const r of selectedTeamRecords) {
      if (!playerMap[r.player_id]) {
        playerMap[r.player_id] = {
          id: r.player_id,
          name: r.full_name,
          records: [],
          activeRecords: [],
          benchRecords: []
        };
      }
      playerMap[r.player_id].records.push(r);
      if (r.lineup_slot_id !== 16 && r.lineup_slot_id !== 17) {
        playerMap[r.player_id].activeRecords.push(r);
      } else {
        playerMap[r.player_id].benchRecords.push(r);
      }
    }

    const players = Object.values(playerMap).map(p => {
      const activeStats = aggregateStats(p.activeRecords);
      const benchStats = aggregateStats(p.benchRecords, { includeBenchOnly: true });
      const allStats = aggregateStats(p.records, { includeAll: true });

      const activeSlots = [...new Set(p.activeRecords.map(r => LINEUP_SLOTS[r.lineup_slot_id] || 'UTIL'))];
      const allSlots = [...new Set(p.records.map(r => LINEUP_SLOTS[r.lineup_slot_id] || 'BN'))];

      const isPitcher = (allStats.IP > 0 || allStats.K > 0 || allSlots.some(s => ['SP', 'RP', 'P'].includes(s)));

      return {
        id: p.id,
        name: p.name,
        slot: (activeSlots.length > 0 ? activeSlots.join('/') : allSlots.join('/')) || 'BN',
        isPitcher,
        activeStats,
        benchStats,
        allStats,
        gamesActive: p.activeRecords.length,
        gamesBench: p.benchRecords.length
      };
    });

    const activeBatters = players
      .filter(p => (p.activeStats.PA || 0) > 0 || (!p.isPitcher && p.gamesActive > 0))
      .sort((a, b) => (b.activeStats.PA || 0) - (a.activeStats.PA || 0));

    const activePitchers = players
      .filter(p => (p.activeStats.IP || 0) > 0 || (p.isPitcher && p.gamesActive > 0))
      .sort((a, b) => (b.activeStats.IP || 0) - (a.activeStats.IP || 0));

    const benchPlayers = players
      .filter(p => p.gamesActive === 0)
      .sort((a, b) => a.name.localeCompare(b.name));

    const teamTotals = aggregateStats(selectedTeamRecords);

    return { activeBatters, activePitchers, benchPlayers, teamTotals };
  }, [selectedTeamRecords]);

  if (!matchup) return null;

  const handleOverlayClick = (e) => {
    if (e.target === e.currentTarget) onClose();
  };

  // List of teams in the matchup for switching
  const matchupTeams = matchup.type === 'trio'
    ? (matchup.teams || [])
    : [matchup.homeTeam, matchup.awayTeam].filter(Boolean);


  // --- TRIO ADVANCED SHADING LOGIC ---
  const getTrioRankClasses = (cat) => {
    if (matchup.type !== 'trio') return {};
    const { teams, teamStats } = matchup;
    const isTie = (v1, v2) => Math.abs(v1 - v2) < 0.0001;

    const vals = teams.map(t => ({
      id: t.id,
      val: parseFloat(teamStats[t.id]?.[cat.id] || 0)
    }));

    vals.sort((a, b) => {
      if (isTie(a.val, b.val)) return 0;
      return cat.higherIsBetter ? b.val - a.val : a.val - b.val;
    });

    const bestVal = vals[0].val;
    const midVal = vals[1].val;
    const worstVal = vals[2].val;

    const is3WayTie = isTie(bestVal, worstVal);
    const isTieFor1st = !is3WayTie && isTie(bestVal, midVal);
    const isTieFor2nd = !is3WayTie && isTie(midVal, worstVal);

    const classes = {};
    vals.forEach(item => {
      if (is3WayTie) {
        classes[item.id] = "bg-gray-100 text-gray-500 font-bold";
      } else if (isTieFor1st) {
        if (isTie(item.val, bestVal)) classes[item.id] = "bg-gradient-to-br from-green-100 to-yellow-100 text-lime-800 font-bold";
        else classes[item.id] = "bg-red-50 text-red-700 font-bold";
      } else if (isTieFor2nd) {
        if (isTie(item.val, bestVal)) classes[item.id] = "bg-green-100 text-green-800 font-bold";
        else classes[item.id] = "bg-gradient-to-br from-yellow-100 to-red-100 text-orange-800 font-bold";
      } else {
        if (isTie(item.val, bestVal)) classes[item.id] = "bg-green-100 text-green-800 font-bold";
        else if (isTie(item.val, midVal)) classes[item.id] = "bg-yellow-100 text-yellow-800 font-bold";
        else classes[item.id] = "bg-red-50 text-red-700 font-bold";
      }
    });
    return classes;
  };

  return (
    <div onClick={handleOverlayClick} className="fixed inset-0 bg-black/75 flex items-center justify-center z-[60] p-3 sm:p-4 backdrop-blur-sm animate-fade-in">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden border border-gray-200">

        {/* MODAL HEADER */}
        <div className="bg-slate-900 text-white p-4 px-6 flex justify-between items-center shrink-0">
          <div>
            <div className="text-[11px] font-bold text-slate-400 uppercase tracking-widest flex items-center gap-2">
              <span>{matchup.type === 'trio' ? '3-Way Matchup Box Score' : 'Head-to-Head Box Score'}</span>
              {matchup.weekName && (
                <span className="bg-slate-800 text-slate-300 px-2 py-0.5 rounded text-[10px] font-mono">
                  {matchup.weekName}
                </span>
              )}
            </div>
            <h2 className="text-lg sm:text-xl font-black mt-0.5 text-white">{matchup.label || 'Matchup Details'}</h2>
          </div>
          <button 
            onClick={onClose} 
            className="text-slate-400 hover:text-white text-3xl leading-none transition-colors p-1 rounded-lg hover:bg-slate-800"
            title="Close (ESC)"
          >
            &times;
          </button>
        </div>

        {/* SCOREBOARD SUMMARY BAR */}
        {matchup.type === 'trio' ? (
          <div className="flex bg-gray-50 border-b border-gray-200 shrink-0 overflow-x-auto divide-x divide-gray-200">
            {matchup.teams.map((team) => {
              const teamResult = matchup.result?.[team.id] || { points: 0, rank: 0 };
              const isWinner = teamResult.rank === 1;
              const displayPts = Number.isInteger(teamResult.points) ? teamResult.points : teamResult.points.toFixed(1);

              return (
                <div 
                  key={team.id} 
                  onClick={() => { setSelectedTeamId(team.id); setActiveTab('players'); }}
                  className={`flex-1 min-w-[140px] p-3 sm:p-4 flex flex-col items-center justify-center cursor-pointer transition-colors hover:bg-blue-50/50 ${currentTeamId === team.id && activeTab === 'players' ? 'bg-blue-50/70 border-b-2 border-blue-600' : isWinner ? 'bg-green-50/30' : ''}`}
                >
                  <TeamAvatar team={team} size="md" />
                  <div className="mt-1.5 text-sm font-bold text-gray-900 text-center leading-tight truncate w-full">{team.name}</div>
                  <div className="text-[10px] text-gray-500 font-medium">{team.owner}</div>
                  <div className={`mt-1 font-mono text-base sm:text-lg font-black ${isWinner ? 'text-green-600' : 'text-gray-700'}`}>
                    {displayPts} pts
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="flex bg-gray-50 border-b border-gray-200 shrink-0">
            {/* Home Team */}
            <div 
              onClick={() => { setSelectedTeamId(matchup.homeTeam?.id); }}
              className={`w-[40%] p-3 sm:p-4 flex flex-col items-center justify-center transition-colors cursor-pointer hover:bg-blue-50/40 ${currentTeamId === matchup.homeTeam?.id && activeTab === 'players' ? 'bg-blue-50/60' : ''}`}
            >
              <TeamAvatar team={matchup.homeTeam} size="lg" />
              <div className="mt-2 text-sm sm:text-base font-bold text-gray-900 text-center leading-tight truncate w-full">{matchup.homeTeam?.name}</div>
              <div className="text-[11px] text-gray-500 font-semibold uppercase tracking-wider">{matchup.homeTeam?.owner}</div>
              <div className={`font-mono text-2xl sm:text-3xl font-black mt-1 ${matchup.result?.homeScore > matchup.result?.awayScore ? 'text-green-600' : 'text-gray-800'}`}>
                {matchup.result?.homeScore ?? 0}
              </div>
            </div>
            
            {/* Center Ties */}
            <div className="w-[20%] flex flex-col items-center justify-center border-x border-gray-200 bg-white p-2">
              <span className="text-[10px] sm:text-xs font-bold text-gray-400 uppercase tracking-widest">Ties</span>
              <span className="font-mono text-base sm:text-xl font-black text-gray-600">{matchup.result?.ties ?? 0}</span>
              <span className="text-[9px] text-gray-400 font-semibold uppercase mt-0.5">FINAL</span>
            </div>

            {/* Away Team */}
            <div 
              onClick={() => { setSelectedTeamId(matchup.awayTeam?.id); }}
              className={`w-[40%] p-3 sm:p-4 flex flex-col items-center justify-center transition-colors cursor-pointer hover:bg-blue-50/40 ${currentTeamId === matchup.awayTeam?.id && activeTab === 'players' ? 'bg-blue-50/60' : ''}`}
            >
              <TeamAvatar team={matchup.awayTeam} size="lg" />
              <div className="mt-2 text-sm sm:text-base font-bold text-gray-900 text-center leading-tight truncate w-full">{matchup.awayTeam?.name}</div>
              <div className="text-[11px] text-gray-500 font-semibold uppercase tracking-wider">{matchup.awayTeam?.owner}</div>
              <div className={`font-mono text-2xl sm:text-3xl font-black mt-1 ${matchup.result?.awayScore > matchup.result?.homeScore ? 'text-green-600' : 'text-gray-800'}`}>
                {matchup.result?.awayScore ?? 0}
              </div>
            </div>
          </div>
        )}

        {/* TAB CONTROLS */}
        <div className="flex bg-gray-100 p-1.5 border-b border-gray-200 gap-2 shrink-0">
          <button
            onClick={() => setActiveTab('matchup')}
            className={`flex-1 py-2 text-xs sm:text-sm font-bold rounded-lg transition-all flex items-center justify-center gap-1.5 ${
              activeTab === 'matchup' 
                ? 'bg-white text-blue-700 shadow-sm border border-gray-200' 
                : 'text-gray-600 hover:text-gray-900 hover:bg-gray-200/60'
            }`}
          >
            <span>📊</span>
            <span>Category Matchup</span>
          </button>
          <button
            onClick={() => setActiveTab('players')}
            className={`flex-1 py-2 text-xs sm:text-sm font-bold rounded-lg transition-all flex items-center justify-center gap-1.5 ${
              activeTab === 'players' 
                ? 'bg-white text-blue-700 shadow-sm border border-gray-200' 
                : 'text-gray-600 hover:text-gray-900 hover:bg-gray-200/60'
            }`}
          >
            <span>⚾</span>
            <span>Player Box Scores</span>
          </button>
        </div>

        {/* TAB 1: CATEGORY BREAKDOWN */}
        {activeTab === 'matchup' && (
          <div className="overflow-y-auto flex-1 bg-white">
            {matchup.type === 'trio' ? (
              <div className="divide-y divide-gray-100">
                {CATEGORIES.map(cat => {
                  const rankClasses = getTrioRankClasses(cat);
                  return (
                    <div key={cat.id} className="flex hover:bg-gray-50 transition-colors">
                      <div className="w-1/4 p-3 flex items-center justify-center border-r border-gray-100 bg-gray-50/50">
                        <span className="text-xs sm:text-sm font-bold text-gray-700">{cat.name}</span>
                      </div>
                      {matchup.teams.map(team => {
                        const val = parseFloat(matchup.teamStats[team.id]?.[cat.id] || 0);
                        const displayVal = Number.isInteger(val) ? val : val.toFixed(3);
                        const bgClass = rankClasses[team.id] || '';
                        return (
                          <div key={team.id} className={`w-1/4 p-3 flex items-center justify-center border-r border-gray-100 last:border-0 ${bgClass}`}>
                            <span className="font-mono text-sm sm:text-base">{displayVal}</span>
                          </div>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="divide-y divide-gray-100">
                {CATEGORIES.map(cat => {
                  const hVal = parseFloat(matchup.homeStats?.[`${cat.id}_raw`] ?? matchup.homeStats?.[cat.id] ?? 0);
                  const aVal = parseFloat(matchup.awayStats?.[`${cat.id}_raw`] ?? matchup.awayStats?.[cat.id] ?? 0);
                  
                  let homeWins = false;
                  let awayWins = false;

                  if (Math.abs(hVal - aVal) > 0.0001) {
                    if (cat.higherIsBetter) {
                      homeWins = hVal > aVal;
                      awayWins = aVal > hVal;
                    } else {
                      homeWins = hVal < aVal;
                      awayWins = aVal < hVal;
                    }
                  }

                  const displayH = Number.isInteger(hVal) ? hVal : (cat.id === 'OBP' ? hVal.toFixed(4) : hVal.toFixed(2));
                  const displayA = Number.isInteger(aVal) ? aVal : (cat.id === 'OBP' ? aVal.toFixed(4) : aVal.toFixed(2));

                  return (
                    <div key={cat.id} className="flex hover:bg-gray-50 transition-colors items-center">
                      <div className={`w-[40%] p-3 text-center font-mono text-sm sm:text-base ${homeWins ? 'bg-green-100 text-green-900 font-bold' : 'text-gray-700'}`}>
                        {displayH}
                      </div>
                      <div className="w-[20%] p-3 text-center border-x border-gray-100 bg-gray-50/70 flex items-center justify-center">
                        <span className="text-xs font-bold text-gray-700 uppercase tracking-wider">{cat.name}</span>
                      </div>
                      <div className={`w-[40%] p-3 text-center font-mono text-sm sm:text-base ${awayWins ? 'bg-green-100 text-green-900 font-bold' : 'text-gray-700'}`}>
                        {displayA}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}

        {/* TAB 2: PLAYER BOX SCORES */}
        {activeTab === 'players' && (
          <div className="overflow-y-auto flex-1 bg-gray-50 flex flex-col">
            
            {/* TEAM SELECTOR PILLS */}
            <div className="bg-white border-b border-gray-200 px-4 py-2.5 flex items-center gap-2 overflow-x-auto shrink-0">
              <span className="text-xs font-bold text-gray-400 uppercase tracking-wider shrink-0 mr-1">Select Team:</span>
              {matchupTeams.map(t => {
                const isSelected = t.id === currentTeamId;
                return (
                  <button
                    key={t.id}
                    onClick={() => setSelectedTeamId(t.id)}
                    className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-bold transition-all border shrink-0 ${
                      isSelected
                        ? 'bg-blue-600 text-white border-blue-600 shadow-sm'
                        : 'bg-gray-50 text-gray-700 border-gray-200 hover:bg-gray-100'
                    }`}
                  >
                    <TeamAvatar team={t} size="sm" />
                    <span>{t.name}</span>
                    <span className="opacity-75 font-normal">({t.owner})</span>
                  </button>
                );
              })}
            </div>

            {/* GHOST TEAM NOTICE */}
            {currentTeamId === 99 ? (
              <div className="p-8 text-center bg-white m-4 rounded-xl border border-gray-200 text-gray-500">
                <span className="text-3xl block mb-2">👻</span>
                <h3 className="font-bold text-gray-800 text-base">League Average (Ghost Team)</h3>
                <p className="text-xs text-gray-500 max-w-md mx-auto mt-1">
                  League Average stats represent the mathematical composite average across all 9 human teams for this period. 
                  Switch to the opponent team above to view individual player lines.
                </p>
              </div>
            ) : selectedTeamRecords.length === 0 ? (
              <div className="p-12 text-center text-gray-400 italic">No player records recorded for this matchup period.</div>
            ) : (
              <div className="p-4 space-y-6">

                {/* BATTING BOX SCORE */}
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                  <div className="bg-slate-800 text-white px-4 py-2.5 flex justify-between items-center">
                    <div className="flex items-center gap-2">
                      <span className="text-sm">🏏</span>
                      <h3 className="font-bold text-xs uppercase tracking-wider">Active Batters ({activeBatters.length})</h3>
                    </div>
                    <span className="text-[10px] text-slate-300 font-mono">
                      OBP: {teamTotals?.OBP || '.0000'} | HR: {teamTotals?.HR || 0} | R: {teamTotals?.R || 0}
                    </span>
                  </div>

                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs border-collapse">
                      <thead>
                        <tr className="bg-gray-50 border-b border-gray-200 text-gray-600 font-bold">
                          <th className="py-2.5 px-3">Player</th>
                          <th className="py-2.5 px-2 text-center">Pos</th>
                          {BATTER_CATS.map(c => (
                            <th 
                              key={c.id} 
                              className={`py-2.5 px-2.5 text-center ${c.isCategory ? 'text-blue-900 bg-blue-50/50 font-black' : 'font-semibold text-gray-500'}`}
                              title={c.isCategory ? `Scoring Category: ${c.label}` : c.label}
                            >
                              {c.label}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {activeBatters.map(p => (
                          <tr key={p.id} className="hover:bg-blue-50/30 transition-colors">
                            <td className="py-2 px-3 font-semibold text-gray-900">
                              <button
                                onClick={() => onPlayerClick && onPlayerClick(p.id, p.name)}
                                className="text-left hover:text-blue-600 hover:underline cursor-pointer flex items-center gap-1.5"
                              >
                                <span>{p.name}</span>
                              </button>
                            </td>
                            <td className="py-2 px-2 text-center text-gray-400 font-mono text-[11px]">{p.slot}</td>
                            {BATTER_CATS.map(c => {
                              const val = p.activeStats[c.id];
                              return (
                                <td 
                                  key={c.id} 
                                  className={`py-2 px-2.5 text-center font-mono ${c.isCategory ? 'font-bold text-gray-900 bg-blue-50/20' : 'text-gray-600'}`}
                                >
                                  {formatDisplayVal(val, c.id)}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                      {teamTotals && (
                        <tfoot>
                          <tr className="bg-slate-100/90 font-black text-gray-900 border-t-2 border-slate-300">
                            <td className="py-2.5 px-3 uppercase text-[11px]">Total</td>
                            <td className="py-2.5 px-2 text-center text-gray-500 text-[11px]">-</td>
                            {BATTER_CATS.map(c => (
                              <td key={c.id} className={`py-2.5 px-2.5 text-center font-mono ${c.isCategory ? 'text-blue-900 bg-blue-100/40' : ''}`}>
                                {formatDisplayVal(teamTotals[c.id], c.id)}
                              </td>
                            ))}
                          </tr>
                        </tfoot>
                      )}
                    </table>
                  </div>
                </div>

                {/* PITCHING BOX SCORE */}
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                  <div className="bg-slate-800 text-white px-4 py-2.5 flex justify-between items-center">
                    <div className="flex items-center gap-2">
                      <span className="text-sm">⚾</span>
                      <h3 className="font-bold text-xs uppercase tracking-wider">Active Pitchers ({activePitchers.length})</h3>
                    </div>
                    <span className="text-[10px] text-slate-300 font-mono">
                      ERA: {teamTotals?.ERA || '0.00'} | WHIP: {teamTotals?.WHIP || '0.00'} | K: {teamTotals?.K || 0}
                    </span>
                  </div>

                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs border-collapse">
                      <thead>
                        <tr className="bg-gray-50 border-b border-gray-200 text-gray-600 font-bold">
                          <th className="py-2.5 px-3">Player</th>
                          <th className="py-2.5 px-2 text-center">Pos</th>
                          {PITCHER_CATS.map(c => (
                            <th 
                              key={c.id} 
                              className={`py-2.5 px-2.5 text-center ${c.isCategory ? 'text-blue-900 bg-blue-50/50 font-black' : 'font-semibold text-gray-500'}`}
                              title={c.isCategory ? `Scoring Category: ${c.label}` : c.label}
                            >
                              {c.label}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {activePitchers.map(p => (
                          <tr key={p.id} className="hover:bg-blue-50/30 transition-colors">
                            <td className="py-2 px-3 font-semibold text-gray-900">
                              <button
                                onClick={() => onPlayerClick && onPlayerClick(p.id, p.name)}
                                className="text-left hover:text-blue-600 hover:underline cursor-pointer flex items-center gap-1.5"
                              >
                                <span>{p.name}</span>
                              </button>
                            </td>
                            <td className="py-2 px-2 text-center text-gray-400 font-mono text-[11px]">{p.slot}</td>
                            {PITCHER_CATS.map(c => {
                              const val = p.activeStats[c.id];
                              return (
                                <td 
                                  key={c.id} 
                                  className={`py-2 px-2.5 text-center font-mono ${c.isCategory ? 'font-bold text-gray-900 bg-blue-50/20' : 'text-gray-600'}`}
                                >
                                  {formatDisplayVal(val, c.id)}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                      {teamTotals && (
                        <tfoot>
                          <tr className="bg-slate-100/90 font-black text-gray-900 border-t-2 border-slate-300">
                            <td className="py-2.5 px-3 uppercase text-[11px]">Total</td>
                            <td className="py-2.5 px-2 text-center text-gray-500 text-[11px]">-</td>
                            {PITCHER_CATS.map(c => (
                              <td key={c.id} className={`py-2.5 px-2.5 text-center font-mono ${c.isCategory ? 'text-blue-900 bg-blue-100/40' : ''}`}>
                                {formatDisplayVal(teamTotals[c.id], c.id)}
                              </td>
                            ))}
                          </tr>
                        </tfoot>
                      )}
                    </table>
                  </div>
                </div>

                {/* BENCH / INACTIVE SECTION */}
                {benchPlayers.length > 0 && (
                  <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                    <button
                      onClick={() => setShowBench(!showBench)}
                      className="w-full bg-gray-50 hover:bg-gray-100 px-4 py-2.5 flex justify-between items-center transition-colors text-left"
                    >
                      <div className="flex items-center gap-2">
                        <span className="text-gray-500 font-bold text-xs uppercase tracking-wider">
                          🪑 Bench / Inactive Roster ({benchPlayers.length})
                        </span>
                        <span className="text-[10px] text-gray-400 font-normal">
                          (Stats from days sitting on bench did not count toward matchup total)
                        </span>
                      </div>
                      <span className="text-xs font-bold text-blue-600">
                        {showBench ? 'Hide ▲' : 'Show ▼'}
                      </span>
                    </button>

                    {showBench && (
                      <div className="p-3 divide-y divide-gray-100 text-xs">
                        {benchPlayers.map(p => (
                          <div key={p.id} className="py-2 px-2 flex items-center justify-between hover:bg-gray-50 rounded">
                            <button
                              onClick={() => onPlayerClick && onPlayerClick(p.id, p.name)}
                              className="font-semibold text-gray-800 hover:text-blue-600 hover:underline"
                            >
                              {p.name}
                            </button>
                            <div className="flex items-center gap-3 font-mono text-gray-500 text-[11px]">
                              <span>Bench: {p.gamesBench}d</span>
                              {p.isPitcher ? (
                                <span>IP: {formatDisplayVal(p.benchStats.IP, 'IP')} | K: {p.benchStats.K}</span>
                              ) : (
                                <span>PA: {p.benchStats.PA} | HR: {p.benchStats.HR} | R: {p.benchStats.R}</span>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

              </div>
            )}
          </div>
        )}

      </div>
    </div>
  );
}