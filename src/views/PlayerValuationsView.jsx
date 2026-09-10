// src/views/PlayerValuationsView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import defaultKeepers from '../data/keeperInput2026.json';
import { TEAMS } from '../schedule';

const FANTASY_MANAGERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"];

export default function PlayerValuationsView({ allStats = [], onPlayerClick, onOwnerClick }) {
  const [modelType, setModelType] = useState('3_YEAR_KEEPER'); // '3_YEAR_KEEPER' or 'SINGLE_SEASON'
  const [valuations, setValuations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Filters
  const [searchText, setSearchText] = useState('');
  const [posFilter, setPosFilter] = useState('ALL');
  const [teamFilter, setTeamFilter] = useState('ALL'); // 'ALL', 'FA', or manager name
  const [priceFilter, setPriceFilter] = useState('ALL'); // 'ALL', 'TIER_TOP', 'TIER_MID', 'TIER_LOW', 'TIER_FREE'
  const [expandedPlayerId, setExpandedPlayerId] = useState(null);

  // Sorting
  const [sortConfig, setSortConfig] = useState({ key: 'model_rank', direction: 'asc' });

  // Compute Fantasy Team Ownership Map
  const playerOwnershipMap = useMemo(() => {
    const map = {}; // playerId -> { owner, teamId, isKeeper }

    // 1. Ingest declared 2026 keepers (45 players)
    (defaultKeepers?.keepers || defaultKeepers || []).forEach(k => {
      const pid = parseInt(k.espn_player_id || k.player_id);
      if (pid) {
        map[pid] = {
          owner: k.owner === 'Dan' ? 'Daniel' : k.owner,
          teamId: k.team_id,
          isKeeper: true,
        };
      }
    });

    // 2. Cross-reference with active season rosters from allStats (latest scoring period)
    if (allStats && allStats.length > 0) {
      const latestRoster = {};
      allStats.forEach(r => {
        const pid = r.player_id;
        if (!pid) return;
        if (!latestRoster[pid] || r.scoring_period_id > latestRoster[pid].period) {
          latestRoster[pid] = { period: r.scoring_period_id, teamId: r.team_id };
        }
      });

      Object.entries(latestRoster).forEach(([pidStr, obj]) => {
        const pid = parseInt(pidStr);
        const teamInfo = TEAMS[obj.teamId];
        if (teamInfo && teamInfo.id !== 99) {
          map[pid] = {
            owner: teamInfo.name === 'Dan' ? 'Daniel' : teamInfo.name,
            teamId: obj.teamId,
            isKeeper: map[pid]?.isKeeper || false,
          };
        }
      });
    }

    return map;
  }, [allStats]);

  // Fetch valuations from Supabase on mount
  useEffect(() => {
    async function fetchValuations() {
      setLoading(true);
      setError(null);
      try {
        const { data, error: fetchErr } = await supabase
          .from('player_valuations')
          .select('*')
          .order('model_rank', { ascending: true })
          .limit(3000);

        if (fetchErr) throw fetchErr;

        if (data && data.length > 0) {
          setValuations(data);
        } else {
          // If empty, fall back to querying player-pool directly
          const { data: poolData } = await supabase
            .from('player-pool')
            .select('*')
            .limit(2000);

          if (poolData) {
            const fallback = poolData.map((p, idx) => {
              const kRank = parseInt(p['Hefty Keeper Rank'] || idx + 1);
              const kPrice = parseInt(p['Hefty Keeper Price'] || 0);
              const eRank = parseInt(p['ESPN Keeper Rank'] || 999);
              const ePrice = Math.max(0, 40 - Math.round(eRank * 0.25));

              return {
                player_id: parseInt(p['ESPN PlayerID']),
                player_name: p.Player,
                team: p.Team,
                position: p.Position,
                model_type: '3_YEAR_KEEPER',
                total_pr: parseFloat(p['Hefty Keeper PR'] || p['Projected PR'] || 0),
                model_rank: kRank,
                model_price: kPrice,
                espn_rank: eRank,
                espn_price: ePrice,
                category_prs: {},
                projected_stats: {},
              };
            });
            setValuations(fallback);
          }
        }
      } catch (err) {
        console.error('Error loading player valuations:', err);
        setError('Failed to load valuations from database.');
      } finally {
        setLoading(false);
      }
    }

    fetchValuations();
  }, []);

  // Filter current valuations by selected model
  const currentModelData = useMemo(() => {
    return valuations.filter((v) => v.model_type === modelType);
  }, [valuations, modelType]);

  // Apply search, position, fantasy team filter, and price filter
  const filteredPlayers = useMemo(() => {
    return currentModelData.filter((p) => {
      // Search
      if (searchText) {
        const query = searchText.toLowerCase();
        const matchesName = p.player_name?.toLowerCase().includes(query);
        const matchesMlbTeam = p.team?.toLowerCase().includes(query);
        const owner = playerOwnershipMap[p.player_id]?.owner?.toLowerCase() || '';
        const matchesOwner = owner.includes(query);
        if (!matchesName && !matchesMlbTeam && !matchesOwner) return false;
      }

      // Position
      if (posFilter !== 'ALL') {
        const pos = p.position || '';
        if (posFilter === 'OF') {
          if (!pos.includes('OF') && !pos.includes('LF') && !pos.includes('CF') && !pos.includes('RF')) return false;
        } else if (posFilter === 'MI') {
          if (!pos.includes('2B') && !pos.includes('SS')) return false;
        } else if (posFilter === 'CI') {
          if (!pos.includes('1B') && !pos.includes('3B')) return false;
        } else {
          if (!pos.includes(posFilter)) return false;
        }
      }

      // Fantasy Team Filter
      const ownership = playerOwnershipMap[p.player_id];
      if (teamFilter === 'FA' && ownership?.owner) return false;
      if (teamFilter !== 'ALL' && teamFilter !== 'FA' && ownership?.owner !== teamFilter) return false;

      // Price Tier Filter
      const price = p.model_price || 0;
      if (priceFilter === 'TIER_TOP' && price < 25) return false;
      if (priceFilter === 'TIER_MID' && (price < 10 || price >= 25)) return false;
      if (priceFilter === 'TIER_LOW' && (price < 1 || price >= 10)) return false;
      if (priceFilter === 'TIER_FREE' && price > 0) return false;

      return true;
    });
  }, [currentModelData, searchText, posFilter, teamFilter, priceFilter, playerOwnershipMap]);

  // Sorting
  const sortedPlayers = useMemo(() => {
    const list = [...filteredPlayers];
    list.sort((a, b) => {
      let aVal = a[sortConfig.key];
      let bVal = b[sortConfig.key];

      if (sortConfig.key === 'player_name') {
        aVal = aVal?.toLowerCase() || '';
        bVal = bVal?.toLowerCase() || '';
        return sortConfig.direction === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
      }

      if (sortConfig.key === 'fantasy_team') {
        const aOwner = playerOwnershipMap[a.player_id]?.owner || 'ZZZ_FA';
        const bOwner = playerOwnershipMap[b.player_id]?.owner || 'ZZZ_FA';
        return sortConfig.direction === 'asc' ? aOwner.localeCompare(bOwner) : bOwner.localeCompare(aOwner);
      }

      const aNum = parseFloat(aVal) || 0;
      const bNum = parseFloat(bVal) || 0;
      return sortConfig.direction === 'asc' ? aNum - bNum : bNum - aNum;
    });
    return list;
  }, [filteredPlayers, sortConfig, playerOwnershipMap]);

  const requestSort = (key) => {
    setSortConfig((prev) => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc',
    }));
  };

  return (
    <div className="space-y-6 animate-fadeIn pb-12">
      {/* Top Banner / Controls */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div className="absolute top-0 right-0 w-96 h-96 bg-blue-500/5 rounded-full blur-3xl pointer-events-none" />

        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 relative z-10">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-2xl">💰</span>
              <h1 className="text-2xl font-black tracking-tight text-white">Keeper Prices & Player Valuations</h1>
            </div>
            <p className="text-slate-400 text-sm mt-1 max-w-2xl">
              Projected auction dollar values, multi-year keeper costs, and fantasy team ownership across all MLB players.
            </p>
          </div>

          {/* Model Toggle */}
          <div className="inline-flex bg-slate-950 p-1.5 rounded-xl border border-slate-800 shadow-inner">
            <button
              onClick={() => setModelType('3_YEAR_KEEPER')}
              className={`px-4 py-2 rounded-lg text-xs font-black uppercase tracking-wider transition-all cursor-pointer ${
                modelType === '3_YEAR_KEEPER'
                  ? 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
              }`}
            >
              3-Year Keeper Model
            </button>
            <button
              onClick={() => setModelType('SINGLE_SEASON')}
              className={`px-4 py-2 rounded-lg text-xs font-black uppercase tracking-wider transition-all cursor-pointer ${
                modelType === 'SINGLE_SEASON'
                  ? 'bg-gradient-to-r from-emerald-600 to-teal-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
              }`}
            >
              Single-Year Redraft
            </button>
          </div>
        </div>

        {/* Model Methodology Info Note */}
        <div className="mt-4 pt-4 border-t border-slate-800/80 flex items-center justify-between text-xs text-slate-400">
          <div className="flex items-center gap-2">
            <span className="text-blue-400 font-semibold">Methodology:</span>
            {modelType === '3_YEAR_KEEPER' ? (
              <span>
                Weighted blend across 3 seasons (<strong>60% 2026</strong> + <strong>30% 2027</strong> + <strong>10% 2028</strong>). Starters & relievers evaluated on 10 scoring categories with QS/SV+H role protection.
              </span>
            ) : (
              <span>
                Workbook Single-Year Methodology: Combines <strong>2026 Actuals YTD</strong> + <strong>FanGraphs ROS Projections</strong>, evaluated across 3 seasons (<strong>60% Y1</strong> + <strong>30% Y2</strong> + <strong>10% Y3</strong>).
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Filter Toolbar */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-md flex flex-wrap items-center justify-between gap-3">
        {/* Positional Tabs */}
        <div className="flex flex-wrap items-center gap-1">
          {['ALL', 'C', '1B', '2B', '3B', 'SS', 'OF', 'SP', 'RP', 'DH'].map((pos) => (
            <button
              key={pos}
              onClick={() => setPosFilter(pos)}
              className={`px-2.5 py-1 rounded-md text-xs font-bold transition-colors cursor-pointer ${
                posFilter === pos
                  ? 'bg-blue-600 text-white shadow-xs'
                  : 'bg-slate-950 text-slate-400 hover:text-white hover:bg-slate-800'
              }`}
            >
              {pos}
            </button>
          ))}
        </div>

        {/* Search & Select Filters */}
        <div className="flex flex-wrap items-center gap-2 w-full md:w-auto">
          <input
            type="text"
            placeholder="Search player, team, owner..."
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 w-full sm:w-48"
          />

          {/* Fantasy Team / Owner Filter */}
          <select
            value={teamFilter}
            onChange={(e) => setTeamFilter(e.target.value)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500 cursor-pointer"
          >
            <option value="ALL">All Fantasy Rosters</option>
            <option value="FA">Free Agents (Unowned)</option>
            {FANTASY_MANAGERS.map((m) => (
              <option key={m} value={m}>
                {m}&apos;s Team
              </option>
            ))}
          </select>

          {/* Price Filter */}
          <select
            value={priceFilter}
            onChange={(e) => setPriceFilter(e.target.value)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500 cursor-pointer"
          >
            <option value="ALL">All Price Tiers</option>
            <option value="TIER_TOP">Elite ($25+)</option>
            <option value="TIER_MID">Mid-Tier ($10 - $24)</option>
            <option value="TIER_LOW">Value ($1 - $9)</option>
            <option value="TIER_FREE">End of Bench ($0)</option>
          </select>
        </div>
      </div>

      {/* Main Valuations Table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl shadow-xl overflow-hidden">
        {loading ? (
          <div className="py-24 text-center">
            <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mb-3"></div>
            <div className="text-slate-400 text-sm font-semibold">Loading player keeper prices & valuations...</div>
          </div>
        ) : error ? (
          <div className="py-16 text-center text-rose-400 text-sm font-semibold">
            {error}
          </div>
        ) : sortedPlayers.length === 0 ? (
          <div className="py-16 text-center text-slate-400 text-sm">
            No players match the selected filters.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black select-none">
                  <th
                    onClick={() => requestSort('model_rank')}
                    className="py-3 px-3 text-center cursor-pointer hover:text-white transition-colors w-16"
                  >
                    Rank {sortConfig.key === 'model_rank' ? (sortConfig.direction === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => requestSort('player_name')}
                    className="py-3 px-4 cursor-pointer hover:text-white transition-colors"
                  >
                    Player {sortConfig.key === 'player_name' ? (sortConfig.direction === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th className="py-3 px-3 text-center">Pos</th>
                  <th className="py-3 px-3 text-center">MLB</th>
                  <th
                    onClick={() => requestSort('fantasy_team')}
                    className="py-3 px-3 text-center cursor-pointer hover:text-white transition-colors"
                  >
                    Fantasy Team {sortConfig.key === 'fantasy_team' ? (sortConfig.direction === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => requestSort('model_price')}
                    className="py-3 px-4 text-right cursor-pointer hover:text-white transition-colors"
                  >
                    Hefty Price {sortConfig.key === 'model_price' ? (sortConfig.direction === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => requestSort('espn_price')}
                    className="py-3 px-4 text-right cursor-pointer hover:text-white transition-colors"
                  >
                    ESPN Price {sortConfig.key === 'espn_price' ? (sortConfig.direction === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th
                    onClick={() => requestSort('total_pr')}
                    className="py-3 px-4 text-right cursor-pointer hover:text-white transition-colors"
                  >
                    Total PR {sortConfig.key === 'total_pr' ? (sortConfig.direction === 'asc' ? '▲' : '▼') : ''}
                  </th>
                  <th className="py-3 px-4 text-center">Category Breakdown</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {sortedPlayers.slice(0, 250).map((player) => {
                  const isExpanded = expandedPlayerId === player.player_id;
                  const cats = player.category_prs || {};
                  const isPitcher = (player.position || '').includes('SP') || (player.position || '').includes('RP');
                  const ownership = playerOwnershipMap[player.player_id];
                  const ownerName = ownership?.owner;

                  return (
                    <React.Fragment key={`${player.player_id}-${player.model_type}`}>
                      <tr
                        onClick={() => setExpandedPlayerId(isExpanded ? null : player.player_id)}
                        className="hover:bg-slate-800/40 transition-colors cursor-pointer group"
                      >
                        {/* Rank */}
                        <td className="py-3 px-3 text-center font-bold text-slate-400 group-hover:text-white">
                          #{player.model_rank}
                        </td>

                        {/* Player */}
                        <td className="py-3 px-4">
                          <span
                            onClick={(e) => {
                              e.stopPropagation();
                              if (onPlayerClick) onPlayerClick(player.player_id, player.player_name);
                            }}
                            className="font-bold text-white hover:text-blue-400 hover:underline cursor-pointer"
                          >
                            {player.player_name}
                          </span>
                        </td>

                        {/* Pos */}
                        <td className="py-3 px-3 text-center">
                          <span className="px-2 py-0.5 rounded text-[11px] font-bold bg-slate-800 text-slate-300 border border-slate-700/60">
                            {player.position || 'UTIL'}
                          </span>
                        </td>

                        {/* MLB Team */}
                        <td className="py-3 px-3 text-center text-slate-400 font-semibold">
                          {player.team || 'FA'}
                        </td>

                        {/* Fantasy Team / Owner */}
                        <td className="py-3 px-3 text-center">
                          {ownerName ? (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                if (onOwnerClick && ownership.teamId) {
                                  onOwnerClick(TEAMS[ownership.teamId]);
                                }
                              }}
                              className={`px-2.5 py-0.5 rounded-full text-[11px] font-black tracking-tight inline-flex items-center gap-1 transition-all ${
                                ownership.isKeeper
                                  ? 'bg-amber-500/20 text-amber-300 border border-amber-500/40 hover:bg-amber-500/30'
                                  : 'bg-blue-500/20 text-blue-300 border border-blue-500/40 hover:bg-blue-500/30'
                              }`}
                              title={ownership.isKeeper ? `${ownerName} (Official Keeper)` : `${ownerName}'s Roster`}
                            >
                              <span>{ownerName}</span>
                            </button>
                          ) : (
                            <span className="text-slate-600 text-[11px] font-medium">
                              Free Agent
                            </span>
                          )}
                        </td>

                        {/* Hefty Price */}
                        <td className="py-3 px-4 text-right font-black text-xs text-white">
                          ${player.model_price}
                        </td>

                        {/* ESPN Price */}
                        <td className="py-3 px-4 text-right text-slate-400 font-medium">
                          ${player.espn_price || 0}{' '}
                          <span className="text-[10px] text-slate-500">
                            (#{player.espn_rank || 999})
                          </span>
                        </td>

                        {/* Total PR */}
                        <td className="py-3 px-4 text-right font-black">
                          <span
                            className={
                              player.total_pr >= 10
                                ? 'text-amber-400'
                                : player.total_pr >= 5
                                ? 'text-blue-400'
                                : player.total_pr >= 0
                                ? 'text-emerald-400'
                                : 'text-slate-500'
                            }
                          >
                            {player.total_pr > 0 ? `+${player.total_pr?.toFixed(2)}` : player.total_pr?.toFixed(2)}
                          </span>
                        </td>

                        {/* Category Badges Preview */}
                        <td className="py-3 px-4 text-center">
                          <div className="flex items-center justify-center gap-1">
                            {!isPitcher ? (
                              <>
                                <CategoryChip label="R" val={cats.R} />
                                <CategoryChip label="HR" val={cats.HR} />
                                <CategoryChip label="RBI" val={cats.RBI} />
                                <CategoryChip label="SB" val={cats.SB} />
                                <CategoryChip label="OBP" val={cats.OBP} />
                              </>
                            ) : (
                              <>
                                <CategoryChip label="K" val={cats.SO} />
                                <CategoryChip label="QS" val={cats.QS} />
                                <CategoryChip label="SV+H" val={cats.SV_HD} />
                                <CategoryChip label="ERA" val={cats.ERA} />
                                <CategoryChip label="WHIP" val={cats.WHIP} />
                              </>
                            )}
                            <span className="text-slate-500 text-[10px] ml-1">
                              {isExpanded ? '▲' : '▼'}
                            </span>
                          </div>
                        </td>
                      </tr>

                      {/* Expanded Details Row */}
                      {isExpanded && (
                        <tr className="bg-slate-950/80 border-b border-slate-800/80">
                          <td colSpan="9" className="p-4">
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                              {/* PR Category Grid */}
                              <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
                                <div className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-2">
                                  Category Rating Contribution
                                </div>
                                <div className="grid grid-cols-5 gap-2 text-center">
                                  {!isPitcher ? (
                                    <>
                                      <CategoryBox label="Runs" val={cats.R} />
                                      <CategoryBox label="Home Runs" val={cats.HR} />
                                      <CategoryBox label="RBI" val={cats.RBI} />
                                      <CategoryBox label="Stolen Bases" val={cats.SB} />
                                      <CategoryBox label="OBP" val={cats.OBP} />
                                    </>
                                  ) : (
                                    <>
                                      <CategoryBox label="Strikeouts" val={cats.SO} />
                                      <CategoryBox label="Quality Starts" val={cats.QS} />
                                      <CategoryBox label="Saves + Holds" val={cats.SV_HD} />
                                      <CategoryBox label="ERA" val={cats.ERA} />
                                      <CategoryBox label="WHIP" val={cats.WHIP} />
                                    </>
                                  )}
                                </div>
                              </div>

                              {/* Underlying Projections Grid - Matches 10 Scored Categories */}
                              <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
                                <div className="text-[11px] font-black uppercase tracking-wider text-slate-400 mb-2">
                                  Underlying FanGraphs Projections (Scored Categories)
                                </div>
                                <div className="grid grid-cols-5 gap-2 text-center text-slate-300">
                                  {!isPitcher ? (
                                    <>
                                      <StatBox label="R" val={player.projected_stats?.R ?? '-'} />
                                      <StatBox label="HR" val={player.projected_stats?.HR ?? '-'} />
                                      <StatBox label="RBI" val={player.projected_stats?.RBI ?? '-'} />
                                      <StatBox label="SB" val={player.projected_stats?.SB ?? '-'} />
                                      <StatBox
                                        label="OBP"
                                        val={player.projected_stats?.OBP != null ? Number(player.projected_stats.OBP).toFixed(3).replace(/^0/, '') : '-'}
                                      />
                                    </>
                                  ) : (
                                    <>
                                      <StatBox label="K" val={player.projected_stats?.SO ?? player.projected_stats?.K ?? '-'} />
                                      <StatBox label="QS" val={player.projected_stats?.QS ?? '-'} />
                                      <StatBox
                                        label="SV+HD"
                                        val={
                                          player.projected_stats?.SVHD != null
                                            ? Math.round(Number(player.projected_stats.SVHD))
                                            : player.projected_stats?.SV != null
                                            ? Math.round((Number(player.projected_stats.SV) || 0) + (Number(player.projected_stats.HLD) || 0))
                                            : '-'
                                        }
                                      />
                                      <StatBox
                                        label="ERA"
                                        val={player.projected_stats?.ERA != null ? Number(player.projected_stats.ERA).toFixed(2) : '-'}
                                      />
                                      <StatBox
                                        label="WHIP"
                                        val={player.projected_stats?.WHIP != null ? Number(player.projected_stats.WHIP).toFixed(2) : '-'}
                                      />
                                    </>
                                  )}
                                </div>
                              </div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {sortedPlayers.length > 250 && (
          <div className="p-4 text-center text-xs text-slate-500 border-t border-slate-800 bg-slate-950">
            Showing top 250 of {sortedPlayers.length} players. Use search, position, or fantasy team filters to narrow down.
          </div>
        )}
      </div>
    </div>
  );
}

// Mini preview chip for table
function CategoryChip({ label, val }) {
  const num = parseFloat(val) || 0;
  const isPos = num > 0.5;
  const isNeg = num < -0.5;

  return (
    <span
      className={`px-1 py-0.5 rounded text-[10px] font-bold ${
        isPos
          ? 'bg-emerald-500/20 text-emerald-400'
          : isNeg
          ? 'bg-rose-500/20 text-rose-400'
          : 'bg-slate-800 text-slate-400'
      }`}
      title={`${label}: ${num > 0 ? '+' : ''}${num.toFixed(2)}`}
    >
      {label}
    </span>
  );
}

// Box in expanded details
function CategoryBox({ label, val }) {
  const num = parseFloat(val) || 0;
  const isPos = num > 0;

  return (
    <div className="bg-slate-950 p-2 rounded border border-slate-800/80">
      <div className="text-[10px] text-slate-500 font-semibold truncate">{label}</div>
      <div
        className={`text-xs font-black mt-0.5 ${
          num >= 1
            ? 'text-emerald-400'
            : num > 0
            ? 'text-emerald-300'
            : num === 0
            ? 'text-slate-400'
            : 'text-rose-400'
        }`}
      >
        {isPos ? `+${num.toFixed(2)}` : num.toFixed(2)}
      </div>
    </div>
  );
}

function StatBox({ label, val }) {
  return (
    <div className="bg-slate-950 p-2 rounded border border-slate-800/80">
      <div className="text-[10px] text-slate-500 font-semibold truncate">{label}</div>
      <div className="text-xs font-bold text-slate-200 mt-0.5">{val ?? '-'}</div>
    </div>
  );
}
