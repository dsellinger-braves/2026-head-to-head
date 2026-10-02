// src/views/KeepersBudgetsView.jsx
import React, { useState, useEffect } from 'react';
import { supabase } from '../supabaseClient';
import defaultTeamBudgets2026 from '../data/teamBudgets2026.json';
import defaultTeamBudgets2027 from '../data/teamBudgets2027.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers2026 from '../data/keeperInput2026.json';
import defaultKeepers2027 from '../data/keeperInput2027.json';
import defaultCalculations from '../data/keeperCalculations.json';
import KeepersBudgetsPanel from '../components/KeepersBudgetsPanel';

// Team ID mapping matching 2026 Head to Head league structure
const TEAM_OWNERS = {
  1: 'Tim',
  2: 'Adrian',
  3: 'Garrett',
  5: 'Daniel',
  6: 'Anil',
  8: 'Alex',
  12: 'Will',
  13: 'Mark',
  14: 'Preston'
};

function calculateKeeperCostFromRank(rank) {
  if (!rank || rank > 150) return 0;
  if (rank === 1) return 40;
  if (rank === 2) return 36;
  if (rank === 3) return 33;
  if (rank === 4) return 31;
  if (rank === 5) return 30;
  if (rank === 6) return 29;
  if (rank === 7) return 28;
  if (rank === 8) return 27;
  if (rank === 9) return 26;
  if (rank === 10) return 25;
  if (rank <= 14) return 23;
  if (rank <= 16) return 22;
  if (rank <= 19) return 21;
  if (rank <= 22) return 20;
  if (rank <= 25) return 19;
  if (rank <= 28) return 18;
  if (rank <= 31) return 17;
  if (rank <= 35) return 16;
  if (rank <= 40) return 15;
  if (rank <= 45) return 14;
  if (rank <= 50) return 13;
  if (rank <= 55) return 12;
  if (rank <= 65) return 11;
  if (rank <= 75) return 9;
  if (rank <= 85) return 8;
  if (rank <= 95) return 7;
  if (rank <= 110) return 5;
  if (rank <= 125) return 4;
  if (rank <= 140) return 3;
  if (rank <= 150) return 2;
  return 0;
}

const defaultBudgets2026List = defaultTeamBudgets2026?.budgets || defaultTeamBudgets2026 || [];
const defaultBudgets2027List = defaultTeamBudgets2027?.budgets || defaultTeamBudgets2027 || [];
const defaultCompPicksList = defaultCompPicks?.comp_picks || defaultCompPicks || [];
const defaultKeepers2026List = defaultKeepers2026?.keepers || defaultKeepers2026 || [];
const defaultKeepers2027List = defaultKeepers2027?.keepers || defaultKeepers2027 || [];
const defaultCalcPlayers = defaultCalculations?.players || [];

export default function KeepersBudgetsView({
  currentUser = 'Daniel',
  isCommissioner = false,
  seasonYear = 2027,
  onSeasonYearChange,
  onPlayerClick,
  subTab = 'matrix'
}) {
  const defaultBudgets = seasonYear >= 2027 ? defaultBudgets2027List : defaultBudgets2026List;
  const defaultKeepers = seasonYear >= 2027 ? defaultKeepers2027List : defaultKeepers2026List;

  const [teamBudgets, setTeamBudgets] = useState(defaultBudgets);
  const [compPicks, setCompPicks] = useState(seasonYear === 2026 ? defaultCompPicksList : []);
  const [keepers, setKeepers] = useState(defaultKeepers);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  const loadData = React.useCallback(async () => {
    try {
      // 1. Identify the most recent scoring period available in player_daily_stats
      const { data: latestSpData } = await supabase
        .from('player_daily_stats')
        .select('scoring_period_id')
        .order('scoring_period_id', { ascending: false })
        .limit(1);
      const latestSpId = latestSpData?.[0]?.scoring_period_id || 195;

      const [budgetsRes, compRes, keepersRes, p1, p2, p3, p4, pdsRes] = await Promise.all([
        supabase
          .from('draft_team_budgets')
          .select('*')
          .eq('season_year', seasonYear)
          .order('owner', { ascending: true }),
        supabase
          .from('draft_compensation_picks')
          .select('*')
          .eq('season_year', seasonYear)
          .order('round_num', { ascending: true }),
        supabase
          .from('draft_keepers')
          .select('*')
          .eq('season_year', seasonYear)
          .order('team_id', { ascending: true }),
        supabase.from('player-pool').select('*').range(0, 999),
        supabase.from('player-pool').select('*').range(1000, 1999),
        supabase.from('player-pool').select('*').range(2000, 2999),
        supabase.from('player-pool').select('*').range(3000, 3999),
        supabase
          .from('player_daily_stats')
          .select('team_id, player_id, full_name, lineup_slot_id')
          .eq('scoring_period_id', latestSpId)
      ]);

      if (budgetsRes.data?.length > 0) {
        setTeamBudgets(budgetsRes.data);
      } else {
        setTeamBudgets(seasonYear >= 2027 ? defaultBudgets2027List : defaultBudgets2026List);
      }

      if (compRes.data) {
        setCompPicks(compRes.data);
      } else if (seasonYear === 2026) {
        setCompPicks(defaultCompPicksList);
      } else {
        setCompPicks([]);
      }

      if (keepersRes.data?.length > 0) {
        setKeepers(keepersRes.data);
      } else {
        setKeepers(seasonYear >= 2027 ? defaultKeepers2027List : defaultKeepers2026List);
      }

      const rawPool = [
        ...(p1?.data || []),
        ...(p2?.data || []),
        ...(p3?.data || []),
        ...(p4?.data || [])
      ];

      // Build calculation lookups from official PR calculations
      const calcById = new Map();
      const calcByName = new Map();
      defaultCalcPlayers.forEach(cp => {
        const cpid = String(cp.espn_player_id || cp.player_id || '').trim();
        const cpName = (cp.player_name || '').toLowerCase().replace(/\./g, '').replace(/'/g, '').trim();
        if (cpid) calcById.set(cpid, cp);
        if (cpName) calcByName.set(cpName, cp);
      });

      // Build active roster lookup strictly from the most recent scoring period in player_daily_stats
      const rosterById = new Map();
      const rosterByName = new Map();
      (pdsRes?.data || []).forEach(r => {
        const owner = TEAM_OWNERS[r.team_id] || `Team ${r.team_id}`;
        if (r.player_id) rosterById.set(String(r.player_id), owner);
        if (r.full_name) rosterByName.set(r.full_name.toLowerCase().trim(), owner);
      });

      if (rawPool.length > 0) {
        // Pre-count names to detect duplicate names
        const poolNameCounts = new Map();
        rawPool.forEach(p => {
          const pName = (p.Player || p.full_name || '').toLowerCase().trim();
          if (pName) poolNameCounts.set(pName, (poolNameCounts.get(pName) || 0) + 1);
        });

        const poolIds = new Set();
        const poolNames = new Set();

        const mapped = rawPool.map(p => {
          const pid = String(p['ESPN PlayerID'] || p.id || '').trim();
          const pName = p.Player || p.full_name || '';
          const nameKey = pName.toLowerCase().trim();
          const cleanNameKey = nameKey.replace(/\./g, '').replace(/'/g, '');
          if (pid) poolIds.add(pid);
          if (nameKey) poolNames.add(nameKey);

          const isDupName = (poolNameCounts.get(nameKey) || 0) > 1;

          // Match calculation record by ESPN ID or unique name
          const calcMatch = (pid ? calcById.get(pid) : null) || (!isDupName ? calcByName.get(cleanNameKey) : null) || null;

          // Strictly use the most recent scoring period's active roster
          const rosterOwner = rosterById.get(pid) || (!isDupName ? rosterByName.get(nameKey) : null) || (calcMatch && calcMatch.fantasy_owner !== 'Available' && (!isDupName || pid === String(calcMatch.espn_player_id || calcMatch.player_id)) ? calcMatch.fantasy_owner : null) || null;
          const isRostered = Boolean(rosterOwner);
          const availability = rosterOwner || 'Available';

          let rank = 999;
          let cost = 0;

          if (seasonYear >= 2027) {
            if (calcMatch) {
              rank = calcMatch.overall_rank || 999;
              cost = calcMatch.overall_price !== undefined ? calcMatch.overall_price : calculateKeeperCostFromRank(rank);
            } else {
              rank = 999;
              cost = 0;
            }
          } else {
            rank = parseInt(p['Hefty Keeper Rank'] || p['Hefty Single Season Rank'] || p['ESPN Keeper Rank'] || p.rank || 999);
            const rawCost = p['Hefty Keeper Price'] !== undefined && p['Hefty Keeper Price'] !== null && p['Hefty Keeper Price'] !== ''
              ? parseFloat(p['Hefty Keeper Price'])
              : null;
            cost = rawCost !== null && !isNaN(rawCost) ? rawCost : calculateKeeperCostFromRank(rank);
          }

          return {
            ...p,
            id: parseInt(pid) || pid,
            espn_player_id: pid,
            'ESPN PlayerID': pid,
            name: pName,
            Player: pName,
            full_name: pName,
            position: p.Position || p.position || calcMatch?.pos || 'UTIL',
            Position: p.Position || p.position || calcMatch?.pos || 'UTIL',
            team: p.Team || p.team || calcMatch?.team || 'FA',
            Team: p.Team || p.team || calcMatch?.team || 'FA',
            rank: isNaN(rank) ? 999 : rank,
            cost: isNaN(cost) ? 0 : cost,
            rosterOwner: rosterOwner,
            Availability: availability,
            isRostered: isRostered,
            isFreeAgent: !isRostered
          };
        });

        // Ensure any active roster players in the latest scoring period missing from rawPool are synthesized
        const extraRosterPlayers = [];
        (pdsRes?.data || []).forEach(r => {
          const pid = String(r.player_id || '').trim();
          const pName = r.full_name || '';
          const nameKey = pName.toLowerCase().trim();
          const cleanNameKey = nameKey.replace(/\./g, '').replace(/'/g, '');
          if ((pid && !poolIds.has(pid)) && (nameKey && !poolNames.has(nameKey))) {
            const owner = TEAM_OWNERS[r.team_id] || `Team ${r.team_id}`;
            const calcMatch = (pid ? calcById.get(pid) : null) || calcByName.get(cleanNameKey) || null;
            const rank = calcMatch?.overall_rank || 999;
            const cost = calcMatch?.overall_price !== undefined ? calcMatch.overall_price : 0;
            extraRosterPlayers.push({
              id: parseInt(pid) || pid,
              espn_player_id: pid,
              'ESPN PlayerID': pid,
              name: pName,
              Player: pName,
              full_name: pName,
              position: calcMatch?.pos || 'UTIL',
              Position: calcMatch?.pos || 'UTIL',
              team: calcMatch?.team || 'MLB',
              Team: calcMatch?.team || 'MLB',
              rank: rank,
              cost: cost,
              rosterOwner: owner,
              Availability: owner,
              isRostered: true,
              isFreeAgent: false
            });
          }
        });

        setPlayers([...mapped, ...extraRosterPlayers]);
      } else {
        // Fallback directly to official calculation dataset if player-pool query is empty
        const fallbackMapped = defaultCalcPlayers.map(cp => {
          const pid = String(cp.espn_player_id || cp.player_id || '').trim();
          const pName = cp.player_name || '';
          const nameKey = pName.toLowerCase().trim();
          const rosterOwner = rosterById.get(pid) || rosterByName.get(nameKey) || (cp.fantasy_owner !== 'Available' ? cp.fantasy_owner : null) || null;
          const isRostered = Boolean(rosterOwner);
          return {
            id: parseInt(pid) || pid,
            espn_player_id: pid,
            'ESPN PlayerID': pid,
            name: pName,
            Player: pName,
            full_name: pName,
            position: cp.pos || 'UTIL',
            Position: cp.pos || 'UTIL',
            team: cp.team || 'MLB',
            Team: cp.team || 'MLB',
            rank: cp.overall_rank || 999,
            cost: cp.overall_price !== undefined ? cp.overall_price : 0,
            rosterOwner: rosterOwner,
            Availability: rosterOwner || 'Available',
            isRostered: isRostered,
            isFreeAgent: !isRostered
          };
        });
        setPlayers(fallbackMapped);
      }
    } catch (err) {
      console.warn('Using local fallbacks for Keepers & Budgets:', err);
      setTeamBudgets(seasonYear >= 2027 ? defaultBudgets2027List : defaultBudgets2026List);
      setCompPicks(seasonYear === 2026 ? defaultCompPicksList : []);
      setKeepers(seasonYear >= 2027 ? defaultKeepers2027List : defaultKeepers2026List);
    } finally {
      setLoading(false);
    }
  }, [seasonYear]);

  useEffect(() => {
    setLoading(true);
    loadData();
  }, [loadData]);

  if (loading) {
    return (
      <div className="py-24 text-center">
        <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-emerald-500 mb-3"></div>
        <div className="text-slate-400 text-sm font-semibold">
          Loading {seasonYear} {seasonYear === 2027 ? 'Draft Prep' : 'Draft Archive'} data...
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <KeepersBudgetsPanel
        teamBudgets={teamBudgets}
        compPicks={compPicks}
        keepers={keepers}
        players={players}
        currentUser={currentUser}
        isCommissioner={isCommissioner}
        seasonYear={seasonYear}
        onSeasonYearChange={onSeasonYearChange}
        onPlayerClick={onPlayerClick}
        onRefresh={loadData}
        priorKeepers={defaultKeepers2026List}
        initialTab={subTab}
      />
    </div>
  );
}
