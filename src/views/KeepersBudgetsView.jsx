// src/views/KeepersBudgetsView.jsx
import React, { useState, useEffect } from 'react';
import { supabase } from '../supabaseClient';
import defaultTeamBudgets from '../data/teamBudgets2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
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

const defaultBudgetsList = defaultTeamBudgets?.budgets || defaultTeamBudgets || [];
const defaultCompPicksList = defaultCompPicks?.comp_picks || defaultCompPicks || [];
const defaultKeepersList = defaultKeepers?.keepers || defaultKeepers || [];

export default function KeepersBudgetsView({
  currentUser = 'Daniel',
  isCommissioner = false,
  seasonYear = 2027,
  onSeasonYearChange,
  onPlayerClick
}) {
  const [teamBudgets, setTeamBudgets] = useState(seasonYear === 2026 ? defaultBudgetsList : []);
  const [compPicks, setCompPicks] = useState(seasonYear === 2026 ? defaultCompPicksList : []);
  const [keepers, setKeepers] = useState(seasonYear === 2026 ? defaultKeepersList : []);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  const loadData = React.useCallback(async () => {
    try {
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
        supabase.from('player_daily_stats').select('team_id, player_id, full_name, lineup_slot_id').eq('scoring_period_id', 195)
      ]);

      if (budgetsRes.data?.length > 0) {
        setTeamBudgets(budgetsRes.data);
      } else if (seasonYear === 2026) {
        setTeamBudgets(defaultBudgetsList);
      } else {
        setTeamBudgets([]);
      }

      if (compRes.data) {
        setCompPicks(compRes.data);
      } else if (seasonYear === 2026) {
        setCompPicks(defaultCompPicksList);
      } else {
        setCompPicks([]);
      }

      if (keepersRes.data) {
        setKeepers(keepersRes.data);
      } else if (seasonYear === 2026) {
        setKeepers(defaultKeepersList);
      } else {
        setKeepers([]);
      }

      const rawPool = [
        ...(p1?.data || []),
        ...(p2?.data || []),
        ...(p3?.data || []),
        ...(p4?.data || [])
      ];

      // Build active roster lookup from player_daily_stats
      const rosterById = new Map();
      const rosterByName = new Map();
      (pdsRes?.data || []).forEach(r => {
        const owner = TEAM_OWNERS[r.team_id] || `Team ${r.team_id}`;
        if (r.player_id) rosterById.set(String(r.player_id), owner);
        if (r.full_name) rosterByName.set(r.full_name.toLowerCase().trim(), owner);
      });

      if (rawPool.length > 0) {
        const mapped = rawPool.map(p => {
          const pid = String(p['ESPN PlayerID'] || p.id || '');
          const pName = p.Player || p.full_name || '';
          const nameKey = pName.toLowerCase().trim();
          const rosterOwner = rosterById.get(pid) || rosterByName.get(nameKey) || (p.Availability && p.Availability !== 'Available' ? p.Availability : null);
          const isRostered = Boolean(rosterOwner);
          const availability = rosterOwner || 'Available';
          const rank = parseInt(p['Hefty Keeper Rank'] || p['Hefty Single Season Rank'] || p['ESPN Keeper Rank'] || p.rank || 999);
          const rawCost = p['Hefty Keeper Price'] !== undefined && p['Hefty Keeper Price'] !== null && p['Hefty Keeper Price'] !== ''
            ? parseFloat(p['Hefty Keeper Price'])
            : null;
          const cost = rawCost !== null && !isNaN(rawCost) ? rawCost : calculateKeeperCostFromRank(rank);

          return {
            ...p,
            id: parseInt(pid) || pid,
            espn_player_id: pid,
            'ESPN PlayerID': pid,
            name: pName,
            Player: pName,
            full_name: pName,
            position: p.Position || p.position || 'UTIL',
            Position: p.Position || p.position || 'UTIL',
            team: p.Team || p.team || 'FA',
            Team: p.Team || p.team || 'FA',
            rank: isNaN(rank) ? 999 : rank,
            cost: isNaN(cost) ? 0 : cost,
            rosterOwner: rosterOwner,
            Availability: availability,
            isRostered: isRostered,
            isFreeAgent: !isRostered
          };
        });
        setPlayers(mapped);
      }
    } catch (err) {
      console.warn('Using local fallbacks for Keepers & Budgets:', err);
      if (seasonYear === 2026) {
        setTeamBudgets(defaultBudgetsList);
        setCompPicks(defaultCompPicksList);
        setKeepers(defaultKeepersList);
      }
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
      />
    </div>
  );
}
