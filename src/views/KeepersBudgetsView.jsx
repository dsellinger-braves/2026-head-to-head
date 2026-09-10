// src/views/KeepersBudgetsView.jsx
import React, { useState, useEffect } from 'react';
import { supabase } from '../supabaseClient';
import defaultTeamBudgets from '../data/teamBudgets2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import KeepersBudgetsPanel from '../components/KeepersBudgetsPanel';

export default function KeepersBudgetsView({
  currentUser = 'Daniel',
  isCommissioner = false,
  seasonYear = 2027,
  onSeasonYearChange,
  onPlayerClick
}) {
  const [teamBudgets, setTeamBudgets] = useState(seasonYear === 2026 ? defaultTeamBudgets : []);
  const [compPicks, setCompPicks] = useState(seasonYear === 2026 ? defaultCompPicks : []);
  const [keepers, setKeepers] = useState(seasonYear === 2026 ? defaultKeepers : []);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  const loadData = React.useCallback(async () => {
    try {
      const [budgetsRes, compRes, keepersRes, poolRes] = await Promise.all([
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
        supabase.from('player-pool').select('*').limit(2000),
      ]);

      if (budgetsRes.data?.length > 0) {
        setTeamBudgets(budgetsRes.data);
      } else if (seasonYear === 2026) {
        setTeamBudgets(defaultTeamBudgets);
      } else {
        setTeamBudgets([]);
      }

      if (compRes.data) {
        setCompPicks(compRes.data);
      } else if (seasonYear === 2026) {
        setCompPicks(defaultCompPicks);
      } else {
        setCompPicks([]);
      }

      if (keepersRes.data) {
        setKeepers(keepersRes.data);
      } else if (seasonYear === 2026) {
        setKeepers(defaultKeepers);
      } else {
        setKeepers([]);
      }

      if (poolRes.data?.length > 0) {
        const mapped = poolRes.data.map(p => ({
          id: parseInt(p['ESPN PlayerID'] || p.id),
          name: p.Player || p.full_name,
          position: p.Position || p.position,
          team: p.Team || p.team,
          rank: parseInt(p['Hefty Keeper Rank'] || p['ESPN Keeper Rank'] || 999),
          cost: parseFloat(p['Hefty Keeper Price'] || 0),
        }));
        setPlayers(mapped);
      }
    } catch (err) {
      console.warn('Using local fallbacks for Keepers & Budgets:', err);
      if (seasonYear === 2026) {
        setTeamBudgets(defaultTeamBudgets);
        setCompPicks(defaultCompPicks);
        setKeepers(defaultKeepers);
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
