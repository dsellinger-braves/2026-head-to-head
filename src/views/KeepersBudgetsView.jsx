// src/views/KeepersBudgetsView.jsx
import React, { useState, useEffect } from 'react';
import { supabase } from '../supabaseClient';
import defaultTeamBudgets from '../data/teamBudgets2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import KeepersBudgetsPanel from '../components/KeepersBudgetsPanel';

export default function KeepersBudgetsView({ currentUser = 'Daniel', isCommissioner = false, onPlayerClick }) {
  const [teamBudgets, setTeamBudgets] = useState(defaultTeamBudgets);
  const [compPicks, setCompPicks] = useState(defaultCompPicks);
  const [keepers, setKeepers] = useState(defaultKeepers);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  const loadData = React.useCallback(async () => {
    try {
      const [budgetsRes, compRes, keepersRes, poolRes] = await Promise.all([
        supabase.from('draft_team_budgets').select('*').order('owner', { ascending: true }),
        supabase.from('draft_compensation_picks').select('*').order('round_num', { ascending: true }),
        supabase.from('draft_keepers').select('*').order('team_id', { ascending: true }),
        supabase.from('player-pool').select('*').limit(2000),
      ]);

      if (budgetsRes.data?.length > 0) setTeamBudgets(budgetsRes.data);
      if (compRes.data?.length > 0) setCompPicks(compRes.data);
      if (keepersRes.data?.length > 0) setKeepers(keepersRes.data);

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
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setLoading(true);
    loadData();
  }, [loadData]);

  if (loading) {
    return (
      <div className="py-24 text-center">
        <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-emerald-500 mb-3"></div>
        <div className="text-slate-400 text-sm font-semibold">Loading keepers, budgets & draft compensation...</div>
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
        onPlayerClick={onPlayerClick}
        onRefresh={loadData}
      />
    </div>
  );
}
