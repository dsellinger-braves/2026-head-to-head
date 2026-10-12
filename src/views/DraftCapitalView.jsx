// src/views/DraftCapitalView.jsx
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { supabase } from '../supabaseClient';
import defaultDraftAssetTrades from '../data/draftAssetTrades2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import { useAuth } from '../context/useAuth';
import { LEAGUE_OWNERS } from '../utils/mlbTeams';

import {
  DRAFT_OWNERS,
  getTeamId,
  canonicalOwnerName,
  enqueueTradeNotification,
  isPickInAssets,
  isPlayerInAssets,
  compute2027DraftPicks
} from './capital/services/tradeNotificationService';

import DraftCapitalHeader from './capital/components/DraftCapitalHeader';
import DraftPickLedgerTab from './capital/tabs/DraftPickLedgerTab';
import OwnerPickLedgersTab from './capital/tabs/OwnerPickLedgersTab';
import TradeHistoryLedgerTab from './capital/tabs/TradeHistoryLedgerTab';
import Historical2026BoardTab from './capital/tabs/Historical2026BoardTab';
import TradeProposalsTab from './capital/tabs/TradeProposalsTab';
import TradeProposalModal from './capital/modals/TradeProposalModal';
import TradeEditModal from './capital/modals/TradeEditModal';

const HASH_BY_CAPITAL_TAB = {
  board: '#/draft/traded-board',
  ledgers: '#/draft/ledgers',
  history: '#/draft/trade-history',
  '2026board': '#/draft/2026-board',
  proposals: '#/draft/trade-proposals'
};

export default function DraftCapitalView({
  currentUser = 'Daniel',
  isCommissioner: propIsCommissioner = false,
  draftYear = 2027,
  onDraftYearChange,
  subTab
}) {
  const { user, profile, isCommissioner: authIsCommissioner, effectiveOwner, logCommissionerAction, governanceTitle = 'Commissioner', isAdrian: authIsAdrian } = useAuth();
  const isCommissioner = propIsCommissioner || authIsCommissioner;
  const isAdrian = authIsAdrian || profile?.team_id === 2 || profile?.owner_name?.toLowerCase() === 'adrian';

  const [draftTrades, setDraftTrades] = useState(defaultDraftAssetTrades);
  const [compPicks, setCompPicks] = useState(defaultCompPicks);
  const [keepers, setKeepers] = useState(defaultKeepers);
  const [proposals, setProposals] = useState([]);
  const [teamRosters, setTeamRosters] = useState({});
  const [loading, setLoading] = useState(true);

  const canonicalCurrentUser = canonicalOwnerName(effectiveOwner || currentUser);
  const [activeSubTab, setActiveSubTab] = useState(() => {
    if (subTab) return subTab;
    return draftYear === 2026 ? '2026board' : 'board';
  });
  const [selectedOwner, setSelectedOwner] = useState(canonicalCurrentUser);
  const [roundFilter, setRoundFilter] = useState('ALL');

  // Trade Proposals Sub-Navigation & Perspectives
  const [inboxTab, setInboxTab] = useState('inbox'); // 'inbox' | 'outbox' | 'commish' | 'all' | 'archive'
  const [viewPerspectiveOwner, setViewPerspectiveOwner] = useState(canonicalCurrentUser);

  const handleSubTabChange = (tabKey) => {
    setActiveSubTab(tabKey);
    const targetHash = HASH_BY_CAPITAL_TAB[tabKey];
    if (targetHash && window.location.hash !== targetHash) {
      window.location.hash = targetHash;
    }
  };

  // Sync activeSubTab when subTab prop changes
  useEffect(() => {
    if (subTab) {
      setActiveSubTab(subTab);
    }
  }, [subTab]);

  // Sync activeSubTab when draftYear prop changes
  useEffect(() => {
    if (!subTab) {
      if (draftYear === 2026 && activeSubTab === 'board') {
        setActiveSubTab('2026board');
      } else if (draftYear === 2027 && activeSubTab === '2026board') {
        setActiveSubTab('board');
      }
    }
  }, [draftYear, activeSubTab, subTab]);

  // Sync viewPerspectiveOwner when effectiveOwner updates
  useEffect(() => {
    if (effectiveOwner) {
      const canon = canonicalOwnerName(effectiveOwner);
      setViewPerspectiveOwner(canon);
      setSelectedOwner(canon);
      setPropSender(canon);
    }
  }, [effectiveOwner]);

  // Trade Proposal Form State (Multi-asset support: Picks, Players, Budget)
  const [propSender, setPropSender] = useState(canonicalCurrentUser);
  const [propTarget, setPropTarget] = useState(() => {
    return DRAFT_OWNERS.find(o => o !== canonicalCurrentUser) || 'Adrian';
  });
  const [offeredAssets, setOfferedAssets] = useState([]);
  const [requestedAssets, setRequestedAssets] = useState([]);

  // Form input staging
  const [selectedOfferedPickRound, setSelectedOfferedPickRound] = useState('');
  const [selectedOfferedPlayerId, setSelectedOfferedPlayerId] = useState('');
  const [selectedOfferedBudget, setSelectedOfferedBudget] = useState('');

  const [selectedRequestedPickRound, setSelectedRequestedPickRound] = useState('');
  const [selectedRequestedPlayerId, setSelectedRequestedPlayerId] = useState('');
  const [selectedRequestedBudget, setSelectedRequestedBudget] = useState('');

  const [tradeNotes, setTradeNotes] = useState('');
  const [submittingTrade, setSubmittingTrade] = useState(false);
  const [actionLoadingId, setActionLoadingId] = useState(null);
  const [proposeModalOpen, setProposeModalOpen] = useState(false);

  // Commissioner Edit Modal State
  const [editingProposal, setEditingProposal] = useState(null);
  const [editOfferedPickRound, setEditOfferedPickRound] = useState('');
  const [editOfferedPlayerId, setEditOfferedPlayerId] = useState('');
  const [editOfferedBudget, setEditOfferedBudget] = useState('');
  const [editRequestedPickRound, setEditRequestedPickRound] = useState('');
  const [editRequestedPlayerId, setEditRequestedPlayerId] = useState('');
  const [editRequestedBudget, setEditRequestedBudget] = useState('');
  const [savingEdit, setSavingEdit] = useState(false);

  // Load All Data including Active Rosters
  const loadData = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const [tradesRes, compRes, keepersRes, proposalsRes, pdsRes, p1, p2, p3, p4] = await Promise.all([
        supabase.from('draft_asset_trades').select('*').order('trade_id', { ascending: true }),
        supabase.from('draft_compensation_picks').select('*').order('round_num', { ascending: true }),
        supabase.from('draft_keepers').select('*').order('team_id', { ascending: true }),
        supabase.from('league_trade_proposals').select('*').order('created_at', { ascending: false }),
        supabase.from('player_daily_stats').select('team_id, player_id, full_name, lineup_slot_id').eq('scoring_period_id', 195),
        supabase.from('player-pool').select('Player, Team, Position, "ESPN PlayerID"').range(0, 999),
        supabase.from('player-pool').select('Player, Team, Position, "ESPN PlayerID"').range(1000, 1999),
        supabase.from('player-pool').select('Player, Team, Position, "ESPN PlayerID"').range(2000, 2999),
        supabase.from('player-pool').select('Player, Team, Position, "ESPN PlayerID"').range(3000, 3999),
      ]);

      if (tradesRes.data?.length > 0) setDraftTrades(tradesRes.data);
      if (compRes.data?.length > 0) setCompPicks(compRes.data);
      if (keepersRes.data?.length > 0) setKeepers(keepersRes.data);
      if (proposalsRes.data) setProposals(proposalsRes.data);

      const rostersByOwner = {};
      DRAFT_OWNERS.forEach(o => { rostersByOwner[o] = []; });

      const poolMap = new Map();
      const rawPool = [
        ...(p1?.data || []),
        ...(p2?.data || []),
        ...(p3?.data || []),
        ...(p4?.data || [])
      ];
      rawPool.forEach(p => {
        if (p['ESPN PlayerID']) poolMap.set(String(p['ESPN PlayerID']), p);
        if (p.Player) poolMap.set(p.Player.toLowerCase().trim(), p);
      });

      (pdsRes?.data || []).forEach(r => {
        const ownerObj = LEAGUE_OWNERS.find(lo => lo.id === r.team_id);
        const ownerName = ownerObj ? (ownerObj.name === 'Dan' ? 'Daniel' : ownerObj.name) : null;
        if (!ownerName || !rostersByOwner[ownerName]) return;

        const poolPlayer = poolMap.get(String(r.player_id)) || poolMap.get(r.full_name?.toLowerCase().trim());
        const pos = poolPlayer?.Position || 'UTIL';
        const team = poolPlayer?.Team || '';
        rostersByOwner[ownerName].push({
          player_id: r.player_id,
          name: r.full_name,
          position: pos,
          team: team,
          label: `${r.full_name} (${pos}${team ? ' - ' + team : ''})`
        });
      });

      Object.keys(rostersByOwner).forEach(k => {
        rostersByOwner[k].sort((a, b) => a.name.localeCompare(b.name));
      });
      setTeamRosters(rostersByOwner);

    } catch (err) {
      console.warn('Using local fallback for draft assets:', err);
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const computedPicks = useMemo(() => {
    return compute2027DraftPicks(draftTrades);
  }, [draftTrades]);

  const senderAvailablePicks = useMemo(() => {
    return computedPicks.filter(p => p.currentOwner === propSender).sort((a, b) => a.round - b.round);
  }, [computedPicks, propSender]);

  const targetAvailablePicks = useMemo(() => {
    return computedPicks.filter(p => p.currentOwner === propTarget).sort((a, b) => a.round - b.round);
  }, [computedPicks, propTarget]);

  const editSenderAvailablePicks = useMemo(() => {
    if (!editingProposal) return [];
    return computedPicks.filter(p => p.currentOwner === editingProposal.proposing_owner).sort((a, b) => a.round - b.round);
  }, [computedPicks, editingProposal]);

  const editTargetAvailablePicks = useMemo(() => {
    if (!editingProposal) return [];
    return computedPicks.filter(p => p.currentOwner === editingProposal.target_owner).sort((a, b) => a.round - b.round);
  }, [computedPicks, editingProposal]);

  const handleAddOfferedPick = (pickVal) => {
    if (!pickVal) return;
    const p = senderAvailablePicks.find(item => {
      const itemKey = `${item.round}:${item.originalOwner}`;
      return item.id === pickVal || itemKey === pickVal || String(item.round) === String(pickVal);
    });
    if (!p || isPickInAssets(p, offeredAssets)) return;
    setOfferedAssets(prev => [...prev, {
      type: 'pick',
      pick_id: p.id || `pick-${p.round}-${p.originalOwner}`,
      round: Number(p.round),
      original_owner: p?.originalOwner || propSender,
      label: `Round ${p.round} Pick ${p?.originalOwner !== propSender ? `(Orig: ${p.originalOwner})` : ''}`
    }]);
    setSelectedOfferedPickRound('');
  };

  const handleAddOfferedPlayer = (playerIdVal) => {
    const pid = parseInt(playerIdVal, 10);
    if (!pid) return;
    const p = (teamRosters[propSender] || []).find(item => Number(item.player_id) === pid);
    if (!p || isPlayerInAssets(p, offeredAssets)) return;
    setOfferedAssets(prev => [...prev, {
      type: 'player',
      player_id: Number(p.player_id),
      name: p.name,
      position: p.position,
      team: p.team,
      label: p.label
    }]);
    setSelectedOfferedPlayerId('');
  };

  const handleAddOfferedBudget = (amtVal) => {
    const amt = parseFloat(amtVal);
    if (!amt || amt <= 0) return;
    setOfferedAssets(prev => [...prev.filter(a => a.type !== 'budget'), {
      type: 'budget',
      amount: amt,
      label: `$${amt} Draft Budget`
    }]);
    setSelectedOfferedBudget('');
  };

  const handleRemoveOfferedAsset = (index) => {
    setOfferedAssets(prev => prev.filter((_, i) => i !== index));
  };

  const handleAddRequestedPick = (pickVal) => {
    if (!pickVal) return;
    const p = targetAvailablePicks.find(item => {
      const itemKey = `${item.round}:${item.originalOwner}`;
      return item.id === pickVal || itemKey === pickVal || String(item.round) === String(pickVal);
    });
    if (!p || isPickInAssets(p, requestedAssets)) return;
    setRequestedAssets(prev => [...prev, {
      type: 'pick',
      pick_id: p.id || `pick-${p.round}-${p.originalOwner}`,
      round: Number(p.round),
      original_owner: p?.originalOwner || propTarget,
      label: `Round ${p.round} Pick ${p?.originalOwner !== propTarget ? `(Orig: ${p.originalOwner})` : ''}`
    }]);
    setSelectedRequestedPickRound('');
  };

  const handleAddRequestedPlayer = (playerIdVal) => {
    const pid = parseInt(playerIdVal, 10);
    if (!pid) return;
    const p = (teamRosters[propTarget] || []).find(item => Number(item.player_id) === pid);
    if (!p || isPlayerInAssets(p, requestedAssets)) return;
    setRequestedAssets(prev => [...prev, {
      type: 'player',
      player_id: Number(p.player_id),
      name: p.name,
      position: p.position,
      team: p.team,
      label: p.label
    }]);
    setSelectedRequestedPlayerId('');
  };

  const handleAddRequestedBudget = (amtVal) => {
    const amt = parseFloat(amtVal);
    if (!amt || amt <= 0) return;
    setRequestedAssets(prev => [...prev.filter(a => a.type !== 'budget'), {
      type: 'budget',
      amount: amt,
      label: `$${amt} Draft Budget`
    }]);
    setSelectedRequestedBudget('');
  };

  const handleRemoveRequestedAsset = (index) => {
    setRequestedAssets(prev => prev.filter((_, i) => i !== index));
  };

  const handlePropSenderChange = (newSender) => {
    setPropSender(newSender);
    setOfferedAssets([]);
    setSelectedOfferedPickRound('');
    setSelectedOfferedPlayerId('');
    setSelectedOfferedBudget('');
    if (propTarget === newSender) {
      const nextTarget = DRAFT_OWNERS.find(o => o !== newSender) || 'Adrian';
      setPropTarget(nextTarget);
      setRequestedAssets([]);
    }
  };

  const handlePropTargetChange = (newTarget) => {
    setPropTarget(newTarget);
    setRequestedAssets([]);
    setSelectedRequestedPickRound('');
    setSelectedRequestedPlayerId('');
    setSelectedRequestedBudget('');
  };

  const builderOfferedPicksCount = useMemo(() => {
    let count = offeredAssets.filter(a => a.type === 'pick').length;
    if (selectedOfferedPickRound && !offeredAssets.some(a => a.type === 'pick' && a.round === parseInt(selectedOfferedPickRound))) {
      count++;
    }
    return count;
  }, [offeredAssets, selectedOfferedPickRound]);

  const builderRequestedPicksCount = useMemo(() => {
    let count = requestedAssets.filter(a => a.type === 'pick').length;
    if (selectedRequestedPickRound && !requestedAssets.some(a => a.type === 'pick' && a.round === parseInt(selectedRequestedPickRound))) {
      count++;
    }
    return count;
  }, [requestedAssets, selectedRequestedPickRound]);

  const hasFormPickDisparity = useMemo(() => {
    return (builderOfferedPicksCount > 0 || builderRequestedPicksCount > 0) &&
      builderOfferedPicksCount !== builderRequestedPicksCount;
  }, [builderOfferedPicksCount, builderRequestedPicksCount]);

  // Handle Propose Trade
  const handleSendProposal = async (e) => {
    if (e) e.preventDefault();
    if (!user) {
      alert('Please log in with Discord in the top navigation bar to propose trades.');
      return;
    }

    if (!isCommissioner && profile?.owner_name?.toLowerCase() !== propSender?.toLowerCase()) {
      alert(`You are logged in as ${profile?.owner_name}. You can only propose trades from your own team.`);
      return;
    }

    if (propSender === propTarget) {
      alert('Sender and receiving manager cannot be the same team.');
      return;
    }

    const finalOffered = [...offeredAssets];
    if (selectedOfferedPickRound) {
      const p = senderAvailablePicks.find(item => {
        const itemKey = `${item.round}:${item.originalOwner}`;
        return item.id === selectedOfferedPickRound || itemKey === selectedOfferedPickRound || String(item.round) === String(selectedOfferedPickRound);
      });
      if (p && !isPickInAssets(p, finalOffered)) {
        finalOffered.push({
          type: 'pick',
          pick_id: p.id || `pick-${p.round}-${p.originalOwner}`,
          round: Number(p.round),
          original_owner: p?.originalOwner || propSender,
          label: `Round ${p.round} Pick ${p?.originalOwner !== propSender ? `(Orig: ${p.originalOwner})` : ''}`
        });
      }
    }
    if (selectedOfferedPlayerId) {
      const pid = parseInt(selectedOfferedPlayerId, 10);
      const p = (teamRosters[propSender] || []).find(item => Number(item.player_id) === pid);
      if (p && !isPlayerInAssets(p, finalOffered)) {
        finalOffered.push({
          type: 'player',
          player_id: Number(p.player_id),
          name: p.name,
          position: p.position,
          team: p.team,
          label: p.label
        });
      }
    }
    if (selectedOfferedBudget && parseFloat(selectedOfferedBudget) > 0) {
      const amt = parseFloat(selectedOfferedBudget);
      if (!finalOffered.some(a => a.type === 'budget')) {
        finalOffered.push({
          type: 'budget',
          amount: amt,
          label: `$${amt} Draft Budget`
        });
      }
    }

    const finalRequested = [...requestedAssets];
    if (selectedRequestedPickRound) {
      const p = targetAvailablePicks.find(item => {
        const itemKey = `${item.round}:${item.originalOwner}`;
        return item.id === selectedRequestedPickRound || itemKey === selectedRequestedPickRound || String(item.round) === String(selectedRequestedPickRound);
      });
      if (p && !isPickInAssets(p, finalRequested)) {
        finalRequested.push({
          type: 'pick',
          pick_id: p.id || `pick-${p.round}-${p.originalOwner}`,
          round: Number(p.round),
          original_owner: p?.originalOwner || propTarget,
          label: `Round ${p.round} Pick ${p?.originalOwner !== propTarget ? `(Orig: ${p.originalOwner})` : ''}`
        });
      }
    }
    if (selectedRequestedPlayerId) {
      const pid = parseInt(selectedRequestedPlayerId, 10);
      const p = (teamRosters[propTarget] || []).find(item => Number(item.player_id) === pid);
      if (p && !isPlayerInAssets(p, finalRequested)) {
        finalRequested.push({
          type: 'player',
          player_id: Number(p.player_id),
          name: p.name,
          position: p.position,
          team: p.team,
          label: p.label
        });
      }
    }
    if (selectedRequestedBudget && parseFloat(selectedRequestedBudget) > 0) {
      const amt = parseFloat(selectedRequestedBudget);
      if (!finalRequested.some(a => a.type === 'budget')) {
        finalRequested.push({
          type: 'budget',
          amount: amt,
          label: `$${amt} Draft Budget`
        });
      }
    }

    if (finalOffered.length === 0 || finalRequested.length === 0) {
      alert('A trade proposal must include at least one offered asset and one requested asset (pick, player, or budget).');
      return;
    }

    setSubmittingTrade(true);
    try {
      const canonicalSender = canonicalOwnerName(propSender);
      const canonicalTarget = canonicalOwnerName(propTarget);
      const senderTeamId = getTeamId(canonicalSender);
      const targetTeamId = getTeamId(canonicalTarget);

      const isCounter = (tradeNotes || '').toLowerCase().includes('counter-offer');
      const eventType = isCounter ? 'countered' : 'proposed';

      const { data: insertedData, error } = await supabase
        .from('league_trade_proposals')
        .insert({
          season_year: 2026,
          proposing_team_id: senderTeamId,
          proposing_owner: canonicalSender,
          target_team_id: targetTeamId,
          target_owner: canonicalTarget,
          offered_assets: finalOffered,
          requested_assets: finalRequested,
          notes: tradeNotes || null,
          status: 'pending',
          proposed_at: new Date().toISOString()
        })
        .select('id')
        .single();

      if (error) throw error;

      await enqueueTradeNotification({
        proposalId: insertedData?.id || null,
        eventType,
        senderTeamId,
        senderOwner: canonicalSender,
        recipientTeamId: targetTeamId,
        recipientOwner: canonicalTarget,
        offeredAssets: finalOffered,
        requestedAssets: finalRequested,
        notes: tradeNotes || '',
      });

      alert(`Official trade proposal sent to ${canonicalTarget}! 🤝 Discord notification queued.`);
      setOfferedAssets([]);
      setRequestedAssets([]);
      setSelectedOfferedPickRound('');
      setSelectedOfferedPlayerId('');
      setSelectedOfferedBudget('');
      setSelectedRequestedPickRound('');
      setSelectedRequestedPlayerId('');
      setSelectedRequestedBudget('');
      setTradeNotes('');
      setProposeModalOpen(false);
      setInboxTab('outbox');
      await loadData(true);
    } catch (err) {
      console.error('Failed to submit proposal:', err);
      alert('Error submitting proposal: ' + err.message);
    } finally {
      setSubmittingTrade(false);
    }
  };

  const handleCounterOffer = (prop) => {
    setPropSender(prop.target_owner);
    setPropTarget(prop.proposing_owner);
    setOfferedAssets([...(prop.requested_assets || [])]);
    setRequestedAssets([...(prop.offered_assets || [])]);
    setSelectedOfferedPickRound('');
    setSelectedOfferedPlayerId('');
    setSelectedOfferedBudget('');
    setSelectedRequestedPickRound('');
    setSelectedRequestedPlayerId('');
    setSelectedRequestedBudget('');
    setTradeNotes(`Counter-offer to proposal from ${prop.proposing_owner}`);
    setProposeModalOpen(true);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const handlePartnerAccept = async (proposal) => {
    if (!user) {
      alert('Please log in with Discord to accept trades.');
      return;
    }

    const isMe = profile?.owner_name?.toLowerCase() === proposal.target_owner?.toLowerCase() ||
      (profile?.owner_name === 'Dan' && proposal.target_owner === 'Daniel') ||
      (profile?.owner_name === 'Daniel' && proposal.target_owner === 'Dan');

    if (!isCommissioner && !isMe) {
      alert(`Only ${proposal.target_owner}, Commissioner (Adrian), or Admin (Dan) can accept this proposal.`);
      return;
    }

    setActionLoadingId(proposal.id);
    try {
      const { error } = await supabase
        .from('league_trade_proposals')
        .update({
          status: 'accepted_by_partner',
          responded_at: new Date().toISOString(),
          responded_by: profile?.owner_name || 'Owner'
        })
        .eq('id', proposal.id);

      if (error) throw error;

      await enqueueTradeNotification({
        proposalId: proposal.id,
        eventType: 'accepted',
        senderTeamId: proposal.target_team_id || getTeamId(proposal.target_owner),
        senderOwner: proposal.target_owner,
        recipientTeamId: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
        recipientOwner: proposal.proposing_owner,
        offeredAssets: proposal.offered_assets,
        requestedAssets: proposal.requested_assets,
        notes: proposal.notes || '',
      });

      const commishTeamIds = [5, 2];
      for (const cId of commishTeamIds) {
        if (cId !== proposal.proposing_team_id && cId !== proposal.target_team_id) {
          await enqueueTradeNotification({
            proposalId: proposal.id,
            eventType: 'accepted',
            senderTeamId: proposal.target_team_id || getTeamId(proposal.target_owner),
            senderOwner: proposal.target_owner,
            recipientTeamId: cId,
            recipientOwner: cId === 5 ? 'Dan' : 'Adrian',
            offeredAssets: proposal.offered_assets,
            requestedAssets: proposal.requested_assets,
            notes: `[Governance Notice] Trade agreed between ${proposal.proposing_owner} and ${proposal.target_owner}. Ready for your review & execution!`,
          });
        }
      }

      alert('Trade accepted! 🎉 Discord notifications queued, and sent to Commissioner & Admin for final league approval.');
      await loadData(true);
    } catch (err) {
      console.error('Accept failed:', err);
      alert('Failed to accept proposal: ' + err.message);
    } finally {
      setActionLoadingId(null);
    }
  };

  const handleCommissionerApprove = async (proposal) => {
    if (!isCommissioner) {
      alert('Only Commissioner (Adrian) or Admin (Dan) can execute official trade approval.');
      return;
    }

    setActionLoadingId(proposal.id);
    try {
      const tradeId = 'TR-2027-' + Date.now().toString().slice(-6);
      const tradeDate = new Date().toISOString().split('T')[0];
      const newAssetTrades = [];

      (proposal.offered_assets || []).forEach(asset => {
        if (asset.type === 'pick') {
          newAssetTrades.push({
            trade_id: tradeId,
            trade_date: tradeDate,
            season_year: 2026,
            target_draft_year: 2027,
            sending_owner: proposal.proposing_owner,
            from_team_id: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
            receiving_owner: proposal.target_owner,
            to_team_id: proposal.target_team_id || getTeamId(proposal.target_owner),
            asset_type: 'Overall Pick',
            asset_name: `Round ${asset.round}`,
            round_num: asset.round,
            original_owner: asset.original_owner || proposal.proposing_owner,
            notes: proposal.notes || `Trade between ${proposal.proposing_owner} and ${proposal.target_owner}`
          });
        } else if (asset.type === 'budget') {
          newAssetTrades.push({
            trade_id: tradeId,
            trade_date: tradeDate,
            season_year: 2026,
            target_draft_year: 2027,
            sending_owner: proposal.proposing_owner,
            from_team_id: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
            receiving_owner: proposal.target_owner,
            to_team_id: proposal.target_team_id || getTeamId(proposal.target_owner),
            asset_type: 'Budget',
            asset_name: `$${asset.amount} Budget`,
            round_num: null,
            original_owner: proposal.proposing_owner,
            notes: proposal.notes || `Budget transfer`
          });
        } else if (asset.type === 'player') {
          newAssetTrades.push({
            trade_id: tradeId,
            trade_date: tradeDate,
            season_year: 2026,
            target_draft_year: 2027,
            sending_owner: proposal.proposing_owner,
            from_team_id: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
            receiving_owner: proposal.target_owner,
            to_team_id: proposal.target_team_id || getTeamId(proposal.target_owner),
            asset_type: 'Player',
            asset_name: asset.name || asset.label,
            round_num: null,
            original_owner: proposal.proposing_owner,
            notes: proposal.notes || `Player trade: ${asset.name || asset.label}`
          });
        }
      });

      (proposal.requested_assets || []).forEach(asset => {
        if (asset.type === 'pick') {
          newAssetTrades.push({
            trade_id: tradeId,
            trade_date: tradeDate,
            season_year: 2026,
            target_draft_year: 2027,
            sending_owner: proposal.target_owner,
            from_team_id: proposal.target_team_id || getTeamId(proposal.target_owner),
            receiving_owner: proposal.proposing_owner,
            to_team_id: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
            asset_type: 'Overall Pick',
            asset_name: `Round ${asset.round}`,
            round_num: asset.round,
            original_owner: asset.original_owner || proposal.target_owner,
            notes: proposal.notes || `Trade between ${proposal.proposing_owner} and ${proposal.target_owner}`
          });
        } else if (asset.type === 'budget') {
          newAssetTrades.push({
            trade_id: tradeId,
            trade_date: tradeDate,
            season_year: 2026,
            target_draft_year: 2027,
            sending_owner: proposal.target_owner,
            from_team_id: proposal.target_team_id || getTeamId(proposal.target_owner),
            receiving_owner: proposal.proposing_owner,
            to_team_id: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
            asset_type: 'Budget',
            asset_name: `$${asset.amount} Budget`,
            round_num: null,
            original_owner: proposal.target_owner,
            notes: proposal.notes || `Budget transfer`
          });
        } else if (asset.type === 'player') {
          newAssetTrades.push({
            trade_id: tradeId,
            trade_date: tradeDate,
            season_year: 2026,
            target_draft_year: 2027,
            sending_owner: proposal.target_owner,
            from_team_id: proposal.target_team_id || getTeamId(proposal.target_owner),
            receiving_owner: proposal.proposing_owner,
            to_team_id: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
            asset_type: 'Player',
            asset_name: asset.name || asset.label,
            round_num: null,
            original_owner: proposal.target_owner,
            notes: proposal.notes || `Player trade: ${asset.name || asset.label}`
          });
        }
      });

      if (newAssetTrades.length > 0) {
        const { error: insErr } = await supabase
          .from('draft_asset_trades')
          .insert(newAssetTrades);
        if (insErr) throw insErr;
      }

      const { error: propErr } = await supabase
        .from('league_trade_proposals')
        .update({
          status: 'approved',
          responded_at: new Date().toISOString(),
          responded_by: `${profile?.owner_name || governanceTitle} (${isAdrian ? 'Commissioner' : 'Admin'} Approved)`
        })
        .eq('id', proposal.id);

      if (propErr) throw propErr;

      await enqueueTradeNotification({
        proposalId: proposal.id,
        eventType: 'approved',
        senderTeamId: profile?.team_id || 5,
        senderOwner: profile?.owner_name || (isAdrian ? 'Commissioner' : 'Admin'),
        recipientTeamId: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
        recipientOwner: proposal.proposing_owner,
        offeredAssets: proposal.offered_assets,
        requestedAssets: proposal.requested_assets,
        notes: `Trade approved & executed by ${isAdrian ? 'Commissioner' : 'Admin'} ${profile?.owner_name || ''}`,
      });
      await enqueueTradeNotification({
        proposalId: proposal.id,
        eventType: 'approved',
        senderTeamId: profile?.team_id || 5,
        senderOwner: profile?.owner_name || (isAdrian ? 'Commissioner' : 'Admin'),
        recipientTeamId: proposal.target_team_id || getTeamId(proposal.target_owner),
        recipientOwner: proposal.target_owner,
        offeredAssets: proposal.offered_assets,
        requestedAssets: proposal.requested_assets,
        notes: `Trade approved & executed by ${isAdrian ? 'Commissioner' : 'Admin'} ${profile?.owner_name || ''}`,
      });

      if (logCommissionerAction) {
        await logCommissionerAction({
          actionType: 'approve_trade',
          actionDescription: `Approved & executed trade ${proposal.id} between ${proposal.proposing_owner} and ${proposal.target_owner}`,
          targetOwner: proposal.target_owner,
          targetTeamId: proposal.target_team_id,
          details: {
            proposal_id: proposal.id,
            offered_assets: proposal.offered_assets,
            requested_assets: proposal.requested_assets
          }
        });
      }

      alert(`Trade officially APPROVED and EXECUTED! 👑 The 2027 draft board, players, and team ledgers have been updated.`);
      await loadData(true);
    } catch (err) {
      console.error('Approval failed:', err);
      alert('Failed to approve trade: ' + err.message);
    } finally {
      setActionLoadingId(null);
    }
  };

  const handleDeclineOrCancel = async (proposal, newStatus) => {
    if (!user) {
      alert('Please log in with Discord.');
      return;
    }

    setActionLoadingId(proposal.id);
    try {
      const { error } = await supabase
        .from('league_trade_proposals')
        .update({
          status: newStatus,
          responded_at: new Date().toISOString(),
          responded_by: profile?.owner_name || 'User'
        })
        .eq('id', proposal.id);

      if (error) throw error;

      if (newStatus === 'declined') {
        await enqueueTradeNotification({
          proposalId: proposal.id,
          eventType: 'declined',
          senderTeamId: proposal.target_team_id || getTeamId(proposal.target_owner),
          senderOwner: proposal.target_owner,
          recipientTeamId: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
          recipientOwner: proposal.proposing_owner,
          offeredAssets: proposal.offered_assets,
          requestedAssets: proposal.requested_assets,
          notes: proposal.notes || '',
        });
      }

      alert(`Trade proposal ${newStatus}.`);
      await loadData(true);
    } catch (err) {
      console.error('Update failed:', err);
      alert('Error: ' + err.message);
    } finally {
      setActionLoadingId(null);
    }
  };

  const handleOpenEditModal = (prop) => {
    setEditingProposal({
      ...prop,
      offered_assets: [...(prop.offered_assets || [])],
      requested_assets: [...(prop.requested_assets || [])],
      notes: prop.notes || ''
    });
    setEditOfferedPickRound('');
    setEditOfferedPlayerId('');
    setEditOfferedBudget('');
    setEditRequestedPickRound('');
    setEditRequestedPlayerId('');
    setEditRequestedBudget('');
  };

  const handleSaveProposalEdit = async () => {
    if (!editingProposal) return;
    setSavingEdit(true);
    try {
      const pTeamId = getTeamId(editingProposal.proposing_owner);
      const tTeamId = getTeamId(editingProposal.target_owner);

      const { error } = await supabase
        .from('league_trade_proposals')
        .update({
          proposing_owner: editingProposal.proposing_owner,
          proposing_team_id: pTeamId,
          target_owner: editingProposal.target_owner,
          target_team_id: tTeamId,
          offered_assets: editingProposal.offered_assets,
          requested_assets: editingProposal.requested_assets,
          notes: editingProposal.notes,
          status: editingProposal.status,
          updated_at: new Date().toISOString()
        })
        .eq('id', editingProposal.id);

      if (error) throw error;

      if (logCommissionerAction) {
        await logCommissionerAction({
          actionType: 'edit_trade',
          actionDescription: `${isAdrian ? 'Commissioner' : 'Admin'} edited trade proposal ${editingProposal.id} (${editingProposal.proposing_owner} <-> ${editingProposal.target_owner})`,
          targetOwner: editingProposal.target_owner,
          targetTeamId: tTeamId,
          details: {
            proposal_id: editingProposal.id,
            status: editingProposal.status,
            offered_assets: editingProposal.offered_assets,
            requested_assets: editingProposal.requested_assets,
            notes: editingProposal.notes
          }
        });
      }

      alert(`Proposal updated successfully by ${isAdrian ? 'Commissioner. 👑' : 'Admin. ⚡'}`);
      setEditingProposal(null);
      await loadData(true);
    } catch (err) {
      console.error('Failed to update proposal:', err);
      alert('Error updating proposal: ' + err.message);
    } finally {
      setSavingEdit(false);
    }
  };

  const handleDeleteExecutedTrade = async (tradeRow) => {
    if (!isCommissioner) return;
    if (!window.confirm(`Are you sure you want to delete this executed trade record?\n\nAsset: ${tradeRow.asset_name}\nFrom: ${tradeRow.sending_owner} ➔ To: ${tradeRow.receiving_owner}`)) {
      return;
    }
    try {
      const { error } = await supabase
        .from('draft_asset_trades')
        .delete()
        .eq('id', tradeRow.id);
      if (error) throw error;

      if (logCommissionerAction) {
        await logCommissionerAction({
          actionType: 'delete_trade',
          actionDescription: `Deleted executed trade record ${tradeRow.trade_id || tradeRow.id} (${tradeRow.asset_name})`,
          targetOwner: tradeRow.receiving_owner,
          targetTeamId: tradeRow.to_team_id,
          details: { trade: tradeRow }
        });
      }

      alert('Executed trade record deleted from draft_asset_trades.');
      await loadData(true);
    } catch (err) {
      alert('Failed to delete trade record: ' + err.message);
    }
  };

  const ownerStats = useMemo(() => {
    const stats = {};
    DRAFT_OWNERS.forEach(owner => {
      const ownedPicks = computedPicks.filter(p => p.currentOwner === owner);
      const acquired = computedPicks.filter(p => p.currentOwner === owner && p.originalOwner !== owner);
      const tradedAway = computedPicks.filter(p => p.originalOwner === owner && p.currentOwner !== owner);
      stats[owner] = {
        owner,
        total: ownedPicks.length,
        diff: ownedPicks.length - 26,
        acquired,
        tradedAway,
        ownedPicks,
      };
    });
    return stats;
  }, [computedPicks]);

  const assetTradesList = useMemo(() => {
    return (draftTrades || []).filter(t =>
      t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget' || t.asset_type === 'Player'
    );
  }, [draftTrades]);

  const rounds = useMemo(() => {
    const list = [];
    for (let r = 7; r <= 32; r++) list.push(r);
    if (roundFilter === 'ALL') return list;
    if (roundFilter === 'EARLY') return list.filter(r => r <= 15);
    if (roundFilter === 'MID') return list.filter(r => r >= 16 && r <= 23);
    if (roundFilter === 'LATE') return list.filter(r => r >= 24);
    return list;
  }, [roundFilter]);

  const myNormPerspective = useMemo(() => {
    return canonicalOwnerName(viewPerspectiveOwner).toLowerCase();
  }, [viewPerspectiveOwner]);

  const incomingProposals = useMemo(() => {
    return proposals.filter(p => {
      if (p.status !== 'pending') return false;
      const targetNorm = canonicalOwnerName(p.target_owner).toLowerCase();
      return targetNorm === myNormPerspective;
    });
  }, [proposals, myNormPerspective]);

  const outgoingProposals = useMemo(() => {
    return proposals.filter(p => {
      if (p.status !== 'pending') return false;
      const propNorm = canonicalOwnerName(p.proposing_owner).toLowerCase();
      return propNorm === myNormPerspective;
    });
  }, [proposals, myNormPerspective]);

  const commishQueueProposals = useMemo(() => {
    return proposals.filter(p => p.status === 'accepted_by_partner');
  }, [proposals]);

  const leagueAcceptedTrades = useMemo(() => {
    return proposals.filter(p => p.status === 'accepted_by_partner' || p.status === 'approved');
  }, [proposals]);

  const archivedProposals = useMemo(() => {
    if (isCommissioner) {
      return proposals.filter(p => p.status === 'approved' || p.status === 'declined' || p.status === 'cancelled');
    }
    return proposals.filter(p => {
      if (p.status === 'approved') return true;
      const isMine = canonicalOwnerName(p.proposing_owner).toLowerCase() === myNormPerspective ||
                     canonicalOwnerName(p.target_owner).toLowerCase() === myNormPerspective;
      return isMine && (p.status === 'declined' || p.status === 'cancelled');
    });
  }, [proposals, isCommissioner, myNormPerspective]);

  const pendingCount = useMemo(() => {
    return proposals.filter(p => p.status === 'pending' || p.status === 'accepted_by_partner').length;
  }, [proposals]);

  const renderAssetBadge = (asset, onRemove = null) => {
    const isPick = asset.type === 'pick';
    const isPlayer = asset.type === 'player';
    const isBudget = asset.type === 'budget';

    let colorClasses = 'bg-slate-800 border-slate-700 text-slate-200';
    let icon = '📦';

    if (isPick) {
      colorClasses = 'bg-amber-500/15 border-amber-500/40 text-amber-300';
      icon = '🎟️';
    } else if (isPlayer) {
      colorClasses = 'bg-sky-500/15 border-sky-500/40 text-sky-300';
      icon = '👤';
    } else if (isBudget) {
      colorClasses = 'bg-emerald-500/15 border-emerald-500/40 text-emerald-300';
      icon = '💵';
    }

    return (
      <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-bold border ${colorClasses}`}>
        <span>{icon}</span>
        <span>{asset.label || asset.name}</span>
        {onRemove && (
          <button
            type="button"
            onClick={onRemove}
            className="ml-1 text-slate-400 hover:text-rose-400 font-black cursor-pointer leading-none"
            title="Remove asset"
          >
            ✕
          </button>
        )}
      </span>
    );
  };

  if (loading) {
    return (
      <div className="py-24 text-center">
        <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-teal-500 mb-3"></div>
        <div className="text-slate-400 text-sm font-semibold">Loading draft capital & future picks...</div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header Banner */}
      <DraftCapitalHeader
        activeSubTab={activeSubTab}
        onSubTabChange={handleSubTabChange}
        onDraftYearChange={onDraftYearChange}
        assetTradesList={assetTradesList}
        incomingProposals={incomingProposals}
        pendingCount={pendingCount}
        DRAFT_OWNERS={DRAFT_OWNERS}
        ownerStats={ownerStats}
        currentUser={currentUser}
        selectedOwner={selectedOwner}
        setSelectedOwner={setSelectedOwner}
      />

      {/* SUBTAB 1: 2027 TRADED PICK BOARD */}
      {activeSubTab === 'board' && (
        <DraftPickLedgerTab
          roundFilter={roundFilter}
          setRoundFilter={setRoundFilter}
          DRAFT_OWNERS={DRAFT_OWNERS}
          rounds={rounds}
          computedPicks={computedPicks}
        />
      )}

      {/* SUBTAB 2: OWNER LEDGERS */}
      {activeSubTab === 'ledgers' && (
        <OwnerPickLedgersTab
          DRAFT_OWNERS={DRAFT_OWNERS}
          selectedOwner={selectedOwner}
          setSelectedOwner={setSelectedOwner}
          ownerStats={ownerStats}
        />
      )}

      {/* SUBTAB 3: TRADE HISTORY LOG */}
      {activeSubTab === 'history' && (
        <TradeHistoryLedgerTab
          assetTradesList={assetTradesList}
          isCommissioner={isCommissioner}
          isAdrian={isAdrian}
          handleDeleteExecutedTrade={handleDeleteExecutedTrade}
        />
      )}

      {/* SUBTAB 4: 2026 GROUND TRUTH BOARD */}
      {activeSubTab === '2026board' && (
        <Historical2026BoardTab
          roundFilter={roundFilter}
          setRoundFilter={setRoundFilter}
          DRAFT_OWNERS={DRAFT_OWNERS}
          compPicks={compPicks}
          keepers={keepers}
        />
      )}

      {/* SUBTAB 5: TRADE PROPOSALS & COMMISSIONER APPROVALS */}
      {activeSubTab === 'proposals' && (
        <TradeProposalsTab
          viewPerspectiveOwner={viewPerspectiveOwner}
          setViewPerspectiveOwner={setViewPerspectiveOwner}
          setPropSender={setPropSender}
          DRAFT_OWNERS={DRAFT_OWNERS}
          proposeModalOpen={proposeModalOpen}
          setProposeModalOpen={setProposeModalOpen}
          inboxTab={inboxTab}
          setInboxTab={setInboxTab}
          incomingProposals={incomingProposals}
          outgoingProposals={outgoingProposals}
          commishQueueProposals={commishQueueProposals}
          leagueAcceptedTrades={leagueAcceptedTrades}
          archivedProposals={archivedProposals}
          profile={profile}
          isCommissioner={isCommissioner}
          actionLoadingId={actionLoadingId}
          renderAssetBadge={renderAssetBadge}
          handlePartnerAccept={handlePartnerAccept}
          handleCounterOffer={handleCounterOffer}
          handleDeclineOrCancel={handleDeclineOrCancel}
          handleCommissionerApprove={handleCommissionerApprove}
          handleOpenEditModal={handleOpenEditModal}
        />
      )}

      {/* Collapsible Proposal Form Modal */}
      <TradeProposalModal
        isOpen={proposeModalOpen}
        onClose={() => setProposeModalOpen(false)}
        user={user}
        isCommissioner={isCommissioner}
        isAdrian={isAdrian}
        propSender={propSender}
        handlePropSenderChange={handlePropSenderChange}
        propTarget={propTarget}
        handlePropTargetChange={handlePropTargetChange}
        DRAFT_OWNERS={DRAFT_OWNERS}
        getTeamId={getTeamId}
        senderAvailablePicks={senderAvailablePicks}
        targetAvailablePicks={targetAvailablePicks}
        teamRosters={teamRosters}
        offeredAssets={offeredAssets}
        requestedAssets={requestedAssets}
        selectedOfferedPickRound={selectedOfferedPickRound}
        setSelectedOfferedPickRound={setSelectedOfferedPickRound}
        handleAddOfferedPick={handleAddOfferedPick}
        handleRemoveOfferedAsset={handleRemoveOfferedAsset}
        selectedOfferedPlayerId={selectedOfferedPlayerId}
        setSelectedOfferedPlayerId={setSelectedOfferedPlayerId}
        handleAddOfferedPlayer={handleAddOfferedPlayer}
        selectedOfferedBudget={selectedOfferedBudget}
        setSelectedOfferedBudget={setSelectedOfferedBudget}
        handleAddOfferedBudget={handleAddOfferedBudget}
        selectedRequestedPickRound={selectedRequestedPickRound}
        setSelectedRequestedPickRound={setSelectedRequestedPickRound}
        handleAddRequestedPick={handleAddRequestedPick}
        handleRemoveRequestedAsset={handleRemoveRequestedAsset}
        selectedRequestedPlayerId={selectedRequestedPlayerId}
        setSelectedRequestedPlayerId={setSelectedRequestedPlayerId}
        handleAddRequestedPlayer={handleAddRequestedPlayer}
        selectedRequestedBudget={selectedRequestedBudget}
        setSelectedRequestedBudget={setSelectedRequestedBudget}
        handleAddRequestedBudget={handleAddRequestedBudget}
        hasFormPickDisparity={hasFormPickDisparity}
        builderOfferedPicksCount={builderOfferedPicksCount}
        builderRequestedPicksCount={builderRequestedPicksCount}
        tradeNotes={tradeNotes}
        setTradeNotes={setTradeNotes}
        handleSendProposal={handleSendProposal}
        submittingTrade={submittingTrade}
        renderAssetBadge={renderAssetBadge}
        isPickInAssets={isPickInAssets}
        isPlayerInAssets={isPlayerInAssets}
      />

      {/* Commissioner Edit Modal */}
      <TradeEditModal
        editingProposal={editingProposal}
        setEditingProposal={setEditingProposal}
        DRAFT_OWNERS={DRAFT_OWNERS}
        getTeamId={getTeamId}
        renderAssetBadge={renderAssetBadge}
        editOfferedPickRound={editOfferedPickRound}
        setEditOfferedPickRound={setEditOfferedPickRound}
        editSenderAvailablePicks={editSenderAvailablePicks}
        isPickInAssets={isPickInAssets}
        editOfferedPlayerId={editOfferedPlayerId}
        setEditOfferedPlayerId={setEditOfferedPlayerId}
        teamRosters={teamRosters}
        isPlayerInAssets={isPlayerInAssets}
        editOfferedBudget={editOfferedBudget}
        setEditOfferedBudget={setEditOfferedBudget}
        editRequestedPickRound={editRequestedPickRound}
        setEditRequestedPickRound={setEditRequestedPickRound}
        editTargetAvailablePicks={editTargetAvailablePicks}
        editRequestedPlayerId={editRequestedPlayerId}
        setEditRequestedPlayerId={setEditRequestedPlayerId}
        editRequestedBudget={editRequestedBudget}
        setEditRequestedBudget={setEditRequestedBudget}
        handleSaveProposalEdit={handleSaveProposalEdit}
        savingEdit={savingEdit}
      />
    </div>
  );
}
