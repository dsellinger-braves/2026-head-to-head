// src/views/DraftCapitalView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import defaultDraftAssetTrades from '../data/draftAssetTrades2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import { useAuth } from '../context/useAuth';
import { LEAGUE_OWNERS } from '../utils/mlbTeams';

const DRAFT_OWNERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"];

const LEAGUE_OWNERS_NORMALIZED = {
  adrian: { id: 2, name: 'Adrian' },
  adriaxx: { id: 2, name: 'Adrian' },
  alex: { id: 8, name: 'Alex' },
  ay0h: { id: 8, name: 'Alex' },
  anil: { id: 6, name: 'Anil' },
  anilbhairo: { id: 6, name: 'Anil' },
  dan: { id: 5, name: 'Daniel' },
  daniel: { id: 5, name: 'Daniel' },
  dsellinger: { id: 5, name: 'Daniel' },
  garrett: { id: 3, name: 'Garrett' },
  ghutch: { id: 3, name: 'Garrett' },
  mark: { id: 13, name: 'Mark' },
  mrussell38: { id: 13, name: 'Mark' },
  preston: { id: 14, name: 'Preston' },
  pston3: { id: 14, name: 'Preston' },
  tim: { id: 1, name: 'Tim' },
  aznchuy: { id: 1, name: 'Tim' },
  will: { id: 12, name: 'Will' },
  senorspice: { id: 12, name: 'Will' },
};

const getTeamId = (ownerName) => {
  if (!ownerName) return 0;
  const key = String(ownerName).toLowerCase().trim();
  const match = LEAGUE_OWNERS_NORMALIZED[key];
  if (match) return match.id;
  const found = LEAGUE_OWNERS.find(lo => lo.name.toLowerCase() === key);
  return found ? found.id : 0;
};

const canonicalOwnerName = (ownerName) => {
  if (!ownerName) return '';
  const key = String(ownerName).toLowerCase().trim();
  const match = LEAGUE_OWNERS_NORMALIZED[key];
  if (match) return match.name;
  return ownerName;
};

// Enqueue Discord DM notification for the 24/7 Railway bot worker
async function enqueueTradeNotification({
  proposalId = null,
  eventType,
  senderTeamId,
  senderOwner,
  recipientTeamId,
  recipientOwner,
  offeredAssets = [],
  requestedAssets = [],
  notes = ''
}) {
  try {
    const { error } = await supabase
      .from('trade_notifications')
      .insert({
        trade_proposal_id: proposalId,
        event_type: eventType,
        sender_team_id: senderTeamId,
        sender_owner: senderOwner,
        recipient_team_id: recipientTeamId,
        recipient_owner: recipientOwner,
        details: {
          offered_assets: offeredAssets,
          requested_assets: requestedAssets,
          notes: notes || '',
        },
        status: 'pending'
      });
    if (error) {
      console.warn('Could not enqueue trade notification:', error);
    }
  } catch (err) {
    console.warn('enqueueTradeNotification error:', err);
  }
}

function isPickInAssets(pick, assets = []) {
  if (!pick || !assets || assets.length === 0) return false;
  const pRound = Number(pick.round);
  const pOrig = canonicalOwnerName(pick.originalOwner);
  const pId = pick.id || `pick-${pRound}-${pOrig}`;

  return assets.some(a => {
    if (a.type !== 'pick') return false;
    if (a.pick_id && a.pick_id === pId) return true;
    const aRound = Number(a.round);
    if (aRound !== pRound) return false;
    const aOrig = canonicalOwnerName(a.original_owner || a.originalOwner);
    if (aOrig && pOrig) return aOrig === pOrig;
    return true;
  });
}

function isPlayerInAssets(player, assets = []) {
  if (!player || !assets || assets.length === 0) return false;
  const pId = Number(player.player_id);
  return assets.some(a => a.type === 'player' && Number(a.player_id) === pId);
}

function compute2027DraftPicks(draftTrades = []) {
  const owners = [...DRAFT_OWNERS].sort();
  const picks = [];

  // Base draft order: 27 rounds (Rounds 6 through 32), 1 pick per owner per round (243 total picks)
  for (let round = 6; round <= 32; round++) {
    owners.forEach(owner => {
      picks.push({
        id: `pick-${round}-${owner}`,
        round: Number(round),
        originalOwner: owner,
        currentOwner: owner,
        isTraded: false,
        tradeDetails: null,
      });
    });
  }

  // Filter for draft asset trades
  const assetTrades = (draftTrades || []).filter(t =>
    t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget'
  );

  assetTrades.forEach(trade => {
    const round = parseInt(trade.round_num, 10);
    if (!round) return;
    const sending = trade.sending_owner === 'Dan' ? 'Daniel' : trade.sending_owner;
    const receiving = trade.receiving_owner === 'Dan' ? 'Daniel' : trade.receiving_owner;

    // Find the pick in this round currently owned by 'sending'
    const pick = picks.find(p => p.round === round && p.currentOwner === sending);
    if (pick) {
      pick.currentOwner = receiving;
      pick.isTraded = true;
      pick.tradeDetails = trade;
    }
  });

  return picks;
}

export default function DraftCapitalView({
  currentUser = 'Daniel',
  isCommissioner: propIsCommissioner = false,
  draftYear = 2027,
  onDraftYearChange,
  subTab
}) {
  const { user, profile, isCommissioner: authIsCommissioner, effectiveOwner, logCommissionerAction } = useAuth();
  const isCommissioner = propIsCommissioner || authIsCommissioner;

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
  }); // 'board' | 'ledgers' | 'history' | '2026board' | 'proposals'
  const [selectedOwner, setSelectedOwner] = useState(canonicalCurrentUser);
  const [roundFilter, setRoundFilter] = useState('ALL');

  // Trade Proposals Sub-Navigation & Perspectives
  const [inboxTab, setInboxTab] = useState('inbox'); // 'inbox' | 'outbox' | 'commish' | 'all' | 'archive'
  const [viewPerspectiveOwner, setViewPerspectiveOwner] = useState(canonicalCurrentUser);

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
  const [offeredAssets, setOfferedAssets] = useState([]); // [{ type: 'pick'|'player'|'budget', ... }]
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
  const loadData = React.useCallback(async (silent = false) => {
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

      // Build active roster lookup
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

  // Available picks for commissioner edit modal
  const editSenderAvailablePicks = useMemo(() => {
    if (!editingProposal) return [];
    return computedPicks.filter(p => p.currentOwner === editingProposal.proposing_owner).sort((a, b) => a.round - b.round);
  }, [computedPicks, editingProposal]);

  const editTargetAvailablePicks = useMemo(() => {
    if (!editingProposal) return [];
    return computedPicks.filter(p => p.currentOwner === editingProposal.target_owner).sort((a, b) => a.round - b.round);
  }, [computedPicks, editingProposal]);

  // Asset helpers for proposal creation form
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

  // Switch Proposing or Target Owner in Form
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

  // Compute Pick Counts & Imbalance in Form
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

    // Flush any pending selections into asset arrays
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

      // Enqueue Discord DM notification
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

  // Counter Offer Handler: Flips sender/target, inverts assets, and opens the proposal form
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

  // Partner Accept: Moves trade from 'pending' to 'accepted_by_partner' (ready for commish)
  const handlePartnerAccept = async (proposal) => {
    if (!user) {
      alert('Please log in with Discord to accept trades.');
      return;
    }

    const isMe = profile?.owner_name?.toLowerCase() === proposal.target_owner?.toLowerCase() ||
      (profile?.owner_name === 'Dan' && proposal.target_owner === 'Daniel') ||
      (profile?.owner_name === 'Daniel' && proposal.target_owner === 'Dan');

    if (!isCommissioner && !isMe) {
      alert(`Only ${proposal.target_owner} or league commissioners can accept this proposal.`);
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

      // 1. Notify the original proposing owner that their trade was accepted
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

      // 2. Notify commissioners (Dan Team 5 & Adrian Team 2) if they aren't directly party to the trade
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
            notes: `[Commish Notice] Trade agreed between ${proposal.proposing_owner} and ${proposal.target_owner}. Ready for your review & execution!`,
          });
        }
      }

      alert(`Trade accepted! 🎉 Discord notifications queued, and sent to Commissioners for final league approval.`);
      await loadData(true);
    } catch (err) {
      console.error('Accept failed:', err);
      alert('Failed to accept proposal: ' + err.message);
    } finally {
      setActionLoadingId(null);
    }
  };

  // Commissioner Approval: Officially executes the trade into draft_asset_trades!
  const handleCommissionerApprove = async (proposal) => {
    if (!isCommissioner) {
      alert('Only league commissioners (Dan & Adrian) can execute official trade approval.');
      return;
    }

    setActionLoadingId(proposal.id);
    try {
      const tradeId = 'TR-2027-' + Date.now().toString().slice(-6);
      const tradeDate = new Date().toISOString().split('T')[0];
      const newAssetTrades = [];

      // 1. Offered assets move from proposing_owner to target_owner
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

      // 2. Requested assets move from target_owner to proposing_owner
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

      // Insert assets into draft_asset_trades
      if (newAssetTrades.length > 0) {
        const { error: insErr } = await supabase
          .from('draft_asset_trades')
          .insert(newAssetTrades);
        if (insErr) throw insErr;
      }

      // Update proposal status to 'approved'
      const { error: propErr } = await supabase
        .from('league_trade_proposals')
        .update({
          status: 'approved',
          responded_at: new Date().toISOString(),
          responded_by: `${profile?.owner_name || 'Commissioner'} (Approved)`
        })
        .eq('id', proposal.id);

      if (propErr) throw propErr;

      // Notify both parties that the commissioner approved and executed the trade
      await enqueueTradeNotification({
        proposalId: proposal.id,
        eventType: 'approved',
        senderTeamId: profile?.team_id || 5,
        senderOwner: profile?.owner_name || 'Commissioner',
        recipientTeamId: proposal.proposing_team_id || getTeamId(proposal.proposing_owner),
        recipientOwner: proposal.proposing_owner,
        offeredAssets: proposal.offered_assets,
        requestedAssets: proposal.requested_assets,
        notes: `Trade approved & executed by Commissioner ${profile?.owner_name || 'Commish'}`,
      });
      await enqueueTradeNotification({
        proposalId: proposal.id,
        eventType: 'approved',
        senderTeamId: profile?.team_id || 5,
        senderOwner: profile?.owner_name || 'Commissioner',
        recipientTeamId: proposal.target_team_id || getTeamId(proposal.target_owner),
        recipientOwner: proposal.target_owner,
        offeredAssets: proposal.offered_assets,
        requestedAssets: proposal.requested_assets,
        notes: `Trade approved & executed by Commissioner ${profile?.owner_name || 'Commish'}`,
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

  // Decline or Cancel Proposal
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
        // Notify original proposer that trade was declined
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

  // Commissioner Edit Modal Handlers
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
          actionDescription: `Commissioner edited trade proposal ${editingProposal.id} (${editingProposal.proposing_owner} <-> ${editingProposal.target_owner})`,
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

      alert('Proposal updated successfully by Commissioner. 👑');
      setEditingProposal(null);
      await loadData(true);
    } catch (err) {
      console.error('Failed to update proposal:', err);
      alert('Error updating proposal: ' + err.message);
    } finally {
      setSavingEdit(false);
    }
  };

  // Commissioner Delete Executed Trade from draft_asset_trades
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

  // Compute owner statistics
  const ownerStats = useMemo(() => {
    const stats = {};
    DRAFT_OWNERS.forEach(owner => {
      const ownedPicks = computedPicks.filter(p => p.currentOwner === owner);
      const acquired = computedPicks.filter(p => p.currentOwner === owner && p.originalOwner !== owner);
      const tradedAway = computedPicks.filter(p => p.originalOwner === owner && p.currentOwner !== owner);
      stats[owner] = {
        owner,
        total: ownedPicks.length,
        diff: ownedPicks.length - 27,
        acquired,
        tradedAway,
        ownedPicks,
      };
    });
    return stats;
  }, [computedPicks]);

  // Distinct trades involving draft assets (Picks, Budgets, and Players)
  const assetTradesList = useMemo(() => {
    return (draftTrades || []).filter(t =>
      t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget' || t.asset_type === 'Player'
    );
  }, [draftTrades]);

  // Filtered rounds for round-by-round board
  const rounds = useMemo(() => {
    const list = [];
    for (let r = 6; r <= 32; r++) list.push(r);
    if (roundFilter === 'ALL') return list;
    if (roundFilter === 'EARLY') return list.filter(r => r <= 14);
    if (roundFilter === 'MID') return list.filter(r => r >= 15 && r <= 23);
    if (roundFilter === 'LATE') return list.filter(r => r >= 24);
    return list;
  }, [roundFilter]);

  // Proposal Lists Filtered by Inbox Tabs
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

  // Public league view: ONLY show accepted trades (accepted by partner or approved by commissioner)
  // Private pending proposals & active negotiations remain confidential to the involved owners!
  const leagueAcceptedTrades = useMemo(() => {
    return proposals.filter(p => p.status === 'accepted_by_partner' || p.status === 'approved');
  }, [proposals]);

  const archivedProposals = useMemo(() => {
    if (isCommissioner) {
      return proposals.filter(p => p.status === 'approved' || p.status === 'declined' || p.status === 'cancelled');
    }
    // For non-commissioners, only display approved trades or their own historical declined/cancelled negotiations
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

  // Helper to render asset pills
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
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div className="absolute -top-12 -right-12 w-64 h-64 bg-amber-500/10 rounded-full blur-3xl pointer-events-none" />
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 relative z-10">
          <div>
            <div className="flex items-center gap-3">
              <span className="text-3xl">🎟️</span>
              <div>
                <h1 className="text-2xl font-black text-white tracking-tight">Draft Capital & Pick Board</h1>
                <p className="text-slate-400 text-xs mt-0.5">
                  Future 2027 draft assets, trade ledgers, compensation offsets, and historical 2026 draft structure
                </p>
              </div>
            </div>
          </div>

          {/* Subtabs */}
          <div className="flex flex-wrap items-center bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs font-bold">
            <button
              onClick={() => {
                setActiveSubTab('board');
                if (onDraftYearChange) onDraftYearChange(2027);
              }}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'board'
                  ? 'bg-teal-500 text-slate-950 shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              🚀 2027 Traded Board
            </button>
            <button
              onClick={() => setActiveSubTab('ledgers')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'ledgers'
                  ? 'bg-amber-500 text-slate-950 shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              📑 Owner Ledgers
            </button>
            <button
              onClick={() => setActiveSubTab('history')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'history'
                  ? 'bg-purple-500 text-white shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              📜 Trade History ({assetTradesList.length})
            </button>
            <button
              onClick={() => {
                setActiveSubTab('2026board');
                if (onDraftYearChange) onDraftYearChange(2026);
              }}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === '2026board'
                  ? 'bg-blue-600 text-white shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              🏛️ 2026 Ground Truth
            </button>
            <button
              onClick={() => setActiveSubTab('proposals')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer flex items-center gap-1.5 ${
                activeSubTab === 'proposals'
                  ? 'bg-rose-600 text-white shadow-md font-black ring-1 ring-rose-400'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <span>🤝 Trade Proposals</span>
              {incomingProposals.length > 0 ? (
                <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-emerald-400 text-slate-950 animate-pulse">
                  📥 {incomingProposals.length}
                </span>
              ) : pendingCount > 0 ? (
                <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-amber-400 text-slate-950">
                  {pendingCount}
                </span>
              ) : null}
            </button>
          </div>
        </div>

        {/* Quick Manager Capital Summary Cards */}
        <div className="grid grid-cols-3 sm:grid-cols-5 md:grid-cols-9 gap-2 mt-6 pt-4 border-t border-slate-800">
          {DRAFT_OWNERS.map(owner => {
            const stat = ownerStats[owner];
            const diff = stat?.diff || 0;
            const isMe = owner === currentUser;
            return (
              <button
                key={owner}
                onClick={() => {
                  setSelectedOwner(owner);
                  setActiveSubTab('ledgers');
                }}
                className={`p-2 rounded-xl text-center transition-all cursor-pointer border ${
                  selectedOwner === owner && activeSubTab === 'ledgers'
                    ? 'bg-amber-500/20 border-amber-500/60 shadow-lg'
                    : isMe
                    ? 'bg-slate-800/80 border-slate-700 hover:border-slate-600'
                    : 'bg-slate-950/60 border-slate-800/60 hover:bg-slate-800/40'
                }`}
              >
                <div className="text-[11px] font-bold text-slate-300 truncate">{owner}</div>
                <div className="text-sm font-black text-white mt-0.5">{stat?.total ?? 27}</div>
                <div className={`text-[10px] font-extrabold ${diff > 0 ? 'text-emerald-400' : diff < 0 ? 'text-rose-400' : 'text-slate-500'}`}>
                  {diff > 0 ? `+${diff}` : diff === 0 ? '±0' : `${diff}`}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* SUBTAB 1: 2027 TRADED PICK BOARD */}
      {activeSubTab === 'board' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <span className="text-xs font-black uppercase tracking-wider text-slate-400">Rounds Filter:</span>
              <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold">
                {[
                  { id: 'ALL', label: 'All (R6–32)' },
                  { id: 'EARLY', label: 'Early (R6–14)' },
                  { id: 'MID', label: 'Mid (R15–23)' },
                  { id: 'LATE', label: 'Late (R24–32)' },
                ].map(f => (
                  <button
                    key={f.id}
                    onClick={() => setRoundFilter(f.id)}
                    className={`px-2.5 py-1 rounded-md transition-colors cursor-pointer ${
                      roundFilter === f.id
                        ? 'bg-slate-800 text-teal-400 shadow-xs'
                        : 'text-slate-400 hover:text-white'
                    }`}
                  >
                    {f.label}
                  </button>
                ))}
              </div>
            </div>

            <div className="flex items-center gap-4 text-xs">
              <span className="inline-flex items-center gap-1.5 text-slate-400">
                <span className="w-2.5 h-2.5 rounded-full bg-slate-700 border border-slate-600"></span> Original Pick
              </span>
              <span className="inline-flex items-center gap-1.5 text-amber-300 font-bold">
                <span className="w-2.5 h-2.5 rounded-full bg-amber-500 border border-amber-400"></span> Traded / Acquired Pick
              </span>
            </div>
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-3 text-center w-20">Round</th>
                  {DRAFT_OWNERS.map(owner => (
                    <th key={owner} className="py-3 px-3 text-center min-w-[110px]">
                      <div className="font-extrabold text-slate-200">{owner}</div>
                      <div className="text-[10px] text-slate-400 font-semibold lowercase">slot</div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {rounds.map(roundNum => (
                  <tr key={roundNum} className="hover:bg-slate-800/40 transition-colors">
                    <td className="py-2.5 px-3 text-center font-black text-slate-400 bg-slate-950/40">
                      R{roundNum}
                    </td>
                    {DRAFT_OWNERS.map(slotOwner => {
                      const pick = computedPicks.find(p => p.round === roundNum && p.originalOwner === slotOwner);
                      if (!pick) return <td key={slotOwner} className="py-2.5 px-2 text-center text-slate-600">-</td>;

                      const isTraded = pick.isTraded;
                      return (
                        <td
                          key={slotOwner}
                          className={`py-2 px-2 text-center transition-colors ${
                            isTraded
                              ? 'bg-amber-500/10 hover:bg-amber-500/20'
                              : 'hover:bg-slate-800/30'
                          }`}
                        >
                          <div className="inline-flex flex-col items-center">
                            <span className={`px-2 py-0.5 rounded-md text-xs font-bold ${
                              isTraded
                                ? 'bg-amber-500/30 text-amber-300 border border-amber-500/40 shadow-xs'
                                : 'text-slate-300'
                            }`}>
                              {pick.currentOwner}
                            </span>
                            {isTraded && (
                              <span className="text-[9px] text-amber-400/80 font-medium mt-0.5">
                                via {(() => {
                                  const s = pick.tradeDetails?.sending_owner || pick.originalOwner;
                                  return s === 'Dan' ? 'Daniel' : s || 'Trade';
                                })()}
                              </span>
                            )}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* SUBTAB 2: OWNER LEDGERS */}
      {activeSubTab === 'ledgers' && (
        <div className="space-y-6">
          {/* Owner Selector Pills */}
          <div className="flex flex-wrap items-center gap-2">
            {DRAFT_OWNERS.map(owner => (
              <button
                key={owner}
                onClick={() => setSelectedOwner(owner)}
                className={`px-3 py-1.5 rounded-xl text-xs font-bold transition-all cursor-pointer border ${
                  selectedOwner === owner
                    ? 'bg-amber-500 text-slate-950 border-amber-400 shadow-md font-black'
                    : 'bg-slate-900 text-slate-400 border-slate-800 hover:text-white hover:border-slate-700'
                }`}
              >
                {owner} ({ownerStats[owner]?.total ?? 27})
              </button>
            ))}
          </div>

          {/* Ledger Breakdown Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 uppercase tracking-wider">Total Owned 2027 Picks</div>
              <div className="text-3xl font-black text-white mt-1">{ownerStats[selectedOwner]?.total ?? 27}</div>
              <div className="text-xs text-slate-500 mt-1">27 base rounds in draft</div>
            </div>
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 uppercase tracking-wider">Acquired from Others</div>
              <div className="text-3xl font-black text-emerald-400 mt-1">{ownerStats[selectedOwner]?.acquired.length ?? 0}</div>
              <div className="text-xs text-slate-500 mt-1">Incoming pick assets</div>
            </div>
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 uppercase tracking-wider">Traded Away to Others</div>
              <div className="text-3xl font-black text-rose-400 mt-1">{ownerStats[selectedOwner]?.tradedAway.length ?? 0}</div>
              <div className="text-xs text-slate-500 mt-1">Outgoing pick assets</div>
            </div>
          </div>

          {/* Complete Pick Inventory Table */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
            <h3 className="text-sm font-black uppercase tracking-wider text-slate-300">
              {selectedOwner}'s 2027 Draft Pick Inventory
            </h3>
            <div className="overflow-x-auto rounded-xl border border-slate-800">
              <table className="w-full text-left text-xs border-collapse">
                <thead>
                  <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                    <th className="py-2.5 px-4 w-24">Round</th>
                    <th className="py-2.5 px-4 w-32">Status</th>
                    <th className="py-2.5 px-4 w-40">Original Owner</th>
                    <th className="py-2.5 px-4">Trade Context</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                  {ownerStats[selectedOwner]?.ownedPicks.map((pick, idx) => (
                    <tr key={idx} className="hover:bg-slate-800/40 transition-colors">
                      <td className="py-2.5 px-4 font-black text-slate-300">Round {pick.round}</td>
                      <td className="py-2.5 px-4">
                        {pick.originalOwner === selectedOwner ? (
                          <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700">
                            ORIGINAL
                          </span>
                        ) : (
                          <span className="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-amber-500/20 text-amber-300 border border-amber-500/40">
                            ACQUIRED
                          </span>
                        )}
                      </td>
                      <td className="py-2.5 px-4 font-bold text-white">{pick.originalOwner}</td>
                      <td className="py-2.5 px-4 text-slate-400">
                        {pick.isTraded
                          ? `Acquired via Trade #${pick.tradeDetails?.trade_id} (${pick.tradeDetails?.trade_date})${pick.tradeDetails?.notes ? ' • ' + pick.tradeDetails.notes : ''}`
                          : 'Own natural slot'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Traded Away Table if any */}
          {(ownerStats[selectedOwner]?.tradedAway.length ?? 0) > 0 && (
            <div className="bg-slate-900 border border-rose-900/40 rounded-2xl p-5 shadow-xl space-y-4">
              <h3 className="text-sm font-black uppercase tracking-wider text-rose-400">
                Picks {selectedOwner} Has Traded Away
              </h3>
              <div className="overflow-x-auto rounded-xl border border-slate-800">
                <table className="w-full text-left text-xs border-collapse">
                  <thead>
                    <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                      <th className="py-2.5 px-4 w-24">Round</th>
                      <th className="py-2.5 px-4 w-32">Status</th>
                      <th className="py-2.5 px-4 w-40">Now Owned By</th>
                      <th className="py-2.5 px-4">Trade Context</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-rose-900/30 font-medium text-slate-200">
                    {ownerStats[selectedOwner]?.tradedAway.map((pick, idx) => (
                      <tr key={`away-${idx}`} className="hover:bg-rose-950/20 transition-colors">
                        <td className="py-2.5 px-4 font-black text-rose-400 line-through">
                          Round {pick.round}
                        </td>
                        <td className="py-2.5 px-4">
                          <span className="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-rose-500/20 text-rose-300 border border-rose-500/40">
                            TRADED AWAY
                          </span>
                        </td>
                        <td className="py-2.5 px-4 font-bold text-white">
                          {pick.currentOwner}
                        </td>
                        <td className="py-2.5 px-4 text-rose-300/80">
                          Traded to <strong>{pick.currentOwner}</strong> via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                          {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* SUBTAB 3: TRADE HISTORY LOG */}
      {activeSubTab === 'history' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
                Draft Asset & Player Trades ({assetTradesList.length} Transactions)
              </h2>
              <span className="text-xs text-slate-500">
                Synchronized historical ledger from Supabase (draft_asset_trades)
              </span>
            </div>
            {isCommissioner && (
              <span className="text-[10px] font-bold px-2.5 py-1 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40">
                👑 Commissioner Mode: Delete & Modify Actions Enabled
              </span>
            )}
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-4 w-28">Date</th>
                  <th className="py-3 px-3 text-center w-20">Trade #</th>
                  <th className="py-3 px-4 w-32">Sending Owner</th>
                  <th className="py-3 px-4 w-32">Receiving Owner</th>
                  <th className="py-3 px-4 w-52">Asset Traded</th>
                  <th className="py-3 px-4">Notes & Package Details</th>
                  {isCommissioner && <th className="py-3 px-3 text-right w-24">Commish</th>}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {assetTradesList.map((t, idx) => (
                  <tr key={`history-${idx}`} className="hover:bg-slate-800/40 transition-colors">
                    <td className="py-3 px-4 font-mono text-slate-400 text-[11px]">
                      {t.trade_date}
                    </td>
                    <td className="py-3 px-3 text-center">
                      <span className="px-2 py-0.5 bg-slate-800 text-teal-300 rounded font-black text-[11px]">
                        #{t.trade_id}
                      </span>
                    </td>
                    <td className="py-3 px-4 font-bold text-rose-400">
                      {t.sending_owner}
                    </td>
                    <td className="py-3 px-4 font-bold text-emerald-400">
                      {t.receiving_owner}
                    </td>
                    <td className="py-3 px-4">
                      {t.asset_type === 'Player' ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-sky-500/15 border border-sky-500/40 text-sky-300 font-bold text-xs">
                          <span>👤</span>
                          <span>{t.asset_name}</span>
                          <span className="text-[10px] text-sky-400/80">(Player)</span>
                        </span>
                      ) : t.asset_type === 'Budget' ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-emerald-500/15 border border-emerald-500/40 text-emerald-300 font-bold text-xs">
                          <span>💵</span>
                          <span>{t.asset_name}</span>
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-500/15 border border-amber-500/40 text-amber-300 font-bold text-xs">
                          <span>🎟️</span>
                          <span>{t.asset_name}</span>
                          {t.round_num && <span className="text-[10px] text-amber-400/80">(R{t.round_num})</span>}
                        </span>
                      )}
                    </td>
                    <td className="py-3 px-4 text-slate-300">
                      {t.notes || `Trade #${t.trade_id}`}
                    </td>
                    {isCommissioner && (
                      <td className="py-3 px-3 text-right">
                        {t.id && (
                          <button
                            onClick={() => handleDeleteExecutedTrade(t)}
                            className="px-2 py-1 rounded bg-rose-950/60 border border-rose-500/40 text-rose-300 hover:bg-rose-900 text-[10px] font-bold cursor-pointer transition-all"
                            title="Delete this record from draft_asset_trades"
                          >
                            🗑️ Delete
                          </button>
                        )}
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* SUBTAB 4: 2026 GROUND TRUTH BOARD */}
      {activeSubTab === '2026board' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
                2026 Ground Truth Draft Board Architecture
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">
                Rounds 1–5 are Keepers; Rounds 6–20 include purchased Comp Picks; Rounds 27–32 show Offset picks.
              </p>
            </div>
            <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800 text-xs font-bold">
              {['ALL', 'KEEPERS', 'EARLY', 'MID', 'LATE'].map(rf => (
                <button
                  key={rf}
                  onClick={() => setRoundFilter(rf)}
                  className={`px-2.5 py-1 rounded-md transition-colors cursor-pointer ${
                    roundFilter === rf
                      ? 'bg-slate-800 text-emerald-400 shadow-xs'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  {rf === 'ALL' ? 'All (R1–32)' : rf === 'KEEPERS' ? 'Keepers (R1–5)' : rf === 'EARLY' ? 'Early (R6–14)' : rf === 'MID' ? 'Mid (R15–23)' : 'Late (R24–32)'}
                </button>
              ))}
            </div>
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse min-w-[950px]">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-3 text-center w-20">Round</th>
                  {DRAFT_OWNERS.map(owner => (
                    <th key={owner} className="py-3 px-3 text-center">
                      <div className="text-slate-200 font-extrabold">{owner}</div>
                    </th>
                  ))}
                  <th className="py-3 px-3 text-center w-36">Comp Pick (End)</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                {Array.from({ length: 32 }, (_, i) => i + 1)
                  .filter(rNum => {
                    if (roundFilter === 'KEEPERS') return rNum <= 5;
                    if (roundFilter === 'EARLY') return rNum >= 6 && rNum <= 14;
                    if (roundFilter === 'MID') return rNum >= 15 && rNum <= 23;
                    if (roundFilter === 'LATE') return rNum >= 24 && rNum <= 32;
                    return true;
                  })
                  .map(rNum => {
                    const isKeeperRound = rNum <= 5;
                    const compPicksThisRound = (compPicks || []).filter(
                      cp => cp.round_num === rNum &&
                        (cp.action_type === 'COMP_BOUGHT' || cp.action_type === 'BOUGHT') &&
                        (!cp.season_year || cp.season_year === 2026 || cp.season_year === '2026')
                    );

                    return (
                      <tr key={rNum} className="hover:bg-slate-800/40 transition-colors">
                        <td className="py-2 px-3 text-center font-black text-slate-400 bg-slate-950/40">
                          R{rNum}
                          {isKeeperRound && <span className="block text-[9px] text-amber-400">Keeper</span>}
                        </td>
                        {DRAFT_OWNERS.map(owner => {
                          if (isKeeperRound) {
                            const keeperObj = (keepers || []).find(
                              k => k.round_num === rNum &&
                                (k.manager === owner || (owner === 'Daniel' && k.manager === 'Dan'))
                            );
                            return (
                              <td key={owner} className="py-2 px-2 text-center bg-amber-950/10">
                                {keeperObj ? (
                                  <div className="text-[11px] font-bold text-amber-300">
                                    {keeperObj.player_name || keeperObj.player}
                                  </div>
                                ) : (
                                  <span className="text-slate-600 text-[10px]">Keeper Slot</span>
                                )}
                              </td>
                            );
                          }
                          return (
                            <td key={owner} className="py-2 px-2 text-center text-slate-300 text-xs">
                              {owner}
                            </td>
                          );
                        })}
                        <td className="py-2 px-3 text-center bg-emerald-950/10">
                          {compPicksThisRound.length > 0 ? (
                            <div className="flex flex-col gap-1 items-center">
                              {compPicksThisRound.map((cp, idx) => (
                                <span
                                  key={idx}
                                  className="px-2 py-0.5 rounded text-[10px] font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/40"
                                >
                                  {cp.owner_name || cp.manager} (+Comp)
                                </span>
                              ))}
                            </div>
                          ) : (
                            <span className="text-slate-700 text-[11px]">-</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* SUBTAB 5: TRADE PROPOSALS & COMMISSIONER APPROVALS */}
      {activeSubTab === 'proposals' && (
        <div className="space-y-6">
          {/* Hub Header & New Proposal Toggle */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-2">
                <span className="text-2xl">🤝</span>
                <h2 className="text-lg font-black text-white">
                  2027 Offseason Draft Asset, Budget & Player Trading Hub
                </h2>
              </div>
              <p className="text-xs text-slate-400 mt-1 max-w-3xl leading-relaxed">
                Propose trades consisting of <strong>2027 draft picks</strong>, <strong>active rostered players</strong>, and <strong>draft budget cash</strong>. Offers wait in the partner's inbox for agreement before routing to <strong>Commissioners (Dan & Adrian)</strong> for final league execution.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {/* Perspective Owner Selector */}
              <div className="flex items-center gap-2 bg-slate-950 px-3 py-1.5 rounded-xl border border-slate-800 text-xs">
                <span className="text-slate-400 font-semibold">Inbox Perspective:</span>
                <select
                  value={viewPerspectiveOwner}
                  onChange={e => {
                    setViewPerspectiveOwner(e.target.value);
                    setPropSender(e.target.value);
                  }}
                  className="bg-transparent text-amber-300 font-bold focus:outline-none cursor-pointer"
                >
                  {DRAFT_OWNERS.map(o => (
                    <option key={o} value={o} className="bg-slate-900 text-white">{o}</option>
                  ))}
                </select>
              </div>

              <button
                onClick={() => setProposeModalOpen(!proposeModalOpen)}
                className="px-4 py-2.5 rounded-xl bg-gradient-to-r from-rose-600 to-pink-600 hover:from-rose-500 hover:to-pink-500 text-white font-bold text-xs shadow-lg transition-all flex items-center gap-2 cursor-pointer w-fit"
              >
                <span>{proposeModalOpen ? '✕ Close Proposal Form' : '➕ Propose New Trade'}</span>
              </button>
            </div>
          </div>

          {/* Collapsible Proposal Form */}
          {proposeModalOpen && (
            <div className="bg-slate-900/95 border border-rose-500/40 rounded-2xl p-6 shadow-2xl space-y-6 animate-fade-in-up">
              <div className="border-b border-slate-800 pb-3 flex items-center justify-between">
                <h3 className="text-base font-bold text-white flex items-center gap-2">
                  <span>📝</span> Submit New Trade Proposal
                </h3>
                {isCommissioner && (
                  <span className="text-[10px] font-bold px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40">
                    👑 Commissioner Override: Send on behalf of any team
                  </span>
                )}
              </div>

              {!user && (
                <div className="p-3 bg-indigo-950/40 border border-indigo-500/40 rounded-xl text-xs text-indigo-200 flex items-center gap-2">
                  <span>🔒</span>
                  <span>Please log in with Discord via the top menu to propose official trades.</span>
                </div>
              )}

              <form onSubmit={handleSendProposal} className="space-y-6">
                {/* Manager Selection */}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs font-bold text-slate-300 mb-1">
                      Proposing Team (Sending Assets):
                    </label>
                    {isCommissioner ? (
                      <select
                        value={propSender}
                        onChange={e => handlePropSenderChange(e.target.value)}
                        className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-white focus:ring-2 focus:ring-rose-500 cursor-pointer"
                      >
                        {DRAFT_OWNERS.map(o => (
                          <option key={o} value={o}>{o} (Team {getTeamId(o)})</option>
                        ))}
                      </select>
                    ) : (
                      <div className="bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-xs font-bold text-indigo-300">
                        {propSender} (My Team)
                      </div>
                    )}
                  </div>

                  <div>
                    <label className="block text-xs font-bold text-slate-300 mb-1">
                      Target Trading Partner:
                    </label>
                    <select
                      value={propTarget}
                      onChange={e => handlePropTargetChange(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-white focus:ring-2 focus:ring-rose-500 cursor-pointer"
                    >
                      {DRAFT_OWNERS.filter(o => o !== propSender).map(o => (
                        <option key={o} value={o}>{o} (Team {getTeamId(o)})</option>
                      ))}
                    </select>
                  </div>
                </div>

                {/* 2-Column Asset Exchange */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 p-4 rounded-xl bg-slate-950/60 border border-slate-800">
                  {/* Left: What Proposer Sends */}
                  <div className="space-y-4">
                    <div className="text-xs font-black uppercase tracking-wider text-rose-400 border-b border-slate-800 pb-2 flex items-center justify-between">
                      <span>📤 {propSender} Offers:</span>
                      <span className="text-[10px] text-slate-400 lowercase">{senderAvailablePicks.length} picks • {(teamRosters[propSender] || []).length} players</span>
                    </div>

                    {/* Staged Offered Assets Tags */}
                    {offeredAssets.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 p-2 bg-slate-900/80 rounded-lg border border-slate-800 min-h-[38px] items-center">
                        {offeredAssets.map((asset, idx) => (
                          <span key={idx}>
                            {renderAssetBadge(asset, () => handleRemoveOfferedAsset(idx))}
                          </span>
                        ))}
                      </div>
                    )}

                    {/* 1. Pick Dropdown */}
                    <div className="space-y-1">
                      <label className="block text-[11px] font-bold text-slate-400">
                        Add 2027 Draft Pick to Offer:
                      </label>
                      <div className="flex gap-2">
                        <select
                          value={selectedOfferedPickRound}
                          onChange={e => {
                            const val = e.target.value;
                            setSelectedOfferedPickRound(val);
                            if (val) handleAddOfferedPick(val);
                          }}
                          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                        >
                          <option value="">-- Select draft pick --</option>
                          {senderAvailablePicks
                            .filter(p => !isPickInAssets(p, offeredAssets))
                            .map(p => {
                              const val = `${p.round}:${p.originalOwner}`;
                              return (
                                <option key={p.id || val} value={val}>
                                  Round {p.round} Pick {p.originalOwner !== propSender ? `(Orig: ${p.originalOwner})` : ''}
                                </option>
                              );
                            })}
                        </select>
                        <button
                          type="button"
                          onClick={() => handleAddOfferedPick(selectedOfferedPickRound)}
                          disabled={!selectedOfferedPickRound}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold disabled:opacity-40 cursor-pointer whitespace-nowrap"
                        >
                          + Add Pick
                        </button>
                      </div>
                    </div>

                    {/* 2. Player Dropdown */}
                    <div className="space-y-1">
                      <label className="block text-[11px] font-bold text-slate-400">
                        Add Rostered Player to Offer:
                      </label>
                      <div className="flex gap-2">
                        <select
                          value={selectedOfferedPlayerId}
                          onChange={e => {
                            const val = e.target.value;
                            setSelectedOfferedPlayerId(val);
                            if (val) handleAddOfferedPlayer(val);
                          }}
                          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                        >
                          <option value="">-- Select rostered player --</option>
                          {(teamRosters[propSender] || [])
                            .filter(p => !isPlayerInAssets(p, offeredAssets))
                            .map(p => (
                              <option key={p.player_id} value={p.player_id}>
                                {p.name} ({p.position}{p.team ? ' - ' + p.team : ''})
                              </option>
                            ))}
                        </select>
                        <button
                          type="button"
                          onClick={() => handleAddOfferedPlayer(selectedOfferedPlayerId)}
                          disabled={!selectedOfferedPlayerId}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold disabled:opacity-40 cursor-pointer whitespace-nowrap"
                        >
                          + Add Player
                        </button>
                      </div>
                    </div>

                    {/* 3. Budget Cash */}
                    <div className="space-y-1">
                      <label className="block text-[11px] font-bold text-slate-400">
                        Add Draft Budget Cash ($):
                      </label>
                      <div className="flex gap-2">
                        <input
                          type="number"
                          min="0"
                          max="100"
                          placeholder="$ Amount"
                          value={selectedOfferedBudget}
                          onChange={e => setSelectedOfferedBudget(e.target.value)}
                          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                        />
                        <button
                          type="button"
                          onClick={() => handleAddOfferedBudget(selectedOfferedBudget)}
                          disabled={!selectedOfferedBudget || parseFloat(selectedOfferedBudget) <= 0}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold disabled:opacity-40 cursor-pointer whitespace-nowrap"
                        >
                          + Add Budget
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* Right: What Proposer Requests */}
                  <div className="space-y-4">
                    <div className="text-xs font-black uppercase tracking-wider text-teal-400 border-b border-slate-800 pb-2 flex items-center justify-between">
                      <span>📥 {propSender} Receives (from {propTarget}):</span>
                      <span className="text-[10px] text-slate-400 lowercase">{targetAvailablePicks.length} picks • {(teamRosters[propTarget] || []).length} players</span>
                    </div>

                    {/* Staged Requested Assets Tags */}
                    {requestedAssets.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 p-2 bg-slate-900/80 rounded-lg border border-slate-800 min-h-[38px] items-center">
                        {requestedAssets.map((asset, idx) => (
                          <span key={idx}>
                            {renderAssetBadge(asset, () => handleRemoveRequestedAsset(idx))}
                          </span>
                        ))}
                      </div>
                    )}

                    {/* 1. Pick Dropdown */}
                    <div className="space-y-1">
                      <label className="block text-[11px] font-bold text-slate-400">
                        Request 2027 Draft Pick:
                      </label>
                      <div className="flex gap-2">
                        <select
                          value={selectedRequestedPickRound}
                          onChange={e => {
                            const val = e.target.value;
                            setSelectedRequestedPickRound(val);
                            if (val) handleAddRequestedPick(val);
                          }}
                          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                        >
                          <option value="">-- Select draft pick --</option>
                          {targetAvailablePicks
                            .filter(p => !isPickInAssets(p, requestedAssets))
                            .map(p => {
                              const val = `${p.round}:${p.originalOwner}`;
                              return (
                                <option key={p.id || val} value={val}>
                                  Round {p.round} Pick {p.originalOwner !== propTarget ? `(Orig: ${p.originalOwner})` : ''}
                                </option>
                              );
                            })}
                        </select>
                        <button
                          type="button"
                          onClick={() => handleAddRequestedPick(selectedRequestedPickRound)}
                          disabled={!selectedRequestedPickRound}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold disabled:opacity-40 cursor-pointer whitespace-nowrap"
                        >
                          + Add Pick
                        </button>
                      </div>
                    </div>

                    {/* 2. Player Dropdown */}
                    <div className="space-y-1">
                      <label className="block text-[11px] font-bold text-slate-400">
                        Request Rostered Player:
                      </label>
                      <div className="flex gap-2">
                        <select
                          value={selectedRequestedPlayerId}
                          onChange={e => {
                            const val = e.target.value;
                            setSelectedRequestedPlayerId(val);
                            if (val) handleAddRequestedPlayer(val);
                          }}
                          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                        >
                          <option value="">-- Select rostered player --</option>
                          {(teamRosters[propTarget] || [])
                            .filter(p => !isPlayerInAssets(p, requestedAssets))
                            .map(p => (
                              <option key={p.player_id} value={p.player_id}>
                                {p.name} ({p.position}{p.team ? ' - ' + p.team : ''})
                              </option>
                            ))}
                        </select>
                        <button
                          type="button"
                          onClick={() => handleAddRequestedPlayer(selectedRequestedPlayerId)}
                          disabled={!selectedRequestedPlayerId}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold disabled:opacity-40 cursor-pointer whitespace-nowrap"
                        >
                          + Add Player
                        </button>
                      </div>
                    </div>

                    {/* 3. Budget Cash */}
                    <div className="space-y-1">
                      <label className="block text-[11px] font-bold text-slate-400">
                        Request Draft Budget Cash ($):
                      </label>
                      <div className="flex gap-2">
                        <input
                          type="number"
                          min="0"
                          max="100"
                          placeholder="$ Amount"
                          value={selectedRequestedBudget}
                          onChange={e => setSelectedRequestedBudget(e.target.value)}
                          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                        />
                        <button
                          type="button"
                          onClick={() => handleAddRequestedBudget(selectedRequestedBudget)}
                          disabled={!selectedRequestedBudget || parseFloat(selectedRequestedBudget) <= 0}
                          className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold disabled:opacity-40 cursor-pointer whitespace-nowrap"
                        >
                          + Add Budget
                        </button>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Pick Imbalance Warning Alert */}
                {hasFormPickDisparity && (
                  <div className="p-4 rounded-xl bg-amber-950/40 border border-amber-500/60 flex items-start gap-3 animate-fade-in">
                    <span className="text-xl">⚠️</span>
                    <div className="space-y-1">
                      <div className="text-xs font-black text-amber-300 uppercase tracking-wide">
                        Pick Imbalance Warning: Unequal Draft Picks ({builderOfferedPicksCount} offered vs {builderRequestedPicksCount} requested)
                      </div>
                      <p className="text-[11px] text-amber-200/90 leading-relaxed">
                        In our 27-round draft format (Rounds 6 through 32), trading an unequal number of draft picks leaves teams with unequal roster sizes unless an offsetting pick (such as a late-round Round 32 pick swap) is included. You may still proceed if this disparity is intentional.
                      </p>
                    </div>
                  </div>
                )}

                {/* Notes */}
                <div>
                  <label className="block text-xs font-bold text-slate-300 mb-1">
                    Trade Rationale & Conditions (Optional):
                  </label>
                  <input
                    type="text"
                    placeholder="e.g. Multi-player swap, conditional pick exchange, or draft strategy..."
                    value={tradeNotes}
                    onChange={e => setTradeNotes(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white placeholder-slate-600"
                  />
                </div>

                <div className="flex justify-end gap-3 pt-2">
                  <button
                    type="button"
                    onClick={() => setProposeModalOpen(false)}
                    className="px-4 py-2 rounded-xl text-xs font-bold text-slate-400 hover:text-white cursor-pointer"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={submittingTrade || !user}
                    className="px-5 py-2 rounded-xl bg-rose-600 hover:bg-rose-500 text-white font-bold text-xs shadow-md cursor-pointer disabled:opacity-50 transition-all"
                  >
                    {submittingTrade ? 'Submitting...' : '📤 Send Official Trade Proposal'}
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* Proposals Inbox / Outbox / Commish Filter Sub-Navigation */}
          <div className="flex flex-wrap items-center justify-between gap-3 bg-slate-900 border border-slate-800 p-3 rounded-2xl shadow-lg">
            <div className="flex flex-wrap items-center gap-2 text-xs font-bold">
              <button
                onClick={() => setInboxTab('inbox')}
                className={`px-3.5 py-2 rounded-xl transition-all cursor-pointer flex items-center gap-1.5 ${
                  inboxTab === 'inbox'
                    ? 'bg-indigo-600 text-white shadow-md font-black'
                    : 'bg-slate-950 text-slate-400 hover:text-white border border-slate-800'
                }`}
              >
                <span>📥 Incoming Offers</span>
                {incomingProposals.length > 0 && (
                  <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-rose-500 text-white animate-pulse">
                    {incomingProposals.length}
                  </span>
                )}
              </button>

              <button
                onClick={() => setInboxTab('outbox')}
                className={`px-3.5 py-2 rounded-xl transition-all cursor-pointer flex items-center gap-1.5 ${
                  inboxTab === 'outbox'
                    ? 'bg-indigo-600 text-white shadow-md font-black'
                    : 'bg-slate-950 text-slate-400 hover:text-white border border-slate-800'
                }`}
              >
                <span>📤 Sent Proposals</span>
                {outgoingProposals.length > 0 && (
                  <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-slate-800 text-slate-300">
                    {outgoingProposals.length}
                  </span>
                )}
              </button>

              <button
                onClick={() => setInboxTab('commish')}
                className={`px-3.5 py-2 rounded-xl transition-all cursor-pointer flex items-center gap-1.5 ${
                  inboxTab === 'commish'
                    ? 'bg-amber-500 text-slate-950 shadow-md font-black'
                    : 'bg-slate-950 text-amber-300 hover:text-amber-200 border border-slate-800'
                }`}
              >
                <span>👑 Commish Review</span>
                {commishQueueProposals.length > 0 && (
                  <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-amber-400 text-slate-950 animate-pulse">
                    {commishQueueProposals.length}
                  </span>
                )}
              </button>

              <button
                onClick={() => setInboxTab('all')}
                className={`px-3.5 py-2 rounded-xl transition-all cursor-pointer flex items-center gap-1.5 ${
                  inboxTab === 'all'
                    ? 'bg-slate-800 text-white shadow-md font-black'
                    : 'bg-slate-950 text-slate-400 hover:text-white border border-slate-800'
                }`}
              >
                <span>🌐 League All ({leagueAcceptedTrades.length})</span>
              </button>

              <button
                onClick={() => setInboxTab('archive')}
                className={`px-3.5 py-2 rounded-xl transition-all cursor-pointer flex items-center gap-1.5 ${
                  inboxTab === 'archive'
                    ? 'bg-slate-800 text-white shadow-md font-black'
                    : 'bg-slate-950 text-slate-400 hover:text-white border border-slate-800'
                }`}
              >
                <span>📜 Archive ({archivedProposals.length})</span>
              </button>
            </div>

            <div className="text-xs text-slate-400">
              {inboxTab === 'inbox' && `Showing offers waiting for ${viewPerspectiveOwner}'s decision`}
              {inboxTab === 'outbox' && `Showing offers sent by ${viewPerspectiveOwner} awaiting response`}
              {inboxTab === 'commish' && 'Agreed by both parties • Pending commissioner execution'}
              {inboxTab === 'all' && 'Agreed & executed trades across the league • Active proposals and private negotiations remain confidential between parties'}
              {inboxTab === 'archive' && 'Historical approved, declined, and cancelled proposals'}
            </div>
          </div>

          {/* Proposal List Rendering */}
          {inboxTab !== 'archive' ? (
            <div className="space-y-4">
              {(() => {
                let listToRender = [];
                if (inboxTab === 'inbox') listToRender = incomingProposals;
                else if (inboxTab === 'outbox') listToRender = outgoingProposals;
                else if (inboxTab === 'commish') listToRender = commishQueueProposals;
                else if (inboxTab === 'all') listToRender = leagueAcceptedTrades;

                if (listToRender.length === 0) {
                  return (
                    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-10 text-center space-y-2">
                      <div className="text-3xl">
                        {inboxTab === 'inbox' ? '📭' : inboxTab === 'outbox' ? '📤' : inboxTab === 'commish' ? '👑' : '🤝'}
                      </div>
                      <div className="text-slate-400 text-sm font-bold">
                        {inboxTab === 'inbox' && `No pending incoming trade offers for ${viewPerspectiveOwner}.`}
                        {inboxTab === 'outbox' && `No active proposals sent by ${viewPerspectiveOwner}.`}
                        {inboxTab === 'commish' && 'No proposals currently waiting in the Commissioner queue.'}
                        {inboxTab === 'all' && 'No accepted trades in the league yet. Active negotiations remain confidential between owners.'}
                      </div>
                      <div className="text-slate-500 text-xs">
                        Use the "➕ Propose New Trade" button above to initiate a pick, player, or budget proposal.
                      </div>
                    </div>
                  );
                }

                return (
                  <div className="grid grid-cols-1 gap-4">
                    {listToRender.map(prop => {
                      const isSender = profile?.owner_name?.toLowerCase() === prop.proposing_owner?.toLowerCase() ||
                        (profile?.owner_name === 'Dan' && prop.proposing_owner === 'Daniel') ||
                        (profile?.owner_name === 'Daniel' && prop.proposing_owner === 'Dan');

                      const isTarget = profile?.owner_name?.toLowerCase() === prop.target_owner?.toLowerCase() ||
                        (profile?.owner_name === 'Dan' && prop.target_owner === 'Daniel') ||
                        (profile?.owner_name === 'Daniel' && prop.target_owner === 'Dan');

                      const isAwaitingCommish = prop.status === 'accepted_by_partner';
                      const isLoading = actionLoadingId === prop.id;

                      // Check pick imbalance
                      const offeredPicks = (prop.offered_assets || []).filter(a => a.type === 'pick').length;
                      const requestedPicks = (prop.requested_assets || []).filter(a => a.type === 'pick').length;
                      const hasDisparity = (offeredPicks > 0 || requestedPicks > 0) && offeredPicks !== requestedPicks;

                      return (
                        <div
                          key={prop.id}
                          className={`rounded-2xl p-5 border shadow-xl transition-all ${
                            isAwaitingCommish
                              ? 'bg-amber-950/20 border-amber-500/50'
                              : 'bg-slate-900 border-slate-800'
                          }`}
                        >
                          {/* Card Header */}
                          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-3">
                            <div className="flex items-center gap-3">
                              <span className="text-xl">🤝</span>
                              <div>
                                <div className="text-sm font-bold text-white flex items-center gap-2">
                                  <span>{prop.proposing_owner} ⇄ {prop.target_owner}</span>
                                  {hasDisparity && (
                                    <span
                                      className="px-2 py-0.5 rounded-md text-[10px] font-black bg-amber-500/20 border border-amber-500/50 text-amber-300 flex items-center gap-1"
                                      title="Unequal number of draft picks involved"
                                    >
                                      <span>⚠️</span> Pick Disparity ({offeredPicks} vs {requestedPicks})
                                    </span>
                                  )}
                                </div>
                                <div className="text-[10px] text-slate-400">
                                  Proposed: {new Date(prop.proposed_at || prop.created_at).toLocaleDateString()}
                                </div>
                              </div>
                            </div>

                            <div className="flex items-center gap-2">
                              {isAwaitingCommish ? (
                                <span className="px-2.5 py-1 rounded-full text-xs font-black bg-amber-400/20 border border-amber-400/60 text-amber-300 animate-pulse flex items-center gap-1.5">
                                  <span>👑</span> Awaiting Commissioner Approval
                                </span>
                              ) : (
                                <span className="px-2.5 py-1 rounded-full text-xs font-bold bg-indigo-500/20 border border-indigo-500/40 text-indigo-300">
                                  ⏳ Pending Partner Agreement ({prop.target_owner})
                                </span>
                              )}
                            </div>
                          </div>

                          {/* Assets Details */}
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 py-4 text-xs">
                            {/* Left: What Proposing Owner Sends */}
                            <div className="bg-slate-950/60 p-3 rounded-xl border border-slate-800/80">
                              <div className="text-[11px] font-bold text-slate-400 mb-2 flex items-center justify-between">
                                <span>📤 {prop.proposing_owner} Sends:</span>
                                <span className="text-[10px] text-slate-500">{(prop.offered_assets || []).length} assets</span>
                              </div>
                              <div className="flex flex-wrap gap-1.5">
                                {(prop.offered_assets || []).map((a, i) => (
                                  <span key={i}>{renderAssetBadge(a)}</span>
                                ))}
                              </div>
                            </div>

                            {/* Right: What Target Owner Sends */}
                            <div className="bg-slate-950/60 p-3 rounded-xl border border-slate-800/80">
                              <div className="text-[11px] font-bold text-slate-400 mb-2 flex items-center justify-between">
                                <span>📥 {prop.target_owner} Sends:</span>
                                <span className="text-[10px] text-slate-500">{(prop.requested_assets || []).length} assets</span>
                              </div>
                              <div className="flex flex-wrap gap-1.5">
                                {(prop.requested_assets || []).map((a, i) => (
                                  <span key={i}>{renderAssetBadge(a)}</span>
                                ))}
                              </div>
                            </div>
                          </div>

                          {prop.notes && (
                            <div className="text-xs text-slate-400 italic mb-3 bg-slate-950/40 p-2.5 rounded-lg border border-slate-800/50">
                              "{prop.notes}"
                            </div>
                          )}

                          {/* Action Buttons */}
                          <div className="pt-2 border-t border-slate-800 flex flex-wrap items-center justify-between gap-3">
                            <div className="text-[11px] text-slate-500">
                              {isAwaitingCommish
                                ? `Agreed by both parties. Dan or Adrian must officially approve.`
                                : `Awaiting agreement from ${prop.target_owner}.`}
                            </div>

                            <div className="flex flex-wrap items-center gap-2">
                              {/* Stage 1 Actions (Partner Acceptance / Decline / Counter) */}
                              {!isAwaitingCommish && (
                                <>
                                  {(isTarget || isCommissioner) && (
                                    <>
                                      <button
                                        onClick={() => handlePartnerAccept(prop)}
                                        disabled={isLoading}
                                        className="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs shadow-xs cursor-pointer transition-all"
                                      >
                                        {isLoading ? 'Processing...' : '✅ Accept Trade'}
                                      </button>
                                      <button
                                        onClick={() => handleCounterOffer(prop)}
                                        disabled={isLoading}
                                        className="px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs shadow-xs cursor-pointer transition-all"
                                      >
                                        💬 Counter Offer
                                      </button>
                                      <button
                                        onClick={() => handleDeclineOrCancel(prop, 'declined')}
                                        disabled={isLoading}
                                        className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-rose-950 text-rose-400 border border-rose-500/40 font-bold text-xs cursor-pointer transition-all"
                                      >
                                        Decline
                                      </button>
                                    </>
                                  )}

                                  {(isSender || isCommissioner) && !isTarget && (
                                    <button
                                      onClick={() => handleDeclineOrCancel(prop, 'cancelled')}
                                      disabled={isLoading}
                                      className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-400 font-bold text-xs cursor-pointer transition-all"
                                    >
                                      Cancel Proposal
                                    </button>
                                  )}
                                </>
                              )}

                              {/* Stage 2 Actions (Commissioner Approval) */}
                              {isAwaitingCommish && (
                                <>
                                  {isCommissioner ? (
                                    <div className="flex flex-wrap items-center gap-2 bg-amber-950/40 border border-amber-500/50 p-1.5 px-3 rounded-xl">
                                      <span className="text-xs font-bold text-amber-300">👑 Commish Action:</span>
                                      <button
                                        onClick={() => handleCommissionerApprove(prop)}
                                        disabled={isLoading}
                                        className="px-3 py-1.5 rounded-lg bg-gradient-to-r from-amber-500 to-yellow-500 hover:from-amber-400 hover:to-yellow-400 text-slate-950 font-black text-xs shadow-md cursor-pointer transition-all"
                                      >
                                        {isLoading ? 'Executing...' : '✅ Approve & Execute Trade'}
                                      </button>
                                      <button
                                        onClick={() => handleDeclineOrCancel(prop, 'declined')}
                                        disabled={isLoading}
                                        className="px-3 py-1.5 rounded-lg bg-rose-950/80 text-rose-300 border border-rose-500/50 font-bold text-xs hover:bg-rose-900 cursor-pointer transition-all"
                                      >
                                        Veto
                                      </button>
                                    </div>
                                  ) : (
                                    <span className="text-xs font-bold text-amber-300 flex items-center gap-1">
                                      <span>⏳</span> Pending Dan / Adrian Approval
                                    </span>
                                  )}
                                </>
                              )}

                              {/* Commissioner Manual Edit Button */}
                              {isCommissioner && (
                                <button
                                  onClick={() => handleOpenEditModal(prop)}
                                  className="px-2.5 py-1.5 rounded-lg bg-slate-800 hover:bg-amber-950/60 text-amber-300 border border-amber-500/40 font-bold text-xs cursor-pointer transition-all"
                                  title="Commissioner Manual Edit: adjust assets, owners, notes"
                                >
                                  ✏️ Edit (Commish)
                                </button>
                              )}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
            </div>
          ) : (
            /* Historical Proposals Archive */
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-3">
              <h3 className="text-sm font-black uppercase tracking-wider text-slate-300 flex items-center gap-2">
                <span>📜</span> Completed Trade Proposals Archive
              </h3>

              {archivedProposals.length === 0 ? (
                <div className="text-xs text-slate-500 py-3 text-center">
                  No past proposals recorded yet.
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-left text-xs border-collapse">
                    <thead>
                      <tr className="border-b border-slate-800 text-slate-500 text-[11px] font-bold">
                        <th className="py-2.5 px-3">Date</th>
                        <th className="py-2.5 px-3">Proposing</th>
                        <th className="py-2.5 px-3">Target</th>
                        <th className="py-2.5 px-3">Assets Exchanged</th>
                        <th className="py-2.5 px-3 text-center">Status</th>
                        <th className="py-2.5 px-3 text-right">Resolved By</th>
                        {isCommissioner && <th className="py-2.5 px-3 text-right">Commish</th>}
                      </tr>
                    </thead>
                    <tbody>
                      {archivedProposals.map(p => (
                        <tr key={p.id} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                          <td className="py-2.5 px-3 text-slate-400 font-mono text-[11px]">
                            {new Date(p.proposed_at || p.created_at).toLocaleDateString()}
                          </td>
                          <td className="py-2.5 px-3 font-bold text-white">{p.proposing_owner}</td>
                          <td className="py-2.5 px-3 font-bold text-white">{p.target_owner}</td>
                          <td className="py-2.5 px-3 text-slate-300">
                            <div className="flex flex-col gap-1">
                              <div>
                                <span className="text-rose-400 font-bold">{p.proposing_owner}:</span>{' '}
                                {(p.offered_assets || []).map(a => a.label || a.name).join(', ')}
                              </div>
                              <div>
                                <span className="text-teal-400 font-bold">{p.target_owner}:</span>{' '}
                                {(p.requested_assets || []).map(a => a.label || a.name).join(', ')}
                              </div>
                            </div>
                          </td>
                          <td className="py-2.5 px-3 text-center">
                            {p.status === 'approved' && (
                              <span className="px-2 py-0.5 rounded bg-emerald-950/80 border border-emerald-500/50 text-emerald-300 font-black text-[10px]">
                                APPROVED
                              </span>
                            )}
                            {p.status === 'declined' && (
                              <span className="px-2 py-0.5 rounded bg-rose-950/80 border border-rose-500/50 text-rose-300 font-black text-[10px]">
                                DECLINED
                              </span>
                            )}
                            {p.status === 'cancelled' && (
                              <span className="px-2 py-0.5 rounded bg-slate-800 text-slate-400 font-black text-[10px]">
                                CANCELLED
                              </span>
                            )}
                          </td>
                          <td className="py-2.5 px-3 text-right text-slate-400 text-[11px]">
                            {p.responded_by || '-'}
                          </td>
                          {isCommissioner && (
                            <td className="py-2.5 px-3 text-right">
                              <button
                                onClick={() => handleOpenEditModal(p)}
                                className="px-2 py-1 rounded bg-slate-800 text-amber-300 hover:bg-amber-950/60 text-[10px] font-bold border border-amber-500/30 cursor-pointer"
                              >
                                ✏️ Edit
                              </button>
                            </td>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* COMMISSIONER EDIT MODAL */}
          {editingProposal && (
            <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4 overflow-y-auto">
              <div className="bg-slate-900 border border-amber-500/50 rounded-2xl p-6 shadow-2xl max-w-3xl w-full space-y-5 my-8 max-h-[90vh] overflow-y-auto">
                <div className="flex items-center justify-between border-b border-slate-800 pb-3">
                  <div className="flex items-center gap-2">
                    <span className="text-xl">👑</span>
                    <h3 className="text-base font-black text-amber-300">
                      Commissioner Manual Trade Editor
                    </h3>
                  </div>
                  <button
                    onClick={() => setEditingProposal(null)}
                    className="text-slate-400 hover:text-white font-black text-sm cursor-pointer"
                  >
                    ✕
                  </button>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs font-bold text-slate-300 mb-1">
                      Proposing Team:
                    </label>
                    <select
                      value={editingProposal.proposing_owner}
                      onChange={e => setEditingProposal(prev => ({ ...prev, proposing_owner: e.target.value }))}
                      className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-white"
                    >
                      {DRAFT_OWNERS.map(o => (
                        <option key={o} value={o}>{o} (Team {getTeamId(o)})</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs font-bold text-slate-300 mb-1">
                      Target Team:
                    </label>
                    <select
                      value={editingProposal.target_owner}
                      onChange={e => setEditingProposal(prev => ({ ...prev, target_owner: e.target.value }))}
                      className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-white"
                    >
                      {DRAFT_OWNERS.map(o => (
                        <option key={o} value={o}>{o} (Team {getTeamId(o)})</option>
                      ))}
                    </select>
                  </div>
                </div>

                {/* Edit Assets Grid */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 rounded-xl bg-slate-950/60 border border-slate-800">
                  {/* Offered Assets */}
                  <div className="space-y-3">
                    <div className="text-xs font-black uppercase text-rose-400 border-b border-slate-800 pb-1 flex items-center justify-between">
                      <span>📤 {editingProposal.proposing_owner} Assets</span>
                      <span className="text-[10px] text-slate-500">{(editingProposal.offered_assets || []).length} items</span>
                    </div>

                    <div className="flex flex-wrap gap-1.5 min-h-[40px] items-center p-2 bg-slate-900 rounded-lg border border-slate-800">
                      {(editingProposal.offered_assets || []).map((a, i) => (
                        <span key={i}>
                          {renderAssetBadge(a, () => {
                            setEditingProposal(prev => ({
                              ...prev,
                              offered_assets: prev.offered_assets.filter((_, idx) => idx !== i)
                            }));
                          })}
                        </span>
                      ))}
                    </div>

                    {/* Quick Add Pick */}
                    <div className="flex gap-2">
                      <select
                        value={editOfferedPickRound}
                        onChange={e => setEditOfferedPickRound(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2 py-1 text-xs text-white"
                      >
                        <option value="">+ Add Pick...</option>
                        {editSenderAvailablePicks
                          .filter(p => !isPickInAssets(p, editingProposal.offered_assets))
                          .map(p => {
                            const val = `${p.round}:${p.originalOwner}`;
                            return (
                              <option key={p.id || val} value={val}>
                                Round {p.round} (Orig: {p.originalOwner})
                              </option>
                            );
                          })}
                      </select>
                      <button
                        type="button"
                        onClick={() => {
                          if (!editOfferedPickRound) return;
                          const p = editSenderAvailablePicks.find(item => {
                            const itemKey = `${item.round}:${item.originalOwner}`;
                            return item.id === editOfferedPickRound || itemKey === editOfferedPickRound || String(item.round) === String(editOfferedPickRound);
                          });
                          if (!p || isPickInAssets(p, editingProposal.offered_assets)) return;
                          setEditingProposal(prev => ({
                            ...prev,
                            offered_assets: [
                              ...prev.offered_assets,
                              {
                                type: 'pick',
                                pick_id: p.id || `pick-${p.round}-${p.originalOwner}`,
                                round: Number(p.round),
                                original_owner: p?.originalOwner || prev.proposing_owner,
                                label: `Round ${p.round} Pick (Orig: ${p?.originalOwner || prev.proposing_owner})`
                              }
                            ]
                          }));
                          setEditOfferedPickRound('');
                        }}
                        disabled={!editOfferedPickRound}
                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-xs font-bold rounded-lg text-slate-200 disabled:opacity-40 cursor-pointer"
                      >
                        Add
                      </button>
                    </div>

                    {/* Quick Add Player */}
                    <div className="flex gap-2">
                      <select
                        value={editOfferedPlayerId}
                        onChange={e => setEditOfferedPlayerId(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2 py-1 text-xs text-white"
                      >
                        <option value="">+ Add Player...</option>
                        {(teamRosters[editingProposal.proposing_owner] || [])
                          .filter(p => !isPlayerInAssets(p, editingProposal.offered_assets))
                          .map(p => (
                            <option key={p.player_id} value={p.player_id}>
                              {p.name} ({p.position})
                            </option>
                          ))}
                      </select>
                      <button
                        type="button"
                        onClick={() => {
                          const pid = parseInt(editOfferedPlayerId, 10);
                          if (!pid) return;
                          const p = (teamRosters[editingProposal.proposing_owner] || []).find(item => Number(item.player_id) === pid);
                          if (!p || isPlayerInAssets(p, editingProposal.offered_assets)) return;
                          setEditingProposal(prev => ({
                            ...prev,
                            offered_assets: [
                              ...prev.offered_assets,
                              {
                                type: 'player',
                                player_id: Number(p.player_id),
                                name: p.name,
                                position: p.position,
                                team: p.team,
                                label: p.label
                              }
                            ]
                          }));
                          setEditOfferedPlayerId('');
                        }}
                        disabled={!editOfferedPlayerId}
                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-xs font-bold rounded-lg text-slate-200 disabled:opacity-40 cursor-pointer"
                      >
                        Add
                      </button>
                    </div>

                    {/* Quick Add Budget */}
                    <div className="flex gap-2">
                      <input
                        type="number"
                        placeholder="$ Budget"
                        value={editOfferedBudget}
                        onChange={e => setEditOfferedBudget(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2 py-1 text-xs text-white"
                      />
                      <button
                        type="button"
                        onClick={() => {
                          const amt = parseFloat(editOfferedBudget);
                          if (!amt || amt <= 0) return;
                          setEditingProposal(prev => ({
                            ...prev,
                            offered_assets: [
                              ...prev.offered_assets.filter(a => a.type !== 'budget'),
                              {
                                type: 'budget',
                                amount: amt,
                                label: `$${amt} Draft Budget`
                              }
                            ]
                          }));
                          setEditOfferedBudget('');
                        }}
                        disabled={!editOfferedBudget || parseFloat(editOfferedBudget) <= 0}
                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-xs font-bold rounded-lg text-slate-200 disabled:opacity-40 cursor-pointer"
                      >
                        Add
                      </button>
                    </div>
                  </div>

                  {/* Requested Assets */}
                  <div className="space-y-3">
                    <div className="text-xs font-black uppercase text-teal-400 border-b border-slate-800 pb-1 flex items-center justify-between">
                      <span>📥 {editingProposal.target_owner} Assets</span>
                      <span className="text-[10px] text-slate-500">{(editingProposal.requested_assets || []).length} items</span>
                    </div>

                    <div className="flex flex-wrap gap-1.5 min-h-[40px] items-center p-2 bg-slate-900 rounded-lg border border-slate-800">
                      {(editingProposal.requested_assets || []).map((a, i) => (
                        <span key={i}>
                          {renderAssetBadge(a, () => {
                            setEditingProposal(prev => ({
                              ...prev,
                              requested_assets: prev.requested_assets.filter((_, idx) => idx !== i)
                            }));
                          })}
                        </span>
                      ))}
                    </div>

                    {/* Quick Add Pick */}
                    <div className="flex gap-2">
                      <select
                        value={editRequestedPickRound}
                        onChange={e => setEditRequestedPickRound(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2 py-1 text-xs text-white"
                      >
                        <option value="">+ Add Pick...</option>
                        {editTargetAvailablePicks
                          .filter(p => !isPickInAssets(p, editingProposal.requested_assets))
                          .map(p => {
                            const val = `${p.round}:${p.originalOwner}`;
                            return (
                              <option key={p.id || val} value={val}>
                                Round {p.round} (Orig: {p.originalOwner})
                              </option>
                            );
                          })}
                      </select>
                      <button
                        type="button"
                        onClick={() => {
                          if (!editRequestedPickRound) return;
                          const p = editTargetAvailablePicks.find(item => {
                            const itemKey = `${item.round}:${item.originalOwner}`;
                            return item.id === editRequestedPickRound || itemKey === editRequestedPickRound || String(item.round) === String(editRequestedPickRound);
                          });
                          if (!p || isPickInAssets(p, editingProposal.requested_assets)) return;
                          setEditingProposal(prev => ({
                            ...prev,
                            requested_assets: [
                              ...prev.requested_assets,
                              {
                                type: 'pick',
                                pick_id: p.id || `pick-${p.round}-${p.originalOwner}`,
                                round: Number(p.round),
                                original_owner: p?.originalOwner || prev.target_owner,
                                label: `Round ${p.round} Pick (Orig: ${p?.originalOwner || prev.target_owner})`
                              }
                            ]
                          }));
                          setEditRequestedPickRound('');
                        }}
                        disabled={!editRequestedPickRound}
                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-xs font-bold rounded-lg text-slate-200 disabled:opacity-40 cursor-pointer"
                      >
                        Add
                      </button>
                    </div>

                    {/* Quick Add Player */}
                    <div className="flex gap-2">
                      <select
                        value={editRequestedPlayerId}
                        onChange={e => setEditRequestedPlayerId(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2 py-1 text-xs text-white"
                      >
                        <option value="">+ Add Player...</option>
                        {(teamRosters[editingProposal.target_owner] || [])
                          .filter(p => !isPlayerInAssets(p, editingProposal.requested_assets))
                          .map(p => (
                            <option key={p.player_id} value={p.player_id}>
                              {p.name} ({p.position})
                            </option>
                          ))}
                      </select>
                      <button
                        type="button"
                        onClick={() => {
                          const pid = parseInt(editRequestedPlayerId, 10);
                          if (!pid) return;
                          const p = (teamRosters[editingProposal.target_owner] || []).find(item => Number(item.player_id) === pid);
                          if (!p || isPlayerInAssets(p, editingProposal.requested_assets)) return;
                          setEditingProposal(prev => ({
                            ...prev,
                            requested_assets: [
                              ...prev.requested_assets,
                              {
                                type: 'player',
                                player_id: Number(p.player_id),
                                name: p.name,
                                position: p.position,
                                team: p.team,
                                label: p.label
                              }
                            ]
                          }));
                          setEditRequestedPlayerId('');
                        }}
                        disabled={!editRequestedPlayerId}
                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-xs font-bold rounded-lg text-slate-200 disabled:opacity-40 cursor-pointer"
                      >
                        Add
                      </button>
                    </div>

                    {/* Quick Add Budget */}
                    <div className="flex gap-2">
                      <input
                        type="number"
                        placeholder="$ Budget"
                        value={editRequestedBudget}
                        onChange={e => setEditRequestedBudget(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2 py-1 text-xs text-white"
                      />
                      <button
                        type="button"
                        onClick={() => {
                          const amt = parseFloat(editRequestedBudget);
                          if (!amt || amt <= 0) return;
                          setEditingProposal(prev => ({
                            ...prev,
                            requested_assets: [
                              ...prev.requested_assets.filter(a => a.type !== 'budget'),
                              {
                                type: 'budget',
                                amount: amt,
                                label: `$${amt} Draft Budget`
                              }
                            ]
                          }));
                          setEditRequestedBudget('');
                        }}
                        disabled={!editRequestedBudget || parseFloat(editRequestedBudget) <= 0}
                        className="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-xs font-bold rounded-lg text-slate-200 disabled:opacity-40 cursor-pointer"
                      >
                        Add
                      </button>
                    </div>
                  </div>
                </div>

                {/* Status & Notes */}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs font-bold text-slate-300 mb-1">
                      Proposal Status:
                    </label>
                    <select
                      value={editingProposal.status}
                      onChange={e => setEditingProposal(prev => ({ ...prev, status: e.target.value }))}
                      className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-amber-300"
                    >
                      <option value="pending">pending (awaiting partner)</option>
                      <option value="accepted_by_partner">accepted_by_partner (ready for commish execution)</option>
                      <option value="approved">approved (executed)</option>
                      <option value="declined">declined</option>
                      <option value="cancelled">cancelled</option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-xs font-bold text-slate-300 mb-1">
                      Notes / Terms:
                    </label>
                    <input
                      type="text"
                      value={editingProposal.notes || ''}
                      onChange={e => setEditingProposal(prev => ({ ...prev, notes: e.target.value }))}
                      className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white"
                    />
                  </div>
                </div>

                {/* Pick Disparity Warning inside Commish Editor */}
                {(() => {
                  const oPicks = (editingProposal.offered_assets || []).filter(a => a.type === 'pick').length;
                  const rPicks = (editingProposal.requested_assets || []).filter(a => a.type === 'pick').length;
                  if ((oPicks > 0 || rPicks > 0) && oPicks !== rPicks) {
                    return (
                      <div className="p-3 bg-amber-950/40 border border-amber-500/50 rounded-xl text-xs text-amber-200 flex items-center gap-2">
                        <span>⚠️</span>
                        <span>
                          <strong>Pick Disparity:</strong> This trade involves {oPicks} offered pick(s) vs {rPicks} requested pick(s). Unequal picks will cause uneven roster sizes at the 27-round draft.
                        </span>
                      </div>
                    );
                  }
                  return null;
                })()}

                {/* Modal Footer */}
                <div className="flex justify-end gap-3 pt-3 border-t border-slate-800">
                  <button
                    type="button"
                    onClick={() => setEditingProposal(null)}
                    className="px-4 py-2 rounded-xl text-xs font-bold text-slate-400 hover:text-white cursor-pointer"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={handleSaveProposalEdit}
                    disabled={savingEdit}
                    className="px-5 py-2 rounded-xl bg-gradient-to-r from-amber-500 to-yellow-500 text-slate-950 font-black text-xs shadow-md cursor-pointer disabled:opacity-50"
                  >
                    {savingEdit ? 'Saving...' : '💾 Save Changes to Proposal'}
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
