// src/views/DraftCapitalView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import defaultDraftAssetTrades from '../data/draftAssetTrades2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import { useAuth } from '../context/useAuth';
import { LEAGUE_OWNERS } from '../utils/mlbTeams';

const DRAFT_OWNERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"];

function compute2027DraftPicks(draftTrades = []) {
  const owners = [...DRAFT_OWNERS].sort();
  const picks = [];

  // Base draft order: 27 rounds (Rounds 6 through 32), 1 pick per owner per round (243 total picks)
  for (let round = 6; round <= 32; round++) {
    owners.forEach(owner => {
      picks.push({
        round,
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
    const round = trade.round_num;
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

export default function DraftCapitalView({ currentUser = 'Daniel', isCommissioner: propIsCommissioner = false }) {
  const { user, profile, isCommissioner: authIsCommissioner, effectiveOwner } = useAuth();
  const isCommissioner = propIsCommissioner || authIsCommissioner;

  const [draftTrades, setDraftTrades] = useState(defaultDraftAssetTrades);
  const [compPicks, setCompPicks] = useState(defaultCompPicks);
  const [keepers, setKeepers] = useState(defaultKeepers);
  const [proposals, setProposals] = useState([]);
  const [loading, setLoading] = useState(true);

  const [activeSubTab, setActiveSubTab] = useState('board'); // 'board' | 'ledgers' | 'history' | '2026board' | 'proposals'
  const [selectedOwner, setSelectedOwner] = useState(currentUser);
  const [roundFilter, setRoundFilter] = useState('ALL');

  // Trade Proposal Form State
  const [propSender, setPropSender] = useState(effectiveOwner || currentUser);
  const [propTarget, setPropTarget] = useState(DRAFT_OWNERS.find(o => o !== (effectiveOwner || currentUser)) || 'Adrian');
  const [offeredPickRound, setOfferedPickRound] = useState('');
  const [offeredBudget, setOfferedBudget] = useState('');
  const [requestedPickRound, setRequestedPickRound] = useState('');
  const [requestedBudget, setRequestedBudget] = useState('');
  const [tradeNotes, setTradeNotes] = useState('');
  const [submittingTrade, setSubmittingTrade] = useState(false);
  const [actionLoadingId, setActionLoadingId] = useState(null);
  const [proposeModalOpen, setProposeModalOpen] = useState(false);

  // Sync propSender when effectiveOwner updates
  useEffect(() => {
    if (effectiveOwner) {
      setPropSender(effectiveOwner);
      setSelectedOwner(effectiveOwner);
      if (propTarget === effectiveOwner) {
        setPropTarget(DRAFT_OWNERS.find(o => o !== effectiveOwner) || 'Adrian');
      }
    }
  }, [effectiveOwner, propTarget]);

  const loadData = React.useCallback(async () => {
    setLoading(true);
    try {
      const [tradesRes, compRes, keepersRes, proposalsRes] = await Promise.all([
        supabase.from('draft_asset_trades').select('*').order('trade_id', { ascending: true }),
        supabase.from('draft_compensation_picks').select('*').order('round_num', { ascending: true }),
        supabase.from('draft_keepers').select('*').order('team_id', { ascending: true }),
        supabase.from('league_trade_proposals').select('*').order('created_at', { ascending: false })
      ]);

      if (tradesRes.data?.length > 0) setDraftTrades(tradesRes.data);
      if (compRes.data?.length > 0) setCompPicks(compRes.data);
      if (keepersRes.data?.length > 0) setKeepers(keepersRes.data);
      if (proposalsRes.data) setProposals(proposalsRes.data);
    } catch (err) {
      console.warn('Using local fallback for draft assets:', err);
    } finally {
      setLoading(false);
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

  const pendingCount = useMemo(() => {
    return proposals.filter(p => p.status === 'pending' || p.status === 'accepted_by_partner').length;
  }, [proposals]);

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

    const hasOffered = offeredPickRound || (offeredBudget && parseFloat(offeredBudget) > 0);
    const hasRequested = requestedPickRound || (requestedBudget && parseFloat(requestedBudget) > 0);

    if (!hasOffered || !hasRequested) {
      alert('A trade proposal must include at least one offered asset and one requested asset.');
      return;
    }

    setSubmittingTrade(true);
    try {
      const offeredAssets = [];
      if (offeredPickRound) {
        const r = parseInt(offeredPickRound);
        const p = senderAvailablePicks.find(item => item.round === r);
        offeredAssets.push({
          type: 'pick',
          round: r,
          original_owner: p?.originalOwner || propSender,
          label: `Round ${r} Draft Pick (Orig: ${p?.originalOwner || propSender})`
        });
      }
      if (offeredBudget && parseFloat(offeredBudget) > 0) {
        offeredAssets.push({
          type: 'budget',
          amount: parseFloat(offeredBudget),
          label: `$${offeredBudget} Draft Budget`
        });
      }

      const requestedAssets = [];
      if (requestedPickRound) {
        const r = parseInt(requestedPickRound);
        const p = targetAvailablePicks.find(item => item.round === r);
        requestedAssets.push({
          type: 'pick',
          round: r,
          original_owner: p?.originalOwner || propTarget,
          label: `Round ${r} Draft Pick (Orig: ${p?.originalOwner || propTarget})`
        });
      }
      if (requestedBudget && parseFloat(requestedBudget) > 0) {
        requestedAssets.push({
          type: 'budget',
          amount: parseFloat(requestedBudget),
          label: `$${requestedBudget} Draft Budget`
        });
      }

      const senderTeamId = LEAGUE_OWNERS.find(o => o.name.toLowerCase() === propSender.toLowerCase())?.id || 0;
      const targetTeamId = LEAGUE_OWNERS.find(o => o.name.toLowerCase() === propTarget.toLowerCase())?.id || 0;

      const { error } = await supabase
        .from('league_trade_proposals')
        .insert({
          season_year: 2026,
          proposing_team_id: senderTeamId,
          proposing_owner: propSender,
          target_team_id: targetTeamId,
          target_owner: propTarget,
          offered_assets: offeredAssets,
          requested_assets: requestedAssets,
          notes: tradeNotes || null,
          status: 'pending',
          proposed_at: new Date().toISOString()
        });

      if (error) throw error;

      alert(`Official trade proposal sent to ${propTarget}! 🤝`);
      setOfferedPickRound('');
      setOfferedBudget('');
      setRequestedPickRound('');
      setRequestedBudget('');
      setTradeNotes('');
      await loadData();
    } catch (err) {
      console.error('Failed to submit proposal:', err);
      alert('Error submitting proposal: ' + err.message);
    } finally {
      setSubmittingTrade(false);
    }
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

      alert(`Trade accepted! 🎉 It has been sent to Commissioners (Dan & Adrian) for final league approval.`);
      await loadData();
    } catch (err) {
      console.error('Accept failed:', err);
      alert('Failed to accept proposal: ' + err.message);
    } finally {
      setActionLoadingId(null);
    }
  };

  // Commissioner Approval: Dan or Adrian officially executes the trade into draft_asset_trades!
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
            from_team_id: proposal.proposing_team_id,
            receiving_owner: proposal.target_owner,
            to_team_id: proposal.target_team_id,
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
            from_team_id: proposal.proposing_team_id,
            receiving_owner: proposal.target_owner,
            to_team_id: proposal.target_team_id,
            asset_type: 'Budget',
            asset_name: `$${asset.amount} Budget`,
            round_num: null,
            original_owner: proposal.proposing_owner,
            notes: proposal.notes || `Budget transfer`
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
            from_team_id: proposal.target_team_id,
            receiving_owner: proposal.proposing_owner,
            to_team_id: proposal.proposing_team_id,
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
            from_team_id: proposal.target_team_id,
            receiving_owner: proposal.proposing_owner,
            to_team_id: proposal.proposing_team_id,
            asset_type: 'Budget',
            asset_name: `$${asset.amount} Budget`,
            round_num: null,
            original_owner: proposal.target_owner,
            notes: proposal.notes || `Budget transfer`
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

      alert(`Trade officially APPROVED and EXECUTED! 👑 The 2027 draft board and team ledgers have been updated.`);
      await loadData();
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

      alert(`Trade proposal ${newStatus}.`);
      await loadData();
    } catch (err) {
      console.error('Update failed:', err);
      alert('Error: ' + err.message);
    } finally {
      setActionLoadingId(null);
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

  // Distinct trades involving draft assets
  const assetTradesList = useMemo(() => {
    return (draftTrades || []).filter(t =>
      t.asset_type === 'Overall Pick' || t.asset_type === 'Draft Pick' || t.asset_type === 'Budget'
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
              onClick={() => setActiveSubTab('board')}
              className={`px-3.5 py-2 rounded-lg transition-colors cursor-pointer ${
                activeSubTab === 'board'
                  ? 'bg-teal-500 text-slate-950 shadow-md font-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              📊 2027 Traded Board
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
              onClick={() => setActiveSubTab('2026board')}
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
              {pendingCount > 0 && (
                <span className="px-1.5 py-0.2 rounded-full text-[10px] font-black bg-amber-400 text-slate-950 animate-pulse">
                  {pendingCount}
                </span>
              )}
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
                      const isTraded = pick?.isTraded;
                      const currentOwner = pick?.currentOwner || slotOwner;

                      return (
                        <td key={slotOwner} className="py-2 px-2 text-center">
                          {isTraded ? (
                            <div
                              title={`Acquired by ${currentOwner} from ${slotOwner} (Trade #${pick?.tradeDetails?.trade_id || ''})`}
                              className="bg-amber-500/15 border border-amber-500/40 text-amber-300 rounded-lg p-1.5 transition-all shadow-xs"
                            >
                              <div className="font-black text-xs text-amber-200 flex items-center justify-center gap-1">
                                <span>🎟️</span>
                                <span>{currentOwner}</span>
                              </div>
                              <div className="text-[9px] text-amber-400/80 font-bold tracking-tight">
                                ex-{slotOwner} #{pick?.tradeDetails?.trade_id}
                              </div>
                            </div>
                          ) : (
                            <div className="bg-slate-950/70 border border-slate-800/80 text-slate-400 rounded-lg p-1.5">
                              <span className="font-semibold text-slate-400">{currentOwner}</span>
                            </div>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="bg-slate-950 border-t-2 border-slate-800 font-black text-xs">
                  <td className="py-3 px-3 text-center text-slate-400">Total</td>
                  {DRAFT_OWNERS.map(owner => {
                    const count = ownerStats[owner]?.total || 27;
                    const diff = ownerStats[owner]?.diff || 0;
                    return (
                      <td key={owner} className="py-3 px-3 text-center">
                        <span className="text-white font-black">{count}</span>{' '}
                        <span className={`text-[10px] ${diff > 0 ? 'text-emerald-400' : diff < 0 ? 'text-rose-400' : 'text-slate-500'}`}>
                          ({diff > 0 ? `+${diff}` : diff})
                        </span>
                      </td>
                    );
                  })}
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      )}

      {/* SUBTAB 2: OWNER PICK LEDGERS */}
      {activeSubTab === 'ledgers' && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-6">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <span className="text-xs font-black uppercase tracking-wider text-slate-400">Select Manager:</span>
              <select
                value={selectedOwner}
                onChange={e => setSelectedOwner(e.target.value)}
                className="bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-sm font-bold text-white focus:outline-none focus:border-amber-500 cursor-pointer"
              >
                {DRAFT_OWNERS.map(o => (
                  <option key={o} value={o}>
                    {o} ({ownerStats[o]?.total || 27} picks • {ownerStats[o]?.diff >= 0 ? `+${ownerStats[o]?.diff}` : ownerStats[o]?.diff})
                  </option>
                ))}
              </select>
            </div>

            <div className="text-xs text-amber-400 font-bold bg-amber-500/10 border border-amber-500/20 px-3 py-1.5 rounded-lg">
              {selectedOwner} holds <strong className="text-white">{ownerStats[selectedOwner]?.total}</strong> total picks for 2027
            </div>
          </div>

          {/* Currently Owned Picks */}
          <div>
            <div className="text-xs font-black uppercase tracking-wider text-slate-400 mb-2">
              Currently Owned 2027 Picks ({ownerStats[selectedOwner]?.ownedPicks?.length || 0})
            </div>
            <div className="overflow-x-auto rounded-xl border border-slate-800">
              <table className="w-full text-left text-xs border-collapse">
                <thead>
                  <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                    <th className="py-3 px-4 w-28">Round</th>
                    <th className="py-3 px-4 w-32">Status</th>
                    <th className="py-3 px-4 w-36">Original Owner</th>
                    <th className="py-3 px-4">Trade Details & Rationale</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-medium text-slate-200">
                  {ownerStats[selectedOwner]?.ownedPicks.map((pick, idx) => {
                    const isAcquired = pick.originalOwner !== selectedOwner;
                    return (
                      <tr key={`ledger-${selectedOwner}-${idx}`} className="hover:bg-slate-800/40 transition-colors">
                        <td className="py-2.5 px-4 font-black">
                          <span className={isAcquired ? 'text-amber-400' : 'text-teal-400'}>
                            Round {pick.round}
                          </span>
                        </td>
                        <td className="py-2.5 px-4">
                          <span
                            className={`px-2 py-0.5 rounded text-[10px] font-black uppercase ${
                              isAcquired
                                ? 'bg-amber-500/20 text-amber-300 border border-amber-500/40'
                                : 'bg-teal-500/20 text-teal-300 border border-teal-500/40'
                            }`}
                          >
                            {isAcquired ? '✓ ACQUIRED' : 'ORIGINAL'}
                          </span>
                        </td>
                        <td className="py-2.5 px-4 font-bold text-white">
                          {pick.originalOwner}
                        </td>
                        <td className="py-2.5 px-4 text-slate-300">
                          {isAcquired ? (
                            <span className="text-amber-300/90">
                              Acquired from <strong>{pick.originalOwner}</strong> via Trade #{pick.tradeDetails?.trade_id} ({pick.tradeDetails?.trade_date})
                              {pick.tradeDetails?.notes && ` • ${pick.tradeDetails.notes}`}
                            </span>
                          ) : (
                            <span className="text-slate-500">Original slot selection</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Traded Away Picks */}
          {ownerStats[selectedOwner]?.tradedAway.length > 0 && (
            <div>
              <div className="text-xs font-black uppercase tracking-wider text-rose-400 mb-2">
                Original Picks Traded Away by {selectedOwner} ({ownerStats[selectedOwner]?.tradedAway.length})
              </div>
              <div className="overflow-x-auto rounded-xl border border-rose-900/40">
                <table className="w-full text-left text-xs border-collapse">
                  <thead>
                    <tr className="bg-slate-950 text-slate-400 border-b border-rose-900/40 uppercase tracking-wider font-black">
                      <th className="py-3 px-4 w-28">Round</th>
                      <th className="py-3 px-4 w-32">Status</th>
                      <th className="py-3 px-4 w-36">Current Owner</th>
                      <th className="py-3 px-4">Trade Details</th>
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
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-black uppercase tracking-wider text-slate-300">
              Draft Asset Trades ({assetTradesList.length} Transactions)
            </h2>
            <span className="text-xs text-slate-500">
              Source: Google Sheets Trade Log synced to Supabase
            </span>
          </div>

          <div className="overflow-x-auto rounded-xl border border-slate-800">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-slate-950 text-slate-400 border-b border-slate-800 uppercase tracking-wider font-black">
                  <th className="py-3 px-4 w-28">Date</th>
                  <th className="py-3 px-3 text-center w-20">Trade #</th>
                  <th className="py-3 px-4 w-32">Sending Owner</th>
                  <th className="py-3 px-4 w-32">Receiving Owner</th>
                  <th className="py-3 px-4 w-52">Draft Asset Traded</th>
                  <th className="py-3 px-4">Notes & Associated Player Moves</th>
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
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-500/15 border border-amber-500/40 text-amber-300 font-bold text-xs">
                        <span>🎟️</span>
                        <span>{t.asset_name}</span>
                        {t.round_num && <span className="text-[10px] text-amber-400/80">(R{t.round_num})</span>}
                      </span>
                    </td>
                    <td className="py-3 px-4 text-slate-300">
                      {t.notes || `Trade #${t.trade_id}`}
                    </td>
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
                      cp => cp.round_num === rNum && cp.action_type === 'COMP_BOUGHT'
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
                              k => k.owner === owner && k.keeper_slot === rNum
                            );
                            return (
                              <td key={owner} className="py-2 px-2 text-center">
                                {keeperObj ? (
                                  <div className="bg-amber-500/15 border border-amber-500/30 rounded-lg p-1 text-center">
                                    <div className="font-bold text-[11px] text-amber-200 truncate">
                                      {keeperObj.player_name}
                                    </div>
                                    <div className="text-[9px] text-amber-400/90 font-semibold">
                                      ${keeperObj.cost} • #{keeperObj.rank}
                                    </div>
                                  </div>
                                ) : (
                                  <span className="text-slate-600">-</span>
                                )}
                              </td>
                            );
                          }

                          const isOffset = (compPicks || []).some(
                            cp => cp.owner === owner && cp.round_num === rNum && cp.action_type === 'OFFSET_LOST'
                          );

                          return (
                            <td key={owner} className="py-2 px-2 text-center">
                              {isOffset ? (
                                <div className="bg-rose-500/15 border border-rose-500/40 rounded-lg p-1 text-center">
                                  <div className="text-[10px] font-bold text-rose-400 line-through">Pick Slot</div>
                                  <div className="text-[9px] text-rose-300 font-black">🚫 Offset</div>
                                </div>
                              ) : (
                                <div className="bg-slate-950/70 border border-slate-800 rounded-lg p-1 text-center">
                                  <span className="text-[11px] text-slate-400">Standard</span>
                                </div>
                              )}
                            </td>
                          );
                        })}

                        {/* End of round compensation picks */}
                        <td className="py-2 px-2 text-center">
                          {compPicksThisRound.length > 0 ? (
                            <div className="flex flex-col gap-1">
                              {compPicksThisRound.map(cp => (
                                <span
                                  key={cp.id || `${cp.owner}-${cp.round_num}`}
                                  className="px-1.5 py-0.5 rounded bg-purple-500/20 border border-purple-500/40 text-purple-300 font-bold text-[10px]"
                                >
                                  🎟️ {cp.owner} (${cp.cost_or_income})
                                </span>
                              ))}
                            </div>
                          ) : (
                            <span className="text-slate-600 text-[11px]">-</span>
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
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div>
              <h2 className="text-lg font-black text-white flex items-center gap-2">
                <span>🤝</span> 2027 Offseason Draft Asset & Budget Trading Hub
              </h2>
              <p className="text-xs text-slate-400 mt-1 max-w-2xl leading-relaxed">
                Propose pick swaps and budget transfers between league managers. Once both parties agree, trades enter the queue for <strong>Commissioner Approval (Dan & Adrian)</strong> before final execution into the 2027 draft board.
              </p>
            </div>

            <button
              onClick={() => setProposeModalOpen(!proposeModalOpen)}
              className="px-4 py-2.5 rounded-xl bg-gradient-to-r from-rose-600 to-pink-600 hover:from-rose-500 hover:to-pink-500 text-white font-bold text-xs shadow-lg transition-all flex items-center gap-2 cursor-pointer w-fit"
            >
              <span>{proposeModalOpen ? '✕ Close Proposal Form' : '➕ Propose New Trade'}</span>
            </button>
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
                    👑 Commissioner Override: Any Manager
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
                        onChange={e => setPropSender(e.target.value)}
                        className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-white focus:ring-2 focus:ring-rose-500 cursor-pointer"
                      >
                        {DRAFT_OWNERS.map(o => (
                          <option key={o} value={o}>{o} (Team {LEAGUE_OWNERS.find(lo => lo.name === o)?.id})</option>
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
                      onChange={e => setPropTarget(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-bold text-white focus:ring-2 focus:ring-rose-500 cursor-pointer"
                    >
                      {DRAFT_OWNERS.filter(o => o !== propSender).map(o => (
                        <option key={o} value={o}>{o} (Team {LEAGUE_OWNERS.find(lo => lo.name === o)?.id})</option>
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
                      <span className="text-[10px] text-slate-400 lowercase">{senderAvailablePicks.length} picks owned</span>
                    </div>

                    <div>
                      <label className="block text-[11px] font-bold text-slate-400 mb-1">
                        Select 2027 Draft Pick to Send:
                      </label>
                      <select
                        value={offeredPickRound}
                        onChange={e => setOfferedPickRound(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                      >
                        <option value="">-- No pick selected --</option>
                        {senderAvailablePicks.map(p => (
                          <option key={p.round} value={p.round}>
                            Round {p.round} Pick {p.originalOwner !== propSender ? `(Orig: ${p.originalOwner})` : ''}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className="block text-[11px] font-bold text-slate-400 mb-1">
                        And / Or Draft Budget Cash ($):
                      </label>
                      <input
                        type="number"
                        min="0"
                        max="100"
                        placeholder="$0"
                        value={offeredBudget}
                        onChange={e => setOfferedBudget(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                      />
                    </div>
                  </div>

                  {/* Right: What Proposer Requests */}
                  <div className="space-y-4">
                    <div className="text-xs font-black uppercase tracking-wider text-teal-400 border-b border-slate-800 pb-2 flex items-center justify-between">
                      <span>📥 {propSender} Receives (from {propTarget}):</span>
                      <span className="text-[10px] text-slate-400 lowercase">{targetAvailablePicks.length} picks owned</span>
                    </div>

                    <div>
                      <label className="block text-[11px] font-bold text-slate-400 mb-1">
                        Select 2027 Draft Pick to Request:
                      </label>
                      <select
                        value={requestedPickRound}
                        onChange={e => setRequestedPickRound(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                      >
                        <option value="">-- No pick selected --</option>
                        {targetAvailablePicks.map(p => (
                          <option key={p.round} value={p.round}>
                            Round {p.round} Pick {p.originalOwner !== propTarget ? `(Orig: ${p.originalOwner})` : ''}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className="block text-[11px] font-bold text-slate-400 mb-1">
                        And / Or Draft Budget Cash ($):
                      </label>
                      <input
                        type="number"
                        min="0"
                        max="100"
                        placeholder="$0"
                        value={requestedBudget}
                        onChange={e => setRequestedBudget(e.target.value)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white"
                      />
                    </div>
                  </div>
                </div>

                {/* Notes */}
                <div>
                  <label className="block text-xs font-bold text-slate-300 mb-1">
                    Trade Rationale & Conditions (Optional):
                  </label>
                  <input
                    type="text"
                    placeholder="e.g. Conditional swap or draft order swap..."
                    value={tradeNotes}
                    onChange={e => setTradeNotes(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white placeholder-slate-600"
                  />
                </div>

                <div className="flex justify-end gap-3 pt-2">
                  <button
                    type="button"
                    onClick={() => setProposeModalOpen(false)}
                    className="px-4 py-2 rounded-xl text-xs font-bold text-slate-400 hover:text-white"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={submittingTrade || !user}
                    className="px-5 py-2 rounded-xl bg-rose-600 hover:bg-rose-500 text-white font-bold text-xs shadow-md cursor-pointer disabled:opacity-50"
                  >
                    {submittingTrade ? 'Submitting...' : '📤 Send Official Trade Proposal'}
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* Active Proposals Board */}
          <div className="space-y-4">
            <h3 className="text-sm font-black uppercase tracking-wider text-slate-300 flex items-center gap-2">
              <span>⏳</span> Pending Trade Proposals & Approvals
            </h3>

            {proposals.filter(p => p.status === 'pending' || p.status === 'accepted_by_partner').length === 0 ? (
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-8 text-center text-slate-500 text-xs">
                No active proposals pending agreement or commissioner review right now.
              </div>
            ) : (
              <div className="grid grid-cols-1 gap-4">
                {proposals
                  .filter(p => p.status === 'pending' || p.status === 'accepted_by_partner')
                  .map(prop => {
                    const isSender = profile?.owner_name?.toLowerCase() === prop.proposing_owner?.toLowerCase();
                    const isTarget = profile?.owner_name?.toLowerCase() === prop.target_owner?.toLowerCase();
                    const isAwaitingCommish = prop.status === 'accepted_by_partner';
                    const isLoading = actionLoadingId === prop.id;

                    return (
                      <div
                        key={prop.id}
                        className={`rounded-2xl p-5 border shadow-xl transition-all ${
                          isAwaitingCommish
                            ? 'bg-amber-950/20 border-amber-500/50'
                            : 'bg-slate-900 border-slate-800'
                        }`}
                      >
                        {/* Header */}
                        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-3">
                          <div className="flex items-center gap-2">
                            <span className="text-lg">🤝</span>
                            <div>
                              <div className="text-sm font-bold text-white">
                                {prop.proposing_owner} ⇄ {prop.target_owner}
                              </div>
                              <div className="text-[10px] text-slate-400">
                                Proposed: {new Date(prop.proposed_at || prop.created_at).toLocaleDateString()}
                              </div>
                            </div>
                          </div>

                          <div>
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
                            <div className="text-[11px] font-bold text-slate-400 mb-2">
                              {prop.proposing_owner} Sends:
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                              {(prop.offered_assets || []).map((a, i) => (
                                <span
                                  key={i}
                                  className="px-2 py-1 rounded-lg text-xs font-bold bg-rose-500/20 border border-rose-500/40 text-rose-300"
                                >
                                  {a.type === 'pick' ? `🎟️ ${a.label}` : `💵 ${a.label}`}
                                </span>
                              ))}
                            </div>
                          </div>

                          {/* Right: What Target Owner Sends */}
                          <div className="bg-slate-950/60 p-3 rounded-xl border border-slate-800/80">
                            <div className="text-[11px] font-bold text-slate-400 mb-2">
                              {prop.target_owner} Sends:
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                              {(prop.requested_assets || []).map((a, i) => (
                                <span
                                  key={i}
                                  className="px-2 py-1 rounded-lg text-xs font-bold bg-teal-500/20 border border-teal-500/40 text-teal-300"
                                >
                                  {a.type === 'pick' ? `🎟️ ${a.label}` : `💵 ${a.label}`}
                                </span>
                              ))}
                            </div>
                          </div>
                        </div>

                        {prop.notes && (
                          <div className="text-xs text-slate-400 italic mb-3">
                            "{prop.notes}"
                          </div>
                        )}

                        {/* Action Buttons */}
                        <div className="pt-2 border-t border-slate-800 flex flex-wrap items-center justify-between gap-3">
                          <div className="text-[11px] text-slate-500">
                            {isAwaitingCommish
                              ? `Agreed by ${prop.target_owner}. Dan or Adrian must officially approve.`
                              : `Awaiting response from ${prop.target_owner}.`}
                          </div>

                          <div className="flex items-center gap-2">
                            {/* Stage 1 Actions */}
                            {!isAwaitingCommish && (
                              <>
                                {(isTarget || isCommissioner) && (
                                  <>
                                    <button
                                      onClick={() => handlePartnerAccept(prop)}
                                      disabled={isLoading}
                                      className="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs shadow-xs cursor-pointer"
                                    >
                                      {isLoading ? 'Processing...' : '✅ Accept Trade'}
                                    </button>
                                    <button
                                      onClick={() => handleDeclineOrCancel(prop, 'declined')}
                                      disabled={isLoading}
                                      className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-rose-950 text-rose-400 border border-rose-500/40 font-bold text-xs cursor-pointer"
                                    >
                                      Decline
                                    </button>
                                  </>
                                )}

                                {(isSender || isCommissioner) && !isTarget && (
                                  <button
                                    onClick={() => handleDeclineOrCancel(prop, 'cancelled')}
                                    disabled={isLoading}
                                    className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-400 font-bold text-xs cursor-pointer"
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
                                  <div className="flex items-center gap-2 bg-amber-950/40 border border-amber-500/50 p-1.5 px-3 rounded-xl">
                                    <span className="text-xs font-bold text-amber-300">👑 Commish Action:</span>
                                    <button
                                      onClick={() => handleCommissionerApprove(prop)}
                                      disabled={isLoading}
                                      className="px-3 py-1.5 rounded-lg bg-gradient-to-r from-amber-500 to-yellow-500 hover:from-amber-400 hover:to-yellow-400 text-slate-950 font-black text-xs shadow-md cursor-pointer"
                                    >
                                      {isLoading ? 'Executing...' : '✅ Approve & Execute Trade'}
                                    </button>
                                    <button
                                      onClick={() => handleDeclineOrCancel(prop, 'declined')}
                                      disabled={isLoading}
                                      className="px-3 py-1.5 rounded-lg bg-rose-950/80 text-rose-300 border border-rose-500/50 font-bold text-xs hover:bg-rose-900 cursor-pointer"
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
                          </div>
                        </div>
                      </div>
                    );
                  })}
              </div>
            )}
          </div>

          {/* Historical Proposals Table */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl space-y-3">
            <h3 className="text-sm font-black uppercase tracking-wider text-slate-300 flex items-center gap-2">
              <span>📜</span> Completed Trade Proposals Archive
            </h3>

            {proposals.filter(p => p.status === 'approved' || p.status === 'declined' || p.status === 'cancelled').length === 0 ? (
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
                    </tr>
                  </thead>
                  <tbody>
                    {proposals
                      .filter(p => p.status === 'approved' || p.status === 'declined' || p.status === 'cancelled')
                      .map(p => (
                        <tr key={p.id} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                          <td className="py-2.5 px-3 text-slate-400 font-mono text-[11px]">
                            {new Date(p.proposed_at || p.created_at).toLocaleDateString()}
                          </td>
                          <td className="py-2.5 px-3 font-bold text-white">{p.proposing_owner}</td>
                          <td className="py-2.5 px-3 font-bold text-white">{p.target_owner}</td>
                          <td className="py-2.5 px-3 text-slate-300">
                            {(p.offered_assets || []).map(a => a.label).join(', ')} ⇄ {(p.requested_assets || []).map(a => a.label).join(', ')}
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
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
