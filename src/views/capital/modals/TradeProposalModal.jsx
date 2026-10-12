import React from 'react';

export default function TradeProposalModal({
  isOpen,
  onClose,
  user,
  isCommissioner,
  isAdrian,
  propSender,
  handlePropSenderChange,
  propTarget,
  handlePropTargetChange,
  DRAFT_OWNERS,
  getTeamId,
  senderAvailablePicks,
  targetAvailablePicks,
  teamRosters,
  offeredAssets,
  requestedAssets,
  selectedOfferedPickRound,
  setSelectedOfferedPickRound,
  handleAddOfferedPick,
  handleRemoveOfferedAsset,
  selectedOfferedPlayerId,
  setSelectedOfferedPlayerId,
  handleAddOfferedPlayer,
  selectedOfferedBudget,
  setSelectedOfferedBudget,
  handleAddOfferedBudget,
  selectedRequestedPickRound,
  setSelectedRequestedPickRound,
  handleAddRequestedPick,
  handleRemoveRequestedAsset,
  selectedRequestedPlayerId,
  setSelectedRequestedPlayerId,
  handleAddRequestedPlayer,
  selectedRequestedBudget,
  setSelectedRequestedBudget,
  handleAddRequestedBudget,
  hasFormPickDisparity,
  builderOfferedPicksCount,
  builderRequestedPicksCount,
  tradeNotes,
  setTradeNotes,
  handleSendProposal,
  submittingTrade,
  renderAssetBadge,
  isPickInAssets,
  isPlayerInAssets
}) {
  if (!isOpen) return null;

  return (
    <div className="bg-slate-900/95 border border-rose-500/40 rounded-2xl p-6 shadow-2xl space-y-6 animate-fade-in-up">
      <div className="border-b border-slate-800 pb-3 flex items-center justify-between">
        <h3 className="text-base font-bold text-white flex items-center gap-2">
          <span>📝</span> Submit New Trade Proposal
        </h3>
        {isCommissioner && (
          <span className="text-[10px] font-bold px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40">
            {isAdrian ? '👑 Commissioner Override: Send on behalf of any team' : '⚡ Admin Override: Send on behalf of any team'}
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
                Add Draft Budget Cash to Offer ($):
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
              <span>📥 {propSender} Requests from {propTarget}:</span>
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
                In our 26-round draft format (Rounds 7 through 32), trading an unequal number of draft picks leaves teams with unequal roster sizes unless an offsetting pick (such as a late-round Round 32 pick swap) is included. You may still proceed if this disparity is intentional.
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
            onClick={onClose}
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
  );
}
