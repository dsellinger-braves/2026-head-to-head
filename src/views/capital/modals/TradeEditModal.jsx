import React from 'react';

export default function TradeEditModal({
  editingProposal,
  setEditingProposal,
  DRAFT_OWNERS,
  getTeamId,
  renderAssetBadge,
  editOfferedPickRound,
  setEditOfferedPickRound,
  editSenderAvailablePicks,
  isPickInAssets,
  editOfferedPlayerId,
  setEditOfferedPlayerId,
  teamRosters,
  isPlayerInAssets,
  editOfferedBudget,
  setEditOfferedBudget,
  editRequestedPickRound,
  setEditRequestedPickRound,
  editTargetAvailablePicks,
  editRequestedPlayerId,
  setEditRequestedPlayerId,
  editRequestedBudget,
  setEditRequestedBudget,
  handleSaveProposalEdit,
  savingEdit
}) {
  if (!editingProposal) return null;

  return (
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
                  <strong>Pick Disparity:</strong> This trade involves {oPicks} offered pick(s) vs {rPicks} requested pick(s). Unequal picks will cause uneven roster sizes at the 26-round draft (Rounds 7–32).
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
  );
}
