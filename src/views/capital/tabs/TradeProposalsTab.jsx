import React from 'react';

export default function TradeProposalsTab({
  viewPerspectiveOwner,
  setViewPerspectiveOwner,
  setPropSender,
  DRAFT_OWNERS,
  proposeModalOpen,
  setProposeModalOpen,
  inboxTab,
  setInboxTab,
  incomingProposals,
  outgoingProposals,
  commishQueueProposals,
  leagueAcceptedTrades,
  archivedProposals,
  profile,
  isCommissioner,
  actionLoadingId,
  renderAssetBadge,
  handlePartnerAccept,
  handleCounterOffer,
  handleDeclineOrCancel,
  handleCommissionerApprove,
  handleOpenEditModal
}) {
  return (
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
            Propose trades consisting of <strong>2027 draft picks</strong>, <strong>active rostered players</strong>, and <strong>draft budget cash</strong>. Offers wait in the partner's inbox for agreement before routing to <strong>Commissioner (Adrian)</strong> or <strong>Admin (Dan)</strong> for final league execution.
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
    </div>
  );
}
