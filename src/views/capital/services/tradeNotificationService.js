import { supabase } from '../../../supabaseClient';
import {
  DRAFT_OWNERS,
  LEAGUE_OWNERS,
  LEAGUE_OWNERS_NORMALIZED,
  getTeamId,
  normalizeOwner,
  isSameOwner
} from '../../../constants/league';

export {
  DRAFT_OWNERS,
  LEAGUE_OWNERS,
  LEAGUE_OWNERS_NORMALIZED,
  getTeamId,
  isSameOwner,
  normalizeOwner
};



export const canonicalOwnerName = (ownerName) => {
  if (!ownerName) return '';
  const key = String(ownerName).toLowerCase().trim();
  const match = LEAGUE_OWNERS_NORMALIZED[key];
  if (match) return match.name;
  return ownerName;
};

// Enqueue Discord DM notification for the 24/7 Railway bot worker
export async function enqueueTradeNotification({
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

export function isPickInAssets(pick, assets = []) {
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

export function isPlayerInAssets(player, assets = []) {
  if (!player || !assets || assets.length === 0) return false;
  const pId = Number(player.player_id);
  return assets.some(a => a.type === 'player' && Number(a.player_id) === pId);
}

export function compute2027DraftPicks(draftTrades = []) {
  const owners = [...DRAFT_OWNERS].sort();
  const picks = [];

  // Base draft order: 26 rounds (Rounds 7 through 32), 1 pick per owner per round (234 total picks)
  for (let round = 7; round <= 32; round++) {
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
    const sending = normalizeOwner(trade.sending_owner, 'draft');
    const receiving = normalizeOwner(trade.receiving_owner, 'draft');


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
