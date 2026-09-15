// src/utils/acquisition.js
import defaultTransactions2026 from '../data/transactions2026.json';
import defaultDraft2026 from '../data/draft2026.json';
import defaultKeepers2026 from '../data/keeperInput2026.json';
import defaultHistoricalTrades from '../data/historicalTrades.json';
import { TEAMS } from '../schedule';

/**
 * Returns the acquisition story for a given player on a specific team.
 * @param {number|string} playerId
 * @param {number|string} currentTeamId
 * @param {Array} [customTransactions=null] - optional override transactions
 * @param {Array} [customDraftPicks=null] - optional override draft picks
 */
export function getPlayerAcquisition(playerId, currentTeamId, customTransactions = null, customDraftPicks = null) {
  if (!playerId) return null;
  const numPid = parseInt(playerId, 10);
  const numTid = currentTeamId != null ? parseInt(currentTeamId, 10) : null;

  const transactions = customTransactions || defaultTransactions2026 || [];
  const draftPicks = customDraftPicks || defaultDraft2026 || [];
  const keepers = defaultKeepers2026?.keepers || [];
  const trades = defaultHistoricalTrades || [];

  // Filter transactions involving this player
  const playerTxs = transactions
    .filter(t => parseInt(t.player_id, 10) === numPid)
    .sort((a, b) => new Date(b.transaction_date || 0) - new Date(a.transaction_date || 0));

  // Check transactions specifically to this current team
  const toThisTeamTxs = playerTxs.filter(t => parseInt(t.to_team_id, 10) === numTid);

  // Check 2026 Draft pick
  const draftPick = draftPicks.find(p => parseInt(p.player_id, 10) === numPid);

  // Check 2026 Keeper
  const keeperRecord = keepers.find(k => parseInt(k.espn_player_id, 10) === numPid);

  // Determine latest transaction that brought the player to this team
  const latestMoveToTeam = toThisTeamTxs[0];

  let acqType = 'UNKNOWN';
  let badgeText = 'Rostered';
  let detailText = '';
  let date = null;
  let metadata = {};

  if (latestMoveToTeam) {
    date = latestMoveToTeam.transaction_date ? latestMoveToTeam.transaction_date.split('T')[0] : null;
    const tType = latestMoveToTeam.transaction_type;
    const rawType = latestMoveToTeam.raw_type;

    if (tType === 'TRADE' || rawType === 'TRADE') {
      acqType = 'TRADE';
      const fromTeamId = latestMoveToTeam.from_team_id;
      const fromTeamName = TEAMS[fromTeamId]?.name || `Team ${fromTeamId}`;
      badgeText = `Trade (${date || 'In-Season'})`;
      detailText = `Acquired via trade from ${fromTeamName} on ${date || 'In-Season'}`;

      const rootId = latestMoveToTeam.espn_transaction_id?.split('_')[0];
      const matchingTrade = trades.find(tr => tr.espn_root_id === rootId || (tr.trade_date === date && tr.participants?.includes(TEAMS[fromTeamId]?.name)));
      metadata = {
        fromTeamId,
        fromTeamName,
        tradeDeal: matchingTrade,
        rootId
      };
    } else if (tType === 'WAIVER_ADD' || rawType === 'WAIVER') {
      acqType = 'WAIVER';
      badgeText = `Waivers (${date})`;
      detailText = `Claimed off Waivers on ${date}`;
      metadata = { rawType: 'WAIVER' };
    } else if (tType === 'ADD' || rawType === 'FREEAGENT') {
      acqType = 'FREEAGENT';
      badgeText = `Free Agency (${date})`;
      detailText = `Added from Free Agency on ${date}`;
      metadata = { rawType: 'FREEAGENT' };
    }
  }

  // If no in-season move to this team was recorded, check if drafted or kept by this team
  if (acqType === 'UNKNOWN') {
    if (keeperRecord && (numTid == null || parseInt(keeperRecord.team_id, 10) === numTid)) {
      acqType = 'KEEPER';
      badgeText = `Keeper ($${keeperRecord.cost || 0})`;
      detailText = `Kept for $${keeperRecord.cost || 0} by ${TEAMS[keeperRecord.team_id]?.name || keeperRecord.owner} (Slot #${keeperRecord.keeper_slot || 1})`;
      metadata = { keeper: keeperRecord };
    } else if (draftPick && (numTid == null || parseInt(draftPick.team_id, 10) === numTid)) {
      if (draftPick.is_keeper) {
        acqType = 'KEEPER';
        badgeText = `Keeper (R${draftPick.round})`;
        detailText = `Kept in Round ${draftPick.round} (Pick #${draftPick.overall_pick}) by ${draftPick.team_owner}`;
      } else {
        acqType = 'DRAFT';
        badgeText = `Draft (R${draftPick.round}, #${draftPick.overall_pick})`;
        detailText = `Drafted in Round ${draftPick.round}, Pick ${draftPick.pick} (Overall #${draftPick.overall_pick}) by ${draftPick.team_owner}`;
      }
      metadata = { draft: draftPick };
    } else if (draftPick && numTid != null && parseInt(draftPick.team_id, 10) !== numTid) {
      const tradeTx = playerTxs.find(t => t.transaction_type === 'TRADE');
      if (tradeTx) {
        acqType = 'TRADE';
        const fromTeamId = tradeTx.from_team_id;
        date = tradeTx.transaction_date ? tradeTx.transaction_date.split('T')[0] : null;
        badgeText = `Trade (${date || 'In-Season'})`;
        detailText = `Acquired via trade from ${TEAMS[fromTeamId]?.name || `Team ${fromTeamId}`} (originally drafted by ${draftPick.team_owner})`;
        metadata = { fromTeamId, draft: draftPick };
      } else {
        acqType = 'ORIGINAL_ROSTER';
        badgeText = `Rostered (Opening Day)`;
        detailText = `On active roster since Opening Day`;
      }
    } else {
      acqType = 'ORIGINAL_ROSTER';
      badgeText = `Rostered`;
      detailText = `Opening day rostered player`;
    }
  }

  return {
    playerId: numPid,
    currentTeamId: numTid,
    type: acqType,
    badgeText,
    detailText,
    date,
    draftPick,
    keeperRecord,
    history: playerTxs,
    metadata
  };
}
