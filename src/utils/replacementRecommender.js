// src/utils/replacementRecommender.js

/**
 * Replacement Recommender Engine for Slumping / Cold Players
 * 
 * Provides context-aware waiver wire / free agent replacement targets:
 * 1. Positional Match (direct eligibility for lineup slot)
 * 2. Capital Invested Protection (draft round & keeper cost check to prevent dropping elite stars)
 * 3. Standings & Category Weakness Synergy (boosts players addressing team category deficits)
 */

import keeperData2026 from '../data/keeperInput2026.json' with { type: 'json' };

// Map eligible slots to readable positions
const SLOT_TO_POS = {
  0: 'C',
  1: '1B',
  2: '2B',
  3: '3B',
  4: 'SS',
  5: 'OF',
  11: 'DH',
  12: 'UTIL',
  13: 'P',
  14: 'SP',
  15: 'RP'
};

/**
 * Evaluate the draft & keeper capital invested in a player
 * @param {Object} player - The cold player object { id, name }
 * @param {Array} draftPicks - ESPN 2026 draft detail picks array
 * @returns {Object} Capital evaluation
 */
export function evaluatePlayerCapital(player, draftPicks = []) {
  if (!player) {
    return { tier: 'LOW', badgeText: 'CUTTABLE', color: 'rose', advice: 'Safe to stream or drop.' };
  }

  const pId = parseInt(player.id, 10);
  const pName = (player.name || '').toLowerCase().trim();

  // 1. Check Keeper Input Data
  const keepers = keeperData2026?.keepers || [];
  const keeperMatch = keepers.find(k => {
    return (k.espn_player_id && parseInt(k.espn_player_id, 10) === pId) ||
           (k.player_name && k.player_name.toLowerCase().trim() === pName);
  });

  if (keeperMatch) {
    const cost = parseFloat(keeperMatch.cost) || 0;
    const rank = keeperMatch.rank || 999;
    if (cost >= 25 || rank <= 30) {
      return {
        tier: 'HIGH',
        keeperCost: cost,
        keeperRank: rank,
        action: 'HOLD_BENCH',
        badgeText: '🛡️ HIGH CAPITAL · DO NOT DROP',
        color: 'purple',
        headline: 'Elite Franchise Asset',
        advice: `Drafted as keeper ($${cost}, #${rank} rank). This is a normal cold slump—do NOT drop! Bench or hold.`
      };
    } else if (cost >= 10 || rank <= 80) {
      return {
        tier: 'MODERATE',
        keeperCost: cost,
        keeperRank: rank,
        action: 'BENCH_PREFERRED',
        badgeText: '⚠️ MODERATE CAPITAL · BENCH PREFERRED',
        color: 'cyan',
        headline: 'Keeper Asset',
        advice: `Acquired for $${cost} keeper budget. Bench for a hot streamer rather than outright cutting.`
      };
    }
  }

  // 2. Check 2026 Draft Detail Picks
  if (draftPicks && draftPicks.length > 0) {
    const pickMatch = draftPicks.find(p => p.playerId === pId);
    if (pickMatch) {
      const overall = pickMatch.overallPickNumber || 999;
      const round = pickMatch.roundId || Math.ceil(overall / 10);

      if (round <= 5 || overall <= 50) {
        return {
          tier: 'HIGH',
          draftRound: round,
          overallPick: overall,
          action: 'HOLD_BENCH',
          badgeText: '🛡️ HIGH DRAFT CAPITAL · DO NOT DROP',
          color: 'purple',
          headline: `Round ${round} Pick (#${overall} Overall)`,
          advice: `Selected in Round ${round} (#${overall} overall). High draft capital—bench for a streamer during slump; DO NOT drop!`
        };
      } else if (round <= 15 || overall <= 150) {
        return {
          tier: 'MODERATE',
          draftRound: round,
          overallPick: overall,
          action: 'BENCH_PREFERRED',
          badgeText: '⚠️ MODERATE CAPITAL · BENCH PREFERRED',
          color: 'cyan',
          headline: `Round ${round} Pick (#${overall} Overall)`,
          advice: `Selected in Round ${round} (#${overall} overall). Solid starter investment. Bench recommended over dropping.`
        };
      } else {
        return {
          tier: 'LOW',
          draftRound: round,
          overallPick: overall,
          action: 'DROP_CANDIDATE',
          badgeText: '✂️ LOW CAPITAL · STREAM / DROP CANDIDATE',
          color: 'rose',
          headline: `Round ${round} Pick (#${overall} Overall)`,
          advice: `Selected in Round ${round} (#${overall} overall). Low capital invested—safe to cut directly for a hot waiver streamer.`
        };
      }
    }
  }

  // 3. Fallback: Free Agent / Waiver Flyer
  return {
    tier: 'LOW',
    draftRound: null,
    overallPick: null,
    action: 'DROP_CANDIDATE',
    badgeText: '✂️ LOW CAPITAL · STREAM / DROP CANDIDATE',
    color: 'rose',
    headline: 'Waiver / Late Flyer',
    advice: 'Undrafted or late pickup. Safe to cut directly for high-momentum waiver wire replacement.'
  };
}

/**
 * Check if a candidate free agent is position-eligible for the cold player
 */
export function isPositionMatch(coldPlayer, candidate) {
  if (!coldPlayer || !candidate) return false;

  const coldPos = (coldPlayer.position || '').toUpperCase();
  const coldIsPitcher = coldPlayer.isPitcher || coldPos.includes('SP') || coldPos.includes('RP') || coldPos.includes('P');
  const candSlots = candidate.player?.eligibleSlots || candidate.slots || [];
  const candDefaultPos = candidate.player?.defaultPositionId || candidate.pos;

  const candIsPitcher = candDefaultPos === 1 || candDefaultPos === 11 || candSlots.includes(13) || candSlots.includes(14) || candSlots.includes(15);

  // Pitchers only replace pitchers, batters only replace batters
  if (coldIsPitcher !== candIsPitcher) return false;

  // For pitchers:
  if (coldIsPitcher) {
    if (coldPos.includes('SP') && (candSlots.includes(14) || candDefaultPos === 1)) return true;
    if (coldPos.includes('RP') && (candSlots.includes(15) || candDefaultPos === 11)) return true;
    return candSlots.includes(13); // Generic P
  }

  // For batters:
  const posKeys = ['C', '1B', '2B', '3B', 'SS', 'OF', 'DH'];
  for (const pk of posKeys) {
    if (coldPos.includes(pk)) {
      const targetSlot = pk === 'C' ? 0 : pk === '1B' ? 1 : pk === '2B' ? 2 : pk === '3B' ? 3 : pk === 'SS' ? 4 : pk === 'OF' ? 5 : 11;
      if (candSlots.includes(targetSlot)) return true;
    }
  }

  // UTIL match
  return candSlots.includes(12);
}

/**
 * Find top 2-3 recommended waiver wire replacements for a cold player
 * @param {Object} coldPlayer - Slumping player { id, name, position, isPitcher }
 * @param {Array} freeAgentPool - Raw players from ESPN free agents query
 * @param {Array} teamWeaknesses - Array of category keys where team is weak in standings (e.g. ['SB', 'HR'])
 * @param {Object} capitalInfo - Result of evaluatePlayerCapital
 * @returns {Array} List of top recommended replacement targets
 */
export function findWaiverReplacements(coldPlayer, freeAgentPool = [], teamWeaknesses = [], capitalInfo = null) {
  if (!coldPlayer || !freeAgentPool || freeAgentPool.length === 0) return [];

  const eligibleCandidates = [];

  freeAgentPool.forEach(entry => {
    const p = entry.player || entry;
    if (!p || !p.fullName) return;

    if (isPositionMatch(coldPlayer, entry)) {
      // 15-day official rating
      const pr15 = entry.ratings?.['2']?.totalRating || p.pr15 || 0;
      const pctOwned = p.ownership?.percentOwned || entry.owned || 0;

      // Check category synergy
      let synergyBonus = 0;
      let fitReason = 'Positional Match';

      const s15 = p.stats?.find(s => s.statSplitTypeId === 2)?.stats || {};

      if (!coldPlayer.isPitcher) {
        const sb = s15['23'] || 0;
        const hr = s15['5'] || 0;
        const rbi = s15['21'] || 0;

        if (teamWeaknesses.includes('SB') && sb >= 2) {
          synergyBonus += 3.5;
          fitReason = `Addresses Deficit: SB (${sb} SBs last 15d)`;
        } else if (teamWeaknesses.includes('HR') && hr >= 2) {
          synergyBonus += 3.0;
          fitReason = `Addresses Deficit: HR Power (${hr} HRs last 15d)`;
        } else if (teamWeaknesses.includes('RBI') && rbi >= 5) {
          synergyBonus += 2.5;
          fitReason = `Addresses Deficit: RBI (${rbi} RBIs last 15d)`;
        }
      } else {
        const sv = (s15['57'] || 0) + (s15['60'] || 0);
        const k = s15['48'] || 0;
        const qs = s15['63'] || 0;

        if (teamWeaknesses.includes('SV+HDs') && sv >= 2) {
          synergyBonus += 3.5;
          fitReason = `Addresses Deficit: SV+H (${sv} Saves/Holds last 15d)`;
        } else if (teamWeaknesses.includes('K') && k >= 10) {
          synergyBonus += 3.0;
          fitReason = `Addresses Deficit: Strikeouts (${k} Ks last 15d)`;
        } else if (teamWeaknesses.includes('QS') && qs >= 1) {
          synergyBonus += 3.0;
          fitReason = `Addresses Deficit: Quality Starts (${qs} QS)`;
        }
      }

      // Compute total recommendation score
      const recScore = (pr15 * 1.5) + synergyBonus + (pctOwned / 25);

      // Extract stats summary
      let statSummary = '';
      if (!coldPlayer.isPitcher) {
        const ab = s15['0'] || 0;
        const h = s15['1'] || 0;
        const hr = s15['5'] || 0;
        const sb = s15['23'] || 0;
        if (ab > 0) {
          const avg = (h / ab).toFixed(3).replace(/^0/, '');
          statSummary = `${avg} AVG · ${hr} HR · ${sb} SB (${ab} AB)`;
        } else {
          statSummary = `${pctOwned.toFixed(1)}% rostered in ESPN`;
        }
      } else {
        const ip = (s15['34'] || 0) / 3;
        const k = s15['48'] || 0;
        const er = s15['45'] || 0;
        if (ip > 0) {
          const era = ((er * 9) / ip).toFixed(2);
          statSummary = `${ip.toFixed(1)} IP · ${k} K · ${era} ERA`;
        } else {
          statSummary = `${pctOwned.toFixed(1)}% rostered in ESPN`;
        }
      }

      // Pos display
      const slots = p.eligibleSlots || [];
      const mappedPos = slots.filter(s => SLOT_TO_POS[s] && s !== 11 && s !== 12).map(s => SLOT_TO_POS[s]);
      const displayPos = mappedPos.length > 0 ? Array.from(new Set(mappedPos)).join(', ') : 'UTIL';

      eligibleCandidates.push({
        id: p.id,
        name: p.fullName,
        position: displayPos,
        proTeamId: p.proTeamId,
        pr15: parseFloat(pr15.toFixed(2)),
        pctOwned: parseFloat(pctOwned.toFixed(1)),
        statSummary,
        recScore: parseFloat(recScore.toFixed(2)),
        fitReason,
        _raw: p
      });
    }
  });

  // Sort descending by recommendation score
  eligibleCandidates.sort((a, b) => b.recScore - a.recScore);

  // Return top 2 candidates
  return eligibleCandidates.slice(0, 2).map(cand => {
    const isHold = capitalInfo?.action === 'HOLD_BENCH';
    const actionText = isHold
      ? `Bench ${coldPlayer.name}; Stream ${cand.name}`
      : `Drop ${coldPlayer.name} for ${cand.name}`;

    return {
      ...cand,
      actionText,
      actionType: isHold ? 'BENCH_STREAM' : 'CUT_ADD'
    };
  });
}
