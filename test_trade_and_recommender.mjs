// test_trade_and_recommender.mjs
import assert from 'node:assert';
import historicalTrades from './src/data/historicalTrades.json' with { type: 'json' };
import { calculatePickValue, calculateBudgetValue, gradeTrade } from './src/utils/tradeGrading.js';
import { evaluatePlayerCapital, isPositionMatch, findWaiverReplacements } from './src/utils/replacementRecommender.js';

console.log('🧪 Starting Trade Repository & Recommender Test Suite...');

// --- 1. PICK CURVE & BUDGET TESTS ---
console.log('Testing Pick & Budget Math...');
assert.strictEqual(calculatePickValue(1), 100.0, 'Pick 1 should be 100.0');
const pick50 = calculatePickValue(50);
assert(pick50 > 45 && pick50 < 50, `Pick 50 should be ~47.7 (got ${pick50})`);
const pick100 = calculatePickValue(100);
assert(pick100 > 20 && pick100 < 25, `Pick 100 should be ~22.6 (got ${pick100})`);
const pick250 = calculatePickValue(250);
assert(pick250 > 1 && pick250 < 4, `Pick 250 should be ~2.4 (got ${pick250})`);

assert.strictEqual(calculateBudgetValue(10), 35.0, '$10 should be 35.0 pts');
assert.strictEqual(calculateBudgetValue(4), 14.0, '$4 should be 14.0 pts');
assert.strictEqual(calculateBudgetValue(1), 3.5, '$1 should be 3.5 pts');
console.log('✅ Pick Curve & Budget Math verified.');

// --- 2. MULTI-YEAR TRADES DATASET TESTS ---
console.log('Testing Historical Trades Dataset...');
assert.strictEqual(historicalTrades.length, 20, 'Should have exactly 20 trades');

const seasons = new Set(historicalTrades.map(t => t.season_year));
assert(seasons.has(2024), 'Must have 2024 trades');
assert(seasons.has(2025), 'Must have 2025 trades');
assert(seasons.has(2026), 'Must have 2026 trades');

// Verify 2024 Trade 1 (Tim <-> Daniel: Trout + Pick 80 + $11 for Turner + Pick 243)
const t2024_1 = historicalTrades.find(t => t.season_year === 2024 && t.trade_id === '1');
assert(t2024_1, '2024 Trade 1 must exist');
const graded2024_1 = gradeTrade(t2024_1);
assert(graded2024_1.winner === 'Daniel', 'Daniel should be winning side of 2024 Trade 1');
assert.strictEqual(graded2024_1.gradedPackages['Daniel'].grade, 'A+', 'Daniel should have A+ grade');
assert.strictEqual(graded2024_1.gradedPackages['Tim'].grade, 'F', 'Tim should have F grade');

// Verify 2025 Trade 1 (Anil <-> Mark: Corbin Carroll + Pick 52 + $4 for Bobby Witt Jr + Pick 235)
const t2025_1 = historicalTrades.find(t => t.season_year === 2025 && t.trade_id === '1');
assert(t2025_1, '2025 Trade 1 must exist');
const graded2025_1 = gradeTrade(t2025_1);
assert(graded2025_1.winner === 'Mark', 'Mark should win 2025 Trade 1');
console.log('✅ Historical Trades Dataset & Grading verified.');

// --- 3. REPLACEMENT RECOMMENDER & CAPITAL INVESTED TESTS ---
console.log('Testing Capital Invested Protection & Recommender...');

// Star player keeper test
const ohtani = { id: 39832, name: 'Shohei Ohtani', position: 'DH, SP', isPitcher: true };
const capOhtani = evaluatePlayerCapital(ohtani);
assert.strictEqual(capOhtani.tier, 'HIGH', 'Ohtani should be HIGH capital');
assert.strictEqual(capOhtani.action, 'HOLD_BENCH', 'Ohtani must have HOLD_BENCH action');
assert(capOhtani.badgeText.includes('DO NOT DROP'), 'Must include DO NOT DROP warning');

// Early round draft pick test
const mockPicks = [
  { playerId: 1001, overallPickNumber: 12, roundId: 2 },
  { playerId: 1002, overallPickNumber: 85, roundId: 9 },
  { playerId: 1003, overallPickNumber: 220, roundId: 22 }
];

const r2Player = { id: 1001, name: 'Round 2 Superstar', position: 'OF', isPitcher: false };
const capR2 = evaluatePlayerCapital(r2Player, mockPicks);
assert.strictEqual(capR2.tier, 'HIGH', 'Round 2 pick must be HIGH capital');
assert.strictEqual(capR2.action, 'HOLD_BENCH', 'Round 2 pick must be HOLD_BENCH');

const r9Player = { id: 1002, name: 'Round 9 Starter', position: '3B', isPitcher: false };
const capR9 = evaluatePlayerCapital(r9Player, mockPicks);
assert.strictEqual(capR9.tier, 'MODERATE', 'Round 9 pick must be MODERATE capital');
assert.strictEqual(capR9.action, 'BENCH_PREFERRED', 'Round 9 pick must be BENCH_PREFERRED');

const r22Player = { id: 1003, name: 'Round 22 Flyer', position: 'SS', isPitcher: false };
const capR22 = evaluatePlayerCapital(r22Player, mockPicks);
assert.strictEqual(capR22.tier, 'LOW', 'Round 22 pick must be LOW capital');
assert.strictEqual(capR22.action, 'DROP_CANDIDATE', 'Round 22 pick must be DROP_CANDIDATE');

// Positional match tests
const coldSS = { id: 2001, name: 'Slumping SS', position: 'SS', isPitcher: false };
const candidateSS = { player: { fullName: 'Waiver SS', eligibleSlots: [4, 7, 12], defaultPositionId: 6 } };
const candidateSP = { player: { fullName: 'Waiver SP', eligibleSlots: [13, 14], defaultPositionId: 1 } };
assert(isPositionMatch(coldSS, candidateSS), 'SS candidate should match cold SS');
assert(!isPositionMatch(coldSS, candidateSP), 'Pitcher candidate should not match cold SS');

// Waiver replacement ranking with standings synergy
const mockFAs = [
  {
    player: {
      id: 3001,
      fullName: 'Speedy Base Stealer',
      eligibleSlots: [4, 7, 12],
      defaultPositionId: 6,
      stats: [{ statSplitTypeId: 2, stats: { '23': 5, '0': 30, '1': 9 } }],
      ownership: { percentOwned: 35.0 }
    },
    ratings: { '2': { totalRating: 4.5 } }
  },
  {
    player: {
      id: 3002,
      fullName: 'Average Bat',
      eligibleSlots: [4, 7, 12],
      defaultPositionId: 6,
      stats: [{ statSplitTypeId: 2, stats: { '23': 0, '0': 30, '1': 8 } }],
      ownership: { percentOwned: 20.0 }
    },
    ratings: { '2': { totalRating: 4.2 } }
  }
];

const repsWithSBSynergy = findWaiverReplacements(coldSS, mockFAs, ['SB'], capR2);
assert(repsWithSBSynergy.length > 0, 'Should return waiver recommendations');
assert.strictEqual(repsWithSBSynergy[0].id, 3001, 'Speedy player should rank #1 when SB is weak');
assert(repsWithSBSynergy[0].fitReason.includes('Addresses Deficit: SB'), 'Fit reason should highlight SB deficit');
assert(repsWithSBSynergy[0].actionText.includes('Bench'), 'Action for High Capital player should advise Bench');

console.log('✅ Recommender & Capital Protection verified.');
console.log('🎉 ALL TESTS PASSED SUCCESSFULLY!');
