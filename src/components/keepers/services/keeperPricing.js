// Price schedule constants matching Google Sheet 'Compensation Picks'
// For 2026: 5 keepers, comp picks start at Round 6 (buy $15, sell $8)
export const COMP_BUY_PRICES_2026 = {
  6: 15, 7: 14, 8: 13, 9: 12, 10: 11,
  11: 10, 12: 9, 13: 8, 14: 7, 15: 6,
  16: 5, 17: 4, 18: 3, 19: 2, 20: 1
};
export const COMP_SELL_PRICES_2026 = {
  6: 8, 7: 4, 8: 2, 9: 1
};

// Starting in 2027: 6 keepers, comp picks start at Round 7 (buy $15, sell $8)
export const COMP_BUY_PRICES_2027 = {
  7: 15, 8: 14, 9: 13, 10: 12, 11: 11,
  12: 10, 13: 9, 14: 8, 15: 7, 16: 6,
  17: 5, 18: 4, 19: 3, 20: 2, 21: 1
};
export const COMP_SELL_PRICES_2027 = {
  7: 8, 8: 4, 9: 2, 10: 1
};

export function getCompBuyPrices(year) {
  return year >= 2027 ? COMP_BUY_PRICES_2027 : COMP_BUY_PRICES_2026;
}

export function getCompSellPrices(year) {
  return year >= 2027 ? COMP_SELL_PRICES_2027 : COMP_SELL_PRICES_2026;
}

export function getKeeperSlots(year) {
  return year >= 2027 ? [1, 2, 3, 4, 5, 6] : [1, 2, 3, 4, 5];
}

export const DRAFT_MANAGERS = [
  'Tim', 'Daniel', 'Will', 'Adrian', 'Garrett', 'Alex', 'Mark', 'Preston', 'Anil'
];

export function normalizeManager(mgr) {
  if (!mgr) return '';
  const s = String(mgr).trim();
  if (s.toLowerCase() === 'dan') return 'Daniel';
  return s;
}

// Canonical league finishes to guarantee correct #1-#9 display regardless of DB schema team IDs
export const CANONICAL_FINISHES = {
  2026: {
    'Tim': 1,
    'Daniel': 2,
    'Dan': 2,
    'Will': 3,
    'Adrian': 4,
    'Garrett': 5,
    'Alex': 6,
    'Mark': 7,
    'Preston': 8,
    'Anil': 9
  },
  2027: {
    'Daniel': 1,
    'Dan': 1,
    'Tim': 2,
    'Will': 3,
    'Adrian': 4,
    'Garrett': 5,
    'Anil': 6,
    'Alex': 7,
    'Preston': 8,
    'Mark': 9
  }
};

export function getActualFinish(season, owner, dbRank) {
  const norm = normalizeManager(owner);
  if (CANONICAL_FINISHES[season]?.[norm] !== undefined) {
    return CANONICAL_FINISHES[season][norm];
  }
  if (dbRank && dbRank >= 1 && dbRank <= 9) return dbRank;
  return CANONICAL_FINISHES[2027]?.[norm] || dbRank || '-';
}

export function getRotoBadgeStyle(pts) {
  if (pts >= 8.0) return { bg: 'rgba(16, 185, 129, 0.2)', text: '#10b981', border: 'rgba(16, 185, 129, 0.45)' };
  if (pts >= 6.0) return { bg: 'rgba(3, 218, 198, 0.18)', text: '#03dac6', border: 'rgba(3, 218, 198, 0.4)' };
  if (pts >= 4.0) return { bg: 'rgba(251, 191, 36, 0.18)', text: '#fbbf24', border: 'rgba(251, 191, 36, 0.4)' };
  if (pts >= 2.5) return { bg: 'rgba(249, 115, 22, 0.18)', text: '#f97316', border: 'rgba(249, 115, 22, 0.4)' };
  return { bg: 'rgba(244, 63, 94, 0.18)', text: '#f43f5e', border: 'rgba(244, 63, 94, 0.4)' };
}

export function formatRotoStat(key, val) {
  if (val === undefined || val === null || isNaN(val)) return '-';
  if (key === 'OBP') return (val < 1 ? val.toFixed(3).replace(/^0/, '') : val.toFixed(3));
  if (key === 'ERA' || key === 'WHIP') return val >= 90 ? 'N/A' : val.toFixed(2);
  if (key === 'QS') return val.toFixed(1);
  return String(Math.round(val));
}

export function calculateKeeperCostFromRank(rank) {
  if (!rank || rank > 150) return 0;
  if (rank === 1) return 40;
  if (rank === 2) return 36;
  if (rank === 3) return 33;
  if (rank === 4) return 31;
  if (rank === 5) return 30;
  if (rank === 6) return 29;
  if (rank === 7) return 28;
  if (rank === 8) return 27;
  if (rank === 9) return 26;
  if (rank === 10) return 25;
  if (rank <= 14) return 23;
  if (rank <= 16) return 22;
  if (rank <= 19) return 21;
  if (rank <= 22) return 20;
  if (rank <= 25) return 19;
  if (rank <= 28) return 18;
  if (rank <= 31) return 17;
  if (rank <= 35) return 16;
  if (rank <= 40) return 15;
  if (rank <= 45) return 14;
  if (rank <= 50) return 13;
  if (rank <= 55) return 12;
  if (rank <= 65) return 11;
  if (rank <= 75) return 9;
  if (rank <= 85) return 8;
  if (rank <= 95) return 7;
  if (rank <= 110) return 5;
  if (rank <= 125) return 4;
  if (rank <= 140) return 3;
  if (rank <= 150) return 2;
  return 0;
}
