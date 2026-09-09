import historicalFinishes from './historicalFinishes.json';
import { TEAMS } from '../schedule';

// Canonical owner name mapping
export const CANONICAL_OWNERS = {
  tim: 'Tim',
  adrian: 'Adrian',
  garrett: 'Garrett',
  daniel: 'Dan',
  dan: 'Dan',
  anil: 'Anil',
  alex: 'Alex',
  will: 'Will',
  mark: 'Mark',
  preston: 'Preston'
};

export const normalizeOwner = (raw) => {
  if (!raw) return 'Unknown';
  const k = raw.trim().toLowerCase();
  return CANONICAL_OWNERS[k] || raw.trim();
};

export function getHistoricalSeasons(active2026Data = null) {
  // Deep clone bundled finishes (2012-2025)
  const allRows = historicalFinishes.map(r => ({
    year: parseInt(r.Year, 10),
    rank: parseInt(r['Final Rank'], 10),
    rawOwner: r.Owner,
    owner: normalizeOwner(r.Owner),
    teamName: r['Team Name'] || r.Owner,
    points: parseFloat(r.Points) || 0,
    hittingPoints: parseFloat(r['Hitting Points']) || 0,
    pitchingPoints: parseFloat(r['Pitching Points']) || 0,
    isActiveOwner: r['Active Owner?'] === 'Y',
    categories: {
      R: parseFloat(r.R) || 0,
      HR: parseFloat(r.HR) || 0,
      RBI: parseFloat(r.RBI) || 0,
      SB: parseFloat(r.SB) || 0,
      OBP: parseFloat(r.OBP) || 0,
      K: parseFloat(r.K) || 0,
      QS: parseFloat(r.QS) || 0,
      'SV+HDs': parseFloat(r.SVHLD || r['SV+HDs']) || 0,
      ERA: parseFloat(r.ERA) || 0,
      WHIP: parseFloat(r.WHIP) || 0
    }
  }));

  // Append live 2026 standings if available
  if (active2026Data && active2026Data.length > 0) {
    const sorted2026 = [...active2026Data].sort((a, b) => b.totalPoints - a.totalPoints);
    sorted2026.forEach((item, idx) => {
      allRows.push({
        year: 2026,
        rank: idx + 1,
        rawOwner: item.owner,
        owner: normalizeOwner(item.owner),
        teamName: item.name || item.owner,
        points: item.totalPoints,
        hittingPoints: item.hittingPoints || 0,
        pitchingPoints: item.pitchingPoints || 0,
        isActiveOwner: true,
        isCurrentSeason: true,
        categories: item.categories || {}
      });
    });
  }

  return allRows.sort((a, b) => a.year - b.year);
}

// Compute all-time franchise leaderboard
export function getFranchiseLeaderboard(allRows) {
  const byOwner = {};

  allRows.forEach(row => {
    const o = row.owner;
    if (!byOwner[o]) {
      byOwner[o] = {
        owner: o,
        teamName: row.teamName,
        isActiveOwner: row.isActiveOwner,
        seasonsCount: 0,
        titles: [],
        top3Count: 0,
        ranks: [],
        totalPoints: 0,
        seasons: []
      };
    }
    const rec = byOwner[o];
    rec.seasonsCount += 1;
    rec.ranks.push(row.rank);
    rec.totalPoints += row.points;
    rec.seasons.push(row);
    if (row.rank === 1) rec.titles.push(row.year);
    if (row.rank <= 3) rec.top3Count += 1;
  });

  return Object.values(byOwner).map(item => {
    const avgRank = item.ranks.reduce((a, b) => a + b, 0) / item.ranks.length;
    const avgPts = item.totalPoints / item.seasonsCount;
    const bestRank = Math.min(...item.ranks);
    const worstRank = Math.max(...item.ranks);

    return {
      ...item,
      avgRank: avgRank.toFixed(1),
      avgPoints: avgPts.toFixed(1),
      bestRank,
      worstRank
    };
  }).sort((a, b) => {
    // Sort by titles desc, then top 3 desc, then avg rank asc
    if (b.titles.length !== a.titles.length) return b.titles.length - a.titles.length;
    if (b.top3Count !== a.top3Count) return b.top3Count - a.top3Count;
    return parseFloat(a.avgRank) - parseFloat(b.avgRank);
  });
}
