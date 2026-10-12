import defaultCalculations from '../../../data/keeperCalculations.json';
import defaultDraft2026 from '../../../data/draft2026.json';

// --- OWNER & MAPPING HELPERS ---
export function getFriendlyOwnerName(raw) {
  if (!raw && raw !== 0) return 'Unknown';
  const str = String(raw).trim();
  const match = str.match(/^team\s*(\d+)$/i);
  const id = match ? parseInt(match[1], 10) : (isNaN(parseInt(str, 10)) ? null : parseInt(str, 10));
  
  if (id !== null) {
    if (id === 1) return 'Tim';
    if (id === 2) return 'Adrian';
    if (id === 3) return 'Garrett';
    if (id === 5) return 'Daniel';
    if (id === 6 || id === 10) return 'Anil';
    if (id === 8) return 'Alex';
    if (id === 11) return 'Owens';
    if (id === 12) return 'Will';
    if (id === 13) return 'Mark';
    if (id === 9 || id === 14) return 'Preston';
  }
  
  const lower = str.toLowerCase();
  if (lower === 'dan' || lower === 'dsellinger') return 'Daniel';
  if (lower === 'timothy') return 'Tim';
  if (lower === 'owens' || lower === 'team owens') return 'Owens';
  if (lower === 'joseph mattingly') return 'Joseph';
  return str;
}

export const mappedDraft2026History = (defaultDraft2026 || []).map(p => ({
  Year: '2026',
  year: 2026,
  Round: String(p.round || ''),
  round: p.round,
  Pick_Overall: String(p.overall_pick),
  overall_pick: p.overall_pick,
  Player_Name: p.player_name,
  player_name: p.player_name,
  Player: p.player_name,
  Team_ID: p.team_owner || p.team_id,
  Owner: p.team_owner,
  owner: p.team_owner,
  team_id: p.team_id,
  player_id: String(p.player_id || ''),
  'ESPN PlayerID': String(p.player_id || ''),
  Keeper: p.is_keeper ? 'True' : 'False',
  is_keeper: Boolean(p.is_keeper),
  cost: p.cost || 0
}));

export function normalizeName(str) {
  return str
    ? str
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[.,'-]/g, '')
        .replace(/\s+(jr|sr|ii|iii|iv)$/i, '')
        .replace(/\s+/g, ' ')
        .trim()
    : '';
}

export function roundNumber(val, decimals = 3) {
  if (val == null) return null;
  const n = parseFloat(val);
  return isNaN(n) ? null : parseFloat(n.toFixed(decimals));
}

// Global calculation index from keeperCalculations.json (authoritative source of prices and ranks)
export const calcIndexById = new Map();
export const calcIndexByName = new Map();
(defaultCalculations?.players || []).forEach(p => {
  const pid = String(p.espn_player_id || p.player_id || p['ESPN PlayerID'] || '').trim();
  const name = normalizeName(p.player_name || p.Player || p.name || '');
  if (pid) calcIndexById.set(pid, p);
  if (name && !calcIndexByName.has(name)) calcIndexByName.set(name, p);
});

export function getEffectiveQS(player) {
  if (!player) return 0;
  // 1. Explicit calculation stats from keeperCalculations.json
  const calcQs = parseFloat(player.calc_qs || player.y1_qs || player?.y1?.stats?.QS);
  if (!isNaN(calcQs) && calcQs > 0) return Math.round(calcQs);
  // 2. ZiPS Quality Starts if > 0
  const zips = parseFloat(player.ZIPSQS);
  if (!isNaN(zips) && zips > 0) return Math.round(zips);
  // 3. ESPN Quality Starts projection
  const espn = parseFloat(player.ESPNQS || player.ESPN_QS);
  if (!isNaN(espn) && espn > 0) return Math.round(espn);
  // 4. Starting pitcher games started estimate (approx 45% of starts are QS)
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('P') || Boolean(player.Pitcher);
  if (isPitcher) {
    const gs = parseFloat(player.ZIPSGS || player.GS || player.G || player.calc_gs || player?.y1?.stats?.GS);
    if (!isNaN(gs) && gs >= 5) return Math.round(gs * 0.45);
    const ip = parseFloat(player.ZIPSIP || player.IP || player.ESPNIP || player?.y1?.stats?.IP);
    if (!isNaN(ip) && ip >= 60) return Math.round((ip / 6) * 0.55);
  }
  return 0;
}

export function enrichPlayerWithCalculations(p, roomSeason = 2027) {
  if (!p) return p;
  const pid = String(p['ESPN PlayerID'] || p.espn_player_id || p.id || p.player_id || '').trim();
  const cleanName = normalizeName(p.Player || p.Name || p.player_name || p.name || '');
  const calc = (pid ? calcIndexById.get(pid) : null) || (cleanName ? calcIndexByName.get(cleanName) : null);

  const zipsVal = parseFloat(p.ZIPSQS);
  const calcVal = parseFloat(calc?.y1?.stats?.QS);
  const espnVal = parseFloat(p.ESPNQS || p.ESPN_QS);
  const gsVal = parseFloat(calc?.y1?.stats?.GS || p.ZIPSGS || p.GS);
  const effectiveQS = (!isNaN(zipsVal) && zipsVal > 0 ? Math.round(zipsVal) : null) ??
                      (!isNaN(calcVal) && calcVal > 0 ? Math.round(calcVal) : null) ??
                      (!isNaN(espnVal) && espnVal > 0 ? Math.round(espnVal) : null) ??
                      (!isNaN(gsVal) && gsVal >= 5 ? Math.round(gsVal * 0.45) : null) ??
                      0;

  if (!calc) {
    const rawRank = parseFloat(p['Hefty Keeper Rank'] ?? p.rank);
    const fallbackRank = (!isNaN(rawRank) && rawRank > 1155) ? rawRank : 9999;
    return {
      ...p,
      'Hefty Keeper Price': 0,
      'Hefty Keeper Rank': fallbackRank,
      'Hefty Single Season Rank': fallbackRank,
      'Hefty Single Season Price': 0,
      overall_price: 0,
      overall_rank: fallbackRank,
      price: 0,
      rank: fallbackRank,
      ZIPSQS: effectiveQS > 0 ? effectiveQS : (p.ZIPSQS || 0),
      ESPNQS: !isNaN(espnVal) && espnVal > 0 ? espnVal : (effectiveQS > 0 ? effectiveQS : p.ESPNQS),
      calc_qs: effectiveQS > 0 ? effectiveQS : undefined
    };
  }

  const overallPrice = calc.overall_price !== undefined ? calc.overall_price : (p['Hefty Keeper Price'] ?? p.price ?? 0);
  const overallRank = calc.overall_rank !== undefined ? calc.overall_rank : (p['Hefty Keeper Rank'] ?? p.rank ?? 9999);
  const ssRank = calc.y1?.rank || calc.overall_rank || p['Hefty Single Season Rank'] || overallRank;
  const ssPrice = calc.espn_price ?? calc.overall_price ?? p['Hefty Single Season Price'] ?? overallPrice;

  const effectiveRank = roomSeason === 2027 ? overallRank : (ssRank || overallRank);
  const effectivePrice = overallPrice;

  return {
    ...p,
    'Hefty Keeper Price': overallPrice,
    'Hefty Keeper Rank': effectiveRank,
    'Hefty Single Season Rank': ssRank,
    'Hefty Single Season Price': ssPrice,
    overall_price: overallPrice,
    overall_rank: overallRank,
    price: effectivePrice,
    rank: effectiveRank,
    overall_pr: calc.overall_pr ?? p.overall_pr,
    'Projected PR': calc.overall_pr ?? p['Projected PR'],
    ZIPSR: p.ZIPSR ?? calc.y1?.stats?.R,
    ZIPSHR: p.ZIPSHR ?? calc.y1?.stats?.HR,
    ZIPSRBI: p.ZIPSRBI ?? calc.y1?.stats?.RBI,
    ZIPSSB: p.ZIPSSB ?? calc.y1?.stats?.SB,
    ZIPSOBP: p.ZIPSOBP ?? calc.y1?.stats?.OBP,
    ZIPSK: p.ZIPSK ?? calc.y1?.stats?.SO,
    ZIPSQS: effectiveQS > 0 ? effectiveQS : (p.ZIPSQS || 0),
    ESPNQS: !isNaN(espnVal) && espnVal > 0 ? espnVal : (effectiveQS > 0 ? effectiveQS : p.ESPNQS),
    calc_qs: calcVal > 0 ? calcVal : (effectiveQS > 0 ? effectiveQS : undefined),
    'ZIPSSV+HDs': p['ZIPSSV+HDs'] ?? calc.y1?.stats?.SVHD,
    ZIPSERA: p.ZIPSERA ?? calc.y1?.stats?.ERA,
    ZIPSWHIP: p.ZIPSWHIP ?? calc.y1?.stats?.WHIP,
    Position: p.Position || calc.position || (calc.is_pitcher ? (calc.pitcher_role || 'SP') : 'DH'),
    Team: p.Team || calc.team || 'MLB'
  };
}

export function mergeEspnData(playerPool, espnData) {
  if (!playerPool || !espnData || espnData.length === 0) return playerPool;
  const espnMap = new Map();
  espnData.forEach(p => {
    const pid = p.player_id || p['ESPN PlayerID'];
    if (pid) espnMap.set(String(pid), p);
  });
  console.log(`📺 ESPN merge: ${espnMap.size} records available`);
  let matched = 0;
  const merged = playerPool.map(p => {
    const pid = String(p['ESPN PlayerID']);
    const e = espnMap.get(pid);
    if (e) {
      matched++;
      return {
        ...p,
        Position: e.eligiblePositions || p.Position,
        ADP: e.averageDraftPosition ?? p.ADP,
        averageDraftPosition: e.averageDraftPosition,
        ADPChange: e.averageDraftPositionPercentChange,
        averageDraftPositionPercentChange: e.averageDraftPositionPercentChange,
        'Percent Owned': e.percentOwned ?? p['Percent Owned'],
        percentOwned: e.percentOwned,
        OwnershipChange: e.percentChange,
        percentChange: e.percentChange,
        'ESPN ROTO Rank': e.rotoRank,
        rotoRank: e.rotoRank,
        'ESPN Single Season Ranking': e.rotoRank,
        injuryStatus: e.injuryStatus || e.injured_status,
        injured: e.injured,
        seasonOutlook: e.seasonOutlook,
        ESPNPA: e.ESPN_PA ?? p.ESPNPA,
        ESPNHR: e.ESPN_HR ?? p.ESPNHR,
        ESPNR: e.ESPN_R ?? p.ESPNR,
        ESPNRBI: e.ESPN_RBI ?? p.ESPNRBI,
        ESPNSB: e.ESPN_SB ?? p.ESPNSB,
        ESPNOBP: e.ESPN_OBP ?? p.ESPNOBP,
        ESPNIP: e.ESPN_IP ?? p.ESPNIP,
        ESPNK: e.ESPN_K ?? p.ESPNK,
        ESPNERA: e.ESPN_ERA ?? p.ESPNERA,
        ESPNWHIP: e.ESPN_WHIP ?? p.ESPNWHIP,
        ESPNQS: e.ESPN_QS ?? p.ESPNQS,
        'ESPNSV+HDs': (e.ESPN_SV ?? 0) + (e.ESPN_HD ?? 0) || p['ESPNSV+HDs'],
        stats2025: e.stats2025
      };
    }
    return p;
  });
  console.log(`📺 ESPN merge complete: ${matched} players matched`);
  return merged;
}

export function mergeZipsData(playerPool, battersZips = [], pitchersZips = []) {
  if (!playerPool || playerPool.length === 0) {
    console.warn('No players to merge ZiPS data into');
    return playerPool;
  }

  const getName = p => p.Name || p.PlayerName || p.name || p.playername || '';
  const getMlbamId = p => p.xMLBAMID || p.XMLBAMID || p.mlbamid || p.MLBAMID || p.mlbam_id || '';

  const batterIdMap = new Map();
  const pitcherIdMap = new Map();
  const batterNameMap = new Map();
  const pitcherNameMap = new Map();
  const ambiguousBatters = new Set();
  const ambiguousPitchers = new Set();

  battersZips.forEach(p => {
    const mid = getMlbamId(p);
    if (mid) batterIdMap.set(String(mid), p);
    const name = getName(p);
    if (name) {
      const clean = normalizeName(name);
      if (batterNameMap.has(clean)) {
        ambiguousBatters.add(clean);
        batterNameMap.delete(clean);
      } else if (!ambiguousBatters.has(clean)) {
        batterNameMap.set(clean, p);
      }
    }
  });

  pitchersZips.forEach(p => {
    const mid = getMlbamId(p);
    if (mid) pitcherIdMap.set(String(mid), p);
    const name = getName(p);
    if (name) {
      const clean = normalizeName(name);
      if (pitcherNameMap.has(clean)) {
        ambiguousPitchers.add(clean);
        pitcherNameMap.delete(clean);
      } else if (!ambiguousPitchers.has(clean)) {
        pitcherNameMap.set(clean, p);
      }
    }
  });

  console.log(`📊 ZiPS merge maps: ${batterIdMap.size} batters by ID, ${batterNameMap.size} by name (${ambiguousBatters.size} ambiguous names skipped: ${[...ambiguousBatters].join(', ') || 'none'})`);
  console.log(`📊 ZiPS merge maps: ${pitcherIdMap.size} pitchers by ID, ${pitcherNameMap.size} by name (${ambiguousPitchers.size} ambiguous names skipped: ${[...ambiguousPitchers].join(', ') || 'none'})`);

  const poolNameCounts = new Map();
  playerPool.forEach(p => {
    const clean = normalizeName(p.Player || p.Name || '');
    if (clean) poolNameCounts.set(clean, (poolNameCounts.get(clean) || 0) + 1);
  });
  const poolAmbiguous = new Set([...poolNameCounts.entries()].filter(([, count]) => count > 1).map(([name]) => name));
  if (poolAmbiguous.size > 0) {
    console.log(`📊 Pool-ambiguous names (name fallback disabled for these): ${[...poolAmbiguous].join(', ')}`);
  }

  let bMatched = 0;
  let pMatched = 0;
  let nameFallbackCount = 0;

  const merged = playerPool.map(player => {
    const mlbRaw = String(player.MLBAMID || player.mlbamid || '').trim();
    const mlbamId = mlbRaw === '' || mlbRaw === '0' ? null : mlbRaw;
    const cleanName = normalizeName(player.Player || player.Name || '');
    const canFallback = !poolAmbiguous.has(cleanName) && !ambiguousBatters.has(cleanName) && !ambiguousPitchers.has(cleanName);

    let bData = null;
    if (mlbamId) bData = batterIdMap.get(mlbamId);
    if (!bData && cleanName && canFallback) {
      bData = batterNameMap.get(cleanName);
      if (bData) nameFallbackCount++;
    }

    let pData = null;
    if (mlbamId) pData = pitcherIdMap.get(mlbamId);
    if (!pData && cleanName && canFallback) {
      pData = pitcherNameMap.get(cleanName);
      if (pData) nameFallbackCount++;
    }

    const res = { ...player };

    if (bData) {
      bMatched++;
      res.ZIPSPA = bData.PA ?? player.ZIPSPA;
      res.ZIPSAB = bData.AB ?? player.ZIPSAB;
      res.ZIPSH = bData.H ?? player.ZIPSH;
      res.ZIPSHR = bData.HR ?? player.ZIPSHR;
      res.ZIPSR = bData.R ?? player.ZIPSR;
      res.ZIPSRBI = bData.RBI ?? player.ZIPSRBI;
      res.ZIPSSB = bData.SB ?? player.ZIPSSB;
      res.ZIPSCS = bData.CS ?? player.ZIPSCS;
      res.ZIPSBB = bData.BB ?? player.ZIPSBB;
      res.ZIPSK = bData.SO ?? bData.K ?? player.ZIPSK;
      res.ZIPSSO = bData.SO ?? bData.K ?? player.ZIPSSO;
      res.ZIPSOBP = roundNumber(bData.OBP) ?? player.ZIPSOBP;
      res.ZIPSSLG = roundNumber(bData.SLG) ?? player.ZIPSSLG;
      res.ZIPSAVG = roundNumber(bData.AVG) ?? player.ZIPSAVG;
      res.ZIPSwOBA = roundNumber(bData.wOBA) ?? player.ZIPSwOBA;
      res.ZIPSwRC = bData.wRC ?? player.ZIPSwRC;
      res.ZIPSwRCplus = bData['wRC+'] ?? player.ZIPSwRCplus;
      res.ZIPSWAR = bData.WAR ?? player.ZIPSWAR;
      res.ZIPS2B = bData['2B'] ?? player.ZIPS2B;
      res.ZIPS3B = bData['3B'] ?? player.ZIPS3B;
    }

    if (pData) {
      pMatched++;
      res.ZIPSW = pData.W ?? player.ZIPSW;
      res.ZIPSL = pData.L ?? player.ZIPSL;
      res.ZIPSG = pData.G ?? player.ZIPSG;
      res.ZIPSGS = pData.GS ?? player.ZIPSGS;
      res.ZIPSIP = pData.IP ?? player.ZIPSIP;
      res.ZIPSK = pData.SO ?? pData.K ?? res.ZIPSK ?? player.ZIPSK;
      res.ZIPSSO = pData.SO ?? pData.K ?? player.ZIPSSO;
      res.ZIPSBB = pData.BB ?? res.ZIPSBB ?? player.ZIPSBB;
      res.ZIPSERA = roundNumber(pData.ERA, 2) ?? player.ZIPSERA;
      res.ZIPSWHIP = roundNumber(pData.WHIP, 2) ?? player.ZIPSWHIP;
      res.ZIPSFIP = roundNumber(pData.FIP, 2) ?? player.ZIPSFIP;
      res.ZIPSxFIP = roundNumber(pData.xFIP, 2) ?? player.ZIPSxFIP;
      res.ZIPSKper9 = roundNumber(pData['K/9'], 2) ?? player.ZIPSKper9;
      res.ZIPSBBper9 = roundNumber(pData['BB/9'], 2) ?? player.ZIPSBBper9;
      res.ZIPSSV = pData.SV ?? player.ZIPSSV ?? 0;
      res.ZIPSHD = pData.HLD ?? pData.HD ?? player.ZIPSHD ?? 0;
      res['ZIPSSV+HDs'] = (res.ZIPSSV || 0) + (res.ZIPSHD || 0);
      if (pData.QS !== undefined && pData.QS !== null && !isNaN(parseFloat(pData.QS)) && parseFloat(pData.QS) > 0) {
        res.ZIPSQS = pData.QS;
      } else if (pData.GS && pData.IP) {
        res.ZIPSQS = Math.round(pData.GS * 0.45);
      } else {
        const fallbackQs = getEffectiveQS({ ...player, ...res, ZIPSGS: pData.GS || player.ZIPSGS });
        if (fallbackQs > 0) res.ZIPSQS = fallbackQs;
      }
      res.ZIPSWAR_pit = pData.WAR ?? player.ZIPSWAR_pit;
    }

    return res;
  });

  console.log(`📊 ZiPS merge complete: ${bMatched} batters, ${pMatched} pitchers matched (${nameFallbackCount} via name fallback)`);
  return merged;
}

// --- CONSTANTS ---
export const DRAFT_OWNERS = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"].sort();

export const ROSTER_SLOTS = [
  { id: 'C1', label: 'C', eligible: ['C'] },
  { id: 'C2', label: 'C', eligible: ['C'] },
  { id: '1B', label: '1B', eligible: ['1B'] },
  { id: '2B', label: '2B', eligible: ['2B'] },
  { id: '3B', label: '3B', eligible: ['3B'] },
  { id: 'SS', label: 'SS', eligible: ['SS'] },
  { id: 'OF1', label: 'OF', eligible: ['OF'] },
  { id: 'OF2', label: 'OF', eligible: ['OF'] },
  { id: 'OF3', label: 'OF', eligible: ['OF'] },
  { id: 'OF4', label: 'OF', eligible: ['OF'] },
  { id: 'OF5', label: 'OF', eligible: ['OF'] },
  { id: 'DH', label: 'DH', eligible: ['DH', 'C', '1B', '2B', '3B', 'SS', 'OF'] },
  { id: 'SP1', label: 'SP', eligible: ['SP'] },
  { id: 'SP2', label: 'SP', eligible: ['SP'] },
  { id: 'SP3', label: 'SP', eligible: ['SP'] },
  { id: 'SP4', label: 'SP', eligible: ['SP'] },
  { id: 'RP1', label: 'RP', eligible: ['RP'] },
  { id: 'RP2', label: 'RP', eligible: ['RP'] },
  { id: 'BN1', label: 'Bench', eligible: ['ALL'] },
  { id: 'BN2', label: 'Bench', eligible: ['ALL'] },
  { id: 'BN3', label: 'Bench', eligible: ['ALL'] }
];

export const OWNER_AVATARS = {
  Dan: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/dan.png',
  Daniel: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/dan.png',
  Alex: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/alex.png',
  Adrian: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/adrian.png',
  Garrett: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/garrett.png',
  Mark: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/mark.png',
  Preston: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/preston.png',
  Tim: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/tim.png',
  Will: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/will.png',
  Anil: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/anil.png',
  default: 'https://raw.githubusercontent.com/dsellinger-braves/fantasy-draft/gh-pages/images/owners/default.png'
};

export function generateDefaultDraftOrder() {
  const order = [];
  let overall = 1;
  for (let round = 1; round <= 21; round++) {
    const roundOwners = round % 2 === 1 ? [...DRAFT_OWNERS] : [...DRAFT_OWNERS].reverse();
    roundOwners.forEach((owner, idx) => {
      order.push({
        'Overall Pick': overall,
        'Round': round,
        'Pick': idx + 1,
        'Owner': owner,
        'ESPN PlayerID': null,
        'Selection': null
      });
      overall++;
    });
  }
  return order;
}

export function formatPlayerStats(player) {
  const isPitcher = player.Position?.includes('SP') || player.Position?.includes('RP');
  if (isPitcher) {
    const effQS = getEffectiveQS(player);
    return `ERA: ${player.ZIPSERA || player.ESPNERA || 'N/A'}, WHIP: ${player.ZIPSWHIP || player.ESPNWHIP || 'N/A'}, K: ${player.ZIPSK || player.ESPNK || 'N/A'}, QS: ${effQS || player.ZIPSQS || player.ESPNQS || 'N/A'}, SV+H: ${player['ZIPSSV+HDs'] || player['ESPNSV+HDs'] || 'N/A'}`;
  }
  return `R: ${player.ZIPSR || player.ESPNR || 'N/A'}, HR: ${player.ZIPSHR || player.ESPNHR || 'N/A'}, RBI: ${player.ZIPSRBI || player.ESPNRBI || 'N/A'}, SB: ${player.ZIPSSB || player.ESPNSB || 'N/A'}, OBP: ${player.ZIPSOBP || player.ESPNOBP || 'N/A'}`;
}

export function getEffectiveRank(player) {
  if (!player) return null;
  const r = player['Hefty Keeper Rank'] ?? 
            player.overall_rank ??
            player['Hefty Single Season Rank'] ?? 
            player['ESPN Keeper Rank'] ?? 
            player['ESPN Single Season Rank'] ?? 
            player['ESPN ROTO Rank'] ?? 
            player.ADP ?? 
            player.rank;
  const num = parseFloat(r);
  return isNaN(num) || num <= 0 ? null : Math.round(num);
}

export function getEffectivePrice(player) {
  if (!player) return null;
  const p = player['Hefty Keeper Price'] ?? 
            player.overall_price ??
            player['Hefty Single Season Price'] ?? 
            player['ESPN Price'] ?? 
            player.Value ?? 
            player.price;
  const num = parseFloat(p);
  return isNaN(num) || num < 0 ? null : Math.round(num);
}

// --- INJURY HELPER ---
export function getInjuryIndicator(playerId, playerInfoArray) {
  if (!playerInfoArray || !playerInfoArray.length) return null;
  const pId = String(playerId);
  const info = playerInfoArray.find(i => String(i.player_id) === pId);
  if (!info || !info.injured_status || info.injured_status === 'ACTIVE') return null;

  const status = info.injured_status.toUpperCase();
  let color = '#ff9800';
  let displayText = status;

  if (status === 'DAY_TO_DAY' || status === 'DTD') {
    color = '#ffc107';
    displayText = 'DTD';
  } else if (status.includes('IL') || status === 'OUT') {
    color = '#f44336';
    if (status === 'SEVEN_DAY_IL') displayText = 'IL-7';
    else if (status === 'TEN_DAY_IL') displayText = 'IL-10';
    else if (status === 'FIFTEEN_DAY_IL') displayText = 'IL-15';
    else if (status === 'SIXTY_DAY_IL') displayText = 'IL-60';
    else displayText = 'IL';
  }

  return { color, status: displayText };
}

// --- 2027 DRAFT CAPITAL CALCULATOR ---
export function compute2027DraftPicks(draftTrades = []) {
  const owners = ["Adrian", "Alex", "Anil", "Daniel", "Garrett", "Mark", "Preston", "Tim", "Will"].sort();
  const picks = [];

  // Base draft order: 26 rounds (Rounds 7 through 32), 1 pick per owner per round (234 total picks)
  for (let round = 7; round <= 32; round++) {
    owners.forEach(owner => {
      picks.push({
        round,
        originalOwner: owner,
        currentOwner: owner,
        isTraded: false,
        tradeDetails: null
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

// --- 2027 DRAFT ORDER GENERATOR ---
export function generate2027DraftOrder(draftTrades = [], keepers2027 = [], compPicks2027 = []) {
  const fullOrder = [];
  let overall = 1;

  // Rounds 1-6: Keepers (54 picks across 9 owners)
  for (let r = 1; r <= 6; r++) {
    const roundOwners = r % 2 === 1 ? [...DRAFT_OWNERS] : [...DRAFT_OWNERS].reverse();
    roundOwners.forEach((owner, idx) => {
      // Find keeper assigned to this owner and slot if submitted
      const keeper = (keepers2027 || []).find(k => {
        const rawOwner = k.owner || k.owner_name || k.team_owner || '';
        const oName = (rawOwner === 'Dan' || rawOwner === 'dsellinger') ? 'Daniel' : rawOwner;
        return oName.toLowerCase() === owner.toLowerCase() && (Number(k.keeper_slot) === r || (!k.keeper_slot && idx === 0));
      });

      const espnId = keeper ? String(keeper.espn_player_id || keeper.player_id || '') : null;

      fullOrder.push({
        'Overall Pick': overall,
        Round: r,
        Pick: idx + 1,
        'Raw Pick Number': overall,
        'Original Owner': owner,
        Owner: owner,
        'Pick Traded?': 'N',
        'Comp Pick?': null,
        'Number Pick for Owner': `${owner}${r - 1}`,
        'ESPN PlayerID': espnId || null,
        Selection: keeper ? (keeper.player_name || keeper.Player) : null,
        isKeeper: true
      });
      overall++;
    });
  }

  // Rounds 7-32: Drafted rounds with 2026/2027 trades applied
  const basePicks = compute2027DraftPicks(draftTrades);
  const ownerPickCounters = {};
  DRAFT_OWNERS.forEach(o => { ownerPickCounters[o] = 1; });

  for (let r = 7; r <= 32; r++) {
    const roundPicks = basePicks.filter(p => p.round === r);
    const roundCompPicks = (compPicks2027 || []).filter(cp => cp.round_num === r);

    let pickInRound = 1;
    roundPicks.forEach(p => {
      const owner = p.currentOwner;
      const count = ownerPickCounters[owner] || 1;
      ownerPickCounters[owner] = count + 1;

      fullOrder.push({
        'Overall Pick': overall,
        Round: r,
        Pick: pickInRound,
        'Raw Pick Number': overall,
        'Original Owner': p.originalOwner,
        Owner: p.currentOwner,
        'Pick Traded?': p.isTraded ? 'Y' : 'N',
        'Comp Pick?': null,
        'Number Pick for Owner': `${owner}${count}`,
        'ESPN PlayerID': null,
        Selection: null,
        tradeDetails: p.tradeDetails
      });
      pickInRound++;
      overall++;
    });

    roundCompPicks.forEach(cp => {
      const owner = cp.owner_name === 'Dan' ? 'Daniel' : cp.owner_name;
      const count = ownerPickCounters[owner] || 1;
      ownerPickCounters[owner] = count + 1;

      fullOrder.push({
        'Overall Pick': overall,
        Round: r,
        Pick: pickInRound,
        'Raw Pick Number': overall,
        'Original Owner': 'Comp Pick',
        Owner: owner,
        'Pick Traded?': 'N',
        'Comp Pick?': cp.comp_type || 'Y - Awarded',
        'Number Pick for Owner': `${owner}${count}`,
        'ESPN PlayerID': null,
        Selection: null
      });
      pickInRound++;
      overall++;
    });
  }

  return fullOrder;
}
