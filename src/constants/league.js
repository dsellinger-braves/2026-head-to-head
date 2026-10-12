/**
 * @file league.js
 * @description Centralized Single Source of Truth for League Configuration,
 * Teams, Owners, Scoring Categories, Lineup Slots, and ESPN Stat IDs.
 *
 * League Ground Truth (from AGENTS.md):
 * - League ID: 130215
 * - Season Year: 2026
 * - Teams: 9 Active Human Teams + 1 Ghost/League Average Team (ID: 99)
 */

// ============================================================================
// 1. LEAGUE METADATA & CONFIGURATION
// ============================================================================
export const LEAGUE_CONFIG = {
  leagueId: 130215,
  seasonYear: 2026,
  scoringFormat: 'Head-to-Head Each Category',
  totalHumanTeams: 9,
  totalTeams: 10, // 9 humans + 1 ghost team
  ghostTeamId: 99
};

export const LEAGUE_ID = 130215;
export const CURRENT_SEASON = 2026;

// ============================================================================
// 2. TEAMS & FRANCHISES
// ============================================================================
export const TEAMS = {
  1: { id: 1, name: 'Tim', owner: 'Tim', slug: 'tim', color: '#dc2626' },
  2: { id: 2, name: 'Adrian', owner: 'Adrian', slug: 'adrian', color: '#2563eb' },
  3: { id: 3, name: 'Garrett', owner: 'Garrett', slug: 'garrett', color: '#16a34a' },
  5: { id: 5, name: 'Daniel', owner: 'Daniel', slug: 'daniel', color: '#d97706' },
  6: { id: 6, name: 'Anil', owner: 'Anil', slug: 'anil', color: '#9333ea' },
  8: { id: 8, name: 'Alex', owner: 'Alex', slug: 'alex', color: '#db2777' },
  12: { id: 12, name: 'Will', owner: 'Will', slug: 'will', color: '#4f46e5' },
  13: { id: 13, name: 'Mark', owner: 'Mark', slug: 'mark', color: '#0d9488' },
  14: { id: 14, name: 'Preston', owner: 'Preston', slug: 'preston', color: '#ea580c' },
  99: { id: 99, name: 'League Average', owner: 'The House', slug: 'ghost', color: '#4b5563' }
};

/** Array of 9 active league owners with ID and name */
export const LEAGUE_OWNERS = [
  { id: 1, name: 'Tim' },
  { id: 2, name: 'Adrian' },
  { id: 3, name: 'Garrett' },
  { id: 5, name: 'Daniel' },
  { id: 6, name: 'Anil' },
  { id: 8, name: 'Alex' },
  { id: 12, name: 'Will' },
  { id: 13, name: 'Mark' },
  { id: 14, name: 'Preston' }
];

/** Static league profiles mapping Discord/platform handles to owner names and roles */
export const STATIC_LEAGUE_PROFILES = {
  dsellinger: { team_id: 5, id: 5, owner_name: 'Daniel', name: 'Daniel', role: 'admin' },
  dan: { team_id: 5, id: 5, owner_name: 'Daniel', name: 'Daniel', role: 'admin' },
  daniel: { team_id: 5, id: 5, owner_name: 'Daniel', name: 'Daniel', role: 'admin' },
  adriaxx: { team_id: 2, id: 2, owner_name: 'Adrian', name: 'Adrian', role: 'commissioner' },
  adrian: { team_id: 2, id: 2, owner_name: 'Adrian', name: 'Adrian', role: 'commissioner' },
  aznchuy: { team_id: 1, id: 1, owner_name: 'Tim', name: 'Tim', role: 'owner' },
  tim: { team_id: 1, id: 1, owner_name: 'Tim', name: 'Tim', role: 'owner' },
  ghutch: { team_id: 3, id: 3, owner_name: 'Garrett', name: 'Garrett', role: 'owner' },
  garrett: { team_id: 3, id: 3, owner_name: 'Garrett', name: 'Garrett', role: 'owner' },
  anilbhairo: { team_id: 6, id: 6, owner_name: 'Anil', name: 'Anil', role: 'owner' },
  anil: { team_id: 6, id: 6, owner_name: 'Anil', name: 'Anil', role: 'owner' },
  ay0h: { team_id: 8, id: 8, owner_name: 'Alex', name: 'Alex', role: 'owner' },
  alex: { team_id: 8, id: 8, owner_name: 'Alex', name: 'Alex', role: 'owner' },
  senorspice: { team_id: 12, id: 12, owner_name: 'Will', name: 'Will', role: 'owner' },
  will: { team_id: 12, id: 12, owner_name: 'Will', name: 'Will', role: 'owner' },
  mrussell38: { team_id: 13, id: 13, owner_name: 'Mark', name: 'Mark', role: 'owner' },
  mark: { team_id: 13, id: 13, owner_name: 'Mark', name: 'Mark', role: 'owner' },
  pston3: { team_id: 14, id: 14, owner_name: 'Preston', name: 'Preston', role: 'owner' },
  preston: { team_id: 14, id: 14, owner_name: 'Preston', name: 'Preston', role: 'owner' }
};

/** Backward-compatible alias for trade services */
export const LEAGUE_OWNERS_NORMALIZED = STATIC_LEAGUE_PROFILES;

/**
 * Returns team ID from an owner name or Discord handle.
 * @param {string} ownerName
 * @returns {number}
 */
export const getTeamId = (ownerName) => {
  if (!ownerName) return 0;
  const key = String(ownerName).toLowerCase().trim();
  if (STATIC_LEAGUE_PROFILES[key]) return STATIC_LEAGUE_PROFILES[key].team_id;
  const found = LEAGUE_OWNERS.find(o => o.name.toLowerCase() === key);
  return found ? found.id : 0;
};


/** List of active 9 human owner names (alphabetical) */
export const ACTIVE_OWNERS = Object.values(TEAMS)
  .filter(t => t.id !== 99)
  .map(t => t.owner)
  .sort();

/** Standard alias used across draft & transaction calculators */
export const DRAFT_OWNERS = [...ACTIVE_OWNERS];

/** Active human team IDs */
export const ACTIVE_TEAM_IDS = Object.keys(TEAMS)
  .map(Number)
  .filter(id => id !== 99);

// ============================================================================
// 3. OWNER NORMALIZATION & EQUIVALENCE
// ============================================================================
export const CANONICAL_OWNERS = {
  tim: 'Tim',
  timothy: 'Tim',
  adrian: 'Adrian',
  garrett: 'Garrett',
  daniel: 'Dan',
  dan: 'Dan',
  dsellinger: 'Dan',
  anil: 'Anil',
  alex: 'Alex',
  will: 'Will',
  'will alexander': 'Will',
  mark: 'Mark',
  preston: 'Preston',
  owens: 'Owens',
  'team owens': 'Owens',
  'joseph mattingly': 'Joseph Mattingly',
  joseph: 'Joseph Mattingly',
  jihad: 'Jihad Mims',
  'jihad mims': 'Jihad Mims',
  jordy: 'Jordy',
  'kevin allen': 'Kevin Allen',
  'kishan patel': 'Kishan Patel',
  'matt bernhard': 'Matt Bernhard',
  miggy: 'Miggy',
  park: 'Park',
  'patrick fennell': 'Patrick Fennell',
  'scott ngueyen': 'Scott Ngueyen',
  'tim ficken': 'Tim Ficken',
  'andrew entwistle': 'Andrew Entwistle',
  'anurag komavarelli': 'Anurag Komavarelli'
};

export const AVAILABLE_SEASONS = [
  'ALL',
  2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012
];


export const CANONICAL_DRAFT_OWNERS = {
  ...CANONICAL_OWNERS,
  daniel: 'Daniel',
  dan: 'Daniel',
  dsellinger: 'Daniel'
};

/**
 * Normalizes an arbitrary owner string into its canonical league name.
 * @param {string} raw - Raw owner name or identifier
 * @param {'history' | 'draft'} [style='history'] - Format style ('history' uses 'Dan', 'draft' uses 'Daniel')
 * @returns {string} Normalized owner name
 */
export function normalizeOwner(raw, style = 'history') {
  if (!raw && raw !== 0) return 'Unknown';
  const str = String(raw).trim();
  const k = str.toLowerCase();
  const map = style === 'draft' ? CANONICAL_DRAFT_OWNERS : CANONICAL_OWNERS;
  return map[k] || str;
}

/**
 * Checks whether two owner names/representations represent the exact same owner.
 * Robust against "Dan" vs "Daniel", case differences, trailing whitespace, and nulls.
 * @param {string} a
 * @param {string} b
 * @returns {boolean}
 */
export function isSameOwner(a, b) {
  if (!a || !b) return false;
  const sA = String(a).trim().toLowerCase();
  const sB = String(b).trim().toLowerCase();
  if (sA === sB) return true;
  const isDanA = sA === 'dan' || sA === 'daniel' || sA === 'dsellinger';
  const isDanB = sB === 'dan' || sB === 'daniel' || sB === 'dsellinger';
  if (isDanA && isDanB) return true;
  const isTimA = sA === 'tim' || sA === 'timothy';
  const isTimB = sB === 'tim' || sB === 'timothy';
  if (isTimA && isTimB) return true;
  return false;
}

/**
 * Resolves a friendly owner name from either a numeric ID, a "team X" string, or a raw name.
 * @param {string|number} raw
 * @returns {string}
 */
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
    if (id === 99) return 'The House';
  }

  return normalizeOwner(str, 'draft');
}

/**
 * Retrieves the team metadata for an owner name.
 * @param {string} ownerName
 * @returns {typeof TEAMS[keyof typeof TEAMS] | null}
 */
export function getTeamByOwner(ownerName) {
  if (!ownerName) return null;
  return Object.values(TEAMS).find(t => isSameOwner(t.owner, ownerName)) || null;
}

// ============================================================================
// 4. OWNER ASSETS (AVATARS)
// ============================================================================
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

// ============================================================================
// 5. SCORING CATEGORIES
// ============================================================================
export const HITTING_CATEGORIES = ['R', 'HR', 'RBI', 'SB', 'OBP'];
export const PITCHING_CATEGORIES = ['K', 'QS', 'SV+HDs', 'ERA', 'WHIP'];

export const CATEGORIES = [...HITTING_CATEGORIES, ...PITCHING_CATEGORIES];

export const LEGACY_CATEGORIES = [
  'R', 'HR', 'RBI', 'SB', 'OBP',
  'K', 'QS', 'SVHLD', 'ERA', 'WHIP'
];

export const CAT_ICONS = {
  R: '⚡', HR: '💥', RBI: '🎯', SB: '🏃', OBP: '👁️',
  K: '🔥', QS: '💎', 'SV+HDs': '🛡️', ERA: '🎯', WHIP: '🔒'
};

export const SCORING_CATS = {
  // Hitting
  R: { label: 'Runs', name: 'Runs Scored', type: 'high' },
  HR: { label: 'HR', name: 'Home Runs', type: 'high' },
  RBI: { label: 'RBI', name: 'Runs Batted In', type: 'high' },
  SB: { label: 'SB', name: 'Stolen Bases', type: 'high' },
  OBP: { label: 'OBP', name: 'On-Base Percentage', type: 'high', isRate: true },

  // Pitching
  K: { label: 'K', name: 'Strikeouts', type: 'high' },
  QS: { label: 'Quality Starts', name: 'Quality Starts', type: 'high' },
  'SV+HDs': { label: 'Save+Holds', name: 'Saves + Holds', type: 'high' },
  ERA: { label: 'ERA', name: 'Earned Run Average', type: 'low', isRate: true },
  WHIP: { label: 'WHIP', name: 'Walks + Hits per Inning Pitched', type: 'low', isRate: true }
};

export const CATEGORY_DETAILS = Object.keys(SCORING_CATS).map(key => ({
  id: key,
  name: SCORING_CATS[key].name || SCORING_CATS[key].label,
  higherIsBetter: SCORING_CATS[key].type === 'high',
  isRate: Boolean(SCORING_CATS[key].isRate),
  icon: CAT_ICONS[key] || '📊'
}));

// ============================================================================
// 6. ROSTER LINEUP SLOTS
// ============================================================================
export const LINEUP_SLOTS = {
  0: 'C',
  1: '1B',
  2: '2B',
  3: '3B',
  4: 'SS',
  5: 'OF',
  6: '2B/SS',
  7: '1B/3B',
  11: 'DH',
  12: 'UTIL',
  13: 'P',
  14: 'SP',
  15: 'RP',
  16: 'Bench',
  17: 'IL',
  19: 'IF'
};

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

// ============================================================================
// 7. ESPN NUMERIC STAT ID MAPPING
// ============================================================================
export const ESPN_STAT_IDS = {
  // Batting
  '0': 'AB',
  '1': 'H',
  '2': 'AVG_raw',
  '3': '2B',
  '4': '3B',
  '5': 'HR',
  '6': 'TB_raw',
  '8': 'TB',
  '10': 'BB',
  '12': 'HBP',
  '13': 'SF',
  '16': 'PA',
  '17': 'OBP',
  '20': 'R',
  '21': 'RBI',
  '23': 'SB',
  '24': 'CS',
  '26': 'GDP',
  '27': 'SO',
  '72': 'E',

  // Pitching
  '32': 'W_app',
  '33': 'GS',
  '34': 'IP_raw',
  '35': 'TBF',
  '36': 'Pitches',
  '37': 'H_Allowed',
  '39': 'BB_Allowed',
  '44': 'R_Allowed',
  '45': 'ER',
  '46': 'HR_Allowed',
  '48': 'K',
  '50': 'WP',
  '53': 'W',
  '54': 'L',
  '57': 'SV',
  '58': 'BS',
  '60': 'HD',
  '63': 'QS'
};
