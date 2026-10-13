/**
 * @file routes.js
 * @description Centralized declarative routing, hash mapping, and sub-tab synchronization.
 */

export const AVAILABLE_SEASONS = [2027, ...Array.from({ length: 2026 - 2012 + 1 }, (_, i) => 2026 - i)];

export const VIEW_TO_HASH = {
  dashboard: 'dashboard',
  weekly: 'matchups',
  summary: 'standings',
  teams: 'teams',
  rosters: 'rosters',
  transactions: 'transactions',
  trades: 'trades',
  players: 'players',
  disparities: 'disparities',
  progression: 'progression',
  highlights: 'highlights',
  history: 'history',
  fantasycast: 'fantasycast',
  valuations: 'keeper-prices',
  draft: 'draft',
  drafthistory: 'draft/history',
  pickem: 'pickem',
  capital: 'draft-capital',
  keepers: 'keepers-budgets',
  'player-owners': 'player-owners',
  draftprep: 'draft/tiers',
};

export const HASH_TO_VIEW = {
  '': 'dashboard',
  dashboard: 'dashboard',
  home: 'dashboard',
  landing: 'dashboard',
  roto: 'teams',
  teams: 'teams',
  'teams/roto': 'teams',
  'teams/what-if': 'teams',
  'what-if': 'teams',
  'teams/simulator': 'teams',
  'teams/bench': 'teams',
  'teams/gap': 'teams',
  'teams/minutiae': 'teams',
  'teams/best-lineup': 'teams',
  'best-lineup': 'teams',
  'teams/optimal': 'teams',
  'optimal': 'teams',
  'simulator/best-lineup': 'teams',
  'player-simulator': 'teams',
  'impact-simulator': 'teams',
  'player-owners': 'player-owners',
  'player-owner': 'player-owners',
  'players/owner-season': 'player-owners',
  'players/owner-seasons': 'player-owners',
  'players/owner-career': 'player-owners',
  'players/owners': 'player-owners',
  'owner-players': 'player-owners',
  rosters: 'rosters',
  roster: 'rosters',
  lineups: 'rosters',
  lineup: 'rosters',
  'teams/rosters': 'rosters',
  'teams/lineups': 'rosters',
  standings: 'summary',
  summary: 'summary',
  'h2h-standings': 'summary',
  'teams/standings': 'summary',
  matchups: 'weekly',
  weekly: 'weekly',
  'teams/matchups': 'weekly',
  progression: 'progression',
  'teams/progression': 'progression',
  highlights: 'highlights',
  'teams/highlights': 'highlights',
  history: 'history',
  'league-history': 'history',
  'teams/history': 'history',
  finishes: 'history',
  'draft/finishes': 'history',
  'history/standings': 'history',
  'history/highlights': 'history',
  'history/records': 'history',
  'history/hall-of-fame': 'history',
  'history/hall': 'history',
  fantasycast: 'fantasycast',
  stats: 'players',
  players: 'players',
  'players/stats': 'players',
  transactions: 'transactions',
  'players/transactions': 'transactions',
  trades: 'trades',
  'trade-repository': 'trades',
  'trades/history': 'trades',
  'players/trades': 'trades',
  disparities: 'disparities',
  'players/disparities': 'disparities',
  'keeper-prices': 'keepers',
  'keeper-pricing': 'keepers',
  valuations: 'valuations',
  pricing: 'keepers',
  draft: 'draft',
  'draft/lobby': 'draft',
  'draft/tiers': 'draftprep',
  'draft/prep': 'draftprep',
  'draft-prep': 'draftprep',
  tiers: 'draftprep',
  'draft/history': 'drafthistory',
  'draft-history': 'drafthistory',
  drafthistory: 'drafthistory',
  pickem: 'pickem',
  'draft-capital': 'capital',
  capital: 'capital',
  'draft/traded-board': 'capital',
  'draft/ledgers': 'capital',
  'draft/trade-proposals': 'capital',
  'draft/proposals': 'capital',
  'draft/trade-history': 'capital',
  'draft/trades': 'capital',
  'draft/2026-board': 'capital',
  'draft/2026': 'capital',
  'keepers-budgets': 'keepers',
  keepers: 'keepers',
  'keepers/prices': 'keepers',
  'keepers/rosters': 'keepers',
  'keepers/comp-picks': 'keepers',
  'keepers/planner': 'keepers',
  'keepers/calculations': 'keepers',
  'keepers/math': 'keepers',
  'keepers/settings': 'keepers',
  'keeper-settings': 'keepers',
  'keeper-calculations': 'keepers',
  'keeper-calc': 'keepers',
  'keeper-math': 'keepers',
  calculations: 'keepers',
  'pr-math': 'keepers',
  budgets: 'keepers',
};

export const OFFSEASON_VIEWS = new Set(['valuations', 'draft', 'pickem', 'capital', 'keepers', 'drafthistory', 'draftprep']);

/**
 * Parses the view identifier from the current window hash, safely handling OAuth tokens.
 * @returns {string} View identifier
 */
export function getViewFromHash() {
  if (typeof window === 'undefined') return 'dashboard';
  const rawHash = window.location.hash || '';
  if (rawHash.includes('access_token=') || rawHash.includes('refresh_token=') || rawHash.includes('error=')) {
    try {
      const saved = sessionStorage.getItem('oauth_pre_login_hash');
      if (saved) {
        const cleanSaved = saved.replace(/^#\/?/, '').trim().toLowerCase();
        const route = cleanSaved.split('?')[0];
        if (HASH_TO_VIEW[route]) return HASH_TO_VIEW[route];
        const baseRoute = route.split('/')[0];
        if (HASH_TO_VIEW[baseRoute]) return HASH_TO_VIEW[baseRoute];
      }
    } catch {
      // ignore
    }
    return 'dashboard';
  }
  const hash = rawHash.replace(/^#\/?/, '').trim().toLowerCase();
  const route = hash.split('?')[0];
  if (HASH_TO_VIEW[route]) return HASH_TO_VIEW[route];
  const baseRoute = route.split('/')[0];
  return HASH_TO_VIEW[baseRoute] || 'dashboard';
}

/**
 * Extracts sub-tab states for complex views from the current window hash.
 * @returns {{ keepersSubTab: string, draftSubTab: string, teamsSubTab: string, playerOwnerMode: string, historySubTab: string }}
 */
export function getSubTabsFromHash() {
  if (typeof window === 'undefined') {
    return {
      keepersSubTab: 'matrix',
      draftSubTab: 'board',
      teamsSubTab: 'raw',
      playerOwnerMode: 'season',
      historySubTab: 'standings',
    };
  }
  const rawHash = window.location.hash || '';
  const hash = rawHash.replace(/^#\/?/, '').trim().toLowerCase();
  let keepersSubTab = 'matrix';
  let draftSubTab = 'board';
  let teamsSubTab = 'raw';
  let playerOwnerMode = 'season';

  if (hash.includes('rosters') && !hash.includes('teams')) keepersSubTab = 'rosters';
  else if (hash.includes('comp') || (hash.includes('simulator') && hash.includes('keepers'))) keepersSubTab = 'simulator';
  else if (hash.includes('planner')) keepersSubTab = 'planner';
  else if (hash.includes('calc') || hash.includes('math')) keepersSubTab = 'calculations';
  else if (hash.includes('settings')) keepersSubTab = 'settings';
  else if (hash.includes('prices') || hash.includes('matrix') || hash.includes('summary')) keepersSubTab = 'matrix';

  if (hash.includes('proposal')) draftSubTab = 'proposals';
  else if (hash.includes('ledgers')) draftSubTab = 'ledgers';
  else if (hash.includes('trade-history') || hash.includes('trades')) draftSubTab = 'history';
  else if (hash.includes('2026')) draftSubTab = '2026board';
  else if (hash.includes('traded') || hash.includes('board') || hash.includes('capital')) draftSubTab = 'board';

  if (hash.includes('best-lineup') || hash.includes('optimal')) teamsSubTab = 'best-lineup';
  else if (hash.includes('what-if') || hash.includes('teams/simulator') || hash.includes('impact') || hash === 'simulator' || hash === 'what-if') teamsSubTab = 'simulator';
  else if (hash.includes('bench')) teamsSubTab = 'bench';
  else if (hash.includes('gap')) teamsSubTab = 'gap';
  else if (hash.includes('minutiae')) teamsSubTab = 'minutiae';
  else if (hash.includes('roto')) teamsSubTab = 'roto';

  if (hash.includes('career') || hash.includes('all-time')) playerOwnerMode = 'career';

  let historySubTab = 'standings';
  if (hash.includes('history/highlight') || hash.includes('history-highlight')) historySubTab = 'highlights';
  else if (hash.includes('history/record') || hash.includes('history/hall') || hash.includes('franchise-hall')) historySubTab = 'records';
  else if (hash.includes('history/standing') || hash.includes('finis')) historySubTab = 'standings';

  return { keepersSubTab, draftSubTab, teamsSubTab, playerOwnerMode, historySubTab };
}

/**
 * Calculates the canonical URL hash for a view and its active sub-tabs.
 * @param {string} currentView
 * @param {object} subTabs
 * @returns {string} URL hash string with leading #/
 */
export function getHashForRoute(currentView, { keepersSubTab, draftSubTab, teamsSubTab, historySubTab } = {}) {
  let slug = VIEW_TO_HASH[currentView] || currentView;
  if (currentView === 'keepers') {
    if (keepersSubTab === 'rosters') slug = 'keepers/rosters';
    else if (keepersSubTab === 'simulator') slug = 'keepers/comp-picks';
    else if (keepersSubTab === 'planner') slug = 'keepers/planner';
    else if (keepersSubTab === 'calculations') slug = 'keeper-calculations';
    else if (keepersSubTab === 'settings') slug = 'keepers/settings';
    else slug = 'keepers/summary';
  } else if (currentView === 'capital') {
    if (draftSubTab === 'proposals') slug = 'draft/trade-proposals';
    else if (draftSubTab === 'ledgers') slug = 'draft/ledgers';
    else if (draftSubTab === 'history') slug = 'draft/trade-history';
    else if (draftSubTab === '2026board') slug = 'draft/2026-board';
    else slug = 'draft/traded-board';
  } else if (currentView === 'history') {
    if (historySubTab === 'highlights') slug = 'history/highlights';
    else if (historySubTab === 'records') slug = 'history/records';
    else slug = 'history/standings';
  } else if (currentView === 'drafthistory') {
    slug = 'draft/history';
  } else if (currentView === 'draftprep') {
    slug = 'draft/tiers';
  } else if (currentView === 'teams') {
    if (teamsSubTab === 'simulator') slug = 'teams/what-if';
    else if (teamsSubTab === 'bench') slug = 'teams/bench';
    else if (teamsSubTab === 'gap') slug = 'teams/gap';
    else if (teamsSubTab === 'minutiae') slug = 'teams/minutiae';
    else if (teamsSubTab === 'roto') slug = 'teams/roto';
    else slug = 'teams';
  }

  return `#/${slug}`;
}
