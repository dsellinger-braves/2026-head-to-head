# Graph Report - 2026 Head to Head Heftystrong  (2026-10-12)

## Corpus Check
- 208 files · ~2,445,467 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 10 file(s) not represented in the graph (top: (none) 5, .toml 2, .css 2)

## Summary
- 1312 nodes · 2959 edges · 75 communities (65 shown, 10 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 17 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `6194876c`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- discord-daily-recap.py
- discord-bot.py
- test_trade_and_recommender.mjs
- historyFinishesService.js
- run_pipeline
- projections.py
- aggregateStats
- TeamsView.jsx
- build_mlb_live_data
- package.json
- compute_player_values.py
- historical.py
- AGENTS.md: Architecture & Developer Guidelines
- os
- FullRosterView.jsx
- KeepersBudgetsPanel.jsx
- update_live_pickem_standings.py
- ingest_historical_trades.py
- draftCalculations.js
- league.js
- useSeasonData.js
- devDependencies
- update_league_context.py
- HighlightsView.jsx
- 5. Team Retrospectives & Verified 2026 Player Anchors
- LiveScoreboardView.jsx
- live_command
- KeepersBudgetsView.jsx
- DraftCapitalView.jsx
- is_season_active
- PickemView.jsx
- react
- migrate_gcs_to_supabase.py
- App.jsx
- ingest_draft_infrastructure.py
- sync_player_news.py
- SCORING_CATS
- build_live_roster_map
- What You Must Do When Invoked
- scripts
- inspect_db.py
- deploy
- index.ts
- ref_fs
- getPlayerHeadshotUrl
- graphify reference: extra exports and benchmark
- dependencies
- ingest_pickem.py
- vite.config.js
- overrides
- sync_draft_pool.py
- graphify reference: query, path, explain
- AuthContext.jsx
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- rules/graphify.md
- extraction-spec.md
- workflows/graphify.md
- datetime
- draftAudio.js
- Python Execution & Scripting Rules
- OwnerLandingView.jsx
- DraftRoomView.jsx
- src_schedule_teams
- rosterOptimizer.js
- useAuth
- draftRotoEngine.js
- DraftPrepView.jsx
- supabaseClient.js
- build_recap_context.py

## God Nodes (most connected - your core abstractions)
1. `react` - 88 edges
2. `aggregateStats()` - 37 edges
3. `DraftRoomView()` - 32 edges
4. `build_context()` - 30 edges
5. `TEAMS` - 28 edges
6. `normalizeOwner()` - 27 edges
7. `TeamAvatar()` - 24 edges
8. `getPlayerHeadshotUrl()` - 24 edges
9. `handleHeadshotError()` - 24 edges
10. `run_weekly_recap()` - 23 edges

## Surprising Connections (you probably didn't know these)
- `build_context()` --calls--> `format_all_active_owner_summaries()`  [EXTRACTED]
  discord-bot.py → historical.py
- `build_context()` --calls--> `format_league_champions()`  [EXTRACTED]
  discord-bot.py → historical.py
- `build_context()` --calls--> `format_owner_history()`  [EXTRACTED]
  discord-bot.py → historical.py
- `build_context()` --calls--> `build_projection_map()`  [EXTRACTED]
  discord-bot.py → projections.py
- `build_context()` --calls--> `fetch_projections()`  [EXTRACTED]
  discord-bot.py → projections.py

## Import Cycles
- None detected.

## Communities (75 total, 10 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.08
Nodes (55): aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates(), espn_ip_to_innings() (+47 more)

### Community 1 - "discord-bot.py"
Cohesion: 0.09
Nodes (41): asyncio, before_loop, discord, aggregate_by_player(), aggregate_by_team(), before_check_trade_notifications(), before_season_sentinel_loop(), build_context() (+33 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.07
Nodes (36): ref_node_assert, src_data_keeperinput2026, findWaiverReplacements(), isPositionMatch(), SLOT_TO_POS, calculateBudgetValue(), calculatePickValue(), calculatePlayerStatValue() (+28 more)

### Community 3 - "historyFinishesService.js"
Cohesion: 0.07
Nodes (38): LeagueHistoryView, PlayerValuationsView, normalizeOwner(), src_data_historicalrawstats, src_data_league_context, ALL_COUNTING_STATS, BAT_COUNTING_STATS, getRosterMultiplier() (+30 more)

### Community 4 - "run_pipeline"
Cohesion: 0.07
Nodes (20): calculate_category_benchmarks(), compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank(), get_supabase_headers(), load_pricing_curve() (+12 more)

### Community 5 - "projections.py"
Cohesion: 0.11
Nodes (26): build_projection_map(), _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), fetch_projections(), _fg_get(), forecast_final_standings(), forecast_ros_only_standings() (+18 more)

### Community 6 - "aggregateStats"
Cohesion: 0.11
Nodes (27): OwnerDisparitiesView, PlayerHistoryModal, WeeklyView, BATTER_CATS, BoxScoreModal(), formatDisplayVal(), PITCHER_CATS, MatchupCard() (+19 more)

### Community 7 - "TeamsView.jsx"
Cohesion: 0.16
Nodes (17): OwnerDetailModal, TeamsView, OwnerDetailModal(), getStatMeta(), MINUTIAE_STATS, src_utils_scoring_scoring_cats, computeDefendingBehindCushion(), computeTargetAheadGap() (+9 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.11
Nodes (19): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _mlb_get(), _mlb_ip_to_decimal() (+11 more)

### Community 9 - "package.json"
Cohesion: 0.11
Nodes (19): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+11 more)

### Community 10 - "compute_player_values.py"
Cohesion: 0.11
Nodes (21): json, math, pipelines/compute_player_values.py Automated Player Valuation & Pricing Engine…, pipelines/generate_keeper_calculations.py Generates detailed multi-year keeper…, requests, sys, enrich_player_names(), fetch_activity_trades() (+13 more)

### Community 11 - "historical.py"
Cohesion: 0.20
Nodes (13): _canonical(), format_all_active_owner_summaries(), format_league_champions(), format_owner_history(), get_owner_seasons(), load_historical_data(), historical.py — shared historical context module Import this in discord-bot.py…, Championship counts for all active owners. (+5 more)

### Community 12 - "AGENTS.md: Architecture & Developer Guidelines"
Cohesion: 0.04
Nodes (43): 1. `player_daily_stats`, 1. System Overview, 2. League & Configuration Ground Truth, 2. `transactions`, 3. Component Breakdown, 3. `historical_data`, 4. Database Schema (Supabase PostgreSQL), 5. Development & GitHub Guidelines for Agents (+35 more)

### Community 13 - "os"
Cohesion: 0.15
Nodes (8): dotenv, google_cloud, io, migrate_table(), Client, os, pandas, supabase

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.18
Nodes (14): FullRosterView, src_data_historicaltrades, src_data_transactions2026, getPlayerAcquisition(), getTeamDisplayName(), BATTER_SLOT_ORDER, formatOBP(), formatRate() (+6 more)

### Community 15 - "KeepersBudgetsPanel.jsx"
Cohesion: 0.11
Nodes (24): KeepersHeaderNav(), calculateKeeperCostFromRank(), CANONICAL_FINISHES, COMP_BUY_PRICES_2026, COMP_BUY_PRICES_2027, COMP_SELL_PRICES_2026, COMP_SELL_PRICES_2027, DRAFT_MANAGERS (+16 more)

### Community 16 - "update_live_pickem_standings.py"
Cohesion: 0.26
Nodes (14): build_live_in_progress_snapshot(), evaluate_owner_projected_scores(), fetch_mlb_standings(), fetch_projected_war_leaders(), get_team_code(), matches_team_or_val(), normalize_text(), Any (+6 more)

### Community 17 - "ingest_historical_trades.py"
Cohesion: 0.08
Nodes (37): collections, concurrent_futures, aggregate_season(), calculate_fantasy_points(), fetch_2020_season(), fetch_2026_supabase(), fetch_espn_sp(), fetch_season_from_espn() (+29 more)

### Community 18 - "draftCalculations.js"
Cohesion: 0.20
Nodes (14): DraftCapitalPanel(), MyPicksPanel(), fetchFanGraphs(), calcIndexById, calcIndexByName, compute2027DraftPicks(), generate2027DraftOrder(), generateDefaultDraftOrder() (+6 more)

### Community 19 - "league.js"
Cohesion: 0.12
Nodes (18): ACTIVE_OWNERS, ACTIVE_TEAM_IDS, AVAILABLE_SEASONS, CANONICAL_DRAFT_OWNERS, CAT_ICONS, CATEGORIES, CATEGORY_DETAILS, CURRENT_SEASON (+10 more)

### Community 20 - "useSeasonData.js"
Cohesion: 0.20
Nodes (12): idb-keyval, App(), LEAGUE_ID, normalizeHistoricalRecord(), useSeasonData(), AVAILABLE_SEASONS, getHashForRoute(), getSubTabsFromHash() (+4 more)

### Community 21 - "devDependencies"
Cohesion: 0.14
Nodes (14): devDependencies, autoprefixer, eslint, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, gh-pages, globals (+6 more)

### Community 22 - "update_league_context.py"
Cohesion: 0.18
Nodes (14): calculate_roto_points(), compute_playoff_matchups(), compute_rates(), fetch_supabase_table(), get_period_map(), get_stat(), is_season_active(), Computes live head-to-head category scores for Playoff Semi-Finals (Week 24)… (+6 more)

### Community 23 - "HighlightsView.jsx"
Cohesion: 0.14
Nodes (17): HighlightsView, BAT_CATS, BATTER_SLOT_ORDER, DayRosterModal(), formatVal(), PITCH_CATS, PITCHER_SLOT_ORDER, StatCell() (+9 more)

### Community 24 - "5. Team Retrospectives & Verified 2026 Player Anchors"
Cohesion: 0.05
Nodes (38): 1. Dan — The Silver Bullets (`Team 5`), 1. Executive Summary & The 2026 Storyline, 1. The Six-Way Fractional OBP Dogfight (.003 Margin), 2026 Category Champions, 2026 League Storylines & Season Context Guide, 2. Final 2026 Regular Season Standings (Week 23), 2. The Heavyweight Home Run Jam (21-Homer Window), 2. Tim — Anti-lock Brake Systems (`Team 1`) (+30 more)

### Community 25 - "LiveScoreboardView.jsx"
Cohesion: 0.42
Nodes (7): LiveScoreboardView, GameDetailModal(), buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard(), normalizeName(), LiveScoreboardView()

### Community 26 - "live_command"
Cohesion: 0.16
Nodes (15): command, describe, ask_command(), build_mlb_live_embed_fields(), check_trades_command(), current_scoring_period(), _game_field_value(), generate_answer() (+7 more)

### Community 27 - "KeepersBudgetsView.jsx"
Cohesion: 0.14
Nodes (14): KeepersBudgetsView, src_data_compensationpicks2026, src_data_keepercalculations, src_data_keeperinput2027, src_data_teambudgets2026, src_data_teambudgets2027, updateGlobalPlayerLookup(), KeeperCalculationsView() (+6 more)

### Community 28 - "DraftCapitalView.jsx"
Cohesion: 0.12
Nodes (20): DraftCapitalView, DRAFT_OWNERS, getTeamId(), LEAGUE_OWNERS_NORMALIZED, src_data_draftassettrades2026, DraftCapitalHeader(), TradeEditModal(), TradeProposalModal() (+12 more)

### Community 29 - "is_season_active"
Cohesion: 0.12
Nodes (13): check_trade_notifications(), HEFTYBot, handle_ping(), is_season_active(), offseason_sleep_service(), date, Checks Supabase for pending trade lifecycle notifications and delivers DMs., Daily check to disconnect the bot if regular season has concluded. (+5 more)

### Community 30 - "PickemView.jsx"
Cohesion: 0.13
Nodes (18): PickemView, LEAGUE_OWNERS, MLB_DIVISIONS, MLB_TEAMS, PICKEM_RULES, PROMINENT_AWARD_CANDIDATES, teamsMatch(), PickemHeader() (+10 more)

### Community 31 - "react"
Cohesion: 0.18
Nodes (16): react, calculateRotoPoints(), PlayerImpactSimulatorView(), SimulatorHeader(), SimulatorReplacementControls(), SimulatorRotoSummary(), SimulatorStandingsRipple(), BATTING_CATS (+8 more)

### Community 32 - "migrate_gcs_to_supabase.py"
Cohesion: 0.39
Nodes (8): fetch_json(), main(), migrate_draft_history(), migrate_historical_finishes(), normalize_name(), Client, pipelines/migrate_gcs_to_supabase.py Migrates historical draft picks (draft-…, unicodedata

### Community 33 - "App.jsx"
Cohesion: 0.09
Nodes (26): PlayerOwnerStatsView, ProgressionView, SummaryView, TradeRepositoryView, TransactionsView, TeamAvatar(), TEAMS, src_data_playerownerseasonstats (+18 more)

### Community 34 - "ingest_draft_infrastructure.py"
Cohesion: 0.17
Nodes (18): csv, clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert(), pipelines/ingest_draft_infrastructure.py Ingests: 1. Compensation Picks & Team… (+10 more)

### Community 35 - "sync_player_news.py"
Cohesion: 0.27
Nodes (9): argparse, fetch_player_news(), load_player_pool(), main(), Any, pipelines/sync_player_news.py Syncs live player news directly from the ESPN…, Loads player pool from local file, Supabase, or GCS., Fetches news feed for an individual ESPN player ID. (+1 more)

### Community 36 - "SCORING_CATS"
Cohesion: 0.32
Nodes (7): PlayersView, SCORING_CATS, BAT_COLS, formatStat(), getLabel(), PITCH_COLS, PlayersView()

### Community 37 - "build_live_roster_map"
Cohesion: 0.18
Nodes (12): build_live_roster_map(), build_roster_name_map(), fetch_current_roster_records(), fetch_espn_live_rosters(), _match_player(), normalize_name(), Lowercase, strip punctuation and suffixes for fuzzy matching., Build {normalized_name: {owner, team_id, full_name}} from a set of records.… (+4 more)

### Community 38 - "What You Must Do When Invoked"
Cohesion: 0.08
Nodes (24): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+16 more)

### Community 39 - "scripts"
Cohesion: 0.25
Nodes (8): scripts, build, deploy, dev, lint, predeploy, preview, start

### Community 40 - "inspect_db.py"
Cohesion: 0.53
Nodes (8): Namespace, cmd_budgets(), cmd_keepers(), cmd_pool(), cmd_roster(), get_headers(), main(), scripts/inspect_db.py Safe, structured CLI inspector for Supabase database…

### Community 41 - "deploy"
Cohesion: 0.25
Nodes (7): build, builder, deploy, restartPolicyMaxRetries, restartPolicyType, startCommand, $schema

### Community 42 - "index.ts"
Cohesion: 0.25
Nodes (4): ref_https, PlayerRecord, SEASON_START, TEAM_IDS

### Community 44 - "getPlayerHeadshotUrl"
Cohesion: 0.22
Nodes (15): renderPlayerStatBadges(), TierBoard(), getPlayerHeadshotUrl(), globalPlayerLookup, handleHeadshotError(), TAG_OPTIONS, DraftLogPanel(), DraftAssistantWidget() (+7 more)

### Community 45 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 47 - "ingest_pickem.py"
Cohesion: 0.26
Nodes (11): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), pipelines/ingest_pickem.py Annual MLB Pick'em Ingestion & Management Pipeline…, Fetch raw CSV rows for a tab from Google Sheets. (+3 more)

### Community 52 - "sync_draft_pool.py"
Cohesion: 0.36
Nodes (9): extract_player_record(), fetch_espn_players(), get_current_season(), main(), normalize_name(), Any, Client, pipelines/sync_draft_pool.py Automated replacement for fantasy-baseball… (+1 more)

### Community 53 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 54 - "AuthContext.jsx"
Cohesion: 0.29
Nodes (7): react-dom, STATIC_LEAGUE_PROFILES, AuthProvider(), initAuth(), cleanupOAuthHash(), AuthContext, src_index

### Community 55 - "graphify reference: add a URL and watch a folder"
Cohesion: 0.50
Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder

### Community 56 - "graphify reference: commit hook and native CLAUDE.md integration"
Cohesion: 0.50
Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration

### Community 57 - "graphify reference: incremental update and cluster-only"
Cohesion: 0.50
Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only

### Community 63 - "datetime"
Cohesion: 0.17
Nodes (11): datetime, check_game_status(), get_periods_to_focus(), log_status(), date, mlb_schedule_check.py ───────────────────── Drop-in module for active-stats-…, Print a human-readable summary of today's game status., Decide whether the scraper should proceed. mode="force" — always run regardless… (+3 more)

### Community 64 - "draftAudio.js"
Cohesion: 0.18
Nodes (26): getFriendlyOwnerName(), findMlbTeam(), AnalysisHistoryPanel(), calculatePickGrade(), callGemini(), currentPodcastAudios, determinePickAngle(), formatDraftHistorySummary() (+18 more)

### Community 65 - "Python Execution & Scripting Rules"
Cohesion: 0.17
Nodes (11): 1. Avoid Inline `python3 -c '...'` for Complex or Interpolated Code, 2. Best Practices for Dictionary Indexing in F-Strings, 3. Python Module Path Constraints, 4. Zero Speculative / Blind File Access, 5. Canonical Data Files in `src/data/`, Operational Rules:, Python Execution & Scripting Rules, Required Pattern A: Quoted Heredoc (`python3 - << 'EOF'`) (+3 more)

### Community 66 - "OwnerLandingView.jsx"
Cohesion: 0.28
Nodes (8): OwnerLandingView, evaluatePlayerCapital(), DEFAULT_POS_MAP, getPlayerPositions(), LEAGUE_MANAGERS, MLB_TEAMS, OwnerLandingView(), SLOT_MAP

### Community 67 - "DraftRoomView.jsx"
Cohesion: 0.16
Nodes (19): DraftRoomView, ROSTER_SLOTS, DesktopHeader(), CountdownTimer(), OnDeckSidebar(), PositionalRunRadar(), QueuePreviewWidget(), RecentActivityWidget() (+11 more)

### Community 68 - "src_schedule_teams"
Cohesion: 0.23
Nodes (11): CANONICAL_OWNERS, src_data_historicalfinishes, getFranchiseLeaderboard(), getHistoricalSeasons(), src_data_historicalfranchisedata_normalizeowner, src_schedule_teams, ALL_TIME_METRICS, formatVal() (+3 more)

### Community 69 - "rosterOptimizer.js"
Cohesion: 0.33
Nodes (10): BATTER_SLOT_DEFS, buildPlayerPositionRegistry(), getDailyBatterScore(), getDailyPitcherScore(), optimizeDailyTeamLineup(), PITCHER_SLOT_DEFS, simulateSeasonBestLineups(), solveOptimalAssignment() (+2 more)

### Community 70 - "useAuth"
Cohesion: 0.54
Nodes (4): CommishActiveBanner(), CommishCheckoutModal(), UserNavWidget(), useAuth()

### Community 71 - "draftRotoEngine.js"
Cohesion: 0.21
Nodes (17): calcIndexById, calcIndexByName, CATEGORIES, computeLiveDraftRoto(), detectPositionalRun(), DRAFT_MANAGERS, formatRotoStat(), getPlayerProjectionStats() (+9 more)

### Community 72 - "DraftPrepView.jsx"
Cohesion: 0.32
Nodes (16): DraftPrepView, assignPlayerTier(), DEFAULT_TIERS, generateAutoTiers(), getTiersStorageKey(), loadUserQueue(), loadUserTiers(), saveUserQueue() (+8 more)

### Community 74 - "supabaseClient.js"
Cohesion: 0.16
Nodes (18): DraftHistoryView, src_data_draft2026, DEFAULT_SUPABASE_ANON_KEY, DEFAULT_SUPABASE_URL, supabase, supabaseUrl, PlayerModal(), fetchPlayerData() (+10 more)

### Community 75 - "build_recap_context.py"
Cohesion: 0.20
Nodes (11): detect_active_roto_battles(), ensure_context_is_fresh(), fmt_cat_val(), load_league_context(), Format a category stat value cleanly., Dynamically identify active category volatility and standings deadlocks where…, pipelines/build_recap_context.py Fetches real-time MLB news from ESPN and…, Ensure data/league_context.json is updated from Supabase if needed. (+3 more)

## Knowledge Gaps
- **268 isolated node(s):** `name`, `private`, `version`, `homepage`, `type` (+263 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 499 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `historyFinishesService.js`, `aggregateStats`, `TeamsView.jsx`, `package.json`, `FullRosterView.jsx`, `KeepersBudgetsPanel.jsx`, `draftCalculations.js`, `league.js`, `useSeasonData.js`, `HighlightsView.jsx`, `LiveScoreboardView.jsx`, `KeepersBudgetsView.jsx`, `DraftCapitalView.jsx`, `PickemView.jsx`, `App.jsx`, `SCORING_CATS`, `getPlayerHeadshotUrl`, `AuthContext.jsx`, `draftAudio.js`, `OwnerLandingView.jsx`, `DraftRoomView.jsx`, `src_schedule_teams`, `rosterOptimizer.js`, `useAuth`, `draftRotoEngine.js`, `DraftPrepView.jsx`, `supabaseClient.js`?**
  _High betweenness centrality (0.110) - this node is a cross-community bridge._
- **Why does `normalizeOwner()` connect `historyFinishesService.js` to `draftAudio.js`, `src_schedule_teams`, `DraftPrepView.jsx`, `KeepersBudgetsPanel.jsx`, `draftCalculations.js`, `league.js`, `DraftCapitalView.jsx`?**
  _High betweenness centrality (0.011) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.010) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `build_context()` (e.g. with `ask_command()` and `.on_message()`) actually correct?**
  _`build_context()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `name`, `private`, `version` to the rest of the system?**
  _268 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.07622504537205081 - nodes in this community are weakly interconnected._
- **Should `discord-bot.py` be split into smaller, more focused modules?**
  _Cohesion score 0.0859465737514518 - nodes in this community are weakly interconnected._