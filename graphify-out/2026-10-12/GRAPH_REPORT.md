# Graph Report - 2026 Head to Head Heftystrong  (2026-10-12)

## Corpus Check
- 199 files · ~2,435,571 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 10 file(s) not represented in the graph (top: (none) 5, .toml 2, .css 2)

## Summary
- 1267 nodes · 2767 edges · 69 communities (59 shown, 10 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 17 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `7201aa02`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- discord-daily-recap.py
- discord-bot.py
- test_trade_and_recommender.mjs
- historyFinishesService.js
- compute_player_values.py
- projections.py
- react
- aggregateStats
- build_mlb_live_data
- package.json
- transaction-scraper.py
- historical.py
- AGENTS.md: Architecture & Developer Guidelines
- supabaseClient.js
- FullRosterView.jsx
- KeepersBudgetsPanel.jsx
- update_live_pickem_standings.py
- ingest_historical_trades.py
- HighlightsView.jsx
- league.js
- useSeasonData.js
- devDependencies
- update_league_context.py
- sync_draft_pool.py
- 5. Team Retrospectives & Verified 2026 Player Anchors
- TeamAvatar.jsx
- live_command
- OwnerDisparitiesView.jsx
- DraftCapitalView.jsx
- is_season_active
- PickemView.jsx
- TransactionsView.jsx
- os
- schedule.js
- ingest_draft_infrastructure.py
- sync_player_news.py
- AuthContext.jsx
- build_live_roster_map
- What You Must Do When Invoked
- scripts
- inspect_db.py
- deploy
- index.ts
- ref_fs
- build_recap_context.py
- graphify reference: extra exports and benchmark
- dependencies
- ingest_pickem.py
- vite.config.js
- overrides
- App.jsx
- graphify reference: query, path, explain
- useAuth
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- rules/graphify.md
- extraction-spec.md
- workflows/graphify.md
- active-stats-pull.py
- Python Execution & Scripting Rules
- DraftRoomView.jsx
- ProgressionView.jsx
- getDateFromPeriodId
- PlayersView.jsx

## God Nodes (most connected - your core abstractions)
1. `react` - 80 edges
2. `aggregateStats()` - 35 edges
3. `build_context()` - 30 edges
4. `TEAMS` - 28 edges
5. `DraftRoomView()` - 28 edges
6. `TeamAvatar()` - 24 edges
7. `normalizeOwner()` - 24 edges
8. `run_weekly_recap()` - 23 edges
9. `getPlayerHeadshotUrl()` - 20 edges
10. `handleHeadshotError()` - 20 edges

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

## Communities (69 total, 10 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.08
Nodes (55): aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates(), espn_ip_to_innings() (+47 more)

### Community 1 - "discord-bot.py"
Cohesion: 0.09
Nodes (41): asyncio, before_loop, discord, aggregate_by_player(), aggregate_by_team(), before_check_trade_notifications(), before_season_sentinel_loop(), build_context() (+33 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.07
Nodes (38): ref_node_assert, evaluatePlayerCapital(), findWaiverReplacements(), isPositionMatch(), SLOT_TO_POS, calculateBudgetValue(), calculatePickValue(), calculatePlayerStatValue() (+30 more)

### Community 3 - "historyFinishesService.js"
Cohesion: 0.08
Nodes (38): LeagueHistoryView, CANONICAL_OWNERS, normalizeOwner(), src_data_historicalrawstats, src_data_league_context, ALL_COUNTING_STATS, BAT_COUNTING_STATS, getRosterMultiplier() (+30 more)

### Community 4 - "compute_player_values.py"
Cohesion: 0.07
Nodes (27): json, math, calculate_category_benchmarks(), compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank() (+19 more)

### Community 5 - "projections.py"
Cohesion: 0.11
Nodes (26): build_projection_map(), _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), fetch_projections(), _fg_get(), forecast_final_standings(), forecast_ros_only_standings() (+18 more)

### Community 6 - "react"
Cohesion: 0.12
Nodes (21): react, BATTER_CATS, BoxScoreModal(), formatDisplayVal(), PITCHER_CATS, BAT_CATS, BATTER_SLOT_ORDER, formatVal() (+13 more)

### Community 7 - "aggregateStats"
Cohesion: 0.12
Nodes (28): OwnerDetailModal, TeamsView, OwnerDetailModal(), aggregateBenchStats(), aggregateStats(), calculateRotoPoints(), getStatMeta(), MINUTIAE_STATS (+20 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.11
Nodes (19): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _mlb_get(), _mlb_ip_to_decimal() (+11 more)

### Community 9 - "package.json"
Cohesion: 0.12
Nodes (18): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+10 more)

### Community 10 - "transaction-scraper.py"
Cohesion: 0.12
Nodes (15): time, enrich_player_names(), fetch_activity_trades(), fetch_all_player_names(), fetch_historical_transactions(), fetch_transactions(), parse_activity_trades(), parse_standard_transactions() (+7 more)

### Community 11 - "historical.py"
Cohesion: 0.20
Nodes (13): _canonical(), format_all_active_owner_summaries(), format_league_champions(), format_owner_history(), get_owner_seasons(), load_historical_data(), historical.py — shared historical context module Import this in discord-bot.py…, Championship counts for all active owners. (+5 more)

### Community 12 - "AGENTS.md: Architecture & Developer Guidelines"
Cohesion: 0.04
Nodes (43): 1. `player_daily_stats`, 1. System Overview, 2. League & Configuration Ground Truth, 2. `transactions`, 3. Component Breakdown, 3. `historical_data`, 4. Database Schema (Supabase PostgreSQL), 5. Development & GitHub Guidelines for Agents (+35 more)

### Community 13 - "supabaseClient.js"
Cohesion: 0.20
Nodes (6): @supabase/supabase-js, PlayerValuationsView, DEFAULT_SUPABASE_ANON_KEY, DEFAULT_SUPABASE_URL, supabase, supabaseUrl

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.21
Nodes (12): FullRosterView, getPlayerAcquisition(), getTeamDisplayName(), BATTER_SLOT_ORDER, formatOBP(), formatRate(), FullRosterView(), getBatterDailyScore() (+4 more)

### Community 15 - "KeepersBudgetsPanel.jsx"
Cohesion: 0.09
Nodes (30): KeepersBudgetsView, KeepersHeaderNav(), calculateKeeperCostFromRank(), CANONICAL_FINISHES, COMP_BUY_PRICES_2026, COMP_BUY_PRICES_2027, COMP_SELL_PRICES_2026, COMP_SELL_PRICES_2027 (+22 more)

### Community 16 - "update_live_pickem_standings.py"
Cohesion: 0.26
Nodes (14): build_live_in_progress_snapshot(), evaluate_owner_projected_scores(), fetch_mlb_standings(), fetch_projected_war_leaders(), get_team_code(), matches_team_or_val(), normalize_text(), Any (+6 more)

### Community 17 - "ingest_historical_trades.py"
Cohesion: 0.08
Nodes (36): collections, concurrent_futures, aggregate_season(), calculate_fantasy_points(), fetch_2020_season(), fetch_2026_supabase(), fetch_espn_sp(), fetch_season_from_espn() (+28 more)

### Community 18 - "HighlightsView.jsx"
Cohesion: 0.29
Nodes (10): HighlightsView, DayRosterModal(), calculateEraAdjustedStat(), formatHighlightVal(), getMlbMultiplier(), AVAILABLE_SEASONS, BAT_STATS, HighlightsView() (+2 more)

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

### Community 23 - "sync_draft_pool.py"
Cohesion: 0.36
Nodes (9): extract_player_record(), fetch_espn_players(), get_current_season(), main(), normalize_name(), Any, Client, pipelines/sync_draft_pool.py Automated replacement for fantasy-baseball… (+1 more)

### Community 24 - "5. Team Retrospectives & Verified 2026 Player Anchors"
Cohesion: 0.05
Nodes (38): 1. Dan — The Silver Bullets (`Team 5`), 1. Executive Summary & The 2026 Storyline, 1. The Six-Way Fractional OBP Dogfight (.003 Margin), 2026 Category Champions, 2026 League Storylines & Season Context Guide, 2. Final 2026 Regular Season Standings (Week 23), 2. The Heavyweight Home Run Jam (21-Homer Window), 2. Tim — Anti-lock Brake Systems (`Team 1`) (+30 more)

### Community 25 - "TeamAvatar.jsx"
Cohesion: 0.24
Nodes (11): TradeRepositoryView, GameDetailModal(), TeamAvatar(), src_data_historicaltrades, src_schedule_teams, buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard() (+3 more)

### Community 26 - "live_command"
Cohesion: 0.16
Nodes (15): command, describe, ask_command(), build_mlb_live_embed_fields(), check_trades_command(), current_scoring_period(), _game_field_value(), generate_answer() (+7 more)

### Community 27 - "OwnerDisparitiesView.jsx"
Cohesion: 0.36
Nodes (6): OwnerDisparitiesView, calculateBatterValue(), calculatePitcherValue(), cleanPlayerName(), mlbSeasonDataCache, OwnerDisparitiesView()

### Community 28 - "DraftCapitalView.jsx"
Cohesion: 0.14
Nodes (17): getTeamId(), src_data_draftassettrades2026, DraftCapitalHeader(), TradeEditModal(), TradeProposalModal(), canonicalOwnerName(), compute2027DraftPicks(), src_views_capital_services_tradenotificationservice_draft_owners (+9 more)

### Community 29 - "is_season_active"
Cohesion: 0.12
Nodes (13): check_trade_notifications(), HEFTYBot, handle_ping(), is_season_active(), offseason_sleep_service(), date, Checks Supabase for pending trade lifecycle notifications and delivers DMs., Daily check to disconnect the bot if regular season has concluded. (+5 more)

### Community 30 - "PickemView.jsx"
Cohesion: 0.12
Nodes (19): PickemView, LEAGUE_OWNERS, findMlbTeam(), MLB_DIVISIONS, MLB_TEAMS, PICKEM_RULES, PROMINENT_AWARD_CANDIDATES, teamsMatch() (+11 more)

### Community 31 - "TransactionsView.jsx"
Cohesion: 0.29
Nodes (6): TransactionsView, src_data_transactions2026, src_data_transactions_historical, groupTransactions(), TransactionsView(), TYPE_CONFIG

### Community 32 - "os"
Cohesion: 0.17
Nodes (13): dotenv, migrate_table(), Client, os, fetch_json(), main(), migrate_draft_history(), migrate_historical_finishes() (+5 more)

### Community 33 - "schedule.js"
Cohesion: 0.19
Nodes (13): SummaryView, TEAMS, useProcessedSchedule(), generateSchedule(), getPeriodRangeForWeek(), getTriosMatchups(), SEASON_DATES, SEASON_START_DATES (+5 more)

### Community 34 - "ingest_draft_infrastructure.py"
Cohesion: 0.17
Nodes (18): csv, clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert(), pipelines/ingest_draft_infrastructure.py Ingests: 1. Compensation Picks & Team… (+10 more)

### Community 35 - "sync_player_news.py"
Cohesion: 0.27
Nodes (9): argparse, fetch_player_news(), load_player_pool(), main(), Any, pipelines/sync_player_news.py Syncs live player news directly from the ESPN…, Loads player pool from local file, Supabase, or GCS., Fetches news feed for an individual ESPN player ID. (+1 more)

### Community 36 - "AuthContext.jsx"
Cohesion: 0.29
Nodes (7): react-dom, STATIC_LEAGUE_PROFILES, AuthProvider(), initAuth(), cleanupOAuthHash(), AuthContext, src_index

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

### Community 44 - "build_recap_context.py"
Cohesion: 0.20
Nodes (11): detect_active_roto_battles(), ensure_context_is_fresh(), fmt_cat_val(), load_league_context(), Format a category stat value cleanly., Dynamically identify active category volatility and standings deadlocks where…, pipelines/build_recap_context.py Fetches real-time MLB news from ESPN and…, Ensure data/league_context.json is updated from Supabase if needed. (+3 more)

### Community 45 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 47 - "ingest_pickem.py"
Cohesion: 0.26
Nodes (11): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), pipelines/ingest_pickem.py Annual MLB Pick'em Ingestion & Management Pipeline…, Fetch raw CSV rows for a tab from Google Sheets. (+3 more)

### Community 52 - "App.jsx"
Cohesion: 0.09
Nodes (17): DraftCapitalView, DraftHistoryView, DraftRoomView, LiveScoreboardView, OwnerLandingView, PlayerHistoryModal, PlayerOwnerStatsView, WeeklyView (+9 more)

### Community 53 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 54 - "useAuth"
Cohesion: 0.54
Nodes (4): CommishActiveBanner(), CommishCheckoutModal(), UserNavWidget(), useAuth()

### Community 55 - "graphify reference: add a URL and watch a folder"
Cohesion: 0.50
Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder

### Community 56 - "graphify reference: commit hook and native CLAUDE.md integration"
Cohesion: 0.50
Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration

### Community 57 - "graphify reference: incremental update and cluster-only"
Cohesion: 0.50
Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only

### Community 63 - "active-stats-pull.py"
Cohesion: 0.11
Nodes (14): datetime, google_cloud, io, check_game_status(), get_periods_to_focus(), log_status(), date, mlb_schedule_check.py ───────────────────── Drop-in module for active-stats-… (+6 more)

### Community 65 - "Python Execution & Scripting Rules"
Cohesion: 0.17
Nodes (11): 1. Avoid Inline `python3 -c '...'` for Complex or Interpolated Code, 2. Best Practices for Dictionary Indexing in F-Strings, 3. Python Module Path Constraints, 4. Zero Speculative / Blind File Access, 5. Canonical Data Files in `src/data/`, Operational Rules:, Python Execution & Scripting Rules, Required Pattern A: Quoted Heredoc (`python3 - << 'EOF'`) (+3 more)

### Community 67 - "DraftRoomView.jsx"
Cohesion: 0.05
Nodes (103): DRAFT_OWNERS, getFriendlyOwnerName(), OWNER_AVATARS, ROSTER_SLOTS, src_data_draft2026, src_data_keepercalculations, src_data_keeperinput2026, src_data_keeperinput2027 (+95 more)

### Community 68 - "ProgressionView.jsx"
Cohesion: 0.24
Nodes (10): ProgressionView, src_data_historicalfinishes, getFranchiseLeaderboard(), getHistoricalSeasons(), src_data_historicalfranchisedata_normalizeowner, ALL_TIME_METRICS, formatVal(), ProgressionView() (+2 more)

### Community 69 - "getDateFromPeriodId"
Cohesion: 0.31
Nodes (11): getDateFromPeriodId(), BATTER_SLOT_DEFS, buildPlayerPositionRegistry(), getDailyBatterScore(), getDailyPitcherScore(), optimizeDailyTeamLineup(), PITCHER_SLOT_DEFS, simulateSeasonBestLineups() (+3 more)

### Community 70 - "PlayersView.jsx"
Cohesion: 0.38
Nodes (6): PlayersView, BAT_COLS, formatStat(), getLabel(), PITCH_COLS, PlayersView()

## Knowledge Gaps
- **267 isolated node(s):** `name`, `private`, `version`, `homepage`, `type` (+262 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 497 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `historyFinishesService.js`, `aggregateStats`, `package.json`, `supabaseClient.js`, `FullRosterView.jsx`, `KeepersBudgetsPanel.jsx`, `HighlightsView.jsx`, `league.js`, `useSeasonData.js`, `TeamAvatar.jsx`, `OwnerDisparitiesView.jsx`, `DraftCapitalView.jsx`, `PickemView.jsx`, `TransactionsView.jsx`, `schedule.js`, `AuthContext.jsx`, `App.jsx`, `useAuth`, `DraftRoomView.jsx`, `ProgressionView.jsx`, `getDateFromPeriodId`, `PlayersView.jsx`?**
  _High betweenness centrality (0.109) - this node is a cross-community bridge._
- **Why does `PickemView()` connect `PickemView.jsx` to `useAuth`?**
  _High betweenness centrality (0.011) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.010) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `build_context()` (e.g. with `ask_command()` and `.on_message()`) actually correct?**
  _`build_context()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `name`, `private`, `version` to the rest of the system?**
  _267 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.07622504537205081 - nodes in this community are weakly interconnected._
- **Should `discord-bot.py` be split into smaller, more focused modules?**
  _Cohesion score 0.0859465737514518 - nodes in this community are weakly interconnected._