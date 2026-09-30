# Graph Report - 2026 Head to Head Heftystrong  (2026-09-30)

## Corpus Check
- 136 files · ~1,800,359 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 10 file(s) not represented in the graph (top: (none) 5, .toml 2, .css 2)

## Summary
- 1040 nodes · 2002 edges · 60 communities (50 shown, 10 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `7635967a`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- discord-daily-recap.py
- DraftRoomView.jsx
- test_trade_and_recommender.mjs
- LeagueHistoryView.jsx
- run_pipeline
- projections.py
- LiveScoreboardView.jsx
- aggregateStats
- build_mlb_live_data
- package.json
- compute_player_values.py
- discord-bot.py
- AGENTS.md: Architecture & Developer Guidelines
- App.jsx
- FullRosterView.jsx
- update_league_context.py
- update_live_pickem_standings.py
- ingest_historical_trades.py
- historical.py
- fetch_current_roster_records
- ingest_pickem.py
- devDependencies
- react
- sync_draft_pool.py
- 5. Team Retrospectives & Verified 2026 Player Anchors
- HEFTYBot
- live_command
- migrate_gcs_to_supabase.py
- ProgressionView.jsx
- is_season_active
- getDateFromPeriodId
- PlayersView.jsx
- os
- ingest_draft_infrastructure.py
- ingest_draft_trades.py
- TEAMS
- fetch_historical_transactions
- build_live_roster_map
- What You Must Do When Invoked
- scripts
- deploy
- index.ts
- ref_fs
- rosterOptimizer.js
- graphify reference: extra exports and benchmark
- dependencies
- vite
- overrides
- scoring.js
- graphify reference: query, path, explain
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- rules/graphify.md
- extraction-spec.md
- workflows/graphify.md

## God Nodes (most connected - your core abstractions)
1. `react` - 41 edges
2. `aggregateStats()` - 35 edges
3. `build_context()` - 30 edges
4. `TEAMS` - 27 edges
5. `run_weekly_recap()` - 23 edges
6. `TeamAvatar()` - 22 edges
7. `run_daily_recap()` - 19 edges
8. `useAuth()` - 17 edges
9. `getDateFromPeriodId()` - 17 edges
10. `DraftRoomView()` - 17 edges

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

## Communities (60 total, 10 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.06
Nodes (66): aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates(), espn_ip_to_innings() (+58 more)

### Community 1 - "DraftRoomView.jsx"
Cohesion: 0.06
Nodes (47): src_data_keepercalculations, getPlayerHeadshotUrl(), globalPlayerLookup, handleHeadshotError(), updateGlobalPlayerLookup(), callESPNProxy(), callGemini(), compute2027DraftPicks() (+39 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.06
Nodes (42): ref_node_assert, src_data_keeperinput2026, evaluatePlayerCapital(), findWaiverReplacements(), isPositionMatch(), SLOT_TO_POS, calculateBudgetValue(), calculatePickValue() (+34 more)

### Community 3 - "LeagueHistoryView.jsx"
Cohesion: 0.08
Nodes (38): idb-keyval, BAT_CATS, BATTER_SLOT_ORDER, DayRosterModal(), formatVal(), PITCH_CATS, PITCHER_SLOT_ORDER, StatCell() (+30 more)

### Community 4 - "run_pipeline"
Cohesion: 0.07
Nodes (20): calculate_category_benchmarks(), compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank(), get_supabase_headers(), load_pricing_curve() (+12 more)

### Community 5 - "projections.py"
Cohesion: 0.12
Nodes (25): build_projection_map(), _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), fetch_projections(), _fg_get(), forecast_final_standings(), forecast_ros_only_standings() (+17 more)

### Community 6 - "LiveScoreboardView.jsx"
Cohesion: 0.49
Nodes (6): GameDetailModal(), buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard(), normalizeName(), LiveScoreboardView()

### Community 7 - "aggregateStats"
Cohesion: 0.14
Nodes (25): OwnerDetailModal(), aggregateBenchStats(), aggregateStats(), calculateRotoPoints(), getStatMeta(), MINUTIAE_STATS, BenchStatsView(), ESPN_STAT_NAMES (+17 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.11
Nodes (19): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _mlb_get(), _mlb_ip_to_decimal() (+11 more)

### Community 9 - "package.json"
Cohesion: 0.13
Nodes (17): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+9 more)

### Community 10 - "compute_player_values.py"
Cohesion: 0.14
Nodes (17): json, math, pipelines/compute_player_values.py Automated Player Valuation & Pricing Engine…, pipelines/generate_keeper_calculations.py Generates detailed multi-year keeper…, requests, sys, enrich_player_names(), fetch_activity_trades() (+9 more)

### Community 11 - "discord-bot.py"
Cohesion: 0.12
Nodes (27): asyncio, before_loop, discord, aggregate_by_player(), before_check_trade_notifications(), before_season_sentinel_loop(), build_context(), fetch_recent_transactions() (+19 more)

### Community 12 - "AGENTS.md: Architecture & Developer Guidelines"
Cohesion: 0.04
Nodes (42): 1. `player_daily_stats`, 1. System Overview, 2. League & Configuration Ground Truth, 2. `transactions`, 3. Component Breakdown, 3. `historical_data`, 4. Database Schema (Supabase PostgreSQL), 5. Development & GitHub Guidelines for Agents (+34 more)

### Community 13 - "App.jsx"
Cohesion: 0.09
Nodes (27): App(), AVAILABLE_SEASONS, getSubTabsFromHash(), getViewFromHash(), HASH_TO_VIEW, OFFSEASON_VIEWS, NOTE: Make sure to export calculateTrioMatchupResult from scoring.js!, VIEW_TO_HASH (+19 more)

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.27
Nodes (9): BATTER_SLOT_ORDER, formatOBP(), formatRate(), FullRosterView(), getBatterDailyScore(), getRPDailyScore(), getSPStartScore(), PITCHER_SLOT_ORDER (+1 more)

### Community 15 - "update_league_context.py"
Cohesion: 0.18
Nodes (14): calculate_roto_points(), compute_playoff_matchups(), compute_rates(), fetch_supabase_table(), get_period_map(), get_stat(), is_season_active(), Computes live head-to-head category scores for Playoff Semi-Finals (Week 24)… (+6 more)

### Community 16 - "update_live_pickem_standings.py"
Cohesion: 0.11
Nodes (25): datetime, check_game_status(), get_periods_to_focus(), log_status(), date, mlb_schedule_check.py ───────────────────── Drop-in module for active-stats-…, Print a human-readable summary of today's game status., Decide whether the scraper should proceed. mode="force" — always run regardless… (+17 more)

### Community 17 - "ingest_historical_trades.py"
Cohesion: 0.08
Nodes (36): collections, concurrent_futures, aggregate_season(), calculate_fantasy_points(), fetch_2020_season(), fetch_2026_supabase(), fetch_espn_sp(), fetch_season_from_espn() (+28 more)

### Community 18 - "historical.py"
Cohesion: 0.20
Nodes (13): _canonical(), format_all_active_owner_summaries(), format_league_champions(), format_owner_history(), get_owner_seasons(), load_historical_data(), historical.py — shared historical context module Import this in discord-bot.py…, Championship counts for all active owners. (+5 more)

### Community 19 - "fetch_current_roster_records"
Cohesion: 0.15
Nodes (15): aggregate_by_team(), compute_roto_standings(), compute_standings_delta(), current_scoring_period(), espn_ip_to_innings(), fetch_current_roster_records(), fetch_current_roster_records_with_bench(), fetch_stats_for_periods() (+7 more)

### Community 20 - "ingest_pickem.py"
Cohesion: 0.26
Nodes (11): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), pipelines/ingest_pickem.py Annual MLB Pick'em Ingestion & Management Pipeline…, Fetch raw CSV rows for a tab from Google Sheets. (+3 more)

### Community 21 - "devDependencies"
Cohesion: 0.14
Nodes (14): devDependencies, autoprefixer, eslint, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, gh-pages, globals (+6 more)

### Community 22 - "react"
Cohesion: 0.06
Nodes (51): react, react-dom, @supabase/supabase-js, CommishActiveBanner(), CommishCheckoutModal(), calculateKeeperCostFromRank(), COMP_BUY_PRICES_2026, COMP_BUY_PRICES_2027 (+43 more)

### Community 23 - "sync_draft_pool.py"
Cohesion: 0.31
Nodes (10): extract_player_record(), fetch_espn_players(), get_current_season(), main(), normalize_name(), Any, Client, pipelines/sync_draft_pool.py Automated replacement for fantasy-baseball… (+2 more)

### Community 24 - "5. Team Retrospectives & Verified 2026 Player Anchors"
Cohesion: 0.05
Nodes (38): 1. Dan — The Silver Bullets (`Team 5`), 1. Executive Summary & The 2026 Storyline, 1. The Six-Way Fractional OBP Dogfight (.003 Margin), 2026 Category Champions, 2026 League Storylines & Season Context Guide, 2. Final 2026 Regular Season Standings (Week 23), 2. The Heavyweight Home Run Jam (21-Homer Window), 2. Tim — Anti-lock Brake Systems (`Team 1`) (+30 more)

### Community 25 - "HEFTYBot"
Cohesion: 0.25
Nodes (4): generate_answer(), HEFTYBot, handle_ping(), Message

### Community 26 - "live_command"
Cohesion: 0.21
Nodes (12): command, describe, ask_command(), build_mlb_live_embed_fields(), check_trades_command(), _game_field_value(), live_command(), ping_command() (+4 more)

### Community 27 - "migrate_gcs_to_supabase.py"
Cohesion: 0.33
Nodes (9): fetch_json(), main(), migrate_draft_history(), migrate_historical_finishes(), normalize_name(), Client, pipelines/migrate_gcs_to_supabase.py Migrates historical draft picks (draft-…, unicodedata (+1 more)

### Community 28 - "ProgressionView.jsx"
Cohesion: 0.26
Nodes (11): recharts, src_data_historicalfinishes, CANONICAL_OWNERS, getFranchiseLeaderboard(), getHistoricalSeasons(), normalizeOwner(), ALL_TIME_METRICS, formatVal() (+3 more)

### Community 29 - "is_season_active"
Cohesion: 0.13
Nodes (15): canonical_owner_name(), check_trade_notifications(), format_asset_list(), is_season_active(), offseason_sleep_service(), process_trade_notifications_once(), date, Processes pending trade lifecycle notifications from Supabase and delivers DMs. (+7 more)

### Community 30 - "getDateFromPeriodId"
Cohesion: 0.39
Nodes (6): getDateFromPeriodId(), calculateBatterValue(), calculatePitcherValue(), cleanPlayerName(), mlbSeasonDataCache, OwnerDisparitiesView()

### Community 31 - "PlayersView.jsx"
Cohesion: 0.47
Nodes (5): BAT_COLS, formatStat(), getLabel(), PITCH_COLS, PlayersView()

### Community 32 - "os"
Cohesion: 0.15
Nodes (8): dotenv, google_cloud, io, migrate_table(), Client, os, pandas, supabase

### Community 33 - "ingest_draft_infrastructure.py"
Cohesion: 0.50
Nodes (7): clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert(), pipelines/ingest_draft_infrastructure.py Ingests: 1. Compensation Picks & Team…

### Community 34 - "ingest_draft_trades.py"
Cohesion: 0.27
Nodes (10): csv, fetch_sheet_csv(), main(), parse_round_number(), parse_trade_log(), Finds the trade table starting at the header row containing 'Trade ID'., pipelines/ingest_draft_trades.py Ingests traded draft assets from the Google…, Parses round number from asset name (e.g., '14th round' -> 14, 'Worst… (+2 more)

### Community 35 - "TEAMS"
Cohesion: 0.11
Nodes (11): src_data_draft2026, src_data_draftassettrades2026, src_data_historicaltrades, src_data_transactions2026, src_data_transactions_historical, TEAMS, FANTASY_MANAGERS, PlayerValuationsView() (+3 more)

### Community 36 - "fetch_historical_transactions"
Cohesion: 0.40
Nodes (4): fetch_historical_transactions(), parse_standard_transactions(), Fetch all historical transactions (adds, drops, waivers, trades) across seasons…, Parse standard add, drop, waiver claim, and trade transactions from…

### Community 37 - "build_live_roster_map"
Cohesion: 0.22
Nodes (10): build_live_roster_map(), build_roster_name_map(), fetch_espn_live_rosters(), _match_player(), normalize_name(), Lowercase, strip punctuation and suffixes for fuzzy matching., Build {normalized_name: {owner, team_id, full_name}} from a set of records.…, Try to match an MLB API player name to a fantasy roster entry. (+2 more)

### Community 38 - "What You Must Do When Invoked"
Cohesion: 0.08
Nodes (24): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+16 more)

### Community 39 - "scripts"
Cohesion: 0.25
Nodes (8): scripts, build, deploy, dev, lint, predeploy, preview, start

### Community 41 - "deploy"
Cohesion: 0.25
Nodes (7): build, builder, deploy, restartPolicyMaxRetries, restartPolicyType, startCommand, $schema

### Community 42 - "index.ts"
Cohesion: 0.25
Nodes (4): ref_https, PlayerRecord, SEASON_START, TEAM_IDS

### Community 44 - "rosterOptimizer.js"
Cohesion: 0.33
Nodes (10): BATTER_SLOT_DEFS, buildPlayerPositionRegistry(), getDailyBatterScore(), getDailyPitcherScore(), optimizeDailyTeamLineup(), PITCHER_SLOT_DEFS, simulateSeasonBestLineups(), solveOptimalAssignment() (+2 more)

### Community 45 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 52 - "scoring.js"
Cohesion: 0.19
Nodes (14): BATTER_CATS, BoxScoreModal(), formatDisplayVal(), PITCHER_CATS, MatchupCard(), fetchPlayerOverallStats(), overallStatsCache, PlayerHistoryModal() (+6 more)

### Community 53 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 55 - "graphify reference: add a URL and watch a folder"
Cohesion: 0.50
Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder

### Community 56 - "graphify reference: commit hook and native CLAUDE.md integration"
Cohesion: 0.50
Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration

### Community 57 - "graphify reference: incremental update and cluster-only"
Cohesion: 0.50
Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only

## Knowledge Gaps
- **252 isolated node(s):** `name`, `private`, `version`, `homepage`, `type` (+247 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 475 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `DraftRoomView.jsx`, `test_trade_and_recommender.mjs`, `LeagueHistoryView.jsx`, `TEAMS`, `LiveScoreboardView.jsx`, `aggregateStats`, `package.json`, `rosterOptimizer.js`, `App.jsx`, `FullRosterView.jsx`, `scoring.js`, `ProgressionView.jsx`, `getDateFromPeriodId`, `PlayersView.jsx`?**
  _High betweenness centrality (0.071) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.022) - this node is a cross-community bridge._
- **Why does `aggregateStats()` connect `aggregateStats` to `test_trade_and_recommender.mjs`, `LeagueHistoryView.jsx`, `rosterOptimizer.js`, `App.jsx`, `FullRosterView.jsx`, `scoring.js`, `ProgressionView.jsx`, `getDateFromPeriodId`, `PlayersView.jsx`?**
  _High betweenness centrality (0.009) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `build_context()` (e.g. with `ask_command()` and `.on_message()`) actually correct?**
  _`build_context()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `name`, `private`, `version` to the rest of the system?**
  _252 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.060455486542443065 - nodes in this community are weakly interconnected._
- **Should `DraftRoomView.jsx` be split into smaller, more focused modules?**
  _Cohesion score 0.06393442622950819 - nodes in this community are weakly interconnected._