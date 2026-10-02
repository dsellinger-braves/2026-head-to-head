# Graph Report - 2026 Head to Head Heftystrong  (2026-10-01)

## Corpus Check
- 140 files · ~2,415,177 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 10 file(s) not represented in the graph (top: (none) 5, .toml 2, .css 2)

## Summary
- 1054 nodes · 2034 edges · 69 communities (59 shown, 10 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 13 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `cb8573a7`
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
- transaction-scraper.py
- build_context
- AGENTS.md: Architecture & Developer Guidelines
- fetch_current_roster_records
- FullRosterView.jsx
- supabaseClient.js
- update_live_pickem_standings.py
- generate_player_owner_stats.py
- build_recap_context.py
- ingest_historical_trades.py
- ingest_pickem.py
- devDependencies
- KeepersBudgetsPanel.jsx
- sync_draft_pool.py
- 5. Team Retrospectives & Verified 2026 Player Anchors
- discord-bot.py
- live_command
- migrate_gcs_to_supabase.py
- ProgressionView.jsx
- is_season_active
- HEFTYBot
- sys
- os
- ingest_draft_infrastructure.py
- ingest_draft_trades.py
- compute_player_values.py
- App.jsx
- build_live_roster_map
- What You Must Do When Invoked
- scripts
- sync_player_news.py
- deploy
- index.ts
- ref_fs
- getDateFromPeriodId
- graphify reference: extra exports and benchmark
- dependencies
- PlayersView.jsx
- vite
- overrides
- BoxScoreModal.jsx
- graphify reference: query, path, explain
- react
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- rules/graphify.md
- extraction-spec.md
- workflows/graphify.md
- KeeperCalculationsView.jsx
- TeamAvatar.jsx
- fetchFromGCS
- compute_roto_standings
- KeepersBudgetsView.jsx
- fetch_projections

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
- `build_context()` --calls--> `fetch_projections()`  [EXTRACTED]
  discord-bot.py → projections.py
- `build_context()` --calls--> `forecast_final_standings()`  [EXTRACTED]
  discord-bot.py → projections.py

## Import Cycles
- None detected.

## Communities (69 total, 10 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.06
Nodes (67): aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates(), espn_ip_to_innings() (+59 more)

### Community 1 - "DraftRoomView.jsx"
Cohesion: 0.09
Nodes (24): callESPNProxy(), callGemini(), compute2027DraftPicks(), DEFAULT_OWNER_PROFILES, DRAFT_OWNERS, DraftCapitalPanel(), DraftRoomView(), fetchAllEspnPlayers() (+16 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.05
Nodes (47): ref_node_assert, src_data_draft2026, src_data_historicaltrades, src_data_keeperinput2026, src_data_transactions2026, evaluatePlayerCapital(), findWaiverReplacements(), isPositionMatch() (+39 more)

### Community 3 - "LeagueHistoryView.jsx"
Cohesion: 0.08
Nodes (38): idb-keyval, BAT_CATS, BATTER_SLOT_ORDER, DayRosterModal(), formatVal(), PITCH_CATS, PITCHER_SLOT_ORDER, StatCell() (+30 more)

### Community 4 - "run_pipeline"
Cohesion: 0.07
Nodes (20): calculate_category_benchmarks(), compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank(), get_supabase_headers(), load_pricing_curve() (+12 more)

### Community 5 - "projections.py"
Cohesion: 0.17
Nodes (18): _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), forecast_final_standings(), forecast_ros_only_standings(), get_player_projection(), _get_stat_val(), normalize_name() (+10 more)

### Community 6 - "LiveScoreboardView.jsx"
Cohesion: 0.49
Nodes (6): GameDetailModal(), buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard(), normalizeName(), LiveScoreboardView()

### Community 7 - "aggregateStats"
Cohesion: 0.13
Nodes (28): OwnerDetailModal(), aggregateBenchStats(), aggregateStats(), calculateRotoPoints(), CATEGORIES, ESPN_STAT_IDS, getStatMeta(), MINUTIAE_STATS (+20 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.11
Nodes (19): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _mlb_get(), _mlb_ip_to_decimal() (+11 more)

### Community 9 - "package.json"
Cohesion: 0.13
Nodes (17): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+9 more)

### Community 10 - "transaction-scraper.py"
Cohesion: 0.12
Nodes (15): time, enrich_player_names(), fetch_activity_trades(), fetch_all_player_names(), fetch_historical_transactions(), fetch_transactions(), parse_activity_trades(), parse_standard_transactions() (+7 more)

### Community 11 - "build_context"
Cohesion: 0.13
Nodes (18): build_context(), fetch_recent_transactions(), fetch_stats_up_to_period(), fetch_team_transactions(), format_trades_block(), format_transaction_context(), format_transaction_with_stats(), get_supabase() (+10 more)

### Community 12 - "AGENTS.md: Architecture & Developer Guidelines"
Cohesion: 0.04
Nodes (42): 1. `player_daily_stats`, 1. System Overview, 2. League & Configuration Ground Truth, 2. `transactions`, 3. Component Breakdown, 3. `historical_data`, 4. Database Schema (Supabase PostgreSQL), 5. Development & GitHub Guidelines for Agents (+34 more)

### Community 13 - "fetch_current_roster_records"
Cohesion: 0.24
Nodes (10): compute_standings_delta(), current_scoring_period(), fetch_current_roster_records(), fetch_current_roster_records_with_bench(), fetch_stats_for_periods(), filter_active(), get_trend_block(), Fetch the most recent scoring period's active records for roster… (+2 more)

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.17
Nodes (15): fetchPlayerOverallStats(), overallStatsCache, PlayerHistoryModal(), loadOverall(), getPlayerAcquisition(), LINEUP_SLOTS, BATTER_SLOT_ORDER, formatOBP() (+7 more)

### Community 15 - "supabaseClient.js"
Cohesion: 0.22
Nodes (7): @supabase/supabase-js, DEFAULT_SUPABASE_ANON_KEY, DEFAULT_SUPABASE_URL, supabase, supabaseUrl, AVAILABLE_YEARS, DraftHistoryView()

### Community 16 - "update_live_pickem_standings.py"
Cohesion: 0.26
Nodes (14): build_live_in_progress_snapshot(), evaluate_owner_projected_scores(), fetch_mlb_standings(), fetch_projected_war_leaders(), get_team_code(), matches_team_or_val(), normalize_text(), Any (+6 more)

### Community 17 - "generate_player_owner_stats.py"
Cohesion: 0.07
Nodes (36): collections, concurrent_futures, ensure_context_is_fresh(), load_league_context(), Ensure data/league_context.json is updated from Supabase if needed., Load the persistent league context document, optionally refreshing first., aggregate_season(), calculate_fantasy_points() (+28 more)

### Community 18 - "build_recap_context.py"
Cohesion: 0.25
Nodes (8): detect_active_roto_battles(), fmt_cat_val(), Format a category stat value cleanly., Dynamically identify active category volatility and standings deadlocks where…, pipelines/build_recap_context.py Fetches real-time MLB news from ESPN and…, re, typing, xml_etree_elementtree

### Community 19 - "ingest_historical_trades.py"
Cohesion: 0.19
Nodes (18): compute_historical_post_trade_stats(), compute_post_trade_stats(), extract_stats_from_draft_record(), fetch_sheet_rows(), load_2026_daily_records(), load_draft_history(), main(), normalize_name() (+10 more)

### Community 20 - "ingest_pickem.py"
Cohesion: 0.26
Nodes (11): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), pipelines/ingest_pickem.py Annual MLB Pick'em Ingestion & Management Pipeline…, Fetch raw CSV rows for a tab from Google Sheets. (+3 more)

### Community 21 - "devDependencies"
Cohesion: 0.14
Nodes (14): devDependencies, autoprefixer, eslint, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, gh-pages, globals (+6 more)

### Community 22 - "KeepersBudgetsPanel.jsx"
Cohesion: 0.22
Nodes (11): calculateKeeperCostFromRank(), COMP_BUY_PRICES_2026, COMP_BUY_PRICES_2027, COMP_SELL_PRICES_2026, COMP_SELL_PRICES_2027, DRAFT_MANAGERS, getCompBuyPrices(), getCompSellPrices() (+3 more)

### Community 23 - "sync_draft_pool.py"
Cohesion: 0.36
Nodes (9): extract_player_record(), fetch_espn_players(), get_current_season(), main(), normalize_name(), Any, Client, pipelines/sync_draft_pool.py Automated replacement for fantasy-baseball… (+1 more)

### Community 24 - "5. Team Retrospectives & Verified 2026 Player Anchors"
Cohesion: 0.05
Nodes (38): 1. Dan — The Silver Bullets (`Team 5`), 1. Executive Summary & The 2026 Storyline, 1. The Six-Way Fractional OBP Dogfight (.003 Margin), 2026 Category Champions, 2026 League Storylines & Season Context Guide, 2. Final 2026 Regular Season Standings (Week 23), 2. The Heavyweight Home Run Jam (21-Homer Window), 2. Tim — Anti-lock Brake Systems (`Team 1`) (+30 more)

### Community 25 - "discord-bot.py"
Cohesion: 0.15
Nodes (16): asyncio, before_loop, discord, aggregate_by_player(), before_check_trade_notifications(), before_season_sentinel_loop(), canonical_owner_name(), format_asset_list() (+8 more)

### Community 26 - "live_command"
Cohesion: 0.21
Nodes (12): command, describe, ask_command(), build_mlb_live_embed_fields(), check_trades_command(), _game_field_value(), live_command(), ping_command() (+4 more)

### Community 27 - "migrate_gcs_to_supabase.py"
Cohesion: 0.39
Nodes (8): fetch_json(), main(), migrate_draft_history(), migrate_historical_finishes(), normalize_name(), Client, pipelines/migrate_gcs_to_supabase.py Migrates historical draft picks (draft-…, unicodedata

### Community 28 - "ProgressionView.jsx"
Cohesion: 0.26
Nodes (11): recharts, src_data_historicalfinishes, CANONICAL_OWNERS, getFranchiseLeaderboard(), getHistoricalSeasons(), normalizeOwner(), ALL_TIME_METRICS, formatVal() (+3 more)

### Community 29 - "is_season_active"
Cohesion: 0.25
Nodes (9): check_trade_notifications(), is_season_active(), date, Checks Supabase for pending trade lifecycle notifications and delivers DMs., Daily check to disconnect the bot if regular season has concluded., Check if check_date (defaults to today) falls within the active MLB regular…, scoring_period_for_date(), season_sentinel_loop() (+1 more)

### Community 30 - "HEFTYBot"
Cohesion: 0.18
Nodes (6): generate_answer(), HEFTYBot, handle_ping(), offseason_sleep_service(), Keeps process alive (and HTTP healthcheck responsive for Railway) while…, Message

### Community 31 - "sys"
Cohesion: 0.15
Nodes (12): datetime, check_game_status(), get_periods_to_focus(), log_status(), date, mlb_schedule_check.py ───────────────────── Drop-in module for active-stats-…, Print a human-readable summary of today's game status., Decide whether the scraper should proceed. mode="force" — always run regardless… (+4 more)

### Community 32 - "os"
Cohesion: 0.29
Nodes (5): dotenv, migrate_table(), Client, os, supabase

### Community 33 - "ingest_draft_infrastructure.py"
Cohesion: 0.50
Nodes (7): clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert(), pipelines/ingest_draft_infrastructure.py Ingests: 1. Compensation Picks & Team…

### Community 34 - "ingest_draft_trades.py"
Cohesion: 0.31
Nodes (9): fetch_sheet_csv(), main(), parse_round_number(), parse_trade_log(), Finds the trade table starting at the header row containing 'Trade ID'., pipelines/ingest_draft_trades.py Ingests traded draft assets from the Google…, Parses round number from asset name (e.g., '14th round' -> 14, 'Worst…, save_local_json() (+1 more)

### Community 35 - "compute_player_values.py"
Cohesion: 0.18
Nodes (9): csv, google_cloud, io, json, math, pandas, pipelines/compute_player_values.py Automated Player Valuation & Pricing Engine…, pipelines/generate_keeper_calculations.py Generates detailed multi-year keeper… (+1 more)

### Community 36 - "App.jsx"
Cohesion: 0.09
Nodes (25): App(), AVAILABLE_SEASONS, getSubTabsFromHash(), getViewFromHash(), HASH_TO_VIEW, OFFSEASON_VIEWS, NOTE: Make sure to export calculateTrioMatchupResult from scoring.js!, VIEW_TO_HASH (+17 more)

### Community 37 - "build_live_roster_map"
Cohesion: 0.22
Nodes (10): build_live_roster_map(), build_roster_name_map(), fetch_espn_live_rosters(), _match_player(), normalize_name(), Lowercase, strip punctuation and suffixes for fuzzy matching., Build {normalized_name: {owner, team_id, full_name}} from a set of records.…, Try to match an MLB API player name to a fantasy roster entry. (+2 more)

### Community 38 - "What You Must Do When Invoked"
Cohesion: 0.08
Nodes (24): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+16 more)

### Community 39 - "scripts"
Cohesion: 0.25
Nodes (8): scripts, build, deploy, dev, lint, predeploy, preview, start

### Community 40 - "sync_player_news.py"
Cohesion: 0.24
Nodes (10): argparse, fetch_player_news(), load_player_pool(), main(), Any, pipelines/sync_player_news.py Syncs live player news directly from the ESPN…, Loads player pool from local file, Supabase, or GCS., Fetches news feed for an individual ESPN player ID. (+2 more)

### Community 41 - "deploy"
Cohesion: 0.25
Nodes (7): build, builder, deploy, restartPolicyMaxRetries, restartPolicyType, startCommand, $schema

### Community 42 - "index.ts"
Cohesion: 0.25
Nodes (4): ref_https, PlayerRecord, SEASON_START, TEAM_IDS

### Community 44 - "getDateFromPeriodId"
Cohesion: 0.31
Nodes (11): getDateFromPeriodId(), BATTER_SLOT_DEFS, buildPlayerPositionRegistry(), getDailyBatterScore(), getDailyPitcherScore(), optimizeDailyTeamLineup(), PITCHER_SLOT_DEFS, simulateSeasonBestLineups() (+3 more)

### Community 45 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 47 - "PlayersView.jsx"
Cohesion: 0.47
Nodes (5): BAT_COLS, formatStat(), getLabel(), PITCH_COLS, PlayersView()

### Community 52 - "BoxScoreModal.jsx"
Cohesion: 0.32
Nodes (6): BATTER_CATS, BoxScoreModal(), formatDisplayVal(), PITCHER_CATS, MatchupCard(), WeeklyView()

### Community 53 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 54 - "react"
Cohesion: 0.10
Nodes (30): react, react-dom, CommishActiveBanner(), CommishCheckoutModal(), UserNavWidget(), AuthProvider(), initAuth(), cleanupOAuthHash() (+22 more)

### Community 55 - "graphify reference: add a URL and watch a folder"
Cohesion: 0.50
Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder

### Community 56 - "graphify reference: commit hook and native CLAUDE.md integration"
Cohesion: 0.50
Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration

### Community 57 - "graphify reference: incremental update and cluster-only"
Cohesion: 0.50
Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only

### Community 63 - "KeeperCalculationsView.jsx"
Cohesion: 0.16
Nodes (18): src_data_keeperinput2027, getPlayerHeadshotUrl(), globalPlayerLookup, handleHeadshotError(), updateGlobalPlayerLookup(), DraftLogPanel(), getInjuryIndicator(), PlayerModal() (+10 more)

### Community 64 - "TeamAvatar.jsx"
Cohesion: 0.19
Nodes (10): TeamAvatar(), src_data_transactions_historical, calculateBatterValue(), calculatePitcherValue(), cleanPlayerName(), mlbSeasonDataCache, OwnerDisparitiesView(), groupTransactions() (+2 more)

### Community 65 - "fetchFromGCS"
Cohesion: 0.60
Nodes (6): draftDbGet(), draftDbSet(), fetchFromGCS(), fetchPlayerNews(), getDraftDb(), fetchPlayerData()

### Community 66 - "compute_roto_standings"
Cohesion: 0.40
Nodes (5): aggregate_by_team(), compute_roto_standings(), espn_ip_to_innings(), Slot-aware team aggregation. Only hitting stats count from hitting slots and…, ESPN stores IP as total outs. Divide by 3 for decimal innings.

### Community 67 - "KeepersBudgetsView.jsx"
Cohesion: 0.29
Nodes (7): src_data_compensationpicks2026, src_data_keepercalculations, src_data_teambudgets2026, src_data_teambudgets2027, calculateKeeperCostFromRank(), KeepersBudgetsView(), TEAM_OWNERS

### Community 68 - "fetch_projections"
Cohesion: 0.50
Nodes (4): fetch_projections(), _fg_get(), Fetch a FanGraphs projection endpoint with caching., Fetch both batting and pitching ROS projections.

## Knowledge Gaps
- **252 isolated node(s):** `name`, `private`, `version`, `homepage`, `type` (+247 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 480 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `TeamAvatar.jsx`, `DraftRoomView.jsx`, `test_trade_and_recommender.mjs`, `LeagueHistoryView.jsx`, `App.jsx`, `KeepersBudgetsView.jsx`, `LiveScoreboardView.jsx`, `aggregateStats`, `package.json`, `getDateFromPeriodId`, `FullRosterView.jsx`, `supabaseClient.js`, `PlayersView.jsx`, `BoxScoreModal.jsx`, `KeepersBudgetsPanel.jsx`, `ProgressionView.jsx`, `KeeperCalculationsView.jsx`?**
  _High betweenness centrality (0.072) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.018) - this node is a cross-community bridge._
- **Why does `aggregateStats()` connect `aggregateStats` to `TeamAvatar.jsx`, `test_trade_and_recommender.mjs`, `LeagueHistoryView.jsx`, `App.jsx`, `getDateFromPeriodId`, `FullRosterView.jsx`, `PlayersView.jsx`, `BoxScoreModal.jsx`, `ProgressionView.jsx`?**
  _High betweenness centrality (0.008) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `build_context()` (e.g. with `ask_command()` and `.on_message()`) actually correct?**
  _`build_context()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `name`, `private`, `version` to the rest of the system?**
  _252 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.05985915492957746 - nodes in this community are weakly interconnected._
- **Should `DraftRoomView.jsx` be split into smaller, more focused modules?**
  _Cohesion score 0.08571428571428572 - nodes in this community are weakly interconnected._