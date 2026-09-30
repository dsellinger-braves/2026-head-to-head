# Graph Report - 2026-head-to-head  (2026-09-30)

## Corpus Check
- 133 files · ~1,456,920 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 10 file(s) not represented in the graph (top: (none) 5, .toml 2, .css 2)

## Summary
- 1014 nodes · 2031 edges · 64 communities (54 shown, 10 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `bd117e03`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- discord-daily-recap.py
- DraftRoomView.jsx
- test_trade_and_recommender.mjs
- LeagueHistoryView.jsx
- compute_player_values.py
- projections.py
- schedule.js
- TeamsView.jsx
- build_mlb_live_data
- package.json
- json
- discord-bot.py
- AGENTS.md: Architecture & Developer Guidelines
- App.jsx
- FullRosterView.jsx
- build_recap_context.py
- update_league_context.py
- ingest_historical_trades.py
- historical.py
- update_live_pickem_standings.py
- ingest_pickem.py
- devDependencies
- KeepersBudgetsPanel.jsx
- supabaseClient.js
- 5. Team Retrospectives & Verified 2026 Player Anchors
- sync_draft_pool.py
- live_command
- requests
- ProgressionView.jsx
- is_season_active
- TeamAvatar
- PickemView.jsx
- os
- AuthContext.jsx
- ingest_draft_trades.py
- LiveScoreboardView.jsx
- DraftCapitalView.jsx
- build_live_roster_map
- What You Must Do When Invoked
- scripts
- migrate_gcs_to_supabase.py
- deploy
- index.ts
- react
- getDateFromPeriodId
- graphify reference: extra exports and benchmark
- dependencies
- OwnerLandingView.jsx
- vite
- overrides
- aggregateStats
- graphify reference: query, path, explain
- PlayerValuationsView.jsx
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- rules/graphify.md
- extraction-spec.md
- workflows/graphify.md

## God Nodes (most connected - your core abstractions)
1. `TeamAvatar()` - 43 edges
2. `react` - 40 edges
3. `App()` - 35 edges
4. `DraftRoomView()` - 35 edges
5. `aggregateStats()` - 33 edges
6. `build_context()` - 30 edges
7. `TEAMS` - 27 edges
8. `run_weekly_recap()` - 23 edges
9. `run_daily_recap()` - 19 edges
10. `useAuth()` - 17 edges

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

## Communities (64 total, 10 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.08
Nodes (39): aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates(), espn_ip_to_innings() (+31 more)

### Community 1 - "DraftRoomView.jsx"
Cohesion: 0.08
Nodes (52): getPlayerHeadshotUrl(), globalPlayerLookup, handleHeadshotError(), updateGlobalPlayerLookup(), AnalysisHistoryPanel(), callESPNProxy(), callGemini(), compute2027DraftPicks() (+44 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.07
Nodes (33): findWaiverReplacements(), isPositionMatch(), SLOT_TO_POS, calculateBudgetValue(), calculatePickValue(), calculatePlayerStatValue(), evaluateAsset(), getKeeperSurplus() (+25 more)

### Community 3 - "LeagueHistoryView.jsx"
Cohesion: 0.08
Nodes (38): idb-keyval, BAT_CATS, BATTER_SLOT_ORDER, ColHeader(), DayRosterModal(), formatVal(), PITCH_CATS, PITCHER_SLOT_ORDER (+30 more)

### Community 4 - "compute_player_values.py"
Cohesion: 0.10
Nodes (9): calculate_category_benchmarks(), compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank(), get_supabase_headers(), load_pricing_curve() (+1 more)

### Community 5 - "projections.py"
Cohesion: 0.11
Nodes (13): build_projection_map(), _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), fetch_projections(), _fg_get(), forecast_final_standings(), forecast_ros_only_standings() (+5 more)

### Community 6 - "schedule.js"
Cohesion: 0.27
Nodes (10): generateSchedule(), getTriosMatchups(), SEASON_DATES, SEASON_START_DATES, TEAMS, calculateStandings(), BracketMatch(), getStandings() (+2 more)

### Community 7 - "TeamsView.jsx"
Cohesion: 0.19
Nodes (19): calculateRotoPoints(), getStatMeta(), BATTING_CATS, ESPN_STAT_NAMES, parseRecord(), PITCHING_CATS, PlayerImpactSimulatorView(), computeDefendingBehindCushion() (+11 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.11
Nodes (10): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _mlb_get(), _mlb_ip_to_decimal() (+2 more)

### Community 9 - "package.json"
Cohesion: 0.13
Nodes (17): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+9 more)

### Community 10 - "json"
Cohesion: 0.12
Nodes (7): enrich_player_names(), fetch_activity_trades(), fetch_all_player_names(), fetch_historical_transactions(), fetch_transactions(), parse_activity_trades(), parse_standard_transactions()

### Community 11 - "discord-bot.py"
Cohesion: 0.09
Nodes (27): aggregate_by_player(), aggregate_by_team(), before_check_trade_notifications(), before_season_sentinel_loop(), build_context(), canonical_owner_name(), compute_roto_standings(), compute_standings_delta() (+19 more)

### Community 12 - "AGENTS.md: Architecture & Developer Guidelines"
Cohesion: 0.04
Nodes (42): 1. `player_daily_stats`, 1. System Overview, 2. League & Configuration Ground Truth, 2. `transactions`, 3. Component Breakdown, 3. `historical_data`, 4. Database Schema (Supabase PostgreSQL), 5. Development & GitHub Guidelines for Agents (+34 more)

### Community 13 - "App.jsx"
Cohesion: 0.16
Nodes (16): App(), AVAILABLE_SEASONS, getSubTabsFromHash(), getViewFromHash(), HASH_TO_VIEW, OFFSEASON_VIEWS, VIEW_TO_HASH, OwnerDetailModal() (+8 more)

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.29
Nodes (12): getPlayerAcquisition(), AcquisitionChip(), BATTER_SLOT_ORDER, formatOBP(), formatRate(), FullRosterView(), getBatterDailyScore(), getRPDailyScore() (+4 more)

### Community 15 - "build_recap_context.py"
Cohesion: 0.20
Nodes (4): detect_active_roto_battles(), ensure_context_is_fresh(), fmt_cat_val(), load_league_context()

### Community 16 - "update_league_context.py"
Cohesion: 0.18
Nodes (9): calculate_roto_points(), compute_playoff_matchups(), compute_rates(), fetch_supabase_table(), get_period_map(), get_stat(), is_season_active(), refresh_context_files() (+1 more)

### Community 17 - "ingest_historical_trades.py"
Cohesion: 0.08
Nodes (21): aggregate_season(), calculate_fantasy_points(), fetch_2020_season(), fetch_2026_supabase(), fetch_espn_sp(), fetch_season_from_espn(), get_owner_name(), main() (+13 more)

### Community 18 - "historical.py"
Cohesion: 0.20
Nodes (6): _canonical(), format_all_active_owner_summaries(), format_league_champions(), format_owner_history(), get_owner_seasons(), load_historical_data()

### Community 19 - "update_live_pickem_standings.py"
Cohesion: 0.26
Nodes (8): build_live_in_progress_snapshot(), evaluate_owner_projected_scores(), fetch_mlb_standings(), fetch_projected_war_leaders(), get_team_code(), matches_team_or_val(), normalize_text(), run()

### Community 20 - "ingest_pickem.py"
Cohesion: 0.21
Nodes (7): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), run_pipeline()

### Community 21 - "devDependencies"
Cohesion: 0.14
Nodes (14): devDependencies, autoprefixer, eslint, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, gh-pages, globals (+6 more)

### Community 22 - "KeepersBudgetsPanel.jsx"
Cohesion: 0.20
Nodes (9): calculateKeeperCostFromRank(), COMP_BUY_PRICES, COMP_SELL_PRICES, DRAFT_MANAGERS, KeepersBudgetsPanel(), normalizeManager(), calculateKeeperCostFromRank(), KeepersBudgetsView() (+1 more)

### Community 23 - "supabaseClient.js"
Cohesion: 0.15
Nodes (9): @supabase/supabase-js, DEFAULT_SUPABASE_ANON_KEY, DEFAULT_SUPABASE_URL, supabase, supabaseUrl, AVAILABLE_YEARS, groupTransactions(), TransactionsView() (+1 more)

### Community 24 - "5. Team Retrospectives & Verified 2026 Player Anchors"
Cohesion: 0.05
Nodes (38): 1. Dan — The Silver Bullets (`Team 5`), 1. Executive Summary & The 2026 Storyline, 1. The Six-Way Fractional OBP Dogfight (.003 Margin), 2026 Category Champions, 2026 League Storylines & Season Context Guide, 2. Final 2026 Regular Season Standings (Week 23), 2. The Heavyweight Home Run Jam (21-Homer Window), 2. Tim — Anti-lock Brake Systems (`Team 1`) (+30 more)

### Community 25 - "sync_draft_pool.py"
Cohesion: 0.24
Nodes (6): extract_player_record(), fetch_espn_players(), get_current_season(), main(), normalize_name(), sync_pool()

### Community 26 - "live_command"
Cohesion: 0.16
Nodes (8): ask_command(), build_mlb_live_embed_fields(), check_trades_command(), current_scoring_period(), _game_field_value(), generate_answer(), live_command(), ping_command()

### Community 27 - "requests"
Cohesion: 0.10
Nodes (4): check_game_status(), get_periods_to_focus(), log_status(), should_scrape()

### Community 28 - "ProgressionView.jsx"
Cohesion: 0.26
Nodes (10): recharts, CANONICAL_OWNERS, getFranchiseLeaderboard(), getHistoricalSeasons(), normalizeOwner(), ALL_TIME_METRICS, formatVal(), ProgressionView() (+2 more)

### Community 29 - "is_season_active"
Cohesion: 0.12
Nodes (7): check_trade_notifications(), HEFTYBot, handle_ping(), is_season_active(), offseason_sleep_service(), scoring_period_for_date(), season_sentinel_loop()

### Community 30 - "TeamAvatar"
Cohesion: 0.19
Nodes (10): BoxScoreModal(), MatchupCard(), TeamAvatar(), CATEGORIES, PlayerOwnerStatsView(), POSITIONS, SEASONS, ALL_MANAGERS (+2 more)

### Community 31 - "PickemView.jsx"
Cohesion: 0.27
Nodes (8): findMlbTeam(), LEAGUE_OWNERS, MLB_DIVISIONS, MLB_TEAMS, PICKEM_RULES, PROMINENT_AWARD_CANDIDATES, teamsMatch(), PickemView()

### Community 32 - "os"
Cohesion: 0.18
Nodes (7): migrate_table(), clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert()

### Community 33 - "AuthContext.jsx"
Cohesion: 0.29
Nodes (6): react-dom, AuthProvider(), initAuth(), cleanupOAuthHash(), STATIC_LEAGUE_PROFILES, AuthContext

### Community 34 - "ingest_draft_trades.py"
Cohesion: 0.31
Nodes (6): fetch_sheet_csv(), main(), parse_round_number(), parse_trade_log(), save_local_json(), upsert_to_supabase()

### Community 35 - "LiveScoreboardView.jsx"
Cohesion: 0.53
Nodes (7): FantasyBadge(), GameDetailModal(), buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard(), normalizeName(), LiveScoreboardView()

### Community 36 - "DraftCapitalView.jsx"
Cohesion: 0.36
Nodes (9): canonicalOwnerName(), compute2027DraftPicks(), DRAFT_OWNERS, DraftCapitalView(), enqueueTradeNotification(), getTeamId(), isPickInAssets(), isPlayerInAssets() (+1 more)

### Community 37 - "build_live_roster_map"
Cohesion: 0.18
Nodes (6): build_live_roster_map(), build_roster_name_map(), fetch_current_roster_records(), fetch_espn_live_rosters(), _match_player(), normalize_name()

### Community 38 - "What You Must Do When Invoked"
Cohesion: 0.08
Nodes (24): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+16 more)

### Community 39 - "scripts"
Cohesion: 0.25
Nodes (8): scripts, build, deploy, dev, lint, predeploy, preview, start

### Community 40 - "migrate_gcs_to_supabase.py"
Cohesion: 0.46
Nodes (5): fetch_json(), main(), migrate_draft_history(), migrate_historical_finishes(), normalize_name()

### Community 41 - "deploy"
Cohesion: 0.25
Nodes (7): build, builder, deploy, restartPolicyMaxRetries, restartPolicyType, startCommand, $schema

### Community 42 - "index.ts"
Cohesion: 0.25
Nodes (3): PlayerRecord, SEASON_START, TEAM_IDS

### Community 43 - "react"
Cohesion: 0.56
Nodes (5): react, CommishActiveBanner(), CommishCheckoutModal(), UserNavWidget(), useAuth()

### Community 44 - "getDateFromPeriodId"
Cohesion: 0.31
Nodes (11): getDateFromPeriodId(), BATTER_SLOT_DEFS, buildPlayerPositionRegistry(), getDailyBatterScore(), getDailyPitcherScore(), optimizeDailyTeamLineup(), PITCHER_SLOT_DEFS, simulateSeasonBestLineups() (+3 more)

### Community 45 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 47 - "OwnerLandingView.jsx"
Cohesion: 0.32
Nodes (7): evaluatePlayerCapital(), DEFAULT_POS_MAP, getPlayerPositions(), LEAGUE_MANAGERS, MLB_TEAMS, OwnerLandingView(), SLOT_MAP

### Community 52 - "aggregateStats"
Cohesion: 0.14
Nodes (20): overallStatsCache, aggregateBenchStats(), aggregateStats(), calculateBatterValue(), calculatePitcherValue(), ESPN_STAT_IDS, LINEUP_SLOTS, MINUTIAE_STATS (+12 more)

### Community 53 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 54 - "PlayerValuationsView.jsx"
Cohesion: 0.36
Nodes (5): CategoryBox(), CategoryChip(), FANTASY_MANAGERS, PlayerValuationsView(), StatBox()

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
- **245 isolated node(s):** `name`, `private`, `version`, `homepage`, `type` (+240 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 438 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `DraftRoomView.jsx`, `LeagueHistoryView.jsx`, `schedule.js`, `TeamsView.jsx`, `package.json`, `App.jsx`, `FullRosterView.jsx`, `KeepersBudgetsPanel.jsx`, `supabaseClient.js`, `ProgressionView.jsx`, `TeamAvatar`, `PickemView.jsx`, `AuthContext.jsx`, `LiveScoreboardView.jsx`, `DraftCapitalView.jsx`, `getDateFromPeriodId`, `OwnerLandingView.jsx`, `aggregateStats`, `PlayerValuationsView.jsx`?**
  _High betweenness centrality (0.067) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.022) - this node is a cross-community bridge._
- **Why does `TeamAvatar()` connect `TeamAvatar` to `LiveScoreboardView.jsx`, `LeagueHistoryView.jsx`, `schedule.js`, `TeamsView.jsx`, `getDateFromPeriodId`, `App.jsx`, `FullRosterView.jsx`, `OwnerLandingView.jsx`, `aggregateStats`, `supabaseClient.js`?**
  _High betweenness centrality (0.009) - this node is a cross-community bridge._
- **What connects `name`, `private`, `version` to the rest of the system?**
  _245 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.07622504537205081 - nodes in this community are weakly interconnected._
- **Should `DraftRoomView.jsx` be split into smaller, more focused modules?**
  _Cohesion score 0.08484848484848485 - nodes in this community are weakly interconnected._
- **Should `test_trade_and_recommender.mjs` be split into smaller, more focused modules?**
  _Cohesion score 0.06585365853658537 - nodes in this community are weakly interconnected._