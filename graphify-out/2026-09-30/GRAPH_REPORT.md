# Graph Report - 2026-head-to-head  (2026-09-30)

## Corpus Check
- 142 files · ~1,467,810 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 10 file(s) not represented in the graph (top: (none) 5, .toml 2, .css 2)

## Summary
- 1041 nodes · 2128 edges · 64 communities (48 shown, 16 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `6c5c5196`
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
- generate_player_owner_stats.py
- requests
- update_live_pickem_standings.py
- ingest_pickem.py
- devDependencies
- react
- fetch_current_roster_records
- 5. Team Retrospectives & Verified 2026 Player Anchors
- ingest_historical_trades.py
- live_command
- sys
- ProgressionView.jsx
- is_season_active
- BoxScoreModal.jsx
- HEFTYBot
- os
- ingest_draft_infrastructure.py
- ingest_draft_trades.py
- TeamAvatar
- fetch_historical_transactions
- build_live_roster_map
- What You Must Do When Invoked
- scripts
- calculate_category_benchmarks
- deploy
- index.ts
- ref_fs
- rosterOptimizer.js
- graphify reference: extra exports and benchmark
- dependencies
- check_late_sps.mjs
- vite
- overrides
- aggregateStats
- graphify reference: query, path, explain
- check_sp0.mjs
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- rules/graphify.md
- extraction-spec.md
- workflows/graphify.md

## God Nodes (most connected - your core abstractions)
1. `aggregateStats()` - 45 edges
2. `TeamAvatar()` - 43 edges
3. `react` - 40 edges
4. `App()` - 35 edges
5. `DraftRoomView()` - 35 edges
6. `build_context()` - 30 edges
7. `TEAMS` - 30 edges
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

## Communities (64 total, 16 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.09
Nodes (36): aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates(), espn_ip_to_innings() (+28 more)

### Community 1 - "DraftRoomView.jsx"
Cohesion: 0.08
Nodes (52): getPlayerHeadshotUrl(), globalPlayerLookup, handleHeadshotError(), updateGlobalPlayerLookup(), AnalysisHistoryPanel(), callESPNProxy(), callGemini(), compute2027DraftPicks() (+44 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.06
Nodes (40): evaluatePlayerCapital(), findWaiverReplacements(), isPositionMatch(), SLOT_TO_POS, calculateBudgetValue(), calculatePickValue(), calculatePlayerStatValue(), evaluateAsset() (+32 more)

### Community 3 - "LeagueHistoryView.jsx"
Cohesion: 0.08
Nodes (38): idb-keyval, BAT_CATS, BATTER_SLOT_ORDER, ColHeader(), DayRosterModal(), formatVal(), PITCH_CATS, PITCHER_SLOT_ORDER (+30 more)

### Community 4 - "compute_player_values.py"
Cohesion: 0.11
Nodes (8): compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank(), get_supabase_headers(), load_pricing_curve(), run_pipeline()

### Community 5 - "projections.py"
Cohesion: 0.11
Nodes (12): build_projection_map(), _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), fetch_projections(), _fg_get(), forecast_final_standings(), forecast_ros_only_standings() (+4 more)

### Community 6 - "schedule.js"
Cohesion: 0.18
Nodes (23): @supabase/supabase-js, runAudit(), supabase, checkWeek25Details(), supabase, inspectFinals(), supabase, runFullSeasonAudit() (+15 more)

### Community 7 - "TeamsView.jsx"
Cohesion: 0.17
Nodes (20): calculateRotoPoints(), getStatMeta(), MINUTIAE_STATS, BATTING_CATS, ESPN_STAT_NAMES, parseRecord(), PITCHING_CATS, PlayerImpactSimulatorView() (+12 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.11
Nodes (10): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _mlb_get(), _mlb_ip_to_decimal() (+2 more)

### Community 9 - "package.json"
Cohesion: 0.13
Nodes (17): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+9 more)

### Community 10 - "json"
Cohesion: 0.11
Nodes (5): enrich_player_names(), fetch_activity_trades(), fetch_all_player_names(), fetch_transactions(), parse_activity_trades()

### Community 11 - "discord-bot.py"
Cohesion: 0.11
Nodes (20): aggregate_by_player(), before_check_trade_notifications(), before_season_sentinel_loop(), build_context(), canonical_owner_name(), fetch_recent_transactions(), fetch_stats_up_to_period(), fetch_team_transactions() (+12 more)

### Community 12 - "AGENTS.md: Architecture & Developer Guidelines"
Cohesion: 0.04
Nodes (42): 1. `player_daily_stats`, 1. System Overview, 2. League & Configuration Ground Truth, 2. `transactions`, 3. Component Breakdown, 3. `historical_data`, 4. Database Schema (Supabase PostgreSQL), 5. Development & GitHub Guidelines for Agents (+34 more)

### Community 13 - "App.jsx"
Cohesion: 0.09
Nodes (25): App(), AVAILABLE_SEASONS, getSubTabsFromHash(), getViewFromHash(), HASH_TO_VIEW, OFFSEASON_VIEWS, VIEW_TO_HASH, TEAMS (+17 more)

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.32
Nodes (11): AcquisitionChip(), BATTER_SLOT_ORDER, formatOBP(), formatRate(), FullRosterView(), getBatterDailyScore(), getRPDailyScore(), getSPStartScore() (+3 more)

### Community 15 - "build_recap_context.py"
Cohesion: 0.12
Nodes (7): build_roster_player_map(), detect_active_roto_battles(), ensure_context_is_fresh(), fetch_mlb_news(), find_news_roster_overlap(), fmt_cat_val(), load_league_context()

### Community 16 - "update_league_context.py"
Cohesion: 0.18
Nodes (9): calculate_roto_points(), compute_playoff_matchups(), compute_rates(), fetch_supabase_table(), get_period_map(), get_stat(), is_season_active(), refresh_context_files() (+1 more)

### Community 17 - "generate_player_owner_stats.py"
Cohesion: 0.14
Nodes (8): aggregate_season(), calculate_fantasy_points(), fetch_2020_season(), fetch_2026_supabase(), fetch_espn_sp(), fetch_season_from_espn(), get_owner_name(), main()

### Community 18 - "requests"
Cohesion: 0.18
Nodes (6): _canonical(), format_all_active_owner_summaries(), format_league_champions(), format_owner_history(), get_owner_seasons(), load_historical_data()

### Community 19 - "update_live_pickem_standings.py"
Cohesion: 0.26
Nodes (8): build_live_in_progress_snapshot(), evaluate_owner_projected_scores(), fetch_mlb_standings(), fetch_projected_war_leaders(), get_team_code(), matches_team_or_val(), normalize_text(), run()

### Community 20 - "ingest_pickem.py"
Cohesion: 0.26
Nodes (7): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), run_pipeline()

### Community 21 - "devDependencies"
Cohesion: 0.14
Nodes (14): devDependencies, autoprefixer, eslint, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, gh-pages, globals (+6 more)

### Community 22 - "react"
Cohesion: 0.05
Nodes (49): react, react-dom, CommishActiveBanner(), CommishCheckoutModal(), calculateKeeperCostFromRank(), COMP_BUY_PRICES, COMP_SELL_PRICES, DRAFT_MANAGERS (+41 more)

### Community 23 - "fetch_current_roster_records"
Cohesion: 0.15
Nodes (10): aggregate_by_team(), compute_roto_standings(), compute_standings_delta(), current_scoring_period(), espn_ip_to_innings(), fetch_current_roster_records(), fetch_current_roster_records_with_bench(), fetch_stats_for_periods() (+2 more)

### Community 24 - "5. Team Retrospectives & Verified 2026 Player Anchors"
Cohesion: 0.05
Nodes (38): 1. Dan — The Silver Bullets (`Team 5`), 1. Executive Summary & The 2026 Storyline, 1. The Six-Way Fractional OBP Dogfight (.003 Margin), 2026 Category Champions, 2026 League Storylines & Season Context Guide, 2. Final 2026 Regular Season Standings (Week 23), 2. The Heavyweight Home Run Jam (21-Homer Window), 2. Tim — Anti-lock Brake Systems (`Team 1`) (+30 more)

### Community 25 - "ingest_historical_trades.py"
Cohesion: 0.09
Nodes (24): compute_historical_post_trade_stats(), compute_post_trade_stats(), extract_stats_from_draft_record(), fetch_sheet_rows(), load_2026_daily_records(), load_draft_history(), main(), normalize_name() (+16 more)

### Community 26 - "live_command"
Cohesion: 0.21
Nodes (6): ask_command(), build_mlb_live_embed_fields(), check_trades_command(), _game_field_value(), live_command(), ping_command()

### Community 27 - "sys"
Cohesion: 0.17
Nodes (4): check_game_status(), get_periods_to_focus(), log_status(), should_scrape()

### Community 28 - "ProgressionView.jsx"
Cohesion: 0.26
Nodes (10): recharts, CANONICAL_OWNERS, getFranchiseLeaderboard(), getHistoricalSeasons(), normalizeOwner(), ALL_TIME_METRICS, formatVal(), ProgressionView() (+2 more)

### Community 29 - "is_season_active"
Cohesion: 0.25
Nodes (4): check_trade_notifications(), is_season_active(), scoring_period_for_date(), season_sentinel_loop()

### Community 30 - "BoxScoreModal.jsx"
Cohesion: 0.36
Nodes (6): BATTER_CATS, BoxScoreModal(), formatDisplayVal(), PITCHER_CATS, MatchupCard(), WeeklyView()

### Community 31 - "HEFTYBot"
Cohesion: 0.18
Nodes (4): generate_answer(), HEFTYBot, handle_ping(), offseason_sleep_service()

### Community 33 - "ingest_draft_infrastructure.py"
Cohesion: 0.50
Nodes (6): clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert()

### Community 34 - "ingest_draft_trades.py"
Cohesion: 0.31
Nodes (6): fetch_sheet_csv(), main(), parse_round_number(), parse_trade_log(), save_local_json(), upsert_to_supabase()

### Community 35 - "TeamAvatar"
Cohesion: 0.38
Nodes (9): FantasyBadge(), GameDetailModal(), OwnerDetailModal(), TeamAvatar(), buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard(), normalizeName() (+1 more)

### Community 37 - "build_live_roster_map"
Cohesion: 0.22
Nodes (5): build_live_roster_map(), build_roster_name_map(), fetch_espn_live_rosters(), _match_player(), normalize_name()

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
Nodes (3): PlayerRecord, SEASON_START, TEAM_IDS

### Community 44 - "rosterOptimizer.js"
Cohesion: 0.33
Nodes (10): BATTER_SLOT_DEFS, buildPlayerPositionRegistry(), getDailyBatterScore(), getDailyPitcherScore(), optimizeDailyTeamLineup(), PITCHER_SLOT_DEFS, simulateSeasonBestLineups(), solveOptimalAssignment() (+2 more)

### Community 45 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 52 - "aggregateStats"
Cohesion: 0.18
Nodes (16): fetchPlayerOverallStats(), overallStatsCache, PlayerHistoryModal(), loadOverall(), getDateFromPeriodId(), getPlayerAcquisition(), aggregateBenchStats(), aggregateStats() (+8 more)

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
- **255 isolated node(s):** `name`, `private`, `version`, `homepage`, `type` (+250 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 450 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **16 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `DraftRoomView.jsx`, `test_trade_and_recommender.mjs`, `LeagueHistoryView.jsx`, `TeamAvatar`, `TeamsView.jsx`, `package.json`, `rosterOptimizer.js`, `App.jsx`, `FullRosterView.jsx`, `aggregateStats`, `ProgressionView.jsx`, `BoxScoreModal.jsx`?**
  _High betweenness centrality (0.062) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.023) - this node is a cross-community bridge._
- **Why does `aggregateStats()` connect `aggregateStats` to `test_trade_and_recommender.mjs`, `TeamAvatar`, `LeagueHistoryView.jsx`, `schedule.js`, `TeamsView.jsx`, `rosterOptimizer.js`, `App.jsx`, `FullRosterView.jsx`, `ProgressionView.jsx`, `BoxScoreModal.jsx`?**
  _High betweenness centrality (0.012) - this node is a cross-community bridge._
- **What connects `name`, `private`, `version` to the rest of the system?**
  _255 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.09049773755656108 - nodes in this community are weakly interconnected._
- **Should `DraftRoomView.jsx` be split into smaller, more focused modules?**
  _Cohesion score 0.08484848484848485 - nodes in this community are weakly interconnected._
- **Should `test_trade_and_recommender.mjs` be split into smaller, more focused modules?**
  _Cohesion score 0.06262626262626263 - nodes in this community are weakly interconnected._