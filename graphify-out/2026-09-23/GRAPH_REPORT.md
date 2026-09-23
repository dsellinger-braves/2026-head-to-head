# Graph Report - 2026 Head to Head Heftystrong  (2026-09-23)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 810 nodes · 1702 edges · 52 communities (47 shown, 5 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 10 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `e84cda85`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- discord-daily-recap.py
- DraftRoomView.jsx
- test_trade_and_recommender.mjs
- LeagueHistoryView.jsx
- compute_player_values.py
- projections.py
- App.jsx
- TeamsView.jsx
- build_mlb_live_data
- package.json
- active-stats-pull.py
- discord-bot.py
- transaction-scraper.py
- ingest_historical_trades.py
- FullRosterView.jsx
- supabaseClient.js
- build_context
- update_league_context.py
- historical.py
- update_live_pickem_standings.py
- fetch_current_roster_records
- devDependencies
- KeepersBudgetsPanel.jsx
- schedule.js
- build_recap_context.py
- sync_draft_pool.py
- ask_command
- ingest_pickem.py
- ProgressionView.jsx
- PickemView.jsx
- aggregateStats
- react
- os
- ingest_draft_infrastructure.py
- ingest_draft_trades.py
- LiveScoreboardView.jsx
- DraftCapitalView.jsx
- build_live_roster_map
- HEFTYBot
- scripts
- migrate_gcs_to_supabase.py
- deploy
- index.ts
- TeamAvatar.jsx
- OwnerLandingView.jsx
- PlayerValuationsView.jsx
- dependencies
- AuthContext.jsx
- vite
- overrides

## God Nodes (most connected - your core abstractions)
1. `react` - 38 edges
2. `build_context()` - 30 edges
3. `aggregateStats()` - 30 edges
4. `TEAMS` - 25 edges
5. `run_weekly_recap()` - 22 edges
6. `TeamAvatar()` - 20 edges
7. `run_daily_recap()` - 18 edges
8. `DraftRoomView()` - 17 edges
9. `useAuth()` - 17 edges
10. `getDateFromPeriodId()` - 15 edges

## Surprising Connections (you probably didn't know these)
- `ESPN stores IP as total outs. Divide by 3 for decimal innings.` --rationale_for--> `espn_ip_to_innings()`  [EXTRACTED]
  discord-bot.py → discord-daily-recap.py
- `Remove bench and IL player records — their stats don't count in roto.` --rationale_for--> `filter_active()`  [EXTRACTED]
  discord-bot.py → discord-daily-recap.py
- `run_weekly_recap()` --calls--> `format_all_active_owner_summaries()`  [EXTRACTED]
  discord-daily-recap.py → historical.py
- `run_weekly_recap()` --calls--> `format_league_champions()`  [EXTRACTED]
  discord-daily-recap.py → historical.py
- `run_weekly_recap()` --calls--> `format_owner_history()`  [EXTRACTED]
  discord-daily-recap.py → historical.py

## Import Cycles
- None detected.

## Communities (52 total, 5 thin omitted)

### Community 0 - "discord-daily-recap.py"
Cohesion: 0.08
Nodes (52): Remove bench and IL player records — their stats don't count in roto., aggregate_by_team(), build_daily_prompt(), build_weekly_prompt(), compute_averages(), compute_roto_standings(), compute_standings_delta(), compute_weekly_team_rates() (+44 more)

### Community 1 - "DraftRoomView.jsx"
Cohesion: 0.07
Nodes (42): getPlayerHeadshotUrl(), globalPlayerLookup, handleHeadshotError(), updateGlobalPlayerLookup(), callESPNProxy(), callGemini(), compute2027DraftPicks(), DEFAULT_OWNER_PROFILES (+34 more)

### Community 2 - "test_trade_and_recommender.mjs"
Cohesion: 0.06
Nodes (40): ref_node_assert, src_data_draft2026, src_data_historicaltrades, src_data_keeperinput2026, src_data_transactions2026, findWaiverReplacements(), isPositionMatch(), SLOT_TO_POS (+32 more)

### Community 3 - "LeagueHistoryView.jsx"
Cohesion: 0.08
Nodes (39): idb-keyval, BAT_CATS, BATTER_SLOT_ORDER, DayRosterModal(), formatVal(), PITCH_CATS, PITCHER_SLOT_ORDER, StatCell() (+31 more)

### Community 4 - "compute_player_values.py"
Cohesion: 0.10
Nodes (18): math, calculate_category_benchmarks(), compute_player_season_pr(), fetch_csv_from_gsheet(), fetch_live_actuals(), fetch_live_fangraphs(), get_price_for_rank(), get_supabase_headers() (+10 more)

### Community 5 - "projections.py"
Cohesion: 0.14
Nodes (22): build_projection_map(), _compute_roto_points(), _espn_ip_to_innings(), _estimate_qs(), fetch_projections(), _fg_get(), forecast_final_standings(), forecast_ros_only_standings() (+14 more)

### Community 6 - "App.jsx"
Cohesion: 0.15
Nodes (18): App(), AVAILABLE_SEASONS, getSubTabsFromHash(), getViewFromHash(), HASH_TO_VIEW, OFFSEASON_VIEWS, NOTE: Make sure to export calculateTrioMatchupResult from scoring.js!, VIEW_TO_HASH (+10 more)

### Community 7 - "TeamsView.jsx"
Cohesion: 0.17
Nodes (19): calculateRotoPoints(), getStatMeta(), MINUTIAE_STATS, SCORING_CATS, BATTING_CATS, ESPN_STAT_NAMES, parseRecord(), PITCHING_CATS (+11 more)

### Community 8 - "build_mlb_live_data"
Cohesion: 0.10
Nodes (21): build_mlb_live_context(), build_mlb_live_data(), fetch_mlb_boxscore(), fetch_mlb_schedule_today(), _game_status_label(), _game_time_et(), _match_player(), _mlb_get() (+13 more)

### Community 9 - "package.json"
Cohesion: 0.11
Nodes (19): homepage, name, private, type, version, autoprefixer, date-fns, eslint (+11 more)

### Community 10 - "active-stats-pull.py"
Cohesion: 0.11
Nodes (15): datetime, google_cloud, check_game_status(), get_periods_to_focus(), log_status(), date, mlb_schedule_check.py ───────────────────── Drop-in module for active-stats-…, Print a human-readable summary of today's game status. (+7 more)

### Community 11 - "discord-bot.py"
Cohesion: 0.13
Nodes (19): asyncio, before_loop, discord, aggregate_by_player(), before_check_trade_notifications(), canonical_owner_name(), check_trade_notifications(), format_asset_list() (+11 more)

### Community 12 - "transaction-scraper.py"
Cohesion: 0.12
Nodes (16): json, time, enrich_player_names(), fetch_activity_trades(), fetch_all_player_names(), fetch_historical_transactions(), fetch_transactions(), parse_activity_trades() (+8 more)

### Community 13 - "ingest_historical_trades.py"
Cohesion: 0.19
Nodes (18): compute_historical_post_trade_stats(), compute_post_trade_stats(), extract_stats_from_draft_record(), fetch_sheet_rows(), load_2026_daily_records(), load_draft_history(), main(), normalize_name() (+10 more)

### Community 14 - "FullRosterView.jsx"
Cohesion: 0.18
Nodes (15): fetchPlayerOverallStats(), overallStatsCache, PlayerHistoryModal(), loadOverall(), getDateFromPeriodId(), getPlayerAcquisition(), BATTER_SLOT_ORDER, formatOBP() (+7 more)

### Community 15 - "supabaseClient.js"
Cohesion: 0.14
Nodes (12): @supabase/supabase-js, src_data_draftassettrades2026, src_data_transactions_historical, DEFAULT_SUPABASE_ANON_KEY, DEFAULT_SUPABASE_URL, supabase, supabaseUrl, AVAILABLE_YEARS (+4 more)

### Community 16 - "build_context"
Cohesion: 0.15
Nodes (16): build_context(), fetch_recent_transactions(), fetch_stats_up_to_period(), fetch_team_transactions(), format_trades_block(), format_transaction_context(), format_transaction_with_stats(), get_supabase() (+8 more)

### Community 17 - "update_league_context.py"
Cohesion: 0.20
Nodes (13): collections, calculate_roto_points(), compute_playoff_matchups(), compute_rates(), fetch_supabase_table(), get_period_map(), get_stat(), Computes live head-to-head category scores for Playoff Semi-Finals (Week 24)… (+5 more)

### Community 18 - "historical.py"
Cohesion: 0.20
Nodes (13): _canonical(), format_all_active_owner_summaries(), format_league_champions(), format_owner_history(), get_owner_seasons(), load_historical_data(), historical.py — shared historical context module Import this in discord-bot.py…, Championship counts for all active owners. (+5 more)

### Community 19 - "update_live_pickem_standings.py"
Cohesion: 0.26
Nodes (14): build_live_in_progress_snapshot(), evaluate_owner_projected_scores(), fetch_mlb_standings(), fetch_projected_war_leaders(), get_team_code(), matches_team_or_val(), normalize_text(), Any (+6 more)

### Community 20 - "fetch_current_roster_records"
Cohesion: 0.16
Nodes (14): aggregate_by_team(), compute_roto_standings(), compute_standings_delta(), current_scoring_period(), espn_ip_to_innings(), fetch_current_roster_records(), fetch_current_roster_records_with_bench(), fetch_stats_for_periods() (+6 more)

### Community 21 - "devDependencies"
Cohesion: 0.14
Nodes (14): devDependencies, autoprefixer, eslint, @eslint/js, eslint-plugin-react-hooks, eslint-plugin-react-refresh, gh-pages, globals (+6 more)

### Community 22 - "KeepersBudgetsPanel.jsx"
Cohesion: 0.19
Nodes (11): calculateKeeperCostFromRank(), COMP_BUY_PRICES, COMP_SELL_PRICES, DRAFT_MANAGERS, KeepersBudgetsPanel(), normalizeManager(), src_data_compensationpicks2026, src_data_teambudgets2026 (+3 more)

### Community 23 - "schedule.js"
Cohesion: 0.22
Nodes (9): generateSchedule(), getPeriodRangeForWeek(), getTriosMatchups(), SEASON_DATES, SEASON_START_DATES, TEAMS, calculateStandings(), getStandings() (+1 more)

### Community 24 - "build_recap_context.py"
Cohesion: 0.18
Nodes (12): detect_active_roto_battles(), ensure_context_is_fresh(), fmt_cat_val(), load_league_context(), Format a category stat value cleanly., Dynamically identify active category volatility and standings deadlocks where…, pipelines/build_recap_context.py Fetches real-time MLB news from ESPN and…, Ensure data/league_context.json is updated from Supabase if needed. (+4 more)

### Community 25 - "sync_draft_pool.py"
Cohesion: 0.24
Nodes (12): extract_player_record(), fetch_espn_players(), get_current_season(), main(), normalize_name(), Any, Client, pipelines/sync_draft_pool.py Automated replacement for fantasy-baseball… (+4 more)

### Community 26 - "ask_command"
Cohesion: 0.21
Nodes (12): command, describe, ask_command(), build_mlb_live_embed_fields(), check_trades_command(), _game_field_value(), live_command(), ping_command() (+4 more)

### Community 27 - "ingest_pickem.py"
Cohesion: 0.26
Nodes (11): bootstrap_future_season(), fetch_tab_rows(), get_supabase_headers(), get_team_id(), ingest_season(), normalize_owner(), pipelines/ingest_pickem.py Annual MLB Pick'em Ingestion & Management Pipeline…, Fetch raw CSV rows for a tab from Google Sheets. (+3 more)

### Community 28 - "ProgressionView.jsx"
Cohesion: 0.29
Nodes (10): src_data_historicalfinishes, CANONICAL_OWNERS, getFranchiseLeaderboard(), getHistoricalSeasons(), normalizeOwner(), ALL_TIME_METRICS, formatVal(), ProgressionView() (+2 more)

### Community 29 - "PickemView.jsx"
Cohesion: 0.27
Nodes (8): findMlbTeam(), LEAGUE_OWNERS, MLB_DIVISIONS, MLB_TEAMS, PICKEM_RULES, PROMINENT_AWARD_CANDIDATES, teamsMatch(), PickemView()

### Community 30 - "aggregateStats"
Cohesion: 0.27
Nodes (10): aggregateBenchStats(), aggregateStats(), BenchStatsView(), ESPN_STAT_NAMES, parseRecordStats(), BAT_COLS, formatStat(), getLabel() (+2 more)

### Community 31 - "react"
Cohesion: 0.42
Nodes (6): react, CommishActiveBanner(), CommishCheckoutModal(), UserNavWidget(), AuthContext, useAuth()

### Community 32 - "os"
Cohesion: 0.29
Nodes (5): dotenv, migrate_table(), Client, os, supabase

### Community 33 - "ingest_draft_infrastructure.py"
Cohesion: 0.36
Nodes (9): csv, io, clean_currency(), fetch_csv(), ingest_compensation_picks_and_budgets(), ingest_keepers(), main(), postgrest_upsert() (+1 more)

### Community 34 - "ingest_draft_trades.py"
Cohesion: 0.31
Nodes (9): fetch_sheet_csv(), main(), parse_round_number(), parse_trade_log(), Finds the trade table starting at the header row containing 'Trade ID'., pipelines/ingest_draft_trades.py Ingests traded draft assets from the Google…, Parses round number from asset name (e.g., '14th round' -> 14, 'Worst…, save_local_json() (+1 more)

### Community 35 - "LiveScoreboardView.jsx"
Cohesion: 0.49
Nodes (6): GameDetailModal(), buildRosterDictionary(), fetchGameBoxscore(), fetchLiveScoreboard(), normalizeName(), LiveScoreboardView()

### Community 36 - "DraftCapitalView.jsx"
Cohesion: 0.36
Nodes (9): canonicalOwnerName(), compute2027DraftPicks(), DRAFT_OWNERS, DraftCapitalView(), enqueueTradeNotification(), getTeamId(), isPickInAssets(), isPlayerInAssets() (+1 more)

### Community 37 - "build_live_roster_map"
Cohesion: 0.29
Nodes (8): build_live_roster_map(), build_roster_name_map(), fetch_espn_live_rosters(), normalize_name(), Lowercase, strip punctuation and suffixes for fuzzy matching., Build {normalized_name: {owner, team_id, full_name}} from a set of records.…, Query ESPN Fantasy API directly for current roster assignments. Used by /live…, Best-available roster map for /live. Tries ESPN directly first (real-time),…

### Community 38 - "HEFTYBot"
Cohesion: 0.25
Nodes (3): generate_answer(), HEFTYBot, Message

### Community 39 - "scripts"
Cohesion: 0.25
Nodes (8): scripts, build, deploy, dev, lint, predeploy, preview, start

### Community 40 - "migrate_gcs_to_supabase.py"
Cohesion: 0.46
Nodes (7): fetch_json(), main(), migrate_draft_history(), migrate_historical_finishes(), normalize_name(), Client, pipelines/migrate_gcs_to_supabase.py Migrates historical draft picks (draft-…

### Community 41 - "deploy"
Cohesion: 0.25
Nodes (7): build, builder, deploy, restartPolicyMaxRetries, restartPolicyType, startCommand, $schema

### Community 42 - "index.ts"
Cohesion: 0.25
Nodes (4): ref_https, PlayerRecord, SEASON_START, TEAM_IDS

### Community 43 - "TeamAvatar.jsx"
Cohesion: 0.46
Nodes (4): BoxScoreModal(), MatchupCard(), TeamAvatar(), CATEGORIES

### Community 44 - "OwnerLandingView.jsx"
Cohesion: 0.32
Nodes (7): evaluatePlayerCapital(), DEFAULT_POS_MAP, getPlayerPositions(), LEAGUE_MANAGERS, MLB_TEAMS, OwnerLandingView(), SLOT_MAP

### Community 46 - "dependencies"
Cohesion: 0.29
Nodes (7): dependencies, date-fns, idb-keyval, react, react-dom, recharts, @supabase/supabase-js

### Community 47 - "AuthContext.jsx"
Cohesion: 0.43
Nodes (5): AuthProvider(), initAuth(), cleanupOAuthHash(), STATIC_LEAGUE_PROFILES, src_index

## Knowledge Gaps
- **136 isolated node(s):** `PlayerRecord`, `globalPlayerLookup`, `DEFAULT_OWNER_PROFILES`, `DRAFT_OWNERS`, `GEMINI_MODELS` (+131 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 318 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **5 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `DraftRoomView.jsx`, `test_trade_and_recommender.mjs`, `LeagueHistoryView.jsx`, `App.jsx`, `TeamsView.jsx`, `package.json`, `FullRosterView.jsx`, `supabaseClient.js`, `KeepersBudgetsPanel.jsx`, `schedule.js`, `ProgressionView.jsx`, `PickemView.jsx`, `aggregateStats`, `LiveScoreboardView.jsx`, `DraftCapitalView.jsx`, `TeamAvatar.jsx`, `OwnerLandingView.jsx`, `PlayerValuationsView.jsx`, `AuthContext.jsx`?**
  _High betweenness centrality (0.087) - this node is a cross-community bridge._
- **Why does `devDependencies` connect `devDependencies` to `package.json`?**
  _High betweenness centrality (0.016) - this node is a cross-community bridge._
- **Why does `scripts` connect `scripts` to `package.json`?**
  _High betweenness centrality (0.009) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `build_context()` (e.g. with `ask_command()` and `.on_message()`) actually correct?**
  _`build_context()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `PlayerRecord`, `globalPlayerLookup`, `DEFAULT_OWNER_PROFILES` to the rest of the system?**
  _136 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `discord-daily-recap.py` be split into smaller, more focused modules?**
  _Cohesion score 0.08080808080808081 - nodes in this community are weakly interconnected._
- **Should `DraftRoomView.jsx` be split into smaller, more focused modules?**
  _Cohesion score 0.0707070707070707 - nodes in this community are weakly interconnected._