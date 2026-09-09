-- Migration: 20260909_draft_warehouse_schema.sql
-- Description: Creates warehouse tables for multi-year draft history, player registry, season finishes, and league settings.

-- 1. Universal Player Registry
CREATE TABLE IF NOT EXISTS public.dim_players (
    player_id INT PRIMARY KEY,
    mlbam_id INT,
    fangraphs_id VARCHAR(32),
    full_name TEXT NOT NULL,
    clean_name TEXT NOT NULL,
    primary_position VARCHAR(10),
    mlb_team VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_players_clean_name ON public.dim_players(clean_name);
CREATE INDEX IF NOT EXISTS idx_dim_players_mlbam ON public.dim_players(mlbam_id);
CREATE INDEX IF NOT EXISTS idx_dim_players_fg ON public.dim_players(fangraphs_id);

-- 2. Multi-Season Draft Archive
CREATE TABLE IF NOT EXISTS public.draft_picks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL,
    round INT,
    pick INT,
    overall_pick INT NOT NULL,
    team_owner TEXT NOT NULL,
    player_id INT,
    player_name TEXT NOT NULL,
    player_position TEXT,
    player_team TEXT,
    is_keeper BOOLEAN DEFAULT FALSE,
    picked_at TIMESTAMPTZ,
    ai_commentary TEXT,
    CONSTRAINT unique_season_overall UNIQUE (season_year, overall_pick)
);

CREATE INDEX IF NOT EXISTS idx_draft_picks_season ON public.draft_picks(season_year);
CREATE INDEX IF NOT EXISTS idx_draft_picks_owner ON public.draft_picks(team_owner);
CREATE INDEX IF NOT EXISTS idx_draft_picks_player_id ON public.draft_picks(player_id);

-- 3. Historical Season Finishes
CREATE TABLE IF NOT EXISTS public.historical_finishes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL,
    team_owner TEXT NOT NULL,
    final_place INT,
    regular_season_wins INT,
    regular_season_losses INT,
    regular_season_ties INT,
    total_roto_points NUMERIC,
    is_active_owner BOOLEAN DEFAULT TRUE,
    category_ranks JSONB,
    raw_data JSONB,
    CONSTRAINT unique_season_owner UNIQUE (season_year, team_owner)
);

CREATE INDEX IF NOT EXISTS idx_historical_finishes_season ON public.historical_finishes(season_year);
CREATE INDEX IF NOT EXISTS idx_historical_finishes_owner ON public.historical_finishes(team_owner);

-- 4. Dynamic League Configuration
CREATE TABLE IF NOT EXISTS public.league_settings (
    league_id BIGINT PRIMARY KEY,
    current_season INT NOT NULL DEFAULT 2026,
    scoring_type TEXT DEFAULT 'H2H_EACH_CATEGORY',
    total_rounds INT DEFAULT 21,
    time_limit_seconds INT DEFAULT 60,
    draft_status TEXT DEFAULT 'PRE_DRAFT',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed initial league settings for 130215
INSERT INTO public.league_settings (league_id, current_season, total_rounds, draft_status)
VALUES (130215, 2026, 21, 'PRE_DRAFT')
ON CONFLICT (league_id) DO UPDATE 
SET current_season = EXCLUDED.current_season,
    updated_at = NOW();

-- Enable RLS and public read policies
ALTER TABLE public.dim_players ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.draft_picks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.historical_finishes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.league_settings ENABLE ROW LEVEL SECURITY;

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on dim_players') THEN
        CREATE POLICY "Allow public read on dim_players" ON public.dim_players FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on dim_players') THEN
        CREATE POLICY "Allow write on dim_players" ON public.dim_players FOR ALL USING (true) WITH CHECK (true);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on draft_picks') THEN
        CREATE POLICY "Allow public read on draft_picks" ON public.draft_picks FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on draft_picks') THEN
        CREATE POLICY "Allow write on draft_picks" ON public.draft_picks FOR ALL USING (true) WITH CHECK (true);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on historical_finishes') THEN
        CREATE POLICY "Allow public read on historical_finishes" ON public.historical_finishes FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on historical_finishes') THEN
        CREATE POLICY "Allow write on historical_finishes" ON public.historical_finishes FOR ALL USING (true) WITH CHECK (true);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on league_settings') THEN
        CREATE POLICY "Allow public read on league_settings" ON public.league_settings FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on league_settings') THEN
        CREATE POLICY "Allow write on league_settings" ON public.league_settings FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
