-- Migration: Draft Compensation Picks, Team Budgets, and Keepers
-- Created: 2026-09-09

CREATE TABLE IF NOT EXISTS public.draft_team_budgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL DEFAULT 2026,
    owner TEXT NOT NULL,
    team_id INT NOT NULL,
    finish_rank INT,
    base_budget NUMERIC NOT NULL DEFAULT 100,
    keeper_spend NUMERIC NOT NULL DEFAULT 0,
    comp_pick_spend NUMERIC NOT NULL DEFAULT 0,
    comp_pick_income NUMERIC NOT NULL DEFAULT 0,
    final_budget NUMERIC NOT NULL DEFAULT 100,
    net_picks INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_draft_team_budgets UNIQUE (season_year, owner)
);

CREATE TABLE IF NOT EXISTS public.draft_compensation_picks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL DEFAULT 2026,
    owner TEXT NOT NULL,
    team_id INT NOT NULL,
    action_type TEXT NOT NULL, -- 'BOUGHT', 'SOLD', 'OFFSET_LOST'
    round_num INT NOT NULL,
    cost_or_income NUMERIC NOT NULL DEFAULT 0,
    overall_pick_num INT,
    notes TEXT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_draft_compensation_picks UNIQUE (season_year, owner, action_type, round_num)
);

CREATE TABLE IF NOT EXISTS public.draft_keepers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL DEFAULT 2026,
    owner TEXT NOT NULL,
    team_id INT NOT NULL,
    keeper_slot INT NOT NULL, -- 1 to 5
    player_name TEXT NOT NULL,
    espn_player_id TEXT,
    position TEXT,
    mlb_team TEXT,
    rank INT,
    cost NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_draft_keepers UNIQUE (season_year, owner, keeper_slot)
);

-- Enable Row Level Security (RLS)
ALTER TABLE public.draft_team_budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.draft_compensation_picks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.draft_keepers ENABLE ROW LEVEL SECURITY;

-- Allow public read access
CREATE POLICY "Allow public read draft_team_budgets" ON public.draft_team_budgets
    FOR SELECT USING (true);

CREATE POLICY "Allow public read draft_compensation_picks" ON public.draft_compensation_picks
    FOR SELECT USING (true);

CREATE POLICY "Allow public read draft_keepers" ON public.draft_keepers
    FOR SELECT USING (true);

-- Allow public insert/update/upsert
CREATE POLICY "Allow public insert/update draft_team_budgets" ON public.draft_team_budgets
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Allow public insert/update draft_compensation_picks" ON public.draft_compensation_picks
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Allow public insert/update draft_keepers" ON public.draft_keepers
    FOR ALL USING (true) WITH CHECK (true);
