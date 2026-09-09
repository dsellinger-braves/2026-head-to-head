-- ==============================================================================
-- 20260909_pickem_schema.sql
-- Annual MLB Pick'em Forecasting Platform Schema
-- ==============================================================================

-- 1. Create `pickem_seasons` table
CREATE TABLE IF NOT EXISTS public.pickem_seasons (
    season_year INT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'open', -- 'open', 'locked', 'completed'
    picks_deadline TIMESTAMPTZ,
    scoring_rules JSONB DEFAULT '{
        "division_winner": 3,
        "playoff_team": 2,
        "pennant": 5,
        "world_series": 7,
        "award": 4,
        "wins_losses": 3
    }'::jsonb,
    budget_rules JSONB DEFAULT '{
        "1": 4,
        "2": 3,
        "3": 3,
        "4": 3,
        "5": 1
    }'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 2. Create `pickem_questions` table
CREATE TABLE IF NOT EXISTS public.pickem_questions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL REFERENCES public.pickem_seasons(season_year) ON DELETE CASCADE,
    question_key TEXT NOT NULL,
    question_label TEXT NOT NULL,
    category TEXT NOT NULL, -- 'division', 'wild_card', 'playoff_result', 'award', 'extremes'
    options_type TEXT NOT NULL DEFAULT 'mlb_team', -- 'mlb_team', 'player_text'
    display_order INT NOT NULL DEFAULT 0,
    correct_answer TEXT,
    points_exact INT NOT NULL DEFAULT 3,
    points_partial INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_pickem_question UNIQUE (season_year, question_key)
);

-- 3. Create `pickem_picks` table
CREATE TABLE IF NOT EXISTS public.pickem_picks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL REFERENCES public.pickem_seasons(season_year) ON DELETE CASCADE,
    question_id UUID NOT NULL REFERENCES public.pickem_questions(id) ON DELETE CASCADE,
    owner_name TEXT NOT NULL,
    team_id INT,
    pick_value TEXT NOT NULL,
    points_awarded INT NOT NULL DEFAULT 0,
    is_correct BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_pickem_pick UNIQUE (season_year, question_id, owner_name)
);

-- 4. Create `pickem_scores` table
CREATE TABLE IF NOT EXISTS public.pickem_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL REFERENCES public.pickem_seasons(season_year) ON DELETE CASCADE,
    owner_name TEXT NOT NULL,
    team_id INT,
    total_points INT NOT NULL DEFAULT 0,
    place INT,
    budget_awarded TEXT,
    category_scores JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_pickem_score UNIQUE (season_year, owner_name)
);

-- Enable Row Level Security (RLS)
ALTER TABLE public.pickem_seasons ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pickem_questions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pickem_picks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pickem_scores ENABLE ROW LEVEL SECURITY;

-- Read policies for public/anon
DROP POLICY IF EXISTS "Allow public read pickem_seasons" ON public.pickem_seasons;
CREATE POLICY "Allow public read pickem_seasons" ON public.pickem_seasons FOR SELECT USING (true);

DROP POLICY IF EXISTS "Allow public read pickem_questions" ON public.pickem_questions;
CREATE POLICY "Allow public read pickem_questions" ON public.pickem_questions FOR SELECT USING (true);

DROP POLICY IF EXISTS "Allow public read pickem_picks" ON public.pickem_picks;
CREATE POLICY "Allow public read pickem_picks" ON public.pickem_picks FOR SELECT USING (true);

DROP POLICY IF EXISTS "Allow public read pickem_scores" ON public.pickem_scores;
CREATE POLICY "Allow public read pickem_scores" ON public.pickem_scores FOR SELECT USING (true);

-- Write policies for anon/service (allows owners to submit picks and pipeline to ingest)
DROP POLICY IF EXISTS "Allow write pickem_seasons" ON public.pickem_seasons;
CREATE POLICY "Allow write pickem_seasons" ON public.pickem_seasons FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow write pickem_questions" ON public.pickem_questions;
CREATE POLICY "Allow write pickem_questions" ON public.pickem_questions FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow write pickem_picks" ON public.pickem_picks;
CREATE POLICY "Allow write pickem_picks" ON public.pickem_picks FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow write pickem_scores" ON public.pickem_scores;
CREATE POLICY "Allow write pickem_scores" ON public.pickem_scores FOR ALL USING (true) WITH CHECK (true);

-- Indexes for lightning fast queries
CREATE INDEX IF NOT EXISTS idx_pickem_questions_year ON public.pickem_questions(season_year, display_order);
CREATE INDEX IF NOT EXISTS idx_pickem_picks_lookup ON public.pickem_picks(season_year, owner_name);
CREATE INDEX IF NOT EXISTS idx_pickem_scores_year ON public.pickem_scores(season_year, total_points DESC);
