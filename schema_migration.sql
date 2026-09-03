-- Run this script in the SQL Editor of your new Supabase project (https://supabase.com/dashboard/project/wczdkcdqgtzlsbssogoz)

-- 1. Create the `transactions` table
CREATE TABLE IF NOT EXISTS public.transactions (
    espn_transaction_id TEXT PRIMARY KEY,
    league_id BIGINT NOT NULL,
    transaction_type TEXT,
    transaction_date TIMESTAMPTZ,
    scoring_period_id INT,
    to_team_id INT,
    from_team_id INT,
    player_id INT,
    player_name TEXT,
    raw_type TEXT
);

-- 2. Create the `player_daily_stats` table
CREATE TABLE IF NOT EXISTS public.player_daily_stats (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    league_id BIGINT NOT NULL,
    team_id INT,
    scoring_period_id INT,
    player_id INT,
    full_name TEXT,
    lineup_slot_id INT,
    stats JSONB,
    updated_at TIMESTAMPTZ,
    -- Add a unique constraint to avoid duplicating the same player's stats for the same period and league
    UNIQUE (league_id, scoring_period_id, player_id)
);

-- Note: In `active-stats-pull.py`, the old code used `.delete().in_('scoring_period_id', ...)` to clear periods.
-- Going forward, it will filter by `league_id` too, or use `upsert`.
