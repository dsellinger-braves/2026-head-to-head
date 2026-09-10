-- Migration: 20260910_discord_auth_and_profiles.sql
-- Description: Sets up league_profiles table for Discord OAuth account tracking,
--              pre-seeds the 9 owners with Dan & Adrian as commissioners,
--              links auth.users to profiles on login, and creates league_trade_proposals.

CREATE TABLE IF NOT EXISTS public.league_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    discord_username TEXT NOT NULL UNIQUE,
    discord_id TEXT UNIQUE,
    team_id INT NOT NULL UNIQUE,
    owner_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'owner' CHECK (role IN ('owner', 'commissioner')),
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Pre-seed the 9 league owners
INSERT INTO public.league_profiles (discord_username, team_id, owner_name, role)
VALUES 
    ('dsellinger', 5, 'Dan', 'commissioner'),
    ('adriaxx', 2, 'Adrian', 'commissioner'),
    ('aznchuy', 1, 'Tim', 'owner'),
    ('ghutch', 3, 'Garrett', 'owner'),
    ('anilbhairo', 6, 'Anil', 'owner'),
    ('ay0h', 8, 'Alex', 'owner'),
    ('senorspice', 12, 'Will', 'owner'),
    ('mrussell38', 13, 'Mark', 'owner'),
    ('pston3', 14, 'Preston', 'owner')
ON CONFLICT (discord_username) DO UPDATE 
SET role = EXCLUDED.role, 
    team_id = EXCLUDED.team_id, 
    owner_name = EXCLUDED.owner_name,
    updated_at = now();

-- Trade Proposals Table for 2-Party Offseason Draft Asset & Keeper Trading
CREATE TABLE IF NOT EXISTS public.league_trade_proposals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL DEFAULT 2026,
    proposing_team_id INT NOT NULL,
    proposing_owner TEXT NOT NULL,
    target_team_id INT NOT NULL,
    target_owner TEXT NOT NULL,
    offered_assets JSONB NOT NULL DEFAULT '[]'::jsonb,
    requested_assets JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'declined', 'cancelled', 'approved')),
    notes TEXT,
    proposed_at TIMESTAMPTZ DEFAULT now(),
    responded_at TIMESTAMPTZ,
    responded_by TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Enable RLS
ALTER TABLE public.league_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.league_trade_proposals ENABLE ROW LEVEL SECURITY;

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on league_profiles') THEN
        CREATE POLICY "Allow public read on league_profiles" ON public.league_profiles FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on league_profiles') THEN
        CREATE POLICY "Allow write on league_profiles" ON public.league_profiles FOR ALL USING (true) WITH CHECK (true);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on league_trade_proposals') THEN
        CREATE POLICY "Allow public read on league_trade_proposals" ON public.league_trade_proposals FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on league_trade_proposals') THEN
        CREATE POLICY "Allow write on league_trade_proposals" ON public.league_trade_proposals FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
