-- Migration: 20260911_commish_audit_logs.sql
-- Description: Creates commissioner_audit_logs table to track break-glass commissioner
--              checkouts, trade overrides, manual edits, and roster adjustments.

CREATE TABLE IF NOT EXISTS public.commissioner_audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year INT NOT NULL DEFAULT 2026,
    commissioner_name TEXT NOT NULL,
    commissioner_team_id INT NOT NULL,
    commissioner_discord_id TEXT,
    action_type TEXT NOT NULL,
    action_description TEXT NOT NULL,
    target_team_id INT,
    target_owner TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Index for fast lookup by time and commissioner
CREATE INDEX IF NOT EXISTS idx_commish_audit_created ON public.commissioner_audit_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_commish_audit_name ON public.commissioner_audit_logs (commissioner_name);

-- Enable RLS
ALTER TABLE public.commissioner_audit_logs ENABLE ROW LEVEL SECURITY;

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on commissioner_audit_logs') THEN
        CREATE POLICY "Allow public read on commissioner_audit_logs" ON public.commissioner_audit_logs FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on commissioner_audit_logs') THEN
        CREATE POLICY "Allow write on commissioner_audit_logs" ON public.commissioner_audit_logs FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
