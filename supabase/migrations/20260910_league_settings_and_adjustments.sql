-- Migration: 20260910_league_settings_and_adjustments.sql
-- Description: Extends existing league_settings table for keeper lock deadline & baseline budget,
--              adds manual punitive/award adjustments to draft_team_budgets,
--              and configures trade approval workflow.

-- 1. Add manual_adjustment and adjustment_notes to draft_team_budgets
ALTER TABLE public.draft_team_budgets
ADD COLUMN IF NOT EXISTS manual_adjustment NUMERIC NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS adjustment_notes TEXT;

-- 2. Extend existing league_settings table with offseason configuration columns
ALTER TABLE public.league_settings
ADD COLUMN IF NOT EXISTS keeper_lock_deadline TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS base_budget NUMERIC NOT NULL DEFAULT 100,
ADD COLUMN IF NOT EXISTS trades_locked BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS updated_by TEXT;

-- Update default values for the active 2026 row
UPDATE public.league_settings
SET keeper_lock_deadline = COALESCE(keeper_lock_deadline, '2026-03-22 23:59:59-04'::timestamptz),
    base_budget = COALESCE(base_budget, 100),
    trades_locked = COALESCE(trades_locked, false),
    updated_by = 'Commissioner'
WHERE league_id = 130215;

-- 3. Update status constraint on league_trade_proposals to require commissioner approval
ALTER TABLE public.league_trade_proposals 
DROP CONSTRAINT IF EXISTS league_trade_proposals_status_check;

ALTER TABLE public.league_trade_proposals 
ADD CONSTRAINT league_trade_proposals_status_check 
CHECK (status IN ('pending', 'accepted_by_partner', 'approved', 'declined', 'cancelled'));

-- 4. Ensure RLS policies allow update on league_settings
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on league_settings') THEN
        CREATE POLICY "Allow write on league_settings" ON public.league_settings FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
