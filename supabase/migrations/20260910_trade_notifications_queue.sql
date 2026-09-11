-- Migration: 20260910_trade_notifications_queue.sql
-- Description: Creates trade_notifications table to queue trade lifecycle events
--              (proposed, accepted, declined, countered, approved) for Discord DM delivery.

CREATE TABLE IF NOT EXISTS public.trade_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_proposal_id UUID REFERENCES public.league_trade_proposals(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (event_type IN ('proposed', 'accepted', 'declined', 'countered', 'approved')),
    sender_team_id INT NOT NULL,
    sender_owner TEXT NOT NULL,
    recipient_team_id INT NOT NULL,
    recipient_owner TEXT NOT NULL,
    recipient_discord_id TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'dm_blocked', 'failed')),
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    sent_at TIMESTAMPTZ
);

-- Index for fast queue polling
CREATE INDEX IF NOT EXISTS idx_trade_notifications_status ON public.trade_notifications (status, created_at);

-- Enable RLS
ALTER TABLE public.trade_notifications ENABLE ROW LEVEL SECURITY;

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on trade_notifications') THEN
        CREATE POLICY "Allow public read on trade_notifications" ON public.trade_notifications FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on trade_notifications') THEN
        CREATE POLICY "Allow write on trade_notifications" ON public.trade_notifications FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
