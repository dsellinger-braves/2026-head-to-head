-- Migration: 20260909_draft_asset_trades.sql
-- Description: Creates table to store future draft asset trades, picked rounds, and trade metadata.

CREATE TABLE IF NOT EXISTS public.draft_asset_trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    season_year INT NOT NULL DEFAULT 2026,
    target_draft_year INT NOT NULL DEFAULT 2027,
    sending_owner TEXT NOT NULL,
    from_team_id INT,
    receiving_owner TEXT NOT NULL,
    to_team_id INT,
    asset_type TEXT NOT NULL, -- 'Overall Pick', 'Budget', 'Player'
    asset_name TEXT NOT NULL, -- '14th round', 'Worst remaining', etc.
    round_num INT,           -- e.g. 14, 13, 32, 11, 22, 8, 20, 9, 25, 31, 22
    original_owner TEXT,     -- Owner who originally held the pick
    espn_transaction_id TEXT, -- Correlated ESPN trade transaction ID
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_trade_asset UNIQUE (trade_id, sending_owner, receiving_owner, asset_name)
);

CREATE INDEX IF NOT EXISTS idx_draft_asset_trades_target_year ON public.draft_asset_trades(target_draft_year);
CREATE INDEX IF NOT EXISTS idx_draft_asset_trades_from_team ON public.draft_asset_trades(from_team_id);
CREATE INDEX IF NOT EXISTS idx_draft_asset_trades_to_team ON public.draft_asset_trades(to_team_id);
CREATE INDEX IF NOT EXISTS idx_draft_asset_trades_espn_txn ON public.draft_asset_trades(espn_transaction_id);

-- Enable RLS
ALTER TABLE public.draft_asset_trades ENABLE ROW LEVEL SECURITY;

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow public read on draft_asset_trades') THEN
        CREATE POLICY "Allow public read on draft_asset_trades" ON public.draft_asset_trades FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Allow write on draft_asset_trades') THEN
        CREATE POLICY "Allow write on draft_asset_trades" ON public.draft_asset_trades FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
