-- Migration: Add Keeper Midpoint Discount Token columns
-- Created: 2026-09-12

ALTER TABLE public.draft_keepers 
ADD COLUMN IF NOT EXISTS token_applied BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS prior_cost NUMERIC DEFAULT NULL,
ADD COLUMN IF NOT EXISTS new_cost NUMERIC DEFAULT NULL;
