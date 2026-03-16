-- ============================================================
-- Migration 008: Add first_seen_date to vehicles for days-on-lot tracking
-- Run this in the Supabase SQL Editor
-- ============================================================

-- Add the column (nullable, no default — we'll backfill existing rows)
ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS first_seen_date DATE;

-- Backfill: set first_seen_date to the snapshot's report_date for all existing vehicles
UPDATE vehicles v
SET first_seen_date = rs.report_date
FROM report_snapshots rs
WHERE v.snapshot_id = rs.id
  AND v.first_seen_date IS NULL;

-- Index for age queries (e.g., "show me VINs older than 60 days")
CREATE INDEX IF NOT EXISTS idx_vehicles_first_seen ON vehicles(first_seen_date)
WHERE first_seen_date IS NOT NULL;
