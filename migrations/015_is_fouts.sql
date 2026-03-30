-- Migration 015: Add is_fouts flag to vehicles table
-- Fouts Commercial Vehicles is Smyrna Truck / Fouts Bros' own plant/lot.
-- Vehicles at that dealer are "ours" and should be tracked separately from
-- Smyrna-bodied trucks at third-party dealers.

ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS is_fouts BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_vehicles_is_fouts ON vehicles(is_fouts);
