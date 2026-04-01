-- Migration 016: Drop score check constraint
-- Score column now holds vehicle count (can exceed 100)
ALTER TABLE lead_scores DROP CONSTRAINT IF EXISTS lead_scores_score_check;
