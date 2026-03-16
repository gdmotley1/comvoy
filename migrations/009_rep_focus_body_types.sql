-- ============================================================
-- Add focus_body_types column to reps table
-- Optional field for tracking which body types a rep specializes in
-- ============================================================
ALTER TABLE reps ADD COLUMN IF NOT EXISTS focus_body_types TEXT[] DEFAULT '{}';
