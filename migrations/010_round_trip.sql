-- Add round trip flag to travel plans
ALTER TABLE rep_travel_plans ADD COLUMN IF NOT EXISTS is_round_trip BOOLEAN DEFAULT FALSE;
