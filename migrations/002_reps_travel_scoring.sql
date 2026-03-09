-- ============================================================
-- Comvoy Sales Intelligence — Phase 3: Reps, Travel, Lead Scoring
-- Run this in the Supabase SQL Editor
-- ============================================================

-- ============================================================
-- REPS: Sales team members
-- ============================================================
CREATE TABLE reps (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT NOT NULL,
    email            TEXT,
    phone            TEXT,
    territory_states TEXT[] DEFAULT '{}',   -- e.g. {'GA','TN','NC'}
    is_active        BOOLEAN DEFAULT true,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_reps_active ON reps(is_active);

-- ============================================================
-- REP_TRAVEL_PLANS: Daily start/end locations for route planning
-- ============================================================
CREATE TABLE rep_travel_plans (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rep_id          UUID NOT NULL REFERENCES reps(id) ON DELETE CASCADE,
    travel_date     DATE NOT NULL,
    start_location  TEXT NOT NULL,          -- city or address
    start_lat       DOUBLE PRECISION NOT NULL,
    start_lng       DOUBLE PRECISION NOT NULL,
    end_location    TEXT NOT NULL,           -- hotel / end-of-day location
    end_lat         DOUBLE PRECISION NOT NULL,
    end_lng         DOUBLE PRECISION NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(rep_id, travel_date)
);

CREATE INDEX idx_travel_plans_rep ON rep_travel_plans(rep_id);
CREATE INDEX idx_travel_plans_date ON rep_travel_plans(travel_date);

-- ============================================================
-- LEAD_SCORES: Scored whitespace + upsell opportunities
-- Recomputed after each monthly upload
-- ============================================================
CREATE TABLE lead_scores (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dealer_id       UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    snapshot_id     UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    score           INT NOT NULL CHECK (score BETWEEN 0 AND 100),
    tier            TEXT NOT NULL CHECK (tier IN ('hot', 'warm', 'cold')),
    opportunity_type TEXT NOT NULL CHECK (opportunity_type IN ('whitespace', 'upsell', 'at_risk')),
    factors         JSONB NOT NULL DEFAULT '{}',
    scored_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(dealer_id, snapshot_id)
);

CREATE INDEX idx_lead_scores_snapshot ON lead_scores(snapshot_id);
CREATE INDEX idx_lead_scores_tier ON lead_scores(tier);
CREATE INDEX idx_lead_scores_score ON lead_scores(score DESC);

-- ============================================================
-- UPLOAD_REPORTS: Auto-generated summaries after each ingest
-- ============================================================
CREATE TABLE upload_reports (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    report_json     JSONB NOT NULL,         -- full structured report
    summary_text    TEXT NOT NULL,           -- human-readable summary
    generated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(snapshot_id)
);

-- ============================================================
-- HELPER: Find dealers along a travel corridor
-- Uses a bounding box with buffer around start→end line
-- ============================================================
CREATE OR REPLACE FUNCTION find_dealers_along_route(
    p_start_lat DOUBLE PRECISION,
    p_start_lng DOUBLE PRECISION,
    p_end_lat   DOUBLE PRECISION,
    p_end_lng   DOUBLE PRECISION,
    p_buffer_miles DOUBLE PRECISION DEFAULT 20
)
RETURNS TABLE (
    dealer_id      UUID,
    dealer_name    TEXT,
    city           TEXT,
    state          CHAR(2),
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    distance_miles DOUBLE PRECISION
) AS $$
DECLARE
    route_line GEOGRAPHY;
BEGIN
    -- Build a line from start to end
    route_line := ST_SetSRID(
        ST_MakeLine(
            ST_MakePoint(p_start_lng, p_start_lat),
            ST_MakePoint(p_end_lng, p_end_lat)
        ), 4326
    )::geography;

    RETURN QUERY
    SELECT
        d.id,
        d.name,
        d.city,
        d.state,
        d.latitude,
        d.longitude,
        ROUND((ST_Distance(d.location, route_line) / 1609.34)::numeric, 1)::double precision AS distance_miles
    FROM dealers d
    WHERE d.location IS NOT NULL
      AND ST_DWithin(d.location, route_line, p_buffer_miles * 1609.34)
    ORDER BY ST_LineLocatePoint(
        route_line::geometry,
        ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)
    );
END;
$$ LANGUAGE plpgsql;
