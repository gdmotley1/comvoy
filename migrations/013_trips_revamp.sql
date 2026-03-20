-- Multi-day named trips with dealer stop selection + visit tracking
-- Replaces single-day rep_travel_plans with a proper trip → days → stops hierarchy

-- ============================================================
-- 1. trips — the top-level trip grouping
-- ============================================================
CREATE TABLE IF NOT EXISTS trips (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    rep_id          UUID NOT NULL REFERENCES reps(id) ON DELETE CASCADE,
    created_by      TEXT,                   -- auth user name (CEO can assign trips to reps)
    status          TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'planned', 'active', 'completed')),
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trips_rep ON trips(rep_id, start_date DESC);
CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);

-- ============================================================
-- 2. trip_days — one row per travel day within a trip
-- ============================================================
CREATE TABLE IF NOT EXISTS trip_days (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id         UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    day_number      INT NOT NULL,
    travel_date     DATE NOT NULL,
    start_location  TEXT NOT NULL,
    start_lat       DOUBLE PRECISION NOT NULL,
    start_lng       DOUBLE PRECISION NOT NULL,
    end_location    TEXT NOT NULL,
    end_lat         DOUBLE PRECISION NOT NULL,
    end_lng         DOUBLE PRECISION NOT NULL,
    is_round_trip   BOOLEAN DEFAULT FALSE,
    route_polyline  TEXT,                   -- WKT LINESTRING from Google Directions
    notes           TEXT,
    UNIQUE(trip_id, day_number)
);

CREATE INDEX IF NOT EXISTS idx_trip_days_trip ON trip_days(trip_id);

-- ============================================================
-- 3. trip_stops — dealers selected for each day + visit tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS trip_stops (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_day_id     UUID NOT NULL REFERENCES trip_days(id) ON DELETE CASCADE,
    dealer_id       UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    stop_order      INT NOT NULL DEFAULT 0,
    is_included     BOOLEAN DEFAULT TRUE,   -- toggle on/off without deleting
    visited         BOOLEAN DEFAULT FALSE,  -- checklist: did the rep visit?
    visited_at      TIMESTAMPTZ,
    visit_notes     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(trip_day_id, dealer_id)
);

CREATE INDEX IF NOT EXISTS idx_trip_stops_day ON trip_stops(trip_day_id);
CREATE INDEX IF NOT EXISTS idx_trip_stops_dealer ON trip_stops(dealer_id);
CREATE INDEX IF NOT EXISTS idx_trip_stops_visited ON trip_stops(visited) WHERE visited = TRUE;

-- ============================================================
-- 4. Smart time estimation config (used by day-splitting logic)
-- ============================================================
-- These are used by the API, not stored in DB.
-- Default: 45 min per dealer visit, 8 hour max day.
-- Drive times come from Google Directions API at planning time.

-- ============================================================
-- 5. Migrate existing rep_travel_plans → trips + trip_days
-- ============================================================
-- Run this AFTER creating the tables above.
-- Each old plan becomes a single-day trip named "date — start → end".

INSERT INTO trips (name, rep_id, status, start_date, end_date, notes, created_at)
SELECT
    TO_CHAR(travel_date, 'Mon DD') || ' — ' || start_location || ' → ' || end_location,
    rep_id,
    CASE WHEN travel_date < CURRENT_DATE THEN 'completed' ELSE 'planned' END,
    travel_date,
    travel_date,
    notes,
    created_at
FROM rep_travel_plans
ON CONFLICT DO NOTHING;

-- Now insert corresponding trip_days rows
INSERT INTO trip_days (trip_id, day_number, travel_date, start_location, start_lat, start_lng, end_location, end_lat, end_lng, is_round_trip, route_polyline, notes)
SELECT
    t.id,
    1,
    p.travel_date,
    p.start_location,
    p.start_lat,
    p.start_lng,
    p.end_location,
    p.end_lat,
    p.end_lng,
    COALESCE(p.is_round_trip, FALSE),
    p.route_polyline,
    p.notes
FROM rep_travel_plans p
JOIN trips t ON t.start_date = p.travel_date AND t.rep_id = p.rep_id
ON CONFLICT DO NOTHING;
