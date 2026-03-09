-- ============================================================
-- Comvoy — Fix: Add route_position to corridor search
-- Run this in the Supabase SQL Editor
-- ============================================================
-- Returns route_position (0.0 = at start, 1.0 = at end) so
-- dealers can be presented in logical travel order.
-- ============================================================

-- Must drop first — Postgres can't alter return type of existing function
DROP FUNCTION IF EXISTS find_dealers_along_route(double precision, double precision, double precision, double precision, double precision);

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
    distance_miles DOUBLE PRECISION,
    route_position DOUBLE PRECISION   -- 0.0 = start, 1.0 = end
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
        ROUND((ST_Distance(d.location, route_line) / 1609.34)::numeric, 1)::double precision AS distance_miles,
        ST_LineLocatePoint(
            route_line::geometry,
            ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)
        ) AS route_position
    FROM dealers d
    WHERE d.location IS NOT NULL
      AND ST_DWithin(d.location, route_line, p_buffer_miles * 1609.34)
    ORDER BY route_position;
END;
$$ LANGUAGE plpgsql;
