-- ============================================================
-- 005: Real driving routes via Google Directions polylines
-- Adds route_polyline column and polyline-aware search function
-- ============================================================

-- Add polyline column to travel plans (stores WKT LINESTRING)
ALTER TABLE rep_travel_plans
    ADD COLUMN IF NOT EXISTS route_polyline TEXT;

-- New function: search dealers along a real driving route polyline
-- Falls back to straight line if no polyline provided
CREATE OR REPLACE FUNCTION find_dealers_along_route(
    p_start_lat    DOUBLE PRECISION,
    p_start_lng    DOUBLE PRECISION,
    p_end_lat      DOUBLE PRECISION,
    p_end_lng      DOUBLE PRECISION,
    p_buffer_miles DOUBLE PRECISION DEFAULT 20,
    p_polyline_wkt TEXT DEFAULT NULL
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
    IF p_polyline_wkt IS NOT NULL AND length(p_polyline_wkt) > 0 THEN
        -- Use the real driving route polyline
        route_line := ST_SetSRID(
            ST_GeomFromText(p_polyline_wkt),
            4326
        )::geography;
    ELSE
        -- Fall back to straight line
        route_line := ST_SetSRID(
            ST_MakeLine(
                ST_MakePoint(p_start_lng, p_start_lat),
                ST_MakePoint(p_end_lng, p_end_lat)
            ), 4326
        )::geography;
    END IF;

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
