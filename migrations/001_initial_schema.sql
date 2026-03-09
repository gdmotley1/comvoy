-- ============================================================
-- Comvoy Sales Intelligence — Phase 1 Schema
-- Run this in the Supabase SQL Editor
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================
-- REPORT_SNAPSHOTS: Track each monthly Excel upload
-- ============================================================
CREATE TABLE report_snapshots (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_date      DATE NOT NULL,
    file_name        TEXT NOT NULL,
    uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
    total_dealers    INT,
    total_vehicles   INT,
    total_brands     INT,
    total_body_types INT,
    UNIQUE(report_date)
);

-- ============================================================
-- BRANDS: 13 chassis brands
-- ============================================================
CREATE TABLE brands (
    id         SERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    short_name TEXT
);

-- ============================================================
-- BODY_TYPES: 25 body types
-- ============================================================
CREATE TABLE body_types (
    id         SERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    short_name TEXT
);

-- ============================================================
-- DEALERS: Core entity — stable across monthly snapshots
-- ============================================================
CREATE TABLE dealers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    state       CHAR(2) NOT NULL,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    location    GEOGRAPHY(POINT, 4326),
    geocoded_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name, city, state)
);

-- ============================================================
-- DEALER_SNAPSHOTS: Per-month summary for each dealer
-- ============================================================
CREATE TABLE dealer_snapshots (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dealer_id         UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    snapshot_id       UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    rank              INT,
    total_vehicles    INT NOT NULL DEFAULT 0,
    brand_count       INT,
    body_type_count   INT,
    top_brand         TEXT,
    smyrna_units      INT DEFAULT 0,
    smyrna_percentage DECIMAL(7,4) DEFAULT 0,
    top_body_types    TEXT,
    UNIQUE(dealer_id, snapshot_id)
);

-- ============================================================
-- DEALER_BRAND_INVENTORY: Dealer × Brand vehicle counts
-- ============================================================
CREATE TABLE dealer_brand_inventory (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dealer_id     UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    snapshot_id   UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    brand_id      INT NOT NULL REFERENCES brands(id),
    vehicle_count INT NOT NULL DEFAULT 0,
    UNIQUE(dealer_id, snapshot_id, brand_id)
);

-- ============================================================
-- DEALER_BODY_TYPE_INVENTORY: Dealer × Body Type vehicle counts
-- ============================================================
CREATE TABLE dealer_body_type_inventory (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dealer_id     UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    snapshot_id   UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    body_type_id  INT NOT NULL REFERENCES body_types(id),
    vehicle_count INT NOT NULL DEFAULT 0,
    UNIQUE(dealer_id, snapshot_id, body_type_id)
);

-- ============================================================
-- DEALER_SMYRNA_DETAILS: Smyrna/Fouts product placement
-- ============================================================
CREATE TABLE dealer_smyrna_details (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dealer_id             UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    snapshot_id           UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    smyrna_units          INT NOT NULL DEFAULT 0,
    dealer_total          INT NOT NULL DEFAULT 0,
    smyrna_percentage     DECIMAL(7,4) DEFAULT 0,
    top_smyrna_body_types TEXT,
    avg_days_since_upfit  INT,
    UNIQUE(dealer_id, snapshot_id)
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX idx_dealers_state ON dealers(state);
CREATE INDEX idx_dealers_location ON dealers USING GIST(location);
CREATE INDEX idx_dealer_snapshots_snapshot ON dealer_snapshots(snapshot_id);
CREATE INDEX idx_dealer_snapshots_dealer ON dealer_snapshots(dealer_id);
CREATE INDEX idx_dealer_snapshots_rank ON dealer_snapshots(rank);
CREATE INDEX idx_dealer_snapshots_smyrna ON dealer_snapshots(smyrna_units DESC NULLS LAST);
CREATE INDEX idx_dealer_brand_inv_snapshot ON dealer_brand_inventory(snapshot_id);
CREATE INDEX idx_dealer_brand_inv_dealer ON dealer_brand_inventory(dealer_id);
CREATE INDEX idx_dealer_body_type_inv_snapshot ON dealer_body_type_inventory(snapshot_id);
CREATE INDEX idx_dealer_body_type_inv_dealer ON dealer_body_type_inventory(dealer_id);
CREATE INDEX idx_dealer_smyrna_snapshot ON dealer_smyrna_details(snapshot_id);

-- ============================================================
-- HELPER: Function to auto-set location from lat/lng
-- ============================================================
CREATE OR REPLACE FUNCTION update_dealer_location()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.location := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_dealer_location
    BEFORE INSERT OR UPDATE OF latitude, longitude ON dealers
    FOR EACH ROW
    EXECUTE FUNCTION update_dealer_location();

-- ============================================================
-- HELPER: Function to find dealers within radius
-- ============================================================
CREATE OR REPLACE FUNCTION find_nearby_dealers(
    p_lat DOUBLE PRECISION,
    p_lng DOUBLE PRECISION,
    p_radius_miles DOUBLE PRECISION DEFAULT 30
)
RETURNS TABLE (
    dealer_id UUID,
    dealer_name TEXT,
    city TEXT,
    state CHAR(2),
    distance_miles DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.id,
        d.name,
        d.city,
        d.state,
        ROUND((ST_Distance(
            d.location,
            ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
        ) / 1609.34)::numeric, 1)::double precision AS distance_miles
    FROM dealers d
    WHERE d.location IS NOT NULL
      AND ST_DWithin(
            d.location,
            ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography,
            p_radius_miles * 1609.34
          )
    ORDER BY distance_miles;
END;
$$ LANGUAGE plpgsql;
