-- ============================================================
-- Migration 007: VIN-level vehicle inventory
-- Run this in the Supabase SQL Editor
-- ============================================================

-- ============================================================
-- VEHICLES: Individual vehicle records with VIN, price, specs
-- ============================================================
CREATE TABLE vehicles (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vin           TEXT NOT NULL,
    dealer_id     UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    snapshot_id   UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    brand         TEXT NOT NULL,
    model         TEXT,
    body_type     TEXT NOT NULL,
    body_builder  TEXT,
    price         INT,
    condition     TEXT,
    transmission  TEXT,
    fuel_type     TEXT,
    color         TEXT,
    listing_url   TEXT,
    image_url     TEXT,
    is_smyrna     BOOLEAN DEFAULT FALSE,
    scraped_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(vin, snapshot_id)
);

-- ============================================================
-- VEHICLE_DIFFS: Track sold/new/price-changed between snapshots
-- ============================================================
CREATE TABLE vehicle_diffs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    prev_snapshot_id UUID NOT NULL REFERENCES report_snapshots(id) ON DELETE CASCADE,
    diff_type       TEXT NOT NULL CHECK (diff_type IN ('new', 'sold', 'price_change')),
    vin             TEXT NOT NULL,
    dealer_id       UUID REFERENCES dealers(id),
    brand           TEXT,
    body_type       TEXT,
    old_price       INT,
    new_price       INT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX idx_vehicles_vin ON vehicles(vin);
CREATE INDEX idx_vehicles_snapshot ON vehicles(snapshot_id);
CREATE INDEX idx_vehicles_dealer ON vehicles(dealer_id);
CREATE INDEX idx_vehicles_brand ON vehicles(brand);
CREATE INDEX idx_vehicles_body_type ON vehicles(body_type);
CREATE INDEX idx_vehicles_price ON vehicles(price) WHERE price IS NOT NULL;
CREATE INDEX idx_vehicles_smyrna ON vehicles(is_smyrna) WHERE is_smyrna = TRUE;
CREATE INDEX idx_vehicles_dealer_snapshot ON vehicles(dealer_id, snapshot_id);

CREATE INDEX idx_vehicle_diffs_snapshot ON vehicle_diffs(snapshot_id);
CREATE INDEX idx_vehicle_diffs_type ON vehicle_diffs(diff_type);
CREATE INDEX idx_vehicle_diffs_vin ON vehicle_diffs(vin);

-- ============================================================
-- FUNCTION: Search vehicles with filters
-- ============================================================
CREATE OR REPLACE FUNCTION search_vehicles(
    p_snapshot_id UUID,
    p_brand TEXT DEFAULT NULL,
    p_body_type TEXT DEFAULT NULL,
    p_min_price INT DEFAULT NULL,
    p_max_price INT DEFAULT NULL,
    p_state TEXT DEFAULT NULL,
    p_dealer_id UUID DEFAULT NULL,
    p_is_smyrna BOOLEAN DEFAULT NULL,
    p_limit INT DEFAULT 20
)
RETURNS TABLE (
    vehicle_id UUID,
    vin TEXT,
    dealer_id UUID,
    dealer_name TEXT,
    city TEXT,
    state CHAR(2),
    brand TEXT,
    model TEXT,
    body_type TEXT,
    body_builder TEXT,
    price INT,
    condition TEXT,
    is_smyrna BOOLEAN,
    listing_url TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        v.id AS vehicle_id,
        v.vin,
        v.dealer_id,
        d.name AS dealer_name,
        d.city,
        d.state,
        v.brand,
        v.model,
        v.body_type,
        v.body_builder,
        v.price,
        v.condition,
        v.is_smyrna,
        v.listing_url
    FROM vehicles v
    JOIN dealers d ON d.id = v.dealer_id
    WHERE v.snapshot_id = p_snapshot_id
      AND (p_brand IS NULL OR v.brand ILIKE p_brand)
      AND (p_body_type IS NULL OR v.body_type ILIKE '%' || p_body_type || '%')
      AND (p_min_price IS NULL OR v.price >= p_min_price)
      AND (p_max_price IS NULL OR v.price <= p_max_price)
      AND (p_state IS NULL OR d.state = UPPER(p_state))
      AND (p_dealer_id IS NULL OR v.dealer_id = p_dealer_id)
      AND (p_is_smyrna IS NULL OR v.is_smyrna = p_is_smyrna)
    ORDER BY v.price ASC NULLS LAST
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;
