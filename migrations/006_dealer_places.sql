-- Migration 006: Google Places API cache for dealer business data
-- Stores rating, reviews, phone, website, hours, photos from Google Places API
-- 30-day TTL with lazy refresh on access

CREATE TABLE dealer_places (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dealer_id         UUID NOT NULL REFERENCES dealers(id) ON DELETE CASCADE,
    google_place_id   TEXT,
    rating            NUMERIC(2,1),
    review_count      INT,
    phone             TEXT,
    website           TEXT,
    google_maps_url   TEXT,
    formatted_address TEXT,
    hours_json        JSONB,
    photos_json       JSONB,
    business_status   TEXT DEFAULT 'OPERATIONAL',
    fetched_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(dealer_id)
);

CREATE INDEX idx_dealer_places_dealer ON dealer_places(dealer_id);
CREATE INDEX idx_dealer_places_rating ON dealer_places(rating DESC NULLS LAST);
CREATE INDEX idx_dealer_places_fetched ON dealer_places(fetched_at);
