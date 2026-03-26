-- ============================================================
-- Rep Visit Schedules — imported from CSV/Excel
-- A schedule is a planning artifact (who to visit over months),
-- not a day-by-day routed trip. It feeds INTO trip planning.
-- ============================================================

-- ============================================================
-- 1. rep_schedules — top-level schedule grouping
-- ============================================================
CREATE TABLE IF NOT EXISTS rep_schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rep_id          UUID NOT NULL REFERENCES reps(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    source_filename TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rep_schedules_rep ON rep_schedules(rep_id);

-- ============================================================
-- 2. rep_schedule_dealers — individual dealer entries
-- ============================================================
CREATE TABLE IF NOT EXISTS rep_schedule_dealers (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_id      UUID NOT NULL REFERENCES rep_schedules(id) ON DELETE CASCADE,
    dealer_id        UUID REFERENCES dealers(id) ON DELETE SET NULL,  -- NULL if no Otto match
    raw_name         TEXT NOT NULL,
    raw_city         TEXT,
    raw_state        TEXT,
    relationship     TEXT,          -- HOT, WARM, COLD
    dealership_type  TEXT,          -- CVC, RETAIL
    lead_contacts    TEXT,
    visit_date       DATE,
    zone_label       TEXT,          -- e.g. "GEORGIA (92)"
    notes            TEXT,
    match_confidence DOUBLE PRECISION,  -- 0-1 fuzzy match score
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rsd_schedule ON rep_schedule_dealers(schedule_id);
CREATE INDEX IF NOT EXISTS idx_rsd_dealer ON rep_schedule_dealers(dealer_id) WHERE dealer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rsd_active ON rep_schedule_dealers(is_active) WHERE is_active = TRUE;
