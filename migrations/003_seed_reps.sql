-- ============================================================
-- Seed: 2 sales reps + sample travel plans
-- Run after 002_reps_travel_scoring.sql
-- ============================================================

-- Wesley White — covers GA, TN, NC, SC, AL
INSERT INTO reps (id, name, email, territory_states) VALUES
    ('a1b2c3d4-0001-4000-8000-000000000001', 'Wesley White', 'wwhite@comvoy.com', '{GA,TN,NC,SC,AL}');

-- Kenneth Greene — covers TX, LA, OK, AR, MS
INSERT INTO reps (id, name, email, territory_states) VALUES
    ('a1b2c3d4-0002-4000-8000-000000000002', 'Kenneth Greene', 'kgreene@comvoy.com', '{TX,LA,OK,AR,MS}');

-- ============================================================
-- Wesley White — sample travel week (GA/TN route)
-- ============================================================
INSERT INTO rep_travel_plans (rep_id, travel_date, start_location, start_lat, start_lng, end_location, end_lat, end_lng, notes) VALUES
    ('a1b2c3d4-0001-4000-8000-000000000001', '2026-03-09',
     'Smyrna, TN (HQ)', 36.0023, -86.5186,
     'Chattanooga, TN — Hampton Inn', 35.0456, -85.3097,
     'South TN route — hit dealers along I-24'),
    ('a1b2c3d4-0001-4000-8000-000000000001', '2026-03-10',
     'Chattanooga, TN — Hampton Inn', 35.0456, -85.3097,
     'Atlanta, GA — Marriott Perimeter', 33.9304, -84.3733,
     'Chattanooga to Atlanta via I-75'),
    ('a1b2c3d4-0001-4000-8000-000000000001', '2026-03-11',
     'Atlanta, GA — Marriott Perimeter', 33.9304, -84.3733,
     'Gainesville, GA — Holiday Inn', 34.2979, -83.8241,
     'North GA dealers — Gainesville, Buford area'),
    ('a1b2c3d4-0001-4000-8000-000000000001', '2026-03-12',
     'Gainesville, GA — Holiday Inn', 34.2979, -83.8241,
     'Greenville, SC — Hyatt', 34.8526, -82.3940,
     'GA to SC via I-85'),
    ('a1b2c3d4-0001-4000-8000-000000000001', '2026-03-13',
     'Greenville, SC — Hyatt', 34.8526, -82.3940,
     'Charlotte, NC — Hilton Uptown', 35.2271, -80.8431,
     'SC to Charlotte — wrap up week');

-- ============================================================
-- Kenneth Greene — sample travel week (TX route)
-- ============================================================
INSERT INTO rep_travel_plans (rep_id, travel_date, start_location, start_lat, start_lng, end_location, end_lat, end_lng, notes) VALUES
    ('a1b2c3d4-0002-4000-8000-000000000002', '2026-03-09',
     'Dallas, TX — Home', 32.7767, -96.7970,
     'Fort Worth, TX — Courtyard', 32.7555, -97.3308,
     'DFW metro dealers — Grand Prairie, Arlington'),
    ('a1b2c3d4-0002-4000-8000-000000000002', '2026-03-10',
     'Fort Worth, TX — Courtyard', 32.7555, -97.3308,
     'Waco, TX — La Quinta', 31.5493, -97.1467,
     'Fort Worth south to Waco via I-35'),
    ('a1b2c3d4-0002-4000-8000-000000000002', '2026-03-11',
     'Waco, TX — La Quinta', 31.5493, -97.1467,
     'San Antonio, TX — Drury Inn', 29.4241, -98.4936,
     'Waco to San Antonio — I-35 corridor'),
    ('a1b2c3d4-0002-4000-8000-000000000002', '2026-03-12',
     'San Antonio, TX — Drury Inn', 29.4241, -98.4936,
     'Houston, TX — Residence Inn', 29.7604, -95.3698,
     'San Antonio to Houston via I-10'),
    ('a1b2c3d4-0002-4000-8000-000000000002', '2026-03-13',
     'Houston, TX — Residence Inn', 29.7604, -95.3698,
     'Dallas, TX — Home', 32.7767, -96.7970,
     'Houston back to Dallas — I-45');
