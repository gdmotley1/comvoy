---
name: Technical Reference
description: Stack details, database schema, API config, lead scoring, gotchas, and scrape pipeline config
type: reference
---

## Stack
- **Backend**: Python + FastAPI + Supabase (PostgreSQL + PostGIS)
- **Frontend**: Single-file `static/index.html` — 10 IIFE modules
- **Agent**: Claude Sonnet 4 tool-use loop with 20 tools (Haiku routing for simple queries)
- **Auth**: Supabase Auth — users in `auth.users` table, view at Supabase dashboard
- **Weekly Scrape**: `python scripts/monthly_scrape.py`
- **Dev server**: `python run.py` (uvicorn, no --reload), port 8000

## Deployment
- **Frontend**: GitHub Pages → https://gdmotley1.github.io/comvoy/
- **Backend API**: Vercel **Pro** serverless → https://comvoy-api.vercel.app
- **GitHub**: `gdmotley1/comvoy` (public repo, `master` branch)
- **Deploy**: `bash deploy.sh`

## Vercel Pro Config
- vercel.json: `maxDuration: 300`, `memory: 1024`, `regions: ["iad1"]`
- Agent: Tokens 4096, loop 5, history 30, tool cap 12000, parallel via `asyncio.gather`

## Database
- **Host**: Supabase project `ihcgsmlvnjmerziwvsqo` (free tier — 500MB limit)
- **Credentials**: in `comvoy/.env`
- **Tables**: dealers, vehicles, vehicle_diffs, dealer_snapshots, dealer_brand_inventory, dealer_body_type_inventory, dealer_smyrna_details, report_snapshots, lead_scores, dealer_places, brands, body_types, snapshot_metrics, chat_sessions

## Dealer Tiering
- Config in `app/api/scoring.py` — thresholds at top
- **Strictly lot-size based**: score = total vehicles on lot
- **Tiers**: Hot 50+ vehicles | Warm 20-49 | Cold <20
- `opportunity_type` column still in DB (legacy CHECK constraint) — set based on Smyrna presence
- **Scoring runs**: Must call `compute_lead_scores(snap_id)` after each data load (auto-runs in load_vehicles.py)

## Smyrna Body Types (used by briefing/agent)
Service Trucks, Flatbed Trucks, Box Trucks, Box Vans, Stake Beds, Mechanic Body, Enclosed Service, Dump Trucks, Landscape Dumps, Flatbed Dump, Combo Body
- Contractor Trucks are NOT built by Comvoy — correctly excluded

## Our Brands Config
- **Smyrna Truck**: `is_smyrna=true` — VIN-matched from WTS. Source of truth is the WTS scrape, not body_builder field.
- **Fouts Commercial Vehicles**: `is_fouts=true` — `FOUTS_DEALER_NAME = 'Fouts Commercial Vehicles'` + `is_fouts_dealer()` in `app/config.py`. Migration 015 added `is_fouts BOOLEAN NOT NULL DEFAULT FALSE` + index.
- Always report separately — never blend without showing the split.

## Excluded Dealers
- **Single source of truth**: `app/config.py` — `EXCLUDED_DEALER_PATTERNS = ['penske', 'mhc ', 'ryder']` + `is_excluded_dealer()` helper
- All layers import from `app/config.py`

## Scrape Pipeline
- **Frequency**: Weekly — ~87 min per run
- **Storage**: Flat ~6MB (load_vehicles.py purges old snapshot vehicle rows after each load)
- **VIN Age Tracking**: `first_seen_date` column — carry-forward logic in `load_vehicles.py`
- **Post-load steps**: Recompute dealer_snapshots, run lead scoring, compute snapshot metrics (all 3 auto-run in load_vehicles.py)

## Database Migrations Log
- 001–006: Schema, reps, PostGIS functions, route polylines, dealer_places cache
- 007: vehicles + vehicle_diffs tables, search_vehicles RPC, indexes
- 008: first_seen_date DATE column on vehicles
- 009–014: (various incremental changes)
- 015: is_fouts BOOLEAN NOT NULL DEFAULT FALSE + index on vehicles table

## Known Gotchas
- uvicorn `--reload` doesn't work on this Windows setup
- Supabase PostgREST default limit is 1000 rows — must paginate
- API search endpoint max limit is 200 — must paginate for full dealer lists
- Both static HTML and `buildWelcomeHTML()` must be updated when changing welcome screen
- GitHub Pages caching: may need 20-30s + hard refresh after push
- `lead_scores` table has legacy `opportunity_type` NOT NULL CHECK constraint — must set whitespace/upsell on insert
- After loading new data: must recompute dealer_snapshots, lead scoring, AND snapshot metrics (all 3 auto-run in load_vehicles.py)
- openpyxl `insert_rows` breaks merged cells — must `unmerge_cells` first when editing spreadsheets with merged rows
- `~/Documents/Comvoy_Multi_Brand_Report.xlsx` is stale — scraper falls back to it as comparison baseline if present. **Delete it.**
- When regenerating Excel report, always pass `--old-report scrape_output/Comvoy_Multi_Brand_Report_YYYY-MM-DD.xlsx` explicitly
