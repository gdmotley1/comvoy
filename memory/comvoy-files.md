---
name: comvoy-files
description: Complete file map for the Comvoy project — all backend, frontend, ETL, scrape, and deployment files
type: reference
---

# Comvoy File Map
Root: `C:\Users\motle\claude-code\comvoy\`

## Config
- `.env` — Supabase URL/keys, Anthropic key, Google Maps key, SMTP creds
- `run.py` — Dev server launcher (os.chdir + uvicorn, no --reload)
- `requirements.txt` — Dependencies
- `CLAUDE.md` — Primary project context for Claude Code (auto-loaded)
- `memory/` — Detailed reference files (@imported by CLAUDE.md)

## Deployment
- `deploy.sh` — Deploys Vercel backend + `git push origin master` (triggers GitHub Pages)
- `vercel.json` — Vercel serverless config, routes `/api/*` and `/health` to `api/index.py`
- `api/index.py` — Vercel entry point (`from app.main import app`)

## App Core
- `app/main.py` — FastAPI app, routers + static mount + health check
- `app/config.py` — Pydantic Settings + EXCLUDED_DEALER_PATTERNS + FOUTS_DEALER_NAME + token budget guardrails
- `app/database.py` — Supabase client singletons (service + anon)
- `app/models.py` — Pydantic models with Field validation

## ETL Pipeline
- `app/etl/parser.py` — Parses Excel sheets
- `app/etl/geocoder.py` — Nominatim geocoder
- `app/etl/loader.py` — Loads into Supabase
- `app/etl/routing.py` — Google Directions + Distance Matrix API
- `app/etl/places.py` — Google Places API client + dealer_places cache

## API Endpoints
- `app/api/ingest.py` — Upload + rescore
- `app/api/dealers.py` — Search, map, briefing, territory
- `app/api/chat.py` — AI chat with Sonnet 4 agent loop
- `app/api/trends.py` — Dealer + territory trends
- `app/api/alerts.py` — Monthly change detection
- `app/api/scoring.py` — Lead scoring engine + SMYRNA_BODY_TYPES constant
- `app/api/reports.py` — Upload reports
- `app/api/travel.py` — Rep + travel plan CRUD, auto-brief on save
- `app/api/briefing.py` — Auto-brief engine: PostGIS + scores → HTML email → SMTP
- `app/api/dashboard.py` — Territory dashboard (vehicles, pricing, brands, builders, lead scores, Smyrna + Fouts intel)
- `app/api/metrics.py` — Snapshot metrics computation (smyrna_share, fouts_plant_units, our_total_units)

## Agent Layer
- `app/agent/tools.py` — 20 tools (search, briefing, scoring, travel, vehicles, pricing, competitive intel)
- `app/agent/prompts.py` — Sales agent system prompt (Smyrna + Fouts rules, 21 rules total)

## Scrape Pipeline
- `scripts/monthly_scrape.py` — Master 5-step pipeline (scrape → Smyrna WTS → diff → Excel → validate)
- `scripts/load_vehicles.py` — CSV → Supabase loader (vehicles, aggregates, Smyrna/Fouts tagging, scoring, purge old snapshots)
- `scrape_output/` — CSVs, diffs, Excel reports

## Frontend
- `static/index.html` — Single-file UI with 10 IIFE JS modules, ARIA accessibility, keyboard nav

## Database Migrations
- `migrations/001–014` — Schema, reps, PostGIS, route polylines, dealer_places, vehicles, first_seen_date, etc.
- `migrations/015_is_fouts.sql` — is_fouts BOOLEAN + index on vehicles table
