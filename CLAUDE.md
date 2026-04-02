# Comvoy / Otto — Project Context

## What It Is
**Otto** — Sales Intelligence Platform for **Comvoy** (Smyrna Truck / Fouts Bros), a commercial truck distributor. Conversational query layer over market data. **New vehicles only — used inventory permanently excluded at every layer.**

### Product Identity
- **Name**: Otto | **Users**: Wesley White · Kenneth Greene · AJ Delange (home: Manchester TN) + leadership
- **Core Pillars**: Market Intelligence, Lead Scoring, Territory Analytics, Dealer Briefings, Velocity Tracking

## Project Root
`C:\Users\motle\claude-code\comvoy\`

## Key Commands
```bash
python run.py                                    # Dev server (port 8000, no --reload)
python scripts/monthly_scrape.py                 # Full weekly scrape (~87 min)
python scripts/monthly_scrape.py --resume        # Resume interrupted scrape
python scripts/monthly_scrape.py --skip-scrape --skip-smyrna  # Regen report only
python scripts/monthly_scrape.py --old-report scrape_output/Comvoy_Multi_Brand_Report_YYYY-MM-DD.xlsx  # Explicit baseline
python scripts/load_vehicles.py                  # Load latest CSV into Supabase
bash deploy.sh                                   # Deploy backend (Vercel) + frontend (GitHub Pages)
```

## Current Data State (March 30, 2026)
- **8,908 vehicles** · 364 dealers · 10 brands · 12 states
- **Smyrna Truck**: 69 units at third-party dealers (`is_smyrna=true`)
- **Fouts Commercial Vehicles**: 62 units at our plant (`is_fouts=true`)
- Supabase storage: flat ~6MB — old snapshot vehicle rows auto-purged on each load

## Our Two Brands — Always Keep Separate
Two businesses, same ownership. Never blend into one number without showing the split.

| | Smyrna Truck | Fouts Commercial Vehicles |
|---|---|---|
| Flag | `is_smyrna=true` | `is_fouts=true` |
| What | Bodies at third-party dealers | Our own plant/lot in Smyrna GA |
| Builders | Reading, Warner, Cadet, Switch-N-Go, Stellar, Dakota + others | Warner Truck Bodies, Miller Industries |
| Body types | Service trucks, flatbeds, box vans, etc. | Mechanic body, rollback, bucket trucks |
| Source of truth | VIN-matched from smyrnatruck.worktrucksolutions.com | dealer_name = 'Fouts Commercial Vehicles' |
| Sales target? | No — it's us | No — it's us |

## Permanently Excluded
- **Dealers**: Penske, MHC, Ryder — `EXCLUDED_DEALER_PATTERNS` in `app/config.py`
- **Vehicles**: All used/condition != 'New' — filtered at scrape and load

## Stack (Quick Reference)
- **Backend**: Python + FastAPI + Supabase (PostgreSQL + PostGIS) → Vercel Pro
- **Frontend**: Single-file `static/index.html` (10 IIFE modules) → GitHub Pages
- **Agent**: Claude Sonnet 4 tool-use loop, 20 tools, Haiku routing for simple queries
- **Auth**: Supabase Auth
- **Repo**: `gdmotley1/comvoy` (public, master branch)
- **API**: https://comvoy-api.vercel.app | **Frontend**: https://gdmotley1.github.io/comvoy/

## Critical Gotchas
- `--reload` doesn't work on this Windows setup — use `python run.py` only
- Supabase PostgREST default limit 1000 rows — always paginate
- `~/Documents/Comvoy_Multi_Brand_Report.xlsx` is a stale undated file — **delete it** or it corrupts scraper comparison baseline
- Scraper Excel comparison: always pass `--old-report` with a dated file explicitly
- `lead_scores` table has legacy `opportunity_type` NOT NULL CHECK — must set whitespace/upsell on insert
- Both `static/index.html` and `buildWelcomeHTML()` must be updated together when changing the welcome screen
- GitHub Pages: 20-30s + hard refresh after push

## Assessment Backlog
- #3 Clean up `opportunity_type` dead code in lead_scores
- #4 Delete or expose `snapshot_metrics` (computed but never surfaced)
- #5 Add frontend error handling (toast notifications)
- #6 Delete `C:\Users\motle\Documents\Comvoy_Multi_Brand_Report.xlsx`

## Next Steps (Roadmap)
1. Velocity Excel Report — add velocity sheet to weekly Excel report
2. Email automation — at-risk alerts, weekly digest, hot lead notifications
3. Salesforce integration — CEO approved exploration
4. Trips hero redesign — Mini Motorways-style map (future)
5. Feedback loop — thumbs up/down on Otto answers
6. Conversation memory Level 2 — per-rep preference summaries

## Detailed Reference Files
@memory/technical-reference.md
@memory/weekly-scrape.md
@memory/decisions.md
@memory/feedback_greeting_style.md
@memory/feedback_scraper_consistency.md
@memory/comvoy-files.md
