---
name: Weekly Comvoy Scrape Pipeline
description: Complete instructions for running the weekly Comvoy inventory scrape — scraper behavior, gotchas, Smyrna logic, validation steps
type: project
---

# Weekly Comvoy Scrape Pipeline
**Cadence**: Weekly

## How to Run
```bash
cd C:\Users\motle\claude-code\comvoy
python scripts/monthly_scrape.py
```
- Resume interrupted: `--resume`
- Report only (reuse today's CSV): `--skip-scrape --skip-smyrna`
- Custom date: `--date 2026-04-15`
- Explicit comparison baseline: `--old-report scrape_output/Comvoy_Multi_Brand_Report_YYYY-MM-DD.xlsx`

## What It Does (5 Steps)
1. **Comvoy scrape** — 13 brands x 25 body types x 12 states (3,900 combos). ~87 min. Outputs `inventory_YYYY-MM-DD.csv`
2. **Smyrna WTS scrape** — Hits smyrnatruck.worktrucksolutions.com for all Smyrna-distributed VINs. Outputs `smyrna_inventory_YYYY-MM-DD.csv`
3. **Diff** — Compares against previous scrape's CSV. Outputs new/sold/price-change CSVs
4. **Excel report** — 9-sheet `Comvoy_Multi_Brand_Report_YYYY-MM-DD.xlsx`
5. **Validation** — Flags anomalies

## Output Location
All files → `C:\Users\motle\claude-code\comvoy\scrape_output\`

## Post-Scrape Steps
1. `python scripts/load_vehicles.py` — loads into Supabase (vehicles, aggregates, diffs, lead scores, snapshot metrics all auto-run)
2. `bash deploy.sh` — deploy

## Validation Checklist
1. Every vehicle has a dealer_name (zero blanks)
2. Dealer count within 10% of previous scrape
3. Vehicle count within 15% of previous scrape (~8,900 current baseline)
4. Smyrna VIN count matches WTS site count (~74 current)
5. Spot-check 3–5 known dealers against comvoy.com live numbers
6. Penske locations appear as separate rows (not collapsed)
7. No used vehicles in inventory CSV (condition != New should be 0)

## Critical Gotchas

### Comvoy URL Slug Changes
- Comvoy dropped "-for-sale" from 8 body type slugs (March 2026) — old slugs 301 redirect but strip query params
- If scrape suddenly slows or returns huge result counts for small brands → check for new slug redirects first
- Fixed slugs in `BODY_TYPES` dict in `monthly_scrape.py`

### Loop Order Matters
- **MUST iterate forward** (Ford first) — reversed() was added for --resume but causes Hino to hit fat cross-brand categories, inflating runtime from 87 min to 5+ hours

### Dealer Grouping
- Always group by (name, city, state) tuple — never name alone. Penske has 122 locations.

### Smyrna Product Identification
- DO NOT rely on `body_builder` field alone — source of truth is WTS VINs
- body_builder="Smyrna Truck" catches ~38 units; WTS scrape catches ~74

### Scraping Method
- Dealer data: HTML `category-heading` / `category-content` divs
- Vehicle data: JSON-LD `application/ld+json` — site uses `type` (not `@type`)
- Matching: positional (nth card = nth JSON-LD vehicle)

### Excel Comparison Baseline
- Scraper auto-finds previous report; falls back to `~/Documents/Comvoy_Multi_Brand_Report.xlsx` if present
- That file is STALE — delete it or always pass `--old-report` explicitly

### Used Vehicle False "Sold" (Fixed March 30, 2026)
- Old baselines contained used vehicles with `-WTS` placeholder VINs
- These appeared as "sold" in diffs because the current scraper correctly excludes them
- Baselines cleaned; issue won't recur on future scrapes

## Current Baseline (March 30, 2026)
- 8,908 vehicles · 364 dealers · 10 brands · 12 states
- Smyrna Truck: 74 WTS VINs (69 matched in main inventory)
- Fouts CV: 62 units
- 613 new · 692 sold · 163 price changes vs March 18
- 85 min runtime, zero errors

## Page Sizes
- Comvoy: 30 results/page
- Smyrna WTS: 10 results/page

## Data Fields Per Vehicle
VIN, listing_id, condition, year, brand, model, body_type, body_builder, transmission, fuel_type, color, price, image_url, listing_url, dealer_name, city, state, scrape_date
