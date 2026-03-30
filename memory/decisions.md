# Decision Log

Permanent decisions made during the project — recorded to avoid re-litigating in future sessions.

---

## New Vehicles Only
**Date**: Project inception
**Decision**: Otto tracks new commercial vehicles exclusively. Used inventory excluded at every layer — scraper, loader, DB, agent.
**Why**: Smyrna Truck / Fouts Bros sells new trucks. Used inventory is noise and pollutes lead scoring, market share, and penetration metrics. Penske/rental chains list used trucks in bulk and would dominate counts if included.

---

## Excluded Dealers: Penske, MHC, Ryder
**Date**: Project inception
**Decision**: These three national rental/fleet chains are permanently excluded from all counts, scoring, and agent responses.
**Why**: They are not sales prospects. Their listings inflate market counts, distort lead scores, and waste rep attention. Single source of truth: `EXCLUDED_DEALER_PATTERNS` in `app/config.py`.

---

## Smyrna Truck Source of Truth = WTS VINs
**Date**: March 2026
**Decision**: Smyrna unit identification uses VIN-matching from `smyrnatruck.worktrucksolutions.com`, NOT the `body_builder` field.
**Why**: Smyrna is a distributor — they sell products built by Reading, Warner, Cadet, and others. The `body_builder` field says "Reading" not "Smyrna Truck" for many products. The WTS site lists all Smyrna-distributed products by VIN regardless of builder name. body_builder="Smyrna Truck" only catches ~38/74 units; WTS catches all 74.

---

## Fouts Commercial Vehicles = Ours, Kept Separate
**Date**: March 30, 2026
**Decision**: Fouts Commercial Vehicles (our plant lot) is flagged `is_fouts=true` and treated as "our" inventory — but always reported separately from Smyrna Truck units, never blended.
**Why**: Different business entity under same ownership. Fouts CV stocks Warner/Miller bodies (mechanic, rollback, bucket) — different product line than Smyrna Truck bodies. Blending misleads on product mix and market placement. Combined totals are fine as context but the breakdown must always be visible.

---

## Scraper: No Mid-Run Optimization
**Date**: March 2026
**Decision**: Never add shortcuts that change what data the scraper collects (early VIN bail, page skipping, mid-run dedup). Speed fixes only — must not change what gets scraped.
**Why**: Week-over-week diffs are only meaningful if both scrapes used identical methodology. If one week skips pages another didn't, "sold" counts become garbage. Fix slowness by fixing root causes (broken URL slugs, wrong loop order) not by skipping data.

---

## Snapshot Storage: Latest Only
**Date**: March 30, 2026
**Decision**: `load_vehicles.py` purges vehicle rows from all old snapshots after loading a new one. Only the latest snapshot's vehicle rows are retained.
**Why**: Supabase free tier (500MB). Full vehicle rows per snapshot = ~6MB/week growing forever. Historical analysis uses `vehicle_diffs` + `dealer_snapshots` + `snapshot_metrics` — none of which are purged. You never need to query "full inventory as of March 12" — only diffs matter historically.

---

## Import Schedule + Nearby Opportunities Removed
**Date**: March 26, 2026
**Decision**: Both features removed from the trips tab UI. Backend endpoints still exist.
**Why**: User decision — removed from frontend. Endpoints kept in case they're useful later.

---

## Vercel Pro Required, Supabase Free Tier
**Date**: March 2026
**Decision**: Keep Vercel Pro, stay on Supabase free.
**Why**: Vercel Pro is non-negotiable — `maxDuration: 300s` is required for the Claude agent loop (free tier caps at 10s, agent routinely takes 15-45s). Supabase Pro not needed — free tier 500MB is sufficient with snapshot purging, and auto-pause is mitigated by regular rep usage.
