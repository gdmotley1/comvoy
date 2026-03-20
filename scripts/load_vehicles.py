"""
Load scraped VIN-level inventory into Supabase vehicles table.

Usage:
    python scripts/load_vehicles.py                          # load latest scrape
    python scripts/load_vehicles.py --csv path/to/file.csv   # load specific CSV
    python scripts/load_vehicles.py --date 2026-03-12        # load by date
    python scripts/load_vehicles.py --diff                   # also compute diffs

Requires migration 007 to be applied first.
"""

import sys
import os
import csv
import argparse
import asyncio
import glob
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.database import get_service_client
from app.etl.geocoder import geocode_single
from app.etl.places import search_place
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Excluded Dealers ─────────────────────────────────────────────────────────
EXCLUDED_DEALER_PATTERNS = ['penske', 'mhc ']


def _is_excluded(name: str) -> bool:
    n = name.lower()
    return any(pat in n for pat in EXCLUDED_DEALER_PATTERNS)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BATCH_SIZE = 500
SCRAPE_DIR = os.path.join(os.path.dirname(__file__), "..", "scrape_output")


def find_latest_csv(scrape_dir):
    pattern = os.path.join(scrape_dir, "inventory_*.csv")
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def find_smyrna_csv(scrape_dir, date_str):
    candidates = [
        os.path.join(scrape_dir, f"smyrna_inventory_{date_str}.csv"),
        os.path.join(scrape_dir, "smyrna_inventory_full.csv"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def load_smyrna_vins(smyrna_csv):
    if not smyrna_csv or not os.path.exists(smyrna_csv):
        return set()
    vins = set()
    with open(smyrna_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vin = row.get("vin", "").strip()
            if vin:
                vins.add(vin)
    logger.info(f"Loaded {len(vins)} Smyrna VINs from {os.path.basename(smyrna_csv)}")
    return vins


def get_or_create_snapshot(db, report_date, csv_path):
    existing = db.table("report_snapshots").select("id").eq("report_date", report_date).execute()
    if existing.data:
        return existing.data[0]["id"]

    # Count from CSV
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    dealers = set((r["dealer_name"], r["city"], r["state"]) for r in rows)
    brands = set(r["brand"] for r in rows)
    body_types = set(r["body_type"] for r in rows)

    snap = db.table("report_snapshots").insert({
        "report_date": report_date,
        "file_name": os.path.basename(csv_path),
        "total_dealers": len(dealers),
        "total_vehicles": len(rows),
        "total_brands": len(brands),
        "total_body_types": len(body_types),
    }).execute()
    return snap.data[0]["id"]


def get_dealer_map(db):
    result = db.table("dealers").select("id, name, city, state").execute()
    return {(r["name"], r["city"], r["state"]): r["id"] for r in result.data}


def upsert_dealers_from_csv(db, csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        dealers = set()
        for row in reader:
            dealers.add((row["dealer_name"], row["city"], row["state"]))

    # Filter out excluded dealers (Penske, MHC, etc.)
    dealers = {d for d in dealers if not _is_excluded(d[0])}
    rows = [{"name": d[0], "city": d[1], "state": d[2]} for d in dealers]
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        db.table("dealers").upsert(batch, on_conflict="name,city,state").execute()
    logger.info(f"Upserted {len(rows)} dealers")


def enrich_new_dealers(db):
    """Fetch Google Places data for dealers missing coordinates, use Places lat/lng."""
    # Find dealers without lat/lng
    missing = db.table("dealers").select("id, name, city, state").is_("latitude", "null").execute()
    if not missing.data:
        logger.info("All dealers already geocoded — nothing to enrich")
        return

    logger.info(f"Enriching {len(missing.data)} new dealers (Places API for coords + metadata)...")

    async def _enrich_all():
        geocoded = 0
        places_ok = 0

        for d in missing.data:
            # Google Places returns precise lat/lng + all metadata in one call
            place = await search_place(d["name"], d["city"], d["state"])
            if place:
                from datetime import timezone
                lat = place.get("latitude")
                lng = place.get("longitude")

                # Update dealer coordinates from Places (street-level precision)
                if lat and lng:
                    db.table("dealers").update({
                        "latitude": lat, "longitude": lng,
                    }).eq("id", d["id"]).execute()
                    geocoded += 1

                # Cache Places metadata
                row = {
                    "dealer_id": d["id"],
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    **{k: v for k, v in place.items() if k not in ("latitude", "longitude")},
                }
                try:
                    db.table("dealer_places").upsert(row, on_conflict="dealer_id").execute()
                    places_ok += 1
                except Exception as e:
                    logger.warning(f"  Places upsert failed for {d['name']}: {e}")
            else:
                logger.warning(f"  Places not found: {d['name']} — {d['city']}, {d['state']}")

            # Rate limit (Places API allows 600 QPM, stay safe)
            await asyncio.sleep(0.2)

        logger.info(f"Enrichment complete: {geocoded}/{len(missing.data)} geocoded, {places_ok}/{len(missing.data)} Places fetched")

    asyncio.run(_enrich_all())


def load_vehicles(db, csv_path, snapshot_id, dealer_map, smyrna_vins):
    # Clear existing vehicles for this snapshot (idempotent re-run)
    db.table("vehicles").delete().eq("snapshot_id", snapshot_id).execute()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    # Build first_seen_date lookup: VIN → earliest date we've ever seen it
    # Query all previous vehicles to find the first appearance of each VIN
    logger.info("Building first_seen_date lookup from historical data...")
    first_seen_map = {}
    offset = 0
    page_size = 1000
    while True:
        batch_result = db.table("vehicles").select(
            "vin, first_seen_date"
        ).not_.is_("first_seen_date", "null").range(offset, offset + page_size - 1).execute()
        for r in batch_result.data:
            vin = r["vin"]
            seen = r["first_seen_date"]
            if vin not in first_seen_map or (seen and seen < first_seen_map[vin]):
                first_seen_map[vin] = seen
        if len(batch_result.data) < page_size:
            break
        offset += page_size
    logger.info(f"  Found first_seen_date for {len(first_seen_map)} historical VINs")

    # Get this snapshot's report_date for new VINs
    snap_row = db.table("report_snapshots").select("report_date").eq("id", snapshot_id).execute()
    today_date = snap_row.data[0]["report_date"] if snap_row.data else datetime.now().strftime("%Y-%m-%d")

    vehicles = []
    skipped = 0
    seen_vins = set()
    new_vins = 0
    carried = 0

    for row in all_rows:
        vin = row.get("vin", "").strip()
        if not vin or vin in seen_vins:
            continue
        seen_vins.add(vin)

        # Skip excluded dealers (Penske, MHC, etc.)
        if _is_excluded(row.get("dealer_name", "")):
            skipped += 1
            continue
        # Skip used vehicles — Comvoy only sells new
        if row.get("condition", "").strip().lower() != "new":
            skipped += 1
            continue

        key = (row["dealer_name"], row["city"], row["state"])
        dealer_id = dealer_map.get(key)
        if not dealer_id:
            skipped += 1
            continue

        price_str = row.get("price", "").strip()
        price = int(price_str) if price_str and price_str.isdigit() else None

        # Carry forward first_seen_date if we've seen this VIN before
        if vin in first_seen_map:
            first_seen = first_seen_map[vin]
            carried += 1
        else:
            first_seen = today_date
            new_vins += 1

        vehicles.append({
            "vin": vin,
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "brand": row.get("brand", ""),
            "model": row.get("model", ""),
            "body_type": row.get("body_type", ""),
            "body_builder": row.get("body_builder", "") or None,
            "price": price,
            "condition": row.get("condition", "") or None,
            "transmission": row.get("transmission", "") or None,
            "fuel_type": row.get("fuel_type", "") or None,
            "color": row.get("color", "") or None,
            "listing_url": row.get("listing_url", "") or None,
            "image_url": row.get("image_url", "") or None,
            "is_smyrna": vin in smyrna_vins,
            "first_seen_date": first_seen,
        })

    # Batch insert
    for i in range(0, len(vehicles), BATCH_SIZE):
        batch = vehicles[i:i + BATCH_SIZE]
        db.table("vehicles").upsert(batch, on_conflict="vin,snapshot_id").execute()
        if (i // BATCH_SIZE) % 5 == 0:
            logger.info(f"  Loaded {min(i + BATCH_SIZE, len(vehicles))}/{len(vehicles)} vehicles")

    logger.info(f"Loaded {len(vehicles)} vehicles ({skipped} skipped — no dealer match)")
    logger.info(f"  first_seen_date: {carried} carried forward, {new_vins} new VINs (first appearance)")
    return len(vehicles)


def load_aggregate_snapshots(db, csv_path, snapshot_id, dealer_map):
    """Also populate dealer_snapshots, dealer_brand_inventory, dealer_body_type_inventory
    from the vehicle-level CSV so all existing Otto tools keep working."""

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    # Build per-dealer aggregates
    from collections import defaultdict, Counter
    dealer_data = defaultdict(lambda: {"vehicles": 0, "brands": Counter(), "body_types": Counter(), "smyrna": 0})

    seen_vins = set()
    for row in all_rows:
        vin = row.get("vin", "").strip()
        if not vin or vin in seen_vins:
            continue
        seen_vins.add(vin)

        # Skip excluded dealers and used vehicles (same filters as vehicle insert)
        if _is_excluded(row.get("dealer_name", "")):
            continue
        if row.get("condition", "").strip().lower() != "new":
            continue

        key = (row["dealer_name"], row["city"], row["state"])
        if key not in dealer_map:
            continue

        d = dealer_data[key]
        d["vehicles"] += 1
        d["brands"][row.get("brand", "")] += 1
        d["body_types"][row.get("body_type", "")] += 1

    # Upsert brands & body_types lookup tables
    all_brands = set()
    all_body_types = set()
    for d in dealer_data.values():
        all_brands.update(d["brands"].keys())
        all_body_types.update(d["body_types"].keys())

    for b in all_brands:
        if b:
            db.table("brands").upsert({"name": b}, on_conflict="name").execute()
    for bt in all_body_types:
        if bt:
            db.table("body_types").upsert({"name": bt}, on_conflict="name").execute()

    brand_map = {r["name"]: r["id"] for r in db.table("brands").select("id, name").execute().data}
    bt_map = {r["name"]: r["id"] for r in db.table("body_types").select("id, name").execute().data}

    # Clear old snapshot data for idempotent re-run
    for table in ["dealer_smyrna_details", "dealer_body_type_inventory", "dealer_brand_inventory", "dealer_snapshots"]:
        db.table(table).delete().eq("snapshot_id", snapshot_id).execute()

    # Dealer snapshots
    snap_rows = []
    sorted_dealers = sorted(dealer_data.items(), key=lambda x: x[1]["vehicles"], reverse=True)
    for rank, (key, d) in enumerate(sorted_dealers, 1):
        dealer_id = dealer_map.get(key)
        if not dealer_id:
            continue
        top_brand = d["brands"].most_common(1)[0][0] if d["brands"] else None
        top_body = ", ".join(bt for bt, _ in d["body_types"].most_common(3))
        snap_rows.append({
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "rank": rank,
            "total_vehicles": d["vehicles"],
            "brand_count": len(d["brands"]),
            "body_type_count": len(d["body_types"]),
            "top_brand": top_brand,
            "smyrna_units": d["smyrna"],
            "smyrna_percentage": round(d["smyrna"] / d["vehicles"] * 100, 2) if d["vehicles"] else 0,
            "top_body_types": top_body,
        })

    for i in range(0, len(snap_rows), BATCH_SIZE):
        db.table("dealer_snapshots").upsert(snap_rows[i:i + BATCH_SIZE], on_conflict="dealer_id,snapshot_id").execute()
    logger.info(f"Loaded {len(snap_rows)} dealer snapshots")

    # Brand inventory
    brand_rows = []
    for key, d in dealer_data.items():
        dealer_id = dealer_map.get(key)
        if not dealer_id:
            continue
        for brand_name, count in d["brands"].items():
            bid = brand_map.get(brand_name)
            if bid:
                brand_rows.append({
                    "dealer_id": dealer_id,
                    "snapshot_id": snapshot_id,
                    "brand_id": bid,
                    "vehicle_count": count,
                })

    for i in range(0, len(brand_rows), BATCH_SIZE):
        db.table("dealer_brand_inventory").upsert(
            brand_rows[i:i + BATCH_SIZE], on_conflict="dealer_id,snapshot_id,brand_id"
        ).execute()
    logger.info(f"Loaded {len(brand_rows)} brand inventory rows")

    # Body type inventory
    bt_rows = []
    for key, d in dealer_data.items():
        dealer_id = dealer_map.get(key)
        if not dealer_id:
            continue
        for bt_name, count in d["body_types"].items():
            btid = bt_map.get(bt_name)
            if btid:
                bt_rows.append({
                    "dealer_id": dealer_id,
                    "snapshot_id": snapshot_id,
                    "body_type_id": btid,
                    "vehicle_count": count,
                })

    for i in range(0, len(bt_rows), BATCH_SIZE):
        db.table("dealer_body_type_inventory").upsert(
            bt_rows[i:i + BATCH_SIZE], on_conflict="dealer_id,snapshot_id,body_type_id"
        ).execute()
    logger.info(f"Loaded {len(bt_rows)} body type inventory rows")


def compute_diffs(db, snapshot_id, prev_snapshot_id):
    """Compare two snapshots and insert vehicle_diffs."""
    logger.info("Computing diffs between snapshots...")

    # Get VIN sets
    current = db.table("vehicles").select("vin, dealer_id, brand, body_type, price").eq("snapshot_id", snapshot_id).execute()
    previous = db.table("vehicles").select("vin, dealer_id, brand, body_type, price").eq("snapshot_id", prev_snapshot_id).execute()

    curr_map = {r["vin"]: r for r in current.data}
    prev_map = {r["vin"]: r for r in previous.data}

    curr_vins = set(curr_map.keys())
    prev_vins = set(prev_map.keys())

    # Clear old diffs
    db.table("vehicle_diffs").delete().eq("snapshot_id", snapshot_id).execute()

    diffs = []

    # New vehicles
    for vin in curr_vins - prev_vins:
        v = curr_map[vin]
        diffs.append({
            "snapshot_id": snapshot_id,
            "prev_snapshot_id": prev_snapshot_id,
            "diff_type": "new",
            "vin": vin,
            "dealer_id": v["dealer_id"],
            "brand": v["brand"],
            "body_type": v["body_type"],
            "new_price": v["price"],
        })

    # Sold vehicles
    for vin in prev_vins - curr_vins:
        v = prev_map[vin]
        diffs.append({
            "snapshot_id": snapshot_id,
            "prev_snapshot_id": prev_snapshot_id,
            "diff_type": "sold",
            "vin": vin,
            "dealer_id": v["dealer_id"],
            "brand": v["brand"],
            "body_type": v["body_type"],
            "old_price": v["price"],
        })

    # Price changes
    for vin in curr_vins & prev_vins:
        old_p = prev_map[vin]["price"]
        new_p = curr_map[vin]["price"]
        if old_p and new_p and old_p != new_p:
            v = curr_map[vin]
            diffs.append({
                "snapshot_id": snapshot_id,
                "prev_snapshot_id": prev_snapshot_id,
                "diff_type": "price_change",
                "vin": vin,
                "dealer_id": v["dealer_id"],
                "brand": v["brand"],
                "body_type": v["body_type"],
                "old_price": old_p,
                "new_price": new_p,
            })

    for i in range(0, len(diffs), BATCH_SIZE):
        db.table("vehicle_diffs").insert(diffs[i:i + BATCH_SIZE]).execute()

    new_count = sum(1 for d in diffs if d["diff_type"] == "new")
    sold_count = sum(1 for d in diffs if d["diff_type"] == "sold")
    price_count = sum(1 for d in diffs if d["diff_type"] == "price_change")
    logger.info(f"Diffs: {new_count} new, {sold_count} sold, {price_count} price changes")
    return {"new": new_count, "sold": sold_count, "price_changes": price_count}


def main():
    parser = argparse.ArgumentParser(description="Load scraped inventory into Supabase")
    parser.add_argument("--csv", help="Path to inventory CSV")
    parser.add_argument("--date", help="Date string YYYY-MM-DD")
    parser.add_argument("--diff", action="store_true", help="Compute diffs against previous snapshot")
    args = parser.parse_args()

    # Find CSV
    if args.csv:
        csv_path = args.csv
    elif args.date:
        csv_path = os.path.join(SCRAPE_DIR, f"inventory_{args.date}.csv")
    else:
        csv_path = find_latest_csv(SCRAPE_DIR)

    if not csv_path or not os.path.exists(csv_path):
        logger.error(f"CSV not found: {csv_path}")
        sys.exit(1)

    # Extract date from filename
    basename = os.path.basename(csv_path)
    date_str = basename.replace("inventory_", "").replace(".csv", "")

    # Find Smyrna VINs
    smyrna_csv = find_smyrna_csv(SCRAPE_DIR, date_str)
    smyrna_vins = load_smyrna_vins(smyrna_csv)

    logger.info(f"Loading {csv_path}")
    logger.info(f"Date: {date_str}")

    db = get_service_client()

    # Ensure dealers exist
    upsert_dealers_from_csv(db, csv_path)
    enrich_new_dealers(db)
    dealer_map = get_dealer_map(db)

    # Get/create snapshot
    snapshot_id = get_or_create_snapshot(db, date_str, csv_path)
    logger.info(f"Snapshot: {snapshot_id}")

    # Load vehicles
    vehicle_count = load_vehicles(db, csv_path, snapshot_id, dealer_map, smyrna_vins)

    # Load aggregate data (keeps existing Otto tools working)
    load_aggregate_snapshots(db, csv_path, snapshot_id, dealer_map)

    # Update Smyrna counts in dealer_snapshots using VIN truth
    smyrna_vehicles = db.table("vehicles").select("dealer_id").eq("snapshot_id", snapshot_id).eq("is_smyrna", True).execute()
    from collections import Counter
    smyrna_by_dealer = Counter(v["dealer_id"] for v in smyrna_vehicles.data)
    for dealer_id, count in smyrna_by_dealer.items():
        total_row = db.table("dealer_snapshots").select("total_vehicles").eq(
            "dealer_id", dealer_id).eq("snapshot_id", snapshot_id).execute()
        total = total_row.data[0]["total_vehicles"] if total_row.data else 0
        pct = round(count / total * 100, 2) if total else 0
        db.table("dealer_snapshots").update({
            "smyrna_units": count,
            "smyrna_percentage": pct,
        }).eq("dealer_id", dealer_id).eq("snapshot_id", snapshot_id).execute()
    logger.info(f"Updated Smyrna counts for {len(smyrna_by_dealer)} dealers")

    # Smyrna details table
    smyrna_detail_rows = []
    for dealer_id, count in smyrna_by_dealer.items():
        total_row = db.table("dealer_snapshots").select("total_vehicles").eq(
            "dealer_id", dealer_id).eq("snapshot_id", snapshot_id).execute()
        total = total_row.data[0]["total_vehicles"] if total_row.data else 0
        # Get top body types for smyrna vehicles at this dealer
        sv = db.table("vehicles").select("body_type").eq("dealer_id", dealer_id).eq(
            "snapshot_id", snapshot_id).eq("is_smyrna", True).execute()
        bt_counts = Counter(v["body_type"] for v in sv.data)
        top_bts = ", ".join(f"{bt} ({c})" for bt, c in bt_counts.most_common(3))
        smyrna_detail_rows.append({
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "smyrna_units": count,
            "dealer_total": total,
            "smyrna_percentage": round(count / total * 100, 2) if total else 0,
            "top_smyrna_body_types": top_bts,
        })

    if smyrna_detail_rows:
        db.table("dealer_smyrna_details").delete().eq("snapshot_id", snapshot_id).execute()
        for i in range(0, len(smyrna_detail_rows), BATCH_SIZE):
            db.table("dealer_smyrna_details").upsert(
                smyrna_detail_rows[i:i + BATCH_SIZE], on_conflict="dealer_id,snapshot_id"
            ).execute()
        logger.info(f"Loaded {len(smyrna_detail_rows)} Smyrna detail rows")

    # Diffs
    prev_id = None
    if args.diff:
        snapshots = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True).limit(2).execute()
        if len(snapshots.data) >= 2:
            prev_id = snapshots.data[1]["id"]
            compute_diffs(db, snapshot_id, prev_id)
        else:
            logger.info("Only one snapshot — skipping diff")

    # Snapshot metrics (market KPIs for agent intelligence)
    try:
        from app.api.metrics import compute_snapshot_metrics
        compute_snapshot_metrics(snapshot_id, prev_id)
    except Exception as e:
        logger.warning(f"Snapshot metrics failed (non-fatal): {e}")

    logger.info("=" * 60)
    logger.info("LOAD COMPLETE")
    logger.info(f"  Vehicles: {vehicle_count}")
    logger.info(f"  Smyrna VINs tagged: {len(smyrna_vins)}")
    logger.info(f"  Snapshot: {snapshot_id}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
