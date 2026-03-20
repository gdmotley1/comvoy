"""
Load parsed Excel data into Supabase.

Flow:
  1. Create report_snapshots row
  2. Upsert brands and body_types
  3. Upsert dealers (with geocoded coords)
  4. Insert dealer_snapshots
  5. Insert dealer_brand_inventory
  6. Insert dealer_body_type_inventory
  7. Insert dealer_smyrna_details
"""

import logging
from datetime import datetime

from app.database import get_service_client
from app.etl.geocoder import geocode_dealers

logger = logging.getLogger(__name__)

# Supabase batch insert limit
BATCH_SIZE = 500

# ── Excluded Dealers ─────────────────────────────────────────────────────────
# Rental/national chains — not sales prospects, excluded from all loads
EXCLUDED_DEALER_PATTERNS = ['penske', 'mhc ', 'ryder']


def _is_excluded_dealer(name: str) -> bool:
    """Check if dealer name matches an excluded pattern."""
    n = name.lower()
    return any(pat in n for pat in EXCLUDED_DEALER_PATTERNS)


async def load_report(parsed: dict, file_name: str) -> dict:
    """Load a fully parsed report into the database. Returns summary stats.

    Uses snapshot-scoped rollback: if any step fails after the snapshot is
    created, all rows referencing that snapshot_id are deleted to prevent
    partial/inconsistent data.
    """
    db = get_service_client()
    meta = parsed["metadata"]

    # 1. Create snapshot
    snap = db.table("report_snapshots").upsert({
        "report_date": str(meta["report_date"]),
        "file_name": file_name,
        "total_dealers": meta["total_dealers"],
        "total_vehicles": meta["total_vehicles"],
        "total_brands": meta["total_brands"],
        "total_body_types": meta["total_body_types"],
    }, on_conflict="report_date").execute()
    snapshot_id = snap.data[0]["id"]
    logger.info(f"Snapshot created: {snapshot_id} for {meta['report_date']}")

    try:
        # 2. Upsert brands
        brand_map = _upsert_brands(db, parsed["brands"])

        # 3. Upsert body types
        body_type_map = _upsert_body_types(db, parsed["body_types"])

        # 4. Geocode + upsert dealers
        dealers = parsed["dealers"]
        # Pull existing dealer coords so we don't re-geocode
        existing = db.table("dealers").select("name, city, state, latitude, longitude").execute()
        existing_coords = {
            (r["name"], r["city"], r["state"]): (r["latitude"], r["longitude"])
            for r in existing.data
            if r["latitude"] and r["longitude"]
        }
        for d in dealers:
            key = (d["name"], d["city"], d["state"])
            if key in existing_coords:
                d["latitude"] = existing_coords[key][0]
                d["longitude"] = existing_coords[key][1]

        dealers = await geocode_dealers(dealers)
        dealer_map = _upsert_dealers(db, dealers)

        # 5. Insert dealer_snapshots
        _load_dealer_snapshots(db, dealers, dealer_map, snapshot_id)

        # 6. Insert dealer_brand_inventory
        brand_inv_count = _load_brand_inventory(db, parsed["brand_matrix"], dealer_map, brand_map, snapshot_id)

        # 7. Insert dealer_body_type_inventory
        bt_inv_count = _load_body_type_inventory(db, parsed["body_type_matrix"], dealer_map, body_type_map, snapshot_id)

        # 8. Insert smyrna details
        smyrna_count = _load_smyrna_details(db, parsed["smyrna_details"], dealer_map, snapshot_id)

    except Exception as e:
        logger.error(f"ETL failed at snapshot {snapshot_id}, rolling back snapshot-scoped data")
        _rollback_snapshot(db, snapshot_id)
        raise

    geocoded = sum(1 for d in dealers if d.get("latitude"))

    return {
        "snapshot_id": snapshot_id,
        "report_date": str(meta["report_date"]),
        "dealers_loaded": len(dealer_map),
        "brands_loaded": len(brand_map),
        "body_types_loaded": len(body_type_map),
        "brand_inventory_rows": brand_inv_count,
        "body_type_inventory_rows": bt_inv_count,
        "smyrna_details_loaded": smyrna_count,
        "geocoded_count": geocoded,
    }


def _rollback_snapshot(db, snapshot_id: str):
    """Delete all data scoped to a snapshot_id to restore consistency.

    Order matters — delete child rows before parent to respect FK constraints.
    """
    tables = [
        "dealer_smyrna_details",
        "dealer_body_type_inventory",
        "dealer_brand_inventory",
        "dealer_snapshots",
        "lead_scores",
    ]
    for table in tables:
        try:
            db.table(table).delete().eq("snapshot_id", snapshot_id).execute()
            logger.info(f"Rollback: cleaned {table} for snapshot {snapshot_id}")
        except Exception as cleanup_err:
            logger.error(f"Rollback failed on {table}: {cleanup_err}")

    # Delete the snapshot record itself
    try:
        db.table("report_snapshots").delete().eq("id", snapshot_id).execute()
        logger.info(f"Rollback: deleted snapshot {snapshot_id}")
    except Exception as cleanup_err:
        logger.error(f"Rollback failed on report_snapshots: {cleanup_err}")


def _upsert_brands(db, brands: list[dict]) -> dict[str, int]:
    """Upsert brand rows, return {name: id} map."""
    for b in brands:
        db.table("brands").upsert({"name": b["name"]}, on_conflict="name").execute()
    result = db.table("brands").select("id, name").execute()
    return {r["name"]: r["id"] for r in result.data}


def _upsert_body_types(db, body_types: list[dict]) -> dict[str, int]:
    """Upsert body type rows, return {name: id} map."""
    for bt in body_types:
        db.table("body_types").upsert({"name": bt["name"]}, on_conflict="name").execute()
    result = db.table("body_types").select("id, name").execute()
    return {r["name"]: r["id"] for r in result.data}


def _upsert_dealers(db, dealers: list[dict]) -> dict[tuple, str]:
    """Upsert dealer rows in batches, return {(name, city, state): id} map."""
    # Filter out excluded dealers (Penske, MHC, etc.)
    dealers = [d for d in dealers if not _is_excluded_dealer(d["name"])]
    rows = []
    for d in dealers:
        row = {
            "name": d["name"],
            "city": d["city"],
            "state": d["state"],
        }
        if d.get("latitude"):
            row["latitude"] = d["latitude"]
            row["longitude"] = d["longitude"]
            row["geocoded_at"] = datetime.utcnow().isoformat()
        rows.append(row)

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        db.table("dealers").upsert(batch, on_conflict="name,city,state").execute()

    result = db.table("dealers").select("id, name, city, state").execute()
    return {(r["name"], r["city"], r["state"]): r["id"] for r in result.data}


def _load_dealer_snapshots(db, dealers: list[dict], dealer_map: dict, snapshot_id: str):
    """Insert dealer_snapshots rows for this month."""
    rows = []
    for d in dealers:
        key = (d["name"], d["city"], d["state"])
        dealer_id = dealer_map.get(key)
        if not dealer_id:
            continue
        rows.append({
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "rank": d["rank"],
            "total_vehicles": d["total_vehicles"],
            "brand_count": d["brand_count"],
            "body_type_count": d["body_type_count"],
            "top_brand": d["top_brand"],
            "smyrna_units": d["smyrna_units"],
            "smyrna_percentage": d["smyrna_percentage"],
            "top_body_types": d["top_body_types"],
        })
    _batch_upsert(db, "dealer_snapshots", rows, "dealer_id,snapshot_id")


def _load_brand_inventory(db, brand_matrix: list[dict], dealer_map: dict, brand_map: dict, snapshot_id: str) -> int:
    """Insert dealer_brand_inventory rows."""
    rows = []
    for r in brand_matrix:
        key = (r["dealer_name"], r["city"], r["state"])
        dealer_id = dealer_map.get(key)
        brand_id = brand_map.get(r["brand"])
        if not dealer_id or not brand_id:
            continue
        rows.append({
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "brand_id": brand_id,
            "vehicle_count": r["vehicle_count"],
        })
    _batch_upsert(db, "dealer_brand_inventory", rows, "dealer_id,snapshot_id,brand_id")
    return len(rows)


def _load_body_type_inventory(db, bt_matrix: list[dict], dealer_map: dict, body_type_map: dict, snapshot_id: str) -> int:
    """Insert dealer_body_type_inventory rows."""
    rows = []
    for r in bt_matrix:
        key = (r["dealer_name"], r["city"], r["state"])
        dealer_id = dealer_map.get(key)
        bt_id = body_type_map.get(r["body_type"])
        if not dealer_id or not bt_id:
            continue
        rows.append({
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "body_type_id": bt_id,
            "vehicle_count": r["vehicle_count"],
        })
    _batch_upsert(db, "dealer_body_type_inventory", rows, "dealer_id,snapshot_id,body_type_id")
    return len(rows)


def _load_smyrna_details(db, smyrna: list[dict], dealer_map: dict, snapshot_id: str) -> int:
    """Insert dealer_smyrna_details rows."""
    rows = []
    for s in smyrna:
        key = (s["dealer_name"], s["city"], s["state"])
        dealer_id = dealer_map.get(key)
        if not dealer_id:
            continue
        rows.append({
            "dealer_id": dealer_id,
            "snapshot_id": snapshot_id,
            "smyrna_units": s["smyrna_units"],
            "dealer_total": s["dealer_total"],
            "smyrna_percentage": s["smyrna_percentage"],
            "top_smyrna_body_types": s["top_smyrna_body_types"],
            "avg_days_since_upfit": s["avg_days_since_upfit"] or None,
        })
    _batch_upsert(db, "dealer_smyrna_details", rows, "dealer_id,snapshot_id")
    return len(rows)


def _batch_upsert(db, table: str, rows: list[dict], on_conflict: str):
    """Insert rows in batches with upsert to handle re-runs gracefully."""
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        db.table(table).upsert(batch, on_conflict=on_conflict).execute()
