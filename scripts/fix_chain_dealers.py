"""
Re-fetch Google Places data for chain dealers (Penske, etc.) that got mapped
to regional offices instead of their actual city locations.

The root cause was comma-separated query format in places.py which has been fixed.
This script re-runs the Places lookup for affected dealers and updates their coordinates.

Usage:
    python scripts/fix_chain_dealers.py              # fix known chain dealers
    python scripts/fix_chain_dealers.py --all         # re-fetch ALL dealers
    python scripts/fix_chain_dealers.py --dry-run     # just list affected dealers
"""

import sys
import os
import asyncio
import argparse
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from app.database import get_service_client
from app.etl.places import search_place
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# Chain dealer name patterns known to return wrong Places results with comma format
CHAIN_PATTERNS = [
    "penske",
    "ryder",
    "idealease",
    "rush truck",
    "nextran",
    "mhc kenworth",
    "bruckner",
    "transwest",
    "inland kenworth",
    "truckland",
]


def find_chain_dealers(db):
    """Find dealers whose names match known chain patterns."""
    all_dealers = db.table("dealers").select("id, name, city, state, latitude, longitude").execute()
    chain_dealers = []
    for d in (all_dealers.data or []):
        name_lower = d["name"].lower()
        if any(p in name_lower for p in CHAIN_PATTERNS):
            chain_dealers.append(d)
    return chain_dealers


async def refetch_dealer(db, dealer):
    """Re-fetch Places data for a single dealer and update coords."""
    place = await search_place(dealer["name"], dealer["city"], dealer["state"])
    if not place:
        logger.warning(f"  No Places result for: {dealer['name']} ({dealer['city']}, {dealer['state']})")
        return False

    lat = place.get("latitude")
    lng = place.get("longitude")

    if not lat or not lng:
        logger.warning(f"  No coordinates in Places result for: {dealer['name']}")
        return False

    old_lat = dealer.get("latitude")
    old_lng = dealer.get("longitude")
    moved = False
    if old_lat and old_lng:
        # Check if location changed significantly (> 0.01 degrees ~ 1km)
        if abs(lat - old_lat) > 0.01 or abs(lng - old_lng) > 0.01:
            moved = True
            logger.info(f"  MOVED: {dealer['name']} ({dealer['city']}, {dealer['state']})")
            logger.info(f"    Old: ({old_lat:.4f}, {old_lng:.4f}) → New: ({lat:.4f}, {lng:.4f})")

    # Update dealer coordinates
    db.table("dealers").update({
        "latitude": lat,
        "longitude": lng,
    }).eq("id", dealer["id"]).execute()

    # Upsert dealer_places cache
    row = {
        "dealer_id": dealer["id"],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        **{k: v for k, v in place.items() if k not in ("latitude", "longitude")},
    }
    db.table("dealer_places").upsert(row, on_conflict="dealer_id").execute()

    return moved


async def main(args):
    db = get_service_client()

    if args.all:
        dealers_result = db.table("dealers").select("id, name, city, state, latitude, longitude").execute()
        dealers = dealers_result.data or []
        logger.info(f"Re-fetching Places for ALL {len(dealers)} dealers")
    else:
        dealers = find_chain_dealers(db)
        logger.info(f"Found {len(dealers)} chain dealers to re-fetch")

    if args.dry_run:
        for d in dealers:
            logger.info(f"  {d['name']} — {d['city']}, {d['state']} ({d.get('latitude', '?')}, {d.get('longitude', '?')})")
        return

    moved = 0
    failed = 0
    for i, dealer in enumerate(dealers):
        logger.info(f"[{i+1}/{len(dealers)}] {dealer['name']} ({dealer['city']}, {dealer['state']})")
        try:
            was_moved = await refetch_dealer(db, dealer)
            if was_moved:
                moved += 1
        except Exception as e:
            logger.error(f"  Error: {e}")
            failed += 1
        await asyncio.sleep(0.2)  # Rate limit

    logger.info(f"\nDone: {len(dealers)} dealers processed, {moved} moved, {failed} failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Re-fetch all dealers, not just chains")
    parser.add_argument("--dry-run", action="store_true", help="Just list affected dealers")
    args = parser.parse_args()
    asyncio.run(main(args))
