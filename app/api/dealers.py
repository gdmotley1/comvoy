"""Dealer query endpoints — used by both the API and the agent tools."""

import logging
import time

from fastapi import APIRouter, Query, HTTPException, Response

from app.database import get_service_client
from app.models import DealerSummary, DealerBriefing, NearbyQuery, SnapshotInfo

router = APIRouter(prefix="/api/dealers", tags=["dealers"])
logger = logging.getLogger(__name__)

# In-memory TTL cache — data only changes after monthly scrape
_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 300  # 5 minutes

# ── Excluded Dealers ─────────────────────────────────────────────────────────
# Rental/national chains — not sales prospects, excluded from all results
EXCLUDED_DEALER_PATTERNS = ['penske', 'mhc ']


def _is_excluded(name: str) -> bool:
    """Check if dealer name matches an excluded pattern."""
    n = name.lower()
    return any(pat in n for pat in EXCLUDED_DEALER_PATTERNS)


def _latest_snapshot_id(db) -> str:
    """Get the most recent snapshot ID."""
    result = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    if not result.data:
        raise HTTPException(404, "No report snapshots found. Upload a report first.")
    return result.data[0]["id"]


@router.get("/snapshots", response_model=list[SnapshotInfo])
def list_snapshots(response: Response):
    """List all ingested report snapshots."""
    response.headers["Cache-Control"] = "public, s-maxage=300, stale-while-revalidate=60"
    db = get_service_client()
    result = db.table("report_snapshots").select("*").order("report_date", desc=True).execute()
    return result.data


@router.get("/search", response_model=list[DealerSummary])
def search_dealers(
    q: str = Query(None, description="Search dealer name"),
    state: str = Query(None, description="Filter by state (2-letter code)"),
    min_vehicles: int = Query(None, description="Minimum total vehicles"),
    has_smyrna: bool = Query(None, description="Only dealers with Smyrna products"),
    limit: int = Query(50, le=200),
):
    """Search dealers with filters."""
    db = get_service_client()
    snap_id = _latest_snapshot_id(db)

    query = db.table("dealer_snapshots").select(
        "*, dealers!inner(id, name, city, state, latitude, longitude)"
    ).eq("snapshot_id", snap_id)

    if state:
        query = query.eq("dealers.state", state.upper())
    if min_vehicles:
        query = query.gte("total_vehicles", min_vehicles)
    if has_smyrna:
        query = query.gt("smyrna_units", 0)

    query = query.order("total_vehicles", desc=True).limit(limit)
    result = query.execute()

    dealers = []
    for row in result.data:
        d = row["dealers"]
        if q and q.lower() not in d["name"].lower():
            continue
        if _is_excluded(d["name"]):
            continue
        dealers.append(DealerSummary(
            id=d["id"],
            name=d["name"],
            city=d["city"],
            state=d["state"],
            latitude=d.get("latitude"),
            longitude=d.get("longitude"),
            total_vehicles=row["total_vehicles"],
            brand_count=row["brand_count"],
            body_type_count=row["body_type_count"],
            top_brand=row["top_brand"],
            smyrna_units=row["smyrna_units"] or 0,
            smyrna_percentage=float(row["smyrna_percentage"] or 0),
            top_body_types=row["top_body_types"],
            rank=row["rank"],
        ))
    return dealers


@router.post("/nearby", response_model=list[DealerSummary])
def find_nearby(query: NearbyQuery):
    """Find dealers within a radius of a lat/lng point."""
    db = get_service_client()
    snap_id = _latest_snapshot_id(db)

    # Use the PostGIS function we created in the migration
    result = db.rpc("find_nearby_dealers", {
        "p_lat": query.latitude,
        "p_lng": query.longitude,
        "p_radius_miles": query.radius_miles,
    }).execute()

    if not result.data:
        return []

    # Enrich with snapshot data
    dealer_ids = [r["dealer_id"] for r in result.data]
    distance_map = {r["dealer_id"]: r["distance_miles"] for r in result.data}

    snapshots = db.table("dealer_snapshots").select(
        "*, dealers!inner(id, name, city, state, latitude, longitude)"
    ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()

    dealers = []
    for row in snapshots.data:
        d = row["dealers"]
        if _is_excluded(d["name"]):
            continue
        dealers.append(DealerSummary(
            id=d["id"],
            name=d["name"],
            city=d["city"],
            state=d["state"],
            latitude=d.get("latitude"),
            longitude=d.get("longitude"),
            total_vehicles=row["total_vehicles"],
            brand_count=row["brand_count"],
            body_type_count=row["body_type_count"],
            top_brand=row["top_brand"],
            smyrna_units=row["smyrna_units"] or 0,
            smyrna_percentage=float(row["smyrna_percentage"] or 0),
            top_body_types=row["top_body_types"],
            rank=row["rank"],
            distance_miles=distance_map.get(d["id"]),
        ))

    dealers.sort(key=lambda x: x.distance_miles or 999)
    return dealers


@router.get("/{dealer_id}/briefing", response_model=DealerBriefing)
def get_dealer_briefing(dealer_id: str):
    """Get a full pre-call briefing for a dealer."""
    db = get_service_client()
    snap_id = _latest_snapshot_id(db)

    # Dealer info + latest snapshot
    dealer = db.table("dealers").select("*").eq("id", dealer_id).single().execute()
    if not dealer.data:
        raise HTTPException(404, "Dealer not found")

    snapshot = db.table("dealer_snapshots").select("*").eq(
        "dealer_id", dealer_id
    ).eq("snapshot_id", snap_id).single().execute()

    summary = DealerSummary(
        id=dealer.data["id"],
        name=dealer.data["name"],
        city=dealer.data["city"],
        state=dealer.data["state"],
        latitude=dealer.data.get("latitude"),
        longitude=dealer.data.get("longitude"),
        total_vehicles=snapshot.data["total_vehicles"] if snapshot.data else 0,
        brand_count=snapshot.data["brand_count"] if snapshot.data else None,
        body_type_count=snapshot.data["body_type_count"] if snapshot.data else None,
        top_brand=snapshot.data["top_brand"] if snapshot.data else None,
        smyrna_units=snapshot.data["smyrna_units"] or 0 if snapshot.data else 0,
        smyrna_percentage=float(snapshot.data["smyrna_percentage"] or 0) if snapshot.data else 0,
        top_body_types=snapshot.data["top_body_types"] if snapshot.data else None,
        rank=snapshot.data["rank"] if snapshot.data else None,
    )

    # Brand breakdown
    brands = db.table("dealer_brand_inventory").select(
        "vehicle_count, brands(name)"
    ).eq("dealer_id", dealer_id).eq("snapshot_id", snap_id).order(
        "vehicle_count", desc=True
    ).execute()

    brand_breakdown = [
        {"brand": r["brands"]["name"], "vehicles": r["vehicle_count"]}
        for r in brands.data
    ]

    # Body type breakdown
    body_types = db.table("dealer_body_type_inventory").select(
        "vehicle_count, body_types(name)"
    ).eq("dealer_id", dealer_id).eq("snapshot_id", snap_id).order(
        "vehicle_count", desc=True
    ).execute()

    body_type_breakdown = [
        {"body_type": r["body_types"]["name"], "vehicles": r["vehicle_count"]}
        for r in body_types.data
    ]

    # Smyrna details
    smyrna = db.table("dealer_smyrna_details").select("*").eq(
        "dealer_id", dealer_id
    ).eq("snapshot_id", snap_id).execute()

    smyrna_details = None
    if smyrna.data:
        s = smyrna.data[0]
        smyrna_details = {
            "smyrna_units": s["smyrna_units"],
            "dealer_total": s["dealer_total"],
            "smyrna_percentage": float(s["smyrna_percentage"] or 0),
            "top_smyrna_body_types": s["top_smyrna_body_types"],
            "avg_days_since_upfit": s["avg_days_since_upfit"],
        }

    return DealerBriefing(
        dealer=summary,
        brand_breakdown=brand_breakdown,
        body_type_breakdown=body_type_breakdown,
        smyrna_details=smyrna_details,
    )


@router.get("/map")
def get_map_data(response: Response):
    """All dealers with lat/lng + lead scores for map display."""
    response.headers["Cache-Control"] = "public, s-maxage=300, stale-while-revalidate=60"

    # Check cache
    now = time.time()
    if "map" in _cache:
        ts, cached = _cache["map"]
        if now - ts < _CACHE_TTL:
            return cached

    db = get_service_client()
    snap_id = _latest_snapshot_id(db)

    result = db.table("dealer_snapshots").select(
        "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, top_brand, rank, "
        "dealers!inner(id, name, city, state, latitude, longitude)"
    ).eq("snapshot_id", snap_id).execute()

    dealer_ids = [r["dealers"]["id"] for r in result.data if r["dealers"].get("latitude")]

    score_map = {}
    if dealer_ids:
        scores = db.table("lead_scores").select(
            "dealer_id, score, tier"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        score_map = {r["dealer_id"]: r for r in scores.data}

    # Fetch cached Places data (single query, no API calls)
    places_map = {}
    try:
        places_data = db.table("dealer_places").select(
            "dealer_id, rating, review_count, phone, hours_json, business_status, formatted_address"
        ).execute()
        places_map = {r["dealer_id"]: r for r in (places_data.data or [])}
    except Exception as e:
        logger.debug(f"dealer_places fetch skipped: {e}")

    # Fetch body type inventory for all dealers (for map filtering)
    body_type_map: dict[int, list[str]] = {}
    if dealer_ids:
        try:
            bt_data = db.table("dealer_body_type_inventory").select(
                "dealer_id, vehicle_count, body_types(name)"
            ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
            for r in (bt_data.data or []):
                did = r["dealer_id"]
                bt_name = r["body_types"]["name"]
                if did not in body_type_map:
                    body_type_map[did] = []
                body_type_map[did].append(bt_name)
        except Exception as e:
            logger.warning(f"Body type inventory fetch failed: {e}")

    # Fetch brand inventory for all dealers (for map filtering)
    brand_map: dict[int, list[str]] = {}
    if dealer_ids:
        try:
            br_data = db.table("dealer_brand_inventory").select(
                "dealer_id, vehicle_count, brands(name)"
            ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
            for r in (br_data.data or []):
                did = r["dealer_id"]
                br_name = r["brands"]["name"]
                if did not in brand_map:
                    brand_map[did] = []
                brand_map[did].append(br_name)
        except Exception as e:
            logger.warning(f"Brand inventory fetch failed: {e}")

    markers = []
    for row in result.data:
        d = row["dealers"]
        if not d.get("latitude") or not d.get("longitude"):
            continue
        # Skip excluded dealers (Penske, MHC, etc.)
        if _is_excluded(d["name"]):
            continue
        sc = score_map.get(d["id"], {})
        pl = places_map.get(d["id"], {})
        markers.append({
            "id": d["id"],
            "name": d["name"],
            "city": d["city"],
            "state": d["state"],
            "lat": d["latitude"],
            "lng": d["longitude"],
            "vehicles": row["total_vehicles"],
            "smyrna": row["smyrna_units"] or 0,
            "smyrna_pct": float(row["smyrna_percentage"] or 0),
            "top_brand": row["top_brand"],
            "rank": row["rank"],
            "score": sc.get("score"),
            "tier": sc.get("tier"),
            "rating": pl.get("rating"),
            "reviews": pl.get("review_count"),
            "hours": pl.get("hours_json"),
            "biz_status": pl.get("business_status"),
            "address": pl.get("formatted_address"),
            "body_types": body_type_map.get(d["id"], []),
            "brands": brand_map.get(d["id"], []),
        })

    resp = {"dealers": markers, "total": len(markers)}
    _cache["map"] = (time.time(), resp)
    return resp


@router.get("/territory/{state}")
def get_territory_summary(state: str):
    """Get a state-level territory summary."""
    # Check cache
    cache_key = f"territory_{state.upper()}"
    now = time.time()
    if cache_key in _cache:
        ts, cached = _cache[cache_key]
        if now - ts < _CACHE_TTL:
            return cached

    db = get_service_client()
    snap_id = _latest_snapshot_id(db)

    result = db.table("dealer_snapshots").select(
        "total_vehicles, smyrna_units, smyrna_percentage, rank, dealers!inner(id, name, city, state)"
    ).eq("snapshot_id", snap_id).eq("dealers.state", state.upper()).order(
        "total_vehicles", desc=True
    ).execute()

    if not result.data:
        raise HTTPException(404, f"No dealers found in {state.upper()}")

    filtered = [r for r in result.data if not _is_excluded(r["dealers"]["name"])]

    total_dealers = len(filtered)
    total_vehicles = sum(r["total_vehicles"] or 0 for r in filtered)
    total_smyrna = sum(r["smyrna_units"] or 0 for r in filtered)
    dealers_with_smyrna = sum(1 for r in filtered if (r["smyrna_units"] or 0) > 0)

    top_dealers = [
        {
            "name": r["dealers"]["name"],
            "city": r["dealers"]["city"],
            "total_vehicles": r["total_vehicles"],
            "smyrna_units": r["smyrna_units"] or 0,
            "rank": r["rank"],
        }
        for r in filtered[:10]
    ]

    resp = {
        "state": state.upper(),
        "total_dealers": total_dealers,
        "total_vehicles": total_vehicles,
        "total_smyrna_units": total_smyrna,
        "dealers_with_smyrna": dealers_with_smyrna,
        "smyrna_penetration_pct": round(dealers_with_smyrna / total_dealers * 100, 1) if total_dealers else 0,
        "top_10_dealers": top_dealers,
    }
    _cache[cache_key] = (time.time(), resp)
    return resp


# ---------------------------------------------------------------------------
# Google Places data endpoints
# ---------------------------------------------------------------------------

@router.get("/{dealer_id}/places")
async def get_dealer_places_endpoint(
    dealer_id: str,
    refresh: bool = Query(False, description="Force refresh from Google"),
):
    """Get Google Places business data for a dealer (lazy-fetches if needed)."""
    from app.etl.places import get_dealer_places, format_hours_today

    places = await get_dealer_places(dealer_id, force_refresh=refresh)
    if not places:
        return {"status": "not_found", "dealer_id": dealer_id}

    result = {
        "status": "ok",
        "dealer_id": dealer_id,
        "places": {
            "rating": places.get("rating"),
            "review_count": places.get("review_count"),
            "phone": places.get("phone"),
            "website": places.get("website"),
            "google_maps_url": places.get("google_maps_url"),
            "formatted_address": places.get("formatted_address"),
            "business_status": places.get("business_status"),
            "hours_today": format_hours_today(places.get("hours_json")),
            "hours_json": places.get("hours_json"),
            "photos_json": places.get("photos_json"),
            "fetched_at": places.get("fetched_at"),
        },
    }
    return result


@router.post("/places/enrich")
async def enrich_all_places(
    limit: int = Query(50, le=588, description="Max dealers to enrich"),
):
    """Bulk enrich dealers with Google Places data. Management endpoint."""
    from app.etl.places import enrich_dealers_bulk

    db = get_service_client()
    all_dealers = db.table("dealers").select("id").limit(limit).execute()
    dealer_ids = [r["id"] for r in (all_dealers.data or [])]

    result = await enrich_dealers_bulk(dealer_ids=dealer_ids, batch_size=10)
    return {"status": "ok", **result}
