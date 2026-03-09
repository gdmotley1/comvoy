"""Travel & route optimization — finds dealers along a rep's daily route.

Uses Google Directions API to get the real driving route polyline,
then PostGIS corridor search to find dealers within X miles of the
actual highway. Falls back to straight line if no API key.

When start ≈ end (same-location day trip), falls back to a radius
search sorted by distance from base, since route_position is
meaningless on a zero-length line.

CRUD endpoints let reps/VPs enter trip details. On save, a BackgroundTask
auto-generates a route briefing email with dealer intel — zero Claude cost.
"""

import asyncio
import logging
import math
from datetime import date

from fastapi import APIRouter, Query, HTTPException, BackgroundTasks

from app.database import get_service_client
from app.etl.geocoder import geocode_single
from app.etl.routing import get_driving_route
from app.models import TravelPlanCreate, TravelPlanUpdate

router = APIRouter(prefix="/api/travel", tags=["travel"])
logger = logging.getLogger(__name__)


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two points."""
    R = 6371  # Earth radius km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@router.get("/reps")
def list_reps():
    """List all active sales reps."""
    db = get_service_client()
    result = db.table("reps").select("id, name, email, territory_states").eq(
        "is_active", True
    ).execute()
    return result.data


@router.get("/plans/{rep_id}")
def get_travel_plans(
    rep_id: str,
    start_date: date = Query(None, description="Filter from this date"),
    end_date: date = Query(None, description="Filter to this date"),
):
    """Get travel plans for a rep, optionally filtered by date range."""
    db = get_service_client()

    query = db.table("rep_travel_plans").select("*").eq("rep_id", rep_id).order("travel_date")

    if start_date:
        query = query.gte("travel_date", start_date.isoformat())
    if end_date:
        query = query.lte("travel_date", end_date.isoformat())

    result = query.execute()
    return result.data


@router.get("/route-dealers")
async def get_route_dealers(
    rep_id: str = Query(..., description="Rep UUID"),
    travel_date: date = Query(..., description="Travel date (YYYY-MM-DD)"),
    buffer_miles: float = Query(20, description="How far off-route to search (miles)"),
    limit: int = Query(25, le=50),
):
    """Find dealers along a rep's route for a specific travel day.

    Returns dealers within buffer_miles of the straight-line path from
    start → end location, enriched with lead scores and inventory data.
    """
    db = get_service_client()

    # Get the travel plan
    plan = db.table("rep_travel_plans").select("*").eq(
        "rep_id", rep_id
    ).eq("travel_date", travel_date.isoformat()).execute()

    if not plan.data:
        raise HTTPException(404, f"No travel plan for {travel_date}")

    p = plan.data[0]

    # Auto-backfill: if trip has no polyline, fetch one from Google and save it
    if not p.get("route_polyline"):
        try:
            route_wkt = await get_driving_route(
                p["start_lat"], p["start_lng"], p["end_lat"], p["end_lng"]
            )
            if route_wkt:
                db.table("rep_travel_plans").update(
                    {"route_polyline": route_wkt}
                ).eq("id", p["id"]).execute()
                p["route_polyline"] = route_wkt
                logger.info(f"Backfilled route polyline for plan {p['id']}")
        except Exception as e:
            logger.warning(f"Polyline backfill failed: {e}")

    # Detect same-location day trip (start ≈ end within ~0.5 miles)
    # On a zero-length line, ST_LineLocatePoint returns 0 for everything,
    # so route_position is meaningless. We flag it and sort by distance instead.
    dist_km = _haversine(p["start_lat"], p["start_lng"], p["end_lat"], p["end_lng"])
    is_day_trip = dist_km < 0.8  # ~0.5 miles

    # Use PostGIS corridor search (real driving route if polyline exists)
    rpc_params = {
        "p_start_lat": p["start_lat"],
        "p_start_lng": p["start_lng"],
        "p_end_lat": p["end_lat"],
        "p_end_lng": p["end_lng"],
        "p_buffer_miles": buffer_miles,
    }
    if p.get("route_polyline"):
        rpc_params["p_polyline_wkt"] = p["route_polyline"]

    route_dealers = db.rpc("find_dealers_along_route", rpc_params).execute()

    if not route_dealers.data:
        return {
            "travel_date": travel_date.isoformat(),
            "start": p["start_location"],
            "end": p["end_location"],
            "dealers": [],
            "total": 0,
        }

    dealer_ids = [r["dealer_id"] for r in route_dealers.data]
    dist_map = {r["dealer_id"]: r["distance_miles"] for r in route_dealers.data}
    # route_position: 0.0 = at start, 1.0 = at end (from PostGIS ST_LineLocatePoint)
    pos_map = {r["dealer_id"]: r.get("route_position", 0) for r in route_dealers.data}

    # Get latest snapshot data
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    snap_id = snap.data[0]["id"] if snap.data else None

    # Get inventory data
    inv_map = {}
    if snap_id:
        inv = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank, top_brand"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        inv_map = {r["dealer_id"]: r for r in inv.data}

    # Get lead scores
    score_map = {}
    if snap_id:
        scores = db.table("lead_scores").select(
            "dealer_id, score, tier, opportunity_type"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        score_map = {r["dealer_id"]: r for r in scores.data}

    # Build enriched results
    dealers = []
    for rd in route_dealers.data:
        did = rd["dealer_id"]
        inv = inv_map.get(did, {})
        sc = score_map.get(did, {})

        dealers.append({
            "dealer_id": did,
            "name": rd["dealer_name"],
            "city": rd["city"],
            "state": rd["state"],
            "lat": rd["latitude"],
            "lng": rd["longitude"],
            "route_position": round(pos_map.get(did, 0), 3),
            "dist_from_route_mi": dist_map.get(did, 0),
            "vehicles": inv.get("total_vehicles", 0),
            "smyrna_units": inv.get("smyrna_units", 0),
            "rank": inv.get("rank"),
            "top_brand": inv.get("top_brand"),
            "lead_score": sc.get("score"),
            "lead_tier": sc.get("tier"),
            "opportunity": sc.get("opportunity_type"),
        })

    if is_day_trip:
        # Same-location day trip: sort by distance from base (closest first)
        dealers.sort(key=lambda x: x["dist_from_route_mi"])
    else:
        # Route: sort by position along route (start → end)
        dealers.sort(key=lambda x: x["route_position"])
    dealers = dealers[:limit]

    result = {
        "travel_date": travel_date.isoformat(),
        "rep_id": rep_id,
        "start": p["start_location"],
        "end": p["end_location"],
        "notes": p.get("notes"),
        "buffer_miles": buffer_miles,
        "dealers": dealers,
        "total": len(dealers),
    }
    if is_day_trip:
        result["mode"] = "radius"  # signals this is a proximity search, not a route
    return result


# ---------------------------------------------------------------------------
# Geocoding helper
# ---------------------------------------------------------------------------

async def _geocode_location(text: str) -> tuple[float, float] | None:
    """Parse freeform text ('Hampton Inn, Macon GA') into (lat, lng).

    Extracts city/state, then calls Nominatim via geocode_single().
    """
    parts = [p.strip().rstrip(",") for p in text.replace(",", " ").split() if p.strip()]
    city_parts = list(parts)
    state = ""
    for i in range(len(parts) - 1, -1, -1):
        if len(parts[i]) == 2 and parts[i].isalpha():
            state = parts[i].upper()
            city_parts = parts[:i]
            break
    city = " ".join(city_parts) if city_parts else text
    return await geocode_single(city, state)


# ---------------------------------------------------------------------------
# CRUD — Create / Update / Delete travel plans
# ---------------------------------------------------------------------------

@router.post("/plans")
async def create_travel_plan(body: TravelPlanCreate, bg: BackgroundTasks):
    """Create a trip day. Geocodes locations, fires auto-briefing email."""
    db = get_service_client()

    # Validate rep exists
    rep = db.table("reps").select("id, name, email").eq("id", body.rep_id).execute()
    if not rep.data:
        raise HTTPException(404, "Rep not found")

    # Geocode start + end concurrently
    start_coords, end_coords = await asyncio.gather(
        _geocode_location(body.start_location),
        _geocode_location(body.end_location),
    )

    if not start_coords:
        raise HTTPException(422, f"Could not geocode start location: {body.start_location}")
    if not end_coords:
        raise HTTPException(422, f"Could not geocode end location: {body.end_location}")

    # Fetch real driving route polyline (falls back gracefully if unavailable)
    route_wkt = await get_driving_route(
        start_coords[0], start_coords[1],
        end_coords[0], end_coords[1],
    )

    record = {
        "rep_id": body.rep_id,
        "travel_date": body.travel_date.isoformat(),
        "start_location": body.start_location,
        "start_lat": start_coords[0],
        "start_lng": start_coords[1],
        "end_location": body.end_location,
        "end_lat": end_coords[0],
        "end_lng": end_coords[1],
        "notes": body.notes,
        "route_polyline": route_wkt,
    }

    try:
        result = db.table("rep_travel_plans").insert(record).execute()
    except Exception as e:
        err = str(e)
        if "duplicate" in err.lower() or "unique" in err.lower() or "23505" in err:
            raise HTTPException(409, f"Travel plan already exists for {body.travel_date}")
        raise HTTPException(500, f"Database error: {err}")

    plan = result.data[0] if result.data else record

    # Fire auto-briefing in background (doesn't block response)
    bg.add_task(_fire_auto_brief, plan["id"] if "id" in plan else None, plan)

    return {"status": "created", "plan": plan}


@router.put("/plans/{plan_id}")
async def update_travel_plan(plan_id: str, body: TravelPlanUpdate, bg: BackgroundTasks):
    """Update an existing trip day. Re-geocodes if locations changed."""
    db = get_service_client()

    existing = db.table("rep_travel_plans").select("*").eq("id", plan_id).execute()
    if not existing.data:
        raise HTTPException(404, "Travel plan not found")
    old = existing.data[0]

    updates = {}
    locations_changed = False

    if body.travel_date is not None:
        updates["travel_date"] = body.travel_date.isoformat()

    if body.start_location is not None and body.start_location != old["start_location"]:
        coords = await _geocode_location(body.start_location)
        if not coords:
            raise HTTPException(422, f"Could not geocode start location: {body.start_location}")
        updates["start_location"] = body.start_location
        updates["start_lat"] = coords[0]
        updates["start_lng"] = coords[1]
        locations_changed = True

    if body.end_location is not None and body.end_location != old["end_location"]:
        coords = await _geocode_location(body.end_location)
        if not coords:
            raise HTTPException(422, f"Could not geocode end location: {body.end_location}")
        updates["end_location"] = body.end_location
        updates["end_lat"] = coords[0]
        updates["end_lng"] = coords[1]
        locations_changed = True

    if body.notes is not None:
        updates["notes"] = body.notes

    # Re-fetch driving route if either location changed
    if locations_changed:
        merged = {**old, **updates}
        route_wkt = await get_driving_route(
            merged["start_lat"], merged["start_lng"],
            merged["end_lat"], merged["end_lng"],
        )
        updates["route_polyline"] = route_wkt

    if not updates:
        return {"status": "no_changes", "plan": old}

    result = db.table("rep_travel_plans").update(updates).eq("id", plan_id).execute()
    plan = result.data[0] if result.data else {**old, **updates}

    # Re-brief if route changed
    if locations_changed:
        bg.add_task(_fire_auto_brief, plan_id, plan)

    return {"status": "updated", "plan": plan, "rebriefed": locations_changed}


@router.delete("/plans/{plan_id}")
def delete_travel_plan(plan_id: str):
    """Delete a trip day."""
    db = get_service_client()

    existing = db.table("rep_travel_plans").select("id").eq("id", plan_id).execute()
    if not existing.data:
        raise HTTPException(404, "Travel plan not found")

    db.table("rep_travel_plans").delete().eq("id", plan_id).execute()
    return {"status": "deleted", "plan_id": plan_id}


# ---------------------------------------------------------------------------
# Auto-briefing background task
# ---------------------------------------------------------------------------

def _fire_auto_brief(plan_id: str | None, plan_data: dict):
    """Background task: generate + email route briefing. Fails gracefully."""
    try:
        from app.api.briefing import auto_brief_trip
        if plan_id:
            auto_brief_trip(plan_id)
        else:
            logger.warning("No plan_id for auto-brief — skipping")
    except ImportError:
        logger.warning("briefing module not available — skipping auto-brief")
    except Exception as e:
        logger.error(f"Auto-brief failed for plan {plan_id}: {e}")
