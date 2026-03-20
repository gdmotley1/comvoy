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
from datetime import date, datetime, timezone

from typing import Optional

from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.database import get_service_client
from app.etl.geocoder import geocode_single
from app.etl.routing import get_driving_route
from app.models import (
    TravelPlanCreate, TravelPlanUpdate,
    TripCreate, TripUpdate, TripDayInput, TripStopUpdate, TripStopBulkSet,
)

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


class RepCreate(BaseModel):
    name: str
    email: str
    territory_states: Optional[list[str]] = None
    focus_body_types: Optional[list[str]] = None


@router.post("/reps")
def create_rep(body: RepCreate, background_tasks: BackgroundTasks):
    """Create a new sales rep."""
    db = get_service_client()
    row = {
        "name": body.name,
        "email": body.email,
        "territory_states": body.territory_states or [],
        "is_active": True,
    }
    if body.focus_body_types:
        row["focus_body_types"] = body.focus_body_types
    result = db.table("reps").insert(row).execute()
    if not result.data:
        raise HTTPException(500, "Failed to create rep")

    # Send welcome email in background (best-effort)
    if body.email:
        try:
            from app.api.briefing import send_welcome_email
            background_tasks.add_task(
                send_welcome_email, body.name, body.email, body.territory_states
            )
        except Exception as e:
            logger.warning(f"Could not queue welcome email: {e}")

    return {"status": "created", "rep": result.data[0]}


@router.get("/geocode")
async def geocode_location_endpoint(q: str = Query(..., description="Location text to geocode")):
    """Geocode a location string to lat/lng for live route preview."""
    coords = await _geocode_location(q)
    if not coords:
        raise HTTPException(404, f"Could not geocode: {q}")
    return {"lat": coords[0], "lng": coords[1]}


@router.get("/preview-route")
async def preview_route(
    start: str = Query(..., description="Start location text"),
    end: str = Query("", description="End location text"),
    round_trip: bool = Query(False, description="Round trip (end = start)"),
):
    """Geocode start + end, fetch driving route polyline for live map preview."""
    start_coords = await _geocode_location(start)
    if not start_coords:
        raise HTTPException(422, f"Could not geocode start: {start}")

    if round_trip or not end:
        # Round trip: end = start, no driving route needed
        return {
            "start": {"lat": start_coords[0], "lng": start_coords[1]},
            "end": {"lat": start_coords[0], "lng": start_coords[1]},
            "route_polyline": None,
            "is_round_trip": True,
        }

    end_coords = await _geocode_location(end)
    if not end_coords:
        raise HTTPException(422, f"Could not geocode end: {end}")

    # Fetch real driving route (WKT LINESTRING)
    route_wkt = await get_driving_route(
        start_coords[0], start_coords[1],
        end_coords[0], end_coords[1],
    )

    return {
        "start": {"lat": start_coords[0], "lng": start_coords[1]},
        "end": {"lat": end_coords[0], "lng": end_coords[1]},
        "route_polyline": route_wkt,  # WKT LINESTRING or None
    }


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
    optimize: bool = Query(False, description="Use Distance Matrix API to optimize stop order with real drive times"),
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
            "dealer_id, score, tier"
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
        })

    if is_day_trip:
        # Same-location day trip: sort by distance from base (closest first)
        dealers.sort(key=lambda x: x["dist_from_route_mi"])
        dealers = dealers[:limit]
    else:
        # Route: distribute evenly along the route to avoid endpoint clustering
        # Divide route into segments, pick best dealers from each
        n_segments = min(limit, 10)
        seg_size = 1.0 / n_segments
        selected = []
        remaining = []
        for seg_i in range(n_segments):
            seg_start = seg_i * seg_size
            seg_end = seg_start + seg_size
            seg_dealers = [d for d in dealers if seg_start <= d["route_position"] < seg_end]
            # Sort by score descending, then distance ascending
            seg_dealers.sort(key=lambda x: (-(x.get("lead_score") or 0), x["dist_from_route_mi"]))
            per_seg = max(1, limit // n_segments)
            selected.extend(seg_dealers[:per_seg])
            remaining.extend(seg_dealers[per_seg:])
        # Fill remaining slots with best leftover dealers
        remaining.sort(key=lambda x: (-(x.get("lead_score") or 0), x["dist_from_route_mi"]))
        selected.extend(remaining[:max(0, limit - len(selected))])
        # Sort final list by route position for display order
        selected.sort(key=lambda x: x["route_position"])
        dealers = selected[:limit]

    # Optimize stop order with Distance Matrix API if requested
    optimized = False
    if optimize and not is_day_trip and len(dealers) >= 2:
        try:
            from app.etl.routing import optimize_stop_order

            # Optimize top stops by score (cap at 10 to control API cost)
            top_dealers = sorted(dealers, key=lambda x: -(x.get("lead_score") or 0))[:10]
            start_point = (p["start_lat"], p["start_lng"])
            end_point = (p["end_lat"], p["end_lng"])

            opt_result = await optimize_stop_order(start_point, end_point, top_dealers)
            if opt_result:
                dealers = opt_result
                optimized = True
        except Exception as e:
            logger.warning(f"Stop optimization failed: {e}")

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
        result["mode"] = "radius"
    if optimized:
        result["optimized"] = True
    return result


# ---------------------------------------------------------------------------
# Geocoding helper
# ---------------------------------------------------------------------------

_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}


async def _geocode_location(text: str) -> tuple[float, float] | None:
    """Parse freeform text ('Hampton Inn, Macon GA' or just 'atlanta') into (lat, lng).

    Extracts city/state if a valid US state abbreviation is found,
    otherwise passes the full text as the city with no state.
    """
    parts = [p.strip().rstrip(",") for p in text.replace(",", " ").split() if p.strip()]
    city_parts = list(parts)
    state = ""
    for i in range(len(parts) - 1, -1, -1):
        if len(parts[i]) == 2 and parts[i].upper() in _US_STATES:
            state = parts[i].upper()
            city_parts = parts[:i]
            break
    city = " ".join(city_parts) if city_parts else text
    return await geocode_single(city, state)


# ---------------------------------------------------------------------------
# CRUD — Create / Update / Delete travel plans
# ---------------------------------------------------------------------------

@router.post("/plans")
async def create_travel_plan(body: TravelPlanCreate):
    """Create a trip day. Geocodes locations, fires auto-briefing email."""
    db = get_service_client()

    # Validate rep exists
    rep = db.table("reps").select("id, name, email").eq("id", body.rep_id).execute()
    if not rep.data:
        raise HTTPException(404, "Rep not found")

    # Round trip: end = start
    if body.is_round_trip:
        body.end_location = body.start_location

    # Geocode start
    start_coords = await _geocode_location(body.start_location)
    if not start_coords:
        raise HTTPException(422, f"Could not geocode start location: {body.start_location}")

    if body.is_round_trip:
        end_coords = start_coords
        route_wkt = None
    else:
        end_coords = await _geocode_location(body.end_location)
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
        "is_round_trip": body.is_round_trip,
    }

    try:
        result = db.table("rep_travel_plans").insert(record).execute()
    except Exception as e:
        err = str(e)
        if "duplicate" in err.lower() or "unique" in err.lower() or "23505" in err:
            raise HTTPException(409, f"Travel plan already exists for {body.travel_date}")
        raise HTTPException(500, "Failed to save travel plan. Check server logs for details.")

    plan = result.data[0] if result.data else record

    # Send auto-briefing inline (reliable in serverless)
    _fire_auto_brief(plan["id"] if "id" in plan else None, plan)

    return {"status": "created", "plan": plan}


@router.put("/plans/{plan_id}")
async def update_travel_plan(plan_id: str, body: TravelPlanUpdate):
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

    if body.is_round_trip is not None:
        updates["is_round_trip"] = body.is_round_trip
        if body.is_round_trip:
            # Round trip: sync end to start
            merged = {**old, **updates}
            updates["end_location"] = merged["start_location"]
            updates["end_lat"] = merged["start_lat"]
            updates["end_lng"] = merged["start_lng"]
            updates["route_polyline"] = None
            locations_changed = False  # skip route fetch below

    # Re-fetch driving route if either location changed (non-round-trip only)
    if locations_changed:
        merged = {**old, **updates}
        is_rt = merged.get("is_round_trip", old.get("is_round_trip", False))
        if is_rt:
            updates["route_polyline"] = None
        else:
            route_wkt = await get_driving_route(
                merged["start_lat"], merged["start_lng"],
                merged["end_lat"], merged["end_lng"],
            )
            updates["route_polyline"] = route_wkt

    if not updates:
        return {"status": "no_changes", "plan": old}

    result = db.table("rep_travel_plans").update(updates).eq("id", plan_id).execute()
    plan = result.data[0] if result.data else {**old, **updates}

    # Re-brief if route changed (inline for serverless reliability)
    if locations_changed:
        _fire_auto_brief(plan_id, plan)

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


@router.get("/route-hot-count/{rep_id}")
def get_route_hot_count(rep_id: str, buffer_miles: float = Query(20)):
    """Count unique hot-tier dealers along all upcoming trip routes for a rep."""
    db = get_service_client()
    today = date.today().isoformat()

    plans = db.table("rep_travel_plans").select(
        "start_lat, start_lng, end_lat, end_lng, route_polyline"
    ).eq("rep_id", rep_id).gte("travel_date", today).execute()

    if not plans.data:
        return {"hot_count": 0}

    # Get latest snapshot for lead scores
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    snap_id = snap.data[0]["id"] if snap.data else None
    if not snap_id:
        return {"hot_count": 0}

    hot_dealer_ids = set()
    for p in plans.data:
        rpc_params = {
            "p_start_lat": p["start_lat"],
            "p_start_lng": p["start_lng"],
            "p_end_lat": p["end_lat"],
            "p_end_lng": p["end_lng"],
            "p_buffer_miles": buffer_miles,
        }
        if p.get("route_polyline"):
            rpc_params["p_polyline_wkt"] = p["route_polyline"]

        try:
            rd = db.rpc("find_dealers_along_route", rpc_params).execute()
            if rd.data:
                hot_dealer_ids.update(r["dealer_id"] for r in rd.data)
        except Exception as e:
            logger.warning(f"Route dealer lookup failed: {e}")

    if not hot_dealer_ids:
        return {"hot_count": 0}

    # Filter to hot-tier only
    scores = db.table("lead_scores").select("dealer_id").eq(
        "snapshot_id", snap_id
    ).eq("tier", "hot").in_("dealer_id", list(hot_dealer_ids)).execute()

    return {"hot_count": len(scores.data) if scores.data else 0}


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


# ===========================================================================
# Named Multi-Day Trips — CRUD + dealer stops + visit tracking
# ===========================================================================

VISIT_DURATION_MIN = 45  # minutes per dealer visit
MAX_DAY_HOURS = 8        # max working hours per day


@router.post("/trips")
async def create_trip(body: TripCreate):
    """Create a named trip with optional days. Geocodes all locations."""
    db = get_service_client()

    # Validate rep
    rep = db.table("reps").select("id, name").eq("id", body.rep_id).execute()
    if not rep.data:
        raise HTTPException(404, "Rep not found")

    trip_row = {
        "name": body.name,
        "rep_id": body.rep_id,
        "created_by": body.created_by,
        "status": "draft" if body.days else "draft",
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "notes": body.notes,
    }
    result = db.table("trips").insert(trip_row).execute()
    if not result.data:
        raise HTTPException(500, "Failed to create trip")
    trip = result.data[0]
    trip_id = trip["id"]

    # Create days if provided
    if body.days:
        await _create_trip_days(db, trip_id, body.days)

    return {"status": "created", "trip": trip}


@router.get("/trips")
def list_trips(
    rep_id: str = Query(None),
    status: str = Query(None),
    limit: int = Query(50, le=100),
):
    """List trips with summary stats (dealer count, visited count)."""
    db = get_service_client()

    query = db.table("trips").select("*").order("start_date", desc=True).limit(limit)
    if rep_id:
        query = query.eq("rep_id", rep_id)
    if status:
        query = query.eq("status", status)
    result = query.execute()
    trips = result.data or []

    if not trips:
        return []

    trip_ids = [t["id"] for t in trips]

    # Get rep names
    rep_ids = list(set(t["rep_id"] for t in trips))
    reps = db.table("reps").select("id, name").in_("id", rep_ids).execute()
    rep_map = {r["id"]: r["name"] for r in (reps.data or [])}

    # Get day + stop counts per trip
    days = db.table("trip_days").select("id, trip_id").in_("trip_id", trip_ids).execute()
    day_map = {}  # trip_id -> [day_ids]
    for d in (days.data or []):
        day_map.setdefault(d["trip_id"], []).append(d["id"])

    all_day_ids = [d["id"] for d in (days.data or [])]
    stops = []
    if all_day_ids:
        stops = db.table("trip_stops").select(
            "trip_day_id, is_included, visited"
        ).in_("trip_day_id", all_day_ids).execute().data or []

    # Aggregate stops per trip
    day_to_trip = {d["id"]: d["trip_id"] for d in (days.data or [])}
    trip_stats = {}  # trip_id -> {dealer_count, visited_count}
    for s in stops:
        tid = day_to_trip.get(s["trip_day_id"])
        if not tid:
            continue
        stats = trip_stats.setdefault(tid, {"dealer_count": 0, "visited_count": 0})
        if s["is_included"]:
            stats["dealer_count"] += 1
            if s["visited"]:
                stats["visited_count"] += 1

    # Enrich trips
    for t in trips:
        t["rep_name"] = rep_map.get(t["rep_id"], "Unknown")
        t["day_count"] = len(day_map.get(t["id"], []))
        stats = trip_stats.get(t["id"], {})
        t["dealer_count"] = stats.get("dealer_count", 0)
        t["visited_count"] = stats.get("visited_count", 0)

    return trips


@router.get("/trips/rep-days")
def get_rep_trip_days(
    rep_id: str = Query(..., description="Rep UUID"),
):
    """Get all trip days for a rep, flattened for the map route overlay.

    Returns trip_day rows enriched with trip name, so the route panel
    can show: 'West GA Tour — Day 1 (Mar 25)'.
    """
    db = get_service_client()

    trips = db.table("trips").select("id, name, status, start_date, end_date").eq(
        "rep_id", rep_id
    ).order("start_date", desc=True).limit(50).execute()

    if not trips.data:
        return []

    trip_ids = [t["id"] for t in trips.data]
    trip_map = {t["id"]: t for t in trips.data}

    days = db.table("trip_days").select("*").in_("trip_id", trip_ids).order("travel_date").execute()
    if not days.data:
        return []

    result = []
    for d in days.data:
        t = trip_map.get(d["trip_id"], {})
        result.append({
            "id": d["id"],
            "trip_id": d["trip_id"],
            "trip_name": t.get("name", ""),
            "trip_status": t.get("status", ""),
            "day_number": d["day_number"],
            "travel_date": d["travel_date"],
            "start_location": d["start_location"],
            "end_location": d["end_location"],
            "start_lat": d["start_lat"],
            "start_lng": d["start_lng"],
            "end_lat": d["end_lat"],
            "end_lng": d["end_lng"],
            "is_round_trip": d.get("is_round_trip", False),
            "route_polyline": d.get("route_polyline"),
        })

    return result


@router.get("/trip-day-dealers/{trip_day_id}")
async def get_trip_day_dealers(
    trip_day_id: str,
    buffer_miles: float = Query(20, description="How far off-route to search (miles)"),
    limit: int = Query(25, le=50),
):
    """Find dealers along a trip day's route. Like route-dealers but reads from trip_days."""
    db = get_service_client()

    day = db.table("trip_days").select("*").eq("id", trip_day_id).execute()
    if not day.data:
        raise HTTPException(404, "Trip day not found")

    p = day.data[0]

    # Auto-backfill polyline if missing
    if not p.get("route_polyline") and not p.get("is_round_trip"):
        try:
            route_wkt = await get_driving_route(
                p["start_lat"], p["start_lng"], p["end_lat"], p["end_lng"]
            )
            if route_wkt:
                db.table("trip_days").update(
                    {"route_polyline": route_wkt}
                ).eq("id", p["id"]).execute()
                p["route_polyline"] = route_wkt
                logger.info(f"Backfilled route polyline for trip_day {p['id']}")
        except Exception as e:
            logger.warning(f"Polyline backfill failed: {e}")

    # Detect round trip / same-location
    dist_km = _haversine(p["start_lat"], p["start_lng"], p["end_lat"], p["end_lng"])
    is_day_trip = dist_km < 0.8

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
            "trip_day_id": trip_day_id,
            "start": p["start_location"],
            "end": p["end_location"],
            "dealers": [],
            "total": 0,
        }

    dealer_ids = [r["dealer_id"] for r in route_dealers.data]
    dist_map = {r["dealer_id"]: r["distance_miles"] for r in route_dealers.data}
    pos_map = {r["dealer_id"]: r.get("route_position", 0) for r in route_dealers.data}

    # Get latest snapshot data
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    snap_id = snap.data[0]["id"] if snap.data else None

    inv_map = {}
    if snap_id:
        inv = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank, top_brand"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        inv_map = {r["dealer_id"]: r for r in inv.data}

    score_map = {}
    if snap_id:
        scores = db.table("lead_scores").select(
            "dealer_id, score, tier"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        score_map = {r["dealer_id"]: r for r in scores.data}

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
        })

    if is_day_trip:
        dealers.sort(key=lambda x: x["dist_from_route_mi"])
        dealers = dealers[:limit]
    else:
        n_segments = min(limit, 10)
        seg_size = 1.0 / n_segments
        selected = []
        remaining = []
        for seg_i in range(n_segments):
            seg_start = seg_i * seg_size
            seg_end = seg_start + seg_size
            seg_dealers = [d for d in dealers if seg_start <= d["route_position"] < seg_end]
            seg_dealers.sort(key=lambda x: (-(x.get("lead_score") or 0), x["dist_from_route_mi"]))
            per_seg = max(1, limit // n_segments)
            selected.extend(seg_dealers[:per_seg])
            remaining.extend(seg_dealers[per_seg:])
        remaining.sort(key=lambda x: (-(x.get("lead_score") or 0), x["dist_from_route_mi"]))
        selected.extend(remaining[:max(0, limit - len(selected))])
        selected.sort(key=lambda x: x["route_position"])
        dealers = selected[:limit]

    return {
        "trip_day_id": trip_day_id,
        "start": p["start_location"],
        "end": p["end_location"],
        "dealers": dealers,
        "total": len(dealers),
    }


@router.get("/trips/estimate-days")
async def estimate_trip_days(
    start: str = Query(...),
    end: str = Query(...),
    buffer_miles: float = Query(20),
    visit_minutes: int = Query(VISIT_DURATION_MIN),
    max_hours: int = Query(MAX_DAY_HOURS),
):
    """Estimate how many days a route needs and suggest day breakpoints."""
    start_coords = await _geocode_location(start)
    if not start_coords:
        raise HTTPException(422, f"Could not geocode: {start}")
    end_coords = await _geocode_location(end)
    if not end_coords:
        raise HTTPException(422, f"Could not geocode: {end}")

    route_wkt = await get_driving_route(
        start_coords[0], start_coords[1], end_coords[0], end_coords[1]
    )

    db = get_service_client()
    rpc_params = {
        "p_start_lat": start_coords[0],
        "p_start_lng": start_coords[1],
        "p_end_lat": end_coords[0],
        "p_end_lng": end_coords[1],
        "p_buffer_miles": buffer_miles,
    }
    if route_wkt:
        rpc_params["p_polyline_wkt"] = route_wkt

    route_dealers = db.rpc("find_dealers_along_route", rpc_params).execute()
    all_dealers = route_dealers.data or []

    if not all_dealers:
        return {
            "total_dealers": 0,
            "suggested_days": 1,
            "day_splits": [{"day": 1, "dealer_count": 0, "start": start, "end": end}],
        }

    all_dealers.sort(key=lambda d: d.get("route_position", 0))

    dealer_ids = [d["dealer_id"] for d in all_dealers]
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    score_map = {}
    inv_map = {}
    aging_map = {}
    snap_id = None
    if snap.data:
        snap_id = snap.data[0]["id"]
        scores = db.table("lead_scores").select("dealer_id, score, tier").eq(
            "snapshot_id", snap_id
        ).in_("dealer_id", dealer_ids).execute()
        score_map = {r["dealer_id"]: r for r in (scores.data or [])}

        inv = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, top_brand"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        inv_map = {r["dealer_id"]: r for r in (inv.data or [])}

    # Compute avg days on lot per dealer from vehicles.first_seen_date
    if dealer_ids:
        try:
            from datetime import date as _date
            vehicles = db.table("vehicles").select(
                "dealer_id, first_seen_date"
            ).in_("dealer_id", dealer_ids).execute()
            today = _date.today()
            dealer_ages = {}
            for v in (vehicles.data or []):
                fsd = v.get("first_seen_date")
                if fsd:
                    try:
                        d_date = _date.fromisoformat(fsd) if isinstance(fsd, str) else fsd
                        age = (today - d_date).days
                        dealer_ages.setdefault(v["dealer_id"], []).append(age)
                    except Exception:
                        pass
            for did, ages in dealer_ages.items():
                aging_map[did] = round(sum(ages) / len(ages), 1) if ages else 0
        except Exception:
            pass

    # Get per-dealer brand and body type inventory
    brand_map = {}   # dealer_id -> [brand_name, ...]
    btype_map = {}   # dealer_id -> [body_type_name, ...]
    sold_brand_map = {}  # dealer_id -> [brand_name, ...]
    sold_btype_map = {}  # dealer_id -> [body_type_name, ...]
    all_brand_names = set()
    all_btype_names = set()

    if dealer_ids and snap_id:
        # Current stock brands
        try:
            brand_inv = db.table("dealer_brand_inventory").select(
                "dealer_id, brand_id"
            ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
            brand_ids_needed = list(set(r["brand_id"] for r in (brand_inv.data or [])))
            brand_lookup = {}
            if brand_ids_needed:
                bl = db.table("brands").select("id, name").in_("id", brand_ids_needed).execute()
                brand_lookup = {r["id"]: r["name"] for r in (bl.data or [])}
            for r in (brand_inv.data or []):
                bname = brand_lookup.get(r["brand_id"], "")
                if bname:
                    brand_map.setdefault(r["dealer_id"], []).append(bname)
                    all_brand_names.add(bname)
        except Exception:
            pass

        # Current stock body types
        try:
            btype_inv = db.table("dealer_body_type_inventory").select(
                "dealer_id, body_type_id"
            ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
            btype_ids_needed = list(set(r["body_type_id"] for r in (btype_inv.data or [])))
            btype_lookup = {}
            if btype_ids_needed:
                btl = db.table("body_types").select("id, name").in_("id", btype_ids_needed).execute()
                btype_lookup = {r["id"]: r["name"] for r in (btl.data or [])}
            for r in (btype_inv.data or []):
                btname = btype_lookup.get(r["body_type_id"], "")
                if btname:
                    btype_map.setdefault(r["dealer_id"], []).append(btname)
                    all_btype_names.add(btname)
        except Exception:
            pass

        # Recently sold (from vehicle_diffs where diff_type='sold')
        try:
            sold = db.table("vehicle_diffs").select(
                "dealer_id, brand, body_type"
            ).eq("snapshot_id", snap_id).eq("diff_type", "sold").in_(
                "dealer_id", dealer_ids
            ).execute()
            for r in (sold.data or []):
                if r.get("brand"):
                    sold_brand_map.setdefault(r["dealer_id"], set()).add(r["brand"])
                    all_brand_names.add(r["brand"])
                if r.get("body_type"):
                    sold_btype_map.setdefault(r["dealer_id"], set()).add(r["body_type"])
                    all_btype_names.add(r["body_type"])
            # Convert sets to lists
            sold_brand_map = {k: list(v) for k, v in sold_brand_map.items()}
            sold_btype_map = {k: list(v) for k, v in sold_btype_map.items()}
        except Exception:
            pass

    for d in all_dealers:
        sc = score_map.get(d["dealer_id"], {})
        inv = inv_map.get(d["dealer_id"], {})
        d["lead_score"] = sc.get("score", 0)
        d["lead_tier"] = sc.get("tier")
        d["total_vehicles"] = inv.get("total_vehicles", 0)
        d["smyrna_units"] = inv.get("smyrna_units", 0)
        d["smyrna_pct"] = inv.get("smyrna_percentage", 0)
        d["top_brand"] = inv.get("top_brand")
        d["avg_days_on_lot"] = aging_map.get(d["dealer_id"], 0)
        d["brands"] = brand_map.get(d["dealer_id"], [])
        d["body_types"] = btype_map.get(d["dealer_id"], [])
        d["sold_brands"] = sold_brand_map.get(d["dealer_id"], [])
        d["sold_body_types"] = sold_btype_map.get(d["dealer_id"], [])

    total_drive_km = _haversine(
        start_coords[0], start_coords[1], end_coords[0], end_coords[1]
    )
    total_drive_hours = (total_drive_km / 1.609) / 55

    max_day_min = max_hours * 60
    visit_min = visit_minutes

    day_splits = []
    current_day = {"day": 1, "dealers": [], "drive_min": 0, "visit_min": 0}
    prev_lat, prev_lng = start_coords

    for dealer in all_dealers:
        dlat, dlng = dealer["latitude"], dealer["longitude"]
        drive_km = _haversine(prev_lat, prev_lng, dlat, dlng)
        drive_min = (drive_km / 1.609) / 55 * 60

        total_min = current_day["drive_min"] + current_day["visit_min"] + drive_min + visit_min

        if total_min > max_day_min and current_day["dealers"]:
            day_splits.append(current_day)
            current_day = {"day": len(day_splits) + 1, "dealers": [], "drive_min": 0, "visit_min": 0}
            if day_splits[-1]["dealers"]:
                last = day_splits[-1]["dealers"][-1]
                prev_lat, prev_lng = last["latitude"], last["longitude"]
                drive_km = _haversine(prev_lat, prev_lng, dlat, dlng)
                drive_min = (drive_km / 1.609) / 55 * 60

        current_day["dealers"].append(dealer)
        current_day["drive_min"] += drive_min
        current_day["visit_min"] += visit_min
        prev_lat, prev_lng = dlat, dlng

    if current_day["dealers"]:
        day_splits.append(current_day)

    suggestions = []
    for split in day_splits:
        dealers_in_day = split["dealers"]
        first_d = dealers_in_day[0] if dealers_in_day else None
        last_d = dealers_in_day[-1] if dealers_in_day else None
        suggestions.append({
            "day": split["day"],
            "dealer_count": len(dealers_in_day),
            "estimated_hours": round((split["drive_min"] + split["visit_min"]) / 60, 1),
            "start_area": f"{first_d['city']}, {first_d['state']}" if first_d else start,
            "end_area": f"{last_d['city']}, {last_d['state']}" if last_d else end,
            "dealers": [{
                "dealer_id": d["dealer_id"],
                "name": d["dealer_name"],
                "city": d["city"],
                "state": d["state"],
                "lat": d["latitude"],
                "lng": d["longitude"],
                "route_position": round(d.get("route_position", 0), 3),
                "lead_score": d.get("lead_score", 0),
                "lead_tier": d.get("lead_tier"),
                "total_vehicles": d.get("total_vehicles", 0),
                "smyrna_units": d.get("smyrna_units", 0),
                "smyrna_pct": d.get("smyrna_pct", 0),
                "top_brand": d.get("top_brand"),
                "avg_days_on_lot": d.get("avg_days_on_lot", 0),
                "brands": d.get("brands", []),
                "body_types": d.get("body_types", []),
                "sold_brands": d.get("sold_brands", []),
                "sold_body_types": d.get("sold_body_types", []),
            } for d in dealers_in_day],
        })

    return {
        "total_dealers": len(all_dealers),
        "total_drive_hours": round(total_drive_hours, 1),
        "suggested_days": len(suggestions),
        "visit_minutes": visit_min,
        "max_day_hours": max_hours,
        "day_splits": suggestions,
        "filter_options": {
            "brands": sorted(all_brand_names),
            "body_types": sorted(all_btype_names),
        },
        "route_polyline": route_wkt,
        "start_lat": start_coords[0],
        "start_lng": start_coords[1],
        "end_lat": end_coords[0],
        "end_lng": end_coords[1],
    }


@router.get("/trips/{trip_id}")
def get_trip_detail(trip_id: str):
    """Full trip detail with days, stops, and dealer intel."""
    db = get_service_client()

    trip = db.table("trips").select("*").eq("id", trip_id).execute()
    if not trip.data:
        raise HTTPException(404, "Trip not found")
    trip = trip.data[0]

    # Rep name
    rep = db.table("reps").select("name").eq("id", trip["rep_id"]).execute()
    trip["rep_name"] = rep.data[0]["name"] if rep.data else "Unknown"

    # Get days
    days = db.table("trip_days").select("*").eq("trip_id", trip_id).order("day_number").execute()
    trip["days"] = days.data or []

    # Get all stops across all days
    day_ids = [d["id"] for d in trip["days"]]
    stops = []
    if day_ids:
        stops = db.table("trip_stops").select("*").in_("trip_day_id", day_ids).order("stop_order").execute().data or []

    # Get dealer info + scores for all stop dealers
    dealer_ids = list(set(s["dealer_id"] for s in stops))
    dealer_map = {}
    inv_map = {}
    score_map = {}

    if dealer_ids:
        dealers = db.table("dealers").select(
            "id, name, city, state, latitude, longitude"
        ).in_("id", dealer_ids).execute()
        dealer_map = {d["id"]: d for d in (dealers.data or [])}

        snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
        snap_id = snap.data[0]["id"] if snap.data else None

        if snap_id:
            inv = db.table("dealer_snapshots").select(
                "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, top_brand"
            ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
            inv_map = {r["dealer_id"]: r for r in (inv.data or [])}

            scores = db.table("lead_scores").select(
                "dealer_id, score, tier"
            ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
            score_map = {r["dealer_id"]: r for r in (scores.data or [])}

    # Compute avg days on lot per dealer
    aging_map = {}
    if dealer_ids:
        try:
            from datetime import date as _date
            vehicles = db.table("vehicles").select(
                "dealer_id, first_seen_date"
            ).in_("dealer_id", dealer_ids).execute()
            today = _date.today()
            dealer_ages = {}
            for v in (vehicles.data or []):
                fsd = v.get("first_seen_date")
                if fsd:
                    try:
                        d_date = _date.fromisoformat(fsd) if isinstance(fsd, str) else fsd
                        age = (today - d_date).days
                        dealer_ages.setdefault(v["dealer_id"], []).append(age)
                    except Exception:
                        pass
            for did, ages in dealer_ages.items():
                aging_map[did] = round(sum(ages) / len(ages), 1) if ages else 0
        except Exception:
            pass

    # Enrich stops and attach to days
    stop_by_day = {}
    for s in stops:
        d = dealer_map.get(s["dealer_id"], {})
        inv = inv_map.get(s["dealer_id"], {})
        sc = score_map.get(s["dealer_id"], {})
        s["dealer_name"] = d.get("name", "Unknown")
        s["city"] = d.get("city", "")
        s["state"] = d.get("state", "")
        s["lat"] = d.get("latitude")
        s["lng"] = d.get("longitude")
        s["total_vehicles"] = inv.get("total_vehicles", 0)
        s["smyrna_units"] = inv.get("smyrna_units", 0)
        s["smyrna_pct"] = inv.get("smyrna_percentage", 0)
        s["top_brand"] = inv.get("top_brand")
        s["lead_score"] = sc.get("score")
        s["lead_tier"] = sc.get("tier")
        s["avg_days_on_lot"] = aging_map.get(s["dealer_id"], 0)
        stop_by_day.setdefault(s["trip_day_id"], []).append(s)

    for day in trip["days"]:
        day["stops"] = stop_by_day.get(day["id"], [])

    return trip


@router.put("/trips/{trip_id}")
def update_trip(trip_id: str, body: TripUpdate):
    """Update trip metadata (name, status, notes, dates)."""
    db = get_service_client()

    existing = db.table("trips").select("id").eq("id", trip_id).execute()
    if not existing.data:
        raise HTTPException(404, "Trip not found")

    updates = {}
    if body.name is not None:
        updates["name"] = body.name
    if body.status is not None:
        updates["status"] = body.status
    if body.notes is not None:
        updates["notes"] = body.notes
    if body.start_date is not None:
        updates["start_date"] = body.start_date.isoformat()
    if body.end_date is not None:
        updates["end_date"] = body.end_date.isoformat()

    if not updates:
        return {"status": "no_changes"}

    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    result = db.table("trips").update(updates).eq("id", trip_id).execute()
    return {"status": "updated", "trip": result.data[0] if result.data else None}


@router.delete("/trips/{trip_id}")
def delete_trip(trip_id: str):
    """Delete a trip and cascade to days + stops."""
    db = get_service_client()
    existing = db.table("trips").select("id").eq("id", trip_id).execute()
    if not existing.data:
        raise HTTPException(404, "Trip not found")
    db.table("trips").delete().eq("id", trip_id).execute()
    return {"status": "deleted", "trip_id": trip_id}


# ---------------------------------------------------------------------------
# Trip days
# ---------------------------------------------------------------------------

@router.post("/trips/{trip_id}/days")
async def add_trip_day(trip_id: str, body: TripDayInput):
    """Add a day to an existing trip."""
    db = get_service_client()

    trip = db.table("trips").select("id").eq("id", trip_id).execute()
    if not trip.data:
        raise HTTPException(404, "Trip not found")

    # Get next day_number
    existing = db.table("trip_days").select("day_number").eq("trip_id", trip_id).order("day_number", desc=True).limit(1).execute()
    next_num = (existing.data[0]["day_number"] + 1) if existing.data else 1

    day = await _geocode_and_build_day(body, trip_id, next_num)
    result = db.table("trip_days").insert(day).execute()
    return {"status": "created", "day": result.data[0] if result.data else day}


@router.delete("/trips/{trip_id}/days/{day_id}")
def delete_trip_day(trip_id: str, day_id: str):
    """Remove a day from a trip (cascades to stops)."""
    db = get_service_client()
    db.table("trip_days").delete().eq("id", day_id).eq("trip_id", trip_id).execute()
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Trip stop suggestions (dealers along route) + bulk set
# ---------------------------------------------------------------------------

@router.get("/trips/{trip_id}/days/{day_id}/suggestions")
async def get_day_suggestions(
    trip_id: str,
    day_id: str,
    buffer_miles: float = Query(20),
    limit: int = Query(30, le=50),
):
    """Get suggested dealers along a trip day's route, enriched with intel."""
    db = get_service_client()

    day = db.table("trip_days").select("*").eq("id", day_id).eq("trip_id", trip_id).execute()
    if not day.data:
        raise HTTPException(404, "Trip day not found")
    d = day.data[0]

    # Auto-backfill polyline
    if not d.get("route_polyline") and not d.get("is_round_trip"):
        try:
            route_wkt = await get_driving_route(d["start_lat"], d["start_lng"], d["end_lat"], d["end_lng"])
            if route_wkt:
                db.table("trip_days").update({"route_polyline": route_wkt}).eq("id", day_id).execute()
                d["route_polyline"] = route_wkt
        except Exception as e:
            logger.warning(f"Polyline backfill failed for trip day {day_id}: {e}")

    # PostGIS corridor search
    rpc_params = {
        "p_start_lat": d["start_lat"],
        "p_start_lng": d["start_lng"],
        "p_end_lat": d["end_lat"],
        "p_end_lng": d["end_lng"],
        "p_buffer_miles": buffer_miles,
    }
    if d.get("route_polyline"):
        rpc_params["p_polyline_wkt"] = d["route_polyline"]

    route_dealers = db.rpc("find_dealers_along_route", rpc_params).execute()
    if not route_dealers.data:
        return {"day_id": day_id, "dealers": [], "total": 0}

    dealer_ids = [r["dealer_id"] for r in route_dealers.data]
    dist_map = {r["dealer_id"]: r["distance_miles"] for r in route_dealers.data}
    pos_map = {r["dealer_id"]: r.get("route_position", 0) for r in route_dealers.data}

    # Enrich with inventory + scores
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    snap_id = snap.data[0]["id"] if snap.data else None
    inv_map = {}
    score_map = {}
    if snap_id:
        inv = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, top_brand"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        inv_map = {r["dealer_id"]: r for r in (inv.data or [])}

        scores = db.table("lead_scores").select(
            "dealer_id, score, tier"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        score_map = {r["dealer_id"]: r for r in (scores.data or [])}

    # Get already-included stops for this day
    existing_stops = db.table("trip_stops").select("dealer_id").eq("trip_day_id", day_id).execute()
    already_set = {s["dealer_id"] for s in (existing_stops.data or [])}

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
            "dist_from_route_mi": round(dist_map.get(did, 0), 1),
            "total_vehicles": inv.get("total_vehicles", 0),
            "smyrna_units": inv.get("smyrna_units", 0),
            "top_brand": inv.get("top_brand"),
            "lead_score": sc.get("score"),
            "lead_tier": sc.get("tier"),
            "already_added": did in already_set,
        })

    # Sort by route position
    dealers.sort(key=lambda x: x["route_position"])
    dealers = dealers[:limit]

    return {"day_id": day_id, "dealers": dealers, "total": len(dealers)}


@router.post("/trips/{trip_id}/days/{day_id}/stops")
def bulk_set_stops(trip_id: str, day_id: str, body: TripStopBulkSet):
    """Bulk-set dealer stops for a trip day."""
    db = get_service_client()

    # Validate day belongs to trip
    day = db.table("trip_days").select("id").eq("id", day_id).eq("trip_id", trip_id).execute()
    if not day.data:
        raise HTTPException(404, "Trip day not found")

    rows = []
    for i, stop in enumerate(body.stops):
        rows.append({
            "trip_day_id": day_id,
            "dealer_id": stop["dealer_id"],
            "stop_order": stop.get("stop_order", i),
            "is_included": True,
        })

    if rows:
        db.table("trip_stops").upsert(rows, on_conflict="trip_day_id,dealer_id").execute()

    return {"status": "set", "count": len(rows)}


# ---------------------------------------------------------------------------
# Individual stop updates (toggle, visit)
# ---------------------------------------------------------------------------

@router.put("/trip-stops/{stop_id}")
def update_trip_stop(stop_id: str, body: TripStopUpdate):
    """Toggle a stop's inclusion or mark as visited."""
    db = get_service_client()

    updates = {}
    if body.is_included is not None:
        updates["is_included"] = body.is_included
    if body.visited is not None:
        updates["visited"] = body.visited
        if body.visited:
            updates["visited_at"] = "now()"
        else:
            updates["visited_at"] = None
    if body.visit_notes is not None:
        updates["visit_notes"] = body.visit_notes

    if not updates:
        return {"status": "no_changes"}

    result = db.table("trip_stops").update(updates).eq("id", stop_id).execute()
    if not result.data:
        raise HTTPException(404, "Stop not found")
    return {"status": "updated", "stop": result.data[0]}


# ---------------------------------------------------------------------------
# Coverage report
# ---------------------------------------------------------------------------

@router.get("/coverage")
def get_coverage(
    rep_id: str = Query(None),
    since: date = Query(None),
):
    """Coverage report: visited vs total dealers by rep/territory."""
    db = get_service_client()

    # Get visited dealer IDs from trip_stops
    query = db.table("trip_stops").select(
        "dealer_id, visited_at, trip_day_id"
    ).eq("visited", True)

    if since:
        query = query.gte("visited_at", since.isoformat())

    visited_stops = query.execute().data or []

    # Filter by rep if needed
    if rep_id and visited_stops:
        day_ids = list(set(s["trip_day_id"] for s in visited_stops))
        days = db.table("trip_days").select("id, trip_id").in_("id", day_ids).execute().data or []
        trip_ids = list(set(d["trip_id"] for d in days))
        if trip_ids:
            trips = db.table("trips").select("id").in_("id", trip_ids).eq("rep_id", rep_id).execute().data or []
            valid_trip_ids = {t["id"] for t in trips}
            valid_day_ids = {d["id"] for d in days if d["trip_id"] in valid_trip_ids}
            visited_stops = [s for s in visited_stops if s["trip_day_id"] in valid_day_ids]

    visited_ids = list(set(s["dealer_id"] for s in visited_stops))

    # Total dealers in rep territory
    total_query = db.table("dealers").select("id", count="exact")
    if rep_id:
        rep = db.table("reps").select("territory_states").eq("id", rep_id).execute()
        if rep.data and rep.data[0].get("territory_states"):
            total_query = total_query.in_("state", rep.data[0]["territory_states"])
    total_result = total_query.execute()
    total = total_result.count or 0

    # Hot unvisited
    hot_unvisited = 0
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    if snap.data:
        hot_q = db.table("lead_scores").select("dealer_id").eq("snapshot_id", snap.data[0]["id"]).eq("tier", "hot")
        if visited_ids:
            # Can't do NOT IN easily with supabase-py, so get all hot and subtract
            hot_all = hot_q.execute().data or []
            hot_unvisited = len([h for h in hot_all if h["dealer_id"] not in set(visited_ids)])
        else:
            hot_unvisited = len(hot_q.execute().data or [])

    return {
        "total_dealers": total,
        "visited_dealers": len(visited_ids),
        "coverage_pct": round(len(visited_ids) / total * 100, 1) if total > 0 else 0,
        "hot_unvisited": hot_unvisited,
    }


# (estimate-days moved above /trips/{trip_id} to avoid route conflict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _create_trip_days(db, trip_id: str, days: list[TripDayInput]):
    """Geocode and insert multiple trip days, optionally with pre-selected dealer stops."""
    for i, day_input in enumerate(days):
        day_row = await _geocode_and_build_day(day_input, trip_id, i + 1)
        try:
            result = db.table("trip_days").insert(day_row).execute()
            if result.data and day_input.dealer_ids:
                day_id = result.data[0]["id"]
                stop_rows = [
                    {
                        "trip_day_id": day_id,
                        "dealer_id": did,
                        "stop_order": order,
                        "is_included": True,
                    }
                    for order, did in enumerate(day_input.dealer_ids, 1)
                ]
                if stop_rows:
                    db.table("trip_stops").upsert(
                        stop_rows, on_conflict="trip_day_id,dealer_id"
                    ).execute()
        except Exception as e:
            logger.warning(f"Failed to insert trip day {i+1}: {e}")


async def _geocode_and_build_day(day: TripDayInput, trip_id: str, day_number: int) -> dict:
    """Geocode locations and build a trip_days row dict."""
    start_coords = await _geocode_location(day.start_location)
    if not start_coords:
        raise HTTPException(422, f"Could not geocode: {day.start_location}")

    if day.is_round_trip or not day.end_location:
        end_coords = start_coords
        route_wkt = None
    else:
        end_coords = await _geocode_location(day.end_location)
        if not end_coords:
            raise HTTPException(422, f"Could not geocode: {day.end_location}")
        route_wkt = await get_driving_route(
            start_coords[0], start_coords[1], end_coords[0], end_coords[1]
        )

    return {
        "trip_id": trip_id,
        "day_number": day_number,
        "travel_date": day.travel_date.isoformat(),
        "start_location": day.start_location,
        "start_lat": start_coords[0],
        "start_lng": start_coords[1],
        "end_location": day.end_location or day.start_location,
        "end_lat": end_coords[0],
        "end_lng": end_coords[1],
        "is_round_trip": day.is_round_trip,
        "route_polyline": route_wkt,
        "notes": day.notes,
    }
