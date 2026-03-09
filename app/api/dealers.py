"""Dealer query endpoints — used by both the API and the agent tools."""

import logging

from fastapi import APIRouter, Query, HTTPException

from app.database import get_service_client
from app.models import DealerSummary, DealerBriefing, NearbyQuery, SnapshotInfo

router = APIRouter(prefix="/api/dealers", tags=["dealers"])
logger = logging.getLogger(__name__)


def _latest_snapshot_id(db) -> str:
    """Get the most recent snapshot ID."""
    result = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    if not result.data:
        raise HTTPException(404, "No report snapshots found. Upload a report first.")
    return result.data[0]["id"]


@router.get("/snapshots", response_model=list[SnapshotInfo])
def list_snapshots():
    """List all ingested report snapshots."""
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
        # Apply name filter in Python (Supabase text search on joined tables is limited)
        if q and q.lower() not in d["name"].lower():
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


@router.get("/territory/{state}")
def get_territory_summary(state: str):
    """Get a state-level territory summary."""
    db = get_service_client()
    snap_id = _latest_snapshot_id(db)

    # Get all dealers in state with their snapshot data
    result = db.table("dealer_snapshots").select(
        "total_vehicles, smyrna_units, smyrna_percentage, rank, dealers!inner(id, name, city, state)"
    ).eq("snapshot_id", snap_id).eq("dealers.state", state.upper()).order(
        "total_vehicles", desc=True
    ).execute()

    if not result.data:
        raise HTTPException(404, f"No dealers found in {state.upper()}")

    total_dealers = len(result.data)
    total_vehicles = sum(r["total_vehicles"] or 0 for r in result.data)
    total_smyrna = sum(r["smyrna_units"] or 0 for r in result.data)
    dealers_with_smyrna = sum(1 for r in result.data if (r["smyrna_units"] or 0) > 0)

    top_dealers = [
        {
            "name": r["dealers"]["name"],
            "city": r["dealers"]["city"],
            "total_vehicles": r["total_vehicles"],
            "smyrna_units": r["smyrna_units"] or 0,
            "rank": r["rank"],
        }
        for r in result.data[:10]
    ]

    return {
        "state": state.upper(),
        "total_dealers": total_dealers,
        "total_vehicles": total_vehicles,
        "total_smyrna_units": total_smyrna,
        "dealers_with_smyrna": dealers_with_smyrna,
        "smyrna_penetration_pct": round(dealers_with_smyrna / total_dealers * 100, 1) if total_dealers else 0,
        "top_10_dealers": top_dealers,
    }
