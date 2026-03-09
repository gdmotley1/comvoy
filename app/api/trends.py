"""Trend endpoints — multi-month comparison and analysis."""

import logging

from fastapi import APIRouter, Query, HTTPException

from app.database import get_service_client
from app.models import (
    SnapshotPoint, DealerTrend,
    TerritoryPoint, TerritoryTrend,
)

router = APIRouter(prefix="/api/trends", tags=["trends"])
logger = logging.getLogger(__name__)


def _get_snapshots(db, months: int | None = None) -> list[dict]:
    """Get snapshot IDs ordered by date (newest first), optionally limited."""
    query = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True)
    if months:
        query = query.limit(months)
    result = query.execute()
    return result.data


def _direction(values: list[int | float]) -> str:
    """Determine trend direction from a series of values."""
    if len(values) < 2:
        return "flat"
    first, last = values[0], values[-1]
    if last > first:
        return "up"
    elif last < first:
        return "down"
    return "flat"


@router.get("/dealer/{dealer_id}", response_model=DealerTrend)
def get_dealer_trend(
    dealer_id: str,
    months: int = Query(None, description="Number of recent months (default: all)"),
):
    """Get multi-month trend for a specific dealer."""
    db = get_service_client()

    # Get dealer info
    dealer = db.table("dealers").select("id, name, city, state").eq("id", dealer_id).single().execute()
    if not dealer.data:
        raise HTTPException(404, "Dealer not found")

    snapshots = _get_snapshots(db, months)
    if not snapshots:
        raise HTTPException(404, "No snapshots found")

    snap_ids = [s["id"] for s in snapshots]
    snap_dates = {s["id"]: s["report_date"] for s in snapshots}

    # Fetch all dealer_snapshots for this dealer across all relevant snapshots
    result = db.table("dealer_snapshots").select(
        "snapshot_id, total_vehicles, smyrna_units, smyrna_percentage, rank, top_brand"
    ).eq("dealer_id", dealer_id).in_("snapshot_id", snap_ids).execute()

    # Index by snapshot_id
    by_snap = {row["snapshot_id"]: row for row in result.data}

    # Build points in chronological order (oldest first)
    points: list[SnapshotPoint] = []
    prev = None
    for snap in reversed(snapshots):  # reverse to go oldest-first
        row = by_snap.get(snap["id"])
        if not row:
            continue
        point = SnapshotPoint(
            report_date=snap_dates[snap["id"]],
            snapshot_id=snap["id"],
            total_vehicles=row["total_vehicles"] or 0,
            smyrna_units=row["smyrna_units"] or 0,
            smyrna_percentage=float(row["smyrna_percentage"] or 0),
            rank=row["rank"],
            top_brand=row["top_brand"],
        )
        if prev:
            point.vehicle_delta = point.total_vehicles - prev.total_vehicles
            point.smyrna_delta = point.smyrna_units - prev.smyrna_units
            if point.rank is not None and prev.rank is not None:
                point.rank_delta = prev.rank - point.rank  # positive = improved
        prev = point
        points.append(point)

    if not points:
        raise HTTPException(404, f"No snapshot data found for dealer {dealer_id}")

    vehicles = [p.total_vehicles for p in points]
    smyrna = [p.smyrna_units for p in points]
    ranks = [p.rank for p in points if p.rank is not None]

    # Rank trend: improving means rank number is getting smaller
    rank_trend = "stable"
    if len(ranks) >= 2:
        if ranks[-1] < ranks[0]:
            rank_trend = "improving"
        elif ranks[-1] > ranks[0]:
            rank_trend = "declining"

    smyrna_trend = _direction(smyrna)
    if all(s == 0 for s in smyrna):
        smyrna_trend = "none"

    d = dealer.data
    return DealerTrend(
        dealer_id=d["id"],
        dealer_name=d["name"],
        city=d["city"],
        state=d["state"],
        points=points,
        vehicle_trend=_direction(vehicles),
        smyrna_trend=smyrna_trend,
        rank_trend=rank_trend,
    )


@router.get("/territory/{state}", response_model=TerritoryTrend)
def get_territory_trend(
    state: str,
    months: int = Query(None, description="Number of recent months (default: all)"),
):
    """Get multi-month trend for a state territory."""
    db = get_service_client()
    state = state.upper()

    snapshots = _get_snapshots(db, months)
    if not snapshots:
        raise HTTPException(404, "No snapshots found")

    snap_ids = [s["id"] for s in snapshots]
    snap_dates = {s["id"]: s["report_date"] for s in snapshots}

    # Fetch all dealer_snapshots for this state across all snapshots
    result = db.table("dealer_snapshots").select(
        "snapshot_id, total_vehicles, smyrna_units, dealers!inner(state)"
    ).in_("snapshot_id", snap_ids).eq("dealers.state", state).execute()

    if not result.data:
        raise HTTPException(404, f"No dealers found in {state}")

    # Group by snapshot
    by_snap: dict[str, list] = {}
    for row in result.data:
        sid = row["snapshot_id"]
        by_snap.setdefault(sid, []).append(row)

    # Build points chronologically
    points: list[TerritoryPoint] = []
    for snap in reversed(snapshots):
        rows = by_snap.get(snap["id"], [])
        if not rows:
            continue
        total_dealers = len(rows)
        total_vehicles = sum(r["total_vehicles"] or 0 for r in rows)
        total_smyrna = sum(r["smyrna_units"] or 0 for r in rows)
        dealers_with_smyrna = sum(1 for r in rows if (r["smyrna_units"] or 0) > 0)
        points.append(TerritoryPoint(
            report_date=snap_dates[snap["id"]],
            snapshot_id=snap["id"],
            total_dealers=total_dealers,
            total_vehicles=total_vehicles,
            total_smyrna=total_smyrna,
            dealers_with_smyrna=dealers_with_smyrna,
            smyrna_penetration_pct=round(dealers_with_smyrna / total_dealers * 100, 1) if total_dealers else 0,
        ))

    # Compute deltas between first and last
    dealer_count_delta = None
    vehicle_delta = None
    smyrna_delta = None
    if len(points) >= 2:
        dealer_count_delta = points[-1].total_dealers - points[0].total_dealers
        vehicle_delta = points[-1].total_vehicles - points[0].total_vehicles
        smyrna_delta = points[-1].total_smyrna - points[0].total_smyrna

    return TerritoryTrend(
        state=state,
        points=points,
        dealer_count_delta=dealer_count_delta,
        vehicle_delta=vehicle_delta,
        smyrna_delta=smyrna_delta,
    )


@router.get("/snapshots/compare")
def compare_snapshots(
    snapshot_a: str = Query(..., description="Earlier snapshot UUID"),
    snapshot_b: str = Query(..., description="Later snapshot UUID"),
    state: str = Query(None, description="Optional state filter"),
    limit: int = Query(20, le=100),
):
    """Side-by-side comparison of two snapshots."""
    db = get_service_client()

    # Validate snapshots exist
    for sid in [snapshot_a, snapshot_b]:
        check = db.table("report_snapshots").select("id, report_date").eq("id", sid).execute()
        if not check.data:
            raise HTTPException(404, f"Snapshot {sid} not found")

    # Fetch dealer_snapshots for both
    def _fetch_snap(snap_id):
        q = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, rank, dealers!inner(name, city, state)"
        ).eq("snapshot_id", snap_id)
        if state:
            q = q.eq("dealers.state", state.upper())
        return {row["dealer_id"]: row for row in q.execute().data}

    data_a = _fetch_snap(snapshot_a)
    data_b = _fetch_snap(snapshot_b)

    all_ids = set(data_a.keys()) | set(data_b.keys())

    new_dealers = []
    lost_dealers = []
    movers = []  # rank changed by 5+

    for did in all_ids:
        a = data_a.get(did)
        b = data_b.get(did)

        if b and not a:
            d = b["dealers"]
            new_dealers.append({"dealer_id": did, "name": d["name"], "city": d["city"],
                                "state": d["state"], "total_vehicles": b["total_vehicles"]})
        elif a and not b:
            d = a["dealers"]
            lost_dealers.append({"dealer_id": did, "name": d["name"], "city": d["city"],
                                 "state": d["state"], "total_vehicles": a["total_vehicles"]})
        elif a and b:
            rank_a = a["rank"] or 999
            rank_b = b["rank"] or 999
            rank_delta = rank_a - rank_b  # positive = improved
            if abs(rank_delta) >= 5:
                d = b["dealers"]
                movers.append({
                    "dealer_id": did, "name": d["name"], "city": d["city"], "state": d["state"],
                    "rank_before": a["rank"], "rank_after": b["rank"], "rank_delta": rank_delta,
                    "vehicles_before": a["total_vehicles"], "vehicles_after": b["total_vehicles"],
                })

    movers.sort(key=lambda x: abs(x["rank_delta"]), reverse=True)

    return {
        "snapshot_a": snapshot_a,
        "snapshot_b": snapshot_b,
        "new_dealers": new_dealers[:limit],
        "lost_dealers": lost_dealers[:limit],
        "top_movers": movers[:limit],
        "summary": {
            "new_count": len(new_dealers),
            "lost_count": len(lost_dealers),
            "mover_count": len(movers),
        },
    }
