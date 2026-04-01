"""Lead scoring engine — ranks dealers by lot size.

Runs automatically after each data upload. Tiers every dealer by
total vehicle inventory on the lot:
  - hot (50+):   Major dealers — high-volume targets
  - warm (20-49): Mid-size dealers — worth active pursuit
  - cold (<20):   Small dealers — monitor only
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException

from app.database import get_service_client
from app.config import is_excluded_dealer

router = APIRouter(prefix="/api/scoring", tags=["scoring"])
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# TIER THRESHOLDS — vehicle count on the lot
# ═══════════════════════════════════════════════════════════════════════
TIER_HOT = 50              # 50+ vehicles = hot
TIER_WARM = 20             # 20-49 vehicles = warm; <20 = cold

# Body types that Smyrna/Fouts Bros manufactures (used by briefing/agent)
SMYRNA_BODY_TYPES = {
    "Service Trucks", "Flatbed Trucks", "Box Trucks", "Box Vans",
    "Stake Beds", "Mechanic Body", "Enclosed Service", "Dump Trucks",
    "Landscape Dumps", "Flatbed Dump", "Combo Body",
}


def compute_lead_scores(snapshot_id: str, prev_snapshot_id: str | None = None) -> dict:
    """Tier all dealers for a given snapshot by vehicle count. Returns summary stats."""
    db = get_service_client()

    snap_data = db.table("dealer_snapshots").select(
        "dealer_id, total_vehicles, smyrna_units"
    ).eq("snapshot_id", snapshot_id).execute()

    if not snap_data.data:
        return {"error": "No dealer data for this snapshot"}

    scores = []
    for dealer in snap_data.data:
        did = dealer["dealer_id"]
        vehicles = dealer["total_vehicles"] or 0
        smyrna = dealer["smyrna_units"] or 0

        tier = "hot" if vehicles >= TIER_HOT else "warm" if vehicles >= TIER_WARM else "cold"

        scores.append({
            "dealer_id": did,
            "snapshot_id": snapshot_id,
            "score": vehicles,
            "tier": tier,
            "opportunity_type": "whitespace" if smyrna == 0 else "upsell",
            "factors": {"vehicles": vehicles},
        })

    # Delete old scores then batch insert
    db.table("lead_scores").delete().eq("snapshot_id", snapshot_id).execute()
    batch_size = 500
    for i in range(0, len(scores), batch_size):
        db.table("lead_scores").insert(scores[i:i + batch_size]).execute()

    hot = sum(1 for s in scores if s["tier"] == "hot")
    warm = sum(1 for s in scores if s["tier"] == "warm")
    cold = sum(1 for s in scores if s["tier"] == "cold")
    summary = {
        "total_dealers": len(scores),
        "hot": hot, "warm": warm, "cold": cold,
        "max_vehicles": max(s["score"] for s in scores) if scores else 0,
    }
    logger.info(f"Tiered {len(scores)} dealers: {hot} hot, {warm} warm, {cold} cold")
    return summary


@router.get("/leads")
def get_lead_scores(
    state: str = Query(None, description="Filter by state"),
    tier: str = Query(None, description="Filter by tier: hot, warm, cold"),
    limit: int = Query(25, le=100),
):
    """Get dealers ranked by vehicle count."""
    db = get_service_client()

    # Get latest snapshot
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    if not snap.data:
        raise HTTPException(404, "No snapshots found")
    snap_id = snap.data[0]["id"]

    query = db.table("lead_scores").select(
        "score, tier, factors, "
        "dealers!inner(id, name, city, state, latitude, longitude)"
    ).eq("snapshot_id", snap_id).order("score", desc=True)

    if state:
        query = query.eq("dealers.state", state.upper())
    if tier:
        query = query.eq("tier", tier)

    query = query.limit(limit)
    result = query.execute()

    leads = []
    for row in result.data:
        d = row["dealers"]
        # Skip excluded dealers (Penske, MHC, etc.)
        if is_excluded_dealer(d["name"]):
            continue
        leads.append({
            "dealer_id": d["id"],
            "name": d["name"],
            "city": d["city"],
            "state": d["state"],
            "lat": d["latitude"],
            "lng": d["longitude"],
            "score": row["score"],
            "tier": row["tier"],
            "factors": row["factors"],
        })

    return {"leads": leads, "total": len(leads), "snapshot_id": snap_id}
