"""Lead scoring engine — ranks dealers by opportunity value.

Runs automatically after each monthly data upload. Scores every dealer
on a 0-100 scale based on:
  - Inventory size (bigger = more opportunity)
  - Body type match (do they stock what Smyrna builds?)
  - Smyrna status (whitespace vs upsell vs at-risk)
  - Growth momentum (inventory trending up = active buyer)

Tiers:
  - hot (70-100):  Large whitespace or growing Smyrna accounts
  - warm (40-69):  Medium whitespace or stable Smyrna
  - cold (0-39):   Small dealers or shrinking accounts
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException

from app.database import get_service_client

router = APIRouter(prefix="/api/scoring", tags=["scoring"])
logger = logging.getLogger(__name__)

# Body types that Smyrna/Fouts Bros manufactures
SMYRNA_BODY_TYPES = {
    "Service Trucks", "Flatbed Trucks", "Box Trucks", "Box Vans",
    "Stake Beds", "Mechanic Body", "Enclosed Service", "Dump Trucks",
    "Landscape Dumps", "Flatbed Dump", "Combo Body",
}


def compute_lead_scores(snapshot_id: str, prev_snapshot_id: str | None = None) -> dict:
    """Score all dealers for a given snapshot. Returns summary stats.

    Args:
        snapshot_id: The snapshot to score
        prev_snapshot_id: Previous snapshot for trend scoring (optional)
    """
    db = get_service_client()

    # Fetch all dealer snapshots for this month
    snap_data = db.table("dealer_snapshots").select(
        "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank"
    ).eq("snapshot_id", snapshot_id).execute()

    if not snap_data.data:
        return {"error": "No dealer data for this snapshot"}

    # Fetch body type inventory for body-type-match scoring
    body_types = db.table("dealer_body_type_inventory").select(
        "dealer_id, vehicle_count, body_types(name)"
    ).eq("snapshot_id", snapshot_id).execute()

    # Build body type map: dealer_id → {body_type: count}
    bt_map: dict[str, dict[str, int]] = {}
    for row in body_types.data:
        did = row["dealer_id"]
        bt_name = row["body_types"]["name"]
        bt_map.setdefault(did, {})[bt_name] = row["vehicle_count"]

    # Fetch previous snapshot data for trend scoring
    prev_map: dict[str, dict] = {}
    if prev_snapshot_id:
        prev_data = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units"
        ).eq("snapshot_id", prev_snapshot_id).execute()
        prev_map = {r["dealer_id"]: r for r in prev_data.data}

    # Score each dealer
    scores = []
    for dealer in snap_data.data:
        did = dealer["dealer_id"]
        vehicles = dealer["total_vehicles"] or 0
        smyrna = dealer["smyrna_units"] or 0
        dealer_bts = bt_map.get(did, {})
        prev = prev_map.get(did)

        score, factors, opp_type = _score_dealer(
            vehicles, smyrna, dealer_bts, prev
        )

        tier = "hot" if score >= 70 else "warm" if score >= 40 else "cold"

        scores.append({
            "dealer_id": did,
            "snapshot_id": snapshot_id,
            "score": score,
            "tier": tier,
            "opportunity_type": opp_type,
            "factors": factors,
        })

    # Upsert scores into DB
    # Delete old scores for this snapshot first
    db.table("lead_scores").delete().eq("snapshot_id", snapshot_id).execute()

    # Batch insert (Supabase handles up to 1000 rows)
    batch_size = 500
    for i in range(0, len(scores), batch_size):
        batch = scores[i:i + batch_size]
        db.table("lead_scores").insert(batch).execute()

    # Summary
    hot = sum(1 for s in scores if s["tier"] == "hot")
    warm = sum(1 for s in scores if s["tier"] == "warm")
    cold = sum(1 for s in scores if s["tier"] == "cold")
    whitespace = sum(1 for s in scores if s["opportunity_type"] == "whitespace")

    summary = {
        "total_scored": len(scores),
        "hot": hot, "warm": warm, "cold": cold,
        "whitespace": whitespace,
        "top_score": max(s["score"] for s in scores) if scores else 0,
    }
    logger.info(f"Scored {len(scores)} dealers: {hot} hot, {warm} warm, {cold} cold")
    return summary


def _score_dealer(
    vehicles: int,
    smyrna: int,
    body_types: dict[str, int],
    prev: dict | None,
) -> tuple[int, dict, str]:
    """Score a single dealer. Returns (score, factors_dict, opportunity_type)."""

    factors = {}

    # --- Guard: 0-vehicle dealers are not real leads ---
    if vehicles == 0:
        return 0, {"note": "zero_inventory"}, "whitespace"

    # --- 1. Inventory size (0-30 points) ---
    # Log scale: 10 vehicles = 10pts, 50 = 20pts, 150+ = 30pts
    if vehicles >= 150:
        size_pts = 30
    elif vehicles >= 50:
        size_pts = 15 + int((vehicles - 50) / 100 * 15)
    elif vehicles >= 10:
        size_pts = int(vehicles / 50 * 15)
    else:
        size_pts = max(0, vehicles)
    factors["inventory_size"] = size_pts

    # --- 2. Body type match (0-30 points) ---
    # What % of their inventory is in body types Smyrna builds?
    total_bt_vehicles = sum(body_types.values()) or 1
    smyrna_match_vehicles = sum(
        count for bt, count in body_types.items() if bt in SMYRNA_BODY_TYPES
    )
    match_pct = smyrna_match_vehicles / total_bt_vehicles
    bt_pts = int(match_pct * 30)
    factors["body_type_match"] = bt_pts
    factors["match_pct"] = round(match_pct * 100)

    # --- 3. Smyrna status (0-25 points) ---
    if smyrna == 0:
        # Whitespace — full opportunity
        opp_type = "whitespace"
        smyrna_pts = 25
    elif smyrna > 0 and (smyrna / max(vehicles, 1)) < 0.05:
        # Has some Smyrna but low penetration — upsell
        opp_type = "upsell"
        smyrna_pts = 15
    else:
        # Decent Smyrna penetration — monitor/grow
        opp_type = "upsell"
        smyrna_pts = 8
    factors["smyrna_opportunity"] = smyrna_pts

    # --- 4. Growth momentum (0-15 points) ---
    growth_pts = 0
    if prev:
        prev_vehicles = prev.get("total_vehicles", 0) or 0
        if prev_vehicles > 0 and vehicles > 0:
            growth = (vehicles - prev_vehicles) / prev_vehicles
            if growth >= 0.20:
                growth_pts = 15  # 20%+ growth
            elif growth >= 0.10:
                growth_pts = 10
            elif growth >= 0.0:
                growth_pts = 5   # stable or slight growth
            else:
                growth_pts = 0   # shrinking
            factors["growth_pct"] = round(growth * 100)
        elif prev_vehicles == 0 and vehicles > 0:
            # New dealer appeared with inventory — positive signal
            growth_pts = 10
            factors["growth_pct"] = "new"
    factors["growth_momentum"] = growth_pts

    # Check for at-risk: had Smyrna last month, lost it this month
    if prev and (prev.get("smyrna_units") or 0) > 0 and smyrna == 0:
        opp_type = "at_risk"
        # Boost score so it surfaces
        factors["at_risk_bonus"] = 20

    total = min(100, size_pts + bt_pts + smyrna_pts + growth_pts + factors.get("at_risk_bonus", 0))
    return total, factors, opp_type


@router.get("/leads")
def get_lead_scores(
    state: str = Query(None, description="Filter by state"),
    tier: str = Query(None, description="Filter by tier: hot, warm, cold"),
    opportunity_type: str = Query(None, description="Filter: whitespace, upsell, at_risk"),
    limit: int = Query(25, le=100),
):
    """Get scored leads ranked by opportunity value."""
    db = get_service_client()

    # Get latest snapshot
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    if not snap.data:
        raise HTTPException(404, "No snapshots found")
    snap_id = snap.data[0]["id"]

    query = db.table("lead_scores").select(
        "score, tier, opportunity_type, factors, "
        "dealers!inner(id, name, city, state, latitude, longitude)"
    ).eq("snapshot_id", snap_id).order("score", desc=True)

    if state:
        query = query.eq("dealers.state", state.upper())
    if tier:
        query = query.eq("tier", tier)
    if opportunity_type:
        query = query.eq("opportunity_type", opportunity_type)

    query = query.limit(limit)
    result = query.execute()

    leads = []
    for row in result.data:
        d = row["dealers"]
        leads.append({
            "dealer_id": d["id"],
            "name": d["name"],
            "city": d["city"],
            "state": d["state"],
            "lat": d["latitude"],
            "lng": d["longitude"],
            "score": row["score"],
            "tier": row["tier"],
            "type": row["opportunity_type"],
            "factors": row["factors"],
        })

    return {"leads": leads, "total": len(leads), "snapshot_id": snap_id}
