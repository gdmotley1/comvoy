"""Lead scoring engine — ranks dealers by opportunity value.

Runs automatically after each monthly data upload. Scores every dealer
on a 0-100 scale based on:
  - Fleet scale (0-20) — bigger fleet = bigger order potential
  - Product fit (0-25) — % of inventory in body types Smyrna builds
  - Smyrna penetration (0-30) — proven buyers > speculation
  - Growth signal (0-25) — growing inventory = active buyer

Opportunity types (what the rep should DO):
  - conquest:   Has Smyrna <5% — proven buyer, maximum runway
  - expand:     Smyrna 5-15% — growing relationship, push deeper
  - grow:       Smyrna 15-30% — solid presence, nurture
  - defend:     Smyrna 30%+ — strong presence, protect
  - whitespace: Zero Smyrna — unproven prospect, qualify
  - at_risk:    Had Smyrna last month, lost it — emergency retention

Tiers:
  - hot (70-100):  High-value targets — conquest accounts, at-risk retention
  - warm (40-69):  Active opportunities — growing accounts, qualified whitespace
  - cold (0-39):   Low priority — small dealers, poor fit, or saturated
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException

from app.database import get_service_client

router = APIRouter(prefix="/api/scoring", tags=["scoring"])
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# SCORING CONFIGURATION — CEO-adjustable weights
# Change any value here and re-run scoring to update all dealer ranks.
# ═══════════════════════════════════════════════════════════════════════

# Factor maximums (must sum to 100)
MAX_FLEET_SCALE = 20       # How much raw dealer size matters
MAX_PRODUCT_FIT = 25       # Body type alignment with Smyrna catalog
MAX_SMYRNA_PEN = 30        # Penetration-based opportunity (the key factor)
MAX_GROWTH_SIGNAL = 25     # Inventory growth = active buying signal

# Smyrna penetration points by opportunity type
PEN_WHITESPACE = 18        # Zero Smyrna — unproven prospect
PEN_CONQUEST = 28          # <5% penetration — proven buyer, max runway
PEN_EXPAND = 22            # 5-15% — growing relationship
PEN_GROW = 15              # 15-30% — solid presence, nurture
PEN_DEFEND = 10            # 30%+ — strong presence, protect

# Penetration thresholds (as decimals)
THRESH_CONQUEST = 0.05     # Below this = conquest
THRESH_EXPAND = 0.15       # Below this = expand
THRESH_GROW = 0.30         # Below this = grow; above = defend

# Growth signal points
GROWTH_EXPLOSIVE = 25      # 20%+ growth
GROWTH_STRONG = 18         # 10-19% growth
GROWTH_SOLID = 12          # 5-9% growth
GROWTH_SLIGHT = 6          # 1-4% growth
GROWTH_FLAT = 3            # 0% (flat)
GROWTH_DECLINING = 0       # Negative growth
GROWTH_NEW_DEALER = 15     # New dealer appeared with inventory

# At-risk bonus (on top of whitespace points + tier override to hot)
AT_RISK_BONUS = 12

# Tier boundaries
TIER_HOT = 70              # Score >= this = hot
TIER_WARM = 40             # Score >= this = warm; below = cold

# ═══════════════════════════════════════════════════════════════════════

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

        tier = "hot" if score >= TIER_HOT else "warm" if score >= TIER_WARM else "cold"
        # At-risk dealers always surface as hot — losing a customer is urgent
        if opp_type == "at_risk" and tier != "hot":
            tier = "hot"

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
    """Score a single dealer. Returns (score, factors_dict, opportunity_type).

    Scoring philosophy: proven buyers > speculation. A dealer already buying
    Smyrna with room to grow is a higher-probability sale than a dealer
    who's never bought. Growth signals matter more than raw size.

    Opportunity types tell the rep WHAT to do:
      conquest  — proven buyer <5% pen, go win the account
      expand    — 5-15% pen, push deeper
      grow      — 15-30% pen, nurture
      defend    — 30%+ pen, protect the business
      whitespace — zero Smyrna, qualify and pitch
      at_risk   — lost Smyrna since last month, emergency retention
    """

    factors = {}

    # --- Guard: 0-vehicle dealers are not real leads ---
    if vehicles == 0:
        return 0, {"note": "zero_inventory"}, "whitespace"

    # --- 1. Fleet Scale (0-MAX_FLEET_SCALE points) ---
    # Size is context, not strategy. Reduced weight vs old model.
    if vehicles >= 200:
        scale_pts = MAX_FLEET_SCALE
    elif vehicles >= 100:
        scale_pts = int(MAX_FLEET_SCALE * 0.80)
    elif vehicles >= 50:
        scale_pts = int(MAX_FLEET_SCALE * 0.60)
    elif vehicles >= 25:
        scale_pts = int(MAX_FLEET_SCALE * 0.40)
    elif vehicles >= 10:
        scale_pts = int(MAX_FLEET_SCALE * 0.25)
    else:
        scale_pts = min(vehicles, 3)
    factors["fleet_scale"] = scale_pts

    # --- 2. Product Fit (0-MAX_PRODUCT_FIT points) ---
    # What % of their inventory is body types Smyrna builds?
    total_bt_vehicles = sum(body_types.values()) or 1
    smyrna_match_vehicles = sum(
        count for bt, count in body_types.items() if bt in SMYRNA_BODY_TYPES
    )
    match_pct = smyrna_match_vehicles / total_bt_vehicles
    fit_pts = int(match_pct * MAX_PRODUCT_FIT)
    factors["product_fit"] = fit_pts
    factors["match_pct"] = round(match_pct * 100)

    # --- 3. Smyrna Penetration (0-MAX_SMYRNA_PEN points) — THE key factor ---
    # Proven buyers score higher than speculation. Tells the rep what to DO.
    pen_pct = smyrna / max(vehicles, 1)

    if smyrna == 0:
        opp_type = "whitespace"
        pen_pts = PEN_WHITESPACE
    elif pen_pct < THRESH_CONQUEST:
        opp_type = "conquest"
        pen_pts = PEN_CONQUEST
    elif pen_pct < THRESH_EXPAND:
        opp_type = "expand"
        pen_pts = PEN_EXPAND
    elif pen_pct < THRESH_GROW:
        opp_type = "grow"
        pen_pts = PEN_GROW
    else:
        opp_type = "defend"
        pen_pts = PEN_DEFEND
    factors["smyrna_penetration"] = pen_pts
    factors["penetration_pct"] = round(pen_pct * 100)

    # --- 4. Growth Signal (0-MAX_GROWTH_SIGNAL points) ---
    # Growing inventory = active buyer. Highest signal weight increase.
    growth_pts = 0
    if prev:
        prev_vehicles = prev.get("total_vehicles", 0) or 0
        if prev_vehicles > 0 and vehicles > 0:
            growth = (vehicles - prev_vehicles) / prev_vehicles
            if growth >= 0.20:
                growth_pts = GROWTH_EXPLOSIVE
            elif growth >= 0.10:
                growth_pts = GROWTH_STRONG
            elif growth >= 0.05:
                growth_pts = GROWTH_SOLID
            elif growth > 0.0:
                growth_pts = GROWTH_SLIGHT
            elif growth == 0.0:
                growth_pts = GROWTH_FLAT
            else:
                growth_pts = GROWTH_DECLINING
            factors["growth_pct"] = round(growth * 100)
        elif prev_vehicles == 0 and vehicles > 0:
            # New dealer appeared with inventory — positive signal
            growth_pts = GROWTH_NEW_DEALER
            factors["growth_pct"] = "new"
    factors["growth_signal"] = growth_pts

    # --- At-Risk Override ---
    # Had Smyrna last month, lost it this month = emergency retention
    if prev and (prev.get("smyrna_units") or 0) > 0 and smyrna == 0:
        opp_type = "at_risk"
        # Moderate bonus (tier override to "hot" does the heavy lifting)
        factors["at_risk_bonus"] = AT_RISK_BONUS

    total = min(100, scale_pts + fit_pts + pen_pts + growth_pts + factors.get("at_risk_bonus", 0))
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
