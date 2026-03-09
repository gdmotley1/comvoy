"""Alerts endpoint — computes notable changes between the two most recent snapshots."""

import logging

from fastapi import APIRouter, Query

from app.database import get_service_client
from app.models import Alert, AlertsResponse

router = APIRouter(prefix="/api", tags=["alerts"])
logger = logging.getLogger(__name__)

# Alert thresholds
RANK_CHANGE_THRESHOLD = 10
INVENTORY_CHANGE_PCT = 0.25

# Priority order for sorting
_PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}


@router.get("/alerts", response_model=AlertsResponse)
def get_alerts(
    state: str = Query(None, description="Optional state filter (2-letter code)"),
    limit: int = Query(20, le=50),
):
    """Get notable changes between the two most recent monthly reports."""
    db = get_service_client()

    # Get two most recent snapshots
    snaps = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True).limit(2).execute()

    if len(snaps.data) < 2:
        date_a = snaps.data[0]["report_date"] if snaps.data else "N/A"
        return AlertsResponse(
            snapshot_a_date=date_a,
            snapshot_b_date=date_a,
            alerts=[],
            summary="Need at least 2 monthly snapshots to detect changes. Upload another report to enable alerts.",
            total_alerts=0,
        )

    snap_b = snaps.data[0]  # newer
    snap_a = snaps.data[1]  # older

    # Fetch dealer_snapshots for both, with dealer info
    def _fetch(snap_id):
        q = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank, "
            "dealers!inner(id, name, city, state)"
        ).eq("snapshot_id", snap_id)
        if state:
            q = q.eq("dealers.state", state.upper())
        return {row["dealer_id"]: row for row in q.execute().data}

    data_a = _fetch(snap_a["id"])
    data_b = _fetch(snap_b["id"])

    all_ids = set(data_a.keys()) | set(data_b.keys())
    alerts: list[Alert] = []

    for did in all_ids:
        a = data_a.get(did)
        b = data_b.get(did)

        # Dealer info from whichever snapshot has it
        d = (b or a)["dealers"]
        name, city, st = d["name"], d["city"], d["state"]

        if b and not a:
            alerts.append(Alert(
                alert_type="new_dealer", priority="high",
                dealer_name=name, dealer_id=did, city=city, state=st,
                message=f"New dealer appeared with {b['total_vehicles']} vehicles",
                value_after=b["total_vehicles"],
            ))
            continue

        if a and not b:
            alerts.append(Alert(
                alert_type="lost_dealer", priority="high",
                dealer_name=name, dealer_id=did, city=city, state=st,
                message=f"Dealer no longer in report (had {a['total_vehicles']} vehicles)",
                value_before=a["total_vehicles"],
            ))
            continue

        # Both exist — compare
        smyrna_a = a["smyrna_units"] or 0
        smyrna_b = b["smyrna_units"] or 0

        # Smyrna gained (0 → >0)
        if smyrna_a == 0 and smyrna_b > 0:
            alerts.append(Alert(
                alert_type="smyrna_gained", priority="high",
                dealer_name=name, dealer_id=did, city=city, state=st,
                message=f"Gained Smyrna products: 0 -> {smyrna_b} units",
                value_before=0, value_after=smyrna_b,
            ))
        # Smyrna lost (>0 → 0)
        elif smyrna_a > 0 and smyrna_b == 0:
            alerts.append(Alert(
                alert_type="smyrna_lost", priority="high",
                dealer_name=name, dealer_id=did, city=city, state=st,
                message=f"Lost all Smyrna products: {smyrna_a} -> 0 units",
                value_before=smyrna_a, value_after=0,
            ))
        # Smyrna increase (already had some)
        elif smyrna_b > smyrna_a > 0:
            alerts.append(Alert(
                alert_type="smyrna_increase", priority="medium",
                dealer_name=name, dealer_id=did, city=city, state=st,
                message=f"Smyrna units grew: {smyrna_a} -> {smyrna_b} (+{smyrna_b - smyrna_a})",
                value_before=smyrna_a, value_after=smyrna_b,
            ))
        # Smyrna decrease (still has some)
        elif 0 < smyrna_b < smyrna_a:
            alerts.append(Alert(
                alert_type="smyrna_decrease", priority="medium",
                dealer_name=name, dealer_id=did, city=city, state=st,
                message=f"Smyrna units dropped: {smyrna_a} -> {smyrna_b} ({smyrna_b - smyrna_a})",
                value_before=smyrna_a, value_after=smyrna_b,
            ))

        # Rank changes
        rank_a = a["rank"]
        rank_b = b["rank"]
        if rank_a is not None and rank_b is not None:
            rank_delta = rank_a - rank_b  # positive = improved
            if rank_delta >= RANK_CHANGE_THRESHOLD:
                alerts.append(Alert(
                    alert_type="rank_jump", priority="low",
                    dealer_name=name, dealer_id=did, city=city, state=st,
                    message=f"Rank improved: #{rank_a} -> #{rank_b} (+{rank_delta} positions)",
                    value_before=rank_a, value_after=rank_b,
                ))
            elif rank_delta <= -RANK_CHANGE_THRESHOLD:
                alerts.append(Alert(
                    alert_type="rank_drop", priority="low",
                    dealer_name=name, dealer_id=did, city=city, state=st,
                    message=f"Rank dropped: #{rank_a} -> #{rank_b} ({rank_delta} positions)",
                    value_before=rank_a, value_after=rank_b,
                ))

        # Inventory surges/declines
        veh_a = a["total_vehicles"] or 0
        veh_b = b["total_vehicles"] or 0
        if veh_a > 0:
            pct_change = (veh_b - veh_a) / veh_a
            if pct_change >= INVENTORY_CHANGE_PCT:
                alerts.append(Alert(
                    alert_type="inventory_surge", priority="low",
                    dealer_name=name, dealer_id=did, city=city, state=st,
                    message=f"Inventory surged {pct_change:+.0%}: {veh_a} -> {veh_b} vehicles",
                    value_before=veh_a, value_after=veh_b,
                ))
            elif pct_change <= -INVENTORY_CHANGE_PCT:
                alerts.append(Alert(
                    alert_type="inventory_decline", priority="low",
                    dealer_name=name, dealer_id=did, city=city, state=st,
                    message=f"Inventory declined {pct_change:+.0%}: {veh_a} -> {veh_b} vehicles",
                    value_before=veh_a, value_after=veh_b,
                ))

    # Sort by priority then alert type
    alerts.sort(key=lambda a: (_PRIORITY_ORDER.get(a.priority, 9), a.alert_type, a.dealer_name))
    total = len(alerts)
    alerts = alerts[:limit]

    # Build summary
    type_counts = {}
    for al in alerts:
        type_counts[al.alert_type] = type_counts.get(al.alert_type, 0) + 1

    parts = []
    if type_counts.get("new_dealer"):
        parts.append(f"{type_counts['new_dealer']} new dealer(s)")
    if type_counts.get("lost_dealer"):
        parts.append(f"{type_counts['lost_dealer']} lost dealer(s)")
    if type_counts.get("smyrna_gained"):
        parts.append(f"{type_counts['smyrna_gained']} Smyrna gain(s)")
    if type_counts.get("smyrna_lost"):
        parts.append(f"{type_counts['smyrna_lost']} Smyrna loss(es)")
    if type_counts.get("smyrna_increase"):
        parts.append(f"{type_counts['smyrna_increase']} Smyrna increase(s)")
    if type_counts.get("smyrna_decrease"):
        parts.append(f"{type_counts['smyrna_decrease']} Smyrna decrease(s)")
    rank_changes = type_counts.get("rank_jump", 0) + type_counts.get("rank_drop", 0)
    if rank_changes:
        parts.append(f"{rank_changes} rank change(s)")
    inv_changes = type_counts.get("inventory_surge", 0) + type_counts.get("inventory_decline", 0)
    if inv_changes:
        parts.append(f"{inv_changes} inventory swing(s)")

    summary = f"Between {snap_a['report_date']} and {snap_b['report_date']}: " + (", ".join(parts) if parts else "no notable changes detected")

    return AlertsResponse(
        snapshot_a_date=snap_a["report_date"],
        snapshot_b_date=snap_b["report_date"],
        alerts=alerts,
        summary=summary,
        total_alerts=total,
    )
