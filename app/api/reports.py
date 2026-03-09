"""Post-upload autopilot report — auto-generated after each monthly ingest.

When a new Multi-Brand report is uploaded, this generates a comprehensive
change report covering:
  - Territory-wide stats vs last month
  - Smyrna penetration changes
  - New/lost dealers
  - Lead score distribution
  - Top opportunities by state
  - At-risk accounts
"""

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException

from app.database import get_service_client

router = APIRouter(prefix="/api/reports", tags=["reports"])
logger = logging.getLogger(__name__)


def generate_upload_report(snapshot_id: str) -> dict:
    """Generate a full post-upload report for a snapshot.

    Called automatically after ingest + scoring. Returns the report
    and stores it in upload_reports table.
    """
    db = get_service_client()

    # Get this snapshot info
    snap = db.table("report_snapshots").select("*").eq("id", snapshot_id).single().execute()
    if not snap.data:
        return {"error": "Snapshot not found"}

    current = snap.data

    # Get previous snapshot (if any)
    prev_snaps = db.table("report_snapshots").select("*").lt(
        "report_date", current["report_date"]
    ).order("report_date", desc=True).limit(1).execute()
    prev = prev_snaps.data[0] if prev_snaps.data else None

    # Current snapshot dealer data
    cur_dealers = db.table("dealer_snapshots").select(
        "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank, "
        "dealers!inner(name, city, state)"
    ).eq("snapshot_id", snapshot_id).execute()

    cur_map = {r["dealer_id"]: r for r in cur_dealers.data}

    # Previous snapshot dealer data (join dealers for names — needed for lost dealer reporting)
    prev_map = {}
    if prev:
        prev_dealers = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank, "
            "dealers!inner(name, city, state)"
        ).eq("snapshot_id", prev["id"]).execute()
        prev_map = {r["dealer_id"]: r for r in prev_dealers.data}

    # Lead scores for this snapshot
    scores = db.table("lead_scores").select(
        "dealer_id, score, tier, opportunity_type, factors, "
        "dealers!inner(name, city, state)"
    ).eq("snapshot_id", snapshot_id).order("score", desc=True).execute()

    score_map = {r["dealer_id"]: r for r in scores.data}

    # === Build report ===
    report = _build_report(current, prev, cur_map, prev_map, score_map)

    # Build readable summary
    summary = _build_summary(report)

    # Store in DB
    try:
        db.table("upload_reports").upsert({
            "snapshot_id": snapshot_id,
            "report_json": report,
            "summary_text": summary,
        }).execute()
    except Exception as e:
        logger.warning(f"Failed to store upload report: {e}")

    return {"report": report, "summary": summary}


def _build_report(current, prev, cur_map, prev_map, score_map) -> dict:
    """Build the structured report dict."""

    total_dealers = len(cur_map)
    total_vehicles = sum(r["total_vehicles"] or 0 for r in cur_map.values())
    total_smyrna = sum(r["smyrna_units"] or 0 for r in cur_map.values())
    dealers_with_smyrna = sum(1 for r in cur_map.values() if (r["smyrna_units"] or 0) > 0)

    report = {
        "report_date": current["report_date"],
        "file_name": current["file_name"],
        "overview": {
            "total_dealers": total_dealers,
            "total_vehicles": total_vehicles,
            "total_smyrna_units": total_smyrna,
            "dealers_with_smyrna": dealers_with_smyrna,
            "smyrna_penetration_pct": round(
                dealers_with_smyrna / total_dealers * 100, 2
            ) if total_dealers else 0,
        },
    }

    # Deltas vs previous month
    if prev:
        prev_total_v = prev.get("total_vehicles") or 0
        prev_total_d = prev.get("total_dealers") or 0
        prev_smyrna = sum(r["smyrna_units"] or 0 for r in prev_map.values())
        prev_smyrna_dealers = sum(1 for r in prev_map.values() if (r["smyrna_units"] or 0) > 0)

        report["vs_last_month"] = {
            "dealer_delta": total_dealers - prev_total_d,
            "vehicle_delta": total_vehicles - prev_total_v,
            "smyrna_unit_delta": total_smyrna - prev_smyrna,
            "smyrna_dealer_delta": dealers_with_smyrna - prev_smyrna_dealers,
        }

        # New and lost dealers
        cur_ids = set(cur_map.keys())
        prev_ids = set(prev_map.keys())
        new_ids = cur_ids - prev_ids
        lost_ids = prev_ids - cur_ids

        report["new_dealers"] = [
            {"name": cur_map[did]["dealers"]["name"],
             "city": cur_map[did]["dealers"]["city"],
             "state": cur_map[did]["dealers"]["state"],
             "vehicles": cur_map[did]["total_vehicles"]}
            for did in new_ids
        ]
        report["lost_dealers"] = [
            {"name": prev_map[did].get("dealers", {}).get("name", "Unknown"),
             "city": prev_map[did].get("dealers", {}).get("city", ""),
             "state": prev_map[did].get("dealers", {}).get("state", ""),
             "vehicles": prev_map[did]["total_vehicles"]}
            for did in lost_ids
        ]

        # Smyrna gains and losses
        smyrna_gained = []
        smyrna_lost = []
        for did in cur_ids & prev_ids:
            cur_s = cur_map[did]["smyrna_units"] or 0
            prev_s = prev_map[did]["smyrna_units"] or 0
            if prev_s == 0 and cur_s > 0:
                smyrna_gained.append({
                    "name": cur_map[did]["dealers"]["name"],
                    "state": cur_map[did]["dealers"]["state"],
                    "units": cur_s,
                })
            elif prev_s > 0 and cur_s == 0:
                smyrna_lost.append({
                    "name": cur_map[did]["dealers"]["name"],
                    "state": cur_map[did]["dealers"]["state"],
                    "prev_units": prev_s,
                })
        report["smyrna_gained"] = smyrna_gained
        report["smyrna_lost"] = smyrna_lost

    # Lead score distribution
    if score_map:
        hot = [r for r in score_map.values() if r["tier"] == "hot"]
        warm = [r for r in score_map.values() if r["tier"] == "warm"]
        cold = [r for r in score_map.values() if r["tier"] == "cold"]

        report["lead_scores"] = {
            "hot": len(hot),
            "warm": len(warm),
            "cold": len(cold),
            "top_10": [
                {"name": r["dealers"]["name"], "state": r["dealers"]["state"],
                 "score": r["score"], "type": r["opportunity_type"]}
                for r in sorted(score_map.values(), key=lambda x: x["score"], reverse=True)[:10]
            ],
        }

    # Per-state breakdown
    state_stats = {}
    for r in cur_map.values():
        st = r["dealers"]["state"]
        if st not in state_stats:
            state_stats[st] = {"dealers": 0, "vehicles": 0, "smyrna": 0}
        state_stats[st]["dealers"] += 1
        state_stats[st]["vehicles"] += r["total_vehicles"] or 0
        state_stats[st]["smyrna"] += r["smyrna_units"] or 0

    report["by_state"] = dict(sorted(state_stats.items(), key=lambda x: x[1]["vehicles"], reverse=True))

    return report


def _build_summary(report: dict) -> str:
    """Build a human-readable summary from the report."""
    o = report["overview"]
    lines = [
        f"Monthly Report — {report['report_date']}",
        f"",
        f"Territory: {o['total_dealers']} dealers, {o['total_vehicles']:,} vehicles",
        f"Smyrna: {o['total_smyrna_units']} units at {o['dealers_with_smyrna']} dealers ({o['smyrna_penetration_pct']}% penetration)",
    ]

    if "vs_last_month" in report:
        d = report["vs_last_month"]
        lines.append(f"")
        lines.append(f"vs Last Month:")
        lines.append(f"  Dealers: {d['dealer_delta']:+d} | Vehicles: {d['vehicle_delta']:+,d}")
        lines.append(f"  Smyrna units: {d['smyrna_unit_delta']:+d} | Smyrna dealers: {d['smyrna_dealer_delta']:+d}")

        if report.get("new_dealers"):
            lines.append(f"  New dealers: {len(report['new_dealers'])}")
        if report.get("lost_dealers"):
            lines.append(f"  Lost dealers: {len(report['lost_dealers'])}")
        if report.get("smyrna_gained"):
            names = ", ".join(g["name"] for g in report["smyrna_gained"])
            lines.append(f"  Smyrna GAINED: {names}")
        if report.get("smyrna_lost"):
            names = ", ".join(g["name"] for g in report["smyrna_lost"])
            lines.append(f"  Smyrna LOST: {names}")

    if "lead_scores" in report:
        ls = report["lead_scores"]
        lines.append(f"")
        lines.append(f"Lead Scores: {ls['hot']} hot, {ls['warm']} warm, {ls['cold']} cold")
        if ls.get("top_10"):
            lines.append(f"Top opportunities:")
            for t in ls["top_10"][:5]:
                lines.append(f"  {t['score']}pts — {t['name']} ({t['state']}) [{t['type']}]")

    return "\n".join(lines)


@router.get("/latest")
def get_latest_report():
    """Get the most recent upload report."""
    db = get_service_client()

    result = db.table("upload_reports").select(
        "*, report_snapshots!inner(report_date)"
    ).order("generated_at", desc=True).limit(1).execute()

    if not result.data:
        raise HTTPException(404, "No upload reports found. Upload a report first.")

    row = result.data[0]
    return {
        "report_date": row["report_snapshots"]["report_date"],
        "report": row["report_json"],
        "summary": row["summary_text"],
        "generated_at": row["generated_at"],
    }


@router.get("/{snapshot_id}")
def get_report(snapshot_id: str):
    """Get the upload report for a specific snapshot."""
    db = get_service_client()

    result = db.table("upload_reports").select("*").eq(
        "snapshot_id", snapshot_id
    ).execute()

    if not result.data:
        raise HTTPException(404, "No report for this snapshot")

    row = result.data[0]
    return {
        "report": row["report_json"],
        "summary": row["summary_text"],
        "generated_at": row["generated_at"],
    }
