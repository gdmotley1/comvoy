"""Ingest endpoints — upload and process monthly Excel reports.

After loading data, automatically runs:
1. Lead scoring engine (ranks all dealers by opportunity)
2. Upload report generator (monthly change summary)
"""

import logging
import shutil
from pathlib import Path

from fastapi import APIRouter, File, UploadFile, HTTPException

from app.config import settings
from app.etl.parser import parse_report
from app.etl.loader import load_report
from app.models import IngestResult

router = APIRouter(prefix="/api/ingest", tags=["ingest"])
logger = logging.getLogger(__name__)


def _run_post_ingest(snapshot_id: str) -> list[str]:
    """Run scoring + report generation after a successful ingest.

    Returns a list of warning messages for any steps that failed.
    """
    from app.api.scoring import compute_lead_scores
    from app.api.reports import generate_upload_report
    from app.database import get_service_client

    warnings = []
    db = get_service_client()

    # Find previous snapshot for trend-based scoring
    snaps = db.table("report_snapshots").select("id").order(
        "report_date", desc=True
    ).limit(2).execute()
    prev_id = snaps.data[1]["id"] if len(snaps.data) >= 2 else None

    # 1. Score all dealers
    try:
        score_summary = compute_lead_scores(snapshot_id, prev_id)
        logger.info(f"Lead scoring complete: {score_summary}")
    except Exception as e:
        logger.exception(f"Lead scoring failed: {e}")
        warnings.append(f"Lead scoring failed: {e}")

    # 2. Generate upload report
    try:
        report_result = generate_upload_report(snapshot_id)
        logger.info(f"Upload report generated for {snapshot_id}")
    except Exception as e:
        logger.exception(f"Report generation failed: {e}")
        warnings.append(f"Report generation failed: {e}")

    return warnings


@router.post("/upload", response_model=IngestResult)
async def upload_report(file: UploadFile = File(...)):
    """Upload a monthly Multi-Brand Excel report and ingest it into the database.

    After loading, automatically runs lead scoring and generates the monthly report.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "File must be an Excel file (.xlsx)")

    # Sanitize filename — strip path separators to prevent traversal
    safe_name = Path(file.filename).name
    if not safe_name or safe_name.startswith("."):
        raise HTTPException(400, "Invalid filename")

    # Enforce 10MB upload limit
    MAX_UPLOAD_BYTES = 10 * 1024 * 1024
    contents = await file.read()
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"File too large ({len(contents) // 1024 // 1024}MB). Max 10MB.")

    # Save to reports dir
    reports_dir = Path(settings.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    file_path = reports_dir / safe_name

    with open(file_path, "wb") as f:
        f.write(contents)
    logger.info(f"Saved report: {file_path}")

    try:
        # Parse
        parsed = parse_report(file_path)
        logger.info(f"Parsed {len(parsed['dealers'])} dealers, {len(parsed['brands'])} brands")

        # Load into DB
        result = await load_report(parsed, file.filename)

        # Post-ingest autopilot: score + report
        warnings = _run_post_ingest(result["snapshot_id"])
        result["warnings"] = warnings

        return IngestResult(**result)
    except Exception as e:
        logger.exception("Ingest failed")
        raise HTTPException(500, "Ingest failed. Check server logs for details.")


@router.post("/ingest-local/{filename}", response_model=IngestResult)
async def ingest_local_file(filename: str):
    """Ingest a report already in the data/reports directory.

    After loading, automatically runs lead scoring and generates the monthly report.
    """
    safe_name = Path(filename).name
    file_path = Path(settings.reports_dir) / safe_name
    if not file_path.exists():
        raise HTTPException(404, f"File not found: {filename}")

    try:
        parsed = parse_report(file_path)
        result = await load_report(parsed, filename)

        # Post-ingest autopilot: score + report
        warnings = _run_post_ingest(result["snapshot_id"])
        result["warnings"] = warnings

        return IngestResult(**result)
    except Exception as e:
        logger.exception("Ingest failed")
        raise HTTPException(500, "Ingest failed. Check server logs for details.")


@router.post("/rescore")
async def rescore_latest():
    """Re-run lead scoring and report generation on the latest snapshot.

    Useful if scoring algorithm changes or you need to refresh scores.
    """
    from app.database import get_service_client

    db = get_service_client()
    snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
    if not snap.data:
        raise HTTPException(404, "No snapshots found")

    snapshot_id = snap.data[0]["id"]
    warnings = _run_post_ingest(snapshot_id)
    return {"status": "ok", "snapshot_id": snapshot_id, "message": "Scoring and report regenerated", "warnings": warnings}
