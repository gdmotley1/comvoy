"""Velocity metrics — Days on Lot, Turnover Rate, Price Markdown Velocity.

Activated with 2+ snapshots. Computes per-dealer and market-level metrics
from vehicles.first_seen_date and vehicle_diffs data.

Metrics:
  - Days on Lot: avg/median/max age of current inventory (snapshot_date - first_seen_date)
  - Turnover Rate: % of starting inventory sold in latest period
  - Markdown Velocity: avg price drop %, count of markdowns, avg days before first cut
"""

import logging
import time
from collections import defaultdict
from datetime import date, datetime

from fastapi import APIRouter, Query, HTTPException, Response

from app.database import get_service_client
from app.config import is_excluded_dealer

router = APIRouter(prefix="/api/velocity", tags=["velocity"])
logger = logging.getLogger(__name__)

# In-memory TTL cache
_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 300

# Excluded dealers (consistent with other modules)



def _get_snapshots(db):
    """Get latest two snapshots. Returns (current, previous) or raises."""
    snaps = db.table("report_snapshots").select(
        "id, report_date"
    ).order("report_date", desc=True).limit(2).execute()
    if not snaps.data:
        raise HTTPException(404, "No snapshots found")
    current = snaps.data[0]
    previous = snaps.data[1] if len(snaps.data) > 1 else None
    return current, previous


def _parse_date(d) -> date:
    """Parse a date string or return as-is if already a date."""
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def compute_days_on_lot(db, snap_id: str, snap_date: str, dealer_id: str = None, state: str = None) -> dict:
    """Compute inventory aging stats from first_seen_date.

    Returns per-dealer aging breakdown + market summary.
    """
    report_date = _parse_date(snap_date)

    # Paginate vehicle fetch
    vehicles = []
    offset = 0
    page_size = 1000
    while True:
        q = db.table("vehicles").select(
            "vin, dealer_id, first_seen_date, price, body_type, brand, "
            "dealers!inner(name, city, state)"
        ).eq("snapshot_id", snap_id).not_.is_("first_seen_date", "null")

        if dealer_id:
            q = q.eq("dealer_id", dealer_id)
        if state:
            q = q.eq("dealers.state", state.upper())

        page = q.range(offset, offset + page_size - 1).execute()
        if not page.data:
            break
        vehicles.extend(
            v for v in page.data
            if not is_excluded_dealer(v.get("dealers", {}).get("name", ""))
        )
        if len(page.data) < page_size:
            break
        offset += page_size

    if not vehicles:
        return {"error": "No vehicles with age data found"}

    # Compute per-vehicle age
    dealer_ages: dict[str, list[int]] = defaultdict(list)
    dealer_info: dict[str, dict] = {}
    all_ages = []
    body_type_ages: dict[str, list[int]] = defaultdict(list)

    for v in vehicles:
        fsd = _parse_date(v["first_seen_date"])
        age_days = (report_date - fsd).days
        if age_days < 0:
            age_days = 0

        did = v["dealer_id"]
        dealer_ages[did].append(age_days)
        all_ages.append(age_days)

        if v.get("body_type"):
            body_type_ages[v["body_type"]].append(age_days)

        if did not in dealer_info and v.get("dealers"):
            d = v["dealers"]
            dealer_info[did] = {"name": d["name"], "city": d["city"], "state": d["state"]}

    def _age_stats(ages: list[int]) -> dict:
        if not ages:
            return {}
        ages.sort()
        n = len(ages)
        return {
            "avg_days": round(sum(ages) / n, 1),
            "median_days": ages[n // 2],
            "max_days": ages[-1],
            "min_days": ages[0],
            "count": n,
            "over_30d": sum(1 for a in ages if a > 30),
            "over_60d": sum(1 for a in ages if a > 60),
        }

    # Market summary
    market = _age_stats(all_ages)

    # Per-dealer stats (sorted by avg age descending — slowest movers first)
    dealer_list = []
    for did, ages in dealer_ages.items():
        info = dealer_info.get(did, {})
        stats = _age_stats(ages)
        dealer_list.append({
            "dealer_id": did,
            "name": info.get("name", ""),
            "city": info.get("city", ""),
            "state": info.get("state", ""),
            **stats,
        })
    dealer_list.sort(key=lambda x: x.get("avg_days", 0), reverse=True)

    # Body type aging
    bt_aging = []
    for bt, ages in sorted(body_type_ages.items(), key=lambda x: sum(x[1]) / len(x[1]), reverse=True):
        if len(ages) >= 3:  # need minimum sample
            stats = _age_stats(ages)
            bt_aging.append({"body_type": bt, **stats})

    return {
        "snapshot_date": snap_date,
        "market": market,
        "by_body_type": bt_aging[:15],
        "dealers": dealer_list,
    }


def compute_turnover(db, current_snap: dict, prev_snap: dict, dealer_id: str = None, state: str = None) -> dict:
    """Compute turnover rate: sold VINs / starting inventory.

    Higher turnover = dealer is actively selling = good prospect.
    """
    if not prev_snap:
        return {"error": "Need 2+ snapshots to compute turnover"}

    # Get vehicle_diffs for this period
    diffs = []
    offset = 0
    page_size = 1000
    while True:
        q = db.table("vehicle_diffs").select(
            "diff_type, dealer_id, vin, dealers!inner(name, city, state)"
        ).eq("snapshot_id", current_snap["id"])

        if dealer_id:
            q = q.eq("dealer_id", dealer_id)
        if state:
            q = q.eq("dealers.state", state.upper())

        page = q.range(offset, offset + page_size - 1).execute()
        if not page.data:
            break
        diffs.extend(
            d for d in page.data
            if not is_excluded_dealer(d.get("dealers", {}).get("name", ""))
        )
        if len(page.data) < page_size:
            break
        offset += page_size

    # Get previous inventory counts per dealer (from dealer_snapshots)
    prev_snaps_q = db.table("dealer_snapshots").select(
        "dealer_id, total_vehicles, dealers!inner(name, city, state)"
    ).eq("snapshot_id", prev_snap["id"])
    if dealer_id:
        prev_snaps_q = prev_snaps_q.eq("dealer_id", dealer_id)
    if state:
        prev_snaps_q = prev_snaps_q.eq("dealers.state", state.upper())
    prev_data = prev_snaps_q.execute()

    prev_inv = {}
    dealer_info = {}
    for r in (prev_data.data or []):
        d = r.get("dealers", {})
        if is_excluded_dealer(d.get("name", "")):
            continue
        did = r["dealer_id"]
        prev_inv[did] = r["total_vehicles"] or 0
        dealer_info[did] = {"name": d["name"], "city": d["city"], "state": d["state"]}

    # Aggregate diffs per dealer
    dealer_sold: dict[str, int] = defaultdict(int)
    dealer_new: dict[str, int] = defaultdict(int)
    for d in diffs:
        did = d["dealer_id"]
        if d["diff_type"] == "sold":
            dealer_sold[did] += 1
        elif d["diff_type"] == "new":
            dealer_new[did] += 1
        # Capture dealer info from diffs too
        if did not in dealer_info and d.get("dealers"):
            dl = d["dealers"]
            dealer_info[did] = {"name": dl["name"], "city": dl["city"], "state": dl["state"]}

    # Compute per-dealer turnover
    dealer_list = []
    total_sold = 0
    total_prev = 0
    for did in set(list(prev_inv.keys()) + list(dealer_sold.keys())):
        starting = prev_inv.get(did, 0)
        sold = dealer_sold.get(did, 0)
        added = dealer_new.get(did, 0)
        total_sold += sold
        total_prev += starting

        turnover_pct = round(sold / starting * 100, 1) if starting > 0 else 0
        restock_pct = round(added / sold * 100, 1) if sold > 0 else 0
        info = dealer_info.get(did, {})

        dealer_list.append({
            "dealer_id": did,
            "name": info.get("name", ""),
            "city": info.get("city", ""),
            "state": info.get("state", ""),
            "starting_inv": starting,
            "sold": sold,
            "added": added,
            "net_change": added - sold,
            "turnover_pct": turnover_pct,
            "restock_pct": restock_pct,
        })

    # Sort by turnover rate descending — fastest movers first
    dealer_list.sort(key=lambda x: x["turnover_pct"], reverse=True)

    market_turnover = round(total_sold / total_prev * 100, 1) if total_prev > 0 else 0

    return {
        "period": f"{prev_snap['report_date']} → {current_snap['report_date']}",
        "market": {
            "total_sold": total_sold,
            "total_starting": total_prev,
            "turnover_pct": market_turnover,
            "total_added": sum(d["added"] for d in dealer_list),
            "dealers_with_sales": sum(1 for d in dealer_list if d["sold"] > 0),
        },
        "dealers": dealer_list,
    }


def compute_markdown_velocity(db, current_snap: dict, prev_snap: dict = None, dealer_id: str = None, state: str = None) -> dict:
    """Compute price markdown patterns from vehicle_diffs.

    Tracks: avg markdown %, markdown count, up vs down ratio.
    """
    if not prev_snap:
        return {"error": "Need 2+ snapshots to compute markdown velocity"}

    PRICE_FLOOR = 5000

    # Get price_change diffs
    diffs = []
    offset = 0
    page_size = 1000
    while True:
        q = db.table("vehicle_diffs").select(
            "dealer_id, vin, brand, body_type, old_price, new_price, "
            "dealers!inner(name, city, state)"
        ).eq("snapshot_id", current_snap["id"]).eq("diff_type", "price_change")

        if dealer_id:
            q = q.eq("dealer_id", dealer_id)
        if state:
            q = q.eq("dealers.state", state.upper())

        page = q.range(offset, offset + page_size - 1).execute()
        if not page.data:
            break
        diffs.extend(
            d for d in page.data
            if not is_excluded_dealer(d.get("dealers", {}).get("name", ""))
        )
        if len(page.data) < page_size:
            break
        offset += page_size

    if not diffs:
        return {"error": "No price changes found for this period"}

    # Filter to meaningful prices
    valid_diffs = [
        d for d in diffs
        if d.get("old_price") and d.get("new_price")
        and d["old_price"] >= PRICE_FLOOR and d["new_price"] >= PRICE_FLOOR
    ]

    if not valid_diffs:
        return {"error": "No valid price changes above $5k floor"}

    # Market-level aggregation
    drops = []
    increases = []
    dealer_markdowns: dict[str, list[dict]] = defaultdict(list)
    dealer_info: dict[str, dict] = {}
    bt_markdowns: dict[str, list[float]] = defaultdict(list)

    for d in valid_diffs:
        did = d["dealer_id"]
        old_p = d["old_price"]
        new_p = d["new_price"]
        change_pct = round((new_p - old_p) / old_p * 100, 1)
        change_abs = new_p - old_p

        entry = {
            "vin": d["vin"],
            "brand": d.get("brand"),
            "body_type": d.get("body_type"),
            "old_price": old_p,
            "new_price": new_p,
            "change_pct": change_pct,
            "change_abs": change_abs,
        }

        if change_pct < 0:
            drops.append(entry)
        else:
            increases.append(entry)

        dealer_markdowns[did].append(entry)
        if d.get("body_type"):
            bt_markdowns[d["body_type"]].append(change_pct)

        if did not in dealer_info and d.get("dealers"):
            dl = d["dealers"]
            dealer_info[did] = {"name": dl["name"], "city": dl["city"], "state": dl["state"]}

    # Market summary
    all_pcts = [e["change_pct"] for e in drops + increases]
    drop_pcts = [e["change_pct"] for e in drops]

    market = {
        "total_changes": len(valid_diffs),
        "drops": len(drops),
        "increases": len(increases),
        "avg_change_pct": round(sum(all_pcts) / len(all_pcts), 1) if all_pcts else 0,
        "avg_drop_pct": round(sum(drop_pcts) / len(drop_pcts), 1) if drop_pcts else 0,
        "avg_drop_abs": round(sum(e["change_abs"] for e in drops) / len(drops)) if drops else 0,
        "biggest_drop": min(drops, key=lambda x: x["change_pct"]) if drops else None,
        "biggest_increase": max(increases, key=lambda x: x["change_pct"]) if increases else None,
    }

    # Per-dealer markdown activity
    dealer_list = []
    for did, entries in dealer_markdowns.items():
        info = dealer_info.get(did, {})
        d_drops = [e for e in entries if e["change_pct"] < 0]
        d_increases = [e for e in entries if e["change_pct"] >= 0]
        d_pcts = [e["change_pct"] for e in entries]
        dealer_list.append({
            "dealer_id": did,
            "name": info.get("name", ""),
            "city": info.get("city", ""),
            "state": info.get("state", ""),
            "total_changes": len(entries),
            "drops": len(d_drops),
            "increases": len(d_increases),
            "avg_change_pct": round(sum(d_pcts) / len(d_pcts), 1) if d_pcts else 0,
        })
    dealer_list.sort(key=lambda x: x["drops"], reverse=True)

    # Body type markdown patterns
    bt_list = []
    for bt, pcts in sorted(bt_markdowns.items(), key=lambda x: len(x[1]), reverse=True):
        if len(pcts) >= 2:
            bt_list.append({
                "body_type": bt,
                "changes": len(pcts),
                "avg_change_pct": round(sum(pcts) / len(pcts), 1),
                "drops": sum(1 for p in pcts if p < 0),
            })

    return {
        "period": f"{prev_snap['report_date']} → {current_snap['report_date']}",
        "market": market,
        "by_body_type": bt_list[:12],
        "dealers": dealer_list,
    }


# ── API Endpoint ─────────────────────────────────────────────────────────────

@router.get("")
def get_velocity_metrics(
    response: Response,
    metric: str = Query("all", description="Metric: 'aging', 'turnover', 'markdown', or 'all'"),
    dealer_id: str = Query(None, description="Filter to specific dealer"),
    state: str = Query(None, description="Filter by state code"),
    top_n: int = Query(20, le=50, description="Top N dealers to return"),
):
    """Get velocity metrics — aging, turnover, and markdown velocity."""
    response.headers["Cache-Control"] = "public, s-maxage=300, stale-while-revalidate=60"

    cache_key = f"velocity:{metric}:{dealer_id or ''}:{state or ''}"
    now = time.time()
    if cache_key in _cache:
        ts, cached = _cache[cache_key]
        if now - ts < _CACHE_TTL:
            return cached

    db = get_service_client()
    current, previous = _get_snapshots(db)

    result = {}

    if metric in ("aging", "all"):
        aging = compute_days_on_lot(db, current["id"], current["report_date"], dealer_id, state)
        if "dealers" in aging:
            aging["dealers"] = aging["dealers"][:top_n]
        result["aging"] = aging

    if metric in ("turnover", "all") and previous:
        turnover = compute_turnover(db, current, previous, dealer_id, state)
        if "dealers" in turnover:
            turnover["dealers"] = turnover["dealers"][:top_n]
        result["turnover"] = turnover

    if metric in ("markdown", "all") and previous:
        markdown = compute_markdown_velocity(db, current, previous, dealer_id, state)
        if "dealers" in markdown:
            markdown["dealers"] = markdown["dealers"][:top_n]
        result["markdown"] = markdown

    if not previous and metric in ("turnover", "markdown", "all"):
        result["note"] = "Turnover and markdown metrics require 2+ snapshots."

    _cache[cache_key] = (time.time(), result)
    return result
