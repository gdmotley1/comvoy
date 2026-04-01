"""Dashboard analytics endpoint — returns all data for the visual dashboard in one call."""

import logging
import time
from collections import Counter, defaultdict

from fastapi import APIRouter, Query, HTTPException, Response

from app.database import get_service_client
from app.config import is_excluded_dealer

# In-memory TTL cache — data only changes after monthly scrape
_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 300  # 5 minutes

router = APIRouter(prefix="/api", tags=["dashboard"])
logger = logging.getLogger(__name__)



def _latest_snapshot_id(db) -> str:
    result = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True).limit(1).execute()
    if not result.data:
        raise HTTPException(404, "No report snapshots found.")
    return result.data[0]["id"], result.data[0]["report_date"]


def _paginate_vehicles(db, snap_id, state=None, body_type=None):
    """Fetch all vehicles for a snapshot, paginating past Supabase 1000-row limit."""
    vehicles = []
    offset = 0
    page_size = 1000
    while True:
        if state:
            q = db.table("vehicles").select(
                "vin, brand, body_type, body_builder, price, condition, fuel_type, "
                "transmission, is_smyrna, is_fouts, dealer_id, first_seen_date, dealers!inner(name, city, state)"
            ).eq("snapshot_id", snap_id).eq("condition", "New").eq("dealers.state", state.upper())
        else:
            q = db.table("vehicles").select(
                "vin, brand, body_type, body_builder, price, condition, fuel_type, "
                "transmission, is_smyrna, is_fouts, dealer_id, first_seen_date, dealers!inner(name, city, state)"
            ).eq("snapshot_id", snap_id).eq("condition", "New")
        if body_type:
            q = q.eq("body_type", body_type)
        page = q.range(offset, offset + page_size - 1).execute()
        if not page.data:
            break
        # Filter out excluded dealers
        vehicles.extend(
            v for v in page.data
            if not is_excluded_dealer(v.get("dealers", {}).get("name", ""))
        )
        if len(page.data) < page_size:
            break
        offset += page_size
    return vehicles


@router.get("/dashboard")
def get_dashboard(
    response: Response,
    state: str = Query(None, description="Filter by state code"),
    body_type: str = Query(None, description="Filter by body type (exact match)"),
):
    """Single endpoint returning all dashboard analytics."""
    response.headers["Cache-Control"] = "public, s-maxage=300, stale-while-revalidate=60"

    # Check cache
    cache_key = f"dashboard:{state or 'all'}:{body_type or 'all'}"
    now = time.time()
    if cache_key in _cache:
        ts, cached = _cache[cache_key]
        if now - ts < _CACHE_TTL:
            return cached

    db = get_service_client()
    snap_id, snap_date = _latest_snapshot_id(db)

    # Fetch all vehicles (paginated)
    vehicles = _paginate_vehicles(db, snap_id, state, body_type)

    if not vehicles:
        raise HTTPException(404, "No vehicle data found for this filter.")

    # Aggregate from vehicle data
    # Filter out junk prices (placeholders, errors) — $5k floor for commercial trucks
    PRICE_FLOOR = 5000
    prices = [v["price"] for v in vehicles if v["price"] and v["price"] >= PRICE_FLOOR]
    brand_counter = Counter()
    body_type_counter = Counter()
    builder_counter = Counter()
    fuel_counter = Counter()
    trans_counter = Counter()
    smyrna_count = 0
    fouts_count = 0
    smyrna_prices = []
    dealer_vehicles = defaultdict(int)
    dealer_info = {}

    for v in vehicles:
        brand_counter[v["brand"]] += 1
        body_type_counter[v["body_type"]] += 1
        if v["body_builder"]:
            builder_counter[v["body_builder"]] += 1
        if v["fuel_type"]:
            fuel_counter[v["fuel_type"]] += 1
        if v["transmission"]:
            trans_counter[v["transmission"]] += 1
        if v["is_smyrna"]:
            smyrna_count += 1
            if v["price"] and v["price"] >= PRICE_FLOOR:
                smyrna_prices.append(v["price"])
        if v.get("is_fouts"):
            fouts_count += 1
        did = v["dealer_id"]
        dealer_vehicles[did] += 1
        if did not in dealer_info and v.get("dealers"):
            d = v["dealers"]
            dealer_info[did] = {"name": d["name"], "city": d["city"], "state": d["state"]}

    total = len(vehicles)

    # Price stats
    prices.sort()
    n = len(prices)
    avg_price = round(sum(prices) / n) if n else 0
    median_price = (prices[(n - 1) // 2] + prices[n // 2]) // 2 if n else 0

    # Price brackets (overall + per body type)
    def _bracket(p):
        if p < 30000: return "<30k"
        if p < 50000: return "30-50k"
        if p < 75000: return "50-75k"
        if p < 100000: return "75-100k"
        return "100k+"

    bracket_keys = ["<30k", "30-50k", "50-75k", "75-100k", "100k+"]
    brackets = {k: 0 for k in bracket_keys}
    bt_brackets = defaultdict(lambda: {k: 0 for k in bracket_keys})
    for v in vehicles:
        if v["price"] and v["price"] >= PRICE_FLOOR:
            bk = _bracket(v["price"])
            brackets[bk] += 1
            if v["body_type"]:
                bt_brackets[v["body_type"]][bk] += 1

    # Build per-body-type bracket data for top body types
    price_by_body_type = {}
    for bt, counts in sorted(bt_brackets.items(), key=lambda x: sum(x[1].values()), reverse=True)[:12]:
        bt_prices_list = [v["price"] for v in vehicles if v["price"] and v["price"] >= PRICE_FLOOR and v["body_type"] == bt]
        price_by_body_type[bt] = {
            "brackets": counts,
            "avg": round(sum(bt_prices_list) / len(bt_prices_list)) if bt_prices_list else 0,
            "count": len(bt_prices_list),
        }

    # Lead scores — filter to dealers present in the current (filtered) vehicle set
    lead_query = db.table("lead_scores").select("tier, score, dealer_id").eq("snapshot_id", snap_id)
    filtered_dealer_ids = list(dealer_info.keys())
    if filtered_dealer_ids and (state or body_type):
        lead_query = lead_query.in_("dealer_id", filtered_dealer_ids)
    leads_data = lead_query.execute().data or []
    lead_tiers = Counter(l["tier"] for l in leads_data)
    lead_by_dealer = {l["dealer_id"]: l for l in leads_data}

    # Top dealers by inventory
    top_dealers = sorted(dealer_vehicles.items(), key=lambda x: x[1], reverse=True)[:15]
    top_dealer_list = []
    for did, vcount in top_dealers:
        info = dealer_info.get(did, {})
        ld = lead_by_dealer.get(did, {})
        top_dealer_list.append({
            "id": did,
            "name": info.get("name", ""),
            "city": info.get("city", ""),
            "state": info.get("state", ""),
            "vehicles": vcount,
            "tier": ld.get("tier", "cold"),
        })

    # Smyrna intel
    smyrna_by_dealer = defaultdict(lambda: {"units": 0, "body_types": Counter()})
    for v in vehicles:
        if v["is_smyrna"]:
            did = v["dealer_id"]
            smyrna_by_dealer[did]["units"] += 1
            smyrna_by_dealer[did]["body_types"][v["body_type"]] += 1

    smyrna_top_dealers = sorted(smyrna_by_dealer.items(), key=lambda x: x[1]["units"], reverse=True)[:5]
    smyrna_body_mix = Counter()
    for v in vehicles:
        if v["is_smyrna"]:
            smyrna_body_mix[v["body_type"]] += 1

    # State breakdown
    state_vehicle_counter = Counter()
    for v in vehicles:
        if v.get("dealers"):
            state_vehicle_counter[v["dealers"]["state"]] += 1
    by_state = [{"state": s, "vehicles": c} for s, c in state_vehicle_counter.most_common()]

    # Smyrna price position vs market by body type
    bt_prices_all = defaultdict(list)
    bt_prices_smyrna = defaultdict(list)
    for v in vehicles:
        if v["price"] and v["price"] >= PRICE_FLOOR and v["body_type"]:
            bt_prices_all[v["body_type"]].append(v["price"])
            if v["is_smyrna"]:
                bt_prices_smyrna[v["body_type"]].append(v["price"])

    smyrna_price_position = []
    for bt, s_prices in bt_prices_smyrna.items():
        if len(s_prices) >= 2:  # need at least 2 Smyrna units for meaningful avg
            m_prices = bt_prices_all[bt]
            s_avg = round(sum(s_prices) / len(s_prices))
            m_avg = round(sum(m_prices) / len(m_prices))
            delta_pct = round((s_avg - m_avg) / m_avg * 100, 1) if m_avg else 0
            smyrna_price_position.append({
                "body_type": bt, "smyrna_avg": s_avg, "market_avg": m_avg,
                "delta_pct": delta_pct, "smyrna_count": len(s_prices), "market_count": len(m_prices),
            })
    smyrna_price_position.sort(key=lambda x: x["smyrna_count"], reverse=True)

    # City breakdown (when state filter is active)
    by_city = []
    if state:
        city_counter = Counter()
        for v in vehicles:
            if v.get("dealers"):
                city_counter[v["dealers"]["city"]] += 1
        by_city = [{"city": c, "vehicles": n} for c, n in city_counter.most_common(12)]

    # ── Velocity metrics (aging + turnover summary + dealer-level) ──────────────
    velocity_summary = {}
    dealer_velocity_list = []
    try:
        from datetime import datetime, date as date_type
        report_date = datetime.strptime(str(snap_date), "%Y-%m-%d").date() if isinstance(snap_date, str) else snap_date

        # Aging — market-level + per-dealer
        ages = []
        dealer_ages = defaultdict(list)
        for v in vehicles:
            fsd = v.get("first_seen_date")
            if fsd:
                if isinstance(fsd, str):
                    fsd = datetime.strptime(fsd, "%Y-%m-%d").date()
                age = (report_date - fsd).days
                if age >= 0:
                    ages.append(age)
                    dealer_ages[v["dealer_id"]].append(age)
        if ages:
            ages.sort()
            n = len(ages)
            velocity_summary["aging"] = {
                "avg_days": round(sum(ages) / n, 1),
                "median_days": ages[n // 2],
                "over_30d": sum(1 for a in ages if a > 30),
                "over_60d": sum(1 for a in ages if a > 60),
            }

        # Turnover — fetch from vehicle_diffs (with dealer_id for per-dealer grouping)
        prev_snap = db.table("report_snapshots").select(
            "id, report_date"
        ).order("report_date", desc=True).limit(2).execute()
        dealer_diffs = defaultdict(lambda: {"sold": 0, "added": 0, "price_changes": 0})
        period_str = ""
        if prev_snap.data and len(prev_snap.data) > 1:
            prev = prev_snap.data[1]
            period_str = f"{prev['report_date']} → {snap_date}"
            # Paginate diffs (could exceed 1000)
            diffs_all = []
            doff = 0
            while True:
                diffs_q = db.table("vehicle_diffs").select("diff_type, dealer_id").eq(
                    "snapshot_id", snap_id
                )
                if body_type:
                    diffs_q = diffs_q.eq("body_type", body_type)
                if state and filtered_dealer_ids:
                    diffs_q = diffs_q.in_("dealer_id", filtered_dealer_ids)
                page = diffs_q.range(doff, doff + 999).execute()
                if not page.data:
                    break
                diffs_all.extend(page.data)
                if len(page.data) < 1000:
                    break
                doff += 1000
            if diffs_all:
                sold = added = price_changes = 0
                for d in diffs_all:
                    did = d.get("dealer_id")
                    dt = d["diff_type"]
                    if dt == "sold":
                        sold += 1
                        if did:
                            dealer_diffs[did]["sold"] += 1
                    elif dt == "new":
                        added += 1
                        if did:
                            dealer_diffs[did]["added"] += 1
                    elif dt == "price_change":
                        price_changes += 1
                        if did:
                            dealer_diffs[did]["price_changes"] += 1
                velocity_summary["turnover"] = {
                    "period": period_str,
                    "sold": sold,
                    "added": added,
                    "price_changes": price_changes,
                    "net_change": added - sold,
                }

        # Build dealer-level velocity rows
        all_dealer_ids = set(dealer_ages.keys()) | set(dealer_diffs.keys())
        for did in all_dealer_ids:
            if did not in dealer_info:
                continue
            info = dealer_info[did]
            d_ages = dealer_ages.get(did, [])
            dd = dealer_diffs.get(did, {"sold": 0, "added": 0, "price_changes": 0})
            inv = dealer_vehicles.get(did, 0)
            starting_inv = inv + dd["sold"] - dd["added"]  # approximate beginning-of-period inventory
            turnover_pct = round(dd["sold"] / starting_inv * 100, 1) if starting_inv > 0 else 0
            row = {
                "id": did,
                "name": info["name"],
                "city": info["city"],
                "state": info["state"],
                "inventory": inv,
                "avg_days": round(sum(d_ages) / len(d_ages), 1) if d_ages else None,
                "sold": dd["sold"],
                "added": dd["added"],
                "price_changes": dd["price_changes"],
                "net_change": dd["added"] - dd["sold"],
                "turnover_pct": turnover_pct,
            }
            ld = lead_by_dealer.get(did)
            if ld:
                row["tier"] = ld.get("tier", "cold")
            dealer_velocity_list.append(row)
        dealer_velocity_list.sort(key=lambda x: x["sold"], reverse=True)
    except Exception as e:
        logger.debug(f"Velocity summary skipped: {e}")

    result = {
        "snapshot_date": snap_date,
        "active_filters": {k: v for k, v in {"state": state, "body_type": body_type}.items() if v},
        "totals": {
            "vehicles": total,
            "dealers": len(dealer_vehicles),
            "avg_price": avg_price,
            "median_price": median_price,
            "smyrna_truck_units": smyrna_count,
            "fouts_cv_units": fouts_count,
            "our_combined_units": smyrna_count + fouts_count,
            "smyrna_avg_price": round(sum(smyrna_prices) / len(smyrna_prices)) if smyrna_prices else 0,
        },
        "leads": {
            "hot": lead_tiers.get("hot", 0),
            "warm": lead_tiers.get("warm", 0),
            "cold": lead_tiers.get("cold", 0),
        },
        "by_brand": [
            {"brand": b, "count": c, "share": round(c / total * 100, 1)}
            for b, c in brand_counter.most_common()
        ],
        "by_body_type": [
            {"body_type": bt, "count": c, "share": round(c / total * 100, 1)}
            for bt, c in body_type_counter.most_common()
        ],
        "by_builder": [
            {"builder": b, "count": c, "share": round(c / total * 100, 1)}
            for b, c in builder_counter.most_common(20)
        ],
        "price_brackets": brackets,
        "price_by_body_type": price_by_body_type,
        "by_state": by_state,
        "by_fuel": [
            {"fuel": f, "count": c, "share": round(c / total * 100, 1)}
            for f, c in fuel_counter.most_common()
        ],
        "by_transmission": [
            {"type": t, "count": c, "share": round(c / total * 100, 1)}
            for t, c in trans_counter.most_common()
        ],
        "top_dealers": top_dealer_list,
        "smyrna_price_position": smyrna_price_position,
        "by_city": by_city,
        "velocity": velocity_summary,
        "dealer_velocity": dealer_velocity_list,
        "smyrna_intel": {
            "smyrna_truck_units": smyrna_count,
            "smyrna_truck_penetration_pct": round(smyrna_count / total * 100, 2) if total else 0,
            "fouts_cv_units": fouts_count,
            "combined_units": smyrna_count + fouts_count,
            "dealer_count": len(smyrna_by_dealer),
            "penetration_pct": round(smyrna_count / total * 100, 2) if total else 0,
            "top_dealers": [
                {"id": did, "name": dealer_info.get(did, {}).get("name", ""), "units": d["units"]}
                for did, d in smyrna_top_dealers
            ],
            "body_type_mix": [
                {"type": bt, "count": c}
                for bt, c in smyrna_body_mix.most_common()
            ],
        },
    }
    _cache[cache_key] = (time.time(), result)
    return result


@router.get("/dashboard/trends")
def dashboard_trends():
    """Week-over-week trend deltas from snapshot_metrics."""
    cache_key = "trends"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < _CACHE_TTL:
        return cached[1]

    db = get_service_client()

    # Get two most recent snapshots
    snaps = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True).limit(2).execute()
    if not snaps.data:
        raise HTTPException(404, "No snapshots found")

    current_snap = snaps.data[0]
    prev_snap = snaps.data[1] if len(snaps.data) >= 2 else None

    # Fetch current snapshot_metrics
    current_metrics_row = db.table("snapshot_metrics").select("metrics").eq(
        "snapshot_id", current_snap["id"]).execute()
    if not current_metrics_row.data:
        raise HTTPException(404, "No metrics for current snapshot")
    cm = current_metrics_row.data[0]["metrics"]

    # Fetch previous snapshot_metrics for delta comparison
    pm = None
    if prev_snap:
        prev_metrics_row = db.table("snapshot_metrics").select("metrics").eq(
            "snapshot_id", prev_snap["id"]).execute()
        if prev_metrics_row.data:
            pm = prev_metrics_row.data[0]["metrics"]

    cs = cm["summary"]
    ps = pm["summary"] if pm else None

    result = {
        "current_date": current_snap["report_date"],
        "prev_date": prev_snap["report_date"] if prev_snap else None,
        "summary": {
            "vehicles": cs["total_vehicles"],
            "dealers": cs["total_dealers"],
            "sold": cs["sold"],
            "added": cs["added"],
            "net_change": cs["net_change"],
            "price_changes": cs["price_changes"],
        },
        "deltas": None,
        "hot_segments": cm.get("hot_segments", [])[:5],
        "dealer_churn": cm.get("dealer_churn"),
        "smyrna_share": cm.get("smyrna_share"),
        "top_growers": cm.get("dealer_growth", {}).get("top_growers", [])[:5],
        "top_shrinkers": cm.get("dealer_growth", {}).get("top_shrinkers", [])[:5],
        "absorption": cm.get("absorption_rate", {}).get("interpretation"),
    }

    # Compute deltas vs previous period
    if ps:
        result["deltas"] = {
            "vehicle_delta": cs["total_vehicles"] - ps["total_vehicles"],
            "dealer_delta": cs["total_dealers"] - ps["total_dealers"],
            "smyrna_delta": (cm.get("smyrna_share", {}).get("total_units", 0)
                            - pm.get("smyrna_share", {}).get("total_units", 0)) if pm else 0,
            "fouts_delta": (cm.get("smyrna_share", {}).get("fouts_plant_units", 0)
                           - pm.get("smyrna_share", {}).get("fouts_plant_units", 0))
                           if pm and "fouts_plant_units" in pm.get("smyrna_share", {}) else None,
        }

    _cache[cache_key] = (time.time(), result)
    return result
