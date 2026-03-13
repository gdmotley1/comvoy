"""Dashboard analytics endpoint — returns all data for the visual dashboard in one call."""

import logging
from collections import Counter, defaultdict

from fastapi import APIRouter, Query, HTTPException, Response

from app.database import get_service_client

router = APIRouter(prefix="/api", tags=["dashboard"])
logger = logging.getLogger(__name__)


def _latest_snapshot_id(db) -> str:
    result = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True).limit(1).execute()
    if not result.data:
        raise HTTPException(404, "No report snapshots found.")
    return result.data[0]["id"], result.data[0]["report_date"]


def _paginate_vehicles(db, snap_id, state=None):
    """Fetch all vehicles for a snapshot, paginating past 1000-row limit."""
    vehicles = []
    offset = 0
    page_size = 1000
    while True:
        if state:
            q = db.table("vehicles").select(
                "vin, brand, body_type, body_builder, price, condition, fuel_type, "
                "transmission, is_smyrna, dealer_id, dealers!inner(name, city, state)"
            ).eq("snapshot_id", snap_id).eq("dealers.state", state.upper())
        else:
            q = db.table("vehicles").select(
                "vin, brand, body_type, body_builder, price, condition, fuel_type, "
                "transmission, is_smyrna, dealer_id, dealers!inner(name, city, state)"
            ).eq("snapshot_id", snap_id)
        page = q.range(offset, offset + page_size - 1).execute()
        if not page.data:
            break
        vehicles.extend(page.data)
        if len(page.data) < page_size:
            break
        offset += page_size
    return vehicles


@router.get("/dashboard")
def get_dashboard(response: Response, state: str = Query(None, description="Filter by state code")):
    """Single endpoint returning all dashboard analytics."""
    response.headers["Cache-Control"] = "public, max-age=300"
    db = get_service_client()
    snap_id, snap_date = _latest_snapshot_id(db)

    # Fetch all vehicles (paginated)
    vehicles = _paginate_vehicles(db, snap_id, state)

    if not vehicles:
        raise HTTPException(404, "No vehicle data found for this filter.")

    # Aggregate from vehicle data
    prices = [v["price"] for v in vehicles if v["price"]]
    brand_counter = Counter()
    body_type_counter = Counter()
    builder_counter = Counter()
    condition_counter = Counter()
    fuel_counter = Counter()
    trans_counter = Counter()
    smyrna_count = 0
    smyrna_prices = []
    dealer_vehicles = defaultdict(int)
    dealer_info = {}

    for v in vehicles:
        brand_counter[v["brand"]] += 1
        body_type_counter[v["body_type"]] += 1
        if v["body_builder"]:
            builder_counter[v["body_builder"]] += 1
        if v["condition"]:
            condition_counter[v["condition"]] += 1
        if v["fuel_type"]:
            fuel_counter[v["fuel_type"]] += 1
        if v["transmission"]:
            trans_counter[v["transmission"]] += 1
        if v["is_smyrna"]:
            smyrna_count += 1
            if v["price"]:
                smyrna_prices.append(v["price"])
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
    median_price = prices[n // 2] if n else 0

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
        if v["price"]:
            bk = _bracket(v["price"])
            brackets[bk] += 1
            if v["body_type"]:
                bt_brackets[v["body_type"]][bk] += 1

    # Build per-body-type bracket data for top body types
    price_by_body_type = {}
    for bt, counts in sorted(bt_brackets.items(), key=lambda x: sum(x[1].values()), reverse=True)[:12]:
        bt_prices_list = [v["price"] for v in vehicles if v["price"] and v["body_type"] == bt]
        price_by_body_type[bt] = {
            "brackets": counts,
            "avg": round(sum(bt_prices_list) / len(bt_prices_list)) if bt_prices_list else 0,
            "count": len(bt_prices_list),
        }

    # Lead scores
    lead_query = db.table("lead_scores").select("tier, score, dealer_id")
    if state:
        lead_query = lead_query.eq("snapshot_id", snap_id)
        # Filter by state via dealer lookup
        dealer_ids_in_state = list(dealer_info.keys())
        if dealer_ids_in_state:
            lead_query = lead_query.in_("dealer_id", dealer_ids_in_state)
    else:
        lead_query = lead_query.eq("snapshot_id", snap_id)
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
            "score": ld.get("score", 0),
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

    # Condition price comparison
    new_prices = [v["price"] for v in vehicles if v["price"] and v.get("condition") == "New"]
    used_prices = [v["price"] for v in vehicles if v["price"] and v.get("condition") == "Used"]

    # Smyrna price position vs market by body type (New vs New only)
    bt_prices_all = defaultdict(list)
    bt_prices_smyrna = defaultdict(list)
    for v in vehicles:
        if v["price"] and v["body_type"] and v.get("condition") == "New":
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

    return {
        "snapshot_date": snap_date,
        "totals": {
            "vehicles": total,
            "dealers": len(dealer_vehicles),
            "avg_price": avg_price,
            "median_price": median_price,
            "smyrna_units": smyrna_count,
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
        "by_condition": {
            "New": {"count": condition_counter.get("New", 0), "avg_price": round(sum(new_prices) / len(new_prices)) if new_prices else 0},
            "Used": {"count": condition_counter.get("Used", 0), "avg_price": round(sum(used_prices) / len(used_prices)) if used_prices else 0},
        },
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
        "smyrna_intel": {
            "total_units": smyrna_count,
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
