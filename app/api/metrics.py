"""Snapshot metrics — pre-computed market KPIs for agent intelligence."""

import logging
from collections import Counter, defaultdict
from datetime import datetime

from app.database import get_service_client
from app.config import is_excluded_dealer

logger = logging.getLogger(__name__)




def _paginate(db, table, select, filters, page_size=1000):
    """Generic paginator for Supabase queries."""
    rows = []
    offset = 0
    while True:
        q = db.table(table).select(select)
        for col, val in filters:
            q = q.eq(col, val)
        page = q.range(offset, offset + page_size - 1).execute()
        if not page.data:
            break
        rows.extend(page.data)
        if len(page.data) < page_size:
            break
        offset += page_size
    return rows


def compute_snapshot_metrics(snapshot_id: str, prev_snapshot_id: str = None):
    """Compute all market KPIs for a snapshot and store in snapshot_metrics table.

    Call after load_vehicles + compute_diffs + compute_lead_scores.
    """
    db = get_service_client()
    logger.info(f"Computing snapshot metrics for {snapshot_id}")

    # ── Fetch vehicles ──────────────────────────────────────────────
    vehicles = _paginate(db, "vehicles",
        "vin, brand, body_type, body_builder, price, dealer_id, first_seen_date, is_smyrna, is_fouts, "
        "dealers!inner(name, city, state)",
        [("snapshot_id", snapshot_id), ("condition", "New")])
    vehicles = [v for v in vehicles if not is_excluded_dealer(v.get("dealers", {}).get("name", ""))]

    if not vehicles:
        logger.warning("No vehicles found — skipping metrics")
        return

    # ── Fetch snapshot date ─────────────────────────────────────────
    snap_row = db.table("report_snapshots").select("report_date").eq("id", snapshot_id).execute()
    snap_date = snap_row.data[0]["report_date"] if snap_row.data else None
    report_date = datetime.strptime(str(snap_date), "%Y-%m-%d").date() if snap_date else None

    # ── Build indexes ───────────────────────────────────────────────
    PRICE_FLOOR = 5000
    dealer_info = {}
    dealer_vehicles = defaultdict(list)
    bt_vehicles = defaultdict(list)
    brand_vehicles = defaultdict(list)
    state_vehicles = defaultdict(list)

    for v in vehicles:
        did = v["dealer_id"]
        dealer_vehicles[did].append(v)
        bt_vehicles[v["body_type"]].append(v)
        brand_vehicles[v["brand"]].append(v)
        if v.get("dealers"):
            state_vehicles[v["dealers"]["state"]].append(v)
            if did not in dealer_info:
                dealer_info[did] = {
                    "name": v["dealers"]["name"],
                    "city": v["dealers"]["city"],
                    "state": v["dealers"]["state"],
                }

    total = len(vehicles)

    # ── Fetch diffs ─────────────────────────────────────────────────
    diffs = []
    if prev_snapshot_id:
        diffs = _paginate(db, "vehicle_diffs",
            "diff_type, vin, dealer_id, brand, body_type, old_price, new_price",
            [("snapshot_id", snapshot_id)])

    diff_by_type = defaultdict(list)
    diff_by_dealer = defaultdict(lambda: {"sold": 0, "added": 0, "price_changes": 0})
    diff_by_bt = defaultdict(lambda: {"sold": 0, "added": 0, "price_changes": 0})
    diff_by_state = defaultdict(lambda: {"sold": 0, "added": 0, "price_changes": 0})

    for d in diffs:
        diff_by_type[d["diff_type"]].append(d)
        did = d.get("dealer_id")
        bt = d.get("body_type")
        if did:
            diff_by_dealer[did][{"new": "added", "sold": "sold", "price_change": "price_changes"}[d["diff_type"]]] += 1
            # Map dealer to state
            if did in dealer_info:
                st = dealer_info[did]["state"]
                diff_by_state[st][{"new": "added", "sold": "sold", "price_change": "price_changes"}[d["diff_type"]]] += 1
        if bt:
            diff_by_bt[bt][{"new": "added", "sold": "sold", "price_change": "price_changes"}[d["diff_type"]]] += 1

    sold_count = len(diff_by_type.get("sold", []))
    added_count = len(diff_by_type.get("new", []))
    price_change_count = len(diff_by_type.get("price_change", []))

    # ── Fetch previous snapshot vehicle counts per dealer (for growth) ──
    prev_dealer_counts = {}
    if prev_snapshot_id:
        prev_vehicles = _paginate(db, "vehicles",
            "dealer_id, dealers!inner(name)",
            [("snapshot_id", prev_snapshot_id)])
        prev_vehicles = [v for v in prev_vehicles if not is_excluded_dealer(v.get("dealers", {}).get("name", ""))]
        for v in prev_vehicles:
            prev_dealer_counts[v["dealer_id"]] = prev_dealer_counts.get(v["dealer_id"], 0) + 1

    # ── Fetch lead scores ───────────────────────────────────────────
    leads = db.table("lead_scores").select("dealer_id, score, tier").eq("snapshot_id", snapshot_id).execute().data or []
    lead_by_dealer = {l["dealer_id"]: l for l in leads}

    # ── METRICS ─────────────────────────────────────────────────────
    metrics = {}

    # 1. Sell-through rate by body type
    sell_through = {}
    for bt, vlist in bt_vehicles.items():
        bt_inv = len(vlist)
        bt_sold = diff_by_bt.get(bt, {}).get("sold", 0)
        if bt_inv > 0:
            sell_through[bt] = {
                "inventory": bt_inv,
                "sold": bt_sold,
                "rate": round(bt_sold / bt_inv * 100, 1),
            }
    metrics["sell_through_by_body_type"] = dict(sorted(sell_through.items(), key=lambda x: x[1]["rate"], reverse=True))

    # 2. Market absorption rate (sold / added)
    absorption = {"overall": round(sold_count / added_count, 2) if added_count else None}
    absorption["by_state"] = {}
    for st, dd in diff_by_state.items():
        if dd["added"] > 0:
            absorption["by_state"][st] = round(dd["sold"] / dd["added"], 2)
    absorption["by_body_type"] = {}
    for bt, dd in diff_by_bt.items():
        if dd["added"] > 0:
            absorption["by_body_type"][bt] = round(dd["sold"] / dd["added"], 2)
    absorption["interpretation"] = (
        "tightening" if absorption["overall"] and absorption["overall"] > 1.0
        else "oversupply" if absorption["overall"] and absorption["overall"] < 0.8
        else "balanced"
    )
    metrics["absorption_rate"] = absorption

    # 3. Dealer growth score
    dealer_growth = []
    for did, vlist in dealer_vehicles.items():
        info = dealer_info.get(did)
        if not info:
            continue
        current = len(vlist)
        previous = prev_dealer_counts.get(did)
        if previous and previous > 0:
            pct = round((current - previous) / previous * 100, 1)
        elif previous == 0 or previous is None:
            pct = None  # new dealer or no prior data
        else:
            pct = 0
        ld = lead_by_dealer.get(did, {})
        row = {
            "name": info["name"], "city": info["city"], "state": info["state"],
            "current_inv": current,
            "prev_inv": previous,
            "growth_pct": pct,
            "net_change": current - (previous or current),
            "score": ld.get("score"), "tier": ld.get("tier"),
        }
        dealer_growth.append(row)
    # Sort by growth % (biggest growers first), nulls last
    dealer_growth.sort(key=lambda x: x["growth_pct"] if x["growth_pct"] is not None else -999, reverse=True)
    metrics["dealer_growth"] = {
        "top_growers": dealer_growth[:15],
        "top_shrinkers": sorted([d for d in dealer_growth if d["growth_pct"] is not None], key=lambda x: x["growth_pct"])[:15],
    }

    # 4. Stale inventory %
    if report_date:
        stale_30 = sum(1 for v in vehicles if v.get("first_seen_date") and
            (report_date - datetime.strptime(str(v["first_seen_date"]), "%Y-%m-%d").date()).days > 30)
        stale_60 = sum(1 for v in vehicles if v.get("first_seen_date") and
            (report_date - datetime.strptime(str(v["first_seen_date"]), "%Y-%m-%d").date()).days > 60)
        metrics["stale_inventory"] = {
            "total": total,
            "over_30d": stale_30,
            "over_60d": stale_60,
            "pct_over_30d": round(stale_30 / total * 100, 1) if total else 0,
            "pct_over_60d": round(stale_60 / total * 100, 1) if total else 0,
        }

    # 5. Price pressure by body type
    price_pressure = {}
    for d in diff_by_type.get("price_change", []):
        bt = d.get("body_type")
        if bt and d.get("old_price") and d.get("new_price"):
            if bt not in price_pressure:
                price_pressure[bt] = {"drops": 0, "increases": 0, "total_drop_pct": 0, "count": 0}
            change_pct = (d["new_price"] - d["old_price"]) / d["old_price"] * 100
            price_pressure[bt]["count"] += 1
            if d["new_price"] < d["old_price"]:
                price_pressure[bt]["drops"] += 1
            else:
                price_pressure[bt]["increases"] += 1
            price_pressure[bt]["total_drop_pct"] += change_pct
    for bt, pp in price_pressure.items():
        pp["avg_change_pct"] = round(pp["total_drop_pct"] / pp["count"], 1) if pp["count"] else 0
        del pp["total_drop_pct"]
    metrics["price_pressure"] = price_pressure

    # 6. Smyrna share trend (Smyrna-bodied trucks at third-party dealers)
    smyrna_units = sum(1 for v in vehicles if v.get("is_smyrna"))
    fouts_units = sum(1 for v in vehicles if v.get("is_fouts"))
    our_units = smyrna_units + fouts_units
    smyrna_by_dealer = defaultdict(int)
    for v in vehicles:
        if v.get("is_smyrna"):
            smyrna_by_dealer[v["dealer_id"]] += 1
    smyrna_share = {
        "total_units": smyrna_units,
        "fouts_plant_units": fouts_units,
        "our_total_units": our_units,
        "market_pct": round(smyrna_units / total * 100, 2) if total else 0,
        "our_market_pct": round(our_units / total * 100, 2) if total else 0,
        "dealer_count": len(smyrna_by_dealer),
        "top_dealers": [
            {"name": dealer_info.get(did, {}).get("name", ""), "units": cnt}
            for did, cnt in sorted(smyrna_by_dealer.items(), key=lambda x: x[1], reverse=True)[:10]
        ],
    }
    metrics["smyrna_share"] = smyrna_share

    # 7. New / lost dealers
    if prev_snapshot_id:
        current_ids = set(dealer_vehicles.keys())
        prev_ids = set(prev_dealer_counts.keys())
        new_ids = current_ids - prev_ids
        lost_ids = prev_ids - current_ids
        metrics["new_dealers"] = [
            {"name": dealer_info.get(did, {}).get("name", ""), "state": dealer_info.get(did, {}).get("state", ""),
             "inventory": len(dealer_vehicles.get(did, []))}
            for did in new_ids if did in dealer_info
        ]
        metrics["lost_dealers"] = [did for did in lost_ids]  # IDs only — names gone from current data
        metrics["dealer_churn"] = {
            "new": len(new_ids), "lost": len(lost_ids),
            "net": len(new_ids) - len(lost_ids),
        }

    # 8. Hot market segments — body types where sold > added
    hot_segments = []
    for bt, dd in diff_by_bt.items():
        if dd["sold"] > dd["added"] and dd["sold"] > 0:
            hot_segments.append({
                "body_type": bt,
                "sold": dd["sold"], "added": dd["added"],
                "deficit": dd["sold"] - dd["added"],
                "inventory": len(bt_vehicles.get(bt, [])),
            })
    hot_segments.sort(key=lambda x: x["deficit"], reverse=True)
    metrics["hot_segments"] = hot_segments

    # 9. Summary
    metrics["summary"] = {
        "snapshot_date": str(snap_date),
        "prev_snapshot_date": None,
        "total_vehicles": total,
        "total_dealers": len(dealer_vehicles),
        "sold": sold_count,
        "added": added_count,
        "price_changes": price_change_count,
        "net_change": added_count - sold_count,
    }
    if prev_snapshot_id:
        prev_row = db.table("report_snapshots").select("report_date").eq("id", prev_snapshot_id).execute()
        if prev_row.data:
            metrics["summary"]["prev_snapshot_date"] = str(prev_row.data[0]["report_date"])

    # ── Store ────────────────────────────────────────────────────────
    db.table("snapshot_metrics").upsert({
        "snapshot_id": snapshot_id,
        "metrics": metrics,
    }, on_conflict="snapshot_id").execute()

    logger.info(f"Snapshot metrics computed: {len(metrics)} categories, "
                f"{total} vehicles, {sold_count} sold, {added_count} added")
    return metrics
