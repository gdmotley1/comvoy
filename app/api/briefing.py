"""Auto-briefing engine — generates route intel + sends email to reps.

Fired as a BackgroundTask when a travel plan is created or updated.
Zero Claude API cost — all data comes from existing DB queries.

Pipeline:
  1. find_dealers_along_route (PostGIS corridor search)
  2. Join with dealer_snapshots + lead_scores + factors
  3. Diff body types vs previous month (what's growing/shrinking?)
  4. Render HTML email with inline CSS
  5. Send via SMTP
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from app.database import get_service_client
from app.config import settings
from app.api.scoring import SMYRNA_BODY_TYPES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Generate route briefing data
# ---------------------------------------------------------------------------

def generate_route_briefing(plan: dict) -> dict:
    """Build structured briefing for a travel plan.

    Returns: {dealers: [...], summary: {...}, has_trends: bool}
    """
    db = get_service_client()

    # Auto-backfill polyline if missing (sync — runs in BackgroundTask)
    if not plan.get("route_polyline"):
        try:
            from app.etl.routing import get_driving_route_sync
            route_wkt = get_driving_route_sync(
                plan["start_lat"], plan["start_lng"],
                plan["end_lat"], plan["end_lng"],
            )
            if route_wkt:
                plan["route_polyline"] = route_wkt
                if plan.get("id"):
                    db.table("rep_travel_plans").update(
                        {"route_polyline": route_wkt}
                    ).eq("id", plan["id"]).execute()
                    logger.info(f"Backfilled polyline for plan {plan['id']}")
        except Exception as e:
            logger.warning(f"Polyline backfill failed in briefing: {e}")

    # Find dealers along route (real driving route if polyline exists)
    rpc_params = {
        "p_start_lat": plan["start_lat"],
        "p_start_lng": plan["start_lng"],
        "p_end_lat": plan["end_lat"],
        "p_end_lng": plan["end_lng"],
        "p_buffer_miles": 20,
    }
    if plan.get("route_polyline"):
        rpc_params["p_polyline_wkt"] = plan["route_polyline"]

    route_dealers = db.rpc("find_dealers_along_route", rpc_params).execute()

    if not route_dealers.data:
        return {"dealers": [], "summary": {"total": 0}, "has_trends": False}

    dealer_ids = [r["dealer_id"] for r in route_dealers.data]
    pos_map = {r["dealer_id"]: r.get("route_position", 0) for r in route_dealers.data}
    dist_map = {r["dealer_id"]: r["distance_miles"] for r in route_dealers.data}

    # Get latest 2 snapshots for trend data
    snaps = db.table("report_snapshots").select("id, report_date").order(
        "report_date", desc=True
    ).limit(2).execute()

    snap_id = snaps.data[0]["id"] if snaps.data else None
    prev_snap_id = snaps.data[1]["id"] if len(snaps.data) > 1 else None
    has_trends = prev_snap_id is not None

    # Get inventory data
    inv_map = {}
    if snap_id:
        inv = db.table("dealer_snapshots").select(
            "dealer_id, total_vehicles, smyrna_units, smyrna_percentage, rank, top_brand"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        inv_map = {r["dealer_id"]: r for r in inv.data}

    # Get lead scores + factors
    score_map = {}
    if snap_id:
        scores = db.table("lead_scores").select(
            "dealer_id, score, tier, opportunity_type, factors"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        score_map = {r["dealer_id"]: r for r in scores.data}

    # Get cached Google Places data (single query, no API calls)
    places_map = {}
    try:
        places_data = db.table("dealer_places").select(
            "dealer_id, rating, review_count, phone, website, google_maps_url"
        ).in_("dealer_id", dealer_ids).execute()
        places_map = {r["dealer_id"]: r for r in (places_data.data or [])}
    except Exception:
        pass  # Table may not exist yet

    # Get body type breakdown for all route dealers (current snapshot)
    bt_current = {}
    if snap_id:
        bt_data = db.table("dealer_body_type_inventory").select(
            "dealer_id, vehicle_count, body_types(name)"
        ).eq("snapshot_id", snap_id).in_("dealer_id", dealer_ids).execute()
        for row in bt_data.data:
            did = row["dealer_id"]
            bt_name = row["body_types"]["name"]
            bt_current.setdefault(did, {})[bt_name] = row["vehicle_count"]

    # Get previous snapshot body types for MoM diff
    bt_previous = {}
    if prev_snap_id:
        bt_prev_data = db.table("dealer_body_type_inventory").select(
            "dealer_id, vehicle_count, body_types(name)"
        ).eq("snapshot_id", prev_snap_id).in_("dealer_id", dealer_ids).execute()
        for row in bt_prev_data.data:
            did = row["dealer_id"]
            bt_name = row["body_types"]["name"]
            bt_previous.setdefault(did, {})[bt_name] = row["vehicle_count"]

    # Build enriched dealer list
    dealers = []
    for rd in route_dealers.data:
        did = rd["dealer_id"]
        inv = inv_map.get(did, {})
        sc = score_map.get(did, {})
        factors = sc.get("factors", {}) or {}
        curr_bts = bt_current.get(did, {})
        prev_bts = bt_previous.get(did, {})

        # Compute body type MoM changes (significant only)
        bt_changes = _compute_bt_changes(curr_bts, prev_bts) if has_trends else []

        # Smyrna-compatible body types at this dealer
        smyrna_overlap = {bt: ct for bt, ct in curr_bts.items() if bt in SMYRNA_BODY_TYPES}

        dealer = {
            "dealer_id": did,
            "name": rd["dealer_name"],
            "city": rd["city"],
            "state": rd["state"],
            "route_position": round(pos_map.get(did, 0), 3),
            "dist_miles": round(dist_map.get(did, 0), 1),
            "vehicles": inv.get("total_vehicles", 0),
            "smyrna_units": inv.get("smyrna_units", 0),
            "smyrna_pct": inv.get("smyrna_percentage", 0),
            "rank": inv.get("rank"),
            "top_brand": inv.get("top_brand"),
            "score": sc.get("score"),
            "tier": sc.get("tier"),
            "opportunity": sc.get("opportunity_type"),
            "factors": factors,
            "smyrna_overlap": smyrna_overlap,
            "bt_changes": bt_changes,
            "places": places_map.get(did, {}),
        }
        dealers.append(dealer)

    # Smart selection: pick top dealers by score, then display in route order.
    # This ensures a long route (e.g. Orlando→Nashville) shows the BEST
    # opportunities from FL, GA, and TN — not just the first 15 near the start.
    dealers.sort(key=lambda x: -(x.get("score") or 0))
    top_pool = dealers[:20]  # Top 20 by opportunity score
    top_pool.sort(key=lambda x: x["route_position"])  # Re-sort in travel order

    # Split: Top 5 get full-detail cards, next 15 get compact rows
    top_stops = top_pool[:5]
    also_on_route = top_pool[5:]

    # Summary stats (across all selected dealers)
    all_selected = top_pool
    hot_count = sum(1 for d in all_selected if d["tier"] == "hot")
    ws_count = sum(1 for d in all_selected if d["opportunity"] == "whitespace")
    total_route_dealers = len(route_dealers.data)  # total before selection

    return {
        "top_stops": top_stops,
        "also_on_route": also_on_route,
        "dealers": all_selected,  # backward compat
        "summary": {
            "total": len(all_selected),
            "total_on_route": total_route_dealers,
            "hot": hot_count,
            "whitespace": ws_count,
        },
        "has_trends": has_trends,
    }


def _compute_bt_changes(current: dict, previous: dict) -> list[dict]:
    """Diff body type counts between two snapshots.

    Only surfaces changes that are ≥3 units AND ≥30% change to avoid noise.
    Returns list of {name, prev, curr, delta, pct_change, direction}.
    """
    changes = []
    all_types = set(current.keys()) | set(previous.keys())

    for bt in all_types:
        curr = current.get(bt, 0)
        prev = previous.get(bt, 0)
        delta = curr - prev

        if delta == 0:
            continue

        # Threshold: ≥3 units change
        if abs(delta) < 3:
            continue

        # Threshold: ≥30% change (avoid div by zero)
        pct = (delta / prev * 100) if prev > 0 else 100.0
        if abs(pct) < 30 and prev > 0:
            continue

        changes.append({
            "name": bt,
            "prev": prev,
            "curr": curr,
            "delta": delta,
            "pct": round(pct),
            "direction": "up" if delta > 0 else "down",
            "smyrna_relevant": bt in SMYRNA_BODY_TYPES,
        })

    # Sort: Smyrna-relevant first, then by absolute delta descending
    changes.sort(key=lambda x: (-x["smyrna_relevant"], -abs(x["delta"])))
    return changes[:5]  # Top 5 most significant changes


# ---------------------------------------------------------------------------
# 2. Render HTML email
# ---------------------------------------------------------------------------

def _build_why_visit(d: dict) -> str:
    """Build a plain-English 'why visit' sentence for a dealer."""
    opp = d.get("opportunity")
    vehicles = d.get("vehicles", 0)
    smyrna = d.get("smyrna_units", 0)
    match_pct = (d.get("factors") or {}).get("match_pct", 0) or 0
    growth = (d.get("factors") or {}).get("growth_pct")
    top_brand = d.get("top_brand", "")
    overlap = d.get("smyrna_overlap", {})
    overlap_units = sum(overlap.values()) if overlap else 0

    if opp == "whitespace" and overlap_units > 0:
        return (f"Carries {overlap_units} trucks in body types Smyrna builds, "
                f"but zero Smyrna product on the lot. Untapped opportunity.")
    elif opp == "whitespace":
        return (f"{vehicles:,} vehicles on lot, top brand {top_brand}. "
                f"No Smyrna product today.")
    elif opp == "at_risk":
        return ("Had Smyrna product last month but dropped to zero. "
                "Worth a check-in to find out what happened.")
    elif smyrna > 0 and match_pct >= 50:
        s_pct = round(smyrna / max(vehicles, 1) * 100)
        return (f"Currently {smyrna} Smyrna units ({s_pct}% of lot). "
                f"{match_pct}% of inventory is in types we build "
                f"-- room to grow.")
    elif smyrna > 0:
        s_pct = round(smyrna / max(vehicles, 1) * 100)
        return (f"Stocks {smyrna} Smyrna units ({s_pct}% penetration). "
                f"Top brand: {top_brand}.")
    elif isinstance(growth, (int, float)) and growth > 15:
        return (f"Inventory up {growth}% month-over-month. "
                f"Growing fast with {vehicles:,} vehicles.")
    else:
        return f"{vehicles:,} vehicles on lot. Top brand: {top_brand}."


def _render_places_line(places: dict) -> str:
    """Render a compact Places info line for email (rating + phone + website)."""
    if not places:
        return ""
    parts = []
    if places.get("rating"):
        stars = f"&#11088; {float(places['rating'])}"
        if places.get("review_count"):
            stars += f" ({places['review_count']} reviews)"
        parts.append(stars)
    if places.get("phone"):
        parts.append(f"&#9742; {places['phone']}")
    if places.get("website"):
        # Truncate long URLs for display
        url = places["website"]
        display = url.replace("https://", "").replace("http://", "").rstrip("/")
        if len(display) > 30:
            display = display[:28] + "..."
        parts.append(
            f'<a href="{url}" style="color:#4f8fff;text-decoration:none;">{display}</a>'
        )
    if not parts:
        return ""
    return f"""
        <tr><td style="padding:4px 20px 0 20px;font-size:12px;color:#8b95a5;line-height:1.5;">
            {' &middot; '.join(parts)}
        </td></tr>"""


def _render_top_stop(d: dict, show_trends: bool) -> str:
    """Render a full-detail card for a top-stop dealer.

    Single-column layout — every row is one <td> spanning full width.
    No multi-column tricks that could break on mobile.
    """
    tier = d.get("tier", "cold")
    tier_label = {"hot": "HIGH PRIORITY", "warm": "OPPORTUNITY", "cold": "MONITOR"}.get(tier, "")
    tier_bg = {"hot": "#ff6b35", "warm": "#22c55e", "cold": "#475569"}.get(tier, "#475569")
    why = _build_why_visit(d)

    score = d.get("score") or 0
    dist_text = f"{d['dist_miles']} mi off route"
    rank_text = f" &middot; #{d['rank']} in territory" if d.get("rank") else ""

    # Stats line
    smyrna_text = ""
    if d.get("smyrna_units", 0) > 0:
        smyrna_text = f" &middot; {d['smyrna_units']} Smyrna units ({d.get('smyrna_pct', 0)}%)"

    # Score factor breakdown
    factors = d.get("factors") or {}
    factor_parts = []
    if factors.get("inventory_size"):
        factor_parts.append(f"Size {factors['inventory_size']}/30")
    if factors.get("body_type_match") is not None:
        factor_parts.append(f"Match {factors['body_type_match']}/30")
    if factors.get("smyrna_opportunity") is not None:
        factor_parts.append(f"Opp. {factors['smyrna_opportunity']}/25")
    if factors.get("growth_momentum") is not None:
        factor_parts.append(f"Growth {factors['growth_momentum']}/15")
    score_line = f"Score: {score}/100 ({' + '.join(factor_parts)})" if factor_parts else f"Score: {score}/100"

    # Body types we build that they carry
    overlap_html = ""
    if d.get("smyrna_overlap"):
        items = [f"{bt} ({ct})" for bt, ct in sorted(
            d["smyrna_overlap"].items(), key=lambda x: -x[1]
        )[:4]]
        if items:
            overlap_html = f"""
            <tr><td style="padding:4px 20px 0 20px;font-size:12px;color:#8b95a5;line-height:1.5;">
                <strong style="color:#9ca3af;">Types we build on their lot:</strong> {', '.join(items)}
            </td></tr>"""

    # MoM changes
    changes_html = ""
    if show_trends and d.get("bt_changes"):
        parts = []
        for ch in d["bt_changes"][:3]:
            arrow = "up" if ch["delta"] > 0 else "down"
            parts.append(f"{ch['name']} {arrow} ({ch['prev']} to {ch['curr']})")
        if parts:
            changes_html = f"""
            <tr><td style="padding:4px 20px 0 20px;font-size:12px;color:#8b95a5;line-height:1.5;">
                <strong style="color:#9ca3af;">Month-over-month:</strong> {' / '.join(parts)}
            </td></tr>"""

    return f"""
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
           style="border-collapse:collapse;">
        <!-- Tier badge + meta -->
        <tr><td style="padding:16px 20px 0 20px;">
            <table role="presentation" cellpadding="0" cellspacing="0"><tr>
                <td style="background:{tier_bg};color:#fff;font-size:10px;
                    font-weight:700;letter-spacing:0.8px;padding:3px 8px;
                    border-radius:3px;mso-line-height-rule:exactly;line-height:16px;">{tier_label}</td>
                <td style="padding-left:10px;font-size:11px;color:#64748b;">
                    {dist_text}{rank_text}
                </td>
            </tr></table>
        </td></tr>
        <!-- Dealer name -->
        <tr><td style="padding:8px 20px 0 20px;">
            <span style="font-size:16px;font-weight:700;color:#e2e8f0;">{d['name']}</span><br>
            <span style="font-size:13px;color:#64748b;">{d['city']}, {d['state']}</span>
        </td></tr>
        {_render_places_line(d.get('places', {}))}
        <!-- Why visit -->
        <tr><td style="padding:8px 20px 0 20px;font-size:14px;color:#b0bac7;line-height:1.55;">
            {why}
        </td></tr>
        <!-- Quick stats -->
        <tr><td style="padding:8px 20px 0 20px;font-size:13px;color:#8b95a5;">
            {d['vehicles']:,} vehicles &middot; Top brand: {d.get('top_brand', 'N/A')}{smyrna_text}
        </td></tr>
        <!-- Score breakdown -->
        <tr><td style="padding:4px 20px 0 20px;font-size:11px;color:#64748b;">
            {score_line}
        </td></tr>
        {overlap_html}
        {changes_html}
        <!-- Card bottom border -->
        <tr><td style="padding:16px 20px 0 20px;">
            <table role="presentation" width="100%" style="border-collapse:collapse;">
                <tr><td style="border-bottom:1px solid #1e293b;font-size:1px;line-height:1px;">&nbsp;</td></tr>
            </table>
        </td></tr>
    </table>"""


def _render_compact_row(d: dict) -> str:
    """Render a compact row for an 'also on route' dealer.

    Single-column: all info in one <td> to prevent overlap on mobile.
    Two lines: name/location on top, score + reason below in muted text.
    """
    tier = d.get("tier", "cold")
    tier_color = {"hot": "#ff6b35", "warm": "#22c55e", "cold": "#475569"}.get(tier, "#475569")

    score = d.get("score") or 0
    opp = d.get("opportunity", "")
    smyrna = d.get("smyrna_units", 0)

    # Short reason
    if opp == "whitespace":
        reason = "Whitespace"
    elif opp == "at_risk":
        reason = "Dropped Smyrna"
    elif smyrna > 0:
        reason = f"{smyrna} Smyrna units"
    else:
        reason = f"{d.get('vehicles', 0):,} vehicles"

    return f"""
    <tr><td style="padding:8px 20px;border-bottom:1px solid #111827;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
               style="border-collapse:collapse;">
            <tr>
                <td style="font-size:13px;color:#e2e8f0;padding:0;">
                    <span style="color:{tier_color};font-size:8px;line-height:1;">&#9632;</span>&nbsp;
                    <strong>{d['name']}</strong>
                    <span style="color:#64748b;font-size:12px;">&nbsp;&mdash; {d['city']}, {d['state']}</span>
                </td>
                <td style="font-size:12px;color:#64748b;padding:0;white-space:nowrap;" align="right" width="100">
                    {score}/100 &middot; {reason}
                </td>
            </tr>
        </table>
    </td></tr>"""


def _divider() -> str:
    """Reusable horizontal divider for email sections."""
    return """
    <tr><td style="padding:0 20px;">
        <table role="presentation" width="100%" style="border-collapse:collapse;">
            <tr><td style="border-bottom:1px solid #1e293b;font-size:1px;line-height:1px;">&nbsp;</td></tr>
        </table>
    </td></tr>"""


def render_briefing_email(rep_name: str, plan: dict, briefing: dict) -> str:
    """Render polished 3-section email with Otto branding.

    Structure:
      1. Otto header (branded)
      2. Metrics bar (4 KPIs)
      3. Executive Summary (paragraph)
      4. Top Stops (5 full-detail cards)
      5. Also On Route (compact rows)
      6. Scoring Key (how ratings work)
      7. Footer

    100% table-based HTML, inline CSS, no flexbox/grid.
    Tested for Gmail, Outlook, Apple Mail.
    """
    top_stops = briefing.get("top_stops", [])
    also_on_route = briefing.get("also_on_route", [])
    all_dealers = briefing.get("dealers", [])
    summary = briefing["summary"]
    has_trends = briefing["has_trends"]
    travel_date = plan.get("travel_date", "")

    try:
        dt = datetime.strptime(str(travel_date), "%Y-%m-%d")
        date_str = dt.strftime("%B %d, %Y")
        date_short = dt.strftime("%b %d")
        day_of_week = dt.strftime("%A")
    except (ValueError, TypeError):
        date_str = str(travel_date)
        date_short = date_str
        day_of_week = ""

    start = plan.get("start_location", "?")
    end = plan.get("end_location", "?")

    # ── Compute summary stats ──
    total_vehicles = sum(d.get("vehicles", 0) for d in all_dealers)
    total_smyrna = sum(d.get("smyrna_units", 0) for d in all_dealers)
    smyrna_pct = round(total_smyrna / max(total_vehicles, 1) * 100, 1)
    hot_dealers = [d for d in all_dealers if d.get("tier") == "hot"]
    ws_dealers = [d for d in all_dealers if d.get("opportunity") == "whitespace"]
    at_risk = [d for d in all_dealers if d.get("opportunity") == "at_risk"]
    total_on_route = summary.get("total_on_route", summary["total"])

    # ── Executive Summary (paragraph, not data dump) ──
    exec_parts = []
    exec_parts.append(
        f"Your {day_of_week} route from {start} to {end} passes within "
        f"20 miles of <strong>{total_on_route} dealers</strong> carrying "
        f"<strong>{total_vehicles:,} vehicles</strong>."
    )
    if total_on_route > summary["total"]:
        exec_parts.append(
            f" We've surfaced the <strong>top {summary['total']}</strong> "
            f"by opportunity score."
        )
    if hot_dealers:
        names = ", ".join(d["name"] for d in hot_dealers[:3])
        exec_parts.append(
            f"<br><br><strong>{len(hot_dealers)} high-priority "
            f"stop{'s' if len(hot_dealers) != 1 else ''}:</strong> {names}."
        )
    if ws_dealers:
        exec_parts.append(
            f" {len(ws_dealers)} carry body types Smyrna builds but stock "
            f"zero Smyrna product today -- fresh whitespace."
        )
    if at_risk:
        names = " and ".join(d["name"] for d in at_risk[:2])
        exec_parts.append(
            f" Heads up: {names} dropped Smyrna since last month."
        )
    if smyrna_pct > 0:
        exec_parts.append(
            f" Current Smyrna penetration along this corridor: {smyrna_pct}%."
        )

    exec_html = "".join(exec_parts)

    # ── Top Stops section (full detail cards) ──
    top_stops_html = ""
    if top_stops:
        for d in top_stops:
            top_stops_html += _render_top_stop(d, has_trends)
    else:
        top_stops_html = """
        <table role="presentation" width="100%"><tr>
            <td style="padding:24px 20px;color:#64748b;font-size:14px;">
                No dealers found along this route.
            </td>
        </tr></table>"""

    # ── Also On Your Route section (compact rows) ──
    also_html = ""
    if also_on_route:
        compact_rows = "".join(_render_compact_row(d) for d in also_on_route)
        also_html = f"""
    {_divider()}
    <tr><td style="padding:20px 20px 10px 20px;">
        <span style="font-size:11px;font-weight:700;letter-spacing:1px;
            color:#94a3b8;text-transform:uppercase;">ALSO ON YOUR ROUTE</span>
        <span style="font-size:11px;color:#475569;">
            &nbsp;&mdash; {len(also_on_route)} more dealers</span>
    </td></tr>
    <tr><td style="padding:0;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
               style="border-collapse:collapse;">
            {compact_rows}
        </table>
    </td></tr>"""

    # ── Remaining dealers note ──
    remaining_note = ""
    if total_on_route > summary["total"]:
        remaining = total_on_route - summary["total"]
        remaining_note = f"""
    <tr><td style="padding:12px 20px;font-size:12px;color:#475569;">
        {remaining} additional dealer{'s' if remaining != 1 else ''} on this route
        available in Otto.
    </td></tr>"""

    # ── Trends note ──
    trends_note = ""
    if not has_trends:
        trends_note = """
    <tr><td style="padding:8px 20px;font-size:11px;color:#475569;">
        Month-over-month trends available after 2+ monthly data uploads.
    </td></tr>"""

    # ── Scoring Key (stacked rows — no multi-column overlap risk) ──
    scoring_key = f"""
    {_divider()}
    <tr><td style="padding:20px 20px 8px 20px;">
        <span style="font-size:11px;font-weight:700;letter-spacing:1px;
            color:#94a3b8;text-transform:uppercase;">HOW SCORING WORKS</span>
    </td></tr>
    <tr><td style="padding:4px 20px 0 20px;font-size:13px;color:#8b95a5;line-height:1.6;">
        Each dealer is scored 0&ndash;100 based on four factors:
    </td></tr>
    <tr><td style="padding:8px 20px 0 20px;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
               style="border-collapse:collapse;">
            <tr>
                <td width="50%" style="padding:6px 8px 6px 0;font-size:12px;color:#94a3b8;
                    border-bottom:1px solid #111827;vertical-align:top;">
                    <strong style="color:#e2e8f0;">Inventory Size</strong><br>
                    <span style="font-size:11px;color:#64748b;">0&ndash;30 pts &middot; Larger lot = more opportunity</span>
                </td>
                <td width="50%" style="padding:6px 0 6px 8px;font-size:12px;color:#94a3b8;
                    border-bottom:1px solid #111827;vertical-align:top;">
                    <strong style="color:#e2e8f0;">Body Type Match</strong><br>
                    <span style="font-size:11px;color:#64748b;">0&ndash;30 pts &middot; % of trucks in types we build</span>
                </td>
            </tr>
            <tr>
                <td width="50%" style="padding:6px 8px 6px 0;font-size:12px;color:#94a3b8;
                    vertical-align:top;">
                    <strong style="color:#e2e8f0;">Smyrna Opportunity</strong><br>
                    <span style="font-size:11px;color:#64748b;">0&ndash;25 pts &middot; Whitespace &gt; Low pen. &gt; Established</span>
                </td>
                <td width="50%" style="padding:6px 0 6px 8px;font-size:12px;color:#94a3b8;
                    vertical-align:top;">
                    <strong style="color:#e2e8f0;">Growth Momentum</strong><br>
                    <span style="font-size:11px;color:#64748b;">0&ndash;15 pts &middot; Inventory growing MoM</span>
                </td>
            </tr>
        </table>
    </td></tr>
    <tr><td style="padding:12px 20px 0 20px;">
        <table role="presentation" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse;">
            <tr>
                <td style="padding:0 12px 0 0;">
                    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                        <tr><td style="background:#ff6b35;color:#fff;font-size:9px;
                            font-weight:700;letter-spacing:0.5px;padding:3px 7px;
                            mso-line-height-rule:exactly;line-height:14px;">HIGH PRIORITY</td></tr>
                    </table>
                    <span style="font-size:11px;color:#64748b;">70&ndash;100</span>
                </td>
                <td style="padding:0 12px 0 0;">
                    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                        <tr><td style="background:#22c55e;color:#fff;font-size:9px;
                            font-weight:700;letter-spacing:0.5px;padding:3px 7px;
                            mso-line-height-rule:exactly;line-height:14px;">OPPORTUNITY</td></tr>
                    </table>
                    <span style="font-size:11px;color:#64748b;">40&ndash;69</span>
                </td>
                <td style="padding:0;">
                    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                        <tr><td style="background:#475569;color:#fff;font-size:9px;
                            font-weight:700;letter-spacing:0.5px;padding:3px 7px;
                            mso-line-height-rule:exactly;line-height:14px;">MONITOR</td></tr>
                    </table>
                    <span style="font-size:11px;color:#64748b;">0&ndash;39</span>
                </td>
            </tr>
        </table>
    </td></tr>"""

    # ── Assemble full email ──
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Route Briefing &mdash; {date_short}</title>
<!--[if mso]>
<style>table,td {{font-family:Arial,Helvetica,sans-serif !important;}}</style>
<![endif]-->
</head>
<body style="margin:0;padding:0;background-color:#0c0c18;
    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
    -webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;">

<!-- Full-width background wrapper -->
<table role="presentation" cellpadding="0" cellspacing="0" width="100%"
       style="background-color:#0c0c18;">
<tr><td align="center" style="padding:0;">

<!-- Main container: 600px max -->
<table role="presentation" cellpadding="0" cellspacing="0" width="600"
       style="max-width:600px;width:100%;border-collapse:collapse;
              background-color:#0c0c18;">

    <!-- ═══════ OTTO HEADER ═══════ -->
    <!-- Blue accent bar at very top -->
    <tr><td style="background:linear-gradient(90deg,#2563eb,#4f8fff,#2563eb);
        height:3px;font-size:1px;line-height:1px;" bgcolor="#3b82f6">&nbsp;</td></tr>

    <tr><td align="center" style="padding:32px 20px 8px 20px;">
        <!-- Otto wordmark — large blue text matching site identity -->
        <span style="font-size:36px;font-weight:700;color:#4f8fff;
            letter-spacing:-1px;line-height:1;">Otto</span>
    </td></tr>
    <tr><td align="center" style="padding:0 20px 24px 20px;">
        <span style="font-size:11px;font-weight:500;letter-spacing:1.5px;
            color:#475569;text-transform:uppercase;">Comvoy Sales Intelligence</span>
    </td></tr>

    <!-- Route info block -->
    <tr><td style="padding:0 20px;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
               style="border-collapse:collapse;background-color:#111827;border-radius:6px;">
            <tr><td style="padding:16px 20px;">
                <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
                       style="border-collapse:collapse;">
                    <tr><td style="font-size:11px;font-weight:700;letter-spacing:1px;
                        color:#4f8fff;text-transform:uppercase;padding-bottom:6px;">
                        ROUTE BRIEFING
                    </td></tr>
                    <tr><td style="font-size:20px;font-weight:700;color:#f1f5f9;
                        line-height:1.3;padding-bottom:4px;">
                        {day_of_week}, {date_str}
                    </td></tr>
                    <tr><td style="font-size:14px;color:#94a3b8;">
                        {start} &rarr; {end}
                    </td></tr>
                </table>
            </td></tr>
        </table>
    </td></tr>

    <!-- Spacer -->
    <tr><td style="height:16px;font-size:1px;line-height:1px;">&nbsp;</td></tr>

    <!-- ═══════ METRICS BAR ═══════ -->
    <tr><td style="padding:0 20px;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%"
               style="border-collapse:collapse;">
            <tr>
                <td width="25%" align="center" style="padding:12px 0;">
                    <span style="font-size:24px;font-weight:700;color:#f1f5f9;
                        line-height:1;">{summary['total']}</span><br>
                    <span style="font-size:10px;color:#64748b;text-transform:uppercase;
                        letter-spacing:0.5px;">Dealers</span>
                </td>
                <td width="25%" align="center" style="padding:12px 0;">
                    <span style="font-size:24px;font-weight:700;color:#f1f5f9;
                        line-height:1;">{total_vehicles:,}</span><br>
                    <span style="font-size:10px;color:#64748b;text-transform:uppercase;
                        letter-spacing:0.5px;">Vehicles</span>
                </td>
                <td width="25%" align="center" style="padding:12px 0;">
                    <span style="font-size:24px;font-weight:700;color:#4f8fff;
                        line-height:1;">{smyrna_pct}%</span><br>
                    <span style="font-size:10px;color:#64748b;text-transform:uppercase;
                        letter-spacing:0.5px;">Smyrna Pen.</span>
                </td>
                <td width="25%" align="center" style="padding:12px 0;">
                    <span style="font-size:24px;font-weight:700;color:#ff6b35;
                        line-height:1;">{summary.get('hot', 0)}</span><br>
                    <span style="font-size:10px;color:#64748b;text-transform:uppercase;
                        letter-spacing:0.5px;">High Priority</span>
                </td>
            </tr>
        </table>
    </td></tr>

    {_divider()}

    <!-- ═══════ EXECUTIVE SUMMARY ═══════ -->
    <tr><td style="padding:20px 20px 0 20px;">
        <span style="font-size:11px;font-weight:700;letter-spacing:1px;
            color:#94a3b8;text-transform:uppercase;">EXECUTIVE SUMMARY</span>
    </td></tr>
    <tr><td style="padding:10px 20px 20px 20px;font-size:14px;color:#cbd5e1;line-height:1.6;">
        {exec_html}
    </td></tr>

    {_divider()}

    {trends_note}

    <!-- ═══════ TOP STOPS ═══════ -->
    <tr><td style="padding:20px 20px 8px 20px;">
        <span style="font-size:11px;font-weight:700;letter-spacing:1px;
            color:#94a3b8;text-transform:uppercase;">TOP STOPS</span>
        <span style="font-size:11px;color:#475569;">
            &nbsp;&mdash; {len(top_stops)} highest-opportunity dealers</span>
    </td></tr>

    <tr><td style="padding:0;">
        {top_stops_html}
    </td></tr>

    <!-- ═══════ ALSO ON ROUTE ═══════ -->
    {also_html}

    {remaining_note}

    <!-- ═══════ SCORING KEY ═══════ -->
    {scoring_key}

    <!-- ═══════ FOOTER ═══════ -->
    {_divider()}
    <tr><td align="center" style="padding:24px 20px 12px 20px;">
        <span style="font-size:26px;font-weight:700;color:#1e293b;
            letter-spacing:-0.5px;">Otto</span>
    </td></tr>
    <tr><td align="center" style="padding:0 20px 6px 20px;
        font-size:12px;color:#475569;">
        Comvoy Sales Intelligence
    </td></tr>
    <tr><td align="center" style="padding:0 20px 28px 20px;
        font-size:11px;color:#334155;">
        Open Otto for the full interactive briefing
    </td></tr>

</table>
<!-- End main container -->

</td></tr>
</table>
</body>
</html>"""

    return html


# ---------------------------------------------------------------------------
# 3. Send email via SMTP
# ---------------------------------------------------------------------------

def send_briefing_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send HTML email via SMTP. Returns True on success."""
    if not settings.smtp_host or not settings.smtp_user:
        logger.warning("SMTP not configured — skipping email send. "
                       "Set SMTP_HOST, SMTP_USER, SMTP_PASSWORD in .env")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from
    msg["To"] = to_email

    # Plain text fallback
    plain = f"Your route briefing is ready. Open Otto for the full interactive version."
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as server:
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_password)
            server.send_message(msg)
        logger.info(f"Briefing email sent to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send briefing email to {to_email}: {e}")
        return False


# ---------------------------------------------------------------------------
# 4. Auto-brief entry point (called from BackgroundTask)
# ---------------------------------------------------------------------------

def auto_brief_trip(plan_id: str):
    """Full auto-brief pipeline: fetch plan → generate → render → send."""
    db = get_service_client()

    # Fetch plan + rep info
    plan_data = db.table("rep_travel_plans").select("*").eq("id", plan_id).execute()
    if not plan_data.data:
        logger.error(f"Auto-brief: plan {plan_id} not found")
        return

    plan = plan_data.data[0]
    rep_id = plan["rep_id"]

    rep_data = db.table("reps").select("name, email").eq("id", rep_id).execute()
    if not rep_data.data:
        logger.error(f"Auto-brief: rep {rep_id} not found")
        return

    rep = rep_data.data[0]
    rep_name = rep["name"]
    rep_email = rep.get("email")

    if not rep_email:
        logger.warning(f"Auto-brief: rep {rep_name} has no email — skipping")
        return

    # Generate briefing
    briefing = generate_route_briefing(plan)

    if not briefing["dealers"]:
        logger.info(f"Auto-brief: no dealers found for plan {plan_id} — skipping email")
        return

    # Render email
    html = render_briefing_email(rep_name, plan, briefing)

    # Format subject
    try:
        dt = datetime.strptime(str(plan["travel_date"]), "%Y-%m-%d")
        date_short = dt.strftime("%b %d")
    except (ValueError, TypeError):
        date_short = str(plan["travel_date"])

    hot_count = briefing["summary"].get("hot", 0)
    hot_note = f" / {hot_count} hot" if hot_count else ""
    subject = (
        f"Route Briefing {date_short}: "
        f"{plan['start_location']} to {plan['end_location']} "
        f"({briefing['summary']['total']} dealers{hot_note})"
    )

    # Send
    send_briefing_email(rep_email, subject, html)
    logger.info(f"Auto-brief complete for {rep_name}'s {plan['travel_date']} trip")
