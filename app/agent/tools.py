"""
Agent tool definitions for Claude API tool-use.

Phase 3 tools: 11 total
  Original 7: search, nearby, briefing, territory, dealer_trend, territory_trend, alerts
  New 4: get_lead_scores, get_route_dealers, get_dealer_intel, get_upload_report

Token-efficiency notes:
- Default search limit is 10 (not 200) to keep results small
- Tool results are compact JSON (minimal keys, no nulls)
- Chat endpoint further truncates results exceeding char budget
"""

import asyncio
import json
import logging
from datetime import date

from app.config import settings
from app.database import get_service_client
from app.api.dealers import (
    search_dealers as _search_dealers,
    find_nearby as _find_nearby,
    get_dealer_briefing as _get_briefing,
    get_territory_summary as _get_territory,
)
from app.api.trends import get_dealer_trend as _get_dealer_trend, get_territory_trend as _get_territory_trend
from app.api.alerts import get_alerts as _get_alerts
from app.api.scoring import get_lead_scores as _get_leads
from app.api.travel import get_route_dealers as _get_route_dealers, _haversine
from app.api.reports import get_latest_report as _get_latest_report
from app.etl.geocoder import geocode_single as _geocode_single_async
from app.models import NearbyQuery

logger = logging.getLogger(__name__)

# Tool definitions for Claude API
TOOL_DEFINITIONS = [
    {
        "name": "search_dealers",
        "description": (
            "Search for commercial truck dealers by name, state, minimum vehicle count, "
            "or Smyrna product presence. Returns a ranked list of matching dealers with "
            "inventory summaries. Use this for questions like 'show me dealers in Texas' "
            "or 'which dealers have Smyrna products' or 'find Capital Chevrolet'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Dealer name to search for (partial match). Optional.",
                },
                "state": {
                    "type": "string",
                    "description": "Two-letter state code to filter by (e.g. 'NC', 'TX'). Optional.",
                },
                "min_vehicles": {
                    "type": "integer",
                    "description": "Minimum total vehicle inventory to filter by. Optional.",
                },
                "has_smyrna": {
                    "type": "boolean",
                    "description": "If true, only return dealers that carry Smyrna/Fouts products. Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default 10, max 50). Use filters to narrow results instead of increasing limit.",
                    "default": 10,
                },
            },
            "required": [],
        },
    },
    {
        "name": "find_nearby_dealers",
        "description": (
            "Find commercial truck dealers within a radius of a geographic point. "
            "Returns dealers sorted by distance with inventory summaries. "
            "Use when a rep asks about dealers near a city, hotel, or GPS coordinates. "
            "For city names, use approximate coordinates (e.g. Charlotte NC = 35.2271, -80.8431)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "latitude": {
                    "type": "number",
                    "description": "Latitude of the center point.",
                },
                "longitude": {
                    "type": "number",
                    "description": "Longitude of the center point.",
                },
                "radius_miles": {
                    "type": "number",
                    "description": "Search radius in miles (default 30).",
                    "default": 30,
                },
            },
            "required": ["latitude", "longitude"],
        },
    },
    {
        "name": "get_dealer_briefing",
        "description": (
            "Get a full pre-call intelligence briefing for a specific dealer. "
            "Includes: total inventory, brand breakdown, body type breakdown, "
            "Smyrna/Fouts product details, and penetration percentage. "
            "Use this before a rep visits or calls a dealer. "
            "Requires the dealer's UUID — use search_dealers first to find it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dealer_id": {
                    "type": "string",
                    "description": "The dealer's UUID (get this from search_dealers results).",
                },
            },
            "required": ["dealer_id"],
        },
    },
    {
        "name": "get_territory_summary",
        "description": (
            "Get a state-level territory summary including total dealers, vehicles, "
            "Smyrna penetration, and top 10 dealers. Use for questions like "
            "'How is North Carolina looking?' or 'Give me the Texas overview'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "Two-letter state code (e.g. 'NC', 'FL', 'TX').",
                },
            },
            "required": ["state"],
        },
    },
    {
        "name": "get_dealer_trend",
        "description": (
            "Get multi-month trend data for a specific dealer. Shows how their "
            "inventory, Smyrna units, and rank have changed over time. "
            "Requires dealer UUID — use search_dealers first to find it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dealer_id": {
                    "type": "string",
                    "description": "The dealer's UUID.",
                },
                "months": {
                    "type": "integer",
                    "description": "Number of recent months to show (default: all available).",
                },
            },
            "required": ["dealer_id"],
        },
    },
    {
        "name": "get_territory_trend",
        "description": (
            "Get multi-month trend data for a state territory. Shows how dealer count, "
            "total vehicles, and Smyrna penetration have changed across months."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "Two-letter state code (e.g. 'NC', 'TX').",
                },
                "months": {
                    "type": "integer",
                    "description": "Number of recent months (default: all).",
                },
            },
            "required": ["state"],
        },
    },
    {
        "name": "get_alerts",
        "description": (
            "Get notable changes between the two most recent monthly reports. "
            "Surfaces: new dealers, lost dealers, Smyrna gains/losses, rank jumps, "
            "inventory surges/declines."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "Optional state filter (2-letter code).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max alerts to return (default 20).",
                    "default": 20,
                },
            },
            "required": [],
        },
    },
    # === PHASE 3: New agentic tools ===
    {
        "name": "get_lead_scores",
        "description": (
            "Get ranked leads scored by opportunity value (0-100). Tiers: hot (70+), warm (40-69), cold (<40). "
            "Scoring factors: inventory size, body type match with Smyrna products, whitespace status, growth momentum. "
            "Use for 'who should I call?', 'top opportunities in TX', 'hot leads', 'best whitespace targets'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "Two-letter state code to filter by. Optional.",
                },
                "tier": {
                    "type": "string",
                    "description": "Filter by tier: 'hot', 'warm', or 'cold'. Optional.",
                    "enum": ["hot", "warm", "cold"],
                },
                "opportunity_type": {
                    "type": "string",
                    "description": "Filter: 'whitespace' (no Smyrna), 'upsell' (has some), 'at_risk' (losing Smyrna). Optional.",
                    "enum": ["whitespace", "upsell", "at_risk"],
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 10, max 25).",
                    "default": 10,
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_route_dealers",
        "description": (
            "Find dealers along a sales rep's travel route for a specific day. "
            "Returns dealers within a corridor of the rep's start→end path, "
            "in geographic travel order (first stop to last stop). Each dealer includes "
            "lead score and distance off-route. Use for 'what dealers can Wesley hit tomorrow?' "
            "or 'who's on Kenneth's route March 10?'. Requires rep name and date."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "rep_name": {
                    "type": "string",
                    "description": "Sales rep name (e.g. 'Wesley White', 'Kenneth Greene').",
                },
                "travel_date": {
                    "type": "string",
                    "description": "Travel date in YYYY-MM-DD format.",
                },
                "buffer_miles": {
                    "type": "number",
                    "description": "How far off-route to search in miles (default 20).",
                    "default": 20,
                },
            },
            "required": ["rep_name", "travel_date"],
        },
    },
    {
        "name": "get_dealer_intel",
        "description": (
            "Generate talking points and key intel about a dealer for email/call prep. "
            "Returns a structured summary a rep can use to write their outreach. "
            "Combines inventory data, Smyrna status, body type fit, and lead score. "
            "Use for 'give me talking points for Century Trucks' or 'prep me for a call with Ancira Chevrolet'. "
            "Requires dealer UUID — use search_dealers first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dealer_id": {
                    "type": "string",
                    "description": "The dealer's UUID.",
                },
            },
            "required": ["dealer_id"],
        },
    },
    {
        "name": "get_upload_report",
        "description": (
            "Get the latest auto-generated monthly report. Shows territory-wide changes, "
            "Smyrna gains/losses, lead score distribution, and top opportunities. "
            "Generated automatically after each monthly data upload. "
            "Use for 'what's the latest report?', 'monthly summary', 'how did we do this month?'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "suggest_travel_plan",
        "description": (
            "Generate a multi-day travel plan for a sales rep, clustering nearby high-value dealers "
            "into daily stops with optimized driving routes. Use when a manager asks to plan a trip, "
            "brainstorm which dealers to visit, or build a travel schedule. Supports iteration — "
            "the manager can exclude specific dealers, add states, adjust days, or raise/lower "
            "the minimum score threshold. "
            "Use for 'plan a 3-day trip for Wesley in GA and TN', 'who should Kenneth visit in Texas?', "
            "'build me a travel plan covering the southeast'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "target_states": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of 2-letter state codes to cover (e.g. ['GA', 'TN', 'NC']).",
                },
                "num_days": {
                    "type": "integer",
                    "description": "Number of travel days (1-7). Default 3.",
                    "default": 3,
                    "minimum": 1,
                    "maximum": 7,
                },
                "base_location": {
                    "type": "string",
                    "description": "Starting location as a place name or address (e.g. 'Smyrna, GA', '123 Peachtree St, Atlanta GA', 'Nashville'). Geocoded automatically. Optional — defaults to centroid of selected dealers.",
                },
                "base_city_lat": {
                    "type": "number",
                    "description": "Latitude of starting city. Use base_location instead when possible. Optional.",
                },
                "base_city_lng": {
                    "type": "number",
                    "description": "Longitude of starting city. Use base_location instead when possible. Optional.",
                },
                "rep_name": {
                    "type": "string",
                    "description": "Sales rep name for context (e.g. 'Wesley White'). Optional.",
                },
                "exclude_dealer_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Dealer UUIDs to skip (for iteration when manager says 'skip that one'). Optional.",
                },
                "min_score": {
                    "type": "integer",
                    "description": "Minimum lead score to include (0-100). Default 30. Use higher for focused trips.",
                    "default": 30,
                },
            },
            "required": ["target_states"],
        },
    },
]


def _compact_dealer(d) -> dict:
    """Strip nulls and low-value fields for token efficiency. Keeps lat/lon for geo."""
    out = {
        "id": d.id, "name": d.name, "city": d.city, "state": d.state,
        "vehicles": d.total_vehicles, "rank": d.rank,
    }
    if d.latitude is not None:
        out["lat"] = round(d.latitude, 4)
        out["lng"] = round(d.longitude, 4)
    if d.smyrna_units:
        out["smyrna"] = d.smyrna_units
        out["smyrna_pct"] = d.smyrna_percentage
    if d.top_brand:
        out["top_brand"] = d.top_brand
    if d.distance_miles is not None:
        out["dist_mi"] = round(d.distance_miles, 1)
    return out


KM_TO_MI = 0.621371


def _geocode_sync(location: str) -> tuple[float, float] | None:
    """Geocode a location string synchronously. Returns (lat, lng) or None.

    Parses 'City, ST' style or freeform addresses via Nominatim.
    Works from sync context by spinning up a temporary event loop.
    """
    # Split into city/state if it looks like "Atlanta, GA" or "Atlanta GA"
    parts = [p.strip().rstrip(",") for p in location.replace(",", " ").split() if p.strip()]
    # Try to extract state (last 2-letter token that looks like a state code)
    city_parts = list(parts)
    state = ""
    for i in range(len(parts) - 1, -1, -1):
        if len(parts[i]) == 2 and parts[i].isalpha():
            state = parts[i].upper()
            city_parts = parts[:i]
            break
    city = " ".join(city_parts) if city_parts else location

    try:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(_geocode_single_async(city, state))
        loop.close()
        return result
    except Exception as e:
        logger.warning(f"Geocoding failed for '{location}': {e}")
        return None


def _centroid(dealers: list[dict]) -> tuple[float, float]:
    """Return (lat, lng) centroid of a list of dealers with lat/lng keys."""
    if not dealers:
        return (0.0, 0.0)
    avg_lat = sum(d["lat"] for d in dealers) / len(dealers)
    avg_lng = sum(d["lng"] for d in dealers) / len(dealers)
    return (avg_lat, avg_lng)


def _cluster_dealers(dealers: list[dict], num_days: int, base: tuple[float, float] | None = None) -> list[list[dict]]:
    """Greedy proximity clustering of dealers into num_days groups.

    Algorithm:
    1. Start from base (or centroid of all dealers)
    2. For each day, grab the closest N dealers to the current anchor
    3. Next day's anchor = centroid of the dealers just assigned
    """
    if not dealers:
        return [[] for _ in range(num_days)]

    remaining = list(dealers)
    clusters: list[list[dict]] = []
    anchor = base or _centroid(remaining)

    for day_idx in range(num_days):
        if not remaining:
            clusters.append([])
            continue

        days_left = num_days - day_idx
        take = max(1, len(remaining) // days_left)

        # Sort remaining by distance to anchor
        remaining.sort(key=lambda d: _haversine(anchor[0], anchor[1], d["lat"], d["lng"]))

        day_dealers = remaining[:take]
        remaining = remaining[take:]
        clusters.append(day_dealers)

        # Next day starts from this day's centroid
        anchor = _centroid(day_dealers)

    return clusters


def _order_nearest_neighbor(dealers: list[dict], start: tuple[float, float]) -> list[dict]:
    """Order dealers by nearest-neighbor traversal starting from a point.

    Greedy: always visit the closest unvisited dealer next.
    Returns the reordered list.
    """
    if len(dealers) <= 1:
        return list(dealers)

    remaining = list(dealers)
    ordered = []
    current = start

    while remaining:
        # Find closest to current position
        closest_idx = min(
            range(len(remaining)),
            key=lambda i: _haversine(current[0], current[1], remaining[i]["lat"], remaining[i]["lng"]),
        )
        closest = remaining.pop(closest_idx)
        ordered.append(closest)
        current = (closest["lat"], closest["lng"])

    return ordered


def _compute_driving_miles(dealers: list[dict]) -> float:
    """Compute total driving distance in miles along ordered dealer stops."""
    total = 0.0
    for i in range(len(dealers) - 1):
        km = _haversine(dealers[i]["lat"], dealers[i]["lng"], dealers[i + 1]["lat"], dealers[i + 1]["lng"])
        total += km * KM_TO_MI
    return round(total, 1)


def _area_label(dealers: list[dict]) -> str:
    """Generate a human-readable area label like 'Atlanta GA area' from a cluster."""
    if not dealers:
        return "No dealers"
    # Most common state
    state_counts: dict[str, int] = {}
    for d in dealers:
        state_counts[d["state"]] = state_counts.get(d["state"], 0) + 1
    top_state = max(state_counts, key=state_counts.get)
    # Most common city
    city_counts: dict[str, int] = {}
    for d in dealers:
        if d["state"] == top_state:
            city_counts[d["city"]] = city_counts.get(d["city"], 0) + 1
    top_city = max(city_counts, key=city_counts.get)
    return f"{top_city} {top_state} area"


def execute_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool by name and return compact JSON string result."""
    try:
        if tool_name == "search_dealers":
            limit = min(tool_input.get("limit", settings.agent_search_limit), 50)
            result = _search_dealers(
                q=tool_input.get("query"),
                state=tool_input.get("state"),
                min_vehicles=tool_input.get("min_vehicles"),
                has_smyrna=tool_input.get("has_smyrna"),
                limit=limit,
            )
            return json.dumps([_compact_dealer(r) for r in result], default=str)

        elif tool_name == "find_nearby_dealers":
            result = _find_nearby(NearbyQuery(
                latitude=tool_input["latitude"],
                longitude=tool_input["longitude"],
                radius_miles=tool_input.get("radius_miles", 30),
            ))
            return json.dumps([_compact_dealer(r) for r in result], default=str)

        elif tool_name == "get_dealer_briefing":
            result = _get_briefing(tool_input["dealer_id"])
            d = result.dealer
            briefing = {
                "dealer": d.name, "city": d.city, "state": d.state,
                "lat": round(d.latitude, 4) if d.latitude else None,
                "lng": round(d.longitude, 4) if d.longitude else None,
                "rank": d.rank, "vehicles": d.total_vehicles,
                "top_brand": d.top_brand,
                "brands": result.brand_breakdown,
                "body_types": result.body_type_breakdown,
            }
            if d.smyrna_units:
                briefing["smyrna_units"] = d.smyrna_units
                briefing["smyrna_pct"] = d.smyrna_percentage
                if result.smyrna_details:
                    briefing["smyrna_body_types"] = result.smyrna_details.get("top_smyrna_body_types")
            else:
                briefing["smyrna"] = "NONE — whitespace opportunity"
            return json.dumps(briefing, default=str)

        elif tool_name == "get_territory_summary":
            result = _get_territory(tool_input["state"])
            compact_top = []
            for td in result.get("top_10_dealers", []):
                entry = {"name": td["name"], "city": td["city"],
                         "vehicles": td["total_vehicles"], "rank": td["rank"]}
                if td.get("smyrna_units"):
                    entry["smyrna"] = td["smyrna_units"]
                compact_top.append(entry)
            return json.dumps({
                "state": result["state"],
                "dealers": result["total_dealers"],
                "vehicles": result["total_vehicles"],
                "smyrna_units": result["total_smyrna_units"],
                "smyrna_dealers": result["dealers_with_smyrna"],
                "smyrna_pct": result["smyrna_penetration_pct"],
                "top_10": compact_top,
            }, default=str)

        elif tool_name == "get_dealer_trend":
            result = _get_dealer_trend(
                dealer_id=tool_input["dealer_id"],
                months=tool_input.get("months"),
            )
            return json.dumps({
                "dealer": f"{result.dealer_name} ({result.city}, {result.state})",
                "vehicle_trend": result.vehicle_trend,
                "smyrna_trend": result.smyrna_trend,
                "rank_trend": result.rank_trend,
                "months": [
                    {"date": p.report_date, "vehicles": p.total_vehicles,
                     "smyrna": p.smyrna_units, "rank": p.rank,
                     "v_delta": p.vehicle_delta, "s_delta": p.smyrna_delta, "r_delta": p.rank_delta}
                    for p in result.points
                ],
            }, default=str)

        elif tool_name == "get_territory_trend":
            result = _get_territory_trend(
                state=tool_input["state"],
                months=tool_input.get("months"),
            )
            return json.dumps({
                "state": result.state,
                "dealer_count_delta": result.dealer_count_delta,
                "vehicle_delta": result.vehicle_delta,
                "smyrna_delta": result.smyrna_delta,
                "months": [
                    {"date": p.report_date, "dealers": p.total_dealers,
                     "vehicles": p.total_vehicles, "smyrna": p.total_smyrna,
                     "smyrna_dealers": p.dealers_with_smyrna, "penetration": p.smyrna_penetration_pct}
                    for p in result.points
                ],
            }, default=str)

        elif tool_name == "get_alerts":
            result = _get_alerts(
                state=tool_input.get("state"),
                limit=tool_input.get("limit", 20),
            )
            return json.dumps({
                "summary": result.summary,
                "total": result.total_alerts,
                "alerts": [
                    {"type": a.alert_type, "dealer": a.dealer_name,
                     "state": a.state, "message": a.message}
                    for a in result.alerts
                ],
            }, default=str)

        # === PHASE 3 TOOLS ===

        elif tool_name == "get_lead_scores":
            limit = min(tool_input.get("limit", 10), 25)
            result = _get_leads(
                state=tool_input.get("state"),
                tier=tool_input.get("tier"),
                opportunity_type=tool_input.get("opportunity_type"),
                limit=limit,
            )
            # Include scoring factors so Otto can explain WHY each dealer scored the way they did
            leads = []
            for lead in result["leads"]:
                entry = {
                    "id": lead["dealer_id"], "name": lead["name"],
                    "city": lead["city"], "state": lead["state"],
                    "score": lead["score"], "tier": lead["lead_tier"] if "lead_tier" in lead else lead["tier"],
                    "type": lead.get("opportunity") or lead.get("type"),
                }
                if lead.get("lat"):
                    entry["lat"] = round(lead["lat"], 4)
                    entry["lng"] = round(lead["lng"], 4)
                f = lead.get("factors", {})
                if f:
                    why = {}
                    if f.get("inventory_size") is not None:
                        why["size_pts"] = f"{f['inventory_size']}/30"
                    if f.get("match_pct") is not None:
                        why["body_match"] = f"{f['match_pct']}%"
                    if f.get("body_type_match") is not None:
                        why["match_pts"] = f"{f['body_type_match']}/30"
                    if f.get("smyrna_opportunity") is not None:
                        why["smyrna_pts"] = f"{f['smyrna_opportunity']}/25"
                    if f.get("growth_momentum") is not None:
                        why["growth_pts"] = f"{f['growth_momentum']}/15"
                    if f.get("growth_pct") is not None:
                        why["growth_pct"] = f"{f['growth_pct']}%"
                    if f.get("at_risk_bonus"):
                        why["at_risk"] = True
                    if f.get("note"):
                        why["note"] = f["note"]
                    entry["why"] = why
                leads.append(entry)
            return json.dumps({"leads": leads, "total": result["total"]}, default=str)

        elif tool_name == "get_route_dealers":
            # Resolve rep name to ID (simple match — small team)
            db = get_service_client()
            rep_name = tool_input["rep_name"].lower()
            all_reps = db.table("reps").select("id, name").eq("is_active", True).execute()
            reps_data = [r for r in all_reps.data if rep_name in r["name"].lower()]
            reps = type('R', (), {'data': reps_data})()
            if not reps.data:
                return json.dumps({"error": f"No rep found matching '{rep_name}'"})
            rep_id = reps.data[0]["id"]

            travel_dt = date.fromisoformat(tool_input["travel_date"])
            try:
                result = _get_route_dealers(
                    rep_id=rep_id,
                    travel_date=travel_dt,
                    buffer_miles=tool_input.get("buffer_miles", 20),
                    limit=30,
                )
            except Exception as e:
                if "No travel plan" in str(e) or "404" in str(e):
                    # Fetch available dates so Otto can suggest alternatives
                    plans = db.table("rep_travel_plans").select(
                        "travel_date, start_location, end_location"
                    ).eq("rep_id", rep_id).order("travel_date").execute()
                    available = [
                        {"date": p["travel_date"], "from": p["start_location"], "to": p["end_location"]}
                        for p in (plans.data or [])
                    ]
                    return json.dumps({
                        "error": f"No travel plan for {reps.data[0]['name']} on {travel_dt.isoformat()}",
                        "available_dates": available,
                        "hint": "Ask which of these dates the rep wants, or suggest they add a plan for this date.",
                    }, default=str)
                raise
            # Fetch scoring factors so Otto can explain WHY each dealer is rated the way they are
            route_dealer_ids = [d["dealer_id"] for d in result["dealers"] if d.get("dealer_id")]
            factors_map = {}
            if route_dealer_ids:
                snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
                if snap.data:
                    factor_rows = db.table("lead_scores").select(
                        "dealer_id, factors"
                    ).eq("snapshot_id", snap.data[0]["id"]).in_(
                        "dealer_id", route_dealer_ids
                    ).execute()
                    factors_map = {r["dealer_id"]: r["factors"] for r in (factor_rows.data or [])}

            # Compact output — dealers already in travel order (start→end)
            dealers = []
            for i, d in enumerate(result["dealers"], 1):
                entry = {
                    "stop": i,
                    "name": d["name"], "city": d["city"], "state": d["state"],
                    "mi_off_route": d["dist_from_route_mi"],
                    "vehicles": d["vehicles"],
                }
                if d.get("smyrna_units"):
                    entry["smyrna"] = d["smyrna_units"]
                if d.get("lead_score"):
                    entry["score"] = d["lead_score"]
                    entry["tier"] = d["lead_tier"]
                if d.get("opportunity"):
                    entry["type"] = d["opportunity"]
                # Include scoring factors for score explanation
                f = factors_map.get(d.get("dealer_id"), {})
                if f:
                    why = {}
                    if f.get("match_pct") is not None:
                        why["body_match"] = f"{f['match_pct']}%"
                    if f.get("smyrna_opportunity") is not None:
                        why["smyrna_pts"] = f"{f['smyrna_opportunity']}/25"
                    if f.get("growth_pct") is not None:
                        why["growth"] = f"{f['growth_pct']}%"
                    if f.get("at_risk_bonus"):
                        why["at_risk"] = True
                    entry["why"] = why
                dealers.append(entry)
            output = {
                "rep": reps.data[0]["name"],
                "date": result["travel_date"],
                "start": result["start"],
                "end": result["end"],
                "notes": result.get("notes"),
                "total": result["total"],
            }
            if result.get("mode") == "radius":
                output["mode"] = "day_trip"
                output["note"] = "Start and end are the same location. Dealers sorted by distance from base."
                output["dealers_by_distance"] = dealers
            else:
                output["dealers_in_travel_order"] = dealers
            return json.dumps(output, default=str)

        elif tool_name == "get_dealer_intel":
            # Build comprehensive talking points from briefing + lead score + trends
            dealer_id = tool_input["dealer_id"]
            result = _get_briefing(dealer_id)
            d = result.dealer

            # Get lead score
            db = get_service_client()
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            score_data = None
            if snap.data:
                scores = db.table("lead_scores").select("score, tier, opportunity_type, factors").eq(
                    "dealer_id", dealer_id
                ).eq("snapshot_id", snap.data[0]["id"]).execute()
                if scores.data:
                    score_data = scores.data[0]

            # Get trend data if 2+ months exist
            trend_intel = None
            try:
                trend = _get_dealer_trend(dealer_id=dealer_id)
                if len(trend.points) >= 2:
                    latest = trend.points[-1]
                    trend_intel = {
                        "months_tracked": len(trend.points),
                        "inventory_trend": trend.vehicle_trend,
                        "smyrna_trend": trend.smyrna_trend,
                        "rank_trend": trend.rank_trend,
                    }
                    if latest.vehicle_delta is not None:
                        trend_intel["last_month_vehicle_change"] = latest.vehicle_delta
                    if latest.smyrna_delta is not None:
                        trend_intel["last_month_smyrna_change"] = latest.smyrna_delta
                    if latest.rank_delta is not None:
                        trend_intel["last_month_rank_change"] = latest.rank_delta
            except Exception:
                pass  # trends unavailable (single month)

            # Build intel
            intel = {
                "dealer": d.name,
                "location": f"{d.city}, {d.state}",
                "inventory_size": f"{d.total_vehicles} vehicles (rank #{d.rank} in territory)",
                "top_brand": d.top_brand,
                "top_body_types": [
                    f"{bt['body_type']} ({bt['vehicles']})"
                    for bt in result.body_type_breakdown[:5]
                ],
            }

            if d.smyrna_units:
                intel["smyrna_status"] = f"EXISTING CUSTOMER — {d.smyrna_units} Smyrna units ({d.smyrna_percentage}% of inventory)"
                if result.smyrna_details:
                    intel["smyrna_products"] = result.smyrna_details.get("top_smyrna_body_types")
                intel["talking_point"] = (
                    f"They already carry {d.smyrna_units} Smyrna units. "
                    f"Look at their body type mix — identify types they stock heavily "
                    f"that Smyrna also builds for upsell opportunities."
                )
            else:
                # Check which of their body types match Smyrna offerings
                from app.api.scoring import SMYRNA_BODY_TYPES
                matching = [
                    f"{bt['body_type']} ({bt['vehicles']})"
                    for bt in result.body_type_breakdown
                    if bt["body_type"] in SMYRNA_BODY_TYPES
                ]
                total_match = sum(
                    bt["vehicles"] for bt in result.body_type_breakdown
                    if bt["body_type"] in SMYRNA_BODY_TYPES
                )
                intel["smyrna_status"] = "WHITESPACE — zero Smyrna products"
                intel["body_type_overlap"] = matching
                intel["overlap_vehicles"] = total_match
                intel["talking_point"] = (
                    f"They have {total_match} vehicles in body types Smyrna builds "
                    f"but zero Smyrna product. That's {total_match} units of addressable opportunity. "
                    f"Lead with {matching[0] if matching else 'their top body type'} — it's their biggest Smyrna-compatible category."
                )

            if score_data:
                intel["lead_score"] = f"{score_data['score']}/100 ({score_data['tier']})"
                intel["opportunity_type"] = score_data["opportunity_type"]
                # Include factor breakdown so Otto can explain the score
                f = score_data.get("factors", {})
                if f:
                    intel["score_breakdown"] = {
                        "inventory_size": f"{f.get('inventory_size', 0)}/30",
                        "body_type_match": f"{f.get('match_pct', 0)}% ({f.get('body_type_match', 0)}/30 pts)",
                        "smyrna_opportunity": f"{f.get('smyrna_opportunity', 0)}/25",
                        "growth_momentum": f"{f.get('growth_momentum', 0)}/15",
                    }
                    if f.get("growth_pct") is not None:
                        intel["score_breakdown"]["growth_pct"] = f"{f['growth_pct']}%"
                    if f.get("at_risk_bonus"):
                        intel["score_breakdown"]["at_risk_bonus"] = "+20 (lost Smyrna)"

            if trend_intel:
                intel["trend"] = trend_intel
                # Enhance talking point with trend context
                if trend_intel["inventory_trend"] == "up":
                    intel["talking_point"] += " Their inventory is growing — they're actively buying."
                elif trend_intel["inventory_trend"] == "down":
                    intel["talking_point"] += " Their inventory has been shrinking — could mean tighter budgets or a pivot."
                if trend_intel["smyrna_trend"] == "up":
                    intel["talking_point"] += " Smyrna units are trending up — they're expanding the relationship."
                elif trend_intel["smyrna_trend"] == "down":
                    intel["talking_point"] += " Smyrna units are declining — worth asking why and if there's a retention issue."

            return json.dumps(intel, default=str)

        elif tool_name == "get_upload_report":
            try:
                result = _get_latest_report()
                report = result.get("report", {})
                # Include structured data so Otto can answer specifics
                output = {
                    "date": result["report_date"],
                    "overview": report.get("overview"),
                }
                if report.get("vs_last_month"):
                    output["vs_last_month"] = report["vs_last_month"]
                if report.get("new_dealers"):
                    output["new_dealers"] = report["new_dealers"]
                if report.get("lost_dealers"):
                    output["lost_dealers"] = report["lost_dealers"]
                if report.get("smyrna_gained"):
                    output["smyrna_gained"] = report["smyrna_gained"]
                if report.get("smyrna_lost"):
                    output["smyrna_lost"] = report["smyrna_lost"]
                if report.get("lead_scores"):
                    output["lead_scores"] = report["lead_scores"]
                if report.get("by_state"):
                    output["by_state"] = report["by_state"]
                output["summary"] = result["summary"]
                return json.dumps(output, default=str)
            except Exception:
                return json.dumps({"error": "No upload reports found. Upload a monthly report first."})

        elif tool_name == "suggest_travel_plan":
            db = get_service_client()
            states = [s.upper() for s in tool_input["target_states"]]
            num_days = min(max(tool_input.get("num_days", 3), 1), 7)
            min_score = tool_input.get("min_score", 30)
            exclude_ids = set(tool_input.get("exclude_dealer_ids") or [])

            # Get base coordinates — prefer base_location (geocoded), fall back to lat/lng
            base = None
            base_label = None
            if tool_input.get("base_location"):
                coords = _geocode_sync(tool_input["base_location"])
                if coords:
                    base = coords
                    base_label = tool_input["base_location"]
                else:
                    logger.warning(f"Could not geocode base_location: {tool_input['base_location']}")
            if base is None and tool_input.get("base_city_lat") and tool_input.get("base_city_lng"):
                base = (tool_input["base_city_lat"], tool_input["base_city_lng"])

            # Get latest snapshot
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            if not snap.data:
                return json.dumps({"error": "No data snapshots found. Upload a monthly report first."})
            snap_id = snap.data[0]["id"]

            # Query lead scores for target states, joined with dealers for location
            # Supabase PostgREST: use foreign key join — include factors for score explanation
            scores = db.table("lead_scores").select(
                "dealer_id, score, tier, opportunity_type, factors, "
                "dealers!inner(id, name, city, state, latitude, longitude)"
            ).eq("snapshot_id", snap_id).gte("score", min_score).in_(
                "dealers.state", states
            ).order("score", desc=True).execute()

            if not scores.data:
                return json.dumps({
                    "error": f"No dealers with score >= {min_score} found in {', '.join(states)}.",
                    "tip": "Try lowering min_score or adding more states.",
                })

            # Build dealer list with location data, filtering exclusions and missing coords
            candidates = []
            for row in scores.data:
                d = row["dealers"]
                if d["id"] in exclude_ids:
                    continue
                if d.get("latitude") is None or d.get("longitude") is None:
                    continue
                candidates.append({
                    "id": d["id"],
                    "name": d["name"],
                    "city": d["city"],
                    "state": d["state"],
                    "lat": d["latitude"],
                    "lng": d["longitude"],
                    "score": row["score"],
                    "tier": row["tier"],
                    "type": row["opportunity_type"],
                    "factors": row.get("factors", {}),
                })

            if not candidates:
                return json.dumps({
                    "error": "No geocoded dealers found matching criteria after exclusions.",
                    "tip": "Try different states or lower the min_score threshold.",
                })

            # Cap at num_days * 8 — a rep can realistically visit ~6-8 dealers/day
            max_dealers = num_days * 8
            candidates = candidates[:max_dealers]

            # Cluster into days
            clusters = _cluster_dealers(candidates, num_days, base)

            # Order within each day using nearest-neighbor
            plan = []
            hot_count = 0
            total_driving = 0.0
            total_dealers_count = 0

            for day_idx, cluster in enumerate(clusters, 1):
                if not cluster:
                    plan.append({
                        "day": day_idx,
                        "area": "No dealers assigned",
                        "dealers": [],
                        "total_driving_mi": 0,
                        "dealer_count": 0,
                    })
                    continue

                # Get starting point for nearest-neighbor ordering
                day_start = base if (base and day_idx == 1) else _centroid(cluster)
                ordered = _order_nearest_neighbor(cluster, day_start)

                # Compute driving distance
                driving_mi = _compute_driving_miles(ordered)
                total_driving += driving_mi
                total_dealers_count += len(ordered)

                day_dealers = []
                for i, d in enumerate(ordered, 1):
                    if d["tier"] == "hot":
                        hot_count += 1
                    entry = {
                        "stop": i,
                        "id": d["id"],
                        "name": d["name"],
                        "city": d["city"],
                        "state": d["state"],
                        "score": d["score"],
                        "tier": d["tier"],
                        "type": d["type"],
                    }
                    # Include compact scoring factors so Otto can explain ratings
                    f = d.get("factors", {})
                    if f:
                        why = {}
                        if f.get("match_pct") is not None:
                            why["body_match"] = f"{f['match_pct']}%"
                        if f.get("smyrna_opportunity") is not None:
                            why["smyrna_pts"] = f"{f['smyrna_opportunity']}/25"
                        if f.get("growth_pct") is not None:
                            why["growth"] = f"{f['growth_pct']}%"
                        if f.get("at_risk_bonus"):
                            why["at_risk"] = True
                        entry["why"] = why
                    # Add distance from previous stop
                    if i > 1:
                        prev = ordered[i - 2]
                        mi = _haversine(prev["lat"], prev["lng"], d["lat"], d["lng"]) * KM_TO_MI
                        entry["mi_from_prev"] = round(mi, 1)
                    day_dealers.append(entry)

                plan.append({
                    "day": day_idx,
                    "area": _area_label(ordered),
                    "dealers": day_dealers,
                    "total_driving_mi": driving_mi,
                    "dealer_count": len(ordered),
                })

            rep_label = tool_input.get("rep_name", "")
            summary = {
                "total_days": num_days,
                "total_dealers": total_dealers_count,
                "total_driving_mi": round(total_driving, 1),
                "hot_count": hot_count,
                "states_covered": states,
                "min_score_used": min_score,
            }
            if rep_label:
                summary["rep"] = rep_label
            if base_label:
                summary["starting_from"] = base_label

            return json.dumps({
                "plan": plan,
                "summary": summary,
                "tip": "Reply to adjust: 'skip dealer X', 'add South Carolina', 'make it 4 days', 'only hot leads', 'what if we start from Nashville?'",
            }, default=str)

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as e:
        logger.exception(f"Tool execution failed: {tool_name}")
        return json.dumps({"error": str(e)})
