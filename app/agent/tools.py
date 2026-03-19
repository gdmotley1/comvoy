"""
Agent tool definitions for Claude API tool-use.

Phase 6 tools: 17 total
  Original 7: search, nearby, briefing, territory, dealer_trend, territory_trend, alerts
  Phase 3: get_lead_scores, get_route_dealers, get_dealer_intel, get_upload_report
  Phase 4: get_dealer_places (Google Places business data)
  Phase 5: search_vehicles, get_dealer_inventory, get_inventory_changes (VIN-level data)
  Phase 6: get_price_analytics, get_market_intel (pricing & competitive intelligence)

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
            "Four factors: fleet scale (0-20), product fit (0-25), Smyrna penetration (0-30), growth signal (0-25). "
            "Use for 'who should I call?', 'top opportunities in TX', 'hot leads'."
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
    # === PHASE 4: Google Places integration ===
    {
        "name": "get_dealer_places",
        "description": (
            "Get Google business info for a dealer — rating, reviews, phone, website, "
            "business hours, and Google Maps link. Use for 'what's their rating?', "
            "'are they open?', 'phone number for X dealer', 'show me dealers rated above 4 stars'. "
            "Can also filter all dealers by minimum Google rating."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dealer_id": {
                    "type": "string",
                    "description": "Dealer UUID to look up. Use search_dealers first to find it. Optional if using min_rating filter.",
                },
                "min_rating": {
                    "type": "number",
                    "description": "Filter all cached dealers by minimum Google rating (e.g. 4.0). Returns up to 20. Optional.",
                },
                "state": {
                    "type": "string",
                    "description": "Two-letter state code to combine with min_rating filter. Optional.",
                },
            },
            "required": [],
        },
    },
    # === PHASE 5: VIN-level vehicle tools ===
    {
        "name": "search_vehicles",
        "description": (
            "Search individual vehicles in dealer inventory by brand, body type, price range, "
            "state, or Smyrna products. Returns specific trucks with VIN, price, dealer, and specs. "
            "Use for 'show me Freightliner service trucks under $80K in Texas', "
            "'find all Smyrna products', 'cheapest flatbeds in Georgia', "
            "'what Ford F-550s are available?'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "brand": {
                    "type": "string",
                    "description": "Chassis brand (e.g. 'Ford', 'Freightliner', 'Chevrolet'). Optional.",
                },
                "body_type": {
                    "type": "string",
                    "description": "Body type (e.g. 'Service Trucks', 'Flatbed Trucks', 'Box Vans'). Partial match. Optional.",
                },
                "min_price": {
                    "type": "integer",
                    "description": "Minimum price in dollars. Optional.",
                },
                "max_price": {
                    "type": "integer",
                    "description": "Maximum price in dollars. Optional.",
                },
                "state": {
                    "type": "string",
                    "description": "Two-letter state code. Optional.",
                },
                "dealer_id": {
                    "type": "string",
                    "description": "Dealer UUID to search within. Optional.",
                },
                "is_smyrna": {
                    "type": "boolean",
                    "description": "If true, only Smyrna-distributed vehicles. Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 15, max 30).",
                    "default": 15,
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_dealer_inventory",
        "description": (
            "Get the full vehicle inventory for a specific dealer — every truck with VIN, "
            "price, brand, model, body type, and specs. Use for 'what does Akins Ford have?', "
            "'show me all trucks at Rush Truck Centers in Atlanta', "
            "'list inventory for this dealer'. Requires dealer UUID — use search_dealers first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dealer_id": {
                    "type": "string",
                    "description": "The dealer's UUID.",
                },
                "brand": {
                    "type": "string",
                    "description": "Filter by brand within this dealer. Optional.",
                },
                "body_type": {
                    "type": "string",
                    "description": "Filter by body type within this dealer. Optional.",
                },
            },
            "required": ["dealer_id"],
        },
    },
    {
        "name": "get_inventory_changes",
        "description": (
            "Get month-over-month inventory changes: new vehicles added, vehicles sold, "
            "and price changes. Use for 'what sold last month?', 'new inventory this month', "
            "'any price drops?', 'what changed at this dealer?'. "
            "Can filter by state, brand, or dealer."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "change_type": {
                    "type": "string",
                    "description": "Type of change: 'new', 'sold', or 'price_change'. Optional (all if omitted).",
                    "enum": ["new", "sold", "price_change"],
                },
                "state": {
                    "type": "string",
                    "description": "Two-letter state filter. Optional.",
                },
                "brand": {
                    "type": "string",
                    "description": "Brand filter. Optional.",
                },
                "dealer_id": {
                    "type": "string",
                    "description": "Dealer UUID filter. Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 20).",
                    "default": 20,
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_price_analytics",
        "description": (
            "Get pricing analytics: average, min, max, and median prices by brand, body type, "
            "state, or dealer. Use for 'what's the market rate for Ford service trucks?', "
            "'cheapest flatbeds in Texas?', 'how does dealer X's pricing compare to market?'. "
            "Can compare a specific dealer's avg price vs the overall market average."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "brand": {
                    "type": "string",
                    "description": "Filter by chassis brand (e.g. 'Ford', 'Freightliner'). Optional.",
                },
                "body_type": {
                    "type": "string",
                    "description": "Filter by body type (e.g. 'Service Trucks', 'Flatbed Trucks'). Partial match. Optional.",
                },
                "state": {
                    "type": "string",
                    "description": "Two-letter state code filter. Optional.",
                },
                "dealer_id": {
                    "type": "string",
                    "description": "Compare this dealer's pricing vs the market. Optional — returns both dealer and market stats.",
                },
                "condition": {
                    "type": "string",
                    "description": "Filter by condition ('New' or 'Used'). Optional.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_market_intel",
        "description": (
            "Get competitive market intelligence: body builder market share, brand concentration "
            "by state, and body type distribution. Use for 'who are the top body builders in Georgia?', "
            "'what brands dominate Texas?', 'market share for Reading vs Morgan?', "
            "'which body builders compete with Smyrna?'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "report_type": {
                    "type": "string",
                    "description": "Type of report: 'body_builders' (market share by body builder), 'brands' (chassis brand concentration), 'body_types' (body type distribution).",
                    "enum": ["body_builders", "brands", "body_types"],
                },
                "state": {
                    "type": "string",
                    "description": "Two-letter state code to focus on. Optional — if omitted, shows all-market data.",
                },
                "body_type": {
                    "type": "string",
                    "description": "Filter to a specific body type segment (e.g. 'Service Trucks'). Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max entries to return (default 15).",
                    "default": 15,
                },
            },
            "required": ["report_type"],
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
        result = asyncio.run(_geocode_single_async(city, state))
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
                briefing["smyrna"] = "NONE — zero Smyrna products"
            # Enrich with cached Google Places data (sync DB read, no API call)
            try:
                db_p = get_service_client()
                places_row = db_p.table("dealer_places").select(
                    "rating, review_count, phone, website, hours_json, business_status, formatted_address"
                ).eq("dealer_id", tool_input["dealer_id"]).execute()
                if places_row.data:
                    p = places_row.data[0]
                    if p.get("rating"):
                        briefing["google_rating"] = float(p["rating"])
                        briefing["reviews"] = p.get("review_count")
                    if p.get("phone"):
                        briefing["phone"] = p["phone"]
                    if p.get("website"):
                        briefing["website"] = p["website"]
                    if p.get("formatted_address"):
                        briefing["address"] = p["formatted_address"]
                    if p.get("business_status") and p["business_status"] != "OPERATIONAL":
                        briefing["business_status"] = p["business_status"]
                    if p.get("hours_json"):
                        from app.etl.places import format_hours_today
                        hours_str = format_hours_today(p["hours_json"])
                        if hours_str:
                            briefing["hours_today"] = hours_str
            except Exception as e:
                logger.debug(f"Places enrichment skipped for briefing: {e}")
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
                limit=limit,
            )
            # Include scoring factors so Otto can explain WHY each dealer scored the way they did
            leads = []
            for lead in result["leads"]:
                entry = {
                    "id": lead["dealer_id"], "name": lead["name"],
                    "city": lead["city"], "state": lead["state"],
                    "score": lead["score"], "tier": lead["lead_tier"] if "lead_tier" in lead else lead["tier"],
                }
                if lead.get("lat"):
                    entry["lat"] = round(lead["lat"], 4)
                    entry["lng"] = round(lead["lng"], 4)
                f = lead.get("factors", {})
                if f:
                    why = {}
                    if f.get("fleet_scale") is not None:
                        why["fleet"] = f"{f['fleet_scale']}/20"
                    if f.get("match_pct") is not None:
                        why["fit"] = f"{f['match_pct']}%"
                    if f.get("product_fit") is not None:
                        why["fit_pts"] = f"{f['product_fit']}/25"
                    if f.get("smyrna_penetration") is not None:
                        why["pen_pts"] = f"{f['smyrna_penetration']}/30"
                    if f.get("penetration_pct") is not None:
                        why["pen_pct"] = f"{f['penetration_pct']}%"
                    if f.get("growth_signal") is not None:
                        why["growth_pts"] = f"{f['growth_signal']}/25"
                    if f.get("growth_pct") is not None:
                        why["growth_pct"] = f"{f['growth_pct']}%"
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
                # _get_route_dealers is async — run it synchronously
                result = asyncio.run(_get_route_dealers(
                    rep_id=rep_id,
                    travel_date=travel_dt,
                    buffer_miles=tool_input.get("buffer_miles", 20),
                    limit=30,
                ))
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
                # Include scoring factors for score explanation
                f = factors_map.get(d.get("dealer_id"), {})
                if f:
                    why = {}
                    if f.get("match_pct") is not None:
                        why["fit"] = f"{f['match_pct']}%"
                    if f.get("smyrna_penetration") is not None:
                        why["pen_pts"] = f"{f['smyrna_penetration']}/30"
                    if f.get("penetration_pct") is not None:
                        why["pen_pct"] = f"{f['penetration_pct']}%"
                    if f.get("growth_pct") is not None:
                        why["growth"] = f"{f['growth_pct']}%"
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
            # Parallelized: group 1 (briefing + snapshot) then group 2 (all enrichments)
            import concurrent.futures
            dealer_id = tool_input["dealer_id"]

            # Group 1: briefing + snapshot (needed by everything else)
            result = _get_briefing(dealer_id)
            d = result.dealer
            db = get_service_client()
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            snap_id = snap.data[0]["id"] if snap.data else None

            # Group 2: parallel enrichment queries
            def _fetch_score():
                if not snap_id:
                    return None
                scores = db.table("lead_scores").select("score, tier, factors").eq(
                    "dealer_id", dealer_id
                ).eq("snapshot_id", snap_id).execute()
                return scores.data[0] if scores.data else None

            def _fetch_trend():
                try:
                    trend = _get_dealer_trend(dealer_id=dealer_id)
                    if len(trend.points) >= 2:
                        latest = trend.points[-1]
                        ti = {
                            "months_tracked": len(trend.points),
                            "inventory_trend": trend.vehicle_trend,
                            "smyrna_trend": trend.smyrna_trend,
                            "rank_trend": trend.rank_trend,
                        }
                        if latest.vehicle_delta is not None:
                            ti["last_month_vehicle_change"] = latest.vehicle_delta
                        if latest.smyrna_delta is not None:
                            ti["last_month_smyrna_change"] = latest.smyrna_delta
                        if latest.rank_delta is not None:
                            ti["last_month_rank_change"] = latest.rank_delta
                        return ti
                except Exception as e:
                    logger.debug(f"Trend lookup skipped for {dealer_id}: {e}")
                return None

            def _fetch_places():
                try:
                    places_row = db.table("dealer_places").select(
                        "formatted_address"
                    ).eq("dealer_id", dealer_id).execute()
                    if places_row.data and places_row.data[0].get("formatted_address"):
                        return places_row.data[0]["formatted_address"]
                except Exception as e:
                    logger.debug(f"Places address lookup skipped for {dealer_id}: {e}")
                return None

            def _fetch_pricing():
                try:
                    if not snap_id:
                        return None
                    price_rows = db.table("vehicles").select("price").eq(
                        "snapshot_id", snap_id
                    ).eq("dealer_id", dealer_id).not_.is_("price", "null").execute()
                    if not price_rows.data:
                        return None
                    dealer_prices = [r["price"] for r in price_rows.data]
                    dealer_avg = round(sum(dealer_prices) / len(dealer_prices))
                    market_rows = db.table("vehicles").select(
                        "price, dealers!inner(state)"
                    ).eq("snapshot_id", snap_id).eq(
                        "dealers.state", d.state
                    ).not_.is_("price", "null").limit(3000).execute()
                    if not market_rows.data:
                        return None
                    market_prices = [r["price"] for r in market_rows.data]
                    market_avg = round(sum(market_prices) / len(market_prices))
                    diff_pct = round((dealer_avg - market_avg) / market_avg * 100)
                    return {
                        "dealer_avg": dealer_avg, "market_avg": market_avg,
                        "vs_market": f"{'+' if diff_pct > 0 else ''}{diff_pct}%",
                        "priced_units": len(dealer_prices), "diff_pct": diff_pct,
                    }
                except Exception as e:
                    logger.debug(f"Pricing context skipped for {dealer_id}: {e}")
                return None

            def _fetch_builders():
                try:
                    if not snap_id:
                        return None
                    builder_rows = db.table("vehicles").select("body_builder").eq(
                        "snapshot_id", snap_id
                    ).eq("dealer_id", dealer_id).not_.is_("body_builder", "null").execute()
                    if not builder_rows.data:
                        return None
                    builder_counts = {}
                    for r in builder_rows.data:
                        b = r["body_builder"]
                        builder_counts[b] = builder_counts.get(b, 0) + 1
                    sorted_builders = sorted(builder_counts.items(), key=lambda x: -x[1])[:5]
                    return [{"name": name, "count": ct} for name, ct in sorted_builders]
                except Exception as e:
                    logger.debug(f"Builder mix lookup skipped for {dealer_id}: {e}")
                return None

            def _fetch_velocity():
                try:
                    snaps_all = db.table("report_snapshots").select("id").order(
                        "report_date", desc=True
                    ).limit(2).execute()
                    if not snaps_all.data or len(snaps_all.data) < 2:
                        return None
                    diff_rows = db.table("vehicle_diffs").select("diff_type").eq(
                        "snapshot_id", snaps_all.data[0]["id"]
                    ).eq("dealer_id", dealer_id).execute()
                    if not diff_rows.data:
                        return None
                    velocity = {"new": 0, "sold": 0, "price_changes": 0}
                    for r in diff_rows.data:
                        if r["diff_type"] == "new":
                            velocity["new"] += 1
                        elif r["diff_type"] == "sold":
                            velocity["sold"] += 1
                        else:
                            velocity["price_changes"] += 1
                    return velocity
                except Exception as e:
                    logger.debug(f"Velocity calc skipped for {dealer_id}: {e}")
                return None

            # Run all enrichments in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                f_score = pool.submit(_fetch_score)
                f_trend = pool.submit(_fetch_trend)
                f_places = pool.submit(_fetch_places)
                f_pricing = pool.submit(_fetch_pricing)
                f_builders = pool.submit(_fetch_builders)
                f_velocity = pool.submit(_fetch_velocity)

            score_data = f_score.result()
            trend_intel = f_trend.result()
            full_address = f_places.result()
            pricing_data = f_pricing.result()
            builders_data = f_builders.result()
            velocity_data = f_velocity.result()

            # Build intel object
            intel = {
                "dealer": d.name,
                "address": full_address or f"{d.city}, {d.state}",
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
                intel["smyrna_status"] = "Zero Smyrna products"
                intel["body_type_overlap"] = matching
                intel["overlap_vehicles"] = total_match
                intel["talking_point"] = (
                    f"They have {total_match} vehicles in body types Smyrna builds "
                    f"but zero Smyrna product. That's {total_match} units of addressable opportunity. "
                    f"Lead with {matching[0] if matching else 'their top body type'} — it's their biggest Smyrna-compatible category."
                )

            if score_data:
                intel["lead_score"] = f"{score_data['score']}/100 ({score_data['tier']})"
                f = score_data.get("factors", {})
                if f:
                    intel["score_breakdown"] = {
                        "fleet_scale": f"{f.get('fleet_scale', 0)}/20",
                        "product_fit": f"{f.get('match_pct', 0)}% ({f.get('product_fit', 0)}/25 pts)",
                        "smyrna_penetration": f"{f.get('smyrna_penetration', 0)}/30 ({f.get('penetration_pct', 0)}% pen)",
                        "growth_signal": f"{f.get('growth_signal', 0)}/25",
                    }
                    if f.get("growth_pct") is not None:
                        intel["score_breakdown"]["growth_pct"] = f"{f['growth_pct']}%"

            if trend_intel:
                intel["trend"] = trend_intel
                if trend_intel["inventory_trend"] == "up":
                    intel["talking_point"] += " Their inventory is growing — they're actively buying."
                elif trend_intel["inventory_trend"] == "down":
                    intel["talking_point"] += " Their inventory has been shrinking — could mean tighter budgets or a pivot."
                if trend_intel["smyrna_trend"] == "up":
                    intel["talking_point"] += " Smyrna units are trending up — they're expanding the relationship."
                elif trend_intel["smyrna_trend"] == "down":
                    intel["talking_point"] += " Smyrna units are declining — worth asking why and if there's a retention issue."

            if pricing_data:
                intel["pricing"] = {k: v for k, v in pricing_data.items() if k != "diff_pct"}
                diff_pct = pricing_data["diff_pct"]
                if diff_pct > 5:
                    intel["talking_point"] += f" They price {diff_pct}% above {d.state} market — premium positioning."
                elif diff_pct < -5:
                    intel["talking_point"] += f" They price {abs(diff_pct)}% below {d.state} market — value buyer, lead with price."

            if builders_data:
                intel["body_builders"] = builders_data

            if velocity_data:
                intel["velocity"] = velocity_data

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

        elif tool_name == "get_dealer_places":
            db = get_service_client()

            if tool_input.get("dealer_id"):
                # Single dealer lookup from cache
                try:
                    row = db.table("dealer_places").select("*").eq(
                        "dealer_id", tool_input["dealer_id"]
                    ).execute()
                except Exception as e:
                    if "PGRST205" in str(e) or "dealer_places" in str(e):
                        return json.dumps({"status": "no_data", "hint": "dealer_places table not found — run migration 006."})
                    raise
                if not row.data:
                    return json.dumps({
                        "status": "no_data",
                        "hint": "No Google Places data cached for this dealer yet. "
                                "Places data is fetched on first briefing request or via bulk enrichment.",
                    })
                p = row.data[0]
                from app.etl.places import format_hours_today
                result = {
                    "rating": float(p["rating"]) if p.get("rating") else None,
                    "reviews": p.get("review_count"),
                    "phone": p.get("phone"),
                    "website": p.get("website"),
                    "maps_url": p.get("google_maps_url"),
                    "status": p.get("business_status"),
                    "address": p.get("formatted_address"),
                    "hours_today": format_hours_today(p.get("hours_json")),
                }
                return json.dumps(
                    {k: v for k, v in result.items() if v is not None}, default=str
                )

            elif tool_input.get("min_rating"):
                # Multi-dealer filter by rating
                try:
                    query = db.table("dealer_places").select(
                        "dealer_id, rating, review_count"
                    ).gte("rating", tool_input["min_rating"]).order(
                        "rating", desc=True
                    ).limit(20)
                    places_rows = query.execute()
                except Exception as e:
                    if "PGRST205" in str(e) or "dealer_places" in str(e):
                        return json.dumps({"dealers": [], "total": 0, "hint": "dealer_places table not found — run migration 006."})
                    raise

                if not places_rows.data:
                    return json.dumps({"dealers": [], "total": 0})

                # Fetch dealer names for matched IDs
                matched_ids = [r["dealer_id"] for r in places_rows.data]
                dealer_rows = db.table("dealers").select(
                    "id, name, city, state"
                ).in_("id", matched_ids).execute()
                dealer_map = {r["id"]: r for r in (dealer_rows.data or [])}

                dealers = []
                for r in places_rows.data:
                    d = dealer_map.get(r["dealer_id"], {})
                    if tool_input.get("state") and d.get("state") != tool_input["state"].upper():
                        continue
                    dealers.append({
                        "name": d.get("name"),
                        "city": d.get("city"),
                        "state": d.get("state"),
                        "rating": float(r["rating"]) if r.get("rating") else None,
                        "reviews": r.get("review_count"),
                        "dealer_id": r["dealer_id"],
                    })
                return json.dumps({"dealers": dealers, "total": len(dealers)}, default=str)

            else:
                return json.dumps({"error": "Provide dealer_id or min_rating"})

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
                "dealer_id, score, tier, factors, "
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
                            why["fit"] = f"{f['match_pct']}%"
                        if f.get("smyrna_penetration") is not None:
                            why["pen_pts"] = f"{f['smyrna_penetration']}/30"
                        if f.get("penetration_pct") is not None:
                            why["pen_pct"] = f"{f['penetration_pct']}%"
                        if f.get("growth_pct") is not None:
                            why["growth"] = f"{f['growth_pct']}%"
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

        elif tool_name == "search_vehicles":
            db = get_service_client()
            # Get latest snapshot
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            if not snap.data:
                return json.dumps({"error": "No data snapshots found."})
            snap_id = snap.data[0]["id"]

            limit = min(tool_input.get("limit", 15), 30)

            # Use the search_vehicles PostgreSQL function
            params = {"p_snapshot_id": snap_id, "p_limit": limit}
            if tool_input.get("brand"):
                params["p_brand"] = tool_input["brand"]
            if tool_input.get("body_type"):
                params["p_body_type"] = tool_input["body_type"]
            if tool_input.get("min_price"):
                params["p_min_price"] = tool_input["min_price"]
            if tool_input.get("max_price"):
                params["p_max_price"] = tool_input["max_price"]
            if tool_input.get("state"):
                params["p_state"] = tool_input["state"]
            if tool_input.get("dealer_id"):
                params["p_dealer_id"] = tool_input["dealer_id"]
            if tool_input.get("is_smyrna") is not None:
                params["p_is_smyrna"] = tool_input["is_smyrna"]

            result = db.rpc("search_vehicles", params).execute()
            vehicles = []
            for v in (result.data or []):
                # Skip used vehicles — Comvoy only sells new
                if v.get("condition", "").lower() == "used":
                    continue
                entry = {
                    "vin": v["vin"],
                    "brand": v["brand"],
                    "body_type": v["body_type"],
                    "dealer": v["dealer_name"],
                    "city": v["city"],
                    "state": v["state"],
                }
                if v.get("model"):
                    entry["model"] = v["model"]
                if v.get("body_builder"):
                    entry["builder"] = v["body_builder"]
                if v.get("price"):
                    entry["price"] = v["price"]
                if v.get("condition"):
                    entry["cond"] = v["condition"]
                if v.get("is_smyrna"):
                    entry["smyrna"] = True
                if v.get("listing_url"):
                    entry["url"] = v["listing_url"]
                vehicles.append(entry)

            return json.dumps({"vehicles": vehicles, "count": len(vehicles)}, default=str)

        elif tool_name == "get_dealer_inventory":
            db = get_service_client()
            dealer_id = tool_input["dealer_id"]

            # Get latest snapshot
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            if not snap.data:
                return json.dumps({"error": "No data snapshots found."})
            snap_id = snap.data[0]["id"]

            # Count total matching vehicles first (new only)
            count_query = db.table("vehicles").select(
                "id", count="exact"
            ).eq("snapshot_id", snap_id).eq("dealer_id", dealer_id).eq("condition", "New")

            query = db.table("vehicles").select(
                "vin, brand, model, body_type, body_builder, price, condition, "
                "transmission, fuel_type, color, is_smyrna, listing_url"
            ).eq("snapshot_id", snap_id).eq("dealer_id", dealer_id).eq("condition", "New").order("price", desc=False, nulls_last=True)

            if tool_input.get("brand"):
                count_query = count_query.ilike("brand", tool_input["brand"])
                query = query.ilike("brand", tool_input["brand"])
            if tool_input.get("body_type"):
                count_query = count_query.ilike("body_type", f"%{tool_input['body_type']}%")
                query = query.ilike("body_type", f"%{tool_input['body_type']}%")

            count_result = count_query.execute()
            total_available = count_result.count if count_result.count is not None else 0
            result = query.limit(50).execute()

            vehicles = []
            for v in (result.data or []):
                entry = {"vin": v["vin"], "brand": v["brand"], "body_type": v["body_type"]}
                if v.get("model"):
                    entry["model"] = v["model"]
                if v.get("body_builder"):
                    entry["builder"] = v["body_builder"]
                if v.get("price"):
                    entry["price"] = v["price"]
                if v.get("condition"):
                    entry["cond"] = v["condition"]
                if v.get("transmission"):
                    entry["trans"] = v["transmission"]
                if v.get("fuel_type"):
                    entry["fuel"] = v["fuel_type"]
                if v.get("color"):
                    entry["color"] = v["color"]
                if v.get("is_smyrna"):
                    entry["smyrna"] = True
                if v.get("listing_url"):
                    entry["url"] = v["listing_url"]
                vehicles.append(entry)

            # Get dealer name for context
            dealer_row = db.table("dealers").select("name, city, state").eq("id", dealer_id).limit(1).execute()
            dealer_info = dealer_row.data[0] if dealer_row.data else {}

            resp = {
                "dealer": f"{dealer_info.get('name', '?')} ({dealer_info.get('city', '')}, {dealer_info.get('state', '')})",
                "vehicles": vehicles,
                "count": len(vehicles),
                "total_available": total_available,
            }
            if total_available > 50:
                resp["note"] = f"Showing 50 of {total_available}. Use brand/body_type filters to narrow."
            return json.dumps(resp, default=str)

        elif tool_name == "get_inventory_changes":
            db = get_service_client()

            # Get latest two snapshots
            snaps = db.table("report_snapshots").select("id, report_date").order("report_date", desc=True).limit(2).execute()
            if not snaps.data or len(snaps.data) < 2:
                return json.dumps({"error": "Need at least 2 monthly snapshots to show changes. Only one snapshot exists."})

            current_snap = snaps.data[0]
            prev_snap = snaps.data[1]

            limit = min(tool_input.get("limit", 20), 50)

            # Build base filter (shared between count and detail queries)
            def _apply_filters(q):
                if tool_input.get("change_type"):
                    q = q.eq("diff_type", tool_input["change_type"])
                if tool_input.get("brand"):
                    q = q.ilike("brand", tool_input["brand"])
                if tool_input.get("state"):
                    q = q.eq("dealers.state", tool_input["state"].upper())
                if tool_input.get("dealer_id"):
                    q = q.eq("dealer_id", tool_input["dealer_id"])
                return q

            # Get true summary counts from full dataset (no limit)
            summary_query = db.table("vehicle_diffs").select(
                "diff_type"
            ).eq("snapshot_id", current_snap["id"])
            # Apply non-join filters for summary (skip state filter as it requires join)
            if tool_input.get("change_type"):
                summary_query = summary_query.eq("diff_type", tool_input["change_type"])
            if tool_input.get("brand"):
                summary_query = summary_query.ilike("brand", tool_input["brand"])
            if tool_input.get("dealer_id"):
                summary_query = summary_query.eq("dealer_id", tool_input["dealer_id"])
            summary_result = summary_query.execute()
            type_counts = {}
            for r in (summary_result.data or []):
                dt = r["diff_type"]
                type_counts[dt] = type_counts.get(dt, 0) + 1
            total_changes = sum(type_counts.values())

            # Get detailed changes (with limit)
            query = db.table("vehicle_diffs").select(
                "diff_type, vin, brand, body_type, old_price, new_price, "
                "dealer_id, dealers!inner(name, city, state)"
            ).eq("snapshot_id", current_snap["id"])
            query = _apply_filters(query)
            result = query.limit(limit).execute()

            changes = []
            for r in (result.data or []):
                d = r.get("dealers", {})
                entry = {
                    "type": r["diff_type"],
                    "vin": r["vin"],
                    "brand": r.get("brand"),
                    "body_type": r.get("body_type"),
                    "dealer": d.get("name"),
                    "location": f"{d.get('city', '')}, {d.get('state', '')}",
                }
                if r["diff_type"] == "price_change":
                    entry["old_price"] = r.get("old_price")
                    entry["new_price"] = r.get("new_price")
                elif r["diff_type"] == "new" and r.get("new_price"):
                    entry["price"] = r["new_price"]
                elif r["diff_type"] == "sold" and r.get("old_price"):
                    entry["was_price"] = r["old_price"]
                changes.append(entry)

            resp = {
                "period": f"{prev_snap['report_date']} → {current_snap['report_date']}",
                "summary": type_counts,
                "total_changes": total_changes,
                "changes": changes,
                "showing": len(changes),
            }
            if total_changes > limit:
                resp["note"] = f"Showing {len(changes)} of {total_changes} total changes."
            return json.dumps(resp, default=str)

        elif tool_name == "get_price_analytics":
            db = get_service_client()
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            if not snap.data:
                return json.dumps({"error": "No data snapshots found."})
            snap_id = snap.data[0]["id"]

            def _price_stats(prices):
                if not prices:
                    return None
                prices.sort()
                n = len(prices)
                return {
                    "avg": round(sum(prices) / n),
                    "min": prices[0],
                    "max": prices[-1],
                    "median": prices[n // 2] if n % 2 else round((prices[n // 2 - 1] + prices[n // 2]) / 2),
                    "count": n,
                }

            def _brackets(prices):
                b = {"<30k": 0, "30-50k": 0, "50-75k": 0, "75-100k": 0, "100k+": 0}
                for p in prices:
                    if p < 30000: b["<30k"] += 1
                    elif p < 50000: b["30-50k"] += 1
                    elif p < 75000: b["50-75k"] += 1
                    elif p < 100000: b["75-100k"] += 1
                    else: b["100k+"] += 1
                return {k: v for k, v in b.items() if v > 0}

            # Paginate market query (Supabase default limit is 1000)
            def _build_price_query(db, snap_id, tool_input, offset, page_size):
                if tool_input.get("state"):
                    q = db.table("vehicles").select(
                        "price, dealers!inner(state)"
                    ).eq("snapshot_id", snap_id).eq("condition", "New").not_.is_("price", "null").eq(
                        "dealers.state", tool_input["state"].upper()
                    )
                else:
                    q = db.table("vehicles").select("price").eq(
                        "snapshot_id", snap_id
                    ).eq("condition", "New").not_.is_("price", "null")
                if tool_input.get("brand"):
                    q = q.ilike("brand", tool_input["brand"])
                if tool_input.get("body_type"):
                    q = q.ilike("body_type", f"%{tool_input['body_type']}%")
                if tool_input.get("condition"):
                    q = q.ilike("condition", tool_input["condition"])
                return q.range(offset, offset + page_size - 1)

            PRICE_FLOOR = 5000  # filter junk/placeholder prices
            market_prices = []
            offset = 0
            page_size = 1000
            while True:
                q = _build_price_query(db, snap_id, tool_input, offset, page_size)
                page = q.execute()
                if not page.data:
                    break
                market_prices.extend(r["price"] for r in page.data if r["price"] >= PRICE_FLOOR)
                if len(page.data) < page_size:
                    break
                offset += page_size

            if not market_prices:
                return json.dumps({"note": "No priced vehicles match these filters."})

            output = {
                "filters": {k: v for k, v in {
                    "brand": tool_input.get("brand"),
                    "body_type": tool_input.get("body_type"),
                    "state": tool_input.get("state"),
                    "condition": tool_input.get("condition"),
                }.items() if v},
                "market": _price_stats(market_prices),
                "brackets": _brackets(market_prices),
            }

            if len(market_prices) < 20:
                output["note"] = f"Small sample size ({len(market_prices)} vehicles)"

            # Dealer comparison if requested
            if tool_input.get("dealer_id"):
                dealer_q = db.table("vehicles").select("price").eq(
                    "snapshot_id", snap_id
                ).eq("dealer_id", tool_input["dealer_id"]).eq("condition", "New").not_.is_("price", "null")
                if tool_input.get("brand"):
                    dealer_q = dealer_q.ilike("brand", tool_input["brand"])
                if tool_input.get("body_type"):
                    dealer_q = dealer_q.ilike("body_type", f"%{tool_input['body_type']}%")
                if tool_input.get("condition"):
                    dealer_q = dealer_q.ilike("condition", tool_input["condition"])

                dealer_result = dealer_q.limit(500).execute()
                dealer_prices = [r["price"] for r in (dealer_result.data or [])]

                if dealer_prices:
                    dealer_stats = _price_stats(dealer_prices)
                    market_avg = output["market"]["avg"]
                    diff_pct = round((dealer_stats["avg"] - market_avg) / market_avg * 100)
                    dealer_stats["vs_market"] = f"{'+' if diff_pct > 0 else ''}{diff_pct}%"
                    # Get dealer name
                    d_row = db.table("dealers").select("name, city, state").eq(
                        "id", tool_input["dealer_id"]
                    ).limit(1).execute()
                    if d_row.data:
                        dealer_stats["name"] = f"{d_row.data[0]['name']} ({d_row.data[0]['city']}, {d_row.data[0]['state']})"
                    output["dealer"] = dealer_stats

            return json.dumps(output, default=str)

        elif tool_name == "get_market_intel":
            db = get_service_client()
            snap = db.table("report_snapshots").select("id").order("report_date", desc=True).limit(1).execute()
            if not snap.data:
                return json.dumps({"error": "No data snapshots found."})
            snap_id = snap.data[0]["id"]

            report_type = tool_input["report_type"]
            limit = min(tool_input.get("limit", 15), 30)

            # Paginate to get all vehicles — new only (Supabase default limit is 1000)
            data = []
            page_size = 1000
            offset = 0
            while True:
                if tool_input.get("state"):
                    q = db.table("vehicles").select(
                        "brand, body_type, body_builder, price, is_smyrna, dealers!inner(state)"
                    ).eq("snapshot_id", snap_id).eq("condition", "New").eq("dealers.state", tool_input["state"].upper())
                else:
                    q = db.table("vehicles").select(
                        "brand, body_type, body_builder, price, is_smyrna"
                    ).eq("snapshot_id", snap_id).eq("condition", "New")
                if tool_input.get("body_type"):
                    q = q.ilike("body_type", f"%{tool_input['body_type']}%")
                q = q.range(offset, offset + page_size - 1)
                page = q.execute()
                if not page.data:
                    break
                data.extend(page.data)
                if len(page.data) < page_size:
                    break
                offset += page_size
            total = len(data)

            if not data:
                return json.dumps({"note": "No vehicles match these filters."})

            from app.api.scoring import SMYRNA_BODY_TYPES

            output = {
                "report": report_type,
                "scope": tool_input.get("state", "all states"),
                "total_vehicles": total,
            }

            if report_type == "body_builders":
                counts = {}
                for r in data:
                    bb = r.get("body_builder")
                    if bb:
                        counts[bb] = counts.get(bb, 0) + 1
                sorted_bb = sorted(counts.items(), key=lambda x: -x[1])[:limit]
                with_builder = sum(counts.values())
                output["with_builder"] = with_builder
                output["top"] = [
                    {"builder": name, "count": ct, "share": f"{round(ct / with_builder * 100, 1)}%"}
                    for name, ct in sorted_bb
                ]
                # Find Smyrna position
                smyrna_names = {"Smyrna Truck", "Smyrna", "Fouts Bros", "Fouts Brothers"}
                smyrna_count = sum(ct for name, ct in counts.items() if name in smyrna_names)
                if smyrna_count:
                    smyrna_rank = sum(1 for _, ct in counts.items() if ct > smyrna_count) + 1
                    output["smyrna_position"] = {
                        "count": smyrna_count, "share": f"{round(smyrna_count / with_builder * 100, 1)}%",
                        "rank": smyrna_rank,
                    }

            elif report_type == "brands":
                brand_data = {}
                for r in data:
                    b = r["brand"]
                    if b not in brand_data:
                        brand_data[b] = {"count": 0, "prices": []}
                    brand_data[b]["count"] += 1
                    if r.get("price"):
                        brand_data[b]["prices"].append(r["price"])
                sorted_brands = sorted(brand_data.items(), key=lambda x: -x[1]["count"])[:limit]
                output["top"] = []
                for name, info in sorted_brands:
                    entry = {
                        "brand": name,
                        "count": info["count"],
                        "share": f"{round(info['count'] / total * 100, 1)}%",
                    }
                    if info["prices"]:
                        entry["avg_price"] = round(sum(info["prices"]) / len(info["prices"]))
                    output["top"].append(entry)

            elif report_type == "body_types":
                bt_data = {}
                for r in data:
                    bt = r["body_type"]
                    if bt not in bt_data:
                        bt_data[bt] = {"count": 0, "smyrna": 0}
                    bt_data[bt]["count"] += 1
                    if r.get("is_smyrna"):
                        bt_data[bt]["smyrna"] += 1
                sorted_bt = sorted(bt_data.items(), key=lambda x: -x[1]["count"])[:limit]
                output["top"] = []
                for name, info in sorted_bt:
                    entry = {
                        "type": name,
                        "count": info["count"],
                        "share": f"{round(info['count'] / total * 100, 1)}%",
                        "smyrna_compatible": name in SMYRNA_BODY_TYPES,
                    }
                    if info["smyrna"]:
                        entry["smyrna_units"] = info["smyrna"]
                    output["top"].append(entry)

            return json.dumps(output, default=str)

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as e:
        logger.exception(f"Tool execution failed: {tool_name}")
        return json.dumps({"error": str(e)})
