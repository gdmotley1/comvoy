"""Google Maps API integration — Directions, Distance Matrix, route optimization.

- Directions API: real driving polylines for PostGIS corridor searches
- Distance Matrix API: drive times between multiple points
- TSP optimizer: nearest-neighbor + 2-opt for stop ordering

Falls back gracefully if API key is missing or request fails.
"""

import asyncio
import logging
import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# Google's encoded polyline algorithm:
# https://developers.google.com/maps/documentation/utilities/polylinealgorithm


def decode_polyline(encoded: str) -> list[tuple[float, float]]:
    """Decode a Google encoded polyline string into (lat, lng) pairs."""
    points = []
    index = 0
    lat = 0
    lng = 0
    length = len(encoded)

    while index < length:
        # Decode latitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        lat += (~(result >> 1) if (result & 1) else (result >> 1))

        # Decode longitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        lng += (~(result >> 1) if (result & 1) else (result >> 1))

        points.append((lat / 1e5, lng / 1e5))

    return points


def points_to_wkt(points: list[tuple[float, float]]) -> str:
    """Convert (lat, lng) points to WKT LINESTRING(lng lat, ...).

    PostGIS expects longitude first, then latitude.
    """
    if len(points) < 2:
        return ""
    coords = ", ".join(f"{lng} {lat}" for lat, lng in points)
    return f"LINESTRING({coords})"


def simplify_points(points: list[tuple[float, float]], max_points: int = 200) -> list[tuple[float, float]]:
    """Reduce point count to stay within PostGIS performance limits.

    Keeps first, last, and evenly spaced intermediate points.
    Google polylines can have 1000+ points for long routes.
    """
    if len(points) <= max_points:
        return points

    step = (len(points) - 1) / (max_points - 1)
    indices = [round(i * step) for i in range(max_points)]
    # Ensure last point is included
    indices[-1] = len(points) - 1
    return [points[i] for i in indices]


async def get_driving_route(
    start_lat: float, start_lng: float,
    end_lat: float, end_lng: float,
) -> str | None:
    """Fetch real driving route from Google Directions API.

    Returns WKT LINESTRING or None if unavailable.
    """
    if not settings.google_maps_api_key:
        logger.debug("No Google Maps API key — skipping driving route")
        return None

    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": f"{start_lat},{start_lng}",
        "destination": f"{end_lat},{end_lng}",
        "key": settings.google_maps_api_key,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            data = resp.json()

        if data.get("status") != "OK":
            logger.warning(f"Google Directions API error: {data.get('status')} "
                           f"— {data.get('error_message', 'no details')}")
            return None

        # Extract the overview polyline (simplified route geometry)
        routes = data.get("routes", [])
        if not routes:
            logger.warning("Google Directions returned no routes")
            return None

        encoded = routes[0].get("overview_polyline", {}).get("points", "")
        if not encoded:
            logger.warning("No polyline in Google Directions response")
            return None

        # Decode → simplify → convert to WKT
        points = decode_polyline(encoded)
        points = simplify_points(points, max_points=200)
        wkt = points_to_wkt(points)

        logger.info(f"Driving route: {len(points)} points, "
                     f"{start_lat:.2f},{start_lng:.2f} -> {end_lat:.2f},{end_lng:.2f}")
        return wkt

    except httpx.TimeoutException:
        logger.warning("Google Directions API timeout")
        return None
    except Exception as e:
        logger.error(f"Google Directions API error: {e}")
        return None


def get_driving_route_sync(
    start_lat: float, start_lng: float,
    end_lat: float, end_lng: float,
) -> str | None:
    """Synchronous version for use in BackgroundTasks / non-async contexts."""
    if not settings.google_maps_api_key:
        return None

    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": f"{start_lat},{start_lng}",
        "destination": f"{end_lat},{end_lng}",
        "key": settings.google_maps_api_key,
    }

    try:
        resp = httpx.get(url, params=params, timeout=10)
        data = resp.json()

        if data.get("status") != "OK":
            logger.warning(f"Google Directions API error: {data.get('status')}")
            return None

        routes = data.get("routes", [])
        if not routes:
            return None

        encoded = routes[0].get("overview_polyline", {}).get("points", "")
        if not encoded:
            return None

        points = decode_polyline(encoded)
        points = simplify_points(points, max_points=200)
        wkt = points_to_wkt(points)

        logger.info(f"Driving route (sync): {len(points)} points")
        return wkt

    except Exception as e:
        logger.error(f"Google Directions sync error: {e}")
        return None


# ---------------------------------------------------------------------------
# Distance Matrix API
# ---------------------------------------------------------------------------

async def get_distance_matrix(
    origins: list[tuple[float, float]],
    destinations: list[tuple[float, float]],
) -> dict | None:
    """Fetch driving time + distance matrix from Google Distance Matrix API.

    Args:
        origins: List of (lat, lng) tuples.
        destinations: List of (lat, lng) tuples.

    Returns:
        {"rows": [{"elements": [{"duration_secs": int, "distance_meters": int, "status": str}]}]}
        or None if unavailable.

    Cost: $5 per 1000 elements. Max 25 origins x 25 destinations = 625 elements.
    """
    if not settings.google_maps_api_key:
        logger.debug("No Google Maps API key — skipping distance matrix")
        return None

    num_elements = len(origins) * len(destinations)
    if num_elements > 625:
        logger.warning(f"Distance matrix too large: {len(origins)}x{len(destinations)} = {num_elements}")
        return None

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": "|".join(f"{lat},{lng}" for lat, lng in origins),
        "destinations": "|".join(f"{lat},{lng}" for lat, lng in destinations),
        "mode": "driving",
        "key": settings.google_maps_api_key,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)
            data = resp.json()

        if data.get("status") != "OK":
            logger.warning(f"Distance Matrix API error: {data.get('status')}")
            return None

        rows = []
        for row in data.get("rows", []):
            elements = []
            for elem in row.get("elements", []):
                elements.append({
                    "duration_secs": elem.get("duration", {}).get("value", 0),
                    "distance_meters": elem.get("distance", {}).get("value", 0),
                    "status": elem.get("status", "UNKNOWN"),
                })
            rows.append({"elements": elements})

        return {"rows": rows}

    except httpx.TimeoutException:
        logger.warning("Distance Matrix API timeout")
        return None
    except Exception as e:
        logger.error(f"Distance Matrix API error: {e}")
        return None


def get_distance_matrix_sync(
    origins: list[tuple[float, float]],
    destinations: list[tuple[float, float]],
) -> dict | None:
    """Synchronous wrapper for distance matrix."""
    try:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(get_distance_matrix(origins, destinations))
        loop.close()
        return result
    except Exception as e:
        logger.error(f"Distance matrix sync error: {e}")
        return None


# ---------------------------------------------------------------------------
# TSP optimizer — nearest-neighbor + 2-opt
# ---------------------------------------------------------------------------

def _total_time(
    time_matrix: list[list[int]],
    start_idx: int,
    end_idx: int,
    order: list[int],
) -> int:
    """Compute total drive time for a given stop ordering."""
    total = 0
    prev = start_idx
    for idx in order:
        total += time_matrix[prev][idx]
        prev = idx
    total += time_matrix[prev][end_idx]
    return total


def _nearest_neighbor_tsp(
    time_matrix: list[list[int]],
    start_idx: int,
    end_idx: int,
    stop_indices: list[int],
) -> list[int]:
    """Greedy nearest-neighbor TSP. Returns ordered list of stop indices."""
    remaining = set(stop_indices)
    order = []
    current = start_idx

    while remaining:
        nearest = min(remaining, key=lambda i: time_matrix[current][i])
        order.append(nearest)
        remaining.remove(nearest)
        current = nearest

    return order


def _two_opt(
    time_matrix: list[list[int]],
    order: list[int],
    start_idx: int,
    end_idx: int,
) -> list[int]:
    """2-opt local search to improve stop ordering."""
    improved = True
    best = list(order)

    while improved:
        improved = False
        for i in range(len(best) - 1):
            for j in range(i + 1, len(best)):
                candidate = best[:i] + list(reversed(best[i:j + 1])) + best[j + 1:]
                if _total_time(time_matrix, start_idx, end_idx, candidate) < \
                   _total_time(time_matrix, start_idx, end_idx, best):
                    best = candidate
                    improved = True

    return best


async def optimize_stop_order(
    start: tuple[float, float],
    end: tuple[float, float],
    stops: list[dict],
) -> list[dict] | None:
    """Optimize visiting order using Distance Matrix + TSP heuristics.

    Args:
        start: (lat, lng) of trip start.
        end: (lat, lng) of trip end.
        stops: List of dicts with at minimum 'lat' and 'lng' keys.

    Returns:
        Reordered list of stops with added 'drive_time_min' and 'drive_dist_mi',
        or None if optimization unavailable.
    """
    if not stops:
        return []
    if len(stops) == 1:
        # Still fetch drive time for the single stop
        matrix = await get_distance_matrix(
            [start, (stops[0]["lat"], stops[0]["lng"])],
            [start, (stops[0]["lat"], stops[0]["lng"])],
        )
        if matrix:
            stops[0]["drive_time_min"] = round(
                matrix["rows"][0]["elements"][1]["duration_secs"] / 60
            )
            stops[0]["drive_dist_mi"] = round(
                matrix["rows"][0]["elements"][1]["distance_meters"] / 1609.34, 1
            )
        return stops

    # Cap at 23 stops + start + end = 25 (API limit per dimension)
    if len(stops) > 23:
        stops = stops[:23]

    # Build point list: [start, stop0, stop1, ..., stopN, end]
    all_points = [start] + [(s["lat"], s["lng"]) for s in stops] + [end]

    matrix = await get_distance_matrix(all_points, all_points)
    if not matrix:
        return None

    # Build time matrix (seconds)
    n = len(all_points)
    time_matrix = []
    for row in matrix["rows"]:
        time_matrix.append([e["duration_secs"] for e in row["elements"]])

    # TSP: nearest-neighbor → 2-opt improvement
    stop_indices = list(range(1, n - 1))
    order = _nearest_neighbor_tsp(time_matrix, 0, n - 1, stop_indices)
    order = _two_opt(time_matrix, order, 0, n - 1)

    # Build result with inter-stop drive times
    result = []
    prev_idx = 0  # start
    for idx in order:
        stop = stops[idx - 1].copy()
        stop["drive_time_min"] = round(time_matrix[prev_idx][idx] / 60)
        stop["drive_dist_mi"] = round(
            matrix["rows"][prev_idx]["elements"][idx]["distance_meters"] / 1609.34, 1
        )
        result.append(stop)
        prev_idx = idx

    # Add final leg time (last stop → end)
    if result:
        last_to_end = time_matrix[prev_idx][n - 1]
        result[-1]["drive_to_end_min"] = round(last_to_end / 60)

    total_secs = _total_time(time_matrix, 0, n - 1, order)
    logger.info(f"Optimized {len(result)} stops: {round(total_secs / 60)} min total drive time")

    return result


def optimize_stop_order_sync(
    start: tuple[float, float],
    end: tuple[float, float],
    stops: list[dict],
) -> list[dict] | None:
    """Synchronous wrapper for stop optimization."""
    try:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(optimize_stop_order(start, end, stops))
        loop.close()
        return result
    except Exception as e:
        logger.error(f"Stop optimization sync error: {e}")
        return None
