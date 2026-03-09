"""Google Directions API integration for real driving routes.

Fetches the actual road polyline between two points and converts it
to a WKT LINESTRING for PostGIS corridor searches.

Falls back gracefully if API key is missing or request fails.
"""

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
