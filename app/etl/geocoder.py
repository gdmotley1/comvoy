"""
Geocode dealers by city + state using Nominatim (OpenStreetMap).
Free, no API key required. Rate limited to 1 request/second per Nominatim policy.
"""

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


async def geocode_dealers(dealers: list[dict]) -> list[dict]:
    """Add latitude/longitude to dealer dicts missing coordinates.

    Deduplicates by city+state to minimize API calls (~200 unique cities
    out of 588 dealers). At 1 req/sec, takes ~3-4 minutes.
    """
    location_cache: dict[str, tuple[float, float]] = {}

    async with httpx.AsyncClient(timeout=15.0) as client:
        for dealer in dealers:
            if dealer.get("latitude") and dealer.get("longitude"):
                continue

            city = dealer.get("city", "")
            state = dealer.get("state", "")
            cache_key = f"{city}, {state}"

            if cache_key in location_cache:
                lat, lng = location_cache[cache_key]
                dealer["latitude"] = lat
                dealer["longitude"] = lng
                continue

            coords = await _geocode_one(client, city, state)
            if coords:
                location_cache[cache_key] = coords
                dealer["latitude"] = coords[0]
                dealer["longitude"] = coords[1]
            else:
                logger.warning(f"Failed to geocode: {cache_key}")

            # Nominatim requires max 1 request/second
            await asyncio.sleep(1.1)

    geocoded = sum(1 for d in dealers if d.get("latitude"))
    logger.info(f"Geocoded {geocoded}/{len(dealers)} dealers ({len(location_cache)} unique cities)")
    return dealers


async def _geocode_one(client: httpx.AsyncClient, city: str, state: str) -> tuple[float, float] | None:
    """Geocode a single city+state via Nominatim. Returns (lat, lng) or None."""
    try:
        resp = await client.get(NOMINATIM_URL, params={
            "q": f"{city}, {state}, USA",
            "format": "json",
            "limit": 1,
            "countrycodes": "us",
        }, headers={
            "User-Agent": "ComvoySalesIntelligence/1.0",
        })
        data = resp.json()
        if data:
            return (float(data[0]["lat"]), float(data[0]["lon"]))
    except Exception as e:
        logger.error(f"Geocoding error for {city}, {state}: {e}")
    return None


async def geocode_single(city: str, state: str) -> tuple[float, float] | None:
    """Geocode a single city+state pair. For use outside of bulk ingest."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        return await _geocode_one(client, city, state)
