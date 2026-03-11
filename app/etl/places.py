"""Google Places API (New) integration — enrich dealers with business metadata.

Uses Places API (New) Text Search to find a dealer by name + location,
then caches rating, hours, phone, website, photos in Supabase.

Caches for 30 days. Lazy fetch on first request per dealer.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from app.config import settings
from app.database import get_service_client

logger = logging.getLogger(__name__)

PLACES_API_BASE = "https://places.googleapis.com/v1"
CACHE_TTL_DAYS = 30

# Field mask controls cost — only request fields we actually use
SEARCH_FIELD_MASK = ",".join([
    "places.id",
    "places.rating",
    "places.userRatingCount",
    "places.regularOpeningHours",
    "places.nationalPhoneNumber",
    "places.websiteUri",
    "places.googleMapsUri",
    "places.photos",
    "places.formattedAddress",
    "places.businessStatus",
    "places.displayName",
])


def _is_cache_fresh(fetched_at: str | None) -> bool:
    """Check if cached data is within TTL."""
    if not fetched_at:
        return False
    if isinstance(fetched_at, str):
        fetched = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    else:
        fetched = fetched_at
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - fetched < timedelta(days=CACHE_TTL_DAYS)


def _parse_place(place: dict) -> dict:
    """Extract the fields we care about from a Places API response."""
    photos = place.get("photos", [])
    photo_refs = [
        {"name": p["name"], "width": p.get("widthPx"), "height": p.get("heightPx")}
        for p in photos[:3]
    ]

    hours = place.get("regularOpeningHours")

    return {
        "google_place_id": place.get("id"),
        "rating": place.get("rating"),
        "review_count": place.get("userRatingCount"),
        "phone": place.get("nationalPhoneNumber"),
        "website": place.get("websiteUri"),
        "google_maps_url": place.get("googleMapsUri"),
        "formatted_address": place.get("formattedAddress"),
        "hours_json": hours,
        "photos_json": photo_refs if photo_refs else None,
        "business_status": place.get("businessStatus", "OPERATIONAL"),
    }


async def search_place(
    name: str, city: str, state: str,
    lat: float | None = None, lng: float | None = None,
) -> dict | None:
    """Find a business via Google Places Text Search.

    Returns parsed place dict or None if unavailable.
    """
    if not settings.google_maps_api_key:
        logger.debug("No Google Maps API key — skipping Places search")
        return None

    url = f"{PLACES_API_BASE}/places:searchText"
    headers = {
        "X-Goog-Api-Key": settings.google_maps_api_key,
        "X-Goog-FieldMask": SEARCH_FIELD_MASK,
        "Content-Type": "application/json",
    }

    body: dict = {
        "textQuery": f"{name}, {city}, {state}",
        "languageCode": "en",
    }

    # Location bias if we have coordinates
    if lat and lng:
        body["locationBias"] = {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": 50000.0,
            }
        }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, headers=headers, json=body)
            data = resp.json()

        places = data.get("places", [])
        if not places:
            logger.info(f"No Places result for: {name}, {city}, {state}")
            return None

        result = _parse_place(places[0])
        logger.info(f"Places found: {name} — rating {result.get('rating')}, "
                     f"{result.get('review_count')} reviews")
        return result

    except httpx.TimeoutException:
        logger.warning(f"Places API timeout for: {name}")
        return None
    except Exception as e:
        logger.error(f"Places API error for {name}: {e}")
        return None


async def get_dealer_places(dealer_id: str, force_refresh: bool = False) -> dict | None:
    """Get Places data for a dealer, with 30-day cache.

    Lazy-fetches from Google if cache is stale or missing.
    Returns dict with rating, phone, website, hours, etc. or None.
    """
    db = get_service_client()

    # Check cache
    if not force_refresh:
        try:
            cached = db.table("dealer_places").select(
                "dealer_id, rating, review_count, phone, website, hours_json, "
                "business_status, photo_refs, fetched_at"
            ).eq("dealer_id", dealer_id).execute()
            if cached.data and _is_cache_fresh(cached.data[0].get("fetched_at")):
                return cached.data[0]
        except Exception as e:
            if "PGRST205" in str(e) or "dealer_places" in str(e):
                logger.warning("dealer_places table not found — run migration 006")
                return None
            raise

    # Fetch dealer info for search query
    dealer = db.table("dealers").select(
        "name, city, state, latitude, longitude"
    ).eq("id", dealer_id).execute()
    if not dealer.data:
        logger.warning(f"Dealer not found: {dealer_id}")
        return None

    d = dealer.data[0]
    place = await search_place(
        d["name"], d["city"], d["state"],
        lat=d.get("latitude"), lng=d.get("longitude"),
    )
    if not place:
        return None

    # Upsert to cache
    row = {
        "dealer_id": dealer_id,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        **place,
    }
    try:
        db.table("dealer_places").upsert(row, on_conflict="dealer_id").execute()
    except Exception as e:
        if "PGRST205" in str(e) or "dealer_places" in str(e):
            logger.warning("dealer_places table not found — run migration 006")
        else:
            logger.error(f"Failed to cache places for {dealer_id}: {e}")

    return row


def get_dealer_places_sync(dealer_id: str, force_refresh: bool = False) -> dict | None:
    """Synchronous wrapper for use in agent tools / non-async contexts."""
    try:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(get_dealer_places(dealer_id, force_refresh))
        loop.close()
        return result
    except Exception as e:
        logger.error(f"Places sync error for {dealer_id}: {e}")
        return None


async def enrich_dealers_bulk(
    dealer_ids: list[str] | None = None,
    batch_size: int = 10,
) -> dict:
    """Batch-enrich dealers with Places data.

    Skips dealers with fresh cache. Processes in batches with rate limiting.
    Returns {enriched, skipped, failed}.
    """
    db = get_service_client()

    if dealer_ids is None:
        all_dealers = db.table("dealers").select("id").execute()
        dealer_ids = [r["id"] for r in (all_dealers.data or [])]

    # Check which are already cached and fresh
    fresh_ids = set()
    try:
        cached = db.table("dealer_places").select(
            "dealer_id, fetched_at"
        ).in_("dealer_id", dealer_ids).execute()
        fresh_ids = {
            r["dealer_id"] for r in (cached.data or [])
            if _is_cache_fresh(r.get("fetched_at"))
        }
    except Exception as e:
        if "PGRST205" in str(e) or "dealer_places" in str(e):
            logger.warning("dealer_places table not found — run migration 006")
            return {"enriched": 0, "skipped": 0, "failed": 0, "total": len(dealer_ids),
                    "error": "dealer_places table missing — run migration 006"}
        raise

    to_enrich = [did for did in dealer_ids if did not in fresh_ids]

    enriched = 0
    failed = 0

    for i in range(0, len(to_enrich), batch_size):
        batch = to_enrich[i:i + batch_size]
        for dealer_id in batch:
            try:
                result = await get_dealer_places(dealer_id, force_refresh=True)
                if result:
                    enriched += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Bulk enrich failed for {dealer_id}: {e}")
                failed += 1

        # Rate limit between batches (Places API allows 600 QPM)
        if i + batch_size < len(to_enrich):
            await asyncio.sleep(0.5)

    return {
        "enriched": enriched,
        "skipped": len(fresh_ids),
        "failed": failed,
        "total": len(dealer_ids),
    }


def format_hours_today(hours_json: dict | None) -> str | None:
    """Parse Google's regularOpeningHours to a human-readable string for today.

    Returns e.g. "Open til 6:00 PM", "Closed today", or None.
    """
    if not hours_json:
        return None

    periods = hours_json.get("periods", [])
    if not periods:
        # Try weekdayDescriptions as fallback
        descriptions = hours_json.get("weekdayDescriptions", [])
        if descriptions:
            day_idx = datetime.now().weekday()  # 0=Monday
            if day_idx < len(descriptions):
                return descriptions[day_idx]
        return None

    # Map Python weekday (0=Mon..6=Sun) to Google's numeric days (0=Sun, 1=Mon..6=Sat)
    py_weekday = datetime.now().weekday()  # 0=Monday
    google_day = (py_weekday + 1) % 7  # Convert: Mon=1, Tue=2, ..., Sat=6, Sun=0

    for period in periods:
        open_day = period.get("open", {}).get("day")
        if open_day == google_day:
            close = period.get("close", {})
            hour = close.get("hour", 0)
            minute = close.get("minute", 0)

            # Format 24h to 12h
            ampm = "AM" if hour < 12 else "PM"
            display_hour = hour % 12 or 12
            if minute:
                return f"Open til {display_hour}:{minute:02d} {ampm}"
            return f"Open til {display_hour} {ampm}"

    return "Closed today"


def get_photo_url(photo_name: str, max_width: int = 400) -> str:
    """Build a Google Places photo URL from a photo resource name."""
    return (
        f"{PLACES_API_BASE}/{photo_name}/media"
        f"?maxWidthPx={max_width}&key={settings.google_maps_api_key}"
    )
