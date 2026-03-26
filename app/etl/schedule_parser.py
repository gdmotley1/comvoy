"""Parse rep visit schedule CSVs and fuzzy-match dealers to Otto DB.

Kenneth's CSV format:
  - Zone header rows: "GEORGIA (92)" in DEALERSHIP column, rest blank
  - Dealer rows: "DEALER NAME - CITY" with optional dealer code, type, relationship, etc.
  - Section breaks: "CONQUEST ACCOUNTS", "BELOW NOT CURRENTLY INTERESTED", "BELOW NOT ON VISIT ROTATION"
  - Empty rows scattered throughout
"""

import csv
import io
import logging
import re
from difflib import SequenceMatcher
from datetime import datetime

from app.database import get_service_client

logger = logging.getLogger(__name__)

# Patterns
ZONE_RE = re.compile(
    r'^([\w/\s]+?)\s*\((\d+)\)\s*$'
)  # e.g. "GEORGIA (210)" or "GEORGIA/TENNESSEE (231)"

INACTIVE_MARKERS = {
    "CONQUEST ACCOUNTS",
    "BELOW NOT CURRENTLY INTERESTED",
    "BELOW NOT ON VISIT ROTATION",
}

# Minimum similarity for a fuzzy match to be accepted
MIN_MATCH_CONFIDENCE = 0.50


def parse_schedule_csv(file_content: str | bytes) -> list[dict]:
    """Parse a rep visit schedule CSV into structured dealer entries.

    Returns list of dicts with keys:
        raw_name, raw_city, raw_state, dealer_code, dealership_type,
        relationship, lead_contacts, wts_member, plant_tour,
        visit_date, zone_label, notes, is_active
    """
    if isinstance(file_content, bytes):
        file_content = file_content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(file_content))
    entries = []
    current_zone = None
    current_state = None
    is_active = True

    for row in reader:
        dealership = (row.get("DEALERSHIP") or "").strip()
        if not dealership:
            continue

        # Check for section break markers
        dealership_upper = dealership.upper()
        if any(marker in dealership_upper for marker in INACTIVE_MARKERS):
            is_active = False
            continue

        # Check for zone header: "STATE (###)" pattern, other columns empty
        zone_match = ZONE_RE.match(dealership)
        other_cols_empty = not any(
            (row.get(k) or "").strip()
            for k in ["DEALER CODE", "DEALERSHIP TYPE", "RELATIONSHIP", "LEAD CONTACT(S)"]
        )
        if zone_match and other_cols_empty:
            current_zone = dealership.strip()
            # Extract state(s) from zone label
            state_part = zone_match.group(1).strip().upper()
            # Handle "GEORGIA/SOUTH CAROLINA" → take first state
            if "/" in state_part:
                states = [s.strip() for s in state_part.split("/")]
                current_state = _state_abbrev(states[0])
            else:
                current_state = _state_abbrev(state_part)
            continue

        # Parse dealer row
        name, city = _parse_dealer_name(dealership)
        if not name:
            continue

        # Parse visit date(s) — may have multiple comma-separated dates
        raw_date = (row.get("SITE VISIT DATE") or "").strip()
        visit_date = _parse_date(raw_date)

        entries.append({
            "raw_name": name,
            "raw_city": city,
            "raw_state": current_state or "",
            "dealer_code": (row.get("DEALER CODE") or "").strip() or None,
            "dealership_type": (row.get("DEALERSHIP TYPE") or "").strip() or None,
            "relationship": (row.get("RELATIONSHIP") or "").strip().upper() or None,
            "lead_contacts": (row.get("LEAD CONTACT(S)") or "").strip() or None,
            "wts_member": (row.get("WTS MEMBER") or "").strip() or None,
            "plant_tour": (row.get("PLANT TOUR") or "").strip() or None,
            "visit_date": visit_date,
            "zone_label": current_zone,
            "notes": (row.get("NOTES") or "").strip() or None,
            "is_active": is_active,
        })

    logger.info(f"Parsed {len(entries)} dealer entries from schedule CSV "
                f"({sum(1 for e in entries if e['is_active'])} active)")
    return entries


def match_dealers_to_db(entries: list[dict]) -> list[dict]:
    """Fuzzy-match parsed schedule entries to Otto's dealers table.

    Adds 'dealer_id' and 'match_confidence' to each entry.
    """
    db = get_service_client()
    all_dealers = db.table("dealers").select(
        "id, name, city, state"
    ).execute().data

    # Build lookup index by state
    by_state: dict[str, list[dict]] = {}
    for d in all_dealers:
        st = (d.get("state") or "").upper()
        by_state.setdefault(st, []).append(d)

    for entry in entries:
        best_id = None
        best_conf = 0.0
        state = (entry["raw_state"] or "").upper()

        # Search in same state, plus neighboring states for border zones
        candidates = list(by_state.get(state, []))
        # For border zones (GEORGIA/ALABAMA etc.), search both states
        if entry.get("zone_label") and "/" in (entry["zone_label"] or ""):
            zone_states = re.findall(r'[A-Z]{2,}', entry["zone_label"].upper())
            for zs in zone_states:
                abbr = _state_abbrev(zs)
                if abbr and abbr != state:
                    candidates.extend(by_state.get(abbr, []))

        entry_name = _normalize(entry["raw_name"])
        entry_city = _normalize(entry.get("raw_city") or "")

        for d in candidates:
            db_name = _normalize(d["name"])
            db_city = _normalize(d.get("city") or "")

            # Name similarity (primary signal)
            name_sim = SequenceMatcher(None, entry_name, db_name).ratio()

            # City match bonus
            city_bonus = 0.0
            if entry_city and db_city:
                if entry_city == db_city:
                    city_bonus = 0.15
                elif entry_city in db_city or db_city in entry_city:
                    city_bonus = 0.10

            confidence = min(name_sim + city_bonus, 1.0)

            if confidence > best_conf:
                best_conf = confidence
                best_id = d["id"]

        if best_conf >= MIN_MATCH_CONFIDENCE:
            entry["dealer_id"] = best_id
            entry["match_confidence"] = round(best_conf, 3)
        else:
            entry["dealer_id"] = None
            entry["match_confidence"] = round(best_conf, 3) if best_conf > 0 else None

    matched = sum(1 for e in entries if e.get("dealer_id"))
    logger.info(f"Matched {matched}/{len(entries)} schedule dealers to Otto DB")
    return entries


def _parse_dealer_name(raw: str) -> tuple[str, str]:
    """Split 'DEALER NAME - CITY' into (name, city).

    Handles edge cases:
      - "J.W. TRUCK SALES, INC." (no city)
      - "PUGMIRE FORD BREMEN/CARROLLTON" (city in name)
      - "JIM ELLIS CHEVROLET " (no dash, no city)
    """
    # Try splitting on " - " (the standard separator)
    if " - " in raw:
        parts = raw.split(" - ", 1)
        return parts[0].strip(), parts[1].strip()

    # No dash — name only, no city
    return raw.strip(), ""


def _parse_date(raw: str) -> str | None:
    """Parse date strings like '1/6/26', '2/18/26', '1/8/26, 2/19/26'.

    Takes the most recent (last) date if multiple. Returns ISO date string or None.
    """
    if not raw:
        return None

    # Take last date if comma-separated
    parts = [p.strip() for p in raw.split(",")]
    date_str = parts[-1]

    # Try M/D/YY format
    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%m/%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            elif dt.year < 2000:
                # Bare month/day — assume 2026
                dt = dt.replace(year=2026)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def _normalize(s: str) -> str:
    """Normalize a string for fuzzy matching — lowercase, strip punctuation."""
    s = s.lower().strip()
    s = re.sub(r'[^\w\s]', '', s)  # remove punctuation
    s = re.sub(r'\s+', ' ', s)     # collapse whitespace
    return s


STATE_MAP = {
    "GEORGIA": "GA", "ALABAMA": "AL", "TENNESSEE": "TN",
    "SOUTH CAROLINA": "SC", "NORTH CAROLINA": "NC", "FLORIDA": "FL",
    "TEXAS": "TX", "LOUISIANA": "LA", "OKLAHOMA": "OK",
    "ARKANSAS": "AR", "MISSISSIPPI": "MS", "KENTUCKY": "KY",
    "VIRGINIA": "VA",
}


def _state_abbrev(name: str) -> str:
    """Convert state name to 2-letter abbreviation. Pass through if already abbreviated."""
    name = name.strip().upper()
    if len(name) == 2:
        return name
    return STATE_MAP.get(name, name[:2])
