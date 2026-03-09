from pydantic import BaseModel
from datetime import date, datetime


# --- Request models ---

class IngestRequest(BaseModel):
    report_date: date | None = None  # auto-detected from Excel if not provided


class ChatMessage(BaseModel):
    message: str


class NearbyQuery(BaseModel):
    latitude: float
    longitude: float
    radius_miles: float = 30


# --- Response models ---

class DealerSummary(BaseModel):
    id: str
    name: str
    city: str
    state: str
    latitude: float | None = None
    longitude: float | None = None
    total_vehicles: int = 0
    brand_count: int | None = None
    body_type_count: int | None = None
    top_brand: str | None = None
    smyrna_units: int = 0
    smyrna_percentage: float = 0
    top_body_types: str | None = None
    rank: int | None = None
    distance_miles: float | None = None  # only for proximity queries


class DealerBriefing(BaseModel):
    dealer: DealerSummary
    brand_breakdown: list[dict]
    body_type_breakdown: list[dict]
    smyrna_details: dict | None = None


class IngestResult(BaseModel):
    snapshot_id: str
    report_date: str
    dealers_loaded: int
    brands_loaded: int
    body_types_loaded: int
    brand_inventory_rows: int
    body_type_inventory_rows: int
    smyrna_details_loaded: int
    geocoded_count: int


class SnapshotInfo(BaseModel):
    id: str
    report_date: str
    file_name: str
    uploaded_at: str
    total_dealers: int | None
    total_vehicles: int | None


# --- Phase 2: Trend & alert models ---

class SnapshotPoint(BaseModel):
    """One month of dealer data with deltas from previous month."""
    report_date: str
    snapshot_id: str
    total_vehicles: int = 0
    smyrna_units: int = 0
    smyrna_percentage: float = 0
    rank: int | None = None
    top_brand: str | None = None
    vehicle_delta: int | None = None
    smyrna_delta: int | None = None
    rank_delta: int | None = None


class DealerTrend(BaseModel):
    """Multi-month trend for a single dealer."""
    dealer_id: str
    dealer_name: str
    city: str
    state: str
    points: list[SnapshotPoint]
    vehicle_trend: str  # "up", "down", "flat"
    smyrna_trend: str   # "up", "down", "flat", "none"
    rank_trend: str     # "improving", "declining", "stable"


class TerritoryPoint(BaseModel):
    """One month of state-level data."""
    report_date: str
    snapshot_id: str
    total_dealers: int
    total_vehicles: int
    total_smyrna: int
    dealers_with_smyrna: int
    smyrna_penetration_pct: float


class TerritoryTrend(BaseModel):
    """Multi-month trend for a state territory."""
    state: str
    points: list[TerritoryPoint]
    dealer_count_delta: int | None = None
    vehicle_delta: int | None = None
    smyrna_delta: int | None = None


class Alert(BaseModel):
    """A single notable change between snapshots."""
    alert_type: str
    priority: str  # "high", "medium", "low"
    dealer_name: str
    dealer_id: str
    city: str
    state: str
    message: str
    value_before: int | float | None = None
    value_after: int | float | None = None


class AlertsResponse(BaseModel):
    """Full alerts payload."""
    snapshot_a_date: str
    snapshot_b_date: str
    alerts: list[Alert]
    summary: str
    total_alerts: int


# --- Phase 3: Travel plan CRUD models ---

class TravelPlanCreate(BaseModel):
    """Create a new travel plan day."""
    rep_id: str
    travel_date: date
    start_location: str  # freeform: "Smyrna, GA" or "Hampton Inn, Macon GA"
    end_location: str
    notes: str | None = None


class TravelPlanUpdate(BaseModel):
    """Update an existing travel plan. All fields optional."""
    travel_date: date | None = None
    start_location: str | None = None
    end_location: str | None = None
    notes: str | None = None
