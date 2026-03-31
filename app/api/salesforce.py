"""Salesforce CRM integration — read-only access to Leads, Contacts, Opportunities.

Lazy singleton client with 5-min TTL cache. Degrades gracefully if creds
are missing (HTTP 503). All queries are SOQL SELECT — no writes.
"""

import logging
import time
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from simple_salesforce import Salesforce, SalesforceExpiredSession, SalesforceAuthenticationFailed

from app.config import settings

router = APIRouter(prefix="/api/salesforce", tags=["salesforce"])
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy singleton + TTL cache
# ---------------------------------------------------------------------------

_sf_client: Optional[Salesforce] = None
_cache: dict[str, tuple[float, list]] = {}
_CACHE_TTL = 300  # 5 minutes


def _get_sf() -> Salesforce:
    """Get or create Salesforce client. Reconnects on expired session."""
    global _sf_client
    if _sf_client is not None:
        return _sf_client
    if not settings.sf_username:
        raise HTTPException(503, "Salesforce credentials not configured")
    try:
        _sf_client = Salesforce(
            username=settings.sf_username,
            password=settings.sf_password,
            security_token=settings.sf_security_token,
            domain=settings.sf_domain,
        )
        logger.info("Salesforce client connected")
        return _sf_client
    except SalesforceAuthenticationFailed as e:
        logger.error(f"Salesforce auth failed: {e}")
        raise HTTPException(503, "Salesforce authentication failed")


def _reset_sf():
    """Clear cached client (forces reconnect on next call)."""
    global _sf_client
    _sf_client = None


def _sanitize(s: str) -> str:
    """Strip characters that could break SOQL strings."""
    return s.replace("'", "").replace("\\", "").strip()


def _cached_query(cache_key: str, soql: str) -> list[dict]:
    """Execute SOQL with cache + expired-session retry."""
    now = time.time()
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if now - ts < _CACHE_TTL:
            return data

    sf = _get_sf()
    try:
        result = sf.query_all(soql)
    except SalesforceExpiredSession:
        _reset_sf()
        sf = _get_sf()
        result = sf.query_all(soql)

    records = result.get("records", [])
    clean = [_clean_record(r) for r in records]
    _cache[cache_key] = (now, clean)
    return clean


def _clean_record(record: dict) -> dict:
    """Strip Salesforce metadata ('attributes' key) recursively."""
    out = {}
    for k, v in record.items():
        if k == "attributes":
            continue
        if v is None:
            out[k] = None
        elif isinstance(v, dict):
            if "records" in v:
                # Nested relationship (e.g., Account.Contacts)
                out[k] = [_clean_record(r) for r in (v["records"] or [])]
            elif "totalSize" in v and "records" not in v:
                # Empty nested relationship
                out[k] = []
            else:
                out[k] = _clean_record(v)
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# Core query functions (called by REST endpoints + agent tools)
# ---------------------------------------------------------------------------

def search_sf_leads(query: str = None, state: str = None, limit: int = 20) -> list[dict]:
    """Search Salesforce Leads by name/company and optional state."""
    conditions = ["IsDeleted = false"]

    if query:
        q = _sanitize(query)
        conditions.append(f"(Name LIKE '%{q}%' OR Company LIKE '%{q}%')")
    if state:
        s = _sanitize(state)
        conditions.append(f"State = '{s}'")

    where = " AND ".join(conditions)
    soql = (
        f"SELECT Id, Name, Company, Email, Phone, State, Status, LeadSource, CreatedDate "
        f"FROM Lead WHERE {where} ORDER BY CreatedDate DESC LIMIT {min(limit, 50)}"
    )
    cache_key = f"leads:{query}:{state}:{limit}"
    return _cached_query(cache_key, soql)


def search_sf_contacts(query: str = None, account_name: str = None, limit: int = 20) -> list[dict]:
    """Search Salesforce Contacts by name and optional account."""
    conditions = []

    if query:
        q = _sanitize(query)
        conditions.append(f"(Name LIKE '%{q}%' OR Email LIKE '%{q}%')")
    if account_name:
        a = _sanitize(account_name)
        conditions.append(f"Account.Name LIKE '%{a}%'")

    where = " AND ".join(conditions) if conditions else "Id != null"
    soql = (
        f"SELECT Id, Name, Email, Phone, Title, Account.Name, Account.Id "
        f"FROM Contact WHERE {where} ORDER BY CreatedDate DESC LIMIT {min(limit, 50)}"
    )
    cache_key = f"contacts:{query}:{account_name}:{limit}"
    return _cached_query(cache_key, soql)


def search_sf_opportunities(query: str = None, stage: str = None, limit: int = 20) -> list[dict]:
    """Search Salesforce Opportunities by name/account and optional stage."""
    conditions = ["IsClosed = false"]

    if query:
        q = _sanitize(query)
        conditions.append(f"(Name LIKE '%{q}%' OR Account.Name LIKE '%{q}%')")
    if stage:
        s = _sanitize(stage)
        conditions.append(f"StageName = '{s}'")

    where = " AND ".join(conditions)
    soql = (
        f"SELECT Id, Name, StageName, Amount, CloseDate, Account.Name, Owner.Name "
        f"FROM Opportunity WHERE {where} ORDER BY CloseDate DESC LIMIT {min(limit, 50)}"
    )
    cache_key = f"opps:{query}:{stage}:{limit}"
    return _cached_query(cache_key, soql)


def get_sf_account(name: str) -> list[dict]:
    """Look up Salesforce Account by name with nested Contacts + Opportunities."""
    n = _sanitize(name)
    if not n:
        return []
    soql = (
        f"SELECT Id, Name, BillingState, BillingCity, Phone, Website, Owner.Name, "
        f"(SELECT Id, Name, StageName, Amount, CloseDate FROM Opportunities "
        f"WHERE IsClosed = false ORDER BY CloseDate DESC LIMIT 5), "
        f"(SELECT Id, Name, Email, Phone, Title FROM Contacts LIMIT 10) "
        f"FROM Account WHERE Name LIKE '%{n}%' LIMIT 5"
    )
    cache_key = f"account:{n}"
    return _cached_query(cache_key, soql)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@router.get("/status")
def api_sf_status():
    """Check Salesforce connection status."""
    if not settings.sf_username:
        return {"status": "not_configured", "sf_username": ""}
    try:
        sf = _get_sf()
        return {"status": "connected", "sf_instance": sf.sf_instance}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/test-soql")
def api_test_soql():
    """Run a static SOQL query to verify Salesforce connectivity end-to-end."""
    try:
        sf = _get_sf()
        soql = "SELECT Id, Email, FirstName, LastName, CreatedDate FROM Lead ORDER BY CreatedDate DESC LIMIT 1"
        result = sf.query(soql)
        records = result.get("records", [])
        clean = [_clean_record(r) for r in records]
        return {
            "status": "ok",
            "soql": soql,
            "totalSize": result.get("totalSize", 0),
            "records": clean,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Salesforce test-soql failed")
        return {"status": "error", "error_type": type(e).__name__, "error": str(e)}


@router.get("/leads")
def api_search_leads(
    q: str = Query(None, description="Search by name or company"),
    state: str = Query(None, description="Two-letter state code"),
    limit: int = Query(20, le=50),
):
    """Search Salesforce leads."""
    try:
        return search_sf_leads(query=q, state=state, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Salesforce leads query failed")
        raise HTTPException(500, f"Salesforce query failed: {e}")


@router.get("/contacts")
def api_search_contacts(
    q: str = Query(None, description="Search by name or email"),
    account: str = Query(None, description="Filter by account name"),
    limit: int = Query(20, le=50),
):
    """Search Salesforce contacts."""
    return search_sf_contacts(query=q, account_name=account, limit=limit)


@router.get("/opportunities")
def api_search_opportunities(
    q: str = Query(None, description="Search by opportunity or account name"),
    stage: str = Query(None, description="Filter by stage (e.g. 'Prospecting')"),
    limit: int = Query(20, le=50),
):
    """Search Salesforce opportunities (open only)."""
    return search_sf_opportunities(query=q, stage=stage, limit=limit)


@router.get("/account")
def api_get_account(
    name: str = Query(..., description="Account name to look up"),
):
    """Look up Salesforce account with contacts and open opportunities."""
    results = get_sf_account(name)
    if not results:
        raise HTTPException(404, f"No Salesforce account matching '{name}'")
    return results
