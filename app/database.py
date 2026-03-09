from supabase import create_client, Client
from app.config import settings

# Service-role client for ETL and admin operations (bypasses RLS)
_service_client: Client | None = None

# Anon client for regular API queries
_anon_client: Client | None = None


def get_service_client() -> Client:
    global _service_client
    if _service_client is None:
        _service_client = create_client(settings.supabase_url, settings.supabase_service_key)
    return _service_client


def get_client() -> Client:
    global _anon_client
    if _anon_client is None:
        _anon_client = create_client(settings.supabase_url, settings.supabase_key)
    return _anon_client
