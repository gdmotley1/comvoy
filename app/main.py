import logging
import os

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.config import settings
from app.database import get_service_client

from app.api.ingest import router as ingest_router
from app.api.dealers import router as dealers_router
from app.api.chat import router as chat_router
from app.api.trends import router as trends_router
from app.api.alerts import router as alerts_router
from app.api.scoring import router as scoring_router
from app.api.reports import router as reports_router
from app.api.travel import router as travel_router
from app.api.dashboard import router as dashboard_router
from app.api.velocity import router as velocity_router
from app.api.salesforce import router as salesforce_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = FastAPI(
    title="Comvoy Sales Intelligence",
    description="Agentic AI system for commercial truck sales reps",
    version="0.3.0",
)

# GZIP — compress responses > 1KB (40-60% reduction for JSON)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS — restricted to GitHub Pages frontend + local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gdmotley1.github.io",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["Content-Type", "x-api-key"],
)


# API key authentication middleware
_PUBLIC_PATHS = {"/", "/health", "/static"}

@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    # Let CORS preflight through — CORSMiddleware handles OPTIONS responses
    if request.method == "OPTIONS":
        return await call_next(request)
    path = request.url.path
    # Allow public paths and static files
    if path in _PUBLIC_PATHS or path.startswith("/static/"):
        return await call_next(request)
    # All /api/ routes require API key
    if path.startswith("/api/") and settings.api_key:
        key = request.headers.get("x-api-key", "")
        if key != settings.api_key:
            return Response(
                content='{"detail":"Unauthorized"}',
                status_code=401,
                media_type="application/json",
            )
    return await call_next(request)


# Security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# API routes
app.include_router(ingest_router)
app.include_router(dealers_router)
app.include_router(chat_router)
app.include_router(trends_router)
app.include_router(alerts_router)
app.include_router(scoring_router)
app.include_router(reports_router)
app.include_router(travel_router)
app.include_router(dashboard_router)
app.include_router(velocity_router)
app.include_router(salesforce_router)

# Static files (web chat UI) — skip in serverless environments
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

    @app.get("/")
    async def root():
        return FileResponse("static/index.html")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "comvoy-sales-intelligence"}


@app.get("/ping")
async def ping():
    """Lightweight DB keep-alive — hit weekly to prevent Supabase free tier auto-pause."""
    try:
        db = get_service_client()
        result = db.table("report_snapshots").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest = result.data[0]["report_date"] if result.data else None
        return {"status": "ok", "db": "connected", "latest_snapshot": latest}
    except Exception as e:
        return {"status": "error", "db": str(e)}
