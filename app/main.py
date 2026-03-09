import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.api.ingest import router as ingest_router
from app.api.dealers import router as dealers_router
from app.api.chat import router as chat_router
from app.api.trends import router as trends_router
from app.api.alerts import router as alerts_router
from app.api.scoring import router as scoring_router
from app.api.reports import router as reports_router
from app.api.travel import router as travel_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = FastAPI(
    title="Comvoy Sales Intelligence",
    description="Agentic AI system for commercial truck sales reps",
    version="0.2.0",
)

# CORS — allow Netlify frontend + local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(ingest_router)
app.include_router(dealers_router)
app.include_router(chat_router)
app.include_router(trends_router)
app.include_router(alerts_router)
app.include_router(scoring_router)
app.include_router(reports_router)
app.include_router(travel_router)

# Static files (web chat UI)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "comvoy-sales-intelligence"}
