import logging
import sys
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import database
import fetcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")

scheduler = BackgroundScheduler(timezone="Pacific/Auckland")


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()
    log.info("Database initialised")

    # Fetch immediately on start so the dashboard has data right away
    log.info("Running initial fetch …")
    fetcher.fetch_all()

    scheduler.add_job(fetcher.fetch_all, "interval", minutes=30, id="poll")
    scheduler.start()
    log.info("Scheduler started – polling every 30 minutes")

    yield

    scheduler.shutdown()


app = FastAPI(title="Auckland Wind Monitor", lifespan=lifespan)


# ── API routes ──────────────────────────────────────────────────────────────

@app.get("/api/latest")
def api_latest():
    return database.get_all_latest()


@app.get("/api/observations/{station_id}")
def api_observations(station_id: str, hours: int = 48):
    return database.get_observations(station_id, hours)


@app.post("/api/refresh")
def api_refresh():
    """Trigger an immediate fetch (useful for testing)."""
    results = fetcher.fetch_all()
    return results


# ── Dashboard ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
