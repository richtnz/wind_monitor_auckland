import logging
import httpx
from database import insert_observation

log = logging.getLogger(__name__)

BASE_URL = "https://www.metservice.com/publicData/webdata/module/weatherStationCurrentConditions"

STATIONS = {
    "93106": "Auckland Harbour Bridge",
    "93133": "Mangere Bridge",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _first(lst: list):
    """Return the first non-None item in a list, or {}."""
    for item in lst or []:
        if item is not None:
            return item
    return {}


def _parse(raw: dict) -> dict:
    obs = raw.get("observations", {})

    wind = _first(obs.get("wind", []))
    pres = _first(obs.get("pressure", []))

    return {
        "wind_speed": wind.get("averageSpeed"),
        "wind_gust":  wind.get("gustSpeed"),
        "wind_dir":   wind.get("direction"),
        "pressure":   pres.get("atSeaLevel"),
    }


def fetch_all():
    results = {}
    with httpx.Client(headers=HEADERS, timeout=20) as client:
        for station_id, station_name in STATIONS.items():
            url = f"{BASE_URL}/{station_id}"
            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = _parse(resp.json())
                insert_observation(station_id, station_name, data)
                results[station_id] = {"ok": True, **data}
                log.info("Fetched %s: wind %s km/h %s",
                         station_name, data.get("wind_speed"), data.get("wind_dir"))
            except Exception as exc:
                log.error("Failed to fetch %s: %s", station_name, exc)
                results[station_id] = {"ok": False, "error": str(exc)}
    return results
