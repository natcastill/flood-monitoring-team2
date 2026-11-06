"""NWS API: station observations and alerts. A User-Agent is mandatory."""
from __future__ import annotations

from ..config import nws_user_agent
from ..http import get

BASE = "https://api.weather.gov"


def _headers() -> dict:
    return {"User-Agent": nws_user_agent(), "Accept": "application/geo+json"}


def observations(station: str, start: str | None = None, limit: int = 500):
    params = {"limit": limit}
    if start:
        params["start"] = start
    return get(f"{BASE}/stations/{station}/observations", params=params,
               headers=_headers())


def station_metadata(station: str):
    return get(f"{BASE}/stations/{station}", headers=_headers())


def alerts_active(area: str = "TX"):
    return get(f"{BASE}/alerts/active", params={"area": area}, headers=_headers())


def alerts_recent(area: str = "TX", limit: int = 500, max_pages: int = 3):
    """Recent alerts (all statuses). Follows pagination.next; returns responses."""
    url, params, pages = f"{BASE}/alerts", {"area": area, "limit": limit}, []
    for _ in range(max_pages):
        resp = get(url, params=params, headers=_headers())
        pages.append(resp)
        if resp.status_code != 200:
            break
        nxt = resp.json().get("pagination", {}).get("next")
        if not nxt:
            break
        url, params = nxt, None
    return pages
