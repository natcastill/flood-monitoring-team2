"""GHCN-Daily precipitation through the NCEI Access Data Service."""
from __future__ import annotations

from ..config import source
from ..http import get


def fetch(station: str, start: str, end: str):
    cfg = source("ncei_ghcnd")
    params = dict(cfg["params"])
    params.update({"stations": station, "startDate": start, "endDate": end,
                   "format": "json", "includeStationName": "true"})
    return get(cfg["url"], params=params)
