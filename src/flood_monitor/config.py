"""Load config/sources.yaml and environment variables."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config" / "sources.yaml"
SAMPLES_DIR = ROOT / "workshop_1" / "samples"
RAW_DIR = ROOT / "data" / "raw"


def _load_dotenv(path: Path = ROOT / ".env") -> None:
    """Minimal .env reader so no extra dependency is needed."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"'))


_load_dotenv()


def load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def source(source_id: str) -> dict:
    for entry in load_config()["sources"]:
        if entry["source_id"] == source_id:
            return entry
    raise KeyError(source_id)


def nws_user_agent() -> str:
    ua = os.environ.get("NWS_USER_AGENT", "").strip()
    if not ua or "example.com" in ua:
        raise SystemExit(
            "Set NWS_USER_AGENT in .env, e.g. "
            "NWS_USER_AGENT=(ud-flood-monitor-team2, your.email@udistrital.edu.co)"
        )
    return ua


# ==============================================================================
# Workshop 2: Medallion Data Lake & Airflow Pipeline Configuration
# ==============================================================================

# Inside Docker the project root is /opt/airflow; outside Docker it is the repository root.
LAKE_ROOT = Path(os.getenv("FLOOD_PROJECT_ROOT", "/opt/airflow" if Path("/opt/airflow").exists() else ROOT))
BRONZE_DIR = str(LAKE_ROOT / "datalake_bronze")
SILVER_DIR = str(LAKE_ROOT / "datalake_silver")
GOLD_DIR = str(LAKE_ROOT / "datalake_gold")
STATE_DIR = str(LAKE_ROOT / "datalake_bronze" / "_state")   # watermarks (Storm Events compile suffixes)
RAW_CACHE_DIR = str(LAKE_ROOT / "data" / "raw")              # full Storm Events yearly files (git-ignored)

# Comes from .env (see .env.example); the fallback carries no personal data.
NWS_USER_AGENT = os.getenv("NWS_USER_AGENT", "(ud-flood-monitor-team2, Universidad Distrital course project)")

# 5 Texas paired stations (NWS ID -> GHCN ID, County, Lat, Lon)
TEXAS_STATIONS = {
    "KIAH": {"ghcn_id": "USW00012960", "name": "Houston IAH", "county": "Harris", "lat": 29.9844, "lon": -95.3607},
    "KAUS": {"ghcn_id": "USW00013904", "name": "Austin-Bergstrom", "county": "Travis", "lat": 30.1831, "lon": -97.6799},
    "KSAT": {"ghcn_id": "USW00012921", "name": "San Antonio", "county": "Bexar", "lat": 29.5337, "lon": -98.4698},
    "KDFW": {"ghcn_id": "USW00003927", "name": "Dallas-Fort Worth", "county": "Tarrant", "lat": 32.8974, "lon": -97.0220},
    "KCRP": {"ghcn_id": "USW00012924", "name": "Corpus Christi", "county": "Nueces", "lat": 27.7742, "lon": -97.5122},
}

# Flood-related NWS event filter (Problem #11 in Workshop 1)
FLOOD_ALERT_EVENTS = {
    "Flood Warning",
    "Flood Watch",
    "Flood Advisory",
    "Flood Statement",
    "Flash Flood Warning",
    "Flash Flood Watch",
    "Flash Flood Statement",
    "Hydrologic Outlook",
}

# Texas WGS84 Bounding Box
TX_BBOX = {
    "min_lat": 25.8,
    "max_lat": 36.5,
    "min_lon": -106.65,
    "max_lon": -93.5,
}
