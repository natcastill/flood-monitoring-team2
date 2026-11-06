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
