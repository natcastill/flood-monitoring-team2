"""NOAA Storm Events Database: yearly gzipped CSV 'details' files."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from ..config import source
from ..http import get

FILE_RE = re.compile(r"StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz")
FLOOD_TYPES = ["Flood", "Flash Flood", "Heavy Rain"]


def list_detail_files():
    """Parse the HTTPS directory listing. Returns (files, response)."""
    base = source("noaa_storm_events")["url"]
    resp = get(base)
    resp.raise_for_status()
    seen = {}
    for m in FILE_RE.finditer(resp.text):
        name = m.group(0)
        seen[name] = {"year": int(m.group(1)), "compiled": m.group(2),
                      "file": name, "url": base + name}
    return sorted(seen.values(), key=lambda r: r["year"]), resp


def content_length(url: str) -> int | None:
    import requests
    r = requests.head(url, timeout=60, allow_redirects=True)
    value = r.headers.get("Content-Length")
    return int(value) if value else None


def download(url: str, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / url.rsplit("/", 1)[-1]
    if dest.exists():  # cache: never re-download the archive needlessly
        return dest
    resp = get(url, stream=True)
    resp.raise_for_status()
    with dest.open("wb") as fh:
        for chunk in resp.iter_content(1 << 20):
            fh.write(chunk)
    return dest


def load_texas_floods(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, compression="gzip", dtype=str, low_memory=False)
    mask = (df["STATE"] == "TEXAS") & (df["EVENT_TYPE"].isin(FLOOD_TYPES))
    return df.loc[mask].reset_index(drop=True)
