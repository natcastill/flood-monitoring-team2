"""HTTP helper that keeps the unmodified response and its headers as evidence."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

DEFAULT_TIMEOUT = 60


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def get(url: str, params: dict | None = None, headers: dict | None = None,
        retries: int = 3, stream: bool = False) -> requests.Response:
    """GET with simple exponential backoff on 429/5xx."""
    last = None
    for attempt in range(retries):
        resp = requests.get(url, params=params, headers=headers or {},
                            timeout=DEFAULT_TIMEOUT, stream=stream)
        if resp.status_code < 500 and resp.status_code != 429:
            return resp
        last = resp
        time.sleep(2 ** attempt * 5)
    return last


def save_evidence(resp: requests.Response, out_body: Path) -> Path:
    """Write the raw body exactly as received plus a .meta.json with the
    request URL, status, retrieval time and response headers."""
    out_body.parent.mkdir(parents=True, exist_ok=True)
    out_body.write_bytes(resp.content)
    meta = {
        "url": resp.url,
        "status_code": resp.status_code,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "headers": dict(resp.headers),
        "bytes": len(resp.content),
    }
    meta_path = out_body.with_name(out_body.name + ".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta_path
