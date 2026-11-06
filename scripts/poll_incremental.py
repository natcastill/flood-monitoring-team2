"""Poll the two incremental sources once and keep the raw responses.

Run it twice, at least one hour apart:
    python scripts/poll_incremental.py
    (wait >= 1 hour)
    python scripts/poll_incremental.py
    python scripts/compare_polls.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flood_monitor.config import SAMPLES_DIR, source  # noqa: E402
from flood_monitor.http import save_evidence, utc_stamp  # noqa: E402
from flood_monitor.sources import nws  # noqa: E402


def poll(stamp: str | None = None) -> Path:
    stamp = stamp or utc_stamp()
    out = SAMPLES_DIR / "polls" / f"poll_{stamp}"

    for station in source("nws_observations")["stations"]:
        resp = nws.observations(station)
        save_evidence(resp, out / "nws_observations" / f"{station}.json")
        n = len(resp.json().get("features", [])) if resp.ok else 0
        print(f"[obs]    {station}: HTTP {resp.status_code}, {n} observations")

    resp = nws.alerts_active("TX")
    save_evidence(resp, out / "nws_alerts" / "active_TX.json")
    n = len(resp.json().get("features", [])) if resp.ok else 0
    print(f"[alerts] active TX: HTTP {resp.status_code}, {n} alerts (an empty list is a valid answer)")

    for i, page in enumerate(nws.alerts_recent("TX"), start=1):
        save_evidence(page, out / "nws_alerts" / f"recent_TX_page{i}.json")
        n = len(page.json().get("features", [])) if page.ok else 0
        print(f"[alerts] recent TX page {i}: HTTP {page.status_code}, {n} alerts")

    (out / "POLL_INFO.json").write_text(json.dumps({"poll_stamp_utc": stamp}, indent=2))
    print(f"Saved to {out}")
    return out


if __name__ == "__main__":
    poll()
