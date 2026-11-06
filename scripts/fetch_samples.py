"""Workshop 1 evidence collector for the four sources (Texas scope).

Usage (from the repository root):
    python scripts/fetch_samples.py               # everything
    python scripts/fetch_samples.py --year 2024   # choose the Storm Events / GHCN sample year

Outputs (small files, committed):  workshop_1/samples/...
Outputs (large files, git-ignored): data/raw/storm_events/*.csv.gz
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flood_monitor.config import RAW_DIR, SAMPLES_DIR, load_config, source  # noqa: E402
from flood_monitor.http import save_evidence  # noqa: E402
from flood_monitor.sources import ghcnd, nws, storm_events  # noqa: E402
from poll_incremental import poll  # noqa: E402


def storm_events_part(year: int | None, start_year: int) -> int:
    out = SAMPLES_DIR / "storm_events"
    files, listing = storm_events.list_detail_files()
    save_evidence(listing, out / "listing.html")
    print(f"[storm]  listing: {len(files)} details files")

    # Volume estimate: Content-Length of every yearly file in the window.
    rows = []
    for f in files:
        if f["year"] >= start_year:
            f["bytes_gz"] = storm_events.content_length(f["url"])
            rows.append(f)
    with (out / "volume_estimate.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["year", "compiled", "file", "bytes_gz", "url"])
        w.writeheader()
        w.writerows(rows)
    total = sum(r["bytes_gz"] or 0 for r in rows)
    print(f"[storm]  {len(rows)} files since {start_year}: {total/1e6:.1f} MB compressed")

    # Sample year: latest complete year available, unless given.
    available = {f["year"]: f for f in files}
    if year is None:
        year = max(y for y in available if y < date.today().year)
    gz = storm_events.download(available[year]["url"], RAW_DIR / "storm_events")
    import pandas as pd
    all_rows = pd.read_csv(gz, compression="gzip", dtype=str, low_memory=False)
    tx = storm_events.load_texas_floods(gz)
    tx.to_csv(out / f"texas_floods_{year}.csv", index=False)
    (out / f"texas_floods_{year}_summary.txt").write_text(
        f"source file: {gz.name}\nall rows in file: {len(all_rows)}\n"
        f"Texas Flood/Flash Flood/Heavy Rain rows: {len(tx)}\n"
        f"by type:\n{tx['EVENT_TYPE'].value_counts().to_string()}\n",
        encoding="utf-8")
    print(f"[storm]  {year}: {len(all_rows)} rows in file, {len(tx)} Texas flood-related rows")
    return year


def ghcnd_part(year: int) -> None:
    out = SAMPLES_DIR / "ghcnd"
    for station in source("ncei_ghcnd")["stations"]:
        resp = ghcnd.fetch(station, f"{year}-01-01", f"{year}-12-31")
        save_evidence(resp, out / f"{station}_{year}.json")
        n = len(resp.json()) if resp.ok else 0
        print(f"[ghcnd]  {station} {year}: HTTP {resp.status_code}, {n} daily records")


def nws_station_metadata() -> None:
    out = SAMPLES_DIR / "nws_stations"
    for station in source("nws_observations")["stations"]:
        resp = nws.station_metadata(station)
        save_evidence(resp, out / f"{station}.json")
        print(f"[nws]    station {station}: HTTP {resp.status_code}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=None)
    ap.add_argument("--skip-storm", action="store_true")
    args = ap.parse_args()
    start_year = int(load_config()["project"]["historical_window"]["start"][:4])

    year = args.year
    if not args.skip_storm:
        year = storm_events_part(args.year, start_year)
    ghcnd_part(year or date.today().year - 1)
    nws_station_metadata()
    poll()  # first poll of the incremental sources
    print("\nDone. Run scripts/poll_incremental.py again in >= 1 hour, "
          "then scripts/compare_polls.py.")


if __name__ == "__main__":
    main()
