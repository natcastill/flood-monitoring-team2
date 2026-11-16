"""Command-line entry point for the Flood Monitoring ingestion pipeline (Team 2).

Usage:
    python ingest.py --mode historical [--source SOURCE_ID] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
    python ingest.py --mode status
    python ingest.py --mode incremental   # Implemented in Workshop 3
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from flood_monitor.config import RAW_DIR, load_config, source  # noqa: E402
from flood_monitor.normalization import (  # noqa: E402
    normalize_ghcnd_record,
    normalize_storm_event,
)
from flood_monitor.sources import ghcnd, storm_events  # noqa: E402
from flood_monitor.storage import (  # noqa: E402
    ensure_schema,
    finish_run,
    get_conn,
    get_watermark,
    start_run,
    sync_sources,
    upsert_ghcnd_partition,
    upsert_storm_partition,
)

PAIRED_STATIONS = {
    "USW00012960": "KIAH",
    "USW00013904": "KAUS",
    "USW00012921": "KSAT",
    "USW00003927": "KDFW",
    "USW00012924": "KCRP",
}


def _year_bounds(cfg: dict, start_arg: str | None, end_arg: str | None) -> tuple[int, int]:
    win = cfg["project"]["historical_window"]
    s_str = start_arg or win["start"]
    e_str = end_arg or win["end"]
    start_y = int(str(s_str)[:4])
    end_y = date.today().year if str(e_str).lower() == "present" else int(str(e_str)[:4])
    return start_y, end_y


def run_status(cfg: dict) -> int:
    with get_conn() as conn:
        ensure_schema(conn)
        sync_sources(conn, cfg)
        print(f"{"SOURCE":<20} {"ROLE":<12} {"EVENTS":>8} {"PARTITIONS":>10} {"LAST_STATUS":<12}")
        print("-" * 68)
        for s in cfg["sources"]:
            sid = s["source_id"]
            ev_count = conn.execute(
                "SELECT COUNT(*) AS n FROM event WHERE source_id = %s", (sid,)
            ).fetchone()["n"]
            wm_count = conn.execute(
                "SELECT COUNT(*) AS n FROM ingestion_state WHERE source_id = %s", (sid,)
            ).fetchone()["n"]
            last_run = conn.execute(
                "SELECT status, rows_new, rows_updated, rows_rejected "
                "FROM ingestion_run WHERE source_id = %s ORDER BY run_id DESC LIMIT 1",
                (sid,),
            ).fetchone()
            st_label = last_run["status"] if last_run else "never_run"
            print(f"{sid:<20} {s['role']:<12} {ev_count:>8} {wm_count:>10} {st_label:<12}")
    return 0


def ingest_storm_events(conn, start_y: int, end_y: int) -> None:
    run_id = start_run(conn, "noaa_storm_events", "historical")
    totals = {"rows_in": 0, "rows_new": 0, "rows_updated": 0, "rows_rejected": 0}
    try:
        files, _ = storm_events.list_detail_files()
        target_files = [f for f in files if start_y <= f["year"] <= end_y]
        print(f"[storm] found {len(target_files)} yearly files in [{start_y}..{end_y}]")

        for f in target_files:
            yr = str(f["year"])
            comp = f["compiled"]
            wm = get_watermark(conn, "noaa_storm_events", yr)
            if wm == comp:
                print(f"[storm] {yr}: already at compiled watermark {comp}, skipping")
                continue

            gz_path = storm_events.download(f["url"], RAW_DIR / "storm_events")
            df = storm_events.load_texas_floods(gz_path)
            records = df.to_dict(orient="records")

            items = []
            for row in records:
                norm, err = normalize_storm_event(row, gz_path.name, comp)
                items.append((norm, err, row))

            p_stats = upsert_storm_partition(
                conn,
                run_id=run_id,
                partition_year=yr,
                watermark_compiled=comp,
                normalized_items=items,
                mark_missing_withdrawn=True,
            )
            for k in totals:
                totals[k] += p_stats[k]
            print(
                f"[storm] {yr} (c{comp}): in={p_stats['rows_in']} "
                f"new={p_stats['rows_new']} upd={p_stats['rows_updated']} "
                f"rej={p_stats['rows_rejected']}"
            )

        finish_run(conn, run_id, "success", totals)
    except Exception as exc:
        finish_run(conn, run_id, "failed", totals, error=str(exc))
        raise


def ingest_ghcnd(conn, start_y: int, end_y: int) -> None:
    cfg_src = source("ncei_ghcnd")
    stations = cfg_src["stations"]
    raw_dir = RAW_DIR / "ghcnd"
    raw_dir.mkdir(parents=True, exist_ok=True)

    run_id = start_run(conn, "ncei_ghcnd", "historical")
    totals = {"rows_in": 0, "rows_new": 0, "rows_updated": 0, "rows_rejected": 0}
    try:
        for st in stations:
            paired = PAIRED_STATIONS.get(st)
            for yr in range(start_y, end_y + 1):
                raw_file = raw_dir / f"{st}_{yr}.json"
                if raw_file.exists():
                    rows = json.loads(raw_file.read_text(encoding="utf-8"))
                else:
                    resp = ghcnd.fetch(st, f"{yr}-01-01", f"{yr}-12-31")
                    resp.raise_for_status()
                    rows = resp.json()
                    raw_file.write_text(json.dumps(rows), encoding="utf-8")
                    time.sleep(1.0)

                if not rows:
                    continue

                items = []
                max_date = ""
                for r in rows:
                    norm, err = normalize_ghcnd_record(r, request_url=cfg_src["url"])
                    items.append((norm, err, r))
                    d = str(r.get("DATE", ""))
                    if d > max_date:
                        max_date = d

                p_stats = upsert_ghcnd_partition(
                    conn,
                    run_id=run_id,
                    station_id=st,
                    watermark_date=max_date,
                    normalized_items=items,
                    paired_station_id=paired,
                )
                for k in totals:
                    totals[k] += p_stats[k]
                print(
                    f"[ghcnd] {st} {yr}: in={p_stats['rows_in']} "
                    f"new={p_stats['rows_new']} upd={p_stats['rows_updated']} "
                    f"rej={p_stats['rows_rejected']}"
                )

        finish_run(conn, run_id, "success", totals)
    except Exception as exc:
        finish_run(conn, run_id, "failed", totals, error=str(exc))
        raise


def main() -> int:
    ap = argparse.ArgumentParser(description="Flood Monitoring ingestion (Team 2)")
    ap.add_argument("--mode", choices=["historical", "incremental", "status"], required=True)
    ap.add_argument("--source", help="optional source_id from config/sources.yaml")
    ap.add_argument("--start", help="historical start date, e.g. 2010-01-01")
    ap.add_argument("--end", help="historical end date, e.g. 2025-12-31")
    args = ap.parse_args()
    cfg = load_config()

    if args.mode == "status":
        return run_status(cfg)

    if args.mode == "incremental":
        print("Incremental mode is scheduled for Workshop 3 (use scripts/poll_incremental.py for raw polls).")
        return 0

    start_y, end_y = _year_bounds(cfg, args.start, args.end)
    targets = [
        s["source_id"]
        for s in cfg["sources"]
        if s["role"] == "historical" and (args.source is None or s["source_id"] == args.source)
    ]
    if not targets:
        print(f"No historical source matched --source={args.source!r}")
        return 1

    with get_conn() as conn:
        ensure_schema(conn)
        sync_sources(conn, cfg)
        if "noaa_storm_events" in targets:
            ingest_storm_events(conn, start_y, end_y)
        if "ncei_ghcnd" in targets:
            ingest_ghcnd(conn, start_y, end_y)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
