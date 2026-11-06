"""Command-line entry point for the ingestion pipeline (stub for Workshop 1).

    python ingest.py --mode historical
    python ingest.py --mode incremental
    python ingest.py --mode status
Historical ingestion is implemented in Workshop 2; incremental in Workshop 3.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from flood_monitor.config import load_config  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Flood Monitoring ingestion (Team 2)")
    ap.add_argument("--mode", choices=["historical", "incremental", "status"], required=True)
    ap.add_argument("--source", help="optional source_id from config/sources.yaml")
    ap.add_argument("--start", help="historical start date, e.g. 2010-01-01")
    ap.add_argument("--end", help="historical end date, e.g. 2025-12-31")
    args = ap.parse_args()
    cfg = load_config()
    if args.mode == "status":
        for s in cfg["sources"]:
            print(f"{s['source_id']:<18} role={s['role']:<11} watermark={s['watermark_field']}")
        print("Stub only: watermarks, last runs and row counts come from the database (Workshop 2-3).")
        return 0
    targets = [s for s in cfg["sources"]
               if (args.mode == "historical") == (s["role"] == "historical")
               and (args.source is None or s["source_id"] == args.source)]
    for s in targets:
        print(f"[{args.mode}] would ingest {s['source_id']} "
              f"(watermark: {s['watermark_field']}, duplicate key: {s['duplicate_key']})")
    print("Stub only: implemented in Workshop 2 (historical) and Workshop 3 (incremental).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
