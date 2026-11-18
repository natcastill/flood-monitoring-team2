"""Rebuild the Silver layer from the (immutable) Bronze files with the current pipeline version.

Used in Workshop 3 after the preprocessing changed from v0.2 to v0.3: Silver files written by
different versions would not share one schema, and Spark reads the Silver folder as one dataset.

    python scripts/replay_bronze_to_silver.py --reset

--reset deletes the current Silver Parquet files, manifest and key registry first.
Without it, only Bronze files missing from the manifest are processed (same as the DAG).
"""
import argparse
import glob
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from flood_monitor.config import BRONZE_DIR, SILVER_DIR  # noqa: E402
from flood_monitor.silver import process_bronze_file, processed_bronze_files  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="delete the current Silver layer first")
    args = ap.parse_args()
    if args.reset:
        for f in glob.glob(os.path.join(SILVER_DIR, "*.parquet")):
            os.remove(f)
        shutil.rmtree(os.path.join(SILVER_DIR, "_registry"), ignore_errors=True)
        manifest = Path(SILVER_DIR) / "_manifest.jsonl"
        if manifest.exists():
            manifest.unlink()
    done = processed_bronze_files(SILVER_DIR)
    for b in sorted(glob.glob(os.path.join(BRONZE_DIR, "texas_flood_sources_*.json"))):
        if os.path.basename(b) in done:
            continue
        for out in process_bronze_file(b, SILVER_DIR):
            print("wrote", os.path.relpath(out, SILVER_DIR))


if __name__ == "__main__":
    main()
