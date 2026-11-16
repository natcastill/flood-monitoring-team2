import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flood_monitor.normalization import normalize_ghcnd_record  # noqa: E402
from flood_monitor.storage import (  # noqa: E402
    ensure_schema,
    finish_run,
    get_conn,
    get_watermark,
    start_run,
    sync_sources,
    upsert_ghcnd_partition,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "USW00012960_2025.json"


def test_ghcnd_storage_idempotency():
    raw_rows = json.loads(FIXTURE.read_text(encoding="utf-8"))[:25]
    # Inject one bad row to verify rejected_record handling
    bad_row = {"STATION": "USW00012960", "DATE": "2025-01-99", "PRCP": "5.0"}

    items = []
    for r in raw_rows + [bad_row]:
        norm, err = normalize_ghcnd_record(r)
        items.append((norm, err, r))

    max_date = max(r["DATE"] for r in raw_rows)

    with get_conn() as conn:
        ensure_schema(conn)
        sync_sources(conn)

        # Clean any previous test artifacts before running
        with conn.transaction():
            conn.execute("DELETE FROM daily_precipitation WHERE station_id = 'USW00012960'")
            conn.execute("DELETE FROM event WHERE source_id = 'ncei_ghcnd' AND location_code = 'USW00012960'")
            conn.execute("DELETE FROM ingestion_state WHERE source_id = 'ncei_ghcnd' AND partition_key = 'USW00012960'")

        # 1st run: should insert all valid rows and reject 1
        run1 = start_run(conn, "ncei_ghcnd", "historical")
        s1 = upsert_ghcnd_partition(conn, run1, "USW00012960", max_date, items, paired_station_id="KIAH")
        finish_run(conn, run1, "success", s1)

        assert s1["rows_new"] == len(raw_rows)
        assert s1["rows_updated"] == 0
        assert s1["rows_rejected"] == 1
        assert get_watermark(conn, "ncei_ghcnd", "USW00012960") == max_date

        # 2nd run with exact same data: must insert 0 and update 0 (idempotent)
        run2 = start_run(conn, "ncei_ghcnd", "historical")
        s2 = upsert_ghcnd_partition(conn, run2, "USW00012960", max_date, items, paired_station_id="KIAH")
        finish_run(conn, run2, "success", s2)

        assert s2["rows_new"] == 0
        assert s2["rows_updated"] == 0

        # Cleanup test rows so production ingestion starts clean
        with conn.transaction():
            conn.execute("DELETE FROM daily_precipitation WHERE station_id = 'USW00012960'")
            conn.execute("DELETE FROM event WHERE source_id = 'ncei_ghcnd' AND location_code = 'USW00012960'")
            conn.execute("DELETE FROM ingestion_state WHERE source_id = 'ncei_ghcnd' AND partition_key = 'USW00012960'")
            conn.execute("DELETE FROM rejected_record WHERE run_id IN (%s, %s)", (run1, run2))
            conn.execute("DELETE FROM ingestion_run WHERE run_id IN (%s, %s)", (run1, run2))
