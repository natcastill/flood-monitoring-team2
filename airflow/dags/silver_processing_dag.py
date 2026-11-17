"""Silver processing DAG (TaskFlow API + FileSensor) - Flood Monitoring and Risk, Team 2.

sense_new_bronze_json          FileSensor on datalake_bronze/texas_flood_sources_*.json
discover_unprocessed_bronze    Bronze files not yet listed in datalake_silver/_manifest.jsonl
transform_and_persist_silver   preprocessing (flood_monitor.preprocessing) -> one Parquet per topic:
    datalake_silver/<topic>_<YYYYMMDD_HHMMSS>_from_<bronze stem>.parquet
    topics: nws_observations, nws_alerts, daily_precipitation, flood_events
Traceability: the Bronze file name is in the Parquet file name, in the column
bronze_source_file and in the Parquet key-value metadata; the manifest lists every
Bronze file processed and what it produced.
"""
import glob
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from flood_monitor.config import BRONZE_DIR, SILVER_DIR
from flood_monitor.preprocessing import (PIPELINE_VERSION, PROCESSORS, drop_already_in_silver,
                                         enforce_schema, remember_in_registry)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "team2_flood_monitor",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
MANIFEST = Path(SILVER_DIR) / "_manifest.jsonl"


def _processed() -> set:
    if not MANIFEST.exists():
        return set()
    return {json.loads(line)["bronze_file"] for line in MANIFEST.read_text(encoding="utf-8").splitlines()
            if line.strip()}


@dag(
    dag_id="silver_flood_processing_dag",
    description="Senses new Bronze JSON files and transforms them into Silver Parquet datasets",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["workshop2", "silver", "parquet", "texas-flood"],
)
def silver_flood_processing():

    # Bronze arrives every 15 min. The sensor checks every 60 s (poke_interval) and gives up
    # after 9 min (timeout), before the next 10-min run starts. "reschedule" frees the worker
    # slot between checks; soft_fail marks the run "skipped" instead of "failed".
    wait_for_bronze_file = FileSensor(
        task_id="sense_new_bronze_json",
        filepath=f"{BRONZE_DIR}/texas_flood_sources_*.json",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=9 * 60,
        mode="reschedule",
        soft_fail=True,
    )

    @task
    def discover_unprocessed_bronze() -> list:
        """Idempotency check: Bronze files that are not in the Silver manifest yet."""
        os.makedirs(SILVER_DIR, exist_ok=True)
        done = _processed()
        bronze_files = sorted(glob.glob(os.path.join(BRONZE_DIR, "texas_flood_sources_*.json")))
        unprocessed = [p for p in bronze_files if os.path.basename(p) not in done]
        logger.info("Discovered %d unprocessed Bronze file(s).", len(unprocessed))
        return unprocessed

    @task
    def transform_and_persist_silver(bronze_files: list) -> list:
        """Preprocess each new Bronze file and persist one Parquet file per topic."""
        if not bronze_files:
            logger.info("No new Bronze files to process. Skipping idempotently.")
            return []
        generated = []
        for b_path in bronze_files:
            b_filename = os.path.basename(b_path)
            b_stem = os.path.splitext(b_filename)[0]
            proc_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            with open(b_path, "r", encoding="utf-8") as f:
                raw_payload = json.load(f)
            outputs = {}
            for topic, process in PROCESSORS.items():
                df = process(raw_payload, source_filename=b_filename)
                if df.empty:
                    outputs[topic] = {"rows_in": 0}
                    continue
                df, stats = drop_already_in_silver(df, SILVER_DIR, topic)   # cross-batch dedup
                if df.empty:
                    logger.info("%s: all %d rows already in Silver.", topic, stats["rows_in"])
                    outputs[topic] = stats
                    continue
                df["processed_at_utc"] = datetime.now(timezone.utc)
                table = enforce_schema(topic, df)
                table = table.replace_schema_metadata({
                    b"bronze_source_file": b_filename.encode(),
                    b"pipeline_version": PIPELINE_VERSION.encode(),
                    b"preprocessing_stats": json.dumps(stats).encode()})
                out = Path(SILVER_DIR) / f"{topic}_{proc_ts}_from_{b_stem}.parquet"
                tmp = out.with_suffix(".parquet.tmp")
                pq.write_table(table, tmp, compression="snappy")
                os.replace(tmp, out)
                remember_in_registry(table, SILVER_DIR, topic)
                generated.append(str(out))
                outputs[topic] = {**stats, "rows_written": table.num_rows, "file": out.name}
                logger.info("Wrote %d rows to %s", table.num_rows, out)
            with MANIFEST.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"bronze_file": b_filename, "processed_at_utc": proc_ts,
                                     "outputs": outputs}) + "\n")
        return generated

    unprocessed_list = discover_unprocessed_bronze()
    wait_for_bronze_file >> unprocessed_list
    transform_and_persist_silver(unprocessed_list)


silver_flood_processing()
