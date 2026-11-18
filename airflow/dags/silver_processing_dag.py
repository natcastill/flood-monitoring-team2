"""Silver processing DAG (TaskFlow API + FileSensor) - Flood Monitoring and Risk, Team 2.

sense_new_bronze_json          FileSensor on datalake_bronze/texas_flood_sources_*.json
discover_unprocessed_bronze    Bronze files not yet listed in datalake_silver/_manifest.jsonl
transform_and_persist_silver   preprocessing (flood_monitor.preprocessing) -> one Parquet per topic:
    datalake_silver/<topic>_<YYYYMMDD_HHMMSS>_from_<bronze stem>.parquet
    topics: nws_observations, nws_alerts, daily_precipitation, flood_events
Traceability: the Bronze file name is in the Parquet file name, in the column
bronze_source_file and in the Parquet key-value metadata; the manifest lists every
Bronze file processed, what it produced and its data-quality counters (used by Gold).
The per-file logic lives in flood_monitor.silver (shared with scripts/replay_bronze_to_silver.py).
"""
import glob
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from flood_monitor.config import BRONZE_DIR, SILVER_DIR
from flood_monitor.silver import process_bronze_file, processed_bronze_files

logger = logging.getLogger(__name__)

default_args = {
    "owner": "team2_flood_monitor",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="silver_flood_processing_dag",
    description="Senses new Bronze JSON files and transforms them into Silver Parquet datasets",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["workshop3", "silver", "parquet", "texas-flood"],
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
        done = processed_bronze_files(SILVER_DIR)
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
            generated.extend(process_bronze_file(b_path, SILVER_DIR))
        return generated

    unprocessed_list = discover_unprocessed_bronze()
    wait_for_bronze_file >> unprocessed_list
    transform_and_persist_silver(unprocessed_list)


silver_flood_processing()
