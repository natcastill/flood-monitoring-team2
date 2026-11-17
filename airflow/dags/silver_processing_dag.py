import glob
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from flood_monitor.config import BRONZE_DIR, SILVER_DIR
from flood_monitor.preprocessing import process_nws_alerts, process_nws_observations

logger = logging.getLogger(__name__)

default_args = {
    "owner": "team2_flood_monitor",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="silver_flood_processing_dag",
    description="Senses new Bronze JSON files and transforms them into Silver Parquet datasets",
    schedule="*/10 * * * *",  # Checks every 10 min for unprocessed Bronze files
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["workshop2", "silver", "parquet", "texas-flood"],
)
def silver_flood_processing():

    wait_for_bronze_file = FileSensor(
        task_id="sense_new_bronze_json",
        filepath=f"{BRONZE_DIR}/texas_flood_sources_*.json",
        fs_conn_id="fs_default",
        poke_interval=30,   # Check every 30 seconds
        timeout=300,        # Timeout after 5 minutes if no file exists
        mode="poke",
    )

    @task
    def discover_unprocessed_bronze() -> list:
        """
        Idempotency check: finds Bronze JSON files whose source stem has not yet
        been persisted into datalake_silver/.
        """
        os.makedirs(SILVER_DIR, exist_ok=True)
        bronze_files = sorted(glob.glob(os.path.join(BRONZE_DIR, "texas_flood_sources_*.json")))
        existing_silver = os.listdir(SILVER_DIR)

        unprocessed = []
        for b_path in bronze_files:
            stem = os.path.splitext(os.path.basename(b_path))[0]
            already_processed = any(stem in s_file for s_file in existing_silver)
            if not already_processed:
                unprocessed.append(b_path)

        logger.info(f"Discovered {len(unprocessed)} unprocessed Bronze file(s).")
        return unprocessed

    @task
    def transform_and_persist_silver(bronze_files: list) -> list:
        """
        Applies preprocessing pipeline (UTC normalization, null handling, IQR/domain outlier
        filtering, text normalization, deduplication) and persists Parquet files.
        """
        if not bronze_files:
            logger.info("No new Bronze files to process. Skipping idempotently.")
            return []

        generated_parquets = []

        for b_path in bronze_files:
            b_filename = os.path.basename(b_path)
            b_stem = os.path.splitext(b_filename)[0]
            proc_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

            with open(b_path, "r", encoding="utf-8") as f:
                raw_payload = json.load(f)

            # 1. Process NWS Station Observations -> Silver Parquet
            df_obs = process_nws_observations(raw_payload, source_filename=b_filename)
            if not df_obs.empty:
                obs_parquet_name = f"nws_observations_{proc_ts}_from_{b_stem}.parquet"
                obs_out_path = os.path.join(SILVER_DIR, obs_parquet_name)
                df_obs.to_parquet(obs_out_path, engine="pyarrow", compression="snappy", index=False)
                generated_parquets.append(obs_out_path)
                logger.info(f"Wrote {len(df_obs)} rows to {obs_out_path}")

            # 2. Process NWS Flood Alerts -> Silver Parquet
            df_alerts = process_nws_alerts(raw_payload, source_filename=b_filename)
            if not df_alerts.empty:
                alerts_parquet_name = f"nws_alerts_{proc_ts}_from_{b_stem}.parquet"
                alerts_out_path = os.path.join(SILVER_DIR, alerts_parquet_name)
                df_alerts.to_parquet(alerts_out_path, engine="pyarrow", compression="snappy", index=False)
                generated_parquets.append(alerts_out_path)
                logger.info(f"Wrote {len(df_alerts)} rows to {alerts_out_path}")

        return generated_parquets

    unprocessed_list = discover_unprocessed_bronze()
    wait_for_bronze_file >> unprocessed_list
    transform_and_persist_silver(unprocessed_list)


silver_flood_processing()