"""Gold processing DAG (TaskFlow API + PySpark) - Flood Monitoring and Risk, Team 2.

Every hour at minute 20 (after the Bronze run at :00/:15 and the Silver run at :10):
  check_silver_inputs          every topic has at least one Silver Parquet file
  build_governance_summary     PySpark -> datalake_gold/governance_YYYYMMDD_HHMMSS.parquet
  build_storytelling_summary   PySpark -> datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet
  validate_gold_outputs        row counts, columns and expected sections / KPI groups
  record_gold_manifest         lineage (Silver files read, outputs) + retention of old runs

PySpark runs in local mode (local[*]) inside the Airflow container: Spark reads the whole
datalake_silver/ folder as one DataFrame per topic, the same pattern as a cloud bucket.
The two Spark tasks run one after the other so that only one Spark JVM (SPARK_DRIVER_MEMORY,
default 1g) is alive at a time on a laptop.
The Gold files are the only data source of the Plotly Dash dashboards (Workshop 4).
"""
import glob
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from flood_monitor.config import BRONZE_DIR, GOLD_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

TOPICS = ("nws_observations", "nws_alerts", "daily_precipitation", "flood_events")
KEEP_GOLD_RUNS = int(os.getenv("GOLD_KEEP_RUNS", "48"))   # 2 days of hourly runs
GOV_GROUPS = {"completeness", "volume", "uniqueness", "validity", "text", "timeliness", "accuracy"}
STORY_SECTIONS = {"headline", "severity_distribution", "trend", "seasonality", "county_history",
                  "category_breakdown", "source_comparison", "keywords", "keyword_severity",
                  "current_rainfall", "rain_climatology", "alerts", "flood_risk_index", "fri_backtest",
                  "narrative"}

default_args = {
    "owner": "team2_flood_monitor",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}


def _run_stamp() -> tuple[str, datetime]:
    """File timestamp = UTC end of the data interval (same rule as Bronze)."""
    ctx = get_current_context()
    end = ctx["data_interval_end"].astimezone(timezone.utc)
    return end.strftime("%Y%m%d_%H%M%S"), end


@dag(
    dag_id="gold_flood_processing_dag",
    description="PySpark: reads all Silver Parquet files and writes the governance and storytelling "
                "Gold summaries",
    schedule="20 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["workshop3", "gold", "pyspark", "texas-flood"],
)
def gold_flood_processing():

    @task
    def check_silver_inputs() -> dict:
        """Inventory of the Silver layer; fail early if a topic has no file yet."""
        inventory = {t: sorted(os.path.basename(p) for p in glob.glob(os.path.join(SILVER_DIR, f"{t}_*.parquet")))
                     for t in TOPICS}
        for t, files in inventory.items():
            logger.info("%s: %d Silver file(s)", t, len(files))
        missing = [t for t, files in inventory.items() if not files]
        if missing:
            raise ValueError(f"No Silver files for {missing}: run the Bronze and Silver DAGs first")
        return inventory

    @task
    def build_governance_summary(inventory: dict) -> str:
        from flood_monitor.gold.governance import build_governance
        from flood_monitor.gold.spark import get_spark, write_single_parquet
        stamp, run_ts = _run_stamp()
        spark = get_spark("flood-gold-governance")
        try:
            logger.info("Spark %s, master %s, conf: %s", spark.version, spark.sparkContext.master,
                        {k: v for k, v in spark.sparkContext.getConf().getAll()
                         if k in ("spark.driver.memory", "spark.sql.shuffle.partitions",
                                  "spark.sql.session.timeZone")})
            df = build_governance(spark, SILVER_DIR, BRONZE_DIR, run_ts)
            out = write_single_parquet(df, os.path.join(GOLD_DIR, f"governance_{stamp}.parquet"))
        finally:
            spark.stop()
        logger.info("Governance summary written: %s", out)
        return out

    @task
    def build_storytelling_summary(inventory: dict) -> str:
        from flood_monitor.gold.spark import get_spark, write_single_parquet
        from flood_monitor.gold.storytelling import build_storytelling
        stamp, run_ts = _run_stamp()
        spark = get_spark("flood-gold-storytelling")
        try:
            df = build_storytelling(spark, SILVER_DIR, run_ts)
            out = write_single_parquet(df, os.path.join(GOLD_DIR, f"storytelling_{stamp}.parquet"))
        finally:
            spark.stop()
        logger.info("Storytelling summary written: %s", out)
        return out

    @task
    def validate_gold_outputs(gov_path: str, story_path: str) -> dict:
        """Read both files back (as the dashboard will) and check their content."""
        import pyarrow.parquet as pq
        gov, story = pq.read_table(gov_path).to_pandas(), pq.read_table(story_path).to_pandas()
        errors = []
        if gov.empty or story.empty:
            errors.append("empty Gold summary")
        missing_groups = GOV_GROUPS - set(gov["kpi_group"])
        missing_sections = STORY_SECTIONS - set(story["section"])
        if missing_groups:
            errors.append(f"governance: missing KPI groups {sorted(missing_groups)}")
        if missing_sections:
            errors.append(f"storytelling: missing sections {sorted(missing_sections)}")
        failed = gov[gov["status"] == "fail"][["kpi_name", "topic", "field"]].to_dict("records")
        report = {"governance_rows": len(gov), "storytelling_rows": len(story),
                  "kpi_fail": failed, "errors": errors}
        logger.info("Gold validation: %s", report)
        if errors:
            raise ValueError(f"Gold validation failed: {errors}")
        return report

    @task
    def record_gold_manifest(inventory: dict, gov_path: str, story_path: str, report: dict) -> None:
        """Lineage of this run and retention of the newest KEEP_GOLD_RUNS files of each summary."""
        stamp, _ = _run_stamp()
        line = {"gold_run": stamp, "governance_file": os.path.basename(gov_path),
                "storytelling_file": os.path.basename(story_path), "silver_inputs": inventory,
                "validation": report, "written_at_utc": datetime.now(timezone.utc).isoformat()}
        with (Path(GOLD_DIR) / "_gold_manifest.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(line) + "\n")
        for kind in ("governance", "storytelling"):
            files = sorted(glob.glob(os.path.join(GOLD_DIR, f"{kind}_*.parquet")))
            for old in files[:-KEEP_GOLD_RUNS]:
                os.remove(old)
                logger.info("Retention: removed %s", old)

    inventory = check_silver_inputs()
    gov = build_governance_summary(inventory)
    story = build_storytelling_summary(inventory)
    gov >> story                      # one Spark JVM at a time
    report = validate_gold_outputs(gov, story)
    record_gold_manifest(inventory, gov, story, report)


gold_flood_processing()
