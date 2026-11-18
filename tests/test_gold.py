"""Gold layer: both PySpark summaries are built from a Silver layer made from the test fixture."""
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required for PySpark", allow_module_level=True)

from flood_monitor.silver import process_bronze_file  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "bronze" / "texas_flood_sources_20261116_233000.json"
RUN = datetime(2026, 11, 17, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    bronze, silver, gold = root / "datalake_bronze", root / "datalake_silver", root / "datalake_gold"
    bronze.mkdir()
    shutil.copy(FIX, bronze / FIX.name)
    process_bronze_file(str(bronze / FIX.name), str(silver))
    return bronze, silver, gold


@pytest.fixture(scope="module")
def spark():
    from flood_monitor.gold.spark import get_spark
    s = get_spark("test-gold")
    yield s
    s.stop()


def test_governance_summary(lake, spark):
    from flood_monitor.gold.governance import build_governance
    from flood_monitor.gold.spark import write_single_parquet
    bronze, silver, gold = lake
    out = write_single_parquet(build_governance(spark, str(silver), str(bronze), RUN),
                               str(gold / "governance_20261117_000000.parquet"))
    df = pq.read_table(out).to_pandas()
    assert {"null_rate", "duplicate_rate_after_dedup", "schema_compliance_files", "outlier_rate",
            "text_length_words", "ingestion_frequency_compliance", "ingestion_lag_hours",
            "record_lag_hours"} <= set(df["kpi_name"])
    files = df[df["kpi_name"] == "schema_compliance_files"]
    assert (files["value"] == 100).all()


def test_storytelling_summary(lake, spark):
    from flood_monitor.gold.spark import write_single_parquet
    from flood_monitor.gold.storytelling import build_storytelling
    _, silver, gold = lake
    out = write_single_parquet(build_storytelling(spark, str(silver), RUN),
                               str(gold / "storytelling_20261117_000000.parquet"))
    df = pq.read_table(out).to_pandas()
    assert {"headline", "keywords", "county_history", "current_rainfall", "flood_risk_index",
            "narrative"} <= set(df["section"])
    assert df.loc[df["section"] == "flood_risk_index", "label"].isin(["LOW", "MEDIUM", "HIGH"]).all()
