"""Spark session and I/O helpers shared by the Gold jobs."""
from __future__ import annotations

import glob
import os
import shutil
import sys
import tempfile
from pathlib import Path

from flood_monitor.preprocessing import SCHEMAS, COMMON

TOPICS = tuple(SCHEMAS)


def get_spark(app_name: str = "flood-monitor-gold"):
    """Local-mode Spark inside the Airflow container (no cluster needed).

    local[*]        one executor thread per CPU core of the container
    driver memory   SPARK_DRIVER_MEMORY (default 1g): driver and executor share one JVM in
                    local mode, so this is the whole Spark heap
    shuffle         SPARK_SHUFFLE_PARTITIONS (default 8 instead of 200): the Gold inputs are
                    a few hundred MB at most, 200 tiny shuffle tasks would only add overhead
    UTC             session and JVM time zone, so dates match the Silver UTC timestamps
    """
    from pyspark.sql import SparkSession
    # Python workers must run the same interpreter as the driver (Airflow task process)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    local_dir = os.getenv("SPARK_LOCAL_DIR", os.path.join(tempfile.gettempdir(), "spark-local"))
    builder = (SparkSession.builder
               .master(os.getenv("SPARK_MASTER", "local[*]"))
               .appName(app_name)
               .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
               .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
               .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
               .config("spark.ui.enabled", "false")
               .config("spark.local.dir", local_dir)
               .config("spark.sql.parquet.compression.codec", "snappy"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_topic(spark, silver_dir: str, topic: str):
    """All Silver files of one topic as ONE DataFrame (cloud-folder read pattern).

    spark.read.parquet(silver_dir) alone would mix the four topics, which have different
    schemas (Workshop 3 experiment), so the folder is read with a file-name filter.
    Files and folders starting with "_" (manifest, key registry) are ignored by Spark.
    """
    return (spark.read
            .option("pathGlobFilter", f"{topic}_*.parquet")
            .option("mergeSchema", "true")
            .parquet(silver_dir))


def latest_version(df):
    """Keep the newest version of each record (Silver keeps changed rows as is_update=True)."""
    from pyspark.sql import Window, functions as F
    w = Window.partitionBy("event_id").orderBy(F.col("processed_at_utc").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def silver_files(silver_dir: str, topic: str) -> list:
    return sorted(glob.glob(os.path.join(silver_dir, f"{topic}_*.parquet")))


def expected_spark_schema(topic: str):
    """Spark StructType equivalent of the PyArrow Silver schema of a topic."""
    import pyarrow as pa
    from pyspark.sql import types as T
    def conv(t):
        if pa.types.is_string(t):
            return T.StringType()
        if pa.types.is_float64(t):
            return T.DoubleType()
        if pa.types.is_boolean(t):
            return T.BooleanType()
        if pa.types.is_timestamp(t):
            return T.TimestampType()
        if pa.types.is_int8(t):
            return T.ByteType()
        if pa.types.is_int32(t):
            return T.IntegerType()
        if pa.types.is_date32(t):
            return T.DateType()
        if pa.types.is_list(t):
            return T.ArrayType(conv(t.value_type))
        raise TypeError(t)
    spec = {**SCHEMAS[topic], **COMMON}
    return T.StructType([T.StructField(c, conv(t), True) for c, (t, _) in spec.items()])


def write_single_parquet(df, out_path: str) -> str:
    """Write a (small) DataFrame as exactly one Parquet file with a fixed name.

    Spark always writes a folder of part files; the summary is coalesced to one partition,
    written to a temporary folder next to the target and the part file is renamed, so the
    dashboard finds datalake_gold/<name>_YYYYMMDD_HHMMSS.parquet.
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = out.parent / f"_tmp_{out.stem}"
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp_dir))
    part = next(tmp_dir.glob("part-*.parquet"))
    tmp_file = out.with_suffix(".parquet.tmp")
    shutil.move(str(part), tmp_file)
    os.replace(tmp_file, out)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return str(out)
