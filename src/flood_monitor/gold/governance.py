"""Governance summary (PySpark): data-quality KPIs over the whole Silver layer.

Output: one tidy ("long") table, one row per KPI value, so the governance dashboard can draw
any KPI with a filter on kpi_name / topic / field:

    kpi_group   kpi_name   topic   field   period_grain   period_start
    value   numerator   denominator   unit   threshold   status   note   gold_run_ts

Every KPI is justified by a Silver data-quality finding (see the Workshop 3 report, Section 3).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F, types as T

from flood_monitor.gold.spark import TOPICS, expected_spark_schema, latest_version, read_topic, silver_files
from flood_monitor.preprocessing import COMMON, SCHEMAS

COLUMNS = [
    ("kpi_group", T.StringType()), ("kpi_name", T.StringType()), ("topic", T.StringType()),
    ("field", T.StringType()), ("period_grain", T.StringType()), ("period_start", T.DateType()),
    ("value", T.DoubleType()), ("numerator", T.LongType()), ("denominator", T.LongType()),
    ("unit", T.StringType()), ("threshold", T.DoubleType()), ("status", T.StringType()),
    ("note", T.StringType()),
]

# Event time of each topic (volume per period and freshness)
EVENT_TIME = {"nws_observations": "ts_utc", "nws_alerts": "sent_utc",
              "daily_precipitation": "obs_date", "flood_events": "begin_utc"}
EVENT_GRAIN = {"nws_observations": "day", "nws_alerts": "day", "daily_precipitation": "year",
               "flood_events": "year"}
# Expected Bronze runs per UTC day for each source (Workshop 1 ingestion contract)
EXPECTED_RUNS_PER_DAY = {"nws_alerts": 96, "storm_events_catalog": 96, "nws_observations": 24,
                         "ghcnd": 1, "storm_events_details": 1}
# Maximum acceptable lag (hours) between the last successful pull of a source and the Gold run
INGESTION_LAG_HOURS = {"nws_alerts": 0.5, "storm_events_catalog": 0.5, "nws_observations": 1.5,
                       "ghcnd": 26, "storm_events_details": 26}
# Nulls that are expected by design (documented, not a defect)
STRUCTURAL_NULLS = {
    ("nws_observations", "precip_1h_mm"): "5-minute reports never carry rainfall (DQ #6)",
    ("nws_observations", "precip_3h_mm"): "only reported in 3/6-hourly METAR",
    ("nws_observations", "precip_6h_mm"): "only reported in 6-hourly METAR",
    ("nws_observations", "severity_level"): "undefined when the rainfall is unknown",
    ("daily_precipitation", "prcp_zscore"): "Z-score only computed on wet days (zero-inflated)",
    ("nws_alerts", "centroid_lat"): "alerts without polygon geometry (DQ #8)",
    ("nws_alerts", "centroid_lon"): "alerts without polygon geometry (DQ #8)",
    ("flood_events", "damage_property_usd"): "damage not estimated by NOAA (DQ #2)",
    ("flood_events", "damage_crops_usd"): "damage not estimated by NOAA (DQ #2)",
}
SENTINELS = ("unknown", "Unknown", "MISSING", "none")


# --------------------------------------------------------------------------- helpers
def std(df: DataFrame, **lits) -> DataFrame:
    """Give any partial KPI DataFrame the standard columns and types."""
    for name, typ in COLUMNS:
        if name in lits:
            df = df.withColumn(name, F.lit(lits[name]).cast(typ))
        elif name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(typ))
        else:
            df = df.withColumn(name, F.col(name).cast(typ))
    return df.select([c for c, _ in COLUMNS])


def pct(num, den):
    return F.when(den > 0, F.round(num * 100.0 / den, 4))


def bronze_day(col="bronze_source_file"):
    return F.to_date(F.regexp_extract(F.col(col), r"(\d{8})_\d{6}", 1), "yyyyMMdd")


def period(col, grain):
    c = F.col(col)
    if grain == "year":
        return F.trunc(F.to_date(c), "year")
    if grain == "month":
        return F.trunc(F.to_date(c), "month")
    return F.to_date(c)


def unpivot(df: DataFrame, cols: list, key="field", val="v") -> DataFrame:
    expr = "stack({n}, {pairs}) as ({k}, {v})".format(
        n=len(cols), pairs=", ".join(f"'{c}', `{c}`" for c in cols), k=key, v=val)
    return df.selectExpr(*[c for c in df.columns if c not in cols], expr)


# --------------------------------------------------------------------------- KPI blocks
def null_rates(df: DataFrame, topic: str) -> list:
    """G1: null (or empty-list) rate per field, and sentinel rate per string field."""
    required = {c for c, (_, req) in {**SCHEMAS[topic], **COMMON}.items() if req}
    nulls, sentinels = [], []
    for f in df.schema.fields:
        c = F.col(f"`{f.name}`")
        cond = (c.isNull() | (F.size(c) == 0)) if isinstance(f.dataType, T.ArrayType) else c.isNull()
        nulls.append(F.sum(cond.cast("long")).alias(f.name))
        if isinstance(f.dataType, T.StringType):
            sentinels.append(F.sum(c.isin(*SENTINELS).cast("long")).alias(f"{f.name}"))
    n = df.count()
    out = []
    long_nulls = unpivot(df.agg(*nulls), [f.name for f in df.schema.fields])
    req = F.col("field").isin(*required)
    notes = F.lit(None).cast("string")
    for (t, fld), note in STRUCTURAL_NULLS.items():
        if t == topic:
            notes = F.when(F.col("field") == fld, F.lit(note)).otherwise(notes)
    out.append(std(long_nulls
                   .withColumn("numerator", F.col("v")).withColumn("denominator", F.lit(n))
                   .withColumn("value", pct(F.col("v"), F.lit(n)))
                   .withColumn("threshold", F.when(req, F.lit(0.0)))
                   .withColumn("status", F.when(req, F.when(F.col("v") == 0, "ok").otherwise("fail"))
                               .otherwise("info"))
                   .withColumn("note", F.when(req, F.lit("required field")).otherwise(notes)),
                   kpi_group="completeness", kpi_name="null_rate", topic=topic, period_grain="all",
                   unit="pct"))
    if sentinels:
        str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
        long_s = unpivot(df.agg(*sentinels), str_cols).where("v > 0")
        out.append(std(long_s.withColumn("numerator", F.col("v")).withColumn("denominator", F.lit(n))
                       .withColumn("value", pct(F.col("v"), F.lit(n))),
                       kpi_group="completeness", kpi_name="sentinel_rate", topic=topic,
                       period_grain="all", unit="pct", status="info",
                       note="values replaced by a sentinel (unknown/MISSING/none)"))
    return out


def volume(df_all: DataFrame, topic: str, files: int) -> list:
    """G2: records per ingestion day (and cumulative), per event period, total and files."""
    from pyspark.sql import Window
    by_day = df_all.groupBy(bronze_day().alias("period_start")).agg(F.count("*").alias("numerator"))
    w = Window.orderBy("period_start").rowsBetween(Window.unboundedPreceding, 0)
    cum = by_day.withColumn("numerator", F.sum("numerator").over(w))
    grain = EVENT_GRAIN[topic]
    by_event = df_all.groupBy(period(EVENT_TIME[topic], grain).alias("period_start")) \
                     .agg(F.count("*").alias("numerator"))
    total = df_all.count()
    spark = df_all.sparkSession
    tot = spark.createDataFrame([(total, files)], "numerator long, files long")
    return [
        std(by_day.withColumn("value", F.col("numerator")), kpi_group="volume", kpi_name="records_ingested",
            topic=topic, period_grain="day", unit="records", note="by Bronze ingestion day (UTC)"),
        std(cum.withColumn("value", F.col("numerator")), kpi_group="volume",
            kpi_name="records_ingested_cumulative", topic=topic, period_grain="day", unit="records"),
        std(by_event.withColumn("value", F.col("numerator")), kpi_group="volume",
            kpi_name="records_by_event_period", topic=topic, period_grain=grain, unit="records",
            note=f"by {EVENT_TIME[topic]}"),
        std(tot.withColumn("value", F.col("numerator")), kpi_group="volume", kpi_name="records_total",
            topic=topic, period_grain="all", unit="records"),
        std(tot.withColumn("value", F.col("files")).withColumn("numerator", F.col("files")),
            kpi_group="volume", kpi_name="silver_files", topic=topic, period_grain="all", unit="files"),
    ]


def manifest_counters(spark, silver_dir: str) -> DataFrame | None:
    """Silver manifest (one JSON line per Bronze file) -> one row per (Bronze file, topic)."""
    path = os.path.join(silver_dir, "_manifest.jsonl")
    if not os.path.exists(path):
        return None
    # Spark skips files whose name starts with "_" (Hadoop hidden-file rule), so the lines are
    # handed to Spark's JSON reader as an RDD of strings.
    with open(path, encoding="utf-8") as fh:
        lines = [ln for ln in fh.read().splitlines() if ln.strip()]
    mf = spark.read.json(spark.sparkContext.parallelize(lines))
    out = None
    counters = ["rows_raw", "filtered_non_flood", "dropped_required", "dropped_outside_texas",
                "out_of_range", "batch_duplicates", "rows_in", "already_in_silver", "updates",
                "rows_written", "schema_rejected", "encoding_repaired"]
    topics_present = mf.schema["outputs"].dataType.names
    for topic in TOPICS:
        if topic not in topics_present:
            continue
        fields = mf.schema["outputs"].dataType[topic].dataType.names
        cols = [F.coalesce(F.col(f"outputs.{topic}.{c}"), F.lit(0)).cast("long").alias(c) if c in fields
                else F.lit(0).cast("long").alias(c) for c in counters]
        part = mf.select(F.col("bronze_file"), F.lit(topic).alias("topic"), *cols)
        out = part if out is None else out.unionByName(part)
    return out


def duplicates(mc: DataFrame, silver: dict) -> list:
    """G3: duplicate rate before de-duplication (in-batch + cross-batch) and after."""
    out = []
    offered = F.col("rows_in") + F.col("batch_duplicates")
    dup = F.col("batch_duplicates") + F.col("already_in_silver")
    overall = mc.groupBy("topic").agg(F.sum(dup).alias("numerator"), F.sum(offered).alias("denominator"))
    out.append(std(overall.withColumn("value", pct(F.col("numerator"), F.col("denominator"))),
                   kpi_group="uniqueness", kpi_name="duplicate_rate_before_dedup", period_grain="all",
                   unit="pct", note="in-batch duplicates + rows already in Silver / rows offered"))
    daily = mc.groupBy("topic", bronze_day("bronze_file").alias("period_start")) \
              .agg(F.sum(dup).alias("numerator"), F.sum(offered).alias("denominator")) \
              .where("denominator > 0")
    out.append(std(daily.withColumn("value", pct(F.col("numerator"), F.col("denominator"))),
                   kpi_group="uniqueness", kpi_name="duplicate_rate_before_dedup", period_grain="day",
                   unit="pct"))
    for topic, df in silver.items():
        n = df.count()
        distinct = df.select("event_id", "record_hash").distinct().count()
        upd = df.where("is_update").count()
        spark = df.sparkSession
        row = spark.createDataFrame([(n - distinct, n, upd)], "dups long, n long, upd long")
        out.append(std(row.withColumn("numerator", F.col("dups")).withColumn("denominator", F.col("n"))
                       .withColumn("value", pct(F.col("dups"), F.col("n")))
                       .withColumn("status", F.when(F.col("dups") == 0, "ok").otherwise("fail")),
                       kpi_group="uniqueness", kpi_name="duplicate_rate_after_dedup", topic=topic,
                       period_grain="all", unit="pct", threshold=0.0))
        out.append(std(row.withColumn("numerator", F.col("upd")).withColumn("denominator", F.col("n"))
                       .withColumn("value", pct(F.col("upd"), F.col("n"))),
                       kpi_group="uniqueness", kpi_name="update_rate", topic=topic, period_grain="all",
                       unit="pct", status="info", note="changed versions of a known record (is_update)"))
    return out


def schema_compliance(spark, silver_dir: str, mc: DataFrame | None) -> list:
    """G4: share of Silver files whose schema equals the expected one, and of records accepted."""
    rows = []
    for topic in TOPICS:
        expected = [(f.name, f.dataType.simpleString()) for f in expected_spark_schema(topic).fields]
        files = silver_files(silver_dir, topic)
        ok = sum(1 for f in files
                 if [(x.name, x.dataType.simpleString()) for x in spark.read.parquet(f).schema.fields] == expected)
        rows.append((topic, ok, len(files)))
    files_df = spark.createDataFrame(rows, "topic string, numerator long, denominator long")
    out = [std(files_df.withColumn("value", pct(F.col("numerator"), F.col("denominator")))
               .withColumn("status", F.when(F.col("numerator") == F.col("denominator"), "ok").otherwise("fail")),
               kpi_group="validity", kpi_name="schema_compliance_files", period_grain="all", unit="pct",
               threshold=100.0, note="column names, order and types equal the Silver contract")]
    if mc is not None:
        rec = mc.groupBy("topic").agg(
            F.sum("rows_written").alias("numerator"),
            F.sum(F.col("rows_written") + F.col("schema_rejected") + F.col("dropped_required")).alias("denominator"))
        out.append(std(rec.withColumn("value", pct(F.col("numerator"), F.col("denominator"))),
                       kpi_group="validity", kpi_name="schema_compliance_records", period_grain="all",
                       unit="pct", note="rows accepted / rows checked (required fields and types)"))
        rej = unpivot(mc.groupBy("topic").agg(*[F.sum(c).alias(c) for c in
                      ("filtered_non_flood", "dropped_required", "dropped_outside_texas", "out_of_range",
                       "schema_rejected")]),
                      ["filtered_non_flood", "dropped_required", "dropped_outside_texas", "out_of_range",
                       "schema_rejected"])
        out.append(std(rej.withColumn("numerator", F.col("v")).withColumn("value", F.col("v")),
                       kpi_group="validity", kpi_name="records_rejected", period_grain="all", unit="records",
                       note="US5: records rejected or nulled, by reason"))
    return out


OUTLIER_FLAGS = {
    "nws_observations": [("out_of_range", "true", "all_measurements"),
                         ("is_outlier_precip", "precip_1h_mm > 0", "precip_1h_mm"),
                         ("abs(temperature_zscore) > 3", "temperature_zscore is not null", "temperature_c")],
    "daily_precipitation": [("is_outlier_iqr", "prcp_mm > 0", "prcp_mm (IQR, k=3)"),
                            ("is_outlier_z", "prcp_mm > 0", "prcp_mm (Z-score, |z|>3)"),
                            ("qc_failed", "true", "prcp_mm (NOAA quality flag)")],
    "flood_events": [("is_outlier_damage", "damage_property_usd > 0", "damage_property_usd"),
                     ("is_outlier_duration", "duration_hours is not null", "duration_hours")],
}


def outliers(silver: dict) -> list:
    """G5: outlier rate per numeric field (flags computed in Silver)."""
    out = []
    for topic, flags in OUTLIER_FLAGS.items():
        df = silver[topic]
        for flag, base, field in flags:
            agg = df.where(base).agg(F.sum(F.expr(flag).cast("long")).alias("numerator"),
                                     F.count("*").alias("denominator"))
            out.append(std(agg.withColumn("value", pct(F.col("numerator"), F.col("denominator"))),
                           kpi_group="validity", kpi_name="outlier_rate", topic=topic, field=field,
                           period_grain="all", unit="pct", status="info",
                           note="flagged and kept: extremes are the flood signal"))
    return out


def text_stats(silver: dict) -> list:
    """G6 and G7: text length (words) per source and period, and language distribution."""
    out = []
    for topic, col, grain, tcol in (("flood_events", "narrative_word_count", "year", "begin_utc"),
                                    ("nws_alerts", "description_word_count", "day", "sent_utc")):
        df = silver[topic]
        stats = [F.round(F.avg(col), 2).alias("mean"), F.percentile_approx(col, 0.5).cast("double").alias("median"),
                 F.min(col).cast("double").alias("min"), F.max(col).cast("double").alias("max")]
        for g, frame in (("all", df.agg(*stats, F.count("*").alias("denominator"))),
                         (grain, df.groupBy(period(tcol, grain).alias("period_start"))
                          .agg(*stats, F.count("*").alias("denominator")))):
            long_ = unpivot(frame, ["mean", "median", "min", "max"])
            out.append(std(long_.withColumn("value", F.col("v")), kpi_group="text",
                           kpi_name="text_length_words", topic=topic, period_grain=g, unit="words"))
        lang = df.groupBy(F.col("text_language").alias("field")).agg(F.count("*").alias("numerator"))
        total = df.count()
        out.append(std(lang.withColumn("denominator", F.lit(total))
                       .withColumn("value", pct(F.col("numerator"), F.lit(total))),
                       kpi_group="text", kpi_name="language_distribution", topic=topic, period_grain="all",
                       unit="pct", note="heuristic stop-word language check"))
        enc = df.agg(F.sum(F.col("encoding_repaired").cast("long")).alias("numerator"),
                     F.count("*").alias("denominator"))
        out.append(std(enc.withColumn("value", pct(F.col("numerator"), F.col("denominator"))),
                       kpi_group="text", kpi_name="encoding_repaired_rate", topic=topic, period_grain="all",
                       unit="pct", note="texts with U+FFFD replacement characters repaired in Silver"))
    return out


def ingestion_compliance(spark, bronze_dir: str, run_ts: datetime) -> list:
    """G8: actual vs expected Bronze runs per UTC day and source (from the Bronze metadata)."""
    status = T.StructType([T.StructField("status", T.StringType()), T.StructField("records", T.LongType())])
    schema = T.StructType([T.StructField("metadata", T.StructType([
        T.StructField("window_end", T.StringType()),
        T.StructField("validation", T.StructType([T.StructField(s, status) for s in EXPECTED_RUNS_PER_DAY]))]))])
    meta = (spark.read.schema(schema).option("multiLine", "true")
            .option("pathGlobFilter", "texas_flood_sources_*.json").json(bronze_dir))
    meta = meta.select(F.to_timestamp("metadata.window_end").alias("window_end"),
                       F.to_date(F.to_timestamp("metadata.window_end")).alias("period_start"),
                       *[F.col(f"metadata.validation.{s}.status").alias(s) for s in EXPECTED_RUNS_PER_DAY]).cache()
    out = []
    now = F.lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    for source, limit in INGESTION_LAG_HOURS.items():
        last = meta.where(F.col(source) == "ok").agg(F.max("window_end").alias("last"))
        lag = F.round((F.unix_timestamp(now) - F.unix_timestamp("last")) / 3600.0, 2)
        out.append(std(last.withColumn("value", lag)
                       .withColumn("status", F.when(lag <= limit, "ok").otherwise("warn"))
                       .withColumn("note", F.concat(F.lit("last successful pull "), F.col("last").cast("string"),
                                                    F.lit(" UTC"))),
                       kpi_group="timeliness", kpi_name="ingestion_lag_hours", topic=source,
                       period_grain="all", unit="hours", threshold=float(limit)))
    runs = meta.groupBy("period_start").agg(F.count("*").alias("numerator"))
    out.append(std(runs.withColumn("denominator", F.lit(96))
                   .withColumn("value", pct(F.col("numerator"), F.lit(96))), kpi_group="timeliness",
                   kpi_name="ingestion_frequency_compliance", topic="bronze_runs", period_grain="day",
                   unit="pct", threshold=95.0,
                   note="Bronze files per day / 96 expected (*/15 schedule)"))
    for source, expected in EXPECTED_RUNS_PER_DAY.items():
        per_day = meta.groupBy("period_start").agg(F.sum((F.col(source) == "ok").cast("long")).alias("numerator"))
        out.append(std(per_day.withColumn("denominator", F.lit(expected))
                       .withColumn("value", F.least(pct(F.col("numerator"), F.lit(expected)), F.lit(100.0)))
                       .withColumn("status", F.when(F.col("numerator") >= expected * 0.95, "ok").otherwise("warn")),
                       kpi_group="timeliness", kpi_name="ingestion_frequency_compliance", topic=source,
                       period_grain="day", unit="pct", threshold=95.0,
                       note=f"successful pulls per day / {expected} expected"))
    return out


def freshness(silver: dict, run_ts: datetime) -> list:
    """G9 (US5): hours between the newest record of each topic and the Gold run (information:
    a quiet week has no new flood alert, and GHCN-Daily / Storm Events are published late)."""
    out = []
    now = F.lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    for topic, df in silver.items():
        col = EVENT_TIME[topic]
        agg = df.agg(F.max(F.col(col).cast("timestamp")).alias("mx"), F.max("processed_at_utc").alias("proc"))
        lag = (F.unix_timestamp(now) - F.unix_timestamp(F.col("mx"))) / 3600.0
        out.append(std(agg.withColumn("value", F.round(lag, 2))
                       .withColumn("note", F.concat(F.lit("newest record "), F.col("mx").cast("string"),
                                                    F.lit(" UTC; last processed "), F.col("proc").cast("string"))),
                       kpi_group="timeliness", kpi_name="record_lag_hours", topic=topic, field=col,
                       period_grain="all", unit="hours", status="info"))
    return out


def daily_completeness(silver: dict) -> list:
    """G11: share of calendar days present per GHCN-Daily station."""
    df = silver["daily_precipitation"]
    agg = df.groupBy(F.col("ghcn_station_id").alias("field")).agg(
        F.countDistinct("obs_date").alias("numerator"),
        (F.datediff(F.max("obs_date"), F.min("obs_date")) + 1).alias("denominator"))
    return [std(agg.withColumn("value", pct(F.col("numerator"), F.col("denominator")))
                .withColumn("status", F.when(F.col("value") >= 99, "ok").otherwise("warn")),
                kpi_group="completeness", kpi_name="station_day_completeness", topic="daily_precipitation",
                period_grain="all", unit="pct", threshold=99.0)]


def observation_trust(silver: dict) -> list:
    """G13: where the hourly rainfall comes from, and sensor-maintenance flags."""
    df = silver["nws_observations"]
    n = df.count()
    src = df.groupBy(F.col("precip_source").alias("field")).agg(F.count("*").alias("numerator"))
    maint = df.groupBy(F.col("station_id").alias("field")).agg(
        F.sum(F.col("maintenance_flag").cast("long")).alias("numerator"), F.count("*").alias("denominator"))
    return [std(src.withColumn("denominator", F.lit(n)).withColumn("value", pct(F.col("numerator"), F.lit(n))),
                kpi_group="accuracy", kpi_name="precip_source_share", topic="nws_observations",
                period_grain="all", unit="pct", status="info"),
            std(maint.withColumn("value", pct(F.col("numerator"), F.col("denominator"))),
                kpi_group="accuracy", kpi_name="maintenance_flag_rate", topic="nws_observations",
                period_grain="all", unit="pct", status="info",
                note="METAR reports ending in '$' (station needs maintenance)")]


# --------------------------------------------------------------------------- entry point
def build_governance(spark, silver_dir: str, bronze_dir: str, run_ts: datetime | None = None) -> DataFrame:
    run_ts = run_ts or datetime.now(timezone.utc)
    n_files = {t: len(read_topic(spark, silver_dir, t).inputFiles()) for t in TOPICS}   # files Spark read
    raw = {t: read_topic(spark, silver_dir, t).cache() for t in TOPICS}
    silver = {t: latest_version(df).cache() for t, df in raw.items()}
    parts = []
    for t in TOPICS:
        parts += null_rates(silver[t], t)
        parts += volume(raw[t], t, n_files[t])
    mc = manifest_counters(spark, silver_dir)
    if mc is not None:
        mc = mc.cache()
        parts += duplicates(mc, raw)
    parts += schema_compliance(spark, silver_dir, mc)
    parts += outliers(silver)
    parts += text_stats(silver)
    parts += ingestion_compliance(spark, bronze_dir, run_ts)
    parts += freshness(silver, run_ts)
    parts += daily_completeness(silver)
    parts += observation_trust(silver)
    # Each block is small: materialize it once (localCheckpoint) so the final union does not
    # carry, and recompute, the lineage of ~60 aggregations (it made the job 4x slower).
    parts = [part.localCheckpoint(eager=True) for part in parts]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("gold_run_ts", F.lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
