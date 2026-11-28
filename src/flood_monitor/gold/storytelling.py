"""Storytelling summary (PySpark): analytical aggregations for the functional-user dashboard.

Narrative: "Where and when does Texas flood, what do floods do, and is today unusual?"
Every section maps to a Workshop 1 user story (column user_story) and to one dashboard element.

Output: one tidy table, one row per value:
    section  metric  user_story  period_grain  period_start  county  station_id  category  term
    value  count  pct  rank  label  latitude  longitude  gold_run_ts
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyspark.ml import Pipeline
from pyspark.ml.feature import NGram, RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame, Window, functions as F, types as T

from flood_monitor.config import TEXAS_STATIONS
from flood_monitor.gold.spark import latest_version, read_topic
from flood_monitor.text import STOP_WORDS

COLUMNS = [
    ("section", T.StringType()), ("metric", T.StringType()), ("user_story", T.StringType()),
    ("period_grain", T.StringType()), ("period_start", T.DateType()), ("county", T.StringType()),
    ("station_id", T.StringType()), ("category", T.StringType()), ("term", T.StringType()),
    ("value", T.DoubleType()), ("count", T.LongType()), ("pct", T.DoubleType()), ("rank", T.IntegerType()),
    ("label", T.StringType()), ("latitude", T.DoubleType()), ("longitude", T.DoubleType()),
]
SEVERITY_NAMES = {0: "none", 1: "minor", 2: "moderate", 3: "severe"}


def severity_label(col: str = "severity_level"):
    return F.create_map(*[x for k, v in SEVERITY_NAMES.items() for x in (F.lit(k), F.lit(v))])[F.col(col)]
TOP_N = 25
STATION_COUNTY = {s: m["county"] for s, m in TEXAS_STATIONS.items()}


def std(df: DataFrame, **lits) -> DataFrame:
    for name, typ in COLUMNS:
        if name in lits:
            df = df.withColumn(name, F.lit(lits[name]).cast(typ))
        elif name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(typ))
        else:
            df = df.withColumn(name, F.col(name).cast(typ))
    return df.select([c for c, _ in COLUMNS])


def share(col="count"):
    return F.round(F.col(col) * 100.0 / F.sum(col).over(Window.partitionBy()), 3)


def ranked(df: DataFrame, order_col: str, n: int | None = None) -> DataFrame:
    out = df.withColumn("rank", F.row_number().over(Window.orderBy(F.col(order_col).desc(), F.col("term"))))
    return out.where(F.col("rank") <= n) if n else out


# --------------------------------------------------------------------------- flood history (Storm Events)
def headline_cards(fe: DataFrame) -> DataFrame:
    """KPI cards of the dashboard header (US6)."""
    agg = fe.agg(
        F.count("*").alias("events"),
        F.sum((F.col("event_type") == "Flash Flood").cast("long")).alias("flash"),
        F.sum("damage_property_usd").alias("damage"),
        F.sum(F.col("deaths_direct") + F.col("deaths_indirect")).alias("deaths"),
        F.countDistinct("county").alias("counties"),
        F.min("begin_utc").alias("first"), F.max("begin_utc").alias("last")).first()
    years = (agg["last"].year - agg["first"].year) + 1
    rows = [("flood_events_total", float(agg["events"]), f"{agg['first']:%Y}-{agg['last']:%Y}"),
            ("flood_events_per_year", round(agg["events"] / years, 1), f"{years} years"),
            ("flash_flood_share_pct", round(agg["flash"] * 100 / agg["events"], 1), "Flash Flood events"),
            ("property_damage_usd", float(agg["damage"] or 0), "reported property damage"),
            ("deaths_total", float(agg["deaths"]), "direct + indirect"),
            ("counties_affected", float(agg["counties"]), "of 254 Texas counties")]
    df = fe.sparkSession.createDataFrame(rows, "metric string, value double, label string")
    return std(df, section="headline", user_story="US6", period_grain="all")


def severity_distribution(fe: DataFrame, al: DataFrame) -> list:
    """Severity as the 'sentiment' of a flood record: distribution overall, per year, per source."""
    out = []
    lab = severity_label()
    overall = fe.groupBy(lab.alias("category"), F.col("severity_level").cast("double").alias("value")) \
                .agg(F.count("*").alias("count")).withColumn("pct", share())
    out.append(std(overall, section="severity_distribution", metric="flood_events_by_severity",
                   user_story="US6", period_grain="all"))
    w = Window.partitionBy("period_start")
    yearly = fe.groupBy(F.trunc(F.to_date("begin_utc"), "year").alias("period_start"), lab.alias("category"),
                        F.col("severity_level").cast("double").alias("value")).agg(F.count("*").alias("count")) \
               .withColumn("pct", F.round(F.col("count") * 100.0 / F.sum("count").over(w), 3))
    out.append(std(yearly, section="severity_distribution", metric="flood_events_by_severity",
                   user_story="US6", period_grain="year"))
    alerts = al.groupBy(F.col("nws_event").alias("category"), F.col("severity_level").cast("double").alias("value")) \
               .agg(F.count("*").alias("count")).withColumn("pct", share())
    out.append(std(alerts, section="severity_distribution", metric="alerts_by_event_type",
                   user_story="US1", period_grain="all"))
    return out


def trends(fe: DataFrame) -> list:
    """Volume and average severity per month, and seasonality (month of year)."""
    month = F.trunc(F.to_date("begin_utc"), "month")
    monthly = fe.groupBy(month.alias("period_start")).agg(
        F.count("*").alias("count"), F.round(F.avg("severity_level"), 3).alias("value"),
        F.sum("damage_property_usd").alias("damage"))
    years = fe.select(F.year("begin_utc").alias("y")).distinct().count()
    season = fe.groupBy(F.month("begin_utc").alias("m"), F.date_format("begin_utc", "MMM").alias("category")) \
               .agg(F.count("*").alias("count"), F.round(F.avg("severity_level"), 3).alias("avg_sev")) \
               .withColumn("value", F.round(F.col("count") / years, 2)).withColumn("pct", share()) \
               .withColumn("rank", F.col("m"))
    return [
        std(monthly.withColumn("label", F.lit("avg severity (0-3)")), section="trend", metric="events_per_month",
            user_story="US6", period_grain="month"),
        std(monthly.withColumn("value", F.col("damage")).drop("count"), section="trend",
            metric="damage_usd_per_month", user_story="US6", period_grain="month"),
        std(season.withColumn("label", F.concat(F.lit("avg severity "), F.col("avg_sev").cast("string"))),
            section="seasonality", metric="events_per_year_by_month", user_story="US6", period_grain="month_of_year"),
    ]


def county_history(fe: DataFrame) -> list:
    """US6: flood frequency and damage per county since 2010, ranked; county x year heat map."""
    years = fe.select(F.year("begin_utc").alias("y")).distinct().count()
    per_county = fe.groupBy("county").agg(
        F.count("*").alias("count"),
        F.sum((F.col("event_type") == "Flash Flood").cast("long")).alias("flash"),
        F.sum("damage_property_usd").alias("damage"),
        F.sum(F.col("deaths_direct") + F.col("deaths_indirect")).alias("deaths"),
        F.round(F.avg("latitude"), 4).alias("latitude"), F.round(F.avg("longitude"), 4).alias("longitude"),
        F.round(F.avg("severity_level"), 3).alias("avg_sev"))
    q75 = per_county.approxQuantile("count", [0.75], 0.0)[0]
    per_county = per_county.withColumn("rank", F.row_number().over(Window.orderBy(F.col("count").desc(), "county")))
    out = [
        std(per_county.withColumn("value", F.round(F.col("count") / years, 3))
            .withColumn("label", F.when(F.col("count") >= q75, "top quartile").otherwise("")),
            section="county_history", metric="floods_per_year", user_story="US6", period_grain="all"),
        std(per_county.withColumn("value", F.col("damage")), section="county_history",
            metric="property_damage_usd", user_story="US6", period_grain="all"),
        std(per_county.withColumn("value", F.col("deaths").cast("double")), section="county_history",
            metric="deaths", user_story="US6", period_grain="all"),
        std(per_county.withColumn("value", F.col("avg_sev")), section="county_history",
            metric="avg_severity", user_story="US6", period_grain="all"),
    ]
    top = per_county.where("rank <= 15").select("county", "rank")
    heat = fe.join(top, "county").groupBy("county", "rank", F.trunc(F.to_date("begin_utc"), "year").alias("period_start")) \
             .agg(F.count("*").alias("count")).withColumn("value", F.col("count").cast("double"))
    out.append(std(heat, section="county_history", metric="events_by_county_year", user_story="US6",
                   period_grain="year"))
    return out


def causes_and_sources(fe: DataFrame, silver: dict) -> list:
    """Category breakdown (event type, flood cause), who reports floods, and source comparison."""
    out = []
    for col, metric in (("event_type", "events_by_event_type"), ("flood_cause", "events_by_flood_cause"),
                        ("report_source", "events_by_report_source")):
        g = fe.groupBy(F.col(col).alias("category")).agg(
            F.count("*").alias("count"), F.round(F.avg("severity_level"), 3).alias("value"),
            F.sum("damage_property_usd").alias("dmg")).withColumn("pct", share()) \
            .withColumn("rank", F.row_number().over(Window.orderBy(F.col("count").desc())))
        out.append(std(g.withColumn("label", F.concat(F.lit("damage USD "), F.format_number("dmg", 0))),
                       section="category_breakdown", metric=metric, user_story="US6", period_grain="all"))
    rows = []
    for topic, df in silver.items():
        rows.append(df.select(F.lit(topic).alias("category"), F.col("source_system").alias("term"),
                              F.col("severity_level").cast("double").alias("sev")))
    allsrc = rows[0]
    for r in rows[1:]:
        allsrc = allsrc.unionByName(r)
    comp = allsrc.groupBy("category", "term").agg(F.count("*").alias("count"),
                                                  F.round(F.avg("sev"), 3).alias("value")) \
                 .withColumn("pct", share()) \
                 .withColumn("label", F.when(F.col("category") == "flood_events", "web scraping (NCEI files)")
                             .otherwise("API"))
    out.append(std(comp, section="source_comparison", metric="records_and_avg_severity_by_source",
                   user_story="US5", period_grain="all"))
    return out


def keywords(fe: DataFrame) -> list:
    """Top unigrams and bigrams with a Spark ML NLP pipeline, and the severity of each keyword.

    sentences (Silver) -> RegexTokenizer -> StopWordsRemover (English + domain list of text.py)
    -> drop tokens with digits or < 3 letters -> unigrams.
    Bigrams: NGram(n=2) on the tokenized sentence BEFORE stop-word removal, then only pairs of two
    content words are kept, so a bigram is a real phrase ("water rescue") and never joins words that
    a stop word separated ("rain ... flash") or two sentences.
    """
    out = []
    n_events = fe.count()
    sent = fe.select("event_id", "severity_level", "damage_property_usd",
                     F.explode("narrative_sentences").alias("sentence"))
    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="sentence", outputCol="raw_words", pattern="\\s+", toLowercase=True),
        StopWordsRemover(inputCol="raw_words", outputCol="words", stopWords=sorted(STOP_WORDS)),
        NGram(n=2, inputCol="raw_words", outputCol="bigrams"),
    ])
    nlp = pipeline.fit(sent).transform(sent) \
        .withColumn("words", F.expr("filter(words, w -> length(w) > 2 AND NOT w RLIKE '[0-9]')")).cache()
    stop = fe.sparkSession.createDataFrame([(w,) for w in sorted(STOP_WORDS)], "w string")
    keep = ["event_id", "severity_level", "damage_property_usd"]
    uni = nlp.select(*keep, F.explode("words").alias("term"))
    bi = nlp.select(*keep, F.explode("bigrams").alias("term")) \
        .withColumn("w1", F.split("term", " ")[0]).withColumn("w2", F.split("term", " ")[1]) \
        .where("length(w1) > 2 AND length(w2) > 2 AND w1 != w2 AND NOT term RLIKE '[0-9]'") \
        .join(F.broadcast(stop.withColumnRenamed("w", "w1")), "w1", "left_anti") \
        .join(F.broadcast(stop.withColumnRenamed("w", "w2")), "w2", "left_anti") \
        .select(*keep, "term")
    for metric, df in (("top_unigrams", uni), ("top_bigrams", bi)):
        per_event = df.dropDuplicates(["event_id", "term"])
        g = per_event.groupBy("term").agg(
            F.count("*").alias("count"),
            F.round(F.avg("severity_level"), 3).alias("value"),
            F.round(F.avg((F.col("severity_level") == 3).cast("double")) * 100, 2).alias("severe_pct"),
            F.round(F.avg("damage_property_usd"), 0).alias("avg_damage"))
        g = ranked(g.withColumn("pct", F.round(F.col("count") * 100.0 / n_events, 3)), "count", TOP_N).cache()
        out.append(std(g.withColumn("label", F.lit("pct = share of events mentioning the term; value = avg severity")),
                       section="keywords", metric=metric, user_story="US6", period_grain="all"))
        out.append(std(g.withColumn("value", F.col("severe_pct")).withColumn("pct", F.lit(None))
                       .withColumn("label", F.concat(F.lit("avg damage USD "),
                                                     F.format_number(F.coalesce("avg_damage", F.lit(0.0)), 0))),
                       section="keyword_severity", metric=f"{metric}_severe_share_pct", user_story="US6",
                       period_grain="all"))
    imp = fe.select("severity_level", "damage_property_usd", F.explode("impact_terms").alias("term")) \
            .groupBy("term").agg(F.count("*").alias("count"), F.round(F.avg("severity_level"), 3).alias("value")) \
            .withColumn("pct", F.round(F.col("count") * 100.0 / n_events, 3))
    out.append(std(ranked(imp, "count"), section="keywords", metric="impact_terms", user_story="US6",
                   period_grain="all"))
    return out


# --------------------------------------------------------------------------- current conditions
def rainfall(gh: DataFrame, obs: DataFrame, run_ts: datetime) -> tuple[list, DataFrame]:
    """US2 rainfall windows and US3 percentile against the same station and month (2010-)."""
    d = gh.select("station_id", "ghcn_station_id", "obs_date", "prcp_mm", "latitude", "longitude")
    days = F.col("obs_date").cast("timestamp").cast("long") / 86400
    w7 = Window.partitionBy("ghcn_station_id").orderBy(days).rangeBetween(-6, 0)
    w30 = Window.partitionBy("ghcn_station_id").orderBy(days).rangeBetween(-29, 0)
    d = d.withColumn("r7", F.sum("prcp_mm").over(w7)).withColumn("r30", F.sum("prcp_mm").over(w30)) \
         .withColumn("month", F.month("obs_date")).withColumn("year", F.year("obs_date"))
    last = d.withColumn("_rn", F.row_number().over(Window.partitionBy("ghcn_station_id").orderBy(F.col("obs_date").desc()))) \
            .where("_rn = 1").drop("_rn")
    hist = d.alias("h").join(last.select("ghcn_station_id", "month", "year", F.col("prcp_mm").alias("cur1"),
                                         F.col("r7").alias("cur7")).alias("c"),
                             ["ghcn_station_id", "month"]) \
            .where(F.col("h.year") < F.col("c.year"))
    pctl = hist.groupBy("ghcn_station_id").agg(
        F.round(F.avg((F.col("h.prcp_mm") <= F.col("cur1")).cast("double")) * 100, 1).alias("p24"),
        F.round(F.avg((F.col("h.r7") <= F.col("cur7")).cast("double")) * 100, 1).alias("p7"),
        F.count("*").alias("n_hist"))
    cur = last.join(pctl, "ghcn_station_id", "left")
    # NWS hourly METAR rainfall of the last 24 h (now available thanks to the W3-1 improvement)
    since = run_ts - timedelta(hours=24)
    nws24 = obs.where(F.col("ts_utc") >= F.lit(since.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")) \
               .where("is_metar_precip_report") \
               .groupBy("station_id").agg(F.round(F.sum("precip_1h_mm"), 2).alias("nws24"),
                                          F.count("*").alias("nws_hours"))
    cur = cur.join(nws24, "station_id", "left")
    out = []
    base = cur.withColumn("period_start", F.col("obs_date")).withColumn("label", F.col("ghcn_station_id"))
    for metric, col, story in (("rain_1d_mm", "prcp_mm", "US2"), ("rain_7d_mm", "r7", "US2"),
                               ("rain_30d_mm", "r30", "US2"), ("percentile_1d_vs_same_month", "p24", "US3"),
                               ("percentile_7d_vs_same_month", "p7", "US3")):
        out.append(std(base.withColumn("value", F.col(col)).withColumn("count", F.col("n_hist")),
                       section="current_rainfall", metric=metric, user_story=story, period_grain="day"))
    out.append(std(cur.withColumn("value", F.col("nws24")).withColumn("count", F.col("nws_hours"))
                   .withColumn("label", F.lit("sum of METAR hourly rainfall, last 24 h")),
                   section="current_rainfall", metric="nws_rain_24h_mm", user_story="US2", period_grain="all"))
    # climatology for context: daily rainfall percentiles per station and month
    clim = d.groupBy("station_id", "month").agg(
        *[F.percentile_approx("prcp_mm", q, 1000).alias(f"p{int(q * 100)}") for q in (0.5, 0.9, 0.95, 0.99)],
        F.round(F.avg((F.col("prcp_mm") > 0).cast("double")) * 100, 1).alias("wet_pct"), F.count("*").alias("count"))
    for p in ("p50", "p90", "p95", "p99", "wet_pct"):
        out.append(std(clim.withColumn("value", F.col(p)).withColumn("rank", F.col("month")),
                       section="rain_climatology", metric=f"daily_rain_{p}" if p != "wet_pct" else "wet_day_pct",
                       user_story="US3", period_grain="month_of_year"))
    cur = cur.withColumn("county", F.create_map(*[x for s, c in STATION_COUNTY.items()
                                                  for x in (F.lit(s), F.lit(c))])[F.col("station_id")])
    return out, cur


def alerts_now(al: DataFrame, run_ts: datetime) -> tuple[list, DataFrame]:
    """US1: alerts in effect at the Gold run, and alerts of the last 7 days."""
    now = F.lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    # An Update or Cancel message lists the alerts it replaces in reference_ids (DQ #10):
    # only the newest message of each chain can be in effect.
    superseded = al.select(F.explode("reference_ids").alias("rid")).distinct()
    live = al.where((F.col("message_type") != "Cancel") & (F.coalesce("ends_utc", "expires_utc") > now)
                    & (F.col("onset_utc") <= now)) \
             .join(superseded, F.col("alert_id") == F.col("rid"), "left_anti")
    active = live.withColumn("county", F.explode_outer("counties"))
    out = [std(active.withColumn("category", F.col("nws_event")).withColumn("value", F.col("severity_level"))
               .withColumn("term", F.col("alert_id")).withColumn("latitude", F.col("centroid_lat"))
               .withColumn("longitude", F.col("centroid_lon"))
               .withColumn("label", F.concat_ws(" | ", F.col("nws_severity"), F.col("headline"))),
               section="alerts", metric="active_alert", user_story="US1", period_grain="all")]
    last7 = al.where(F.col("sent_utc") >= now - F.expr("INTERVAL 7 DAYS"))
    by_day = last7.groupBy(F.to_date("sent_utc").alias("period_start"), F.col("nws_event").alias("category")) \
                  .agg(F.count("*").alias("count"), F.max("severity_level").cast("double").alias("value"))
    out.append(std(by_day, section="alerts", metric="alerts_last_7_days", user_story="US1", period_grain="day"))
    by_county = last7.withColumn("county", F.explode("counties")).groupBy("county") \
                     .agg(F.count("*").alias("count"), F.max("severity_level").cast("double").alias("value"))
    out.append(std(by_county, section="alerts", metric="alerts_last_7_days_by_county", user_story="US1",
                   period_grain="all"))
    level = active.groupBy("county").agg(F.max("severity_level").alias("alert_level"))
    return out, level


def flood_risk_index(cur: DataFrame, alert_level: DataFrame, county_rate: DataFrame) -> list:
    """US4: Flood Risk Index per county with its inputs.

    Workshop 4 recalibration (v2, chosen with the back-test of fri_backtest):
      HIGH    R24 >= 50 mm, or (P24 >= 95 and P7d >= 95), or alert level 3 (warning)
      MEDIUM  P24 >= 95, or R7d >= 100 mm, or alert level 2 (watch)
      LOW     otherwise
    The flood-frequency term is shown as context only: in the Workshop 1 rule it lifted every
    big county by one level, which made 9 of 10 alarms false (Workshop 3 back-test).
    The Workshop 1 score is still written (fri_score_w1) for comparison.
    """
    r24 = F.greatest(F.coalesce(F.col("nws24"), F.lit(0.0)), F.coalesce(F.col("prcp_mm"), F.lit(0.0)))
    rain_level = (F.when((F.col("p24") >= 99) | (r24 >= 75), 3)
                  .when((F.col("p24") >= 95) | (F.col("r7") >= 100), 2)
                  .when(F.col("p24") >= 90, 1).otherwise(0))
    fri = cur.join(alert_level, "county", "left").join(county_rate, "county", "left") \
             .withColumn("A", F.coalesce("alert_level", F.lit(0)).cast("int")) \
             .withColumn("R24", F.round(r24, 2)).withColumn("rain_level", rain_level) \
             .withColumn("topQ", F.coalesce("top_quartile", F.lit(False)).cast("int")) \
             .withColumn("score_w1", F.greatest("A", "rain_level") + F.col("topQ"))
    high = (F.col("R24") >= 50) | ((F.col("p24") >= 95) & (F.col("p7") >= 95)) | (F.col("A") >= 3)
    medium = (F.col("p24") >= 95) | (F.col("r7") >= 100) | (F.col("A") >= 2)
    fri = fri.withColumn("level", F.when(high, 3).when(medium, 2).otherwise(1)) \
             .withColumn("fri", F.when(F.col("level") == 3, "HIGH").when(F.col("level") == 2, "MEDIUM")
                         .otherwise("LOW"))
    base = fri.withColumn("label", F.col("fri")).withColumn("period_start", F.col("obs_date"))
    out = [std(base.withColumn("value", F.col("level")),
               section="flood_risk_index", metric="fri_score", user_story="US4", period_grain="day")]
    for metric, col in (("fri_score_w1", "score_w1"), ("input_R24_mm", "R24"), ("input_R7d_mm", "r7"),
                        ("input_P24_percentile", "p24"), ("input_P7d_percentile", "p7"),
                        ("input_alert_level", "A"), ("input_floods_per_year", "floods_per_year"),
                        ("input_top_quartile", "topQ"), ("input_rain_level", "rain_level")):
        out.append(std(base.withColumn("value", F.col(col)),
                       section="flood_risk_index", metric=metric, user_story="US4", period_grain="day"))
    return out


def fri_backtest(gh: DataFrame, fe: DataFrame, county_rate: DataFrame) -> list:
    """US4 validation: rainfall + frequency part of the index on 2010-2020 (train) and 2021- (test).

    Percentile thresholds per station and month come from the training years only. A flood day
    is a county-day with at least one Storm Events record (local date of begin_utc).
    """
    d = gh.select("station_id", "obs_date", "prcp_mm").withColumn("month", F.month("obs_date")) \
          .withColumn("year", F.year("obs_date"))
    days = F.col("obs_date").cast("timestamp").cast("long") / 86400
    d = d.withColumn("r7", F.sum("prcp_mm").over(Window.partitionBy("station_id").orderBy(days).rangeBetween(-6, 0)))
    thr = d.where("year <= 2020").groupBy("station_id", "month").agg(
        *[F.percentile_approx("prcp_mm", q, 1000).alias(n) for q, n in ((0.90, "t90"), (0.95, "t95"), (0.99, "t99"))],
        F.percentile_approx("r7", 0.95, 1000).alias("r7_95"))
    d = d.join(thr, ["station_id", "month"])
    level = (F.when((F.col("prcp_mm") > F.col("t99")) | (F.col("prcp_mm") >= 75), 3)
             .when((F.col("prcp_mm") > F.col("t95")) | (F.col("r7") >= 100), 2)
             .when(F.col("prcp_mm") > F.col("t90"), 1).otherwise(0))
    county_map = F.create_map(*[x for s, c in STATION_COUNTY.items() for x in (F.lit(s), F.lit(c))])
    d = d.withColumn("county", county_map[F.col("station_id")]).withColumn("rain_level", level) \
         .join(county_rate.select("county", F.col("top_quartile").cast("int").alias("topQ")), "county", "left") \
         .withColumn("score", F.col("rain_level") + F.coalesce("topQ", F.lit(0)))
    flood_days = fe.select("county", F.to_date(F.from_utc_timestamp("begin_utc", "America/Chicago")).alias("obs_date")) \
                   .distinct().withColumn("flood", F.lit(True))
    d = d.join(flood_days, ["county", "obs_date"], "left").withColumn("flood", F.coalesce("flood", F.lit(False))) \
         .withColumn("split", F.when(F.col("year") <= 2020, "train 2010-2020").otherwise("test 2021-")).cache()
    # Workshop 1 rule (frequency term lifts every top-quartile county by one level) against the
    # Workshop 4 recalibration (v2, see flood_risk_index). Alerts are not available for past years.
    p, r7 = F.col("prcp_mm"), F.col("r7")
    v2_high = (p >= 50) | ((p > F.col("t95")) & (r7 > F.col("r7_95")))
    rules = {"W1 rule: MEDIUM or HIGH": F.col("score") >= 2,
             "W1 rule: HIGH": F.col("score") >= 3,
             "v2 rule: MEDIUM or HIGH": (F.col("rain_level") >= 2) | v2_high,
             "v2 rule: HIGH": v2_high}
    out = []
    for rule, alarm in rules.items():
        r = d.withColumn("alarm", alarm)
        aggs = [F.sum(F.col("flood").cast("long")).alias("flood_days"),
                F.countDistinct("year").alias("years"), F.count("*").alias("county_days"),
                F.sum((F.col("flood") & F.col("alarm")).cast("long")).alias("hits"),
                F.sum(F.col("alarm").cast("long")).alias("alarms"),
                F.sum((F.col("alarm") & ~F.col("flood")).cast("long")).alias("false_alarms")]
        g = r.groupBy("split", "county").agg(*aggs) \
             .unionByName(r.groupBy("split").agg(*aggs).withColumn("county", F.lit("ALL")))
        hit = F.round(F.col("hits") * 100.0 / F.col("flood_days"), 2)
        far = F.round(F.col("false_alarms") * 100.0 / F.col("alarms"), 2)
        out += [std(g.withColumn("value", hit).withColumn("count", F.col("flood_days"))
                    .withColumn("category", F.col("split")), section="fri_backtest", metric="hit_rate_pct",
                    term=rule, user_story="US4", period_grain="all"),
                std(g.withColumn("value", far).withColumn("count", F.col("alarms"))
                    .withColumn("category", F.col("split")), section="fri_backtest",
                    metric="false_alarm_rate_pct", term=rule, user_story="US4", period_grain="all"),
                std(g.withColumn("value", F.round(F.col("alarms") * 100.0 / F.col("county_days"), 2))
                    .withColumn("count", F.col("county_days")).withColumn("category", F.col("split")),
                    section="fri_backtest", metric="alarm_days_pct", term=rule, user_story="US4",
                    period_grain="all")]
    return out


def narrative(fe: DataFrame, story_parts: DataFrame) -> DataFrame:
    """Data-driven sentences of the dashboard story, built from the computed aggregations."""
    top = story_parts.where("section = 'county_history' and metric = 'floods_per_year' and rank = 1").first()
    peak = story_parts.where("section = 'seasonality'").orderBy(F.col("count").desc()).first()
    flash = story_parts.where("section = 'headline' and metric = 'flash_flood_share_pct'").first()
    word = story_parts.where("section = 'keywords' and metric = 'top_bigrams' and rank = 1").first()
    fri = story_parts.where("section = 'flood_risk_index' and metric = 'fri_score'") \
                     .orderBy(F.col("value").desc()).first()
    lines = []
    if top:
        lines.append(("1_where", f"{top['county']} County has the most flood reports since 2010 "
                                 f"({top['count']} events, {top['value']:.1f} per year)."))
    if peak:
        lines.append(("2_when", f"{peak['category']} is the peak flood month "
                                f"({peak['value']:.1f} events per year on average)."))
    if flash and word:
        lines.append(("3_what", f"{flash['value']:.0f}% of the events are flash floods; the most frequent "
                                f"two-word phrase in the narratives is '{word['term']}'."))
    lines.append(("4_now", f"Highest current Flood Risk Index: {fri['label']} in {fri['county']} County "
                           f"(latest complete rainfall day: {fri['period_start']})." if fri
                  else "No current Flood Risk Index available."))
    df = fe.sparkSession.createDataFrame(lines, "metric string, label string")
    return std(df, section="narrative", user_story="US4/US6", period_grain="all")


# --------------------------------------------------------------------------- entry point
def build_storytelling(spark, silver_dir: str, run_ts: datetime | None = None) -> DataFrame:
    run_ts = run_ts or datetime.now(timezone.utc)
    silver = {t: latest_version(read_topic(spark, silver_dir, t)).cache()
              for t in ("nws_observations", "nws_alerts", "daily_precipitation", "flood_events")}
    fe, al, gh, obs = (silver["flood_events"], silver["nws_alerts"], silver["daily_precipitation"],
                       silver["nws_observations"])
    years = fe.select(F.year("begin_utc").alias("y")).distinct().count()
    county_counts = fe.groupBy("county").agg(F.count("*").alias("n"))
    q75 = county_counts.approxQuantile("n", [0.75], 0.0)[0]
    county_rate = county_counts.select("county", F.round(F.col("n") / years, 3).alias("floods_per_year"),
                                       (F.col("n") >= q75).alias("top_quartile")).cache()
    parts = [headline_cards(fe)]
    parts += severity_distribution(fe, al)
    parts += trends(fe)
    parts += county_history(fe)
    parts += causes_and_sources(fe, silver)
    parts += keywords(fe)
    rain_parts, cur = rainfall(gh, obs, run_ts)
    parts += rain_parts
    alert_parts, level = alerts_now(al, run_ts)
    parts += alert_parts
    parts += flood_risk_index(cur, level, county_rate)
    parts += fri_backtest(gh, fe, county_rate)
    # Each block is small: materialize it once (localCheckpoint) so the final union does not
    # carry, and recompute, the lineage of ~60 aggregations (it made the job 4x slower).
    parts = [part.localCheckpoint(eager=True) for part in parts]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    out = out.cache()
    out = out.unionByName(narrative(fe, out))
    return out.withColumn("gold_run_ts", F.lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
