"""Data access layer for both dashboards (Workshop 4).

Rules (from the workshop brief):
- Both dashboards MUST read ONLY from the Gold layer Parquet files.
- No Silver, no raw JSON, no external APIs.
- Use pandas pd.read_parquet().

The Gold files use a "long" format: one row per (section, metric, period, ...).
This module provides typed accessors so the Dash apps stay declarative.
"""
from __future__ import annotations

import glob
from pathlib import Path

import pandas as pd

# Repo root = parent of this file's parent (dashboard/ -> repo root)
REPO_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = REPO_ROOT / "datalake_gold"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def _latest_gold(prefix: str) -> Path:
    """Return the most recent governance_*.parquet or storytelling_*.parquet."""
    files = sorted(GOLD_DIR.glob(f"{prefix}_*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No {prefix}_*.parquet found in {GOLD_DIR}. "
            "Run the Gold DAG first (Workshop 3)."
        )
    return files[-1]


def load_governance() -> tuple[pd.DataFrame, pd.Timestamp]:
    """Return (df, gold_run_ts) for the latest governance Parquet."""
    path = _latest_gold("governance")
    df = pd.read_parquet(path)
    ts = pd.to_datetime(df["gold_run_ts"].iloc[0])
    return df, ts


def load_storytelling() -> tuple[pd.DataFrame, pd.Timestamp]:
    """Return (df, gold_run_ts) for the latest storytelling Parquet."""
    path = _latest_gold("storytelling")
    df = pd.read_parquet(path)
    ts = pd.to_datetime(df["gold_run_ts"].iloc[0])
    return df, ts


# ---------------------------------------------------------------------------
# Governance accessors
# ---------------------------------------------------------------------------
def gov_summary_cards(df: pd.DataFrame) -> dict:
    """High-level KPI numbers for the governance dashboard cards.

    - total_records: sum of records_total across topics
    - null_rate_avg: mean null_rate over required fields (unit == 'pct')
    - duplicate_rate_avg: mean duplicate_rate_before_dedup
    - schema_compliance_avg: mean schema_compliance_records
    """
    total = df.loc[df["kpi_name"] == "records_total", "value"].sum()

    null_rows = df[df["kpi_name"] == "null_rate"]
    null_avg = null_rows["value"].mean() if len(null_rows) else 0.0

    dup = df.loc[df["kpi_name"] == "duplicate_rate_before_dedup", "value"]
    dup_avg = dup.mean() if len(dup) else 0.0

    schema = df.loc[df["kpi_name"] == "schema_compliance_records", "value"]
    schema_avg = schema.mean() if len(schema) else 0.0

    warn = int((df["status"] == "warn").sum())
    fail = int((df["status"] == "fail").sum())
    return {
        "total_records": int(total),
        "null_rate_avg": float(null_avg),
        "duplicate_rate_avg": float(dup_avg),
        "schema_compliance_avg": float(schema_avg),
        "warn_count": warn,
        "fail_count": fail,
    }


def gov_null_rates(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    """Top-N fields by null rate (percent), with topic + status."""
    d = df[df["kpi_name"] == "null_rate"].copy()
    d = d[d["value"] > 0]  # hide perfect fields to keep the chart readable
    d = d.sort_values("value", ascending=True).tail(top_n)
    d["label"] = d["topic"].str.replace("_", " ") + " · " + d["field"].astype(str)
    return d


def gov_volume_over_time(df: pd.DataFrame) -> pd.DataFrame:
    """Records by event period (year grain) per source/topic."""
    d = df[(df["kpi_name"] == "records_by_event_period") & (df["period_grain"] == "year")].copy()
    d = d.dropna(subset=["period_start"])
    d["period_start"] = pd.to_datetime(d["period_start"])
    return d.sort_values("period_start")


def gov_outlier_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Outlier rate per field/topic."""
    d = df[df["kpi_name"] == "outlier_rate"].copy()
    d = d[d["value"] > 0].sort_values("value", ascending=True)
    d["label"] = d["topic"].str.replace("_", " ") + " · " + d["field"].astype(str)
    return d


def gov_status_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Count of KPI rows by status."""
    return df["status"].value_counts().rename_axis("status").reset_index(name="count")


def gov_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Compact table for the Data Quality Summary Table component."""
    keep = {
        "records_total": "Total records",
        "null_rate": "Null rate",
        "duplicate_rate_before_dedup": "Duplicate rate (pre-dedup)",
        "duplicate_rate_after_dedup": "Duplicate rate (post-dedup)",
        "outlier_rate": "Outlier rate",
        "schema_compliance_records": "Schema compliance",
        "records_rejected": "Rejected records",
        "ingestion_frequency_compliance": "Ingestion freq. compliance",
    }
    d = df[df["kpi_name"].isin(keep)].copy()
    d["metric"] = d["kpi_name"].map(keep)
    d["scope"] = d["topic"].str.replace("_", " ")
    out = d[["kpi_group", "metric", "scope", "field", "value", "unit", "threshold", "status"]].copy()
    out["value"] = out["value"].round(2)
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Storytelling accessors
# ---------------------------------------------------------------------------
def st_headline(df: pd.DataFrame) -> dict:
    """Headline KPI values (flood_events_total, deaths_total, ...)."""
    d = df[df["section"] == "headline"]
    return {row["metric"]: row["value"] for _, row in d.iterrows()}


def st_severity_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Overall severity split (period_grain == 'all')."""
    d = df[(df["section"] == "severity_distribution")
           & (df["metric"] == "flood_events_by_severity")
           & (df["period_grain"] == "all")].copy()
    d = d.dropna(subset=["category"])
    return d[["category", "count", "pct"]].reset_index(drop=True)


def st_trend_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly timeline with BOTH events count and average severity.

    Gold uses long format with two useful columns per metric:
    - value: avg severity (0-3) for events_per_month, or USD for damage
    - count: number of events (only for events_per_month)

    Returns one row per month with columns:
    events_count, avg_severity, damage_usd
    """
    d = df[(df["section"] == "trend") & (df["period_grain"] == "month")].copy()
    d = d.dropna(subset=["period_start"])
    d["period_start"] = pd.to_datetime(d["period_start"])

    events = d[d["metric"] == "events_per_month"].copy()
    events = events.rename(columns={"value": "avg_severity", "count": "events_count"})
    events = events[["period_start", "events_count", "avg_severity"]]

    damage = d[d["metric"] == "damage_usd_per_month"].copy()
    damage = damage.rename(columns={"value": "damage_usd"})
    damage = damage[["period_start", "damage_usd"]]

    out = events.merge(damage, on="period_start", how="outer").sort_values("period_start")
    return out.reset_index(drop=True)


def st_top_keywords(df: pd.DataFrame, n: int = 12) -> pd.DataFrame:
    """Top-N unigrams by frequency, with average severity (value column)."""
    d = df[(df["section"] == "keywords") & (df["metric"] == "top_unigrams")].copy()
    d = d.sort_values("count", ascending=True).tail(n)
    return d[["term", "count", "pct", "value"]].reset_index(drop=True)


def st_source_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Records + avg severity by source (category)."""
    d = df[(df["section"] == "source_comparison")
           & (df["metric"] == "records_and_avg_severity_by_source")].copy()
    return d[["category", "count", "value"]].reset_index(drop=True)


def st_narrative(df: pd.DataFrame) -> dict:
    """The 4-part narrative (1_where, 2_when, 3_what, 4_now)."""
    d = df[df["section"] == "narrative"]
    return {row["metric"]: {"label": row["label"], "value": row["value"]}
            for _, row in d.iterrows()}


def st_alerts_last_7d(df: pd.DataFrame) -> pd.DataFrame:
    """Alerts last 7 days grouped by category (flood warning / advisory)."""
    d = df[(df["section"] == "alerts") & (df["metric"] == "alerts_last_7_days")].copy()
    return d[["category", "count"]].reset_index(drop=True)


def severity_color(category: str) -> str:
    """Return the semantic colour for a severity category."""
    from . import theme
    return theme.SEVERITY_COLORS.get(str(category).lower(), theme.GREY)