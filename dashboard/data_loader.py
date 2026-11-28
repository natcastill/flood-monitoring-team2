"""Data access layer for both dashboards (Workshop 4).

Rules (from the workshop brief):
- Both dashboards read ONLY the Gold layer Parquet files, with pd.read_parquet().
- No Silver, no raw JSON, no external API.

The Gold files are tidy ("long") tables: one row per value. Every function below is a
filter + reshape of that table for one dashboard component, so the Dash apps stay declarative.
The newest file is chosen by its timestamped name; the apps keep the file name of the page
they rendered, so every callback reads the same Gold run as the page it updates.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = REPO_ROOT / "datalake_gold"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def latest_gold_file(prefix: str) -> Path:
    """Newest governance_*.parquet or storytelling_*.parquet (timestamp in the name)."""
    files = sorted(GOLD_DIR.glob(f"{prefix}_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No {prefix}_*.parquet in {GOLD_DIR}: run the Gold DAG first.")
    return files[-1]


def load_gold(prefix: str, file_name: str | None = None) -> tuple[pd.DataFrame, pd.Timestamp, str]:
    """(table, gold_run_ts, file name). file_name=None -> newest file."""
    path = GOLD_DIR / file_name if file_name else latest_gold_file(prefix)
    df = pd.read_parquet(path)
    return df, pd.to_datetime(df["gold_run_ts"].iloc[0]), path.name


def load_governance(file_name: str | None = None):
    return load_gold("governance", file_name)


def load_storytelling(file_name: str | None = None):
    return load_gold("storytelling", file_name)


def _ratio(rows: pd.DataFrame) -> float:
    """Weighted rate (sum of numerators / sum of denominators), in percent."""
    den = rows["denominator"].sum()
    return float(rows["numerator"].sum() * 100 / den) if den else 0.0


# ---------------------------------------------------------------------------
# Governance accessors
# ---------------------------------------------------------------------------
def gov_summary_cards(df: pd.DataFrame) -> dict:
    """Numbers of the KPI cards, all computed from the Gold rows (nothing hard-coded)."""
    allg = df[df["period_grain"] == "all"]
    nulls = allg[allg["kpi_name"] == "null_rate"]
    required = nulls[nulls["threshold"].notna()]
    dup = allg[allg["kpi_name"] == "duplicate_rate_before_dedup"]
    worst = dup.loc[dup["value"].idxmax()] if len(dup) else None
    status = df["status"].value_counts()
    graded = int(status.get("ok", 0) + status.get("warn", 0) + status.get("fail", 0))
    return {
        "total_records": int(allg.loc[allg["kpi_name"] == "records_total", "value"].sum()),
        "null_rate_all": _ratio(nulls),
        "null_rate_required": _ratio(required),
        "duplicate_before": _ratio(allg[allg["kpi_name"] == "duplicate_rate_before_dedup"]),
        "duplicate_worst": (str(worst["topic"]).replace("_", " "), float(worst["value"])) if worst is not None
        else ("-", 0.0),
        "duplicate_after": _ratio(allg[allg["kpi_name"] == "duplicate_rate_after_dedup"]),
        "schema_records": _ratio(allg[allg["kpi_name"] == "schema_compliance_records"]),
        "schema_files": _ratio(allg[allg["kpi_name"] == "schema_compliance_files"]),
        "ok": int(status.get("ok", 0)), "warn": int(status.get("warn", 0)),
        "fail": int(status.get("fail", 0)), "info": int(status.get("info", 0)), "graded": graded,
    }


def gov_null_rates(df: pd.DataFrame, topic: str | None = None, top_n: int = 15) -> pd.DataFrame:
    """Fields with missing values, worst first; 'kind' tells required vs. expected-by-design."""
    d = df[(df["kpi_name"] == "null_rate") & (df["value"] > 0)].copy()
    if topic and topic != "all":
        d = d[d["topic"] == topic]
    d = d.sort_values("value", ascending=True).tail(top_n)
    d["label"] = d["topic"].str.replace("_", " ") + " · " + d["field"].astype(str)
    d["kind"] = d["threshold"].notna().map({True: "required field", False: "optional / expected"})
    d["note"] = d["note"].fillna("")
    return d


def gov_volume(df: pd.DataFrame, mode: str = "event") -> pd.DataFrame:
    """Record volume per source and period.

    mode="event"     records per year of the event (all four sources, observations/alerts
                     aggregated from day to year)
    mode="ingestion" records ingested per Bronze ingestion day
    """
    name = "records_by_event_period" if mode == "event" else "records_ingested"
    d = df[(df["kpi_name"] == name) & df["period_start"].notna()].copy()
    d["period_start"] = pd.to_datetime(d["period_start"])
    if mode == "event":
        d["period_start"] = d["period_start"].dt.to_period("Y").dt.to_timestamp()
        d = d.groupby(["topic", "period_start"], as_index=False)["value"].sum()
    return d.sort_values("period_start")


def gov_outlier_rates(df: pd.DataFrame) -> pd.DataFrame:
    d = df[(df["kpi_name"] == "outlier_rate") & df["value"].notna()].copy()
    d = d.sort_values("value", ascending=True)
    d["label"] = d["topic"].str.replace("_", " ") + " · " + d["field"].astype(str)
    return d


def gov_freshness(df: pd.DataFrame) -> pd.DataFrame:
    """Hours since the last successful pull of each source, against its contract threshold."""
    d = df[df["kpi_name"] == "ingestion_lag_hours"].copy()
    return d.sort_values("value")


def gov_status_distribution(df: pd.DataFrame) -> pd.DataFrame:
    order = ["ok", "info", "warn", "fail"]
    c = df["status"].value_counts()
    return pd.DataFrame({"status": order, "count": [int(c.get(s, 0)) for s in order]})


def gov_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Every governance KPI at period 'all' with its value, threshold and status."""
    d = df[df["period_grain"] == "all"].copy()
    d["scope"] = d["topic"].str.replace("_", " ")
    d["field"] = d["field"].fillna("")
    d["status"] = d["status"].fillna("—")
    out = d[["kpi_group", "kpi_name", "scope", "field", "value", "unit", "threshold", "status"]].copy()
    out["value"] = out["value"].round(2)
    order = {"fail": 0, "warn": 1, "ok": 2, "info": 3, "—": 4}
    out = out.assign(_o=out["status"].map(order)).sort_values(["_o", "kpi_group", "kpi_name", "scope"])
    return out.drop(columns="_o").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Storytelling accessors
# ---------------------------------------------------------------------------
def st_headline(df: pd.DataFrame) -> dict:
    d = df[df["section"] == "headline"]
    return dict(zip(d["metric"], d["value"]))


def st_narrative(df: pd.DataFrame) -> dict:
    d = df[df["section"] == "narrative"]
    return dict(zip(d["metric"], d["label"]))


def st_severity_distribution(df: pd.DataFrame) -> pd.DataFrame:
    d = df[(df["section"] == "severity_distribution") & (df["metric"] == "flood_events_by_severity")
           & (df["period_grain"] == "all")].dropna(subset=["category"])
    return d.sort_values("value")[["category", "count", "pct"]].reset_index(drop=True)


def st_trend_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """One row per month: events_count, avg_severity (0-3), damage_usd."""
    d = df[(df["section"] == "trend") & (df["period_grain"] == "month")].dropna(subset=["period_start"]).copy()
    d["period_start"] = pd.to_datetime(d["period_start"])
    ev = d[d["metric"] == "events_per_month"].rename(columns={"value": "avg_severity", "count": "events_count"})
    dm = d[d["metric"] == "damage_usd_per_month"].rename(columns={"value": "damage_usd"})
    out = ev[["period_start", "events_count", "avg_severity"]].merge(
        dm[["period_start", "damage_usd"]], on="period_start", how="outer")
    return out.sort_values("period_start").reset_index(drop=True)


def st_keywords(df: pd.DataFrame, kind: str = "top_bigrams", n: int = 15) -> pd.DataFrame:
    """Top terms with % of events mentioning them, average severity and % severe events."""
    kw = df[(df["section"] == "keywords") & (df["metric"] == kind)][["term", "count", "pct", "value", "rank"]]
    sev = df[(df["section"] == "keyword_severity") & (df["metric"] == f"{kind}_severe_share_pct")][
        ["term", "value", "label"]].rename(columns={"value": "severe_pct", "label": "damage_label"})
    d = kw.merge(sev, on="term", how="left").nsmallest(n, "rank")
    return d.sort_values("count").reset_index(drop=True)


def st_source_comparison(df: pd.DataFrame) -> pd.DataFrame:
    d = df[(df["section"] == "source_comparison")].copy()
    names = {"nws_observations": "NWS observations", "nws_alerts": "NWS flood alerts",
             "daily_precipitation": "GHCN-Daily rainfall", "flood_events": "NOAA Storm Events"}
    d["source"] = d["category"].map(names).fillna(d["category"])
    d["method"] = d["label"].str.replace(" (NCEI files)", "", regex=False)
    return d[["source", "method", "count", "value"]].reset_index(drop=True)


def st_top_counties(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    d = df[(df["section"] == "county_history") & (df["metric"] == "floods_per_year")]
    return d.nsmallest(n, "rank")[["county", "value", "count", "rank"]].sort_values("rank", ascending=False)


def st_flood_risk(df: pd.DataFrame) -> pd.DataFrame:
    """One row per county: FRI level/label and its inputs (US4)."""
    d = df[df["section"] == "flood_risk_index"]
    wide = d.pivot_table(index=["county", "station_id", "label"], columns="metric", values="value").reset_index()
    day = d[d["metric"] == "fri_score"].set_index("county")["period_start"]
    wide["rain_day"] = wide["county"].map(day)
    return wide.sort_values(["fri_score", "county"], ascending=[False, True]).reset_index(drop=True)


def st_current_rainfall(df: pd.DataFrame) -> pd.DataFrame:
    """Latest complete day per station: 1/7/30-day totals and percentiles vs. same month (US2, US3)."""
    d = df[(df["section"] == "current_rainfall") & df["period_start"].notna()]
    wide = d.pivot_table(index=["station_id", "period_start"], columns="metric", values="value").reset_index()
    return wide


def st_alerts(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(alerts in effect at the Gold run, alerts of the last 7 days by type) - US1."""
    a = df[df["section"] == "alerts"]
    active = a[a["metric"] == "active_alert"][["county", "category", "value", "label"]]
    last7 = a[a["metric"] == "alerts_last_7_days"].groupby("category", as_index=False)["count"].sum()
    return active, last7


def st_backtest(df: pd.DataFrame) -> pd.DataFrame:
    d = df[(df["section"] == "fri_backtest") & (df["county"] == "ALL")]
    return d.pivot_table(index=["term", "category"], columns="metric", values="value").reset_index()
