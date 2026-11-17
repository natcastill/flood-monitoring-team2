"""Bronze ingestion DAG (TaskFlow API) - Flood Monitoring and Risk, Team 2.

Every 15 minutes (the fastest cadence in the Workshop 1 ingestion contract):
  extract_nws_alerts             NWS API, Texas alerts (recent window)        every run
  extract_nws_observations       NWS API, 5 paired stations, 3 h overlap       hourly runs (minute 00)
  scrape_storm_events_catalog    web scraping (BeautifulSoup) of the NCEI
                                 Storm Events directory -> compile watermarks  every run
  extract_storm_events_details   Texas flood rows of the yearly files whose
                                 compile watermark changed                     daily (11:00 UTC)
  extract_ghcnd                  GHCN-Daily PRCP, last 30 days                 daily (11:00 UTC)
  validate_bronze                basic schema check of every payload
  persist_bronze                 datalake_bronze/texas_flood_sources_YYYYMMDD_HHMMSS.json

The timestamp in the file name is the UTC end of the run's data interval, so
re-running the same interval overwrites the same file (idempotent).
Trigger with config {"force_all": true} to pull every source immediately, and with
{"ghcnd_backfill_from": "2010-01-01"} to load the GHCN-Daily history (station x year).
"""
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from bs4 import BeautifulSoup

from flood_monitor.config import (BRONZE_DIR, NWS_USER_AGENT, RAW_CACHE_DIR, STATE_DIR,
                                  TEXAS_STATIONS)
from flood_monitor.http import get
from flood_monitor.sources import storm_events

logger = logging.getLogger(__name__)

default_args = {
    "owner": "team2_flood_monitor",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}
NWS_HEADERS = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
STORM_EVENTS_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
NCEI_ADS_URL = "https://www.ncei.noaa.gov/access/services/data/v1"
DAILY_HOUR_UTC = 11              # GHCN-Daily and Storm Events details: once a day (06:00 Bogota)
OBS_OVERLAP = timedelta(hours=3)  # re-read window that catches late or corrected observations
STORM_STATE = Path(STATE_DIR) / "storm_events_compiled.json"


def _due(ctx: dict, hourly: bool = False, daily: bool = False) -> bool:
    end = datetime.fromisoformat(ctx["window_end"])
    if ctx["force_all"]:
        return True
    if daily:
        return end.hour == DAILY_HOUR_UTC and end.minute == 0
    if hourly:
        return end.minute == 0
    return True


@dag(
    dag_id="bronze_flood_ingestion_dag",
    description="Extracts NWS observations/alerts, scrapes the NOAA Storm Events index and pulls "
                "GHCN-Daily and Storm Events details into Bronze JSON",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "force_all": Param(False, type="boolean",
                           description="Pull every source in this run, ignoring its cadence"),
        "ghcnd_backfill_from": Param(None, type=["null", "string"],
                                     description="Optional YYYY-MM-DD: load GHCN-Daily from this date "
                                                 "(one request per station x year) instead of the last 30 days"),
    },
    tags=["workshop2", "bronze", "texas-flood"],
)
def bronze_flood_ingestion():

    @task
    def run_context() -> dict:
        """Data interval of this run; it defines the file name and the observation window."""
        c = get_current_context()
        start, end = c["data_interval_start"], c["data_interval_end"]
        if start == end:  # manual trigger
            start = end - timedelta(minutes=15)
        conf = c["dag_run"].conf or {}
        ctx = {
            "run_id": c["run_id"],
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "ts": end.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "force_all": bool(conf.get("force_all", c["params"].get("force_all", False))),
            "ghcnd_backfill_from": conf.get("ghcnd_backfill_from", c["params"].get("ghcnd_backfill_from")),
        }
        logger.info("Run context: %s", ctx)
        return ctx

    @task
    def extract_nws_observations(ctx: dict) -> dict | None:
        """Latest observations of the 5 Texas paired stations (NWS REST API)."""
        if not _due(ctx, hourly=True):
            logger.info("Observations are pulled hourly; skipped in this run.")
            return None
        start = datetime.fromisoformat(ctx["window_start"]) - timedelta(hours=1) - OBS_OVERLAP
        end = datetime.fromisoformat(ctx["window_end"])
        observations_by_station = {}
        for station_id in TEXAS_STATIONS:
            url = f"https://api.weather.gov/stations/{station_id}/observations"
            params = {"start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "limit": 500}
            try:
                resp = get(url, params=params, headers=NWS_HEADERS)
                resp.raise_for_status()
                features = [f for f in resp.json().get("features", [])
                            if f["properties"]["timestamp"] <= end.isoformat()]
                observations_by_station[station_id] = features
                logger.info("Station %s: fetched %d observations.", station_id, len(features))
            except Exception as exc:  # one failing station must not stop the others
                logger.error("Failed fetching observations for %s: %s", station_id, exc)
                observations_by_station[station_id] = []
        return observations_by_station

    @task
    def extract_nws_alerts(ctx: dict) -> list:
        """Recent Texas alerts (all events and message types) from the NWS REST API."""
        # /alerts?area=TX returns the rolling recent window (active ones included), so the
        # Silver layer can follow Update/Cancel chains; duplicates are removed in Silver.
        resp = get("https://api.weather.gov/alerts", params={"area": "TX", "limit": 250},
                   headers=NWS_HEADERS)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        logger.info("Fetched %d raw alerts for TX.", len(features))
        return features

    @task
    def scrape_storm_events_catalog() -> list:
        """
        Web scraping task (BeautifulSoup4): scrapes the NOAA NCEI Storm Events directory
        to track yearly file versions and compile watermarks (_cYYYYMMDD) for 2010-present.
        """
        resp = get(STORM_EVENTS_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        pattern = re.compile(r"StormEvents_details-ftp_v1\.0_d(20[1-9]\d)_c(\d{8})\.csv\.gz")
        catalog = []
        for link in soup.find_all("a", href=True):
            match = pattern.search(link["href"])
            if match:
                catalog.append({
                    "filename": link["href"],
                    "year": int(match.group(1)),
                    "compile_watermark": match.group(2),
                    "download_url": f"{STORM_EVENTS_URL}{link['href']}",
                })
        logger.info("Scraped %d Storm Events yearly details files (2010-present).", len(catalog))
        return catalog

    @task
    def extract_storm_events_details(catalog: list, ctx: dict) -> dict | None:
        """Texas flood rows of every yearly file whose compile watermark changed."""
        if not _due(ctx, daily=True):
            logger.info("Storm Events details are checked once a day; skipped in this run.")
            return None
        seen = json.loads(STORM_STATE.read_text(encoding="utf-8")) if STORM_STATE.exists() else {}
        changed = [f for f in catalog if seen.get(str(f["year"])) != f["compile_watermark"]]
        rows, done = [], {}
        for f in changed:
            gz = storm_events.download(f["download_url"], Path(RAW_CACHE_DIR) / "storm_events")
            df = storm_events.load_texas_floods(gz)
            df = df.astype(object).where(df.notna(), None)
            df["_source_file"] = f["filename"]
            rows.extend(df.to_dict(orient="records"))
            done[str(f["year"])] = f["compile_watermark"]
            logger.info("%s -> %d Texas flood-related rows", f["filename"], len(df))
        if not changed:
            logger.info("No Storm Events file changed since the last run.")
        return {"files": changed, "records": rows, "new_watermarks": done}

    @task
    def extract_ghcnd(ctx: dict) -> list | None:
        """GHCN-Daily precipitation for the 5 paired stations, last 30 days (late values)."""
        backfill = ctx.get("ghcnd_backfill_from")
        if not backfill and not _due(ctx, daily=True):
            logger.info("GHCN-Daily is pulled once a day; skipped in this run.")
            return None
        end = datetime.fromisoformat(ctx["window_end"]).date()
        if backfill:   # historical load: one window per year (Workshop 1 contract: station x year)
            first = datetime.fromisoformat(backfill).date()
            windows = [(max(first, first.replace(year=y, month=1, day=1)).isoformat(),
                        min(end, end.replace(year=y, month=12, day=31)).isoformat())
                       for y in range(first.year, end.year + 1)]
        else:          # daily incremental: last 30 days, because recent values arrive late
            windows = [((end - timedelta(days=30)).isoformat(), end.isoformat())]
        records = []
        for station_id, meta in TEXAS_STATIONS.items():
            for start_date, end_date in windows:
                params = {"dataset": "daily-summaries", "stations": meta["ghcn_id"], "dataTypes": "PRCP",
                          "startDate": start_date, "endDate": end_date,
                          "units": "metric", "format": "json", "includeAttributes": "true",
                          "includeStationName": "true", "includeStationLocation": "1"}
                try:
                    resp = get(NCEI_ADS_URL, params=params)
                    resp.raise_for_status()
                    rows = resp.json()
                    records.extend(rows)
                    logger.info("GHCN %s %s..%s: %d daily records", meta["ghcn_id"], start_date, end_date, len(rows))
                except Exception as exc:  # one failing request must not stop the others
                    logger.error("Failed fetching GHCN-Daily for %s %s: %s", meta["ghcn_id"], start_date, exc)
        return records

    @task
    def validate_bronze(observations, alerts, storm_catalog, storm_details, ghcnd) -> dict:
        """Basic schema check per source. Raises if nothing usable was extracted."""
        report, errors = {}, []

        def check(name, records, required):
            if records is None:
                report[name] = {"status": "not_due"}
                return
            if not isinstance(records, list):
                errors.append(f"{name}: payload is not a list")
                return
            missing = sum(1 for r in records if any(r.get(k) in (None, "") for k in required))
            report[name] = {"status": "ok", "records": len(records), "missing_required": missing,
                            "required_fields": list(required)}

        obs_props = None if observations is None else [
            f.get("properties", {}) for feats in observations.values() for f in feats]
        check("nws_observations", obs_props, ("timestamp",))
        check("nws_alerts", [f.get("properties", {}) for f in alerts], ("id", "event", "sent"))
        check("storm_events_catalog", storm_catalog, ("filename", "year", "compile_watermark"))
        check("storm_events_details", None if storm_details is None else storm_details["records"],
              ("EVENT_ID", "EVENT_TYPE", "BEGIN_DATE_TIME", "STATE"))
        check("ghcnd", ghcnd, ("DATE", "STATION"))
        if not storm_catalog:
            errors.append("storm_events_catalog: scraping returned no files (page layout changed?)")
        usable = sum(v.get("records", 0) for v in report.values())
        if usable == 0:
            errors.append("no source returned records")
        report["errors"] = errors
        logger.info("Validation report: %s", report)
        if errors:
            raise ValueError(f"Bronze schema validation failed: {errors}")
        return report

    @task
    def persist_bronze(observations, alerts, storm_catalog, storm_details, ghcnd,
                       report: dict, ctx: dict) -> str:
        """Write one raw JSON file per run (atomic write; same interval -> same file)."""
        os.makedirs(BRONZE_DIR, exist_ok=True)
        filepath = Path(BRONZE_DIR) / f"texas_flood_sources_{ctx['ts']}.json"
        bronze_document = {
            "metadata": {
                "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "run_id": ctx["run_id"],
                "window_start": ctx["window_start"],
                "window_end": ctx["window_end"],
                "source_owner": "NOAA / NWS / NCEI",
                "scope": "Texas (FIPS 48)",
                "validation": report,
            },
            "nws_observations": observations,
            "nws_alerts": alerts,
            "storm_events_catalog": storm_catalog,
            "storm_events_details": None if storm_details is None else {
                "files": storm_details["files"], "records": storm_details["records"]},
            "ghcnd": ghcnd,
        }
        tmp = filepath.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(bronze_document, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, filepath)
        # advance the Storm Events watermark only after the data is safely on disk
        if storm_details and storm_details["new_watermarks"]:
            Path(STATE_DIR).mkdir(parents=True, exist_ok=True)
            seen = json.loads(STORM_STATE.read_text(encoding="utf-8")) if STORM_STATE.exists() else {}
            seen.update(storm_details["new_watermarks"])
            STORM_STATE.write_text(json.dumps(seen, indent=2), encoding="utf-8")
        logger.info("Successfully persisted Bronze file: %s", filepath)
        return str(filepath)

    ctx = run_context()
    obs_data = extract_nws_observations(ctx)
    alerts_data = extract_nws_alerts(ctx)
    catalog_data = scrape_storm_events_catalog()
    storm_data = extract_storm_events_details(catalog_data, ctx)
    ghcnd_data = extract_ghcnd(ctx)
    report = validate_bronze(obs_data, alerts_data, catalog_data, storm_data, ghcnd_data)
    persist_bronze(obs_data, alerts_data, catalog_data, storm_data, ghcnd_data, report, ctx)


bronze_flood_ingestion()
