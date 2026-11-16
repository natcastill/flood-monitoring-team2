import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
import requests
from bs4 import BeautifulSoup
from airflow.decorators import dag, task
from flood_monitor.config import BRONZE_DIR, NWS_USER_AGENT, TEXAS_STATIONS

logger = logging.getLogger(__name__)

default_args = {
    "owner": "team2_flood_monitor",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="bronze_flood_ingestion_dag",
    description="Extracts NWS API observations/alerts and scrapes NOAA Storm Events index into Bronze JSON",
    schedule="*/30 * * * *",  # Every 30 min (aligned with Workshop 1 15-60 min cadence)
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["workshop2", "bronze", "texas-flood"],
)
def bronze_flood_ingestion():

    @task
    def extract_nws_observations() -> dict:
        """Fetch latest observations from NWS REST API for all 5 Texas paired stations."""
        headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
        observations_by_station = {}

        for station_id in TEXAS_STATIONS.keys():
            url = f"https://api.weather.gov/stations/{station_id}/observations?limit=100"
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                features = data.get("features", [])
                observations_by_station[station_id] = features
                logger.info(f"Station {station_id}: fetched {len(features)} observations.")
            except Exception as exc:
                logger.error(f"Failed fetching observations for {station_id}: {exc}")
                observations_by_station[station_id] = []

        return observations_by_station

    @task
    def extract_nws_alerts() -> list:
        """Fetch active and recent alerts for Texas from NWS REST API."""
        headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
        # Using /alerts?area=TX&limit=250 ensures rich sample records even when 0 alerts are active
        url = "https://api.weather.gov/alerts?area=TX&limit=250"
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        logger.info(f"Fetched {len(features)} raw alerts for TX.")
        return features

    @task
    def scrape_storm_events_catalog() -> list:
        """
        Web scraping task (BeautifulSoup4): Scrapes NOAA NCEI Storm Events directory
        to track yearly file versions and compile watermarks (_cYYYYMMDD) for 2010-2026.
        """
        url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        pattern = re.compile(r"StormEvents_details-ftp_v1\.0_d(201\d|202\d)_c(\d{8})\.csv\.gz")
        catalog = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = pattern.search(href)
            if match:
                catalog.append({
                    "filename": href,
                    "year": int(match.group(1)),
                    "compile_watermark": match.group(2),
                    "download_url": f"{url}{href}",
                })

        logger.info(f"Scraped {len(catalog)} Storm Events yearly details files (2010-present).")
        return catalog

    @task
    def validate_and_persist_bronze(
        observations: dict,
        alerts: list,
        storm_catalog: list,
    ) -> str:
        """Validate basic structure and persist raw JSON in datalake_bronze/ with datetime nomenclature."""
        total_obs = sum(len(v) for v in observations.values())
        if total_obs == 0 and len(alerts) == 0:
            raise ValueError("Schema validation failed: both NWS observations and alerts returned 0 records.")

        os.makedirs(BRONZE_DIR, exist_ok=True)
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"texas_flood_sources_{run_ts}.json"
        filepath = os.path.join(BRONZE_DIR, filename)

        bronze_document = {
            "metadata": {
                "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "source_owner": "NOAA / NWS",
                "scope": "Texas (FIPS 48)",
                "counts": {
                    "nws_observations": total_obs,
                    "nws_alerts": len(alerts),
                    "storm_events_files": len(storm_catalog),
                },
            },
            "nws_observations": observations,
            "nws_alerts": alerts,
            "storm_events_catalog": storm_catalog,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(bronze_document, f, ensure_ascii=False, indent=2)

        logger.info(f"Successfully persisted Bronze file: {filepath}")
        return filepath

    obs_data = extract_nws_observations()
    alerts_data = extract_nws_alerts()
    catalog_data = scrape_storm_events_catalog()
    validate_and_persist_bronze(obs_data, alerts_data, catalog_data)


bronze_flood_ingestion()