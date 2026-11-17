# Flood Monitoring and Risk - Texas (Team 2)

Data Analysis Programming, 2026-III - Universidad Distrital Francisco José de Caldas


**Team:** Oscar Durán (20212020080), Laura Pardo (20221020146), Nicolás Romero (20222020023)

## Hazard
Multi-source ingestion and analysis system for **floods in Texas (USA)**. It combines historical flood events,
historical daily rainfall, current station observations and active NWS flood alerts to produce a
**Flood Risk Index (LOW / MEDIUM / HIGH)** per county. The index is a monitoring aid, not a flood prediction.

## Sources
| Role | Source | Access |
|---|---|---|
| Historical 1 | NOAA Storm Events Database (Flood, Flash Flood, Heavy Rain) | yearly CSV.gz, https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/ |
| Historical 2 | GHCN-Daily precipitation (5 Texas stations) | NCEI Access Data Service, https://www.ncei.noaa.gov/access/services/data/v1 |
| Incremental 1 | NWS station observations (KIAH, KAUS, KSAT, KDFW, KCRP) | https://api.weather.gov/stations/{id}/observations |
| Incremental 2 | NWS active alerts for Texas | https://api.weather.gov/alerts/active?area=TX |

The URLs above are API base endpoints: opening them without parameters returns an error (for example, NCEI answers `400 Bad Request: dataset must not be null`). Example requests that work in a browser:
- GHCN-Daily, San Antonio rainfall 10-14 June 2025: https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations=USW00012921&startDate=2025-06-10&endDate=2025-06-14&dataTypes=PRCP&units=metric&format=json
- NWS active alerts for Texas: https://api.weather.gov/alerts/active?area=TX
- NWS latest observations at Houston IAH: https://api.weather.gov/stations/KIAH/observations?limit=5

All sources are U.S. Government works (public domain); attribution in `config/sources.yaml`.

## Pipeline status (Workshop 2)
| Layer | Status | Where |
|---|---|---|
| Bronze (raw JSON) | working: `bronze_flood_ingestion_dag` (every 15 min) | `datalake_bronze/texas_flood_sources_YYYYMMDD_HHMMSS.json` |
| Silver (clean Parquet) | working: `silver_flood_processing_dag` (FileSensor, every 10 min) | `datalake_silver/<topic>_YYYYMMDD_HHMMSS_from_<bronze>.parquet` |
| Gold (aggregates, Flood Risk Index) | planned, Workshop 3 (PySpark) | `datalake_gold/` |
| Dashboard | planned, Workshop 4 (Plotly Dash) | `dashboard/` |

## Run the pipeline with Docker (Workshop 2)
Requirements: Docker Desktop (Windows/macOS) or Docker Engine + Compose (Linux).
```bash
cp .env.example .env            # Windows: copy .env.example .env  -> put your e-mail in NWS_USER_AGENT
docker compose up -d --build    # first build takes a few minutes
```
1. Open http://localhost:8080 and log in with `admin` and the password in `AIRFLOW_ADMIN_PASSWORD` (default `admin`).
2. Both DAGs start unpaused. To get every source right away, trigger `bronze_flood_ingestion_dag`
   with the play button -> *Trigger DAG w/ config* -> `{"force_all": true}`.
   To load the GHCN-Daily history add `{"ghcnd_backfill_from": "2010-01-01"}`.
3. `silver_flood_processing_dag` runs every 10 minutes; its FileSensor detects the new Bronze files.
4. Explore the results with `notebooks/02_silver_exploration.ipynb`.

Stop with `docker compose down` (the data stays in the `datalake_*` folders). Logs: `airflow/logs/` or `docker compose logs -f airflow-scheduler`.

| DAG | Schedule | Tasks |
|---|---|---|
| `bronze_flood_ingestion_dag` | `*/15 * * * *` | `run_context` -> `extract_nws_alerts` (every run), `extract_nws_observations` (hourly), `scrape_storm_events_catalog` (BeautifulSoup, every run), `extract_storm_events_details` and `extract_ghcnd` (daily at 11:00 UTC) -> `validate_bronze` -> `persist_bronze` |
| `silver_flood_processing_dag` | `*/10 * * * *` | `sense_new_bronze_json` (FileSensor) -> `discover_unprocessed_bronze` -> `transform_and_persist_silver` |

Silver topics: `nws_observations`, `nws_alerts`, `daily_precipitation` (GHCN-Daily), `flood_events` (Storm Events).
Tests (no Docker needed): `pip install -r requirements.txt && pytest -q`.

## Workshop 1 scripts (source validation)
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate   |   macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python scripts/fetch_samples.py      # historical samples + first incremental poll
# wait at least 1 hour
python scripts/poll_incremental.py   # second poll
python scripts/compare_polls.py      # what changed between polls
```

## Structure
```
src/flood_monitor/     package: config, http, sources/ (API clients), preprocessing.py (Silver), text.py (NLP)
airflow/dags/          bronze_ingestion_dag.py, silver_processing_dag.py
docker-compose.yml     Airflow 2.10.2 stack (postgres, init, webserver, scheduler, triggerer)
Dockerfile             Airflow image + requirements-airflow.txt (installed with the Airflow constraints)
datalake_bronze/       raw JSON per ingestion run (Bronze)
datalake_silver/       clean Parquet per topic (Silver) + _manifest.jsonl, _registry/
datalake_gold/         Gold layer (Workshop 3)
ingest.py              CLI entry point: --mode historical | incremental | status (stub)
config/sources.yaml    one entry per source (URL, format, schedule, watermark, license)
data/raw/              raw landing zone (git-ignored)
sql/                   schema definitions (draft)
dashboard/             Plotly Dash app (Workshop 4)
tests/, tests/fixtures/ tests and recorded responses
notebooks/             01_source_profiling.ipynb, 02_silver_exploration.ipynb
scripts/               Workshop 1 sample collection and poll comparison
docs/                  ingestion contract, ER and architecture diagrams, source-status log
workshop_1/            Workshop 1 deliverables: final report PDF, mapping.md and samples/
workshop_2/            Workshop 2 deliverables: report PDF and its LaTeX source (report/)
report/                LaTeX source of the Workshop 1 report (main.tex, references.bib, figures/, generated/)
```

## Workshop 1 deliverables
| Deliverable | Location |
|---|---|
| Project Definition Report (PDF) | `workshop_1/Workshop1_Team2_Report.pdf` |
| Source profiling notebook (executed) | `notebooks/01_source_profiling.ipynb` |
| Raw samples and poll comparison | `workshop_1/samples/` |
| Canonical mapping table | `workshop_1/mapping.md` |
| First draft ER diagram | `docs/er_diagram.png`, `sql/schema_draft.sql` |
| Ingestion contract (draft) | `docs/ingestion_contract.md` |
| Source catalog | `config/sources.yaml` |

The report is written in LaTeX (`report/`). After editing it, compile `report/main.tex` and copy the resulting PDF to `workshop_1/Workshop1_Team2_Report.pdf`. Run `python scripts/make_report_snippets.py` first if the samples changed.

## Workshop 2 deliverables
| Deliverable | Location |
|---|---|
| Technical Architecture and Pipeline Report (PDF) | `workshop_2/Workshop2_Team2_Report.pdf` (LaTeX source in `workshop_2/report/`) |
| Docker stack | `docker-compose.yml`, `Dockerfile`, `requirements-airflow.txt` |
| Bronze ingestion DAG | `airflow/dags/bronze_ingestion_dag.py` |
| Silver processing DAG | `airflow/dags/silver_processing_dag.py` |
| Supporting modules | `src/flood_monitor/preprocessing.py`, `text.py`, `config.py`, `http.py`, `sources/` |
| Bronze samples (3 runs) | `datalake_bronze/*.json` |
| Silver samples | `datalake_silver/*.parquet` (+ `_manifest.jsonl`, `_registry/`) |
| Silver exploration notebook | `notebooks/02_silver_exploration.ipynb` |
| Tests | `tests/test_preprocessing.py` (recorded Bronze file in `tests/fixtures/bronze/`) |
