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

## Pipeline status (Workshop 3)
| Layer | Status | Where |
|---|---|---|
| Bronze (raw JSON) | working: `bronze_flood_ingestion_dag` (every 15 min, incremental alerts, schema-drift checks) | `datalake_bronze/texas_flood_sources_YYYYMMDD_HHMMSS.json` |
| Silver (clean Parquet) | working: `silver_flood_processing_dag` (FileSensor, every 10 min), preprocessing v0.3 | `datalake_silver/<topic>_YYYYMMDD_HHMMSS_from_<bronze>.parquet` |
| Gold (governance + storytelling) | working: `gold_flood_processing_dag` (PySpark local mode, hourly at :20) | `datalake_gold/governance_YYYYMMDD_HHMMSS.parquet`, `datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet` |
| Dashboard | planned, Workshop 4 (Plotly Dash, reads only the Gold files) | `dashboard/` |

## Run the pipeline with Docker
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
4. `gold_flood_processing_dag` runs every hour at minute 20. To run it now, press the play button
   (*Trigger DAG*) once the Silver DAG has finished. It needs Java, which the Dockerfile installs since
   Workshop 3: after pulling, rebuild with `docker compose up -d --build`.
5. Explore the results with `notebooks/02_silver_exploration.ipynb` (Silver) and `notebooks/04_gold_preview.ipynb` (Gold).

After changing the preprocessing, rebuild Silver from the immutable Bronze files:
`python scripts/replay_bronze_to_silver.py --reset` (run on the host with `requirements.txt` installed).

Stop with `docker compose down` (the data stays in the `datalake_*` folders). Logs: `airflow/logs/` or `docker compose logs -f airflow-scheduler`.

| DAG | Schedule | Tasks |
|---|---|---|
| `bronze_flood_ingestion_dag` | `*/15 * * * *` | `run_context` -> `extract_nws_alerts` (every run), `extract_nws_observations` (hourly), `scrape_storm_events_catalog` (BeautifulSoup, every run), `extract_storm_events_details` and `extract_ghcnd` (daily at 11:00 UTC) -> `validate_bronze` -> `persist_bronze` |
| `silver_flood_processing_dag` | `*/10 * * * *` | `sense_new_bronze_json` (FileSensor) -> `discover_unprocessed_bronze` -> `transform_and_persist_silver` |
| `gold_flood_processing_dag` | `20 * * * *` | `check_silver_inputs` -> `build_governance_summary` (PySpark) -> `build_storytelling_summary` (PySpark) -> `validate_gold_outputs` -> `record_gold_manifest` |

Silver topics: `nws_observations`, `nws_alerts`, `daily_precipitation` (GHCN-Daily), `flood_events` (Storm Events).
Gold summaries (tidy tables, one row per value): **governance** = data-quality KPIs (`kpi_group`, `kpi_name`,
`topic`, `field`, `value`, `status`, ...); **storytelling** = analytical aggregations per dashboard section
(`section`, `metric`, `user_story`, `county`, `term`, `value`, ...). See the Workshop 3 report for every KPI and section.

Tests (no Docker needed; the Gold test also needs Java 17): `pip install -r requirements.txt && pytest -q`.

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
src/flood_monitor/     package: config, http, sources/ (API clients), preprocessing.py + silver.py (Silver),
                       text.py (NLP), gold/ (PySpark: spark.py, governance.py, storytelling.py)
airflow/dags/          bronze_ingestion_dag.py, silver_processing_dag.py, gold_processing_dag.py
docker-compose.yml     Airflow 2.10.2 stack (postgres, init, webserver, scheduler, triggerer)
Dockerfile             Airflow image + requirements-airflow.txt (installed with the Airflow constraints)
datalake_bronze/       raw JSON per ingestion run (Bronze)
datalake_silver/       clean Parquet per topic (Silver) + _manifest.jsonl, _registry/
datalake_gold/         Gold summaries (governance_*, storytelling_*) + _gold_manifest.jsonl
ingest.py              CLI entry point: --mode historical | incremental | status (stub)
config/sources.yaml    one entry per source (URL, format, schedule, watermark, license)
data/raw/              raw landing zone (git-ignored)
sql/                   schema definitions (draft)
dashboard/             Plotly Dash app (Workshop 4)
tests/, tests/fixtures/ tests and recorded responses
notebooks/             01_source_profiling, 02_silver_exploration, 03_data_quality_findings, 04_gold_preview
scripts/               sample collection (W1), report assets (W2, W3), replay_bronze_to_silver.py
docs/                  ingestion contract, ER and architecture diagrams, source-status log
workshop_1/            Workshop 1 deliverables: final report PDF, mapping.md and samples/
workshop_2/            Workshop 2 deliverables: report PDF and its LaTeX source (report/)
workshop_3/            Workshop 3 deliverables: report PDF and its LaTeX source (report/)
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

## Workshop 3 deliverables
| Deliverable | Location |
|---|---|
| Pipeline Improvements and Gold Layer Report (PDF) | `workshop_3/Workshop3_Team2_Report.pdf` (LaTeX source in `workshop_3/report/`) |
| Updated Bronze and Silver DAGs (improvements W3-1 ... W3-10) | `airflow/dags/bronze_ingestion_dag.py`, `airflow/dags/silver_processing_dag.py`, `src/flood_monitor/preprocessing.py`, `text.py`, `silver.py` |
| Gold DAG (PySpark) | `airflow/dags/gold_processing_dag.py` |
| PySpark modules | `src/flood_monitor/gold/spark.py`, `governance.py`, `storytelling.py` |
| Docker changes for PySpark | `Dockerfile` (OpenJDK 17), `requirements-airflow.txt` (pyspark), `docker-compose.yml` (`SPARK_*`) |
| Data-quality findings notebook | `notebooks/03_data_quality_findings.ipynb` |
| Gold preview notebook | `notebooks/04_gold_preview.ipynb` |
| Sample Gold files | `datalake_gold/governance_20261117_192000.parquet`, `datalake_gold/storytelling_20261117_192000.parquet` |
| Silver rebuilt with v0.3 | `datalake_silver/*.parquet` (the Workshop 2 Silver files are in the Git history) |
| Tests | `tests/test_text.py`, `tests/test_preprocessing.py`, `tests/test_gold.py` |

---

## Workshop 4 — Interactive Dashboards (Plotly Dash)

Two independent dashboards visualise the Gold layer in real time:

| Dashboard | Port | Audience |
|---|---|---|
| Governance | http://localhost:8050 | Technical reviewer / data engineer |
| Storytelling | http://localhost:8051 | Functional user (analyst) |

Both read **only** from `datalake_gold/*.parquet` (Gold layer is the single source of truth for the presentation layer).

### Run the dashboards

```bash
# One-time setup
python -m venv .venv-dashboard
.venv-dashboard/bin/pip install dash dash-bootstrap-components plotly pandas pyarrow

# Governance dashboard (terminal 1)
.venv-dashboard/bin/python -m dashboard.governance_app

# Storytelling dashboard (terminal 2)
.venv-dashboard/bin/python -m dashboard.storytelling_app

Or activate the venv first:
bash

source .venv-dashboard/bin/activate.fish   # fish
source .venv-dashboard/bin/activate        # bash/zsh
python -m dashboard.governance_app
python -m dashboard.storytelling_app

Refresh with new Gold data

The dashboards auto-select the most recent governance_*.parquet and storytelling_*.parquet on page load. To refresh after a new Gold DAG run, simply reload the browser (Ctrl+Shift+R).
Requirements

All dashboard dependencies are listed in requirements.txt. Minimum: dash, dash-bootstrap-components, plotly, pandas, pyarrow.