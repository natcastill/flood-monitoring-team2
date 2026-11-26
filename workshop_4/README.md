# Workshop 4 — Interactive Dashboards with Plotly Dash

This folder contains the deliverables for Workshop 4 of the Data Analysis Programming course.

## Contents

| File / folder | Description |
|---|---|
| `REPORT.pdf` | Full dashboard implementation report (generated from `REPORT.md`) |
| `REPORT.md` | Source of the report (Markdown) |
| `screenshots/` | Visual evidence: Airflow DAG runs + both dashboards |
| `README.md` | This file |

## Dashboards

Two independent Plotly Dash applications:

| Dashboard | File | Port | Audience |
|---|---|---|---|
| Governance | `dashboard/governance_app.py` | 8050 | Technical reviewer / data engineer |
| Storytelling | `dashboard/storytelling_app.py` | 8051 | Functional user (analyst, journalist, risk researcher) |

Both read **exclusively** from `datalake_gold/*.parquet` via `pd.read_parquet()`, per Workshop 4 spec.

## How to run

See the "Run the dashboards" section in the repository root `README.md`.

## Evidence of pipeline integration

The three DAGs (`bronze_flood_ingestion_dag`, `silver_flood_processing_dag`,
`gold_flood_processing_dag`) were executed end-to-end inside the Docker stack
and produced three generations of Gold Parquet files:

- `governance_20261117_192000.parquet` + `storytelling_20261117_192000.parquet`
- `governance_20261126_182000.parquet` + `storytelling_20261126_182000.parquet`
- `governance_20261126_192000.parquet` + `storytelling_20261126_192000.parquet`

See `screenshots/` for the Airflow UI evidence.