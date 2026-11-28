# Workshop 4 — Interactive Dashboards with Plotly Dash

| File / folder | Content |
|---|---|
| `Workshop4_Team2_Report.pdf` | Dashboard Implementation Report (APA 7, English) |
| `report/` | LaTeX source of the report (`main.tex`, `mibibliografia.bib`); it reads the images from `../screenshots/` |
| `screenshots/` | Airflow evidence (`airflow_dags_list_w4.png`, `bronze_grid_w4.png`, `silver_grid_w4.png`, `gold_grid_w4.png`, `gold_graph_w4.png`) and full-page captures of both dashboards (`governance_dashboard_w4*.png`, `storytelling_dashboard_w4*.png`) |

## Dashboards

| Dashboard | Command (repository root) | URL | Audience |
|---|---|---|---|
| Governance | `python -m dashboard.governance_app` | http://localhost:8050 | technical reviewer / data engineer |
| Storytelling | `python -m dashboard.storytelling_app` | http://localhost:8051 | emergency-management analyst |

Both read **only** the newest `datalake_gold/governance_*.parquet` / `storytelling_*.parquet` with
`pd.read_parquet()`. Setup (Windows, macOS, Linux) is in the root `README.md` (section *Quick start*).

## Integration adjustments of this workshop
A1 container file permissions on Linux hosts · A2 validator allow-list for an empty `alerts` section ·
A3 Flood Risk Index v2 (recalibrated with the back-test) · A4 typed Gold accessors (`dashboard/data_loader.py`) ·
A5 real refresh (newest Gold file on every page load, 60-second check, *Refresh data* button) and KPI cards
computed from Gold · A6 all four sources in the volume chart, Bootstrap CSS served locally, Windows instructions.
Details in Section 2 of the report.
