# Flood Monitoring and Risk - Texas (Team 2)

Data Analysis Programming, 2026-II - Universidad Distrital Francisco José de Caldas
Professor: Eng. Carlos Andrés Sierra, M.Sc.

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

All sources are U.S. Government works (public domain); attribution in `config/sources.yaml`.

## Quick start (Workshop 1)
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate   |   macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # Windows: copy .env.example .env  -> then edit NWS_USER_AGENT
python scripts/fetch_samples.py      # historical samples + first incremental poll
# wait at least 1 hour
python scripts/poll_incremental.py   # second poll
python scripts/compare_polls.py      # what changed between polls
jupyter notebook notebooks/01_source_profiling.ipynb   # Restart & Run All
```
The full Storm Events yearly file is cached in `data/raw/storm_events/` (git-ignored); it is re-downloaded only if missing.

## Structure
```
src/flood_monitor/     ingestion package (config, http, sources/, normalization, storage)
ingest.py              CLI entry point: --mode historical | incremental | status (stub)
config/sources.yaml    one entry per source (URL, format, schedule, watermark, license)
data/raw/              raw landing zone (git-ignored)
sql/                   schema definitions (draft)
dashboard/             Plotly Dash app (Workshop 4)
tests/, tests/fixtures/ tests and recorded responses
notebooks/             01_source_profiling.ipynb
scripts/               Workshop 1 sample collection and poll comparison
docs/                  ingestion contract, ER and architecture diagrams, source-status log
workshop_1/            mapping.md and samples/
report/                Workshop 1 report (LaTeX)
```
