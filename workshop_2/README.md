# Workshop 2 - Historical Ingestion & Canonical PostgreSQL Storage (Team 2)

Data Analysis Programming, 2026-III - Universidad Distrital Francisco Jose de Caldas

**Team:** Oscar Duran (20212020080), Laura Pardo (20221020146), Nicolas Romero (20222020023)

## Overview
Workshop 2 implements the **canonical relational storage layer** in PostgreSQL and the **idempotent historical ingestion pipeline** for Texas flood monitoring. It consumes the two historical sources defined in `config/sources.yaml` (`noaa_storm_events` and `ncei_ghcnd`), normalizes all records into a unified UTC and metric schema, and persists them transactionally without duplicates across re-runs.

## Architecture & Data Flow
- **Raw Landing Zone (`data/raw/`):** Downloaded `.csv.gz` archives and yearly station `.json` responses are cached locally (git-ignored) to avoid redundant network requests.
- **Canonical Normalization (`src/flood_monitor/normalization.py`):** Converts local timestamps (`CST-6`, `CDT-5`, `MST-7`, `MDT-6`) and GHCN observation dates to UTC (`TIMESTAMPTZ`), parses damage suffixes (`K`, `M`, `B`) into USD, converts trace precipitation (`T`) to `0.0 mm` preserving quality flags, replaces missing or `(0, 0)` coordinates with `NULL`, computes a unified ordinal `severity_level` (`0-3`), and generates a deterministic `SHA-256` `record_hash`.
- **Idempotent Storage (`src/flood_monitor/storage.py`):** Applies `sql/schema.sql` (`11` canonical tables + indexes) while keeping `sql/schema_draft.sql` intact. Wraps each partition (`year` or `station x year`) in an atomic transaction using `INSERT ... ON CONFLICT`, logs malformed rows in `rejected_record`, records runs in `ingestion_run`, and updates partition cursors in `ingestion_state`.

## Database Setup & Execution
```bash
sudo -u postgres psql -c "CREATE ROLE flood LOGIN PASSWORD 'change-me';"
sudo -u postgres psql -c "CREATE DATABASE flood_monitor OWNER flood;"
psql -h localhost -U flood -d flood_monitor -f sql/schema.sql

# Ingest 2024 and verify idempotency (second run inserts 0 rows)
python ingest.py --mode historical --start 2024-01-01 --end 2024-12-31
python ingest.py --mode historical --start 2024-01-01 --end 2024-12-31

# Full historical window (2010-present) and status check
python ingest.py --mode historical
python ingest.py --mode status
pytest -v
```

## Workshop 2 Deliverables
| Deliverable | Location |
|---|---|
| Final canonical PostgreSQL schema | `sql/schema.sql` |
| Canonical normalization module | `src/flood_monitor/normalization.py` |
| Idempotent PostgreSQL storage layer | `src/flood_monitor/storage.py` |
| CLI ingestion entry point (`historical`, `status`) | `ingest.py` |
| Unit & database idempotency tests | `tests/test_normalization.py`, `tests/test_storage.py` |
| Workshop 2 documentation | `workshop_2/README.md` |
