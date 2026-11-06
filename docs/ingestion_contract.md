# Ingestion Contract (draft) - Team 2, Flood Monitoring and Risk

All runs are recorded in `ingestion_run` (run_id, source_id, mode, started_at, finished_at, status, records_read, records_inserted, records_updated, records_rejected). The watermark of each source lives in `ingestion_state` (source_id, partition_key, watermark_value, updated_at) and is **advanced only after the run's data is committed**.

## 1. noaa_storm_events (historical, bulk CSV)
- **Historical strategy:** read the HTTPS listing, take every `StormEvents_details-ftp_v1.0_dYYYY_cYYYYMMDD.csv.gz` for 2010-present, download once to `data/raw/storm_events/` (cache), filter `STATE = TEXAS` and `EVENT_TYPE in (Flood, Flash Flood, Heavy Rain)`. Partition: **one year per unit of work**.
- **Incremental strategy:** monthly check of the listing. **Watermark = compile date `_cYYYYMMDD` per year** (`ingestion_state.partition_key = year`). When the suffix of a year changes, that year is re-loaded completely (NOAA re-compiles and may revise or add rows). Overlap window: the last 2 years are re-checked every month because recent months arrive late.
- **Duplicate key:** `EVENT_ID`. Re-load = upsert by `EVENT_ID`; rows that disappear from a re-compiled year are marked `withdrawn_at` (not deleted).

## 2. ncei_ghcnd (historical, REST JSON)
- **Historical strategy:** one request per **station x year** (`startDate`/`endDate`, `dataTypes=PRCP`, `units=metric`), 5 stations x 17 years = 85 requests, cached as raw JSON. Requests spaced (>= 1 s) to be polite to the server.
- **Incremental strategy:** daily. **Watermark = max `DATE` per station**; each run re-requests from `watermark - 30 days` to today, because recent values can be added late or corrected.
- **Duplicate key:** `(STATION, DATE)`. Upsert; a changed value updates the row and stores the previous `record_hash` in the run log.

## 3. nws_observations (incremental, REST GeoJSON)
- **Historical strategy:** not a historical source; the API keeps about 7 days. First run loads everything available.
- **Incremental strategy:** every **60 minutes**. **Watermark = max `properties.timestamp` per station**. Request `start = watermark - 3 h` (overlap catches late or corrected reports). Headers `User-Agent` (from `NWS_USER_AGENT`) and, when present, `If-Modified-Since`/`ETag` to avoid re-downloading unchanged responses.
- **Duplicate key:** `(stationId, timestamp)`. If an overlapping observation arrives with different values, it is updated (QC can change after publication).

## 4. nws_alerts (incremental, REST GeoJSON/CAP)
- **Historical strategy:** `/alerts?area=TX` with pagination (`pagination.next`) for the recent history the API keeps; long-term history comes from Storm Events.
- **Incremental strategy:** every **15 minutes** `/alerts/active?area=TX`. **Watermark = max `properties.sent`**, plus the set of active ids. New id -> insert; `messageType = Update/Cancel` -> link to earlier alerts through `references` (same `event_group`); id no longer active -> set `inactive_at` (it expired or was cancelled, it is not deleted). An empty response is valid and is logged as a successful run with 0 records.
- **Duplicate key:** `properties.id`.

## Failure in the middle of a run
1. Each run writes raw responses first (landing zone), then loads into staging, then upserts into final tables **inside one transaction per partition** (year, station-year, or poll).
2. The watermark is updated in the same transaction as the data of that partition; if the run stops, finished partitions keep their new watermark and the failed partition keeps the old one.
3. Upserts by duplicate key make re-runs **idempotent**: repeating a partition produces the same final state.
4. The run is marked `failed` in `ingestion_run` with the error; the next run resumes from the stored watermarks. Invalid records go to `rejected_record` (source, raw payload, reason) instead of stopping the run.
