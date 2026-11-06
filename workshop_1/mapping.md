# Canonical Mapping - Flood Monitoring and Risk (Team 2, Texas)

Global rules
- **Time:** every timestamp is stored in UTC (`TIMESTAMPTZ`). Storm Events local times are converted with the offset in `CZ_TIMEZONE` (e.g. `CST-6` -> +6 h). NWS offsets (`-05:00`/`-06:00`) are normalized. GHCN daily values are local-day totals: `timestamp` = local date 00:00 converted to UTC, and `time_granularity = 'day'`.
- **Coordinates:** WGS84, decimal degrees, `latitude` in [-90, 90], `longitude` in [-180, 180]; Texas checks use the bbox in `config/sources.yaml`. Missing coordinates stay NULL (never 0,0); zone/county-located records keep the code in `location_code`.
- **Units:** precipitation in **mm** (GHCN requested with `units=metric`; NWS `wmoUnit:mm`; inches x 25.4 if ever received). Damage in USD (`K`=1e3, `M`=1e6, `B`=1e9). Temperature in degC.
- **Severity:** a common 0-3 ordinal (`severity_level`) plus the native value in `severity_value` + `severity_unit`.

| severity_level | Storm Events (impact) | GHCN-Daily (PRCP mm/day) | NWS observation (precip 1 h) | NWS alert |
|---|---|---|---|---|
| 0 none | - | < 10 | < 2 mm | - |
| 1 low | Heavy Rain, no damage | 10-25 | 2-10 mm | Advisory / Statement |
| 2 medium | Flood / Flash Flood, damage < 50K USD | 25-75 | 10-25 mm | Watch |
| 3 high | damage >= 50K USD or deaths/injuries > 0 | >= 75 | >= 25 mm | Warning |

*Preliminary thresholds; calibrated against the historical distribution in Workshop 4.*

## event (canonical table)

| Canonical field | noaa_storm_events | ncei_ghcnd | nws_observations | nws_alerts |
|---|---|---|---|---|
| `event_id` | `se:` + `EVENT_ID` | `gh:` + `STATION` + `:` + `DATE` | `obs:` + `stationId` + `:` + `timestamp` | `al:` + `properties.id` |
| `source` | `noaa_storm_events` | `ncei_ghcnd` | `nws_observations` | `nws_alerts` |
| `event_type` | `EVENT_TYPE` -> `flood` / `flash_flood` / `heavy_rain` | `daily_precipitation` | `precipitation_observation` | `event` -> `flood_warning`, `flash_flood_warning`, `flood_watch`, ... |
| `timestamp` | `BEGIN_DATE_TIME` + `CZ_TIMEZONE` -> UTC | `DATE` (local day) -> UTC | `properties.timestamp` (UTC) | `properties.onset` or `effective` -> UTC |
| `latitude` | `BEGIN_LAT` (NULL if zone-located) | `LATITUDE` | `geometry.coordinates[1]` | polygon centroid, else NULL |
| `longitude` | `BEGIN_LON` | `LONGITUDE` | `geometry.coordinates[0]` | polygon centroid, else NULL |
| `severity` | derived from type + damage + casualties | `PRCP` (mm) | `precipitationLastHour.value` (mm) | `event` + `properties.severity` |
| `raw_reference` | file name + `EVENT_ID` | request URL + `STATION`/`DATE` | `@id` (observation URL) | `@id` (alert URL) |

Additional canonical columns: `end_timestamp`, `severity_level`, `severity_value`, `severity_unit`, `location_code` (FIPS, UGC zone, station id), `ingestion_run_id`, `record_hash`, `first_seen_at`, `last_seen_at`.

## Domain tables

**flood_event** (from Storm Events), 1:1 with `event`
`event_id` (FK), `episode_id`, `state_fips`, `cz_type` (C/Z), `cz_fips`, `cz_name`, `wfo`, `end_timestamp`, `end_lat`, `end_lon`, `flood_cause`, `damage_property_usd`, `damage_crops_usd`, `deaths_direct`, `deaths_indirect`, `injuries_direct`, `injuries_indirect`, `report_source`, `episode_narrative`, `event_narrative`, `source_file`, `source_file_compiled` (date from `_cYYYYMMDD`).

**weather_station** (static entity, from GHCN + NWS station metadata)
`station_id` (PK), `network` (GHCN/NWS), `name`, `latitude`, `longitude`, `elevation_m`, `county_fips`, `paired_station_id` (GHCN <-> NWS, e.g. USW00012960 <-> KIAH).

**daily_precipitation** (from GHCN-Daily), 1:1 with `event`
`event_id` (FK), `station_id` (FK), `obs_date` (local), `prcp_mm`, `mflag`, `qflag`, `sflag`, `obs_time`.

**station_observation** (from NWS observations), 1:1 with `event`
`event_id` (FK), `station_id` (FK), `precip_1h_mm`, `precip_3h_mm`, `precip_6h_mm`, `precip_qc`, `temperature_c`, `relative_humidity_pct`, `wind_speed_kmh`, `text_description`.

**flood_alert** (from NWS alerts), 1:1 with `event`
`event_id` (FK), `alert_id`, `message_type` (Alert/Update/Cancel), `status`, `nws_event`, `nws_severity`, `certainty`, `urgency`, `sent`, `effective`, `onset`, `expires`, `ends`, `area_desc`, `ugc_codes[]`, `same_codes[]`, `references[]`, `headline`, `sender_name`, `geometry` (GeoJSON, nullable).

## Source field coverage (fields not mapped are kept only in the raw landing zone)

| Source | Fields used | Fields ignored (kept raw) |
|---|---|---|
| Storm Events | IDs, dates, zone, location, type, impacts, cause, narratives | TOR_*, MAGNITUDE_TYPE, CATEGORY, BEGIN/END_RANGE/AZIMUTH |
| GHCN-Daily | DATE, STATION, PRCP, PRCP_ATTRIBUTES, NAME, LATITUDE, LONGITUDE, ELEVATION | - |
| NWS observations | timestamp, station, geometry, precipitation*, temperature, humidity, wind, textDescription | rawMessage, cloudLayers, presentWeather, visibility, heatIndex |
| NWS alerts | id, event, messageType, status, severity/certainty/urgency, times, areaDesc, geocode, references, geometry | description, instruction (kept raw) |
