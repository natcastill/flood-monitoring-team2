"""Idempotent PostgreSQL storage layer for Flood Monitoring (Workshop 2)."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import ROOT, load_config

SCHEMA_PATH = ROOT / "sql" / "schema.sql"


def get_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "flood_monitor"),
        user=os.environ.get("POSTGRES_USER", "flood"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        row_factory=dict_row,
    )


def ensure_schema(conn: psycopg.Connection) -> None:
    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    with conn.transaction():
        conn.execute(sql_text)


def sync_sources(conn: psycopg.Connection, cfg: dict | None = None) -> None:
    cfg = cfg or load_config()
    with conn.transaction():
        for s in cfg["sources"]:
            conn.execute(
                """
                INSERT INTO source (source_id, name, url, format, role, license, attribution)
                VALUES (%(source_id)s, %(name)s, %(url)s, %(format)s, %(role)s, %(license)s, %(attribution)s)
                ON CONFLICT (source_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    format = EXCLUDED.format,
                    role = EXCLUDED.role,
                    license = EXCLUDED.license,
                    attribution = EXCLUDED.attribution
                """,
                {
                    "source_id": s["source_id"],
                    "name": s["name"],
                    "url": s["url"],
                    "format": s["format"],
                    "role": s["role"],
                    "license": s["license"],
                    "attribution": s.get("attribution", ""),
                },
            )


def start_run(conn: psycopg.Connection, source_id: str, mode: str) -> int:
    with conn.transaction():
        row = conn.execute(
            """
            INSERT INTO ingestion_run (source_id, mode, started_at, status)
            VALUES (%s, %s, %s, 'running')
            RETURNING run_id
            """,
            (source_id, mode, datetime.now(timezone.utc)),
        ).fetchone()
    return int(row["run_id"])


def finish_run(
    conn: psycopg.Connection,
    run_id: int,
    status: str,
    stats: dict[str, int],
    error: str | None = None,
) -> None:
    with conn.transaction():
        conn.execute(
            """
            UPDATE ingestion_run
            SET finished_at = %s,
                status = %s,
                rows_in = %s,
                rows_new = %s,
                rows_updated = %s,
                rows_rejected = %s,
                error = %s
            WHERE run_id = %s
            """,
            (
                datetime.now(timezone.utc),
                status,
                stats.get("rows_in", 0),
                stats.get("rows_new", 0),
                stats.get("rows_updated", 0),
                stats.get("rows_rejected", 0),
                error,
                run_id,
            ),
        )


def get_watermark(conn: psycopg.Connection, source_id: str, partition_key: str = "*") -> str | None:
    row = conn.execute(
        "SELECT watermark FROM ingestion_state WHERE source_id = %s AND partition_key = %s",
        (source_id, partition_key),
    ).fetchone()
    return row["watermark"] if row else None


def _upsert_event_row(conn: psycopg.Connection, ev: dict[str, Any], run_id: int) -> str:
    """Upsert into canonical event table. Returns 'new', 'updated', or 'unchanged'."""
    existing = conn.execute(
        "SELECT record_hash FROM event WHERE event_id = %s", (ev["event_id"],)
    ).fetchone()
    now = datetime.now(timezone.utc)

    if existing is None:
        conn.execute(
            """
            INSERT INTO event (
                event_id, source_id, event_type, ts_utc, end_ts_utc, time_granularity,
                latitude, longitude, location_code, severity_level, severity_value,
                severity_unit, raw_reference, record_hash, run_id, first_seen_at, last_seen_at
            ) VALUES (
                %(event_id)s, %(source_id)s, %(event_type)s, %(ts_utc)s, %(end_ts_utc)s,
                %(time_granularity)s, %(latitude)s, %(longitude)s, %(location_code)s,
                %(severity_level)s, %(severity_value)s, %(severity_unit)s,
                %(raw_reference)s, %(record_hash)s, %(run_id)s, %(now)s, %(now)s
            )
            """,
            {**ev, "run_id": run_id, "now": now},
        )
        return "new"

    if existing["record_hash"] != ev["record_hash"]:
        conn.execute(
            """
            UPDATE event SET
                event_type = %(event_type)s,
                ts_utc = %(ts_utc)s,
                end_ts_utc = %(end_ts_utc)s,
                time_granularity = %(time_granularity)s,
                latitude = %(latitude)s,
                longitude = %(longitude)s,
                location_code = %(location_code)s,
                severity_level = %(severity_level)s,
                severity_value = %(severity_value)s,
                severity_unit = %(severity_unit)s,
                raw_reference = %(raw_reference)s,
                previous_record_hash = %(prev_hash)s,
                record_hash = %(record_hash)s,
                run_id = %(run_id)s,
                last_seen_at = %(now)s,
                withdrawn_at = NULL
            WHERE event_id = %(event_id)s
            """,
            {**ev, "prev_hash": existing["record_hash"], "run_id": run_id, "now": now},
        )
        return "updated"

    conn.execute(
        "UPDATE event SET last_seen_at = %s, withdrawn_at = NULL WHERE event_id = %s",
        (now, ev["event_id"]),
    )
    return "unchanged"


def _log_rejected(
    conn: psycopg.Connection, run_id: int, source_id: str, reason: str, payload: dict[str, Any]
) -> None:
    conn.execute(
        """
        INSERT INTO rejected_record (run_id, source_id, reason, payload)
        VALUES (%s, %s, %s, %s::jsonb)
        """,
        (run_id, source_id, reason, json.dumps(payload, default=str)),
    )


def upsert_storm_partition(
    conn: psycopg.Connection,
    run_id: int,
    partition_year: str,
    watermark_compiled: str,
    normalized_items: list[tuple[dict[str, Any] | None, str | None, dict[str, Any]]],
    mark_missing_withdrawn: bool = True,
) -> dict[str, int]:
    """Atomically upsert one year partition of Storm Events and advance its watermark."""
    stats = {"rows_in": len(normalized_items), "rows_new": 0, "rows_updated": 0, "rows_rejected": 0}
    seen_ids: set[str] = set()

    with conn.transaction():
        for norm, err, raw_row in normalized_items:
            if err or norm is None:
                stats["rows_rejected"] += 1
                _log_rejected(conn, run_id, "noaa_storm_events", err or "unknown", raw_row)
                continue

            ev = norm["event"]
            fe = norm["flood_event"]
            seen_ids.add(ev["event_id"])
            outcome = _upsert_event_row(conn, ev, run_id)
            if outcome == "new":
                stats["rows_new"] += 1
            elif outcome == "updated":
                stats["rows_updated"] += 1

            conn.execute(
                """
                INSERT INTO flood_event (
                    event_id, episode_id, cz_type, cz_fips, cz_name, wfo, flood_cause,
                    damage_property_usd, damage_crops_usd, deaths_direct, deaths_indirect,
                    injuries_direct, injuries_indirect, event_narrative,
                    source_file, source_file_compiled
                ) VALUES (
                    %(event_id)s, %(episode_id)s, %(cz_type)s, %(cz_fips)s, %(cz_name)s,
                    %(wfo)s, %(flood_cause)s, %(damage_property_usd)s, %(damage_crops_usd)s,
                    %(deaths_direct)s, %(deaths_indirect)s, %(injuries_direct)s,
                    %(injuries_indirect)s, %(event_narrative)s,
                    %(source_file)s, %(source_file_compiled)s
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    episode_id = EXCLUDED.episode_id,
                    cz_type = EXCLUDED.cz_type,
                    cz_fips = EXCLUDED.cz_fips,
                    cz_name = EXCLUDED.cz_name,
                    wfo = EXCLUDED.wfo,
                    flood_cause = EXCLUDED.flood_cause,
                    damage_property_usd = EXCLUDED.damage_property_usd,
                    damage_crops_usd = EXCLUDED.damage_crops_usd,
                    deaths_direct = EXCLUDED.deaths_direct,
                    deaths_indirect = EXCLUDED.deaths_indirect,
                    injuries_direct = EXCLUDED.injuries_direct,
                    injuries_indirect = EXCLUDED.injuries_indirect,
                    event_narrative = EXCLUDED.event_narrative,
                    source_file = EXCLUDED.source_file,
                    source_file_compiled = EXCLUDED.source_file_compiled
                """,
                fe,
            )

        if mark_missing_withdrawn and seen_ids:
            conn.execute(
                """
                UPDATE event
                SET withdrawn_at = %s
                WHERE source_id = 'noaa_storm_events'
                  AND EXTRACT(YEAR FROM ts_utc) = %s
                  AND withdrawn_at IS NULL
                  AND NOT (event_id = ANY(%s))
                """,
                (datetime.now(timezone.utc), int(partition_year), list(seen_ids)),
            )

        conn.execute(
            """
            INSERT INTO ingestion_state (source_id, partition_key, watermark, last_success)
            VALUES ('noaa_storm_events', %s, %s, %s)
            ON CONFLICT (source_id, partition_key) DO UPDATE SET
                watermark = EXCLUDED.watermark,
                last_success = EXCLUDED.last_success
            """,
            (str(partition_year), watermark_compiled, datetime.now(timezone.utc)),
        )

    return stats


def upsert_ghcnd_partition(
    conn: psycopg.Connection,
    run_id: int,
    station_id: str,
    watermark_date: str,
    normalized_items: list[tuple[dict[str, Any] | None, str | None, dict[str, Any]]],
    paired_station_id: str | None = None,
) -> dict[str, int]:
    """Atomically upsert GHCN-Daily records for a station partition and advance watermark."""
    stats = {"rows_in": len(normalized_items), "rows_new": 0, "rows_updated": 0, "rows_rejected": 0}

    with conn.transaction():
        for norm, err, raw_row in normalized_items:
            if err or norm is None:
                stats["rows_rejected"] += 1
                _log_rejected(conn, run_id, "ncei_ghcnd", err or "unknown", raw_row)
                continue

            st = norm["weather_station"]
            conn.execute(
                """
                INSERT INTO weather_station (
                    station_id, network, name, latitude, longitude, elevation_m, paired_station_id
                ) VALUES (
                    %(station_id)s, %(network)s, %(name)s, %(latitude)s, %(longitude)s,
                    %(elevation_m)s, %(paired_station_id)s
                )
                ON CONFLICT (station_id) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, weather_station.name),
                    latitude = COALESCE(EXCLUDED.latitude, weather_station.latitude),
                    longitude = COALESCE(EXCLUDED.longitude, weather_station.longitude),
                    elevation_m = COALESCE(EXCLUDED.elevation_m, weather_station.elevation_m),
                    paired_station_id = COALESCE(EXCLUDED.paired_station_id, weather_station.paired_station_id)
                """,
                {**st, "paired_station_id": paired_station_id},
            )

            ev = norm["event"]
            dp = norm["daily_precipitation"]
            outcome = _upsert_event_row(conn, ev, run_id)
            if outcome == "new":
                stats["rows_new"] += 1
            elif outcome == "updated":
                stats["rows_updated"] += 1

            conn.execute(
                """
                INSERT INTO daily_precipitation (
                    event_id, station_id, obs_date, prcp_mm, mflag, qflag, sflag, obs_time
                ) VALUES (
                    %(event_id)s, %(station_id)s, %(obs_date)s, %(prcp_mm)s,
                    %(mflag)s, %(qflag)s, %(sflag)s, %(obs_time)s
                )
                ON CONFLICT (station_id, obs_date) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    prcp_mm = EXCLUDED.prcp_mm,
                    mflag = EXCLUDED.mflag,
                    qflag = EXCLUDED.qflag,
                    sflag = EXCLUDED.sflag,
                    obs_time = EXCLUDED.obs_time
                """,
                dp,
            )

        if watermark_date:
            conn.execute(
                """
                INSERT INTO ingestion_state (source_id, partition_key, watermark, last_success)
                VALUES ('ncei_ghcnd', %s, %s, %s)
                ON CONFLICT (source_id, partition_key) DO UPDATE SET
                    watermark = GREATEST(ingestion_state.watermark, EXCLUDED.watermark),
                    last_success = EXCLUDED.last_success
                """,
                (station_id, watermark_date, datetime.now(timezone.utc)),
            )

    return stats
