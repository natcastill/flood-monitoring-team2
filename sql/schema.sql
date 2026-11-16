-- Final canonical schema (Workshop 2). PostgreSQL.
-- Preserves sql/schema_draft.sql intact.

CREATE TABLE IF NOT EXISTS source (
    source_id      TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    url            TEXT NOT NULL,
    format         TEXT NOT NULL,
    role           TEXT CHECK (role IN ('historical','incremental')),
    license        TEXT NOT NULL,
    attribution    TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_run (
    run_id         BIGSERIAL PRIMARY KEY,
    source_id      TEXT REFERENCES source(source_id),
    mode           TEXT CHECK (mode IN ('historical','incremental')),
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    status         TEXT CHECK (status IN ('running','success','failed')),
    rows_in        INT DEFAULT 0,
    rows_new       INT DEFAULT 0,
    rows_updated   INT DEFAULT 0,
    rows_rejected  INT DEFAULT 0,
    error          TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_state (
    source_id      TEXT REFERENCES source(source_id),
    partition_key  TEXT NOT NULL DEFAULT '*',
    watermark      TEXT NOT NULL,
    last_success   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, partition_key)
);

CREATE TABLE IF NOT EXISTS event (
    event_id             TEXT PRIMARY KEY,
    source_id            TEXT REFERENCES source(source_id),
    event_type           TEXT NOT NULL,
    ts_utc               TIMESTAMPTZ NOT NULL,
    end_ts_utc           TIMESTAMPTZ,
    time_granularity     TEXT DEFAULT 'instant',
    latitude             DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude            DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    location_code        TEXT,
    severity_level       SMALLINT CHECK (severity_level BETWEEN 0 AND 3),
    severity_value       DOUBLE PRECISION,
    severity_unit        TEXT,
    raw_reference        TEXT NOT NULL,
    record_hash          CHAR(64) NOT NULL,
    previous_record_hash CHAR(64),
    run_id               BIGINT REFERENCES ingestion_run(run_id),
    first_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    withdrawn_at         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS event_group (
    group_id       BIGSERIAL,
    event_id       TEXT REFERENCES event(event_id),
    match_rule     TEXT,
    match_score    DOUBLE PRECISION,
    PRIMARY KEY (group_id, event_id)
);

CREATE TABLE IF NOT EXISTS rejected_record (
    rejected_id    BIGSERIAL PRIMARY KEY,
    run_id         BIGINT REFERENCES ingestion_run(run_id),
    source_id      TEXT REFERENCES source(source_id),
    reason         TEXT NOT NULL,
    payload        JSONB NOT NULL,
    rejected_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS weather_station (
    station_id        TEXT PRIMARY KEY,
    network           TEXT CHECK (network IN ('GHCN','NWS')),
    name              TEXT,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    elevation_m       DOUBLE PRECISION,
    county_fips       TEXT,
    paired_station_id TEXT
);

CREATE TABLE IF NOT EXISTS flood_event (
    event_id             TEXT PRIMARY KEY REFERENCES event(event_id),
    episode_id           TEXT,
    cz_type              CHAR(1),
    cz_fips              TEXT,
    cz_name              TEXT,
    wfo                  TEXT,
    flood_cause          TEXT,
    damage_property_usd  NUMERIC,
    damage_crops_usd     NUMERIC,
    deaths_direct        INT,
    deaths_indirect      INT,
    injuries_direct      INT,
    injuries_indirect    INT,
    event_narrative      TEXT,
    source_file          TEXT,
    source_file_compiled DATE
);

CREATE TABLE IF NOT EXISTS daily_precipitation (
    event_id    TEXT PRIMARY KEY REFERENCES event(event_id),
    station_id  TEXT REFERENCES weather_station(station_id),
    obs_date    DATE NOT NULL,
    prcp_mm     DOUBLE PRECISION CHECK (prcp_mm >= 0),
    mflag       CHAR(1),
    qflag       CHAR(1),
    sflag       CHAR(1),
    obs_time    TEXT,
    UNIQUE (station_id, obs_date)
);

CREATE TABLE IF NOT EXISTS station_observation (
    event_id         TEXT PRIMARY KEY REFERENCES event(event_id),
    station_id       TEXT REFERENCES weather_station(station_id),
    precip_1h_mm     DOUBLE PRECISION,
    precip_3h_mm     DOUBLE PRECISION,
    precip_6h_mm     DOUBLE PRECISION,
    precip_qc        TEXT,
    temperature_c    DOUBLE PRECISION,
    text_description TEXT
);

CREATE TABLE IF NOT EXISTS flood_alert (
    event_id      TEXT PRIMARY KEY REFERENCES event(event_id),
    alert_id      TEXT UNIQUE NOT NULL,
    message_type  TEXT,
    nws_event     TEXT,
    nws_severity  TEXT,
    certainty     TEXT,
    urgency       TEXT,
    sent          TIMESTAMPTZ,
    expires       TIMESTAMPTZ,
    ends          TIMESTAMPTZ,
    area_desc     TEXT,
    ugc_codes     TEXT[],
    reference_ids TEXT[],
    inactive_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_event_source_ts ON event (source_id, ts_utc);
CREATE INDEX IF NOT EXISTS idx_event_location ON event (location_code);
CREATE INDEX IF NOT EXISTS idx_daily_precip_date ON daily_precipitation (obs_date);
CREATE INDEX IF NOT EXISTS idx_flood_alert_expires ON flood_alert (expires);
