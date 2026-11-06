-- Draft schema (Workshop 1). PostgreSQL + PostGIS. Finalized in Workshop 2.
CREATE TABLE source (
    source_id      TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    url            TEXT NOT NULL,
    format         TEXT NOT NULL,
    role           TEXT CHECK (role IN ('historical','incremental')),
    license        TEXT NOT NULL,
    attribution    TEXT
);

CREATE TABLE ingestion_run (
    run_id         BIGSERIAL PRIMARY KEY,
    source_id      TEXT REFERENCES source,
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

CREATE TABLE ingestion_state (
    source_id      TEXT REFERENCES source,
    partition_key  TEXT NOT NULL DEFAULT '*',      -- year, station id, ...
    watermark      TEXT NOT NULL,                  -- timestamp, date or compile suffix
    last_success   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, partition_key)
);

CREATE TABLE event (
    event_id        TEXT PRIMARY KEY,               -- se:/gh:/obs:/al: prefix + source key
    source_id       TEXT REFERENCES source,
    event_type      TEXT NOT NULL,
    ts_utc          TIMESTAMPTZ NOT NULL,
    end_ts_utc      TIMESTAMPTZ,
    time_granularity TEXT DEFAULT 'instant',        -- instant | day
    latitude        DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude       DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    location_code   TEXT,                           -- county FIPS, UGC zone, station id
    severity_level  SMALLINT CHECK (severity_level BETWEEN 0 AND 3),
    severity_value  DOUBLE PRECISION,
    severity_unit   TEXT,
    raw_reference   TEXT NOT NULL,
    record_hash     CHAR(64) NOT NULL,
    run_id          BIGINT REFERENCES ingestion_run,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    withdrawn_at    TIMESTAMPTZ
);

CREATE TABLE event_group (
    group_id        BIGSERIAL,
    event_id        TEXT REFERENCES event,
    match_rule      TEXT,                           -- e.g. 'nearest_station_60km_day', 'alert_reference_chain'
    match_score     DOUBLE PRECISION,
    PRIMARY KEY (group_id, event_id)
);

CREATE TABLE rejected_record (
    rejected_id     BIGSERIAL PRIMARY KEY,
    run_id          BIGINT REFERENCES ingestion_run,
    source_id       TEXT REFERENCES source,
    reason          TEXT NOT NULL,
    payload         JSONB NOT NULL,
    rejected_at     TIMESTAMPTZ DEFAULT now()
);

-- Domain tables
CREATE TABLE weather_station (
    station_id        TEXT PRIMARY KEY,
    network           TEXT CHECK (network IN ('GHCN','NWS')),
    name              TEXT,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    elevation_m       DOUBLE PRECISION,
    county_fips       TEXT,
    paired_station_id TEXT
);

CREATE TABLE flood_event (
    event_id            TEXT PRIMARY KEY REFERENCES event,
    episode_id          TEXT,
    cz_type             CHAR(1),
    cz_fips             TEXT,
    cz_name             TEXT,
    wfo                 TEXT,
    flood_cause         TEXT,
    damage_property_usd NUMERIC,
    damage_crops_usd    NUMERIC,
    deaths_direct       INT, deaths_indirect INT,
    injuries_direct     INT, injuries_indirect INT,
    event_narrative     TEXT,
    source_file         TEXT,
    source_file_compiled DATE
);

CREATE TABLE daily_precipitation (
    event_id    TEXT PRIMARY KEY REFERENCES event,
    station_id  TEXT REFERENCES weather_station,
    obs_date    DATE NOT NULL,
    prcp_mm     DOUBLE PRECISION CHECK (prcp_mm >= 0),
    mflag CHAR(1), qflag CHAR(1), sflag CHAR(1), obs_time TEXT,
    UNIQUE (station_id, obs_date)
);

CREATE TABLE station_observation (
    event_id       TEXT PRIMARY KEY REFERENCES event,
    station_id     TEXT REFERENCES weather_station,
    precip_1h_mm   DOUBLE PRECISION,
    precip_3h_mm   DOUBLE PRECISION,
    precip_6h_mm   DOUBLE PRECISION,
    precip_qc      TEXT,
    temperature_c  DOUBLE PRECISION,
    text_description TEXT
);

CREATE TABLE flood_alert (
    event_id      TEXT PRIMARY KEY REFERENCES event,
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
