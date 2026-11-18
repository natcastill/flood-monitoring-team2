"""Silver preprocessing for the Bronze files written by bronze_flood_ingestion_dag.

One function per topic turns the raw payload into a clean DataFrame:
    process_nws_observations  -> nws_observations
    process_nws_alerts        -> nws_alerts
    process_ghcnd             -> daily_precipitation
    process_storm_events      -> flood_events
Steps (each justified by a Workshop 1 data-quality finding, DQ #n, or by a Workshop 3
experimentation finding, W3-n):
  type normalization, null handling (drop / impute / sentinel / flag), physical-range
  validation, outlier flags (IQR and Z-score), text preprocessing for NLP,
  de-duplication inside the batch and across batches, and schema enforcement.
Every processor leaves its data-quality counters in df.attrs["dq"]; the Silver DAG stores
them in the manifest and the Gold governance summary turns them into KPIs.

v0.3 (Workshop 3): METAR rainfall parsed from rawMessage (W3-1), sensor-maintenance flag
(W3-2), encoding repair and domain stop words (W3-3, W3-4), event narrative separated from
the repeated episode narrative (W3-5), event-duration outliers (W3-6), alert section markers
and county list (W3-7), word counts and language for the governance KPIs.
"""
import hashlib
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

from flood_monitor.config import FLOOD_ALERT_EVENTS, TEXAS_STATIONS, TX_BBOX
from flood_monitor.text import (clean, detect_language, had_encoding_problem, impact_terms, sentences,
                                 tokens, word_count)

PIPELINE_VERSION = "0.3.0"
INCH_TO_MM = 25.4


# --------------------------------------------------------------------------- helpers
def normalize_text(text: str) -> str:
    """Short categorical text: lowercase, no special punctuation, single spaces."""
    if not isinstance(text, str) or not text.strip():
        return "unknown"
    cleaned = text.lower().strip()
    cleaned = re.sub(r"[^a-z0-9\s;/\-]", "", cleaned)
    return re.sub(r"\s+", " ", cleaned)


def record_hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def iqr_flag(values: pd.Series, k: float = 3.0, min_n: int = 10) -> pd.Series:
    """True where value > Q3 + k*IQR. Needs at least min_n values."""
    v = values.dropna()
    if len(v) < min_n:
        return pd.Series(False, index=values.index)
    q1, q3 = v.quantile([0.25, 0.75])
    return (values > q3 + k * (q3 - q1)).fillna(False)


def zscore(values: pd.Series) -> pd.Series:
    sd = values.std()
    if not sd or np.isnan(sd):
        return pd.Series(np.nan, index=values.index)
    return (values - values.mean()) / sd


def compute_hourly_rain_severity(mm: float):
    """Canonical severity scale from Workshop 1 (Section 5); missing stays missing."""
    if pd.isna(mm):
        return pd.NA
    if mm < 2.0:
        return 0
    if mm < 10.0:
        return 1
    if mm < 25.0:
        return 2
    return 3


def compute_alert_severity(event_name: str) -> int:
    """Canonical severity scale for NWS alerts from Workshop 1 (Section 5)."""
    ev = (event_name or "").lower()
    if "warning" in ev:
        return 3
    if "watch" in ev:
        return 2
    if "advisory" in ev or "statement" in ev or "outlook" in ev:
        return 1
    return 0


# --------------------------------------------------------------------------- METAR parsing (W3-1, W3-2)
P_GROUP_RE = re.compile(r"\bP(\d{4})\b")


def metar_precip_mm(raw_message: str):
    """Hourly rainfall from the METAR remarks of an automated (AO2) station.

    Returns (value_mm, source). Federal Meteorological Handbook No. 1: the "Pxxxx" group
    gives the rainfall of the last hour in hundredths of an inch; an AO2 station reports it
    only when rain fell, so an hourly AO2 report without the group means 0 mm. "PNO" means
    the rain gauge is not working, so the value is unknown.
    """
    if not isinstance(raw_message, str) or not raw_message.strip():
        return np.nan, "missing"
    rmk = raw_message.split(" RMK ", 1)[1] if " RMK " in raw_message else ""
    if "PNO" in rmk.split():
        return np.nan, "metar_gauge_down"
    m = P_GROUP_RE.search(rmk)
    if m:
        return round(int(m.group(1)) / 100 * INCH_TO_MM, 2), "metar_p_group"
    if "AO2" in rmk.split():
        return 0.0, "metar_no_p_group"
    return np.nan, "missing"


# --------------------------------------------------------------------------- observations
def process_nws_observations(raw_payload: dict, source_filename: str) -> pd.DataFrame:
    """NWS station observations. DQ #6 (precipitation null in 5-min reports) and #7 (mixed intervals)."""
    records = []
    for station_id, features in (raw_payload.get("nws_observations") or {}).items():
        station_meta = TEXAS_STATIONS.get(station_id, {})
        for feat in features:
            props = feat.get("properties", {})
            coords = (feat.get("geometry") or {}).get("coordinates") or [None, None]
            val = lambda k: (props.get(k) or {}).get("value")
            raw_msg = props.get("rawMessage") or ""
            metar_mm, metar_src = metar_precip_mm(raw_msg)
            records.append({
                "event_id": f"obs:{station_id}:{props.get('timestamp')}",
                "source_system": "nws_observations",
                "station_id": station_id,
                "ghcn_station_id": station_meta.get("ghcn_id"),
                "county": station_meta.get("county"),
                "ts_utc": props.get("timestamp"),
                "latitude": coords[1] if coords[1] is not None else station_meta.get("lat"),
                "longitude": coords[0] if coords[0] is not None else station_meta.get("lon"),
                "precip_1h_mm": val("precipitationLastHour"),
                "precip_3h_mm": val("precipitationLast3Hours"),
                "precip_6h_mm": val("precipitationLast6Hours"),
                "temperature_c": val("temperature"),
                "relative_humidity_pct": val("relativeHumidity"),
                "is_metar": bool(raw_msg),
                "metar_precip_mm": metar_mm,
                "precip_source": metar_src,
                "maintenance_flag": raw_msg.rstrip().endswith("$"),
                "has_precip_key": "precipitationLastHour" in props,
                "qc_flag": (props.get("precipitationLastHour") or {}).get("qualityControl") or "MISSING",
                "text_description": normalize_text(props.get("textDescription", "")),
                "record_hash": record_hash(props),
                "bronze_source_file": source_filename,
            })
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    dq = {"rows_raw": len(df)}

    # 1. Types: UTC timestamps, float64 measurements
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_utc", "station_id"])                    # required fields
    dq["dropped_required"] = dq["rows_raw"] - len(df)
    num = ["precip_1h_mm", "precip_3h_mm", "precip_6h_mm", "temperature_c", "relative_humidity_pct",
           "latitude", "longitude"]
    df[num] = df[num].apply(pd.to_numeric, errors="coerce").astype("float64")

    # 2. Physical range: impossible values -> NULL + flag (sensor errors)
    ranges = {"precip_1h_mm": (0, 250), "precip_3h_mm": (0, 500), "precip_6h_mm": (0, 750),
              "temperature_c": (-40, 55), "relative_humidity_pct": (0, 100)}
    df["out_of_range"] = False
    for col, (lo, hi) in ranges.items():
        bad = df[col].notna() & ~df[col].between(lo, hi)
        df.loc[bad, "out_of_range"] = True
        df.loc[bad, col] = np.nan

    # 3. Nulls (DQ #6): a missing hourly total is NOT 0 mm -> keep NULL and flag it.
    #    Only METAR reports carry precipitation; 5-minute reports never do (the key is absent).
    #    W3-1: the API leaves precipitationLastHour = null in dry METAR hours, so the value is
    #    taken from the METAR "P" group (or 0 mm when an AO2 report has no "P" group).
    df["metar_precip_mm"] = pd.to_numeric(df["metar_precip_mm"], errors="coerce")
    from_api = df["precip_1h_mm"].notna()
    df.loc[from_api, "precip_source"] = "api"
    df["precip_1h_mm"] = df["precip_1h_mm"].fillna(df["metar_precip_mm"])
    df["precip_inferred_zero"] = df["precip_source"].eq("metar_no_p_group")
    df["precip_missing"] = df["precip_1h_mm"].isna()
    df["is_metar_precip_report"] = df["is_metar"] & df["precip_1h_mm"].notna()
    #    Temperature: short gaps interpolated in time (max 2 steps) inside each station + flag
    df = df.sort_values(["station_id", "ts_utc"]).reset_index(drop=True)
    before = df["temperature_c"].isna()
    df["temperature_c"] = (df.set_index("ts_utc").groupby("station_id")["temperature_c"]
                           .transform(lambda s: s.interpolate(method="time", limit=2, limit_area="inside"))
                           .to_numpy())
    df["temperature_imputed"] = before & df["temperature_c"].notna()

    # 4. Outliers: rainfall extremes are the flood signal -> flag only (IQR on wet METAR hours);
    #    temperature Z-score per station
    wet = df["precip_1h_mm"].where(df["precip_1h_mm"] > 0)
    df["is_outlier_precip"] = iqr_flag(wet, k=3.0)
    df["temperature_zscore"] = df.groupby("station_id")["temperature_c"].transform(zscore).round(3)

    # 5. Canonical severity (NULL when there is no rainfall value)
    df["severity_level"] = df["precip_1h_mm"].apply(compute_hourly_rain_severity).astype("Int8")

    # 6. Geographic validation (Texas bounding box)
    valid_geo = (df["latitude"].between(TX_BBOX["min_lat"], TX_BBOX["max_lat"])
                 & df["longitude"].between(TX_BBOX["min_lon"], TX_BBOX["max_lon"]))
    dq["dropped_outside_texas"] = int((~valid_geo).sum())
    df = df[valid_geo].copy()

    # 7. De-duplication inside the batch by the natural key (station_id, ts_utc)
    n = len(df)
    df = df.sort_values("ts_utc").drop_duplicates(subset=["station_id", "ts_utc"], keep="last")
    dq.update(batch_duplicates=n - len(df), out_of_range=int(df["out_of_range"].sum()),
              precip_key_absent=int((~df["has_precip_key"]).sum()))
    df = df.drop(columns=["metar_precip_mm", "has_precip_key"]).reset_index(drop=True)
    df.attrs["dq"] = dq
    return df


# --------------------------------------------------------------------------- alerts
def process_nws_alerts(raw_payload: dict, source_filename: str) -> pd.DataFrame:
    """NWS alerts. DQ #8 (no polygon), #9 (local offsets), #10 (Update chains), #11 (non-flood mix)."""
    records, non_flood = [], 0
    for feat in raw_payload.get("nws_alerts") or []:
        props = feat.get("properties", {})
        event_name = props.get("event", "")
        if event_name not in FLOOD_ALERT_EVENTS:                       # DQ #11
            non_flood += 1
            continue
        geom = feat.get("geometry")
        lat = lon = None
        if geom and geom.get("type") == "Polygon":                     # DQ #8: centroid, never 0,0
            ring = geom.get("coordinates", [[]])[0]
            if ring:
                lon = float(np.mean([pt[0] for pt in ring]))
                lat = float(np.mean([pt[1] for pt in ring]))
        refs = props.get("references") or []
        description = clean(props.get("description"))                 # W3-7: "* WHAT..." removed
        ugc = (props.get("geocode") or {}).get("UGC") or []
        area = props.get("areaDesc") or ""
        records.append({
            "event_id": f"al:{props.get('id')}",
            "alert_id": props.get("id"),
            "source_system": "nws_alerts",
            "nws_event": event_name,
            "message_type": props.get("messageType") or "unknown",
            "status": props.get("status") or "unknown",
            "nws_severity": props.get("severity") or "Unknown",
            "certainty": props.get("certainty") or "Unknown",
            "urgency": props.get("urgency") or "Unknown",
            "sent_utc": props.get("sent"),
            "effective_utc": props.get("effective"),
            "onset_utc": props.get("onset"),
            "expires_utc": props.get("expires"),
            "ends_utc": props.get("ends"),
            "area_desc": normalize_text(area),
            "counties": sorted({re.sub(r",\s*TX$", "", c.strip(), flags=re.I).title()
                                for c in area.split(";") if c.strip()}),
            "ugc_codes": ugc,
            "ugc_type": ("county" if ugc and all(u[2] == "C" for u in ugc)
                         else "zone" if ugc else "unknown"),
            # DQ #10 / W3-7: "@id" is a URL ("https://api.weather.gov/alerts/urn:oid:...") but the
            # alert id is the bare URN, so W2 could never match an Update/Cancel to its alert.
            "reference_ids": [r.get("identifier") or str(r.get("@id", "")).rsplit("/", 1)[-1]
                              for r in refs if isinstance(r, dict)],
            "has_geometry": geom is not None,
            "centroid_lat": lat,
            "centroid_lon": lon,
            "headline": props.get("headline") or "",
            "headline_clean": clean(props.get("headline")),
            "description_clean": description,
            "description_tokens": tokens(description),
            "description_word_count": word_count(description),
            "text_language": detect_language(description),
            "encoding_repaired": had_encoding_problem(props.get("description")),
            "impact_terms": impact_terms(description),
            "severity_level": compute_alert_severity(event_name),
            "record_hash": record_hash(props),
            "bronze_source_file": source_filename,
        })
    if not records:
        empty = pd.DataFrame()
        empty.attrs["dq"] = {"rows_raw": non_flood, "filtered_non_flood": non_flood}
        return empty
    df = pd.DataFrame(records)
    dq = {"rows_raw": len(df) + non_flood, "filtered_non_flood": non_flood}
    for c in ("sent_utc", "effective_utc", "onset_utc", "expires_utc", "ends_utc"):
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")       # DQ #9: offsets -> UTC
    # CAP semantics: no onset -> effective; no ends -> expires (imputed + flagged)
    df["onset_imputed"] = df["onset_utc"].isna() & df["effective_utc"].notna()
    df["onset_utc"] = df["onset_utc"].fillna(df["effective_utc"])
    df["ends_imputed"] = df["ends_utc"].isna() & df["expires_utc"].notna()
    df["ends_utc"] = df["ends_utc"].fillna(df["expires_utc"])
    df["severity_level"] = df["severity_level"].astype("int8")
    n = len(df)
    df = df.dropna(subset=["alert_id", "sent_utc"])
    dq["dropped_required"] = n - len(df)
    n = len(df)
    df = df.drop_duplicates(subset=["alert_id"], keep="last").reset_index(drop=True)
    dq["batch_duplicates"] = n - len(df)
    df.attrs["dq"] = dq
    return df


# --------------------------------------------------------------------------- GHCN-Daily
def process_ghcnd(raw_payload: dict, source_filename: str) -> pd.DataFrame:
    """GHCN-Daily PRCP. DQ #4 (trace flag), #5 (local-day totals), missing days."""
    raw = pd.DataFrame(raw_payload.get("ghcnd") or [])
    if raw.empty:
        return raw
    ghcn_to_nws = {m["ghcn_id"]: s for s, m in TEXAS_STATIONS.items()}
    df = pd.DataFrame({"ghcn_station_id": raw["STATION"]})
    df["record_hash"] = [record_hash(r) for r in raw.to_dict("records")]
    df["event_id"] = "gh:" + raw["STATION"] + ":" + raw["DATE"].astype(str)
    df["source_system"] = "ncei_ghcnd"
    df["station_id"] = df["ghcn_station_id"].map(ghcn_to_nws)
    df["obs_date"] = pd.to_datetime(raw["DATE"], errors="coerce").dt.date   # local day, not an instant
    df["prcp_mm"] = pd.to_numeric(raw.get("PRCP"), errors="coerce")
    attrs = raw.get("PRCP_ATTRIBUTES", pd.Series("", index=raw.index)).fillna("")
    parts = attrs.str.split(",", expand=True).reindex(columns=range(3)).fillna("")
    df["mflag"] = parts[0].str.strip().replace("", "none")
    df["qflag"] = parts[1].str.strip().replace("", "none")
    df["is_trace"] = df["mflag"].eq("T")                                    # DQ #4
    df["qc_failed"] = df["qflag"].ne("none")
    invalid = df["prcp_mm"].notna() & ~df["prcp_mm"].between(0, 1000)
    df.loc[invalid, "prcp_mm"] = np.nan
    dq = {"rows_raw": len(df), "out_of_range": int(invalid.sum())}
    df = df.dropna(subset=["prcp_mm", "obs_date"])                          # no value to analyse
    dq["dropped_required"] = dq["rows_raw"] - len(df)
    wet = df["prcp_mm"].where(df["prcp_mm"] > 0)
    df["is_outlier_iqr"] = wet.groupby(df["ghcn_station_id"]).transform(lambda s: iqr_flag(s)).astype(bool)
    z = np.log1p(wet).groupby(df["ghcn_station_id"]).transform(zscore)
    df["prcp_zscore"] = z.round(3)
    df["is_outlier_z"] = (z.abs() > 3).fillna(False)
    df["severity_level"] = pd.cut(df["prcp_mm"], [-1, 10, 25, 75, np.inf], right=False,
                                  labels=[0, 1, 2, 3]).astype("int8")
    df["latitude"] = pd.to_numeric(raw.get("LATITUDE"), errors="coerce")
    df["longitude"] = pd.to_numeric(raw.get("LONGITUDE"), errors="coerce")
    df["bronze_source_file"] = source_filename
    n = len(df)
    df = df.drop_duplicates(subset=["ghcn_station_id", "obs_date"], keep="last").reset_index(drop=True)
    dq["batch_duplicates"] = n - len(df)
    df.attrs["dq"] = dq
    return df


# --------------------------------------------------------------------------- Storm Events
def _money(v):
    if not isinstance(v, str) or not v.strip():
        return np.nan
    m = re.fullmatch(r"([\d.]+)([KMB]?)", v.strip().upper())
    return float(m.group(1)) * {"": 1, "K": 1e3, "M": 1e6, "B": 1e9}[m.group(2)] if m else np.nan


def process_storm_events(raw_payload: dict, source_filename: str) -> pd.DataFrame:
    """Storm Events details. DQ #1 (local time + zone label), #2 (damage text), #3 (no magnitude)."""
    details = raw_payload.get("storm_events_details") or {}
    raw = pd.DataFrame(details.get("records") or [])
    if raw.empty:
        return raw
    df = pd.DataFrame({"event_id": "se:" + raw["EVENT_ID"].astype(str), "storm_event_id": raw["EVENT_ID"]})
    df["record_hash"] = [record_hash(r) for r in raw.to_dict("records")]
    df["source_system"] = "noaa_storm_events"
    df["episode_id"] = raw.get("EPISODE_ID")
    df["event_type"] = raw["EVENT_TYPE"].fillna("unknown")
    offset = pd.to_numeric(raw["CZ_TIMEZONE"].str.extract(r"(-?\d+)$")[0], errors="coerce")
    for col, out in (("BEGIN_DATE_TIME", "begin_utc"), ("END_DATE_TIME", "end_utc")):   # DQ #1
        local = pd.to_datetime(raw[col], format="%d-%b-%y %H:%M:%S", errors="coerce")
        df[out] = (local - pd.to_timedelta(offset, unit="h")).dt.tz_localize("UTC")
    df["cz_type"] = raw["CZ_TYPE"].fillna("unknown")
    df["cz_fips"] = raw["CZ_FIPS"].fillna("unknown")
    df["county"] = raw["CZ_NAME"].fillna("unknown").str.title()
    df["latitude"] = pd.to_numeric(raw["BEGIN_LAT"], errors="coerce")
    df["longitude"] = pd.to_numeric(raw["BEGIN_LON"], errors="coerce")
    outside = ~(df["latitude"].between(25.0, 37.0) & df["longitude"].between(-107.5, -93.0))
    df.loc[outside, ["latitude", "longitude"]] = np.nan                    # never 0,0
    df["location_known"] = df["latitude"].notna()
    df["flood_cause"] = raw.get("FLOOD_CAUSE").fillna("unknown")
    df["damage_property_usd"] = raw["DAMAGE_PROPERTY"].map(_money)         # DQ #2
    df["damage_crops_usd"] = raw["DAMAGE_CROPS"].map(_money)
    df["damage_missing"] = df["damage_property_usd"].isna()                 # unknown, not 0
    df["is_outlier_damage"] = iqr_flag(np.log10(df["damage_property_usd"].where(df["damage_property_usd"] > 0)), k=1.5)
    # W3-6: a few events last for weeks (river floods); flag long durations instead of dropping them
    df["duration_hours"] = ((df["end_utc"] - df["begin_utc"]).dt.total_seconds() / 3600).round(2)
    bad_duration = df["duration_hours"] < 0
    df.loc[bad_duration, "duration_hours"] = np.nan
    df["is_outlier_duration"] = iqr_flag(np.log1p(df["duration_hours"]), k=3.0)
    df["report_source"] = raw.get("SOURCE", pd.Series(None, index=raw.index)).fillna("unknown")
    for c in ("DEATHS_DIRECT", "DEATHS_INDIRECT", "INJURIES_DIRECT", "INJURIES_INDIRECT"):
        df[c.lower()] = pd.to_numeric(raw[c], errors="coerce").fillna(0).astype("int32")   # mode = 0
    # W3-5: the episode narrative is repeated in every event of the episode (75% of rows), so
    # concatenating it inflated keyword counts. Keywords now come from the event narrative; the
    # episode narrative is kept in its own column, and is used only when the event one is empty.
    event_raw = raw.get("EVENT_NARRATIVE", pd.Series("", index=raw.index)).fillna("")
    episode_raw = raw.get("EPISODE_NARRATIVE", pd.Series("", index=raw.index)).fillna("")
    event_txt, episode_txt = event_raw.map(clean), episode_raw.map(clean)
    use_episode = event_txt.eq("")
    narrative = event_txt.where(~use_episode, episode_txt)
    df["narrative_clean"] = narrative
    df["narrative_sentences"] = event_raw.where(~use_episode, episode_raw).map(sentences)
    df["narrative_source"] = np.where(use_episode, "episode", "event")
    df["episode_narrative_clean"] = episode_txt
    df["narrative_tokens"] = narrative.map(tokens)
    df["narrative_word_count"] = narrative.map(word_count).astype("int32")
    df["text_language"] = narrative.map(detect_language)
    df["encoding_repaired"] = (event_raw.map(had_encoding_problem) | episode_raw.map(had_encoding_problem))
    df["impact_terms"] = narrative.map(impact_terms)
    casualties = df[["deaths_direct", "deaths_indirect", "injuries_direct", "injuries_indirect"]].sum(axis=1)
    sev = np.where(df["event_type"].eq("Heavy Rain"), 1, 2)                 # DQ #3: derived severity
    sev = np.where((df["damage_property_usd"].fillna(0) >= 50_000) | (casualties > 0), 3, sev)
    df["severity_level"] = sev.astype("int8")
    df["storm_events_file"] = raw.get("_source_file")
    df["bronze_source_file"] = source_filename
    dq = {"rows_raw": len(df), "negative_duration": int(bad_duration.sum()),
          "encoding_repaired": int(df["encoding_repaired"].sum())}
    df = df.dropna(subset=["begin_utc"])
    dq["dropped_required"] = dq["rows_raw"] - len(df)
    n = len(df)
    df = df.drop_duplicates(subset=["storm_event_id"], keep="last").reset_index(drop=True)
    dq["batch_duplicates"] = n - len(df)
    df.attrs["dq"] = dq
    return df


PROCESSORS = {"nws_observations": process_nws_observations, "nws_alerts": process_nws_alerts,
              "daily_precipitation": process_ghcnd, "flood_events": process_storm_events}


# --------------------------------------------------------------------------- schema enforcement
S, F, B, TS, L = pa.string(), pa.float64(), pa.bool_(), pa.timestamp("us", tz="UTC"), pa.list_(pa.string())
I8, I32, D = pa.int8(), pa.int32(), pa.date32()
COMMON = {"event_id": (S, True), "source_system": (S, True), "record_hash": (S, True),
          "is_update": (B, True), "bronze_source_file": (S, True), "processed_at_utc": (TS, True)}
SCHEMAS = {
    "nws_observations": {
        "station_id": (S, True), "ghcn_station_id": (S, False), "county": (S, False), "ts_utc": (TS, True),
        "latitude": (F, False), "longitude": (F, False), "precip_1h_mm": (F, False), "precip_3h_mm": (F, False),
        "precip_6h_mm": (F, False), "temperature_c": (F, False), "relative_humidity_pct": (F, False),
        "is_metar": (B, True), "is_metar_precip_report": (B, True), "precip_missing": (B, True),
        "precip_source": (S, True), "precip_inferred_zero": (B, True), "maintenance_flag": (B, True),
        "temperature_imputed": (B, True), "out_of_range": (B, True), "is_outlier_precip": (B, True),
        "temperature_zscore": (F, False), "qc_flag": (S, True), "text_description": (S, True),
        "severity_level": (I8, False)},
    "nws_alerts": {
        "alert_id": (S, True), "nws_event": (S, True), "message_type": (S, True), "status": (S, True),
        "nws_severity": (S, True), "certainty": (S, True), "urgency": (S, True), "sent_utc": (TS, True),
        "effective_utc": (TS, False), "onset_utc": (TS, False), "expires_utc": (TS, False),
        "ends_utc": (TS, False), "onset_imputed": (B, True), "ends_imputed": (B, True),
        "area_desc": (S, True), "counties": (L, False), "ugc_codes": (L, False), "ugc_type": (S, True),
        "reference_ids": (L, False),
        "has_geometry": (B, True), "centroid_lat": (F, False), "centroid_lon": (F, False),
        "headline": (S, False), "headline_clean": (S, False), "description_clean": (S, False), "description_tokens": (L, False),
        "description_word_count": (I32, True), "text_language": (S, True), "encoding_repaired": (B, True),
        "impact_terms": (L, False), "severity_level": (I8, True)},
    "daily_precipitation": {
        "station_id": (S, False), "ghcn_station_id": (S, True), "obs_date": (D, True), "prcp_mm": (F, True),
        "mflag": (S, True), "qflag": (S, True), "is_trace": (B, True), "qc_failed": (B, True),
        "is_outlier_iqr": (B, True), "prcp_zscore": (F, False), "is_outlier_z": (B, True),
        "latitude": (F, False), "longitude": (F, False), "severity_level": (I8, True)},
    "flood_events": {
        "storm_event_id": (S, True), "episode_id": (S, False), "event_type": (S, True),
        "begin_utc": (TS, True), "end_utc": (TS, False), "duration_hours": (F, False),
        "is_outlier_duration": (B, True), "cz_type": (S, True), "cz_fips": (S, True),
        "county": (S, True), "latitude": (F, False), "longitude": (F, False), "location_known": (B, True),
        "flood_cause": (S, True), "report_source": (S, True), "damage_property_usd": (F, False),
        "damage_crops_usd": (F, False),
        "damage_missing": (B, True), "is_outlier_damage": (B, True), "deaths_direct": (I32, True),
        "deaths_indirect": (I32, True), "injuries_direct": (I32, True), "injuries_indirect": (I32, True),
        "narrative_clean": (S, False), "narrative_source": (S, True), "episode_narrative_clean": (S, False),
        "narrative_sentences": (L, False), "narrative_tokens": (L, False), "narrative_word_count": (I32, True), "text_language": (S, True),
        "encoding_repaired": (B, True), "impact_terms": (L, False), "severity_level": (I8, True),
        "storm_events_file": (S, False)},
}


def enforce_schema(topic: str, df: pd.DataFrame) -> pa.Table:
    """Fixed column order and Arrow types; rows missing a required field are dropped."""
    spec = {**SCHEMAS[topic], **COMMON}
    required = [c for c, (_, req) in spec.items() if req]
    df = df.dropna(subset=[c for c in required if c in df.columns])
    missing = [c for c in spec if c not in df.columns]
    if missing:
        raise ValueError(f"{topic}: missing Silver columns {missing}")
    arrays = []
    for col, (typ, _) in spec.items():
        s = df[col]
        if pa.types.is_timestamp(typ):
            arrays.append(pa.array(pd.to_datetime(s, utc=True), type=typ, from_pandas=True))
            continue
        if pa.types.is_list(typ):
            s = s.map(lambda v: list(v) if isinstance(v, (list, tuple, np.ndarray)) else [])
        elif typ == S:
            s = s.astype(object).where(s.notna(), None).map(lambda v: None if v is None else str(v))
        else:
            s = s.astype(object).where(s.notna(), None)
        arrays.append(pa.array(s.tolist(), type=typ, from_pandas=True))
    fields = [pa.field(c, t, nullable=not r) for c, (t, r) in spec.items()]
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


# --------------------------------------------------------------------------- cross-batch dedup
def registry_path(silver_dir: str, topic: str) -> Path:
    return Path(silver_dir) / "_registry" / f"{topic}_keys.parquet"


def drop_already_in_silver(df: pd.DataFrame, silver_dir: str, topic: str) -> tuple[pd.DataFrame, dict]:
    """Skip rows whose (event_id, record_hash) is already in Silver; keep changed rows as updates."""
    reg = registry_path(silver_dir, topic)
    known = pd.read_parquet(reg) if reg.exists() else pd.DataFrame(columns=["event_id", "record_hash"])
    known_hash = dict(zip(known["event_id"], known["record_hash"]))
    same = df["event_id"].map(known_hash).eq(df["record_hash"])
    out = df[~same].copy()
    out["is_update"] = out["event_id"].isin(known_hash)
    return out, {"rows_in": len(df), "already_in_silver": int(same.sum()),
                 "updates": int(out["is_update"].sum()), "rows_new": int((~out["is_update"]).sum())}


def remember_in_registry(table: pa.Table, silver_dir: str, topic: str) -> None:
    reg = registry_path(silver_dir, topic)
    reg.parent.mkdir(parents=True, exist_ok=True)
    new = table.select(["event_id", "record_hash"]).to_pandas()
    old = pd.read_parquet(reg) if reg.exists() else new.iloc[0:0]
    pd.concat([old, new]).drop_duplicates("event_id", keep="last").to_parquet(reg, index=False)
