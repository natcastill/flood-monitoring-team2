"""Canonical schema normalization for Texas Flood Monitoring (Workshop 2)."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

TZ_OFFSETS = {
    "CST-6": -6,
    "CST": -6,
    "CDT-5": -5,
    "CDT": -5,
    "MST-7": -7,
    "MST": -7,
    "MDT-6": -6,
    "MDT": -6,
    "UTC": 0,
    "GMT": 0,
}

EVENT_TYPE_MAP = {
    "Flood": "flood",
    "Flash Flood": "flash_flood",
    "Heavy Rain": "heavy_rain",
}


def compute_record_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_damage_usd(raw: Any) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip().upper()
    if not s or s in {"NAN", "NONE", "NULL"}:
        return 0.0
    mult = 1.0
    if s.endswith("K"):
        mult, s = 1e3, s[:-1]
    elif s.endswith("M"):
        mult, s = 1e6, s[:-1]
    elif s.endswith("B"):
        mult, s = 1e9, s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return 0.0


def parse_storm_timestamp_utc(dt_str: Any, tz_str: Any) -> datetime | None:
    if dt_str is None:
        return None
    s = str(dt_str).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    dt = None
    for fmt in ("%d-%b-%y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        return None
    tz_key = str(tz_str or "CST-6").strip().upper()
    offset_hours = TZ_OFFSETS.get(tz_key, -6)
    local_tz = timezone(timedelta(hours=offset_hours))
    return dt.replace(tzinfo=local_tz).astimezone(timezone.utc)


def clean_coord(lat: Any, lon: Any) -> tuple[float | None, float | None]:
    try:
        la = float(lat) if lat not in (None, "", "nan", "NaN") else None
        lo = float(lon) if lon not in (None, "", "nan", "NaN") else None
    except (TypeError, ValueError):
        return None, None
    if la is None or lo is None or (la == 0.0 and lo == 0.0):
        return None, None
    if not (-90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0):
        return None, None
    return la, lo


def _safe_int(val: Any) -> int:
    try:
        if val is None or str(val).strip() in ("", "nan", "NaN"):
            return 0
        return int(float(val))
    except (TypeError, ValueError):
        return 0


def _storm_severity(event_type: str, total_damage: float, casualties: int) -> int:
    if total_damage >= 50_000.0 or casualties > 0:
        return 3
    if event_type in ("flood", "flash_flood"):
        return 2
    return 1


def normalize_storm_event(
    row: dict[str, Any], source_file: str, compiled_date: str | None = None
) -> tuple[dict[str, Any] | None, str | None]:
    raw_id = str(row.get("EVENT_ID", "")).strip()
    if not raw_id:
        return None, "missing EVENT_ID"

    raw_type = str(row.get("EVENT_TYPE", "")).strip()
    event_type = EVENT_TYPE_MAP.get(raw_type)
    if not event_type:
        return None, f"unsupported EVENT_TYPE: {raw_type}"

    ts_utc = parse_storm_timestamp_utc(row.get("BEGIN_DATE_TIME"), row.get("CZ_TIMEZONE"))
    if ts_utc is None:
        return None, f"unparseable BEGIN_DATE_TIME: {row.get('BEGIN_DATE_TIME')}"

    end_ts_utc = parse_storm_timestamp_utc(row.get("END_DATE_TIME"), row.get("CZ_TIMEZONE"))
    lat, lon = clean_coord(row.get("BEGIN_LAT"), row.get("BEGIN_LON"))

    dmg_prop = parse_damage_usd(row.get("DAMAGE_PROPERTY"))
    dmg_crop = parse_damage_usd(row.get("DAMAGE_CROPS"))
    total_dmg = dmg_prop + dmg_crop

    d_dir = _safe_int(row.get("DEATHS_DIRECT"))
    d_ind = _safe_int(row.get("DEATHS_INDIRECT"))
    i_dir = _safe_int(row.get("INJURIES_DIRECT"))
    i_ind = _safe_int(row.get("INJURIES_INDIRECT"))
    casualties = d_dir + d_ind + i_dir + i_ind

    state_fips = str(row.get("STATE_FIPS", "")).strip().zfill(2) if row.get("STATE_FIPS") else ""
    cz_fips = str(row.get("CZ_FIPS", "")).strip().zfill(3) if row.get("CZ_FIPS") else ""
    cz_type = str(row.get("CZ_TYPE", "")).strip()[:1] or None
    location_code = f"{state_fips}{cz_fips}" if (state_fips and cz_fips) else None

    sev_level = _storm_severity(event_type, total_dmg, casualties)
    event_id = f"se:{raw_id}"

    comp_dt = None
    if compiled_date and len(compiled_date) == 8 and compiled_date.isdigit():
        comp_dt = f"{compiled_date[:4]}-{compiled_date[4:6]}-{compiled_date[6:8]}"

    event_rec = {
        "event_id": event_id,
        "source_id": "noaa_storm_events",
        "event_type": event_type,
        "ts_utc": ts_utc,
        "end_ts_utc": end_ts_utc,
        "time_granularity": "instant",
        "latitude": lat,
        "longitude": lon,
        "location_code": location_code,
        "severity_level": sev_level,
        "severity_value": total_dmg,
        "severity_unit": "USD",
        "raw_reference": f"{source_file}:{raw_id}",
        "record_hash": compute_record_hash(row),
    }
    domain_rec = {
        "event_id": event_id,
        "episode_id": str(row.get("EPISODE_ID", "")).strip() or None,
        "cz_type": cz_type,
        "cz_fips": cz_fips or None,
        "cz_name": str(row.get("CZ_NAME", "")).strip() or None,
        "wfo": str(row.get("WFO", "")).strip() or None,
        "flood_cause": str(row.get("FLOOD_CAUSE", "")).strip() or None,
        "damage_property_usd": dmg_prop,
        "damage_crops_usd": dmg_crop,
        "deaths_direct": d_dir,
        "deaths_indirect": d_ind,
        "injuries_direct": i_dir,
        "injuries_indirect": i_ind,
        "event_narrative": str(row.get("EVENT_NARRATIVE", "")).strip() or None,
        "source_file": source_file,
        "source_file_compiled": comp_dt,
    }
    return {"event": event_rec, "flood_event": domain_rec}, None


def _ghcnd_severity(prcp_mm: float) -> int:
    if prcp_mm >= 75.0:
        return 3
    if prcp_mm >= 25.0:
        return 2
    if prcp_mm >= 10.0:
        return 1
    return 0


def normalize_ghcnd_record(
    row: dict[str, Any], request_url: str = "ncei_ghcnd"
) -> tuple[dict[str, Any] | None, str | None]:
    station = str(row.get("STATION", "")).strip()
    obs_date = str(row.get("DATE", "")).strip()
    if not station or not obs_date:
        return None, "missing STATION or DATE"

    try:
        local_day = datetime.strptime(obs_date, "%Y-%m-%d")
    except ValueError:
        return None, f"invalid DATE: {obs_date}"

    attrs = str(row.get("PRCP_ATTRIBUTES", "") or "")
    parts = attrs.split(",")
    mflag = (parts[0].strip()[:1] or None) if len(parts) > 0 else None
    qflag = (parts[1].strip()[:1] or None) if len(parts) > 1 else None
    sflag = (parts[2].strip()[:1] or None) if len(parts) > 2 else None
    obs_time = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None

    raw_prcp = row.get("PRCP")
    if raw_prcp is None or str(raw_prcp).strip() == "":
        if mflag == "T":
            prcp_mm = 0.0
        else:
            return None, "missing PRCP value"
    elif str(raw_prcp).strip().upper() == "T":
        prcp_mm = 0.0
        mflag = mflag or "T"
    else:
        try:
            prcp_mm = float(raw_prcp)
        except ValueError:
            return None, f"non-numeric PRCP: {raw_prcp}"

    if prcp_mm < 0:
        return None, f"negative PRCP: {prcp_mm}"

    # Texas local day 00:00 CST (-06:00) -> 06:00 UTC
    ts_utc = local_day.replace(tzinfo=timezone(timedelta(hours=-6))).astimezone(timezone.utc)
    lat, lon = clean_coord(row.get("LATITUDE"), row.get("LONGITUDE"))
    elev = None
    if row.get("ELEVATION") not in (None, ""):
        try:
            elev = float(row["ELEVATION"])
        except (TypeError, ValueError):
            elev = None

    event_id = f"gh:{station}:{obs_date}"
    event_rec = {
        "event_id": event_id,
        "source_id": "ncei_ghcnd",
        "event_type": "daily_precipitation",
        "ts_utc": ts_utc,
        "end_ts_utc": ts_utc + timedelta(days=1),
        "time_granularity": "day",
        "latitude": lat,
        "longitude": lon,
        "location_code": station,
        "severity_level": _ghcnd_severity(prcp_mm),
        "severity_value": prcp_mm,
        "severity_unit": "mm",
        "raw_reference": f"{request_url}#{station}/{obs_date}",
        "record_hash": compute_record_hash(row),
    }
    domain_rec = {
        "event_id": event_id,
        "station_id": station,
        "obs_date": obs_date,
        "prcp_mm": prcp_mm,
        "mflag": mflag,
        "qflag": qflag,
        "sflag": sflag,
        "obs_time": obs_time,
    }
    station_rec = {
        "station_id": station,
        "network": "GHCN",
        "name": str(row.get("NAME", "")).strip() or None,
        "latitude": lat,
        "longitude": lon,
        "elevation_m": elev,
    }
    return {
        "event": event_rec,
        "daily_precipitation": domain_rec,
        "weather_station": station_rec,
    }, None


def _unit_to_mm(val_obj: Any) -> tuple[float | None, str | None]:
    if not isinstance(val_obj, dict):
        return None, None
    val = val_obj.get("value")
    qc = val_obj.get("qualityControl")
    if val is None:
        return None, qc
    try:
        num = float(val)
    except (TypeError, ValueError):
        return None, qc
    unit = str(val_obj.get("unitCode", ""))
    if unit.endswith(":m"):
        num *= 1000.0
    elif unit.endswith(":in") or "inch" in unit.lower():
        num *= 25.4
    return num, qc


def _nws_obs_severity(p1h: float | None) -> int:
    if p1h is None or p1h < 2.0:
        return 0
    if p1h < 10.0:
        return 1
    if p1h < 25.0:
        return 2
    return 3


def normalize_nws_observation(
    feature: dict[str, Any], default_station: str | None = None
) -> tuple[dict[str, Any] | None, str | None]:
    props = feature.get("properties") or {}
    ts_raw = props.get("timestamp")
    if not ts_raw:
        return None, "missing properties.timestamp"
    try:
        ts_utc = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None, f"invalid timestamp: {ts_raw}"

    st_url = str(props.get("station", "")).strip()
    station_id = st_url.rsplit("/", 1)[-1] if st_url else (default_station or "")
    if not station_id:
        return None, "missing stationId"

    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None]
    lon, lat = (coords[0], coords[1]) if len(coords) >= 2 else (None, None)
    lat, lon = clean_coord(lat, lon)

    p1h, qc1 = _unit_to_mm(props.get("precipitationLastHour"))
    p3h, _ = _unit_to_mm(props.get("precipitationLast3Hours"))
    p6h, _ = _unit_to_mm(props.get("precipitationLast6Hours"))

    temp_c = None
    temp_obj = props.get("temperature") or {}
    if isinstance(temp_obj, dict) and temp_obj.get("value") is not None:
        try:
            temp_c = float(temp_obj["value"])
            if str(temp_obj.get("unitCode", "")).endswith(":degF"):
                temp_c = (temp_c - 32.0) * 5.0 / 9.0
        except (TypeError, ValueError):
            temp_c = None

    raw_ref = str(feature.get("id") or props.get("@id") or f"{station_id}:{ts_raw}")
    event_id = f"obs:{station_id}:{ts_utc.isoformat()}"

    event_rec = {
        "event_id": event_id,
        "source_id": "nws_observations",
        "event_type": "precipitation_observation",
        "ts_utc": ts_utc,
        "end_ts_utc": None,
        "time_granularity": "instant",
        "latitude": lat,
        "longitude": lon,
        "location_code": station_id,
        "severity_level": _nws_obs_severity(p1h),
        "severity_value": p1h,
        "severity_unit": "mm",
        "raw_reference": raw_ref,
        "record_hash": compute_record_hash(feature),
    }
    domain_rec = {
        "event_id": event_id,
        "station_id": station_id,
        "precip_1h_mm": p1h,
        "precip_3h_mm": p3h,
        "precip_6h_mm": p6h,
        "precip_qc": qc1,
        "temperature_c": temp_c,
        "text_description": props.get("textDescription"),
    }
    return {"event": event_rec, "station_observation": domain_rec}, None
