import re
import numpy as np
import pandas as pd
from flood_monitor.config import FLOOD_ALERT_EVENTS, TEXAS_STATIONS, TX_BBOX


def normalize_text(text: str) -> str:
    """Lowercase, remove special punctuation, and collapse whitespace."""
    if not isinstance(text, str) or not text.strip():
        return "unknown"
    cleaned = text.lower().strip()
    cleaned = re.sub(r"[^a-z0-9\s;/\-]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def compute_hourly_rain_severity(mm: float) -> int:
    """Canonical severity scale from Workshop 1 (Section 5)."""
    if pd.isna(mm) or mm < 2.0:
        return 0
    elif mm < 10.0:
        return 1
    elif mm <= 25.0:
        return 2
    return 3


def compute_alert_severity(event_name: str) -> int:
    """Canonical severity scale for NWS alerts from Workshop 1 (Section 5)."""
    ev = (event_name or "").lower()
    if "warning" in ev:
        return 3
    elif "watch" in ev:
        return 2
    elif "advisory" in ev or "statement" in ev or "outlook" in ev:
        return 1
    return 0


def process_nws_observations(raw_payload: dict, source_filename: str) -> pd.DataFrame:
    """
    Transform raw NWS station observations JSON into clean Silver schema.
    Addresses Workshop 1 DQ Issues #6 (null precip in 5-min ASOS) & #7 (mixed intervals).
    """
    records = []
    stations_data = raw_payload.get("nws_observations", {})

    for station_id, features in stations_data.items():
        station_meta = TEXAS_STATIONS.get(station_id, {})
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]

            precip_1h = (props.get("precipitationLastHour") or {}).get("value")
            precip_3h = (props.get("precipitationLast3Hours") or {}).get("value")
            precip_6h = (props.get("precipitationLast6Hours") or {}).get("value")
            temp_c = (props.get("temperature") or {}).get("value")
            qc_flag = (props.get("precipitationLastHour") or {}).get("qualityControl")

            records.append({
                "event_id": f"obs:{station_id}:{props.get('timestamp')}",
                "source_system": "nws_observations",
                "station_id": station_id,
                "ghcn_station_id": station_meta.get("ghcn_id"),
                "county": station_meta.get("county"),
                "ts_utc": props.get("timestamp"),
                "latitude": coords[1] if coords[1] is not None else station_meta.get("lat"),
                "longitude": coords[0] if coords[0] is not None else station_meta.get("lon"),
                "precip_1h_mm": precip_1h,
                "precip_3h_mm": precip_3h,
                "precip_6h_mm": precip_6h,
                "temperature_c": temp_c,
                "qc_flag": qc_flag if qc_flag else "MISSING",
                "text_description": normalize_text(props.get("textDescription", "")),
                "bronze_source_file": source_filename,
            })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # 1. Data Type Normalization (UTC datetime & float64)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_utc", "station_id"])

    numeric_cols = ["precip_1h_mm", "precip_3h_mm", "precip_6h_mm", "temperature_c", "latitude", "longitude"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # 2. Null Handling (Workshop 1 Issue #6: 5-min ASOS reports have null precip; flag vs METAR)
    df["is_metar_precip_report"] = df["precip_1h_mm"].notna()
    df["precip_1h_mm_clean"] = df["precip_1h_mm"].fillna(0.0)

    # 3. Outlier Detection & Handling (Physical domain + IQR flag)
    # Texas extreme physical cap (~300 mm/h) and negative sensor errors
    df["is_outlier_precip"] = (df["precip_1h_mm_clean"] < 0) | (df["precip_1h_mm_clean"] > 300.0)
    df.loc[df["precip_1h_mm_clean"] < 0, "precip_1h_mm_clean"] = 0.0

    # Geographic bounding box validation (Texas)
    valid_geo = (
        df["latitude"].between(TX_BBOX["min_lat"], TX_BBOX["max_lat"])
        & df["longitude"].between(TX_BBOX["min_lon"], TX_BBOX["max_lon"])
    )
    df = df[valid_geo].copy()

    # 4. Canonical Severity Mapping
    df["severity_level"] = df["precip_1h_mm_clean"].apply(compute_hourly_rain_severity).astype("int8")

    # 5. Deduplication by natural composite key (station_id, ts_utc)
    df = df.sort_values("ts_utc").drop_duplicates(subset=["station_id", "ts_utc"], keep="last")

    # Enforce categorical types for Parquet efficiency
    df["station_id"] = df["station_id"].astype("category")
    df["county"] = df["county"].astype("category")

    return df.reset_index(drop=True)


def process_nws_alerts(raw_payload: dict, source_filename: str) -> pd.DataFrame:
    """
    Transform raw NWS alerts JSON into clean Silver schema.
    Addresses Workshop 1 DQ Issues #8 (missing polygon), #9 (local offsets), #10 (Update chains), #11 (non-flood mix).
    """
    features = raw_payload.get("nws_alerts", [])
    records = []

    for feat in features:
        props = feat.get("properties", {})
        event_name = props.get("event", "")

        # DQ Issue #11: Filter strictly flood-related events
        if event_name not in FLOOD_ALERT_EVENTS:
            continue

        # DQ Issue #8: Polygon centroid if geometry exists, else None (never 0,0)
        geom = feat.get("geometry")
        lat, lon = None, None
        if geom and geom.get("type") == "Polygon":
            ring = geom.get("coordinates", [[]])[0]
            if ring:
                lons = [pt[0] for pt in ring]
                lats = [pt[1] for pt in ring]
                lon = float(np.mean(lons))
                lat = float(np.mean(lats))

        refs = props.get("references") or []
        ref_ids = [r.get("@id", "") for r in refs if isinstance(r, dict)]

        records.append({
            "event_id": f"al:{props.get('id')}",
            "alert_id": props.get("id"),
            "source_system": "nws_alerts",
            "nws_event": event_name,
            "message_type": props.get("messageType", "Alert"),
            "status": props.get("status", "Actual"),
            "nws_severity": props.get("severity", "Unknown"),
            "certainty": props.get("certainty", "Unknown"),
            "urgency": props.get("urgency", "Unknown"),
            "sent_utc": props.get("sent"),
            "onset_utc": props.get("onset") or props.get("effective"),
            "expires_utc": props.get("expires"),
            "area_desc": normalize_text(props.get("areaDesc", "")),
            "ugc_codes": ",".join((props.get("geocode") or {}).get("UGC", [])),
            "reference_ids": ",".join(ref_ids),
            "has_geometry": geom is not None,
            "centroid_lat": lat,
            "centroid_lon": lon,
            "severity_level": compute_alert_severity(event_name),
            "bronze_source_file": source_filename,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # DQ Issue #9: Convert local offsets (-05:00, -06:00, -07:00) to UTC
    for ts_col in ["sent_utc", "onset_utc", "expires_utc"]:
        df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    df["severity_level"] = df["severity_level"].astype("int8")
    df = df.drop_duplicates(subset=["alert_id"], keep="last")

    return df.reset_index(drop=True)