import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flood_monitor.normalization import (  # noqa: E402
    clean_coord,
    normalize_ghcnd_record,
    normalize_nws_observation,
    normalize_storm_event,
    parse_damage_usd,
    parse_storm_timestamp_utc,
)


def test_parse_damage_usd():
    assert parse_damage_usd("25K") == 25_000.0
    assert parse_damage_usd("1.5M") == 1_500_000.0
    assert parse_damage_usd("0.5B") == 500_000_000.0
    assert parse_damage_usd("") == 0.0
    assert parse_damage_usd(None) == 0.0


def test_parse_storm_timestamp_utc():
    dt = parse_storm_timestamp_utc("15-MAY-24 14:30:00", "CST-6")
    assert dt == datetime(2024, 5, 15, 20, 30, tzinfo=timezone.utc)


def test_clean_coord_rejects_zero_zero():
    assert clean_coord(0, 0) == (None, None)
    assert clean_coord("29.76", "-95.36") == (29.76, -95.36)


def test_normalize_storm_event_valid():
    row = {
        "EVENT_ID": "1180001",
        "EPISODE_ID": "190001",
        "STATE_FIPS": "48",
        "CZ_TYPE": "C",
        "CZ_FIPS": "201",
        "CZ_NAME": "HARRIS",
        "EVENT_TYPE": "Flash Flood",
        "BEGIN_DATE_TIME": "04-MAY-24 10:00:00",
        "END_DATE_TIME": "04-MAY-24 13:00:00",
        "CZ_TIMEZONE": "CST-6",
        "BEGIN_LAT": "29.76",
        "BEGIN_LON": "-95.36",
        "DAMAGE_PROPERTY": "100K",
        "DAMAGE_CROPS": "0",
        "DEATHS_DIRECT": "0",
        "DEATHS_INDIRECT": "0",
        "INJURIES_DIRECT": "0",
        "INJURIES_INDIRECT": "0",
        "WFO": "HGX",
        "FLOOD_CAUSE": "Heavy Rain",
        "EVENT_NARRATIVE": "Street flooding reported.",
    }
    res, err = normalize_storm_event(row, "StormEvents_details_2024.csv.gz", "20250115")
    assert err is None
    assert res["event"]["event_id"] == "se:1180001"
    assert res["event"]["severity_level"] == 3
    assert res["event"]["location_code"] == "48201"
    assert res["flood_event"]["damage_property_usd"] == 100_000.0


def test_normalize_ghcnd_trace_and_metric():
    row = {
        "STATION": "USW00012960",
        "NAME": "HOUSTON INTERCONTINENTAL AIRPORT, TX US",
        "DATE": "2024-05-04",
        "PRCP": "T",
        "PRCP_ATTRIBUTES": "T,,W,2400",
        "LATITUDE": "29.98",
        "LONGITUDE": "-95.36",
        "ELEVATION": "29.0",
    }
    res, err = normalize_ghcnd_record(row)
    assert err is None
    assert res["event"]["event_id"] == "gh:USW00012960:2024-05-04"
    assert res["daily_precipitation"]["prcp_mm"] == 0.0
    assert res["daily_precipitation"]["mflag"] == "T"


def test_normalize_nws_observation():
    feat = {
        "id": "https://api.weather.gov/stations/KIAH/observations/2026-11-16T18:00:00+00:00",
        "geometry": {"coordinates": [-95.36, 29.98]},
        "properties": {
            "station": "https://api.weather.gov/stations/KIAH",
            "timestamp": "2026-11-16T18:00:00+00:00",
            "textDescription": "Heavy Rain",
            "temperature": {"value": 22.5, "unitCode": "wmoUnit:degC"},
            "precipitationLastHour": {"value": 15.2, "unitCode": "wmoUnit:mm", "qualityControl": "V"},
        },
    }
    res, err = normalize_nws_observation(feat)
    assert err is None
    assert res["event"]["severity_level"] == 2
    assert res["station_observation"]["precip_1h_mm"] == 15.2
