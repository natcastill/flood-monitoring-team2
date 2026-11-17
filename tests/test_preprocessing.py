"""Silver preprocessing tests on a trimmed real Bronze file (tests/fixtures/bronze/)."""
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from flood_monitor import preprocessing as pp  # noqa: E402
from flood_monitor.text import clean, tokens  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "bronze" / "texas_flood_sources_20261116_233000.json"
RAW = json.loads(FIX.read_text(encoding="utf-8"))


@pytest.mark.parametrize("topic", list(pp.PROCESSORS))
def test_each_topic_produces_rows_that_fit_the_schema(topic):
    df = pp.PROCESSORS[topic](RAW, source_filename=FIX.name)
    assert not df.empty
    df["is_update"] = False
    df["processed_at_utc"] = "2026-11-16T23:45:00Z"
    table = pp.enforce_schema(topic, df)
    assert table.num_rows == len(df)
    assert set(pp.SCHEMAS[topic]) <= set(table.column_names)


def test_missing_rainfall_is_not_turned_into_zero():
    df = pp.process_nws_observations(RAW, FIX.name)
    assert df["precip_missing"].equals(df["precip_1h_mm"].isna())
    assert df.loc[df["precip_missing"], "severity_level"].isna().all()


def test_only_flood_alerts_are_kept():
    df = pp.process_nws_alerts(RAW, FIX.name)
    assert df["nws_event"].isin(pp.FLOOD_ALERT_EVENTS).all()


def test_text_pipeline_removes_stop_words_and_punctuation():
    c = clean("Water rescues on Hwy 90... see https://weather.gov!")
    assert "https" not in c and "highway" in c
    assert "on" not in tokens(c) and "rescues" in tokens(c)


def test_cross_batch_dedup_is_idempotent(tmp_path):
    df = pp.process_storm_events(RAW, FIX.name)
    first, stats1 = pp.drop_already_in_silver(df, str(tmp_path), "flood_events")
    assert stats1["rows_new"] == len(df)
    first["processed_at_utc"] = "2026-11-16T23:45:00Z"
    table = pp.enforce_schema("flood_events", first)
    pq.write_table(table, tmp_path / "flood_events_test.parquet")
    pp.remember_in_registry(table, str(tmp_path), "flood_events")
    second, stats2 = pp.drop_already_in_silver(df, str(tmp_path), "flood_events")
    assert second.empty and stats2["already_in_silver"] == len(df)
