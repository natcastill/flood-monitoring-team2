"""Workshop 4: both dashboards build their full layout and every figure from the Gold files."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
pytest.importorskip("dash")
pytest.importorskip("dash_bootstrap_components")

from dashboard import data_loader as dl  # noqa: E402

pytestmark = pytest.mark.skipif(not list(dl.GOLD_DIR.glob("storytelling_*.parquet")),
                                reason="no Gold files in datalake_gold/")


def test_governance_cards_are_computed_from_gold():
    df, _, _ = dl.load_governance()
    c = dl.gov_summary_cards(df)
    assert c["graded"] == c["ok"] + c["warn"] + c["fail"]
    assert c["ok"] == int((df["status"] == "ok").sum())
    assert 0 <= c["duplicate_after"] <= c["duplicate_before"] <= 100


def test_governance_app_layout_and_figures():
    from dashboard import governance_app as g
    df, _, _ = dl.load_governance()
    assert g.serve_layout() is not None
    for mode in ("event", "ingestion"):
        assert len(g.fig_volume(df, mode).data) == 4          # all four sources
    assert g.fig_null_rates(df, "flood_events").data


def test_storytelling_app_layout_and_figures():
    from dashboard import storytelling_app as s
    df, _, _ = dl.load_storytelling()
    assert s.serve_layout() is not None
    fri = dl.st_flood_risk(df)
    assert set(fri["label"]) <= {"LOW", "MEDIUM", "HIGH"} and len(fri) == 5
    t = dl.st_trend_monthly(df)
    assert s.fig_volume(t).data and s.fig_trend(t).data
    assert s.fig_keywords(df, "top_unigrams").data
