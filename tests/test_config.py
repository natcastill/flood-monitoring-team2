import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from flood_monitor.config import load_config  # noqa: E402


def test_four_sources_two_per_role():
    roles = [s["role"] for s in load_config()["sources"]]
    assert roles.count("historical") == 2
    assert roles.count("incremental") == 2


def test_every_source_has_watermark_and_key():
    for s in load_config()["sources"]:
        assert s["watermark_field"] and s["duplicate_key"] and s["license"]
