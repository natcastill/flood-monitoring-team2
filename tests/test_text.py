"""Workshop 3 text-preprocessing improvements (W3-3, W3-4, W3-7)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from flood_monitor.text import clean, detect_language, repair_encoding, sentences, tokens  # noqa: E402


def test_replacement_characters_from_the_noaa_files_are_repaired():
    assert repair_encoding("Monterrey, Nuevo Leon, M��xico") == "Monterrey, Nuevo Leon, mexico"
    assert repair_encoding("1���2 inches") == "1-2 inches"
    assert repair_encoding("the city���s streets") == "the city's streets"
    assert "�" not in repair_encoding("odd � char")


def test_boilerplate_sentences_and_nws_markers_are_removed():
    c = clean("* WHAT...Flooding caused by excessive rainfall. The cost of damage is a very rough estimate.")
    assert "what" not in c.split() and "rough" not in c
    assert sentences("Heavy rain fell. Roads closed... The total time of occurrence is estimated from radar.") \
        == ["heavy rain fell", "roads closed"]


def test_domain_stop_words_digits_and_repeated_road_ids():
    toks = tokens(clean("Flooding was reported on FM 1960 and US 59 in Harris County, Texas on the 3rd at 8:31 AM CST."))
    assert toks == ["flooding", "road", "harris"]


def test_language_check():
    assert detect_language(clean("Water rescues were needed on the highway")) == "en"
    assert detect_language(clean("la lluvia de la tarde por la calle con agua")) == "es"
