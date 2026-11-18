"""Text preprocessing for the narrative fields (Storm Events narratives and
NWS alert headlines/descriptions).

Pipeline (v0.3, Workshop 3):
  repair_encoding  -> fix the U+FFFD replacement characters found in the NOAA narratives
                      and fold accents to ASCII ("Mexico", "1-2 inches")
  clean            -> lowercase, remove URLs, HTML tags/entities, NWS section markers
                      ("* WHAT...", "* IMPACTS..."), clock times and ordinal dates;
                      expand abbreviations; normalize road identifiers and units;
                      remove punctuation
  tokens           -> whitespace tokenization, English + domain stop words, no tokens
                      with digits, minimum length 3
  impact_terms     -> flood-impact phrases used for keyword flags in Gold

No external corpus download is needed (the stop-word lists are embedded), so the
Airflow containers do not need internet access for NLTK data.
"""
from __future__ import annotations

import html
import re
import unicodedata

ENGLISH_STOP_WORDS = frozenset("""
a about above after again against all am an and any are as at be because been before being below
between both but by can could did do does doing down during each few for from further had has have
having he her here hers herself him himself his how i if in into is it its itself just me more most
my myself no nor not now of off on once only or other our ours ourselves out over own same she should
so some such than that the their theirs them themselves then there these they this those through to
too under until up very was we were what when where which while who whom why will with would you your
yours yourself yourselves also however approximately around near along across several due per via
""".split())

# Workshop 3: words that appear in most narratives but say nothing about the flood itself
# (found in the Workshop 2 Silver keyword counts: "texas", "county", "produced", "moved", ...).
DOMAIN_STOP_WORDS = frozenset("""
texas county counties reported report reports reporting produced producing occurred occurring resulted
resulting led moved moving developed developing area areas portions portion region regions location
locations morning afternoon evening night overnight early late hours hour day days time times week
weeks numerous many much one two three four five six several began beginning continued continuing
north south east west northeast northwest southeast southwest northern southern eastern western
central upper level levels mph miles mile feet foot inches inch approximately located near nearby
national weather service nws including included include also well within still across another
caused estimated multiple approximately
monday tuesday wednesday thursday friday saturday sunday
january february march april may june july august september october november december
""".split())

STOP_WORDS = ENGLISH_STOP_WORDS | DOMAIN_STOP_WORDS

# Repairs for the replacement characters (U+FFFD) found in 125 Storm Events narratives (2010-2026).
# A multi-byte character (dash, apostrophe, quote, accented letter) was lost in the source file.
KNOWN_MOJIBAKE = {"m��xico": "mexico", "lope��o": "lopeno"}
ENC_DIGIT_RANGE_RE = re.compile(r"(?<=\d)�+(?=\d)")         # "1���2" -> "1-2"
ENC_APOSTROPHE_RE = re.compile(r"(?<=[A-Za-z])�+(?=s\b)")       # "city���s" -> "city's"
ENC_ANY_RE = re.compile(r"�+")

URL_RE = re.compile(r"https?://\S+|www\.\S+")
HTML_TAG_RE = re.compile(r"<[^>]+>")
NWS_MARKER_RE = re.compile(r"\*\s*(what|where|when|impacts|additional details|precautionary"
                           r"/preparedness actions)\s*\.\.\.")
CLOCK_RE = re.compile(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b(?:\s*(?:cst|cdt|mst|mdt|est|edt|utc))?"
                      r"|\b\d{3,4}\s*(?:am|pm|cst|cdt|mst|mdt|utc)\b")
# Boilerplate sentences of the Storm Events narratives (found by counting repeated sentences):
# they say how the record was written, not what the flood did, and dominated the bigrams.
BOILERPLATE_RE = re.compile(
    r"(?:the\s+)?(?:total\s+)?time of occurrence (?:is|was) (?:partially\s+)?estimated (?:from|using|by)"
    r" radar[^.]*\.?|the cost of damage is a (?:very\s+)?rough estimate\.?|no (?:property\s+)?damage was"
    r" reported\.?|report from mping:?", re.IGNORECASE)
SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\.\.\.|\n+")
ORDINAL_RE = re.compile(r"\b\d+(?:st|nd|rd|th)\b")
# road identifiers ("fm 1960", "us 59", "ih-35", "sh 71") -> one word, so "us" is not read as a pronoun
FARM_ROAD_RE = re.compile(r"\b(?:farm|ranch)\s*(?:to|-)\s*market\s*(?:road|rd)?\b")
ROAD_ID_RE = re.compile(r"\b(?:fm|cr|rm|sh|ih|us|hwy|loop|spur|i)\s*-?\s*\d+[a-z]?\b")
ABBREVIATIONS = {
    r"\bhwy\b": "highway", r"\brd\b": "road", r"\bst\b": "street", r"\bdr\b": "drive",
    r"\bln\b": "lane", r"\bblvd\b": "boulevard", r"\bcr\b": "county road", r"\bfm\b": "road",
    r"\btx\b": " ", r"\bnws\b": "national weather service", r"\bin\.(?=\s)": "inches",
}
NUM_UNIT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:inches|inch|in)\b")
NON_ALPHA_RE = re.compile(r"[^a-z0-9\s]")
SPACES_RE = re.compile(r"\s+")

# words that carry flood-impact meaning; used to build keyword flags for Gold
IMPACT_TERMS = ("water rescue", "road closed", "roads closed", "low water crossing", "evacuat",
                "swift water", "homes flooded", "vehicle", "stranded", "creek", "river", "bayou",
                "dam", "levee", "drown")


def repair_encoding(text: str) -> str:
    """Fix U+FFFD replacement characters and fold accents/symbols to ASCII."""
    if not isinstance(text, str):
        return ""
    t = html.unescape(text)
    if "�" in t:
        low = t.lower()
        for bad, good in KNOWN_MOJIBAKE.items():
            if bad in low:
                t = re.sub(re.escape(bad), good, t, flags=re.IGNORECASE)
        t = ENC_DIGIT_RANGE_RE.sub("-", t)
        t = ENC_APOSTROPHE_RE.sub("'", t)
        t = ENC_ANY_RE.sub(" ", t)
    return unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode("ascii")


def had_encoding_problem(text) -> bool:
    return isinstance(text, str) and "�" in text


def _normalize(t: str) -> str:
    """Steps shared by clean() and sentences(): t is already repaired and lowercased."""
    t = URL_RE.sub(" ", t)
    t = HTML_TAG_RE.sub(" ", t)
    t = BOILERPLATE_RE.sub(" ", t)
    t = NWS_MARKER_RE.sub(" ", t)
    t = CLOCK_RE.sub(" ", t)
    t = ORDINAL_RE.sub(" ", t)
    t = FARM_ROAD_RE.sub(" road ", t)
    t = ROAD_ID_RE.sub(" road ", t)
    for pattern, repl in ABBREVIATIONS.items():
        t = re.sub(pattern, repl, t)
    t = NUM_UNIT_RE.sub(r"\1 inches", t)          # "2.5in" / "2.5 inch" -> "2.5 inches"
    t = t.replace("\n", " ").replace("...", " ")
    t = NON_ALPHA_RE.sub(" ", t)                   # punctuation and symbols
    return SPACES_RE.sub(" ", t).strip()


def clean(text) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    return _normalize(repair_encoding(text).lower())


def sentences(text) -> list[str]:
    """Cleaned sentences, so that bigrams are not built across a sentence boundary."""
    if not isinstance(text, str) or not text.strip():
        return []
    t = BOILERPLATE_RE.sub(" ", repair_encoding(text))
    parts = (_normalize(s.lower()) for s in SENTENCE_RE.split(t))
    return [s for s in parts if s]


def tokens(cleaned: str) -> list[str]:
    out = []
    for w in cleaned.split():
        if len(w) > 2 and w not in STOP_WORDS and not any(ch.isdigit() for ch in w):
            if not out or out[-1] != w:            # "road road" -> "road"
                out.append(w)
    return out


def word_count(cleaned: str) -> int:
    return len(cleaned.split()) if cleaned else 0


def impact_terms(cleaned: str) -> list[str]:
    return [term.strip() for term in IMPACT_TERMS if term in cleaned]


# Very small language check for the governance KPI (NWS/NOAA text is expected to be English).
_SPANISH_HINTS = frozenset("de la el los las por para con del una que esta como pero".split())


def detect_language(cleaned: str) -> str:
    words = cleaned.split()
    if not words:
        return "empty"
    en = sum(w in ENGLISH_STOP_WORDS for w in words)
    es = sum(w in _SPANISH_HINTS for w in words)
    return "es" if es >= 2 and es > en else "en"
