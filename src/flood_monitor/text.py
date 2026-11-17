"""Text preprocessing for the narrative fields (Storm Events narratives and
NWS alert headlines/descriptions): lowercase -> remove URLs, codes and
punctuation -> tokenize -> remove English stop words -> light normalization.

No external corpus download is needed (the stop-word list is embedded), so the
Airflow containers do not need internet access for NLTK data.
"""
from __future__ import annotations

import re

STOP_WORDS = frozenset("""
a about above after again against all am an and any are as at be because been before being below
between both but by can could did do does doing down during each few for from further had has have
having he her here hers herself him himself his how i if in into is it its itself just me more most
my myself no nor not now of off on once only or other our ours ourselves out over own same she should
so some such than that the their theirs them themselves then there these they this those through to
too under until up very was we were what when where which while who whom why will with would you your
yours yourself yourselves also however approximately around near along across several due per via
""".split())

# NWS products often use abbreviations; expand the frequent ones before tokenizing.
ABBREVIATIONS = {
    r"\bhwy\b": "highway", r"\bfm\b": "farm to market road", r"\bcr\b": "county road",
    r"\bnws\b": "national weather service", r"\bmph\b": "miles per hour",
    r"\bin\.(?=\s)": "inches",
}
URL_RE = re.compile(r"https?://\S+|www\.\S+")
NUM_UNIT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:inches|inch|in)\b")
NON_ALPHA_RE = re.compile(r"[^a-z0-9\s]")
SPACES_RE = re.compile(r"\s+")
# words that carry flood-impact meaning; used to build keyword flags for Gold
IMPACT_TERMS = ("water rescue", "road closed", "roads closed", "low water crossing", "evacuat",
                "swift water", "homes flooded", "vehicle", "stranded", "creek", "river", "bayou")


def clean(text) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    t = text.lower()
    t = URL_RE.sub(" ", t)
    for pattern, repl in ABBREVIATIONS.items():
        t = re.sub(pattern, repl, t)
    t = NUM_UNIT_RE.sub(r"\1 inches", t)          # "2.5in" / "2.5 inch" -> "2.5 inches"
    t = t.replace("\n", " ").replace("...", " ")
    t = NON_ALPHA_RE.sub(" ", t)                   # punctuation and symbols
    return SPACES_RE.sub(" ", t).strip()


def tokens(cleaned: str) -> list[str]:
    return [w for w in cleaned.split() if w not in STOP_WORDS and len(w) > 1 and not w.isdigit()]


def impact_terms(cleaned: str) -> list[str]:
    return [term.strip() for term in IMPACT_TERMS if term in cleaned]
