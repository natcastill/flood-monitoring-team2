"""Compare two polls of the incremental sources and write a Markdown report.

    python scripts/compare_polls.py                 # two most recent polls
    python scripts/compare_polls.py poll_A poll_B   # explicit folder names
Output: workshop_1/samples/polls/poll_comparison.md
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLLS = ROOT / "workshop_1" / "samples" / "polls"
HEADERS = ["ETag", "Last-Modified", "Expires", "Cache-Control", "X-Request-Id", "Date"]


def load(path: Path) -> tuple[dict, dict]:
    body = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    meta_p = path.with_name(path.name + ".meta.json")
    meta = json.loads(meta_p.read_text(encoding="utf-8")) if meta_p.exists() else {}
    return body, meta


def digest(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:12]


def header_rows(name: str, m1: dict, m2: dict) -> list[str]:
    h1, h2 = m1.get("headers", {}), m2.get("headers", {})
    rows = []
    for h in HEADERS:
        a, b = h1.get(h, "-"), h2.get(h, "-")
        rows.append(f"| {name} | {h} | {a} | {b} | {'yes' if a != b else 'no'} |")
    return rows


def compare_alerts(d1: Path, d2: Path, lines: list[str], hdr: list[str]) -> None:
    def features(d):
        feats, meta = [], {}
        for f in sorted((d / "nws_alerts").glob("*.json")):
            if f.name.endswith(".meta.json"):
                continue
            body, m = load(f)
            if f.name == "active_TX.json":
                meta = m
            feats += [x for x in body.get("features", []) if "properties" in x]
        return {x["properties"]["id"]: x["properties"] for x in feats}, meta

    a1, m1 = features(d1)
    a2, m2 = features(d2)
    hdr += header_rows("alerts/active", m1, m2)
    new, gone = set(a2) - set(a1), set(a1) - set(a2)
    changed = [i for i in set(a1) & set(a2) if digest(a1[i]) != digest(a2[i])]
    updates = [a2[i] for i in new if a2[i].get("references")]
    lines += [
        "## NWS alerts (active + recent, Texas)",
        f"- Poll 1: {len(a1)} distinct alert ids; poll 2: {len(a2)}",
        f"- New ids in poll 2: {len(new)}; ids no longer returned: {len(gone)}",
        f"- Same id, different content: {len(changed)}",
        f"- New alerts that reference earlier alerts (Update/Cancel chain): {len(updates)}",
        "- messageType in poll 2: " + ", ".join(
            f"{k}={sum(1 for p in a2.values() if p.get('messageType') == k)}"
            for k in sorted({p.get('messageType') for p in a2.values()} - {None})),
        "",
    ]
    for p in updates[:5]:
        refs = ", ".join(r.get("identifier", "")[-40:] for r in p["references"])
        lines.append(f"  - `{p['id'][-40:]}` {p.get('messageType')} {p.get('event')} -> refs: {refs}")
    lines.append("")


def compare_obs(d1: Path, d2: Path, lines: list[str], hdr: list[str]) -> None:
    lines.append("## NWS station observations")
    lines.append("| station | obs poll 1 | obs poll 2 | newest ts poll 1 | newest ts poll 2 | new ts | overlapping ts with changed values |")
    lines.append("|---|---|---|---|---|---|---|")
    for f2 in sorted((d2 / "nws_observations").glob("*.json")):
        if f2.name.endswith(".meta.json"):
            continue
        b1, m1 = load(d1 / "nws_observations" / f2.name)
        b2, m2 = load(f2)
        o1 = {x["properties"]["timestamp"]: x["properties"] for x in b1.get("features", [])}
        o2 = {x["properties"]["timestamp"]: x["properties"] for x in b2.get("features", [])}
        overlap = set(o1) & set(o2)
        revised = [t for t in overlap if digest(o1[t]) != digest(o2[t])]
        lines.append(f"| {f2.stem} | {len(o1)} | {len(o2)} | {max(o1, default='-')} | "
                     f"{max(o2, default='-')} | {len(set(o2) - set(o1))} | {len(revised)} |")
        if f2.stem == "KIAH":
            hdr += header_rows(f"observations/{f2.stem}", m1, m2)
    lines.append("")


def main() -> None:
    if len(sys.argv) == 3:
        d1, d2 = POLLS / sys.argv[1], POLLS / sys.argv[2]
    else:
        polls = sorted(p for p in POLLS.glob("poll_*") if p.is_dir())
        if len(polls) < 2:
            raise SystemExit("Need two polls. Run scripts/poll_incremental.py twice.")
        d1, d2 = polls[-2], polls[-1]
    lines = [f"# Incremental poll comparison", "",
             f"- Poll 1: `{d1.name}`", f"- Poll 2: `{d2.name}`", ""]
    hdr = ["## HTTP headers", "| endpoint | header | poll 1 | poll 2 | changed |",
           "|---|---|---|---|---|"]
    compare_alerts(d1, d2, lines, hdr)
    compare_obs(d1, d2, lines, hdr)
    lines += hdr + [""]
    out = POLLS / "poll_comparison.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nWritten to {out}")


if __name__ == "__main__":
    main()
