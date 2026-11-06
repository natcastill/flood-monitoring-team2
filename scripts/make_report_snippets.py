"""Turn the real Workshop 1 samples into LaTeX snippets for the report.

Run AFTER fetch_samples.py, the second poll, compare_polls.py and the notebook:
    python scripts/make_report_snippets.py
Writes report/generated/*.tex and *.txt, which report/main.tex includes automatically.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
S = ROOT / "workshop_1" / "samples"
OUT = ROOT / "report" / "generated"
OUT.mkdir(parents=True, exist_ok=True)


def tex(s) -> str:
    s = "" if s is None else str(s)
    rep = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#",
           "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(rep.get(c, c) for c in s)


def first_body(folder: Path, pattern: str = "*.json") -> Path | None:
    for p in sorted(folder.glob(pattern)):
        if not p.name.endswith(".meta.json"):
            return p
    return None


def meta_text(body: Path) -> str:
    m = json.loads(body.with_name(body.name + ".meta.json").read_text(encoding="utf-8"))
    h = m.get("headers", {})
    keep = ["Content-Type", "Content-Length", "Last-Modified", "ETag", "Expires", "Cache-Control"]
    lines = [f"GET {m['url']}", f"HTTP {m['status_code']}  retrieved {m['retrieved_at_utc']}"]
    lines += [f"{k}: {h[k]}" for k in keep if k in h]
    return "\n".join(lines)


def cut(text: str, width: int = 105, max_lines: int = 40) -> str:
    out = []
    for line in text.splitlines()[:max_lines]:
        out.append(line if len(line) <= width else line[: width - 3] + "...")
    return "\n".join(out)


def write(name: str, text: str) -> None:
    (OUT / name).write_text(text, encoding="utf-8")
    print("wrote", OUT / name)


macros = {}

# --- Storm Events ---------------------------------------------------------
se_csv = sorted((S / "storm_events").glob("texas_floods_*.csv"))
if se_csv:
    path = se_csv[-1]
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    cols = ["EVENT_ID", "EVENT_TYPE", "BEGIN_DATE_TIME", "CZ_TIMEZONE", "CZ_TYPE", "CZ_NAME",
            "BEGIN_LAT", "BEGIN_LON", "DAMAGE_PROPERTY"]
    lines = [",".join(cols)] + [",".join(r.get(c, "") for c in cols) for r in rows[:4]]
    listing = S / "storm_events" / "listing.html"
    write("storm_sample.txt", meta_text(listing) + "\n\n# " + path.name + " (selected columns)\n" + cut("\n".join(lines)))
    macros["StormRows"] = len(rows)
    macros["StormYear"] = path.stem.split("_")[-1]
vol = S / "storm_events" / "volume_estimate.csv"
if vol.exists():
    with vol.open(encoding="utf-8") as fh:
        v = list(csv.DictReader(fh))
    total = sum(float(r["bytes_gz"] or 0) for r in v)
    macros["StormFiles"] = len(v)
    macros["StormMB"] = f"{total / 1e6:.1f}"
    macros["StormFirstYear"] = v[0]["year"]
    macros["StormLastYear"] = v[-1]["year"]
    macros["StormLastCompiled"] = v[-1]["compiled"]
    if "StormRows" in macros:
        macros["StormRowsWindow"] = f"{macros['StormRows'] * len(v):,}".replace(",", "{,}")

# --- GHCN-Daily -----------------------------------------------------------
gh = first_body(S / "ghcnd")
if gh:
    data = json.loads(gh.read_text(encoding="utf-8"))
    write("ghcnd_sample.txt", meta_text(gh) + "\n\n" + cut("\n".join(json.dumps(r) for r in data[:4])))
    n = 0
    for p in (S / "ghcnd").glob("*.json"):
        if not p.name.endswith(".meta.json"):
            n += len(json.loads(p.read_text(encoding="utf-8")))
    macros["GhcndRows"] = n

# --- NWS (latest poll) ----------------------------------------------------
polls = sorted(p for p in (S / "polls").glob("poll_*") if p.is_dir())
if polls:
    last = polls[-1]
    macros["PollFirst"] = polls[0].name.replace("poll_", "")
    macros["PollLast"] = last.name.replace("poll_", "")
    ob = first_body(last / "nws_observations")
    if ob:
        data = json.loads(ob.read_text(encoding="utf-8"))
        feat = (data.get("features") or [{}])[0]
        p = feat.get("properties", {})
        keep = {k: p.get(k) for k in ["@id", "stationId", "timestamp", "textDescription",
                                      "temperature", "precipitationLastHour"]}
        write("obs_sample.txt", meta_text(ob) + "\n\n" + cut(json.dumps(
            {"geometry": feat.get("geometry"), "properties": keep}, indent=1)))
        macros["ObsRows"] = sum(len(json.loads(q.read_text(encoding="utf-8")).get("features", []))
                                for q in (last / "nws_observations").glob("*.json")
                                if not q.name.endswith(".meta.json"))
    al = last / "nws_alerts" / "active_TX.json"
    if al.exists():
        data = json.loads(al.read_text(encoding="utf-8"))
        feats = data.get("features", [])
        macros["AlertsActive"] = len(feats)
        if feats:
            p = feats[0]["properties"]
            keep = {k: p.get(k) for k in ["id", "event", "messageType", "status", "severity", "sent",
                                          "onset", "expires", "areaDesc", "references"]}
            body = json.dumps({"geometry": "null" if feats[0].get("geometry") is None else "...",
                               "properties": keep}, indent=1)
        else:
            body = json.dumps({"type": "FeatureCollection", "features": [],
                               "title": data.get("title"), "updated": data.get("updated")}, indent=1)
        write("alerts_sample.txt", meta_text(al) + "\n\n" + cut(body))
cmp = S / "polls" / "poll_comparison.md"
if cmp.exists():
    write("poll_comparison.txt", cut(cmp.read_text(encoding="utf-8"), width=110, max_lines=60))

# --- Notebook outputs -----------------------------------------------------
dq = S / "data_quality_findings.csv"
if dq.exists():
    with dq.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    body = ["{\\small", "\\begin{longtable}{@{}>{\\raggedright}p{0.4cm}>{\\raggedright}p{2.5cm}>{\\raggedright}p{4.2cm}>{\\raggedright}p{1.5cm}p{6.2cm}@{}}",
            "\\caption{Data-quality problems found in the real samples (generated by the profiling notebook).}\\label{tab:dq}\\\\",
            "\\toprule \\# & Source & Problem & Affected & Example record / design impact\\\\ \\midrule \\endhead"]
    for i, r in enumerate(rows, 1):
        body.append(f"{i} & \\texttt{{{tex(r['source']).replace(chr(92)+'_', chr(92)+'_'+chr(92)+'allowbreak ')}}} & {tex(r['problem'])} & {tex(r['affected'])} & "
                    f"\\textit{{{tex(r['example'])}}}. {tex(r['impact_on_design'])}.\\\\")
    body += ["\\bottomrule", "\\end{longtable}", "}"]
    write("dq_table.tex", "\n".join(body))
    macros["DqCount"] = len(rows)
ov = S / "overlap_candidates.csv"
if ov.exists():
    with ov.open(encoding="utf-8") as fh:
        all_rows = list(csv.DictReader(fh))
    # one pair per station-day so the table shows distinct real-world events
    seen, rows = set(), []
    for r in all_rows:
        k = (r["ghcnd_station"], r["ghcnd_date"])
        if k not in seen:
            seen.add(k)
            rows.append(r)
    macros["OverlapDays"] = len(seen)
    rows = rows[:8]
    body = ["\\begin{table}[H]\\centering\\small",
            "\\caption{Candidate pairs: Storm Events record vs.\\ GHCN-Daily station-day (nearest station within 60~km, $\\geq$25~mm); strongest pair per station-day.}\\label{tab:overlap}",
            "\\resizebox{\\textwidth}{!}{\\begin{tabular}{@{}llllrrr@{}}\\toprule",
            "Storm EVENT\\_ID & Type & County & Begin (local) & Station / date & PRCP (mm) & Dist. (km)\\\\ \\midrule"]
    for r in rows:
        body.append(f"{tex(r['storm_event_id'])} & {tex(r['event_type'])} & {tex(r['county'])} & "
                    f"{tex(r['begin_local'])} & {tex(r['ghcnd_station'])} {tex(r['ghcnd_date'])} & "
                    f"{float(r['prcp_mm']):.1f} & {float(r['distance_km']):.1f}\\\\")
    body += ["\\bottomrule\\end{tabular}}\\end{table}"]
    write("overlap_table.tex", "\n".join(body))
    macros["OverlapCount"] = sum(1 for _ in ov.open(encoding="utf-8")) - 1

write("macros.tex", "\n".join(f"\\renewcommand{{\\{k}}}{{{v}}}" for k, v in macros.items()) + "\n")
