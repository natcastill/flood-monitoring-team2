"""Build the evidence tables of the Workshop 2 report from the real data lake.

    python scripts/make_w2_report_assets.py
Writes workshop_2/report/generated/*.tex|*.txt (included by workshop_2/report/main.tex).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
BRONZE, SILVER = ROOT / "datalake_bronze", ROOT / "datalake_silver"
OUT = ROOT / "workshop_2" / "report" / "generated"
OUT.mkdir(parents=True, exist_ok=True)
SECTIONS = ("nws_observations", "nws_alerts", "storm_events_catalog", "storm_events_details", "ghcnd")


def tex(s) -> str:
    s = "" if s is None else str(s)
    rep = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#",
           "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(rep.get(c, c) for c in s)


def write(name, text):
    (OUT / name).write_text(text, encoding="utf-8")
    print("wrote", OUT / name)


def topic_of(path: Path) -> str:
    return path.name.split("_20")[0]


# ---------------------------------------------------------------- Bronze inventory
rows = []
for f in sorted(BRONZE.glob("texas_flood_sources_*.json")):
    d = json.loads(f.read_text(encoding="utf-8"))
    val = d["metadata"].get("validation", {})
    counts = [str(val.get(s, {}).get("records", "--")) for s in SECTIONS]
    rows.append((f.name.replace("texas_flood_sources_", "").replace(".json", ""), counts, f.stat().st_size / 1e6))
body = ["\\begin{table}[H]\\centering\\footnotesize",
        "\\caption{Bronze files produced by \\texttt{bronze\\_flood\\_ingestion\\_dag} (records per section; -- = not due in that run).}\\label{tab:bronze}",
        "\\begin{tabular}{@{}lrrrrrr@{}}\\toprule",
        "\\texttt{texas\\_flood\\_sources\\_}\\ldots & Observations & Alerts & Catalog & Storm Events & GHCN-Daily & MB\\\\\\midrule"]
body += [f"\\texttt{{{tex(n)}}} & {' & '.join(c)} & {s:.1f}\\\\" for n, c, s in rows]
body += ["\\bottomrule\\end{tabular}\\end{table}"]
write("bronze_inventory.tex", "\n".join(body))

# ---------------------------------------------------------------- Silver inventory
rows = []
for f in sorted(SILVER.glob("*.parquet")):
    md = pq.read_metadata(f).metadata
    st = json.loads(md[b"preprocessing_stats"])
    proc, bronze = f.stem.split("_from_")
    rows.append((topic_of(f), proc[-15:], bronze.replace("texas_flood_sources_", ""),
                 pq.ParquetFile(f).metadata.num_rows, st.get("already_in_silver", 0), f.stat().st_size / 1024))
body = ["\\begin{table}[H]\\centering\\footnotesize",
        "\\caption{Silver Parquet files (\\texttt{<topic>\\_<processing time>\\_from\\_<Bronze file>.parquet}).}\\label{tab:silver}",
        "\\begin{tabular}{@{}llllrrr@{}}\\toprule",
        "Topic & Processed (UTC) & From Bronze & Rows & Skipped (already in Silver) & KB\\\\\\midrule"]
body += [f"\\texttt{{{tex(t)}}} & {tex(p)} & {tex(b)} & {n:,} & {sk:,} & {kb:,.0f}\\\\" for t, p, b, n, sk, kb in rows]
body += ["\\bottomrule\\end{tabular}\\end{table}"]
write("silver_inventory.tex", "\n".join(body))

# manifest: files that produced nothing new (idempotency evidence)
man = [json.loads(l) for l in (SILVER / "_manifest.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
lines = []
for m in man:
    parts = []
    for t, o in m["outputs"].items():
        if o.get("rows_in"):
            parts.append(f"{t}: {o.get('rows_written', 0)} written, {o.get('already_in_silver', 0)} skipped")
    lines.append(f"{m['bronze_file']}  ->  " + ("; ".join(parts) or "no records"))
write("manifest.txt", "\n".join(lines))

# ---------------------------------------------------------------- samples
first = sorted(BRONZE.glob("texas_flood_sources_*.json"))[-1]
d = json.loads(first.read_text(encoding="utf-8"))
flood = [x for x in d["nws_alerts"] if "Flood" in x["properties"].get("event", "")]
a = (flood or d["nws_alerts"] or [{"properties": {}}])[0]["properties"]
sample = {
    "metadata": {k: d["metadata"][k] for k in ("ingestion_timestamp_utc", "run_id", "window_start", "window_end")},
    "nws_observations": "{KIAH: [...], KAUS: [...], ...}" if d["nws_observations"] else None,
    "nws_alerts": [{k: a.get(k) for k in ("id", "event", "messageType", "sent", "areaDesc")}, "..."],
    "storm_events_catalog": d["storm_events_catalog"][-1:] + ["..."],
    "storm_events_details": "(daily runs only)" if d["storm_events_details"] is None else "{files, records}",
    "ghcnd": "(daily runs only)" if d["ghcnd"] is None else f"[{len(d['ghcnd'])} records]",
}
sample["metadata"]["validation"] = {k: (v.get("records", v.get("status")) if isinstance(v, dict) else v)
                                    for k, v in d["metadata"]["validation"].items()}
write("bronze_sample.txt", f"// {first.name} (trimmed)\n" + json.dumps(sample, indent=1)[:3200])

def load(topic):
    fs = sorted(SILVER.glob(f"{topic}_*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True) if fs else pd.DataFrame()

ev, pr, ob, al = load("flood_events"), load("daily_precipitation"), load("nws_observations"), load("nws_alerts")
top = ev.sort_values("damage_property_usd", ascending=False).head(5)
body = ["\\begin{table}[H]\\centering\\scriptsize",
        "\\caption{Silver \\texttt{flood\\_events}: the five records with the largest property damage (read with Pandas).}\\label{tab:silversample}",
        "\\resizebox{\\textwidth}{!}{\\begin{tabular}{@{}lllllrl@{}}\\toprule",
        "storm\\_event\\_id & event\\_type & begin\\_utc & county & damage (USD) & sev. & impact\\_terms\\\\\\midrule"]
for r in top.itertuples():
    body.append(f"{r.storm_event_id} & {tex(r.event_type)} & {r.begin_utc:%Y-%m-%d %H:%M} & {tex(r.county)} & "
                f"{r.damage_property_usd:,.0f} & {r.severity_level} & {tex(', '.join(r.impact_terms))}\\\\")
body += ["\\bottomrule\\end{tabular}}\\end{table}"]
write("silver_sample.tex", "\n".join(body))

top = pr.sort_values("prcp_mm", ascending=False).head(5)
body = ["\\begin{table}[H]\\centering\\small",
        "\\caption{Silver \\texttt{daily\\_precipitation}: wettest station-days, flagged but kept.}\\label{tab:wettest}",
        "\\begin{tabular}{@{}llrccc@{}}\\toprule Station & Date & PRCP (mm) & IQR flag & $z$ & Z flag\\\\\\midrule"]
for r in top.itertuples():
    body.append(f"{tex(r.station_id)} ({tex(r.ghcn_station_id)}) & {r.obs_date} & {r.prcp_mm:.1f} & "
                f"{'yes' if r.is_outlier_iqr else 'no'} & {r.prcp_zscore:.2f} & {'yes' if r.is_outlier_z else 'no'}\\\\")
body += ["\\bottomrule\\end{tabular}\\end{table}"]
write("wettest.tex", "\n".join(body))

terms = ev.explode("impact_terms").impact_terms.value_counts().head(6)
macros = {
    "BronzeFiles": len(list(BRONZE.glob("texas_flood_sources_*.json"))),
    "SilverFiles": len(list(SILVER.glob("*.parquet"))),
    "BronzeMB": f"{sum(f.stat().st_size for f in BRONZE.glob('*.json')) / 1e6:.1f}",
    "SilverMB": f"{sum(f.stat().st_size for f in SILVER.glob('*.parquet')) / 1e6:.1f}",
    "EventsRows": f"{len(ev):,}".replace(",", "{,}"), "PrecipRows": f"{len(pr):,}".replace(",", "{,}"),
    "ObsRows": f"{len(ob):,}".replace(",", "{,}"), "AlertRows": f"{len(al):,}".replace(",", "{,}"),
    "PrecipMissingPct": f"{ob['precip_missing'].mean() * 100:.0f}" if len(ob) else "0",
    "TopTerms": tex(", ".join(f"{t} ({n:,})" for t, n in terms.items())),
}
write("macros.tex", "\n".join(f"\\renewcommand{{\\{k}}}{{{v}}}" for k, v in macros.items()) + "\n")
