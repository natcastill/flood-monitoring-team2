"""Build the evidence tables and figures of the Workshop 3 report from the real data lake.

    python scripts/make_w3_report_assets.py
Writes workshop_3/report/generated/*.tex|*.txt and workshop_3/report/figures/*.png
(included by workshop_3/report/main.tex). Run the notebooks 03 and 04 first: their figures
in notebooks/figures/ are copied as well.
"""
from __future__ import annotations

import collections
import json
import re
import shutil
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from flood_monitor.text import ENGLISH_STOP_WORDS  # noqa: E402

BRONZE, SILVER, GOLD = ROOT / "datalake_bronze", ROOT / "datalake_silver", ROOT / "datalake_gold"
REPORT = ROOT / "workshop_3" / "report"
OUT, FIGS = REPORT / "generated", REPORT / "figures"
OUT.mkdir(parents=True, exist_ok=True)
FIGS.mkdir(parents=True, exist_ok=True)
TOPICS = ("nws_observations", "nws_alerts", "daily_precipitation", "flood_events")


def tex(s) -> str:
    s = "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s)
    rep = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#",
           "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(rep.get(c, c) for c in s)


def num(v, d=2) -> str:
    if v is None or pd.isna(v):
        return "--"
    if float(v).is_integer() and abs(v) < 1e7:
        return f"{int(v):,}"
    return f"{v:,.{d}f}"


def write(name, text):
    (OUT / name).write_text(text, encoding="utf-8")
    print("wrote", OUT / name)


def table(caption, label, spec, header, rows, size="footnotesize"):
    body = [f"\\begin{{table}}[H]\\centering\\{size}", f"\\caption{{{caption}}}\\label{{{label}}}",
            f"\\begin{{tabular}}{{@{{}}{spec}@{{}}}}\\toprule", " & ".join(header) + "\\\\\\midrule"]
    body += [" & ".join(r) + "\\\\" for r in rows]
    body += ["\\bottomrule\\end{tabular}\\end{table}"]
    return "\n".join(body)


gov_file = sorted(GOLD.glob("governance_*.parquet"))[-1]
story_file = sorted(GOLD.glob("storytelling_*.parquet"))[-1]
gov, story = pd.read_parquet(gov_file), pd.read_parquet(story_file)

# ---------------------------------------------------------------- data-lake inventory
rows = []
for layer, folder, pattern in (("Bronze", BRONZE, "texas_flood_sources_*.json"),):
    for f in sorted(folder.glob(pattern)):
        rows.append([layer, f"\\texttt{{{tex(f.name)}}}", num(f.stat().st_size / 1e6, 2)])
for t in TOPICS:
    files = sorted(SILVER.glob(f"{t}_*.parquet"))
    n = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    rows.append(["Silver", f"\\texttt{{{tex(t)}\\_*.parquet}} ({len(files)} files, {n:,} rows)",
                 num(sum(f.stat().st_size for f in files) / 1e6, 2)])
for f in (gov_file, story_file):
    rows.append(["Gold", f"\\texttt{{{tex(f.name)}}} ({pq.ParquetFile(f).metadata.num_rows:,} rows)",
                 num(f.stat().st_size / 1e6, 3)])
write("inventory.tex", table("Data-lake files used in this report (Bronze runs of 16--17 November 2026, "
                             "Silver v0.3 rebuilt from them, one Gold run).", "tab:inventory",
                             "llr", ["Layer", "File(s)", "MB"], rows))

# ---------------------------------------------------------------- keywords before / after
storm_raw = pd.DataFrame(json.loads(sorted(BRONZE.glob("texas_flood_sources_*.json"))[0].read_text(encoding="utf-8"))
                         ["storm_events_details"]["records"])
ev, ep = storm_raw.EVENT_NARRATIVE.fillna(""), storm_raw.EPISODE_NARRATIVE.fillna("")


def v02_tokens(text):
    t = re.sub(r"[^a-z0-9\s]", " ", text.lower().replace("...", " "))
    return [w for w in t.split() if w not in ENGLISH_STOP_WORDS and len(w) > 1 and not w.isdigit()]


before = collections.Counter(w for t in (ev + " " + ep) for w in set(v02_tokens(t))).most_common(12)
uni = story[(story.section == "keywords") & (story.metric == "top_unigrams")].nsmallest(12, "rank")
bi = story[(story.section == "keywords") & (story.metric == "top_bigrams")].nsmallest(12, "rank")
rows = [[str(i + 1), tex(b[0]), num(b[1]), tex(u), num(uc), tex(bb), num(bc)]
        for i, (b, u, uc, bb, bc) in enumerate(zip(before, uni.term, uni["count"], bi.term, bi["count"]))]
write("keywords_before_after.tex", table(
    "Top keywords before (Workshop 2 rule: event + episode narrative, English stop words) and after "
    "(Workshop 3: event narrative, domain stop words, boilerplate removed, Spark ML pipeline). "
    "Counts are events that mention the term (8,000 events).", "tab:kw",
    "rlrlrlr", ["\\#", "W2 term", "events", "W3 unigram", "events", "W3 bigram", "events"], rows))

# ---------------------------------------------------------------- governance KPIs (current values)
g = gov
def gv(name, topic=None, field=None, grain="all"):
    q = g[(g.kpi_name == name) & (g.period_grain == grain)]
    if topic:
        q = q[q.topic == topic]
    if field:
        q = q[q.field == field]
    return q
rows = []
for t in TOPICS:
    rows.append([f"\\texttt{{{tex(t)}}}",
                 num(gv("records_total", t).value.iloc[0], 0),
                 num(gv("silver_files", t).value.iloc[0], 0),
                 num(gv("duplicate_rate_before_dedup", t).value.iloc[0]),
                 num(gv("duplicate_rate_after_dedup", t).value.iloc[0]),
                 num(gv("schema_compliance_files", t).value.iloc[0]),
                 num(gv("schema_compliance_records", t).value.iloc[0]),
                 num(gv("record_lag_hours", t).value.iloc[0], 1)])
write("gov_topic_kpis.tex", table(
    f"Governance KPIs per Silver topic (\\texttt{{{tex(gov_file.name)}}}). Dup.\\ = duplicate rate before / after "
    "de-duplication; Schema = schema compliance of files / records; Lag = hours since the newest record.",
    "tab:govtopic", "lrrrrrrr",
    ["Topic", "Records", "Files", "Dup.\\ bef.\\ \\%", "Dup.\\ aft.\\ \\%", "Schema f.\\ \\%",
     "Schema r.\\ \\%", "Lag h"], rows, size="scriptsize"))

nul = g[(g.kpi_name == "null_rate") & (g.value > 0)].sort_values("value", ascending=False)
rows = [[f"\\texttt{{{tex(r.topic)}}}", f"\\texttt{{{tex(r.field)}}}", num(r.value), tex(r.note or "")]
        for r in nul.itertuples()]
sen = g[(g.kpi_name == "sentinel_rate")].sort_values("value", ascending=False)
rows += [[f"\\texttt{{{tex(r.topic)}}}", f"\\texttt{{{tex(r.field)}}} (sentinel)", num(r.value),
          "value replaced by unknown/none/MISSING"] for r in sen.itertuples()]
write("gov_nulls.tex", table("Null-rate and sentinel-rate KPIs above zero (required fields are all 0\\,\\%).",
                             "tab:govnull", "llrp{6.2cm}", ["Topic", "Field", "\\%", "Explanation"], rows,
                             size="scriptsize"))

oth = g[g.kpi_name.isin(["outlier_rate", "encoding_repaired_rate", "precip_source_share", "maintenance_flag_rate",
                         "station_day_completeness", "ingestion_lag_hours", "language_distribution"])]
rows = [[tex(r.kpi_name), f"\\texttt{{{tex(r.topic)}}}", tex(r.field or ""), num(r.value),
         num(r.numerator, 0), num(r.denominator, 0), tex(r.status or "")] for r in oth.itertuples()]
write("gov_other.tex", table("Other governance KPIs of the Gold run (validity, text, accuracy, timeliness).",
                             "tab:govother", "lllrrrl", ["KPI", "Topic", "Field", "Value", "Num.", "Den.", "Status"],
                             rows, size="scriptsize"))

ing = g[g.kpi_name == "ingestion_frequency_compliance"].pivot_table(index="topic", columns="period_start",
                                                                       values="numerator")
exp = {"bronze_runs": 96, "nws_alerts": 96, "storm_events_catalog": 96, "nws_observations": 24, "ghcnd": 1,
       "storm_events_details": 1}
rows = [[f"\\texttt{{{tex(t)}}}", str(exp.get(t, ""))] + [num(v, 0) for v in r] for t, r in ing.iterrows()]
write("gov_ingestion.tex", table("Ingestion frequency compliance: successful pulls per UTC day against the "
                                 "contract (the stack only ran while the team was working).", "tab:goving",
                                 "lr" + "r" * len(ing.columns),
                                 ["Source", "Expected/day"] + [str(c) for c in ing.columns], rows))

# ---------------------------------------------------------------- storytelling samples
s = story
fri = s[s.section == "flood_risk_index"].pivot_table(index=["county", "station_id", "label"], columns="metric",
                                                      values="value").reset_index()
day = s[(s.section == "flood_risk_index") & (s.metric == "fri_score")].set_index("county").period_start
rows = [[tex(r.county), r.station_id, str(day[r.county]), num(r.input_R24_mm, 1), num(r.input_R7d_mm, 1),
         num(r.input_P24_percentile, 1), num(r.input_alert_level, 0), num(r.input_floods_per_year, 1),
         num(r.input_top_quartile, 0), num(r.input_rain_level, 0), num(r.fri_score, 0), f"\\textbf{{{r.label}}}"]
        for r in fri.itertuples()]
write("fri.tex", table("Flood Risk Index per county in the Gold storytelling summary (latest GHCN-Daily day).",
                       "tab:fri", "llcrrrrrrrrl",
                       ["County", "Station", "Day", "$R_{24}$", "$R_{7d}$", "$P_{24}$", "$A$", "$F$/yr",
                        "TopQ", "Rain lvl", "Score", "FRI"], rows, size="scriptsize"))

bt = s[(s.section == "fri_backtest") & (s.county == "ALL")].pivot_table(index=["term", "category"], columns="metric",
                                                                         values="value").reset_index()
cnt = s[(s.section == "fri_backtest") & (s.county == "ALL")].pivot_table(index=["term", "category"],
                                                                          columns="metric", values="count").reset_index()
rows = [[tex(r.term.split(":")[0]), tex(r.category), num(c.hit_rate_pct, 0), num(r.hit_rate_pct), num(c.false_alarm_rate_pct, 0),
         num(r.false_alarm_rate_pct)] for r, c in zip(bt.itertuples(), cnt.itertuples())]
write("backtest.tex", table("Back-test of the rainfall + frequency part of the index (5 counties, county-days "
                            "2010--2026; thresholds from 2010--2020). W1 rule: alarm when rain level + top-quartile term $\\geq$ 2; "
                            "rain-only rule: alarm when rain level $\\geq$ 2.", "tab:backtest", "llrrrr",
                            ["Rule", "Period", "Flood days", "Hit rate \\%", "Alarm days", "False alarms \\%"], rows))

nar = s[s.section == "narrative"].sort_values("metric")
write("narrative.tex", "\\begin{enumerate}\n" + "\n".join(f"\\item {tex(l)}" for l in nar.label) + "\n\\end{enumerate}")

head = s[s.section == "headline"].set_index("metric")
macros = {
    "EventsTotal": num(head.loc["flood_events_total", "value"], 0),
    "EventsPerYear": num(head.loc["flood_events_per_year", "value"], 1),
    "FlashShare": num(head.loc["flash_flood_share_pct", "value"], 1),
    "DamageBn": f"{head.loc['property_damage_usd', 'value'] / 1e9:,.1f}",
    "Deaths": num(head.loc["deaths_total", "value"], 0),
    "GovRows": f"{len(gov):,}", "StoryRows": f"{len(story):,}",
    "GovFile": tex(gov_file.name), "StoryFile": tex(story_file.name),
}
write("macros.tex", "\n".join(f"\\newcommand{{\\{k}}}{{{v}}}" for k, v in macros.items()))

# ---------------------------------------------------------------- "screenshots" of the Gold Parquet content
def render(df, path, title, col_width=None):
    fig, ax = plt.subplots(figsize=(11, 0.32 * (len(df) + 2)))
    ax.axis("off")
    tbl = ax.table(cellText=df.astype(str).values, colLabels=list(df.columns), loc="upper left",
                   cellLoc="left", colLoc="left", colWidths=col_width)
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7)
    tbl.scale(1, 1.25)
    tbl.auto_set_column_width(col=list(range(len(df.columns))))
    for (r, _), cell in tbl.get_celld().items():
        cell.set_edgecolor("#DDDDDD")
        if r == 0:
            cell.set_facecolor("#EEF2F7")
            cell.set_text_props(weight="bold")
    ax.set_title(title, loc="left", fontsize=8, family="monospace")
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


gs = pd.concat([gov[gov.kpi_name == k].head(n) for k, n in (
    ("null_rate", 3), ("duplicate_rate_before_dedup", 2), ("schema_compliance_files", 2), ("outlier_rate", 2),
    ("text_length_words", 2), ("ingestion_lag_hours", 2), ("record_lag_hours", 2))])
gs = gs.drop(columns=["gold_run_ts", "period_start"]).copy()
gs["value"] = gs["value"].round(3)
gs["note"] = gs["note"].fillna("").str.slice(0, 38)
render(gs.fillna(""), FIGS / "gold_governance_content.png",
       f">>> pd.read_parquet('datalake_gold/{gov_file.name}')  # {len(gov)} rows x {gov.shape[1]} columns (sample)")
ss = pd.concat([story[(story.section == sec) & (story.metric == m)].head(n) for sec, m, n in (
    ("headline", "flood_events_total", 1), ("severity_distribution", "flood_events_by_severity", 2),
    ("county_history", "floods_per_year", 2), ("keywords", "top_bigrams", 2), ("current_rainfall", "rain_7d_mm", 2),
    ("flood_risk_index", "fri_score", 2), ("fri_backtest", "hit_rate_pct", 1), ("narrative", "1_where", 1))])
ss = ss.drop(columns=["gold_run_ts", "latitude", "longitude", "period_grain", "pct"]).copy()
ss["value"] = ss["value"].round(3)
ss["label"] = ss["label"].fillna("").str.slice(0, 34)
ss["term"] = ss["term"].fillna("").str.slice(0, 22)
render(ss.fillna(""), FIGS / "gold_storytelling_content.png",
       f">>> pd.read_parquet('datalake_gold/{story_file.name}')  # {len(story)} rows x {story.shape[1]} columns (sample)")

for f in (ROOT / "notebooks" / "figures").glob("w3_*.png"):
    shutil.copy(f, FIGS / f.name)
    print("copied", f.name)
last = json.loads((GOLD / "_gold_manifest.jsonl").read_text().splitlines()[-1])
last["silver_inputs"] = {t: f"{len(v)} files, e.g. {v[-1]}" for t, v in last["silver_inputs"].items()}
(OUT / "gold_manifest.txt").write_text(json.dumps(last, indent=1), encoding="utf-8")
