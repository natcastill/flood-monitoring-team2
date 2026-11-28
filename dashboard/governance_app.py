"""Governance Dashboard - Plotly Dash (Workshop 4).

Audience: technical reviewer / data engineer.
Question: "Can I trust the data of the pipeline right now?"
Port:     8050.
Data:     ONLY the newest datalake_gold/governance_*.parquet (pd.read_parquet).

Run from the repository root:
    python -m dashboard.governance_app
"""
from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, State, dash_table, dcc, html

from . import components as ui
from . import theme
from .data_loader import (gov_freshness, gov_null_rates, gov_outlier_rates, gov_status_distribution,
                          gov_summary_cards, gov_summary_table, gov_volume, latest_gold_file, load_governance)

SOURCE = "datalake_gold/governance_*.parquet (Gold layer)"
TOPIC_NAMES = {"nws_observations": "NWS observations (API)", "nws_alerts": "NWS flood alerts (API)",
               "daily_precipitation": "GHCN-Daily rainfall (API)", "flood_events": "NOAA Storm Events (scraping)"}

app = dash.Dash(__name__, title="Flood Monitoring · Governance Dashboard", update_title=None,
                suppress_callback_exceptions=True)  # Bootstrap (Flatly) served from dashboard/assets/
app.index_string = ui.INDEX_STRING


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------
def fig_null_rates(df, topic="all") -> go.Figure:
    d = gov_null_rates(df, topic)
    fig = go.Figure()
    for kind, color in (("required field", theme.VERMILLION), ("optional / expected", theme.LIGHT_BLUE)):
        sub = d[d["kind"] == kind]
        if sub.empty:
            continue
        fig.add_trace(go.Bar(x=sub["value"], y=sub["label"], orientation="h", name=kind, marker_color=color,
                             text=[f"{v:.1f}%" for v in sub["value"]], textposition="outside",
                             customdata=sub["note"],
                             hovertemplate="<b>%{y}</b><br>Null rate: %{x:.2f}%<br>%{customdata}<extra></extra>"))
    if d.empty:
        fig.add_annotation(text="No missing values for this source.", showarrow=False,
                           font=dict(size=14, color=theme.GREY))
    fig.update_layout(**theme.CHART_LAYOUT, height=440, barmode="overlay",
                      xaxis=dict(title="Null or empty values (% of records)", range=[0, 115]),
                      yaxis=dict(title="", categoryorder="array", categoryarray=list(d["label"])),
                      legend=dict(orientation="h", y=1.08, x=0))
    return fig


def fig_status(df) -> go.Figure:
    d = gov_status_distribution(df)
    fig = go.Figure(go.Bar(x=d["status"], y=d["count"], text=d["count"], textposition="outside",
                           marker_color=[theme.STATUS_COLORS[s] for s in d["status"]],
                           hovertemplate="<b>%{x}</b><br>%{y} KPI values<extra></extra>"))
    fig.update_layout(**theme.CHART_LAYOUT, height=340, showlegend=False,
                      xaxis_title="KPI status (info = no threshold defined)",
                      yaxis=dict(title="Number of KPI values", range=[0, d["count"].max() * 1.2]))
    return fig


def fig_volume(df, mode="event") -> go.Figure:
    d = gov_volume(df, mode)
    fig = go.Figure()
    for topic in TOPIC_NAMES:
        sub = d[d["topic"] == topic]
        if sub.empty:
            continue
        fig.add_trace(go.Bar(x=sub["period_start"], y=sub["value"], name=TOPIC_NAMES[topic],
                             marker_color=theme.TOPIC_COLORS.get(topic, theme.BLUE),
                             hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} records<extra>" + TOPIC_NAMES[topic] + "</extra>"))
    xtitle = "Year of the event" if mode == "event" else "Bronze ingestion day (UTC)"
    fig.update_layout(**theme.CHART_LAYOUT, height=380, barmode="group",
                      xaxis_title=xtitle, yaxis=dict(title="Records (log scale)", type="log", dtick=1),
                      legend=dict(orientation="h", y=1.1, x=0))
    if mode == "ingestion":
        fig.update_xaxes(type="category")
    return fig


def fig_outliers(df) -> go.Figure:
    d = gov_outlier_rates(df)
    fig = go.Figure(go.Bar(x=d["value"], y=d["label"], orientation="h", marker_color=theme.AMBER,
                           text=[f"{v:.2f}%" for v in d["value"]], textposition="outside",
                           customdata=list(zip(d["numerator"].fillna(0), d["denominator"].fillna(0))),
                           hovertemplate="<b>%{y}</b><br>%{x:.2f}% (%{customdata[0]:,.0f} of %{customdata[1]:,.0f})"
                                         "<extra></extra>"))
    fig.update_layout(**theme.CHART_LAYOUT, height=340,
                      xaxis=dict(title="Records flagged as outliers (%)", range=[0, max(d["value"].max() * 1.3, 1)]),
                      yaxis_title="")
    return fig


def fig_freshness(df) -> go.Figure:
    d = gov_freshness(df)
    colors = [theme.STATUS_COLORS.get(s, theme.GREY) for s in d["status"]]
    fig = go.Figure(go.Bar(x=d["value"], y=d["topic"].str.replace("_", " "), orientation="h", marker_color=colors,
                           text=[f"{v:,.1f} h" for v in d["value"]], textposition="outside",
                           customdata=d["threshold"],
                           hovertemplate="<b>%{y}</b><br>%{x:,.1f} h since last pull (limit %{customdata} h)"
                                         "<extra></extra>"))
    import math
    lo, hi = max(d["value"].min(), 0.1) / 2, d["value"].max() * 6
    fig.update_layout(**theme.CHART_LAYOUT, height=340,
                      xaxis=dict(title="Hours since the last successful pull (log scale)", type="log", dtick=1,
                                 range=[math.log10(lo), math.log10(hi)]),
                      yaxis_title="")
    return fig


def summary_table(df) -> dash_table.DataTable:
    t = gov_summary_table(df)
    cols = [("KPI group", "kpi_group"), ("KPI", "kpi_name"), ("Source", "scope"), ("Field", "field"),
            ("Value", "value"), ("Unit", "unit"), ("Threshold", "threshold"), ("Status", "status")]
    return dash_table.DataTable(
        data=t.to_dict("records"),
        columns=[{"name": n, "id": i, **({"type": "numeric"} if i in ("value", "threshold") else {})} for n, i in cols],
        filter_action="native", sort_action="native", page_size=15,
        style_table={"overflowX": "auto"},
        style_cell={"fontFamily": "Inter, Segoe UI, sans-serif", "fontSize": "12px", "padding": "6px 8px",
                    "textAlign": "left", "whiteSpace": "normal"},
        style_header={"backgroundColor": theme.BLUE, "color": "white", "fontWeight": "bold"},
        style_data_conditional=[
            {"if": {"filter_query": '{status} = "warn"'}, "backgroundColor": "#FFF4E0"},
            {"if": {"filter_query": '{status} = "fail"'}, "backgroundColor": "#FFE0E0"},
            {"if": {"filter_query": '{status} = "ok"', "column_id": "status"}, "color": theme.GREEN,
             "fontWeight": "bold"},
        ],
    )


# ---------------------------------------------------------------------------
# Layout (a function: every page load reads the newest Gold file)
# ---------------------------------------------------------------------------
def serve_layout():
    df, ts, name = load_governance()
    c = gov_summary_cards(df)
    topics = [{"label": "All sources", "value": "all"}] + [{"label": v, "value": k} for k, v in TOPIC_NAMES.items()]
    row = lambda items, mb="16px": html.Div(items, className="row g-3", style={"marginBottom": mb})
    col = lambda child, cls: html.Div(child, className=cls)
    return html.Div(
        [
            ui.header("Governance Dashboard", "Can I trust the data of the pipeline right now?", ts, name),
            *ui.refresh_watch(name),
            row([
                col(ui.kpi_card("Total records", f"{c['total_records']:,}",
                                "Silver records read by the Gold run (4 sources)", theme.BLUE), "col-md-3"),
                col(ui.kpi_card("Null rate", f"{c['null_rate_all']:.1f}%",
                                f"All fields · required fields: {c['null_rate_required']:.1f}%", theme.AMBER),
                    "col-md-3"),
                col(ui.kpi_card("Duplicate rate", f"{c['duplicate_before']:.1f}%",
                                f"Before de-dup (worst: {c['duplicate_worst'][0]} {c['duplicate_worst'][1]:.0f}%)"
                                f" · after: {c['duplicate_after']:.1f}%", theme.VERMILLION),
                    "col-md-3"),
                col(ui.kpi_card("Schema compliance", f"{c['schema_records']:.2f}%",
                                f"Records accepted · files: {c['schema_files']:.0f}%", theme.GREEN), "col-md-3"),
            ]),
            row([
                col(ui.kpi_card("KPIs passing", f"{c['ok']} / {c['graded']}",
                                f"KPIs with a threshold that pass it ({c['info']} more are informational)",
                                theme.GREEN), "col-md-4"),
                col(ui.kpi_card("KPI warnings", str(c["warn"]), "Above threshold, need attention",
                                theme.AMBER if c["warn"] else theme.GREEN), "col-md-4"),
                col(ui.kpi_card("KPI failures", str(c["fail"]), "Broken data contract (pipeline blockers)",
                                theme.VERMILLION if c["fail"] else theme.GREEN), "col-md-4"),
            ]),
            row([
                col(ui.chart_card("Null rate by field",
                                  "Red = required field (must be 0%); blue = missing by design (e.g. 5-minute "
                                  "reports carry no rainfall). Hover for the explanation.",
                                  dcc.Graph(id="chart-null", config={"displaylogo": False}), SOURCE,
                                  controls=dcc.Dropdown(id="null-topic", options=topics, value="all",
                                                        clearable=False, style={"width": "320px"})),
                    "col-lg-8"),
                col(ui.chart_card("KPI status distribution", "How many KPI values pass, warn or fail.",
                                  dcc.Graph(figure=fig_status(df), config={"displaylogo": False}), SOURCE),
                    "col-lg-4"),
            ]),
            row([
                col(ui.chart_card("Record volume over time, per source",
                                  "Switch between the year of the event and the day it was ingested.",
                                  dcc.Graph(id="chart-volume", config={"displaylogo": False}), SOURCE,
                                  controls=dcc.RadioItems(id="volume-mode", inline=True,
                                                          options=[{"label": " by event year ", "value": "event"},
                                                                   {"label": " by ingestion day", "value": "ingestion"}],
                                                          value="event", inputStyle={"marginRight": "4px",
                                                                                     "marginLeft": "10px"})),
                    "col-lg-7"),
                col(ui.chart_card("Outlier rate per numeric field",
                                  "Flagged and kept in Silver: extreme rainfall and damage are the flood signal.",
                                  dcc.Graph(figure=fig_outliers(df), config={"displaylogo": False}), SOURCE),
                    "col-lg-5"),
            ]),
            row([
                col(ui.chart_card("Source freshness",
                                  "Hours since each source was last pulled successfully; color = status against "
                                  "the ingestion contract (US5).",
                                  dcc.Graph(figure=fig_freshness(df), config={"displaylogo": False}), SOURCE),
                    "col-lg-5"),
                col(html.Div([html.H5("Data quality summary table", style={"marginBottom": "2px"}),
                              html.P("Every KPI with its current value and threshold. Type in the header row to "
                                     "filter (e.g. warn); click a column to sort.",
                                     style={"color": theme.GREY, "fontSize": "12.5px"}),
                              summary_table(df),
                              html.P(f"Source: {SOURCE}", style={"color": theme.GREY, "fontSize": "11px",
                                                                 "textAlign": "right", "marginTop": "6px"})],
                             style={**theme.CARD_STYLE, "padding": "16px 20px", "background": "white"}),
                    "col-lg-7"),
            ]),
            html.P("Governance Dashboard · Workshop 4 · Data Analysis Programming · Universidad Distrital "
                   "Francisco José de Caldas", style={"textAlign": "center", "color": theme.GREY,
                                                      "fontSize": "11px", "marginTop": "16px"}),
        ],
        style={"maxWidth": "1600px", "margin": "0 auto", "padding": "20px", "backgroundColor": theme.LIGHT_BG},
    )


app.layout = serve_layout
ui.register_refresh_callback(app, "governance", latest_gold_file)


@app.callback(Output("chart-null", "figure"), Input("null-topic", "value"), State("gold-file", "data"))
def _update_nulls(topic, gold_file):
    df, _, _ = load_governance(gold_file)
    return fig_null_rates(df, topic)


@app.callback(Output("chart-volume", "figure"), Input("volume-mode", "value"), State("gold-file", "data"))
def _update_volume(mode, gold_file):
    df, _, _ = load_governance(gold_file)
    return fig_volume(df, mode)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
