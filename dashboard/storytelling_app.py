"""Storytelling Dashboard - Plotly Dash (Workshop 4).

Audience: the emergency-management analyst of Workshop 1 (functional user).
Story:    "Is today unusual?" first (US1-US4), then "where, when and what do Texas floods do?" (US6).
Port:     8051.
Data:     ONLY the newest datalake_gold/storytelling_*.parquet (pd.read_parquet).

The template of the workshop is about public-opinion sentiment; in this project the "sentiment"
of a record is its flood severity (0 none, 1 minor, 2 moderate, 3 severe; Workshop 1), with the
same color code: green = minor, amber = moderate, red = severe.

Run from the repository root:
    python -m dashboard.storytelling_app
"""
from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html
from plotly.subplots import make_subplots

from . import components as ui
from . import theme
from .data_loader import (latest_gold_file, load_storytelling, st_alerts, st_current_rainfall, st_flood_risk,
                          st_headline, st_keywords, st_narrative, st_severity_distribution, st_source_comparison,
                          st_top_counties, st_trend_monthly)

SOURCE = "datalake_gold/storytelling_*.parquet (Gold layer) · NOAA Storm Events, NWS, GHCN-Daily"
STATION_PLACE = {"KIAH": "Houston", "KAUS": "Austin", "KSAT": "San Antonio", "KDFW": "Dallas-Fort Worth",
                 "KCRP": "Corpus Christi"}

app = dash.Dash(__name__, title="Flood Monitoring · Storytelling Dashboard", update_title=None,
                suppress_callback_exceptions=True)  # Bootstrap (Flatly) served from dashboard/assets/
app.index_string = ui.INDEX_STRING
CFG = {"displaylogo": False}


# ---------------------------------------------------------------------------
# "Is today unusual?" (US1-US4)
# ---------------------------------------------------------------------------
def fri_cards(df) -> list:
    fri = st_flood_risk(df)
    explain = {"HIGH": "Unusually heavy rain for this place and month: flooding is likely.",
               "MEDIUM": "Wetter than 19 of 20 days of this month, or 100+ mm in a week: watch.",
               "LOW": "Rainfall within the normal range for this month."}
    cards = []
    for r in fri.itertuples():
        color = theme.FRI_COLORS.get(r.label, theme.GREY)
        cards.append(html.Div(html.Div(
            [
                html.Div(f"{r.county} County · {STATION_PLACE.get(r.station_id, r.station_id)}",
                         style={"fontSize": "12px", "color": theme.GREY, "textTransform": "uppercase"}),
                html.Div(r.label, style={"fontSize": "26px", "fontWeight": 800, "color": color}),
                html.Div(explain[r.label], style={"fontSize": "12.5px", "marginBottom": "6px"}),
                html.Div(f"Last day: {r.input_R24_mm:.1f} mm (higher than {r.input_P24_percentile:.0f}% of "
                         f"same-month days) · last 7 days: {r.input_R7d_mm:.0f} mm · alert level: "
                         f"{int(r.input_alert_level)}", style={"fontSize": "11.5px", "color": theme.GREY}),
            ],
            style={**theme.CARD_STYLE, "padding": "14px 16px", "height": "100%", "background": "white",
                   "borderTop": f"5px solid {color}"}), className="col"))
    return cards


def fig_rain_percentile(df) -> go.Figure:
    d = st_current_rainfall(df)
    d["place"] = d["station_id"].map(STATION_PLACE).fillna(d["station_id"])
    fig = go.Figure()
    for col, name, color in (("percentile_1d_vs_same_month", "Last day", theme.LIGHT_BLUE),
                             ("percentile_7d_vs_same_month", "Last 7 days", theme.BLUE)):
        mm = d["rain_1d_mm"] if col.endswith("1d_vs_same_month") else d["rain_7d_mm"]
        fig.add_trace(go.Bar(x=d["place"], y=d[col], name=name, marker_color=color,
                             customdata=list(zip(mm, d["period_start"].astype(str))),
                             hovertemplate="<b>%{x}</b> · " + name + "<br>%{customdata[0]:.1f} mm on %{customdata[1]}"
                                           "<br>wetter than %{y:.0f}% of past same-month values<extra></extra>"))
    fig.add_hline(y=95, line_dash="dash", line_color=theme.VERMILLION,
                  annotation_text="95th percentile: unusual", annotation_position="top right",
                  annotation_font_size=11)
    fig.update_layout(**theme.CHART_LAYOUT, height=330, barmode="group",
                      yaxis=dict(title="Percentile vs. same month (%)", range=[0, 110]),
                      xaxis_title="Weather station", legend=dict(orientation="h", y=1.12, x=0))
    return fig


def alerts_panel(df) -> html.Div:
    active, last7 = st_alerts(df)
    if active.empty and last7.empty:
        body = [html.P("No NWS flood watches, warnings or advisories in Texas in the last 7 days.",
                       style={"fontSize": "15px", "color": theme.GREEN, "fontWeight": 600}),
                html.P("Alerts appear here as soon as the National Weather Service issues one.",
                       style={"color": theme.GREY, "fontSize": "12.5px"})]
    else:
        body = []
        if not active.empty:
            body.append(html.Ul([html.Li(f"{r.category} · {r.county or 'zone'} County · {r.label}")
                                 for r in active.itertuples()], style={"fontSize": "13px"}))
        if not last7.empty:
            body.append(html.P("Last 7 days: " + ", ".join(f"{int(r.count)} {r.category}"
                                                           for r in last7.itertuples()),
                               style={"fontSize": "13px"}))
    return html.Div([html.H5("Flood alerts in effect", style={"marginBottom": "6px"}), *body,
                     html.P("Source: Gold storytelling summary, section alerts (NWS API)",
                            style={"color": theme.GREY, "fontSize": "11px", "textAlign": "right"})],
                    style={**theme.CARD_STYLE, "padding": "16px 20px", "height": "100%", "background": "white"})


# ---------------------------------------------------------------------------
# History charts (US6)
# ---------------------------------------------------------------------------
def fig_severity(df) -> go.Figure:
    d = st_severity_distribution(df)
    fig = go.Figure(go.Pie(labels=[c.capitalize() for c in d["category"]], values=d["count"], hole=0.55, sort=False,
                           marker=dict(colors=[theme.SEVERITY_COLORS.get(c, theme.GREY) for c in d["category"]],
                                       line=dict(color="white", width=2)),
                           textinfo="label+percent",
                           hovertemplate="<b>%{label}</b><br>%{value:,.0f} flood events (%{percent})<extra></extra>"))
    fig.update_layout(**theme.CHART_LAYOUT, height=360, legend=dict(orientation="h", y=-0.08, x=0.5,
                                                                     xanchor="center"),
                      annotations=[dict(text=f"<b>{int(d['count'].sum()):,}</b><br>events", x=0.5, y=0.5,
                                        showarrow=False, font=dict(size=16))])
    return fig


def fig_sources(df) -> go.Figure:
    d = st_source_comparison(df)
    labels = [f"{s}<br><span style='font-size:10px'>{m}</span>" for s, m in zip(d["source"], d["method"])]
    colors = [theme.VERMILLION if "scraping" in m else theme.BLUE for m in d["method"]]
    fig = make_subplots(rows=1, cols=2, horizontal_spacing=0.16,
                        subplot_titles=("Records contributed", "Average severity of the records"))
    fig.add_trace(go.Bar(x=d["count"], y=labels, orientation="h", marker_color=colors, showlegend=False,
                         text=[f"{int(v):,}" for v in d["count"]], textposition="outside",
                         hovertemplate="%{y}<br>%{x:,.0f} records<extra></extra>"), 1, 1)
    fig.add_trace(go.Bar(x=d["value"], y=labels, orientation="h", marker_color=colors, showlegend=False,
                         text=[f"{v:.2f}" for v in d["value"]], textposition="outside",
                         hovertemplate="%{y}<br>average severity %{x:.2f} (0-3)<extra></extra>"), 1, 2)
    layout = {**theme.CHART_LAYOUT, "margin": dict(l=60, r=30, t=55, b=95)}
    fig.update_layout(**layout, height=380)
    fig.update_xaxes(title_text="Records (log scale)", type="log", dtick=1, range=[1, 5.3], row=1, col=1)
    fig.update_xaxes(title_text="Severity 0-3", range=[0, 3.3], row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    fig.add_annotation(text="Blue = API source · Red = web-scraping source", xref="paper", yref="paper",
                       x=0, y=-0.32, showarrow=False, font=dict(size=11, color=theme.GREY), xanchor="left")
    return fig


def fig_trend(d: pd.DataFrame) -> go.Figure:
    d = d.dropna(subset=["avg_severity"])
    roll = d.set_index("period_start")["avg_severity"].rolling(12, min_periods=6).mean()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["period_start"], y=d["avg_severity"], mode="lines", name="Monthly average",
                             line=dict(color=theme.LIGHT_BLUE, width=1),
                             hovertemplate="%{x|%b %Y}: %{y:.2f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=roll.index, y=roll.values, mode="lines", name="12-month average",
                             line=dict(color=theme.BLUE, width=3),
                             hovertemplate="%{x|%b %Y}: %{y:.2f} (12-month)<extra></extra>"))
    for level, name, color in ((1, "minor", theme.GREEN), (2, "moderate", theme.AMBER), (3, "severe", theme.VERMILLION)):
        fig.add_hline(y=level, line_dash="dot", line_color=color, opacity=0.7, annotation_text=name,
                      annotation_position="right", annotation_font_size=10)
    fig.update_layout(**{**theme.CHART_LAYOUT, "margin": dict(l=60, r=75, t=55, b=60)}, height=360,
                      xaxis_title="Month",
                      yaxis=dict(title="Average severity of the month's events (0-3)", range=[0, 3.2]),
                      legend=dict(orientation="h", y=1.1, x=0))
    return fig


def fig_volume(d: pd.DataFrame) -> go.Figure:
    d = d.dropna(subset=["events_count"])
    thr = d["events_count"].quantile(0.90)
    fig = go.Figure(go.Bar(x=d["period_start"], y=d["events_count"],
                           marker_color=[theme.VERMILLION if v >= thr else theme.LIGHT_BLUE for v in d["events_count"]],
                           hovertemplate="%{x|%b %Y}: %{y:.0f} flood events<extra></extra>"))
    if not d.empty:
        peak = d.loc[d["events_count"].idxmax()]
        fig.add_annotation(x=peak["period_start"], y=peak["events_count"],
                           text=f"Peak: {peak['period_start']:%B %Y} ({int(peak['events_count'])} events)",
                           showarrow=True, arrowhead=2, ax=60, ay=-10, font=dict(size=11))
    fig.update_layout(**theme.CHART_LAYOUT, height=360, showlegend=False, xaxis_title="Month",
                      yaxis_title="Flood events reported per month")
    return fig


def fig_keywords(df, kind="top_bigrams") -> go.Figure:
    d = st_keywords(df, kind, n=15)
    fig = go.Figure(go.Bar(
        x=d["pct"], y=d["term"], orientation="h",
        marker=dict(color=d["severe_pct"], colorscale=theme.SEVERE_SCALE, cmin=0,
                    cmax=max(30, float(d["severe_pct"].max())),
                    colorbar=dict(title="% severe<br>events", thickness=12)),
        text=[f"{p:.1f}%" for p in d["pct"]], textposition="outside",
        customdata=list(zip(d["count"], d["severe_pct"], d["damage_label"].fillna(""))),
        hovertemplate="<b>%{y}</b><br>in %{customdata[0]:,.0f} events (%{x:.1f}%)<br>"
                      "%{customdata[1]:.1f}% of them severe · %{customdata[2]}<extra></extra>"))
    fig.update_layout(**theme.CHART_LAYOUT, height=470,
                      xaxis=dict(title="Share of flood reports that mention the term (%)",
                                 range=[0, d["pct"].max() * 1.32]),
                      yaxis_title="")
    return fig


def fig_counties(df) -> go.Figure:
    d = st_top_counties(df)
    fig = go.Figure(go.Bar(x=d["value"], y=d["county"], orientation="h", marker_color=theme.BLUE,
                           text=[f"{v:.1f}" for v in d["value"]], textposition="outside",
                           customdata=d["count"],
                           hovertemplate="<b>%{y} County</b><br>%{x:.1f} floods per year (%{customdata:,.0f} since 2010)"
                                         "<extra></extra>"))
    fig.update_layout(**theme.CHART_LAYOUT, height=470, xaxis=dict(title="Flood events per year, 2010-2026",
                                                                    range=[0, d["value"].max() * 1.18]),
                      yaxis_title="")
    return fig


# ---------------------------------------------------------------------------
# Layout (a function: every page load reads the newest Gold file)
# ---------------------------------------------------------------------------
def serve_layout():
    df, ts, name = load_storytelling()
    h, story = st_headline(df), st_narrative(df)
    trend = st_trend_monthly(df)
    years = sorted(trend["period_start"].dt.year.unique())
    row = lambda items, mb="18px": html.Div(items, className="row g-3", style={"marginBottom": mb})
    col = lambda child, cls: html.Div(child, className=cls)
    narrative = [("Where", "1_where", theme.BLUE), ("When", "2_when", theme.AMBER),
                 ("What", "3_what", theme.VERMILLION), ("Now", "4_now", theme.GREEN)]
    return html.Div(
        [
            ui.header("Texas Floods: Is Today Unusual?",
                      "Where and when Texas floods, what floods do, and how today compares", ts, name,
                      gradient=f"linear-gradient(135deg, {theme.GREEN} 0%, {theme.BLUE} 100%)"),
            *ui.refresh_watch(name),
            # 1. the key insight in plain words
            ui.section_title("The story in four sentences", "Written by the Gold job from the data of this run."),
            row([col(html.Div([html.Div(t, style={"color": c, "fontWeight": 700, "fontSize": "12px",
                                                  "textTransform": "uppercase"}),
                               html.P(story.get(k, "—"), style={"fontSize": "14px", "margin": "4px 0 0 0"})],
                              style={**theme.CARD_STYLE, "padding": "14px 16px", "height": "100%",
                                     "background": "white", "borderTop": f"4px solid {c}"}), "col-md-3")
                 for t, k, c in narrative]),
            # 2. today (US1-US4)
            ui.section_title("Is today unusual?",
                             "Flood Risk Index per county (US4), rainfall against the same month in history "
                             "(US2, US3) and flood alerts (US1)."),
            row(fri_cards(df), "12px"),
            row([
                col(ui.chart_card("How unusual is the latest rainfall?",
                                  "Percentile of the latest complete day and week against the same station and "
                                  "month since 2010. Above the red line = unusual.",
                                  dcc.Graph(figure=fig_rain_percentile(df), config=CFG), SOURCE), "col-lg-8"),
                col(alerts_panel(df), "col-lg-4"),
            ]),
            # 3. history (US6)
            ui.section_title("Texas floods since 2010",
                             "Every flood report of NOAA Storm Events for Texas, 2010-2026."),
            row([col(ui.kpi_card(t, v, s, c), "col-md-3") for t, v, s, c in (
                ("Flood events", f"{int(h.get('flood_events_total', 0)):,}",
                 f"{h.get('flood_events_per_year', 0):.0f} per year", theme.BLUE),
                ("Flash floods", f"{h.get('flash_flood_share_pct', 0):.0f}%", "of all flood events", theme.AMBER),
                ("Deaths", f"{int(h.get('deaths_total', 0)):,}", "direct and indirect", theme.VERMILLION),
                ("Property damage", f"${h.get('property_damage_usd', 0) / 1e9:.1f} billion",
                 f"reported · {int(h.get('counties_affected', 0))} of 254 counties affected", theme.GREEN))]),
            row([
                col(ui.chart_card("How severe are Texas floods?",
                                  "Most floods are moderate; 1 in 10 is severe (deaths, injuries or "
                                  "$50,000+ damage). Minor = heavy-rain reports.", dcc.Graph(figure=fig_severity(df), config=CFG), SOURCE),
                    "col-lg-4"),
                col(ui.chart_card("Which counties flood most?",
                                  "Flood events per year since 2010 (top 10 of 254 counties).",
                                  dcc.Graph(figure=fig_counties(df), config=CFG), SOURCE), "col-lg-4"),
                col(ui.chart_card("What do flood reports talk about?",
                                  "Most frequent phrases; darker = more of those reports were severe.",
                                  dcc.Graph(id="chart-keywords", config=CFG), SOURCE,
                                  controls=dcc.RadioItems(id="kw-kind", inline=True, value="top_bigrams",
                                                          options=[{"label": " two-word phrases ",
                                                                    "value": "top_bigrams"},
                                                                   {"label": " single words", "value": "top_unigrams"}],
                                                          inputStyle={"marginRight": "4px", "marginLeft": "10px"})),
                    "col-lg-4"),
            ]),
            html.Div([html.Span("Years shown in the two charts below: ", style={"fontWeight": 600}),
                      dcc.RangeSlider(id="year-range", min=years[0], max=years[-1], step=1, value=[years[0], years[-1]],
                                      marks={int(y): str(y) for y in years}, allowCross=False)],
                     style={**theme.CARD_STYLE, "padding": "12px 20px", "background": "white", "marginBottom": "12px"}),
            row([
                col(ui.chart_card("When do floods happen?",
                                  "Flood events per month; red = the 10% busiest months.",
                                  dcc.Graph(id="chart-volume", config=CFG), SOURCE), "col-lg-6"),
                col(ui.chart_card("Are floods getting more severe?",
                                  "Average severity per month and its 12-month average: stable around "
                                  "'moderate' since 2010.", dcc.Graph(id="chart-trend", config=CFG), SOURCE),
                    "col-lg-6"),
            ]),
            row([col(ui.chart_card("Where does the data come from?",
                                   "Volume and average severity of each source: three APIs and one web-scraped "
                                   "NOAA catalog.", dcc.Graph(figure=fig_sources(df), config=CFG), SOURCE),
                     "col-12")]),
            html.P("Storytelling Dashboard · Workshop 4 · Data Analysis Programming · Universidad Distrital "
                   "Francisco José de Caldas", style={"textAlign": "center", "color": theme.GREY,
                                                      "fontSize": "11px", "marginTop": "16px"}),
        ],
        style={"maxWidth": "1600px", "margin": "0 auto", "padding": "20px", "backgroundColor": theme.LIGHT_BG},
    )


app.layout = serve_layout
ui.register_refresh_callback(app, "storytelling", latest_gold_file)


@app.callback(Output("chart-volume", "figure"), Output("chart-trend", "figure"),
              Input("year-range", "value"), State("gold-file", "data"))
def _update_time_charts(year_range, gold_file):
    df, _, _ = load_storytelling(gold_file)
    t = st_trend_monthly(df)
    t = t[t["period_start"].dt.year.between(year_range[0], year_range[1])]
    return fig_volume(t), fig_trend(t)


@app.callback(Output("chart-keywords", "figure"), Input("kw-kind", "value"), State("gold-file", "data"))
def _update_keywords(kind, gold_file):
    df, _, _ = load_storytelling(gold_file)
    return fig_keywords(df, kind)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)
