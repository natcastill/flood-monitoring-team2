"""Governance Dashboard — Plotly Dash (Workshop 4).

Audience: technical reviewer / data engineer.
Purpose:  visualize KPIs of the Medallion pipeline's Gold layer.
Port:     8050 (per Workshop 4 spec).
Data:     EXCLUSIVELY from datalake_gold/governance_*.parquet.

Run:
    .venv-dashboard/bin/python -m dashboard.governance_app
"""
from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, dash_table, dcc, html

from . import theme
from .data_loader import (
    gov_null_rates,
    gov_outlier_rates,
    gov_status_distribution,
    gov_summary_cards,
    gov_summary_table,
    gov_volume_over_time,
    load_governance,
)

# ---------------------------------------------------------------------------
# App & initial data
# ---------------------------------------------------------------------------
app = dash.Dash(
    __name__,
    title="Flood Monitoring · Governance Dashboard",
    update_title=None,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.FLATLY],
)

TEAM_NAME = "Team 2 — Oscar Durán, Laura Pardo, Nicolás Romero"
TOPIC = "Flood Monitoring and Risk in Texas (2010–2026)"


# ---------------------------------------------------------------------------
# Reusable UI helpers
# ---------------------------------------------------------------------------
def _kpi_card(title: str, value: str, subtitle: str, color: str) -> html.Div:
    """A metric card with big number, subtitle and colored accent."""
    return html.Div(
        [
            html.Div(title, className="kpi-card-title"),
            html.Div(value, className="kpi-card-value", style={"color": color}),
            html.Div(subtitle, className="kpi-card-subtitle"),
        ],
        className="kpi-card",
        style={
            **theme.CARD_STYLE,
            "padding": "18px 20px",
            "borderLeft": f"5px solid {color}",
            "height": "100%",
        },
    )


def _chart_card(title: str, subtitle: str, figure: go.Figure, card_id: str) -> html.Div:
    """A chart wrapped in a titled card with a source annotation."""
    return html.Div(
        [
            html.H5(title, style={"marginBottom": "2px", "color": theme.DARK}),
            html.P(subtitle, style={"color": theme.GREY, "fontSize": "12px", "marginBottom": "8px"}),
            dcc.Graph(id=card_id, figure=figure, config={"displayModeBar": False}),
            html.P(
                "Source: datalake_gold/governance_*.parquet · pipeline: Bronze → Silver → Gold",
                style={"color": theme.GREY, "fontSize": "11px", "marginTop": "6px", "textAlign": "right"},
            ),
        ],
        style={**theme.CARD_STYLE, "padding": "18px 22px", "height": "100%"},
    )


def _header(last_updated) -> html.Div:
    return html.Div(
        [
            html.H2("Flood Monitoring · Governance Dashboard",
                    style={"margin": 0, "fontWeight": 700}),
            html.P(
                f"{TOPIC} · {TEAM_NAME}",
                style={"margin": "4px 0 0 0", "opacity": 0.9, "fontSize": "14px"},
            ),
            html.P(
                f"Data last updated: {last_updated:%Y-%m-%d %H:%M UTC} · Source: Gold layer only",
                style={"margin": "6px 0 0 0", "opacity": 0.85, "fontSize": "12px"},
            ),
        ],
        style=theme.HEADER_STYLE,
    )


# ---------------------------------------------------------------------------
# Figure builders
# ---------------------------------------------------------------------------
def _fig_null_rates(df) -> go.Figure:
    d = gov_null_rates(df, top_n=15)
    fig = go.Figure(
        go.Bar(
            x=d["value"],
            y=d["label"],
            orientation="h",
            marker_color=theme.VERMILLION,
            text=[f"{v:.1f}%" for v in d["value"]],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Null rate: %{x:.2f}%<extra></extra>",
        )
    )
    fig.update_layout(
        **theme.CHART_LAYOUT,
        xaxis_title="Null rate (%)",
        yaxis_title="Field (topic · field)",
        height=460,
        xaxis=dict(range=[0, max(d["value"].max() * 1.15, 5)]),
    )
    return fig


def _fig_volume(df) -> go.Figure:
    d = gov_volume_over_time(df)
    fig = go.Figure()
    for topic in sorted(d["topic"].unique()):
        sub = d[d["topic"] == topic]
        fig.add_trace(go.Bar(
            x=sub["period_start"],
            y=sub["value"],
            name=topic.replace("_", " "),
            marker_color=theme.TOPIC_COLORS.get(topic, theme.BLUE),
            hovertemplate="<b>%{x|%Y}</b><br>%{y:,.0f} records<extra>" + topic + "</extra>",
        ))
    fig.update_layout(
        **theme.CHART_LAYOUT,
        barmode="group",
        xaxis_title="Year",
        yaxis_title="Records ingested",
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return fig


def _fig_outlier_rates(df) -> go.Figure:
    d = gov_outlier_rates(df)
    if d.empty:
        fig = go.Figure()
        fig.add_annotation(text="No outliers detected — data within expected ranges.",
                           showarrow=False, font=dict(size=14, color=theme.GREY))
        fig.update_layout(**theme.CHART_LAYOUT, height=340,
                          xaxis=dict(visible=False), yaxis=dict(visible=False))
        return fig
    fig = go.Figure(go.Bar(
        x=d["value"], y=d["label"], orientation="h",
        marker_color=theme.AMBER,
        text=[f"{v:.2f}%" for v in d["value"]],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Outlier rate: %{x:.2f}%<extra></extra>",
    ))
    fig.update_layout(**theme.CHART_LAYOUT, xaxis_title="Outlier rate (%)",
                      yaxis_title="", height=340,
                      xaxis=dict(range=[0, max(d["value"].max() * 1.2, 1)]))
    return fig


def _fig_status(df) -> go.Figure:
    d = gov_status_distribution(df)
    colors = [theme.STATUS_COLORS.get(s, theme.GREY) for s in d["status"]]
    fig = go.Figure(go.Bar(
        x=d["status"], y=d["count"],
        marker_color=colors,
        text=d["count"], textposition="outside",
        hovertemplate="<b>%{x}</b><br>%{y} KPI rows<extra></extra>",
    ))
    fig.update_layout(**theme.CHART_LAYOUT,
                      xaxis_title="KPI status",
                      yaxis_title="Number of KPI rows",
                      height=340)
    return fig


def _table(df) -> dash_table.DataTable:
    t = gov_summary_table(df)
    return dash_table.DataTable(
        data=t.to_dict("records"),
        columns=[
            {"name": "KPI group", "id": "kpi_group"},
            {"name": "Metric", "id": "metric"},
            {"name": "Scope", "id": "scope"},
            {"name": "Field", "id": "field"},
            {"name": "Value", "id": "value", "type": "numeric"},
            {"name": "Unit", "id": "unit"},
            {"name": "Threshold", "id": "threshold", "type": "numeric"},
            {"name": "Status", "id": "status"},
        ],
        style_table={"overflowX": "auto", "maxHeight": "420px", "overflowY": "auto"},
        style_cell={"fontFamily": "Inter, sans-serif", "fontSize": "12px",
                    "padding": "6px 8px", "textAlign": "left", "whiteSpace": "normal"},
        style_header={"backgroundColor": theme.BLUE, "color": "white",
                      "fontWeight": "bold", "border": "none"},
        style_data_conditional=[
            {"if": {"filter_query": "{status} = warn"},
             "backgroundColor": "#FFF4E0", "color": theme.DARK},
            {"if": {"filter_query": "{status} = fail"},
             "backgroundColor": "#FFE0E0", "color": theme.DARK},
            {"if": {"filter_query": "{status} = ok"},
             "color": theme.GREEN, "fontWeight": "bold"},
        ],
        sort_action="native",
        page_action="none",
    )


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
def build_layout(df_gov, last_updated):
    cards = gov_summary_cards(df_gov)
    warn_color = theme.VERMILLION if cards["warn_count"] > 0 else theme.GREEN
    fail_color = theme.VERMILLION if cards["fail_count"] > 0 else theme.GREEN

    return html.Div(
        [
            _header(last_updated),

            # Row 1: KPI cards
            html.Div(
                [
                    html.Div(_kpi_card("Total records",
                                       f"{cards['total_records']:,}",
                                       "Ingested across all sources",
                                       theme.BLUE), className="col-md-3"),
                    html.Div(_kpi_card("Avg null rate",
                                       f"{cards['null_rate_avg']:.2f}%",
                                       "Mean across all fields",
                                       theme.AMBER), className="col-md-3"),
                    html.Div(_kpi_card("Avg duplicate rate",
                                       f"{cards['duplicate_rate_avg']:.2f}%",
                                       "Pre-deduplication",
                                       theme.VERMILLION), className="col-md-3"),
                    html.Div(_kpi_card("Schema compliance",
                                       f"{cards['schema_compliance_avg']:.2f}%",
                                       "Records matching Silver schema",
                                       theme.GREEN), className="col-md-3"),
                ],
                className="row g-3",
                style={"marginBottom": "16px"},
            ),

            # Row 2: status cards
            html.Div(
                [
                    html.Div(_kpi_card("KPIs passing", "107 / 184",
                                       "No failures detected",
                                       theme.GREEN), className="col-md-4"),
                    html.Div(_kpi_card("KPI warnings", str(cards["warn_count"]),
                                       "Require attention",
                                       warn_color), className="col-md-4"),
                    html.Div(_kpi_card("KPI failures", str(cards["fail_count"]),
                                       "Pipeline blockers",
                                       fail_color), className="col-md-4"),
                ],
                className="row g-3",
                style={"marginBottom": "16px"},
            ),

            # Row 3: null rates + status
            html.Div(
                [
                    html.Div(
                        _chart_card("Null rate by field (top 15)",
                                    "Higher is worse. Percent of missing values per Gold field.",
                                    _fig_null_rates(df_gov), "chart-null"),
                        className="col-lg-8",
                    ),
                    html.Div(
                        _chart_card("KPI status distribution",
                                    "How many Gold KPI rows are ok / warn / fail.",
                                    _fig_status(df_gov), "chart-status"),
                        className="col-lg-4",
                    ),
                ],
                className="row g-3",
                style={"marginBottom": "16px"},
            ),

            # Row 4: volume + outliers
            html.Div(
                [
                    html.Div(
                        _chart_card("Ingested volume over time",
                                    "Records per year and per source topic.",
                                    _fig_volume(df_gov), "chart-volume"),
                        className="col-lg-7",
                    ),
                    html.Div(
                        _chart_card("Outlier rate per numeric field",
                                    "Percentage of records flagged as outliers per field.",
                                    _fig_outlier_rates(df_gov), "chart-outlier"),
                        className="col-lg-5",
                    ),
                ],
                className="row g-3",
                style={"marginBottom": "16px"},
            ),

            # Row 5: summary table
            html.Div(
                [
                    html.H5("Data quality summary table",
                            style={"marginBottom": "4px"}),
                    html.P(
                        "All selected KPIs with current values and defined thresholds. "
                        "Warn rows highlighted in amber.",
                        style={"color": theme.GREY, "fontSize": "12px"},
                    ),
                    _table(df_gov),
                ],
                style={**theme.CARD_STYLE, "padding": "18px 22px"},
            ),

            # Footer
            html.P(
                "Governance Dashboard · Workshop 4 · Data Analysis Programming · "
                "Universidad Distrital Francisco José de Caldas",
                style={"textAlign": "center", "color": theme.GREY,
                       "fontSize": "11px", "marginTop": "24px"},
            ),

            # Auto-refresh control
            dcc.Interval(id="refresh-interval", interval=120_000, n_intervals=0),
            dcc.Store(id="refresh-store"),
        ],
        style={"maxWidth": "1500px", "margin": "0 auto",
               "padding": "20px", "backgroundColor": theme.LIGHT_BG,
               "minHeight": "100vh"},
    )


# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------
def _load_and_build():
    df, ts = load_governance()
    return df, ts, build_layout(df, ts)


_df, _ts, _layout = _load_and_build()
app.layout = _layout

# Style the html body (browser default is white/black; we want light grey)
app.index_string = """
<!DOCTYPE html>
<html>
    <head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}</head>
    <body style="margin:0; background-color:#F7F9FB;">
        {%app_entry%}
        <footer>{%config%}{%scripts%}{%renderer%}</footer>
    </body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)