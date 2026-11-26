"""Storytelling Dashboard — Plotly Dash (Workshop 4).

Audience: functional user (emergency-management analyst, risk researcher).
Purpose:  communicate the flood story for Texas (2010–2026) in plain language.
Port:     8051 (per Workshop 4 spec).
Data:     EXCLUSIVELY from datalake_gold/storytelling_*.parquet.

Design notes (mapped from Workshop 4 requirements):
- "Sentiment distribution" -> severity_distribution (minor / moderate / severe)
- "Sentiment trend"        -> monthly flood events + damage over time
- "Top keywords"           -> top unigrams in event narratives, coloured by severity
- "Source comparison"      -> records + avg severity per data source
- "Narrative card"         -> 4-part narrative (where / when / what / now)

Run:
    .venv-dashboard/bin/python -m dashboard.storytelling_app
"""
from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html

from . import theme
from .data_loader import (
    load_storytelling,
    st_headline,
    st_narrative,
    st_severity_distribution,
    st_source_comparison,
    st_top_keywords,
    st_trend_monthly,
)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = dash.Dash(
    __name__,
    title="Flood Monitoring · Storytelling Dashboard",
    update_title=None,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.FLATLY],
)

TEAM_NAME = "Team 2 — Oscar Durán, Laura Pardo, Nicolás Romero"
TOPIC = "Flood Monitoring and Risk in Texas (2010–2026)"


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def _header(last_updated) -> html.Div:
    return html.Div(
        [
            html.H2("Texas Floods: The Story in the Data",
                    style={"margin": 0, "fontWeight": 700}),
            html.P(
                f"{TOPIC} · {TEAM_NAME}",
                style={"margin": "4px 0 0 0", "opacity": 0.95, "fontSize": "14px"},
            ),
            html.P(
                f"Data last updated: {last_updated:%Y-%m-%d %H:%M UTC} · Source: Gold layer only",
                style={"margin": "6px 0 0 0", "opacity": 0.85, "fontSize": "12px"},
            ),
        ],
        style={**theme.HEADER_STYLE,
               "background": f"linear-gradient(135deg, {theme.GREEN} 0%, {theme.BLUE} 100%)"},
    )


def _kpi_card(title: str, value: str, subtitle: str, color: str) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "12px", "color": theme.GREY,
                                    "textTransform": "uppercase", "letterSpacing": "0.5px"}),
            html.Div(value, style={"fontSize": "28px", "fontWeight": 700, "color": color,
                                    "margin": "4px 0"}),
            html.Div(subtitle, style={"fontSize": "12px", "color": theme.GREY}),
        ],
        style={**theme.CARD_STYLE, "padding": "18px 20px",
               "borderLeft": f"5px solid {color}", "height": "100%"},
    )


def _narrative_card(title: str, text: str, color: str) -> html.Div:
    return html.Div(
        [
            html.H6(title, style={"color": color, "fontWeight": 700,
                                   "marginBottom": "6px", "textTransform": "uppercase",
                                   "fontSize": "12px", "letterSpacing": "0.5px"}),
            html.P(text, style={"fontSize": "14px", "color": theme.DARK,
                                 "marginBottom": 0, "lineHeight": "1.5"}),
        ],
        style={**theme.CARD_STYLE, "padding": "16px 18px", "height": "100%",
               "borderTop": f"4px solid {color}"},
    )


def _chart_card(title: str, subtitle: str, figure: go.Figure, card_id: str) -> html.Div:
    return html.Div(
        [
            html.H5(title, style={"marginBottom": "2px", "color": theme.DARK}),
            html.P(subtitle, style={"color": theme.GREY, "fontSize": "12px",
                                     "marginBottom": "8px"}),
            dcc.Graph(id=card_id, figure=figure, config={"displayModeBar": False}),
            html.P(
                "Source: datalake_gold/storytelling_*.parquet",
                style={"color": theme.GREY, "fontSize": "11px",
                       "marginTop": "6px", "textAlign": "right"},
            ),
        ],
        style={**theme.CARD_STYLE, "padding": "18px 22px", "height": "100%"},
    )


# ---------------------------------------------------------------------------
# Figure builders
# ---------------------------------------------------------------------------
def _fig_severity(df) -> go.Figure:
    d = st_severity_distribution(df)
    colors = [theme.SEVERITY_COLORS.get(str(c).lower(), theme.GREY) for c in d["category"]]
    labels = [str(c).capitalize() for c in d["category"]]
    fig = go.Figure(go.Pie(
        labels=labels,
        values=d["count"],
        hole=0.55,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        textinfo="label+percent",
        textfont=dict(size=13),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} events<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(
        **theme.CHART_LAYOUT,
        height=380,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
        annotations=[dict(text=f"{int(d['count'].sum()):,}<br><span style='font-size:11px'>events</span>",
                          x=0.5, y=0.5, font=dict(size=18, color=theme.DARK),
                          showarrow=False)],
    )
    return fig


def _fig_trend(df) -> go.Figure:
    d = st_trend_monthly(df).dropna(subset=["avg_severity"]).sort_values("period_start")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=d["period_start"], y=d["avg_severity"],
        mode="lines+markers",
        name="Avg severity",
        line=dict(color=theme.BLUE, width=2),
        marker=dict(size=4),
        fill="tozeroy", fillcolor="rgba(0,114,178,0.10)",
        hovertemplate="<b>%{x|%b %Y}</b><br>Avg severity: %{y:.2f}<extra></extra>",
    ))
    # Reference lines at severity levels
    for level, name, color in [(1, "Minor", theme.GREEN),
                                (2, "Moderate", theme.AMBER),
                                (3, "Severe", theme.VERMILLION)]:
        fig.add_hline(y=level, line_dash="dash", line_color=color, opacity=0.55,
                      annotation_text=name, annotation_position="right",
                      annotation_font_size=10)
    fig.update_layout(
        **theme.CHART_LAYOUT,
        xaxis_title="Month",
        yaxis_title="Average severity (0–3)",
        yaxis=dict(range=[0, 3.2]),
        height=380,
        showlegend=False,
    )
    return fig


def _fig_volume_activity(df) -> go.Figure:
    d = st_trend_monthly(df).dropna(subset=["events_count"]).sort_values("period_start")
    threshold = d["events_count"].quantile(0.90)
    colors = [theme.VERMILLION if v >= threshold else theme.LIGHT_BLUE
              for v in d["events_count"]]
    fig = go.Figure(go.Bar(
        x=d["period_start"], y=d["events_count"],
        marker_color=colors,
        hovertemplate="<b>%{x|%b %Y}</b><br>%{y:.0f} flood events<extra></extra>",
    ))
    fig.update_layout(
        **theme.CHART_LAYOUT,
        xaxis_title="Month",
        yaxis_title="Flood events per month",
        height=380,
        showlegend=False,
    )
    fig.add_annotation(
        x=1, y=1.05, xref="paper", yref="paper",
        text="🔥 Red bars = top 10% busiest months",
        showarrow=False, font=dict(size=11, color=theme.VERMILLION),
        xanchor="right",
    )
    return fig


def _fig_keywords(df) -> go.Figure:
    d = st_top_keywords(df, n=12).sort_values("count", ascending=True)
    sev = d["value"].dropna()
    # Adaptive thresholds: use terciles of the actual severity values
    if len(sev) >= 3:
        q1, q2 = sev.quantile(0.33), sev.quantile(0.67)
    else:
        q1, q2 = 2.0, 2.5
    colors = []
    for v in d["value"].fillna(sev.median() if len(sev) else 2.0):
        if v > q2:
            colors.append(theme.VERMILLION)  # high severity context
        elif v > q1:
            colors.append(theme.AMBER)
        else:
            colors.append(theme.GREEN)
    fig = go.Figure(go.Bar(
        x=d["count"], y=d["term"], orientation="h",
        marker_color=colors,
        text=[f"{int(c):,}" for c in d["count"]],
        textposition="outside",
        customdata=d["value"],
        hovertemplate="<b>%{y}</b><br>%{x:,.0f} mentions<br>"
                      "Avg severity: %{customdata:.2f}<extra></extra>",
    ))
    fig.update_layout(
        **theme.CHART_LAYOUT,
        xaxis_title="Mentions in event narratives",
        yaxis_title="",
        height=420,
        xaxis=dict(range=[0, d["count"].max() * 1.15]),
    )
    return fig


def _fig_sources(df) -> go.Figure:
    d = st_source_comparison(df).dropna(subset=["category"])
    labels = [str(c).replace("_", " ").title() for c in d["category"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=labels, y=d["count"],
        name="Records",
        marker_color=theme.BLUE,
        text=[f"{int(v):,}" for v in d["count"]],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>%{y:,.0f} records<extra></extra>",
        yaxis="y",
    ))
    if d["value"].notna().any():
        fig.add_trace(go.Scatter(
            x=labels, y=d["value"],
            name="Avg severity",
            mode="markers+lines",
            marker=dict(size=13, color=theme.VERMILLION, symbol="diamond",
                        line=dict(color="white", width=1.5)),
            line=dict(color=theme.VERMILLION, width=2, dash="dot"),
            hovertemplate="<b>%{x}</b><br>Avg severity: %{y:.2f}<extra></extra>",
            yaxis="y2",
        ))
        fig.update_layout(
            yaxis2=dict(title="Avg severity (0–3)", overlaying="y", side="right",
                        range=[0, 3.2], showgrid=False),
        )
    fig.update_layout(
        **theme.CHART_LAYOUT,
        xaxis_title="Data source",
        yaxis_title="Records ingested",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return fig


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
def build_layout(df, last_updated):
    headline = st_headline(df)
    narrative = st_narrative(df)
    sev = st_severity_distribution(df)

    where = narrative.get("1_where", {}).get("label", "—")
    when = narrative.get("2_when", {}).get("label", "—")
    what = narrative.get("3_what", {}).get("label", "—")
    now = narrative.get("4_now", {}).get("label", "—")

    headline_parts = [
        ("Flood events", f"{int(headline.get('flood_events_total', 0)):,}",
         "Recorded 2010–2026", theme.BLUE),
        ("Deaths", f"{int(headline.get('deaths_total', 0)):,}",
         "Direct + indirect", theme.VERMILLION),
        ("Counties affected", f"{int(headline.get('counties_affected', 0)):,}",
         "of 254 Texas counties", theme.AMBER),
        ("Property damage", f"${headline.get('property_damage_usd', 0)/1e9:.1f}B",
         "Reported total (USD)", theme.GREEN),
    ]

    return html.Div(
        [
            _header(last_updated),

            # Narrative: 4 Ws
            html.H4("The story in four questions",
                    style={"marginTop": "8px", "marginBottom": "12px",
                           "color": theme.DARK}),
            html.Div(
                [
                    html.Div(_narrative_card("Where", where, theme.BLUE),
                             className="col-md-3"),
                    html.Div(_narrative_card("When", when, theme.AMBER),
                             className="col-md-3"),
                    html.Div(_narrative_card("What", what, theme.VERMILLION),
                             className="col-md-3"),
                    html.Div(_narrative_card("Now", now, theme.GREEN),
                             className="col-md-3"),
                ],
                className="row g-3",
                style={"marginBottom": "20px"},
            ),

            # Headline KPI cards
            html.Div(
                [html.Div(_kpi_card(t, v, s, c), className="col-md-3")
                 for t, v, s, c in headline_parts],
                className="row g-3",
                style={"marginBottom": "20px"},
            ),

            # Severity + Source comparison
            html.Div(
                [
                    html.Div(
                        _chart_card("How severe are Texas floods?",
                                    "Share of events by severity level — most events are moderate, "
                                    "but severe events cause the biggest impact.",
                                    _fig_severity(df), "chart-severity"),
                        className="col-lg-6",
                    ),
                    html.Div(
                        _chart_card("Where does the data come from?",
                                    "Records contributed by each data source in the Gold layer.",
                                    _fig_sources(df), "chart-sources"),
                        className="col-lg-6",
                    ),
                ],
                className="row g-3",
                style={"marginBottom": "20px"},
            ),

            # Trend + Volume
            html.Div(
                [
                    html.Div(
                        _chart_card("How has flood severity trended over time?",
                                    "Average severity (0–3) per month. Dashed lines mark the minor / "
                                    "moderate / severe thresholds.",
                                    _fig_trend(df), "chart-trend"),
                        className="col-lg-6",
                    ),
                    html.Div(
                        _chart_card("When do floods happen?",
                                    "Flood events per month. Red bars mark the top 10% busiest months.",
                                    _fig_volume_activity(df), "chart-volume"),
                        className="col-lg-6",
                    ),
                ],
                className="row g-3",
                style={"marginBottom": "20px"},
            ),

            # Keywords
            html.Div(
                [
                    _chart_card("What words appear in flood narratives?",
                                "Most frequent terms in flood event descriptions. "
                                "Red = associated with severe events · Amber = moderate · Green = minor.",
                                _fig_keywords(df), "chart-keywords"),
                ],
                className="row g-3",
            ),

            # Footer
            html.P(
                "Storytelling Dashboard · Workshop 4 · Data Analysis Programming · "
                "Universidad Distrital Francisco José de Caldas",
                style={"textAlign": "center", "color": theme.GREY,
                       "fontSize": "11px", "marginTop": "24px"},
            ),
        ],
        style={"maxWidth": "1500px", "margin": "0 auto",
               "padding": "20px", "backgroundColor": theme.LIGHT_BG,
               "minHeight": "100vh"},
    )


# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------
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

_df, _ts = load_storytelling()
app.layout = build_layout(_df, _ts)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)