"""Layout pieces shared by the two dashboards (header, cards, chart frame, refresh logic)."""
from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import dcc, html

from . import theme

POLL_MS = 60_000   # how often the page checks for a newer Gold run


def header(title: str, subtitle: str, last_updated, gold_file: str, gradient: str | None = None) -> html.Div:
    style = dict(theme.HEADER_STYLE)
    if gradient:
        style["background"] = gradient
    return html.Div(
        [
            html.Div(
                [
                    html.H2(title, style={"margin": 0, "fontWeight": 700}),
                    html.P(subtitle, style={"margin": "4px 0 0 0", "fontSize": "15px"}),
                    html.P(f"{theme.TOPIC} · {theme.TEAM_NAME}",
                           style={"margin": "4px 0 0 0", "opacity": 0.9, "fontSize": "13px"}),
                    html.P(f"Data last updated: {last_updated:%Y-%m-%d %H:%M} UTC · Gold file: {gold_file}",
                           style={"margin": "6px 0 0 0", "opacity": 0.9, "fontSize": "12px"}),
                ],
                style={"flex": "1"},
            ),
            html.A("↻ Refresh data", href="/", className="btn btn-light btn-sm",
                   title="Reload the page with the newest Gold Parquet file",
                   style={"alignSelf": "flex-start", "fontWeight": 600}),
        ],
        style={**style, "display": "flex", "gap": "16px"},
    )


def refresh_watch(gold_file: str) -> list:
    """Store of the rendered file + poller + banner (filled by register_refresh_callback)."""
    return [dcc.Store(id="gold-file", data=gold_file),
            dcc.Interval(id="gold-poll", interval=POLL_MS, n_intervals=0),
            html.Div(id="gold-banner")]


def register_refresh_callback(app, prefix: str, latest_fn) -> None:
    """Every POLL_MS, compare the newest Gold file with the one on screen; if a new Gold run
    exists, show a banner with a reload link (auto-detection + manual refresh)."""
    from dash import Input, Output, State

    @app.callback(Output("gold-banner", "children"), Input("gold-poll", "n_intervals"),
                  State("gold-file", "data"))
    def _check(_, shown):
        newest = latest_fn(prefix).name
        if newest == shown:
            return None
        return dbc.Alert([f"A newer Gold run is available ({newest}). ",
                          html.A("Reload the dashboard", href="/", className="alert-link")],
                         color="info", style={"marginBottom": "16px"})


def kpi_card(title: str, value: str, subtitle: str, color: str) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "12px", "color": theme.GREY, "textTransform": "uppercase",
                                   "letterSpacing": "0.5px"}),
            html.Div(value, style={"fontSize": "28px", "fontWeight": 700, "color": color, "margin": "4px 0"}),
            html.Div(subtitle, style={"fontSize": "12px", "color": theme.GREY}),
        ],
        style={**theme.CARD_STYLE, "padding": "16px 18px", "borderLeft": f"5px solid {color}",
               "height": "100%", "background": "white"},
    )


def chart_card(title: str, subtitle: str, graph, source: str, controls=None) -> html.Div:
    """Card = title + one-line 'what to look for' + optional controls + chart + source note."""
    children = [html.H5(title, style={"marginBottom": "2px", "color": theme.DARK}),
                html.P(subtitle, style={"color": theme.GREY, "fontSize": "12.5px", "marginBottom": "8px"})]
    if controls is not None:
        children.append(html.Div(controls, style={"marginBottom": "6px"}))
    children += [graph,
                 html.P(f"Source: {source}", style={"color": theme.GREY, "fontSize": "11px",
                                                    "marginTop": "6px", "textAlign": "right"})]
    return html.Div(children, style={**theme.CARD_STYLE, "padding": "16px 20px", "height": "100%",
                                     "background": "white"})


def section_title(text: str, hint: str = "") -> html.Div:
    return html.Div([html.H4(text, style={"margin": "8px 0 2px 0", "color": theme.DARK}),
                     html.P(hint, style={"color": theme.GREY, "fontSize": "13px", "marginBottom": "10px"})])


INDEX_STRING = """<!DOCTYPE html>
<html>
    <head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}</head>
    <body style="margin:0; background-color:#F7F9FB;">
        {%app_entry%}
        <footer>{%config%}{%scripts%}{%renderer%}</footer>
    </body>
</html>
"""
