"""Shared visual theme for both dashboards (Workshop 4).

Design decisions:
- Okabe-Ito palette: 8-colour, colour-blind-safe, industry standard for data viz.
- Severity colours are FIXED and consistent across both apps:
    minor -> green, moderate -> amber, severe -> red.
- Typography: Bootstrap 5 defaults with a slightly larger base font for 1080p.
"""

# ---------------------------------------------------------------------------
# Core palette (Okabe-Ito, colour-blind-safe)
# ---------------------------------------------------------------------------
BLUE = "#0072B2"
LIGHT_BLUE = "#56B4E9"
GREEN = "#009E73"
AMBER = "#E69F00"
VERMILLION = "#D55E00"
PINK = "#CC79A7"
YELLOW = "#F0E442"
GREY = "#7F7F7F"
DARK = "#222222"
LIGHT_BG = "#F7F9FB"

# ---------------------------------------------------------------------------
# Semantic colours
# ---------------------------------------------------------------------------
SEVERITY_COLORS = {
    "minor": GREEN,
    "moderate": AMBER,
    "severe": VERMILLION,
}

STATUS_COLORS = {
    "ok": GREEN,
    "info": GREY,
    "warn": AMBER,
    "fail": VERMILLION,
}

TOPIC_COLORS = {
    "flood_events": BLUE,
    "nws_alerts": VERMILLION,
    "nws_observations": LIGHT_BLUE,
    "daily_precipitation": GREEN,
}

# ---------------------------------------------------------------------------
# Chart template (Plotly) — single source of truth
# ---------------------------------------------------------------------------
PLOTLY_TEMPLATE = "plotly_white"

CHART_LAYOUT = dict(
    template=PLOTLY_TEMPLATE,
    font=dict(family="Inter, Segoe UI, Roboto, sans-serif", size=13, color=DARK),
    title=dict(font=dict(size=15, color=DARK), x=0.02, xanchor="left"),
    margin=dict(l=60, r=30, t=55, b=60),
    hoverlabel=dict(bgcolor="white", font_size=13),
    colorway=[BLUE, VERMILLION, LIGHT_BLUE, GREEN, AMBER, PINK, GREY],
)

# Bootstrap theme
BOOTSTRAP_THEME = "FLATLY"

# Shared header CSS
HEADER_STYLE = {
    "background": f"linear-gradient(135deg, {BLUE} 0%, {LIGHT_BLUE} 100%)",
    "color": "white",
    "padding": "20px 30px",
    "borderRadius": "8px",
    "marginBottom": "20px",
    "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
}

CARD_STYLE = {
    "borderRadius": "8px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
    "border": "1px solid #E5E8EB",
}
# Flood Risk Index levels (same green / amber / red semantics as severity)
FRI_COLORS = {"LOW": GREEN, "MEDIUM": AMBER, "HIGH": VERMILLION}

# Sequential scale for "share of severe events" (light amber -> vermillion)
SEVERE_SCALE = [[0.0, "#FBE3C0"], [0.5, AMBER], [1.0, VERMILLION]]

TEAM_NAME = "Team 2: Oscar Durán, Laura Natalia Pardo, Nicolás Romero"
TOPIC = "Flood Monitoring and Risk System, Texas (USA), 2010-2026"
