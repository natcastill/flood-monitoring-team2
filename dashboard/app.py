"""Plotly Dash monitoring application (placeholder; built in Workshop 4)."""
from dash import Dash, html

app = Dash(__name__)
app.layout = html.Div([html.H1("Flood Monitoring and Risk - Texas"),
                       html.P("Dashboard under construction (Workshop 4).")])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
