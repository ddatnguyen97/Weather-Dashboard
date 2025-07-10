from dash import html
import dash_bootstrap_components as dbc

def create_navbar():
    return dbc.Navbar(
        html.Div([
            html.H2("HCM City Weather Dashboard", className="navbar-title"),
        ],
    ),
    className="navbar-container",
)