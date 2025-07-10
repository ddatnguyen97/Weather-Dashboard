from dash import Dash
import dash_bootstrap_components as dbc
from dash import html
from components.navbar import create_navbar
from components.sidebar import create_sidebar

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

navbar_div = create_navbar()
sidebar_div = create_sidebar()

app.layout = html.Div([
    html.Div(create_sidebar()),
    html.Div([
        create_navbar(),
        html.Div(id="main-content", className="dashboard-container")
    ], 
    className="main-area")
], 
className="app-container")

if __name__ == '__main__':
    app.run(debug=True)