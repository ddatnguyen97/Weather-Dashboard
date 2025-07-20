from dash import Dash, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import html
from components.navbar import create_navbar
from components.sidebar import create_sidebar
from tabs.weather import layout as weather_layout

app = Dash(__name__,
            external_stylesheets=[dbc.themes.BOOTSTRAP],
            suppress_callback_exceptions=True)

navbar_div = create_navbar()
sidebar_div = create_sidebar()
weather_report_div = weather_layout()
# home_dive = home_layout()

app.layout = html.Div([
    dcc.Location(id='url'),
    html.Div(sidebar_div),
    html.Div([
        navbar_div,
        html.Div(id='main-content', children=weather_report_div, className='dashboard-container')
    ], 
    className='main-area')
], 
className='app-container')

@app.callback(
    Output('main-content', 'children'),
    Input('url', 'pathname')
)
def render_page_content(pathname='/home'):
    if pathname == '/weather':
        return weather_report_div
    # elif pathname == '/air-quality':
    #     return air_quality.layout    
    # else:
    #     return home_div

if __name__ == '__main__':
    app.run(debug=True)