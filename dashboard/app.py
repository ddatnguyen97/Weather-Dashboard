from dash import Dash, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import html
from components.navbar import create_navbar
from components.sidebar import create_sidebar, PAGES
from tabs.weather import layout as weather_layout
from tabs.home import layout as home_layout

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
    )

navbar_div = create_navbar()
sidebar_div = create_sidebar()
weather_report_div = weather_layout()
home_div = home_layout()

app.layout = html.Div([
    html.Div(id='sidebar'), 
    html.Div([
        navbar_div,
        html.Div(id='main-content', className='dashboard-container')
    ], className='main-area'),
    dcc.Location(id='url', refresh=False)
], className='app-container')

@app.callback(
    Output('sidebar', 'children'),
    Input('url', 'pathname')
)
def update_sidebar(pathname):
    return create_sidebar(pathname or '/home')

@app.callback(
    Output('main-content', 'children'),
    Input('url', 'pathname')
)
def render_page_content(pathname):
    if pathname in [None, '/', '/home']:
        return home_div
    elif pathname == '/weather':
        return weather_report_div
    # elif pathname == '/aqi':
    #     return aqi_layout()

@app.callback(
    [Output(page['id'], 'active') for path, page in PAGES.items()],
    Input('url', 'pathname')
)
def highlight_active_link(pathname):
    return [pathname == path for path in PAGES.keys()]

if __name__ == '__main__':
    app.run(debug=True)