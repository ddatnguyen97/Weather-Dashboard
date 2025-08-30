from dash import Dash, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import html
from dashboard.components.navbar import create_navbar
from dashboard.components.sidebar import create_sidebar, PAGES
from dashboard.tabs.weather import layout as weather_layout
from dashboard.tabs.home import layout as home_layout
from dashboard.tabs.aqi import layout as aqi_layout
import os

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
    )
server = app.server 

GA_MEASUREMENT_ID = os.getenv("GA_MEASUREMENT_ID")

app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Dashboard</title>
        {%favicon%}
        {%css%}

        <!-- Google tag (gtag.js) -->
        <script async src="https://www.googletagmanager.com/gtag/js?id=G-SBGP0H0LEN"></script>
        {% raw %}
        <script>
          window.dataLayer = window.dataLayer || [];
          function gtag(){dataLayer.push(arguments);}
          gtag('js', new Date());
          gtag('config', 'G-SBGP0H0LEN');
        </script>
        {% endraw %}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""

navbar_div = create_navbar()
sidebar_div = create_sidebar()
weather_report_div = weather_layout()
home_div = home_layout()
aqi_div = aqi_layout()

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
    [Output(page['id'], 'active') for path, page in PAGES.items()],
    Input('url', 'pathname')
)
def highlight_active_link(pathname):
    if pathname in [None, '/']:
        pathname = '/home'
    return [pathname == path for path in PAGES.keys()]

@app.callback(
    Output('main-content', 'children'),
    Input('url', 'pathname')
)
def render_page_content(pathname):
    if pathname in [None, '/', '/home']:
        return home_div
    elif pathname == '/weekly-weather':
        return weather_report_div
    elif pathname == '/aqi':
        return aqi_div

if __name__ == '__main__':
    app.run(debug=True)