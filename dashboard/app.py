from dash import Dash, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import html
from dashboard.components.navbar import create_navbar
from dashboard.components.sidebar import create_sidebar, PAGES
from dashboard.tabs.weekly_weather import layout as weather_layout
from dashboard.tabs.daily_weather import layout as daily_weather_layout
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
<html lang="en">
<head>
    {%metas%}
    <title>Dashboard</title>
    {%favicon%}
    {%css%}
    <!-- Google Tag Manager -->
    <script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
    new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
    j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
    'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
    })(window,document,'script','dataLayer','GTM-TRTXZP6Q');</script>
    <!-- End Google Tag Manager -->

</head>
<body>
    <!-- Google Tag Manager (noscript) -->
    <noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-TRTXZP6Q"
    height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
    <!-- End Google Tag Manager (noscript) -->
    {%app_entry%}
    
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
        <script src="/assets/events.js"></script>
    </footer>
</body>
</html>
"""

navbar_div = create_navbar()
sidebar_div = create_sidebar()
weekly_weather_div = weather_layout()
daily_weather_div = daily_weather_layout()
home_div = home_layout()
aqi_div = aqi_layout()

app.layout = html.Div([
    html.Div(id='sidebar'), 
    html.Div([
        navbar_div,
        html.Div(id='main-content', className='dashboard-container'),
        # html.Div(id='filter-event-div', style={'display': 'none'})
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
    elif pathname == '/daily-weather':
        return daily_weather_div
    elif pathname == '/weekly-weather':
        return weekly_weather_div
    elif pathname == '/aqi':
        return aqi_div

if __name__ == '__main__':
    app.run(debug=True)