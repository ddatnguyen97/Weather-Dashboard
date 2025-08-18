from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output, callback

PAGES = {
    '/home': {'label': 'Home', 'id': 'home-link'},
    '/weather': {'label': 'Weather Report', 'id': 'weather-link'},
    '/aqi': {'label': 'Air Quality', 'id': 'aqi-link'},
}

def create_sidebar(current_path='/home'):
    nav_links = [
        dbc.NavLink(
            page['label'],
            href=path,
            id=page['id'],
            active='exact',
            className='sidebar-link'
        ) for path, page in PAGES.items()
    ]
    
    return html.Div([
        html.Div(
            html.Img(
                src='/assets/icons/Weather Talks Logo.png',
                className='sidebar-logo'
            ),
            className='sidebar-logo-container'
        ),

        # dcc.Location(id='sidebar-url', refresh=False),
        dbc.Nav(
                nav_links,
                vertical=True,
                pills=True,
                className='sidebar-nav')
    ],
    className='sidebar-container')
