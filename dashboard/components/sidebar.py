from dash import html
import dash_bootstrap_components as dbc

PAGES = {
    '/home': {'label': 'Home', 'id': 'home-link'},
    '/daily-weather': {'label': 'Daily Weather', 'id': 'daily-link'},
    '/weekly-weather': {'label': 'Weekly Weather', 'id': 'weather-link'},
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

        dbc.Nav(
                nav_links,
                vertical=True,
                pills=True,
                className='sidebar-nav')
    ],
    className='sidebar-container')
