from components.cards import weather_card
from dash import html, dcc

layout = html.Div([
    html.H4('Weekly Weather Overview', className='tab'),
    weather_card
], className='weather-layout')