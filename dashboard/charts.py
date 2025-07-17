import dash_bootstrap_components as dbc
from dash import html
from utils import get_weather_icon

def create_weather_card(date, col, data_row=None):
    if data_row is None:
        return dbc.Card([
            dbc.CardHeader(date),
            dbc.CardBody('No Data', className='text-muted')
        ],)
    
    description = data_row.get(col)
    icon = get_weather_icon(description)

    return dbc.Card([
        dbc.CardHeader([date], className='card-header'),
        dbc.CardBody([
            html.Img(src=f'assets/icons/{icon}', className='card-icon'),
            html.P(f"Temperature: {data_row['temperature']:.2f}°C", className='card-content'),
            html.P(f"Humidity: {data_row['humidity']:.2f}%", className='card-content'),
            html.P(f"Precipitation: {data_row['precipitation']:.2f}mm", className='card-content')
        ])
    ], className='weather-card')