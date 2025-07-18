import logging
from utils import get_week_range, get_weather_icon
import pandas as pd
import dash_bootstrap_components as dbc
from dash import html

def create_weather_card(date, col, data_row=None):
    if data_row is None:
        return dbc.Card([
            dbc.CardHeader(date),
            dbc.CardBody('No Data', className='text-muted')
        ],)
    
    description = data_row.get(col)
    icon = get_weather_icon(description)

    return dbc.Card([
        html.Div([date], className='card-header'),
        dbc.CardBody([
            html.Img(src=f'assets/icons/{icon}', className='card-icon'),
            html.P(f"{data_row['overall_weather']}", className='card-content'),
            html.P(f"Temperature: {data_row['temperature']:.2f}°C", className='card-content'),
            html.P(f"Humidity: {data_row['humidity']:.2f}%", className='card-content'),
        ])
    ], className='weather-card')

def generate_weekly_weather_cards(df, selected_date, create_card_func):
    try:
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        start_date, end_date = get_week_range(selected_date)
        date_range = pd.date_range(start=start_date, end=end_date)

        cards = []
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            data_row = None

            if df is not None and not df.empty:
                match = df[df['date'] == date_str]
                if not match.empty:
                    data_row = match.iloc[0]

            card = create_card_func(date_str, data_row)
            cards.append(card)

        return cards

    except Exception as e:
        logging.error(f'Error generating weekly weather cards: {e}')
        return None

