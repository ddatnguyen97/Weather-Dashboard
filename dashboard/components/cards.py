import logging
from dashboard.utils import get_week_range, get_weather_icon, get_min_time, get_max_temperature
import pandas as pd
import dash_bootstrap_components as dbc
from dash import html
from datetime import datetime

def create_weather_card(date_label, col, data_row=None):
    if data_row is None:
        return dbc.Card([
            html.Div(date_label, className='card-header'),
            dbc.CardBody('No Data', className='text-muted')
        ])
    
    description = data_row.get(col)
    icon = get_weather_icon(description)

    return dbc.Card([
        html.P(date_label, className='card-header'),
        dbc.CardBody([
            html.Img(src=f'assets/icons/{icon}', className='card-icon'),
            html.P(f"{data_row['overall_weather']}", className='card-content'),
            html.P(f"Temperature: {data_row['temperature']:.2f}°C", className='card-content'),
            html.P(f"Humidity: {data_row['humidity']:.2f}%", className='card-content'),
        ])
    ], className='weather-card')

def generate_weekly_weather_cards(df, selected_date, create_card_func):
    try:
        df['date'] = pd.to_datetime(df['date'], format='%b %d', errors='coerce')
        start_date, end_date = get_week_range(selected_date)
        date_range = pd.date_range(start=start_date, end=end_date)

        cards = []
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            weekday = date.strftime('%a')
            display_date = date.strftime('%b %d')
            data_row = None
            if df is not None and not df.empty:
                match = df[df['date'].dt.strftime('%Y-%m-%d') == date_str]
                if not match.empty:
                    data_row = match.iloc[0]

            card = create_card_func(f"{weekday} - {display_date}", 'overall_weather', data_row)
            cards.append(card)
        return cards

    except Exception as e:
        logging.error(f'Error generating weekly weather cards: {e}')
        return None

def create_weather_information_card(date_label, col, data_row=None):
    if data_row is None:
        return dbc.Card([
            html.Div(date_label, className='card-header'),
            dbc.CardBody('No Data', className='text-muted')
        ])
    
    description = data_row.get(col)
    icon = get_weather_icon(description)

    return dbc.Card([
        html.P(date_label, className='card-header'),
        dbc.CardBody([
            dbc.Col(
                html.Img(src=f'assets/icons/{icon}', className='daily-weather-icon'),
                dbc.Row([
                    dbc.Col(
                        html.Img(src=f'assets/icons/{icon}', className='daily-information-icon'),
                        html.P(f"{data_row['sunrise']}")
                        ),
                    dbc.Col(
                        html.Img(src=f'assets/icons/{icon}', className='daily-information-icon'),
                        html.P(f"{data_row['sunset']}")
                    )
                ])
            , className='information-logo'),
            dbc.Col(

            )
        ])
    ], className='weather-information-card')
    
# def create_hour_picker_card(hour_label, data_row=None):
#     if data_row is None:
#         return dbc.Card(
#             dbc.CardBody([
#                 html.P(hour_label, className='hour-value text-muted'),
#             ]),
#             id=f"hour-card-{hour_label}",   
#             className="hour-picker-card",  
#             n_clicks=0
#         )
    
#     return dbc.Card(
#         dbc.CardBody([
#             html.P(hour_label, className='hour-value'),
#         ]), 
#         id=f"hour-card-{hour_label}",   
#         className="hour-picker-card",  
#         n_clicks=0
#     )

def create_hour_picker_card(hour_label, data_row=None):
    return dbc.Card(
        dbc.CardBody([
            html.Div(
                hour_label,
                id={'type': 'hour-card', 'index': hour_label},
                className="hour-value",
                n_clicks=0
            )
        ]),
        className="hour-picker-card"
    )

def generate_hour_picker_card(df, create_hour_picker_func):
    try:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

        cards = []
        hours = [datetime.strptime(f"{h:02d}:00", "%H:%M") for h in range(24)]
        for hour in hours:
            data_row = None
            if df is not None and not df.empty:
                match = df[df['time'].dt.hour == hour.hour]
                if not match.empty:
                    data_row = match.iloc[0]

            card = create_hour_picker_func(hour.strftime("%H:%M"), data_row)
            cards.append(card)
        return cards

    except Exception as e:
        logging.error(f'Error generating hour picker cards: {e}')
        return None

# def create_hour_picker_card(hour_label, data_row=None):
#     return dbc.Card(
#         dbc.CardBody([
#             html.Div(hour_label, className="hour-value")
#         ]),
#         className="hour-picker-card"
#     )

# def generate_hour_picker_card(df, create_hour_picker_func):
#     try:
#         df["time"] = pd.to_datetime(df["time"], errors="coerce")
#         cards = []
#         hours = [datetime.strptime(f"{h:02d}:00", "%H:%M") for h in range(24)]
#         for hour in hours:
#             data_row = None
#             if df is not None and not df.empty:
#                 match = df[df['time'].dt.hour == hour.hour]
#                 if not match.empty:
#                     data_row = match.iloc[0]

#             inner_card = create_hour_picker_func(hour.strftime("%H:%M"), data_row)

#             # wrapper has pattern-matching id and n_clicks so Dash can listen to it
#             wrapper = html.Div(
#                 inner_card,
#                 id={'type': 'hour-card', 'index': hour.strftime("%H:%M")},
#                 n_clicks=0,
#                 className='hour-picker-wrapper'
#             )
#             cards.append(wrapper)
#         return cards

#     except Exception as e:
#         logging.error(f'Error generating hour picker cards: {e}')
#         return None