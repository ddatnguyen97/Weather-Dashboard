import logging
from dashboard.utils import get_week_range, get_weather_icon, get_sun_times, get_sun_times_icon
from dashboard.utils import get_min_max_temperature, get_min_max_temperature_icon, get_weather_information_icon
from dashboard.utils import WEATHER_INFORMATION_ICON_MAP
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

def create_weather_information_card(df, selected_date, selected_hour):
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce').dt.date
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.time

    selected_date_dt = pd.to_datetime(selected_date, format='%Y-%m-%d', errors='coerce').date()
    selected_hour_dt = pd.to_datetime(selected_hour, errors='coerce').time()

    match = df[(df['date'] == selected_date_dt) & (df['time'] == selected_hour_dt)]
    if match.empty:
        return dbc.Row([
            html.Div('No Data', className='card-header'),
            dbc.CardBody('No matching data found for selected time.', className='text-muted')
        ])

    data_row = match.iloc[0]

    weather_icon = get_weather_icon(data_row.get("weather", ""))
    
    return dbc.Card(
            dbc.Row([
                dbc.Col([
                    html.Img(src=f"/assets/icons/{weather_icon}", className="information-logo"),
                    html.P(f"{data_row.get('weather', 'N/A')}", className="information-weather-content")
                ], className="weather-information-col"),

                dbc.Col([
                    dbc.Row([
                        dbc.Col(html.Img(src=f"/assets/icons/{get_weather_information_icon('cloud_cover')}", className="information-icon"),
                                        className="information-icon-col"
                                        ),
                        dbc.Col(html.P(f"Cloud Cover: {data_row.get('cloud_cover', 'N/A'):.2f}%", className="information-text"), 
                                className="information-text-col")
                    ], className="weather-information-row"),
                    dbc.Row([
                        dbc.Col(html.Img(src=f"/assets/icons/{get_weather_information_icon('dew_point')}", className="information-icon"), 
                                className="information-icon-col"),
                        dbc.Col(html.P(f"Dew Point: {data_row.get('dew_point', 'N/A'):.2f}°C", className="information-text"), 
                                className="information-text-col")
                    ], className="weather-information-row"),
                    dbc.Row([
                        dbc.Col(html.Img(src=f"/assets/icons/{get_weather_information_icon('humidity')}", className="information-icon"), 
                                className="information-icon-col"),
                        dbc.Col(html.P(f"Humidity: {data_row.get('humidity', 'N/A'):.2f}%", className="information-text"), 
                                className="information-text-col")
                    ], className="weather-information-row"),
                ], className="weather-information-col"),

                dbc.Col([
                    dbc.Row([
                        dbc.Col(html.Img(src=f"/assets/icons/{get_weather_information_icon('wind_speed')}", className="information-icon"), 
                                className="information-icon-col"),
                        dbc.Col(html.P(f"Wind Speed: {data_row.get('wind_speed', 'N/A'):.2f} km/h", className="information-text"), 
                                className="information-text-col")
                    ], className="weather-information-row"),
                    dbc.Row([
                        dbc.Col(html.Img(src=f"/assets/icons/{get_weather_information_icon('wind_gusts')}", className="information-icon"), 
                                className="information-icon-col"),
                        dbc.Col(html.P(f"Wind Gusts: {data_row.get('wind_gusts', 'N/A'):.2f} km/h", className="information-text"), 
                                className="information-text-col")
                    ], className="weather-information-row"),
                    dbc.Row([
                        dbc.Col(html.Img(src=f"/assets/icons/{get_weather_information_icon('wind_direction_label')}", className="information-icon"), 
                                className="information-icon-col"),
                        dbc.Col(html.P(f"Wind Direction: {data_row.get('wind_direction_label', 'N/A')}", className="information-text"), 
                                className="information-text-col")
                    ], className="weather-information-row"),
                ], className="weather-information-col")],
            ),
            className="weather-information-card")

def create_sun_times_card(df, selected_date, project_id, table_name):
    try:
        sunrise = get_sun_times(df, table_name, selected_date, project_id)[0]
        sunrise_icon = get_sun_times_icon("sunrise")
        sunset = get_sun_times(df, table_name, selected_date, project_id)[1]
        sunset_icon = get_sun_times_icon("sunset")
        return dbc.Row([
            dbc.Col([
                    html.Img(src=f'assets/icons/{sunrise_icon}', 
                    className='sun-times-icon'),
                    html.P(f"Sunrise: {sunrise}", 
                    className='sun-times-text')
                ]),
            dbc.Col([
                    html.Img(src=f'assets/icons/{sunset_icon}', 
                    className='sun-times-icon'),
                    html.P(f"Sunset: {sunset}", 
                    className='sun-times-text')
                ])
            ], className='sun-times-card')
    except Exception as e:
        logging.error(f'Error generating hour picker cards: {e}')
        return None

def create_min_max_temperature_card(df, selected_date, project_id, table_name):
    try:
        min_temp = get_min_max_temperature(df, table_name, selected_date, project_id)[0]
        min_temp_icon = get_min_max_temperature_icon("min_temp")
        max_temp = get_min_max_temperature(df, table_name, selected_date, project_id)[1]
        max_temp_icon = get_min_max_temperature_icon("max_temp")
        return dbc.Row([
            dbc.Col([
                    html.Img(src=f'assets/icons/{min_temp_icon}', 
                    className='min-max-icon'),
                    html.P(f"Min: {min_temp:.2f}°C", 
                    className='min-text')
                ]),
            dbc.Col([
                    html.Img(src=f'assets/icons/{max_temp_icon}', 
                    className='min-max-icon'),
                    html.P(f"Max: {max_temp:.2f}°C", 
                    className='max-text')
                ])
            ], className='min-max-card')
    except Exception as e:
        logging.error(f'Error generating min/max temperature card: {e}')
        return None
    
   