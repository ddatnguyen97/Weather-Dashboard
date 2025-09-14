from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.metrics import get_daily_weather_data
from dashboard.utils import get_max_date, get_min_time_of_day
from dashboard.components.slicer import create_hour_picker
from dashboard.components.cards import create_sun_times_card, create_min_max_temperature_card
from dash import ALL
import dash

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

def layout(app):
    register_callbacks(app)

    return html.Div([
        dcc.Store(id='daily-stored-date', data=None),
        dcc.Store(id='selected-hour', data="00:00"),
        html.Div(id='hour-picker-container')
    ], className='weather-information-layout')

def register_callbacks(app):
    @app.callback(
        Output('hour-picker-container', 'children'),
        Input('global-date-picker', 'date'),
        State('daily-stored-date', 'data'),
        State('selected-hour', 'data'),
        prevent_initial_call=False
    )
    def update_weather_layout(selected_date, stored_date, selected_hour):
        try:
            if not selected_date:
                raise PreventUpdate

            if selected_date == stored_date and stored_date is not None:
                raise PreventUpdate

            daily_weather = get_daily_weather_data(selected_date, table_name, project_id)
            initial_date = get_max_date(table_name, project_id)
            initial_hour = get_min_time_of_day(table_name, initial_date, project_id)
            hour_picker = create_hour_picker(initial_hour)
            sun_times_card = create_sun_times_card(daily_weather, selected_date, project_id, table_name)
            min_max_temperature_card = create_min_max_temperature_card(daily_weather, selected_date, project_id, table_name)

            if daily_weather is None or daily_weather.empty:
                logging.warning("No daily weather data found.")
                return html.Div("No data available for selected date.")
        
            hour_cards_layout = dbc.Row([
                dbc.Col(
                    sun_times_card,
                    className="sun-times-container"
                ),
                dbc.Col(
                    min_max_temperature_card,
                    className="min-max-temperature-container"
                ),
                dbc.Col(
                    hour_picker,
                    className="hour-picker-row"
                )
            ], )
            return hour_cards_layout

        except PreventUpdate:
            raise
        except Exception as e:
            logging.error(f"Callback error: {e}")
            return html.Div("Failed to load information.")
