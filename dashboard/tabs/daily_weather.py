from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.components.cards import create_weather_information_card, generate_hour_picker_card, create_hour_picker_card
from dashboard.metrics import get_daily_weather_data
from dashboard.charts import create_bar_chart, create_radar_chart

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

def layout(app):
    register_callbacks(app)

    return html.Div([
        dcc.Store(id='daily-stored-date', data=None),
        # html.Div(id='weather-information-card'),
        html.Div(id='hour-picker-container')
    ], className='weather-information-layout')

def register_callbacks(app):
    @app.callback(
        # Output('weather-information-card', 'children'),
        Output('hour-picker-container', 'children'),
        Input('global-date-picker', 'date'),
        State('daily-stored-date', 'data'),
        prevent_initial_call=False
    )
    def update_weather_layout(selected_date, stored_date):
        try:
            if not selected_date:
                raise PreventUpdate

            if selected_date == stored_date and stored_date is not None:
                raise PreventUpdate

            daily_weather = get_daily_weather_data(selected_date, table_name, project_id)

            if daily_weather is None or daily_weather.empty:
                logging.warning("No daily weather data found.")
                return html.Div("No data available for selected date.")

            hour_picker_cards = generate_hour_picker_card(daily_weather, create_hour_picker_card)
            hour_cards_layout = html.Div(hour_picker_cards, className='hour-picker-row')

            return hour_cards_layout
        except Exception as e:
            logging.error(f"Callback error: {e}")
            return (
            html.Div("Failed to load information."),
            stored_date 
            )

# def layout():
#     return html.Div([
#         dcc.Store(id='stored-date', data=None),
#         # html.Div(id='weather-information-container')
#         html.Div(id='hour-picker-container')
#         # html.H2('This function is under construction. Please check back later!')
#     ], className='weather-information-layout')

# @callback(
#     Output('hour-picker-container', 'children'),
#     Input('global-date-picker', 'date'),
#     State('stored-date', 'data'),
#     prevent_initial_call=False
# )
# def update_weather_layout(selected_date, stored_date):
#     try:
#         if not selected_date:
#             raise PreventUpdate

#         if selected_date == stored_date and stored_date is not None:
#             raise PreventUpdate
        
#         daily_weather = get_daily_weather_data(selected_date, table_name, project_id)
        
#         hour_picker_cards = generate_hour_picker_card(daily_weather, selected_date, create_hour_picker_card)
#         hour_cards_layout = dbc.Row([dbc.Col(card) for card in hour_picker_cards], className='hour-picker-row')
#         # information_card = create_weather_information_card()

#         # information_layout = 
#         return hour_cards_layout
#     except Exception as e:
#         logging.error(f"Callback error: {e}")
#         return (
#         html.Div("Failed to load information."),
#         stored_date 
#         )