from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.components.cards import create_weather_information_card
from dashboard.metrics import get_daily_weather_data
from dashboard.charts import create_bar_chart, create_radar_chart

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

def layout():
    return html.Div([
        dcc.Store(id='stored-date', data=None),
        # html.Div(id='weather-information-container')
        html.H2('This function is under construction. Please check back later!')
    ], className='weather-layout')

@callback(
    Output('weather-information-container', 'children'),
    Input('global-date-picker', 'date'),
    State('stored-date', 'data'),
    prevent_initial_call=False
)
def update_weather_layout(selected_date, stored_date):
    try:
        if not selected_date:
            raise PreventUpdate

        if selected_date == stored_date and stored_date is not None:
            raise PreventUpdate
        
        # daily_weather = get_daily_weather_data(selected_date, table_name, project_id)

        # information_card = create_weather_information_card()

        # information_layout = 
        hour_picker_layout = dbc.Row([
            
        ], className='hour-picker-row')
    except Exception as e:
        logging.error(f"Callback error: {e}")
        return (
        html.Div("Failed to load information."),
        stored_date 
        )