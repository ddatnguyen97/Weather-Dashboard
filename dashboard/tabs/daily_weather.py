from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.components.cards import create_weather_card
from dashboard.metrics import get_weekly_weather_data
from dashboard.charts import create_bar_chart, create_radar_chart

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

def layout():
    return html.Div([
        # dcc.Store(id='stored-date', data=None),
        html.H2('This function is under construction. Please check back later!')
    ], className='weather-layout')

# @callback(
#     Output('weather-information-container', 'children'),
#     State('stored-date', 'data'),
#     prevent_initial_call=False
# )
# def update_daily_weather_layout(stored_date):
    