from components.cards import create_weather_card
from metrics import get_weekly_weather_data
from utils import get_date
from dash import html
import dash_bootstrap_components as dbc
from dash import Input, Output, callback
from utils import get_date
from dotenv import load_dotenv
import os
import logging

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

def layout():
    try:
        selected_date = get_date(table_name, project_id)
        weekly_data = get_weekly_weather_data(selected_date, table_name, project_id)
        weekly_cards = [
            dbc.Col(create_weather_card(row['date'], 'overall_weather', row),) 
            for _, row in weekly_data.iterrows()
        ]
        return html.Div([
            html.Div(id='weather-cards-container', children=dbc.Row(weekly_cards)), 
        ], className='weather-layout')
    except Exception as e:
        logging.error(f'Error in weather layout: {e}')
        return html.Div('Failed to load weather tab.')
    
@callback(
    Output('weather-cards-container', 'children'),
    Input('global-date-picker', 'date'),
)
def update_weather_cards(selected_date):
    try:
        if not selected_date:
            return "Please select a date."

        weekly_data = get_weekly_weather_data(selected_date, table_name, project_id)
        cards = [
            dbc.Col(create_weather_card(row['date'], 'overall_weather', row))
            for _, row in weekly_data.iterrows()
        ]
        return dbc.Row(cards, className='weather-card-row')

    except Exception as e:
        logging.error(f"Callback error: {e}")
        return "Failed to update cards."
