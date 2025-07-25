from components.cards import create_weather_card
from metrics import get_weekly_weather_data
from charts import create_bar_chart
from utils import get_date
from dash import html, dcc
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
            dbc.Col(create_weather_card(row['date'], 'overall_weather', row)) 
            for _, row in weekly_data.iterrows()
        ]

        y_axis = weekly_data['precipitation']
        x_axis = weekly_data['date']
        precipitation_bar_chart = create_bar_chart(weekly_data, x_axis, y_axis)
        bar_chart_component = dcc.Graph(figure=precipitation_bar_chart, id='precipitation-bar-chart', className='bar-chart-layout')

        

        return html.Div([
            html.Div(id='weather-cards-container', children=dbc.Row(weekly_cards), className='weather-card-row'),
            html.Div(id='precipitation-bar-chart-container', children=bar_chart_component, className='chart-row') 
        ], className='weather-layout')
    except Exception as e:
        logging.error(f'Error in weather layout: {e}')
        return html.Div('Failed to load weather tab.')
    
@callback(
    Output('weather-cards-container', 'children'),
    Output('precipitation-bar-chart-container', 'children'),
    Input('global-date-picker', 'date'),
)
def update_weather_layout(selected_date):
    try:
        if not selected_date:
            return "Please select a date.", None

        weekly_data = get_weekly_weather_data(selected_date, table_name, project_id)

        # Cards
        cards = [
            dbc.Col(create_weather_card(row['date'], 'overall_weather', row))
            for _, row in weekly_data.iterrows()
        ]
        cards_layout = dbc.Row(cards)

        # Chart
        y_axis = weekly_data['precipitation']
        x_axis = weekly_data['date']
        figure = create_bar_chart(weekly_data, x_axis, y_axis)
        graph_component = dcc.Graph(figure=figure, className='bar-chart-layout')
        
        return cards_layout, graph_component

    except Exception as e:
        logging.error(f"Callback error: {e}")
        return "Failed to update cards.", None

def update_weather_dashboard(selected_date):
    cards, figure = update_weather_layout(selected_date)
    return cards, dcc.Graph(id='precipitation-bar-chart', figure=figure)
