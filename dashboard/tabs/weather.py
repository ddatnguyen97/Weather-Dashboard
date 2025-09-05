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
        dcc.Store(id='stored-date', data=None),
        # html.Div(id='message-container', children="Please select a date to view the weather data.", className='placeholder-text'),
        html.Div(id='weather-cards-container'),
        html.Div(id='weather-charts-container'),
    ], className='weather-layout')


@callback(
    Output('weather-cards-container', 'children'),
    Output('weather-charts-container', 'children'),
    Output('stored-date', 'data'),
    # Output('message-container', 'children'),
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

        weekly_data = get_weekly_weather_data(selected_date, table_name, project_id)

        cards = [
            dbc.Col(create_weather_card(row['date'], 'overall_weather', row))
            for _, row in weekly_data.iterrows()
        ]
        cards_layout = dbc.Row(cards, className='weather-card-row')

        y_axis = 'precipitation'
        x_axis = 'date'
        color = ['#43C4E3']
        bar_chart = create_bar_chart(weekly_data, x_axis, y_axis, color)

        r = 'wind_speed'
        theta_val = 'daily_frequent_direction'
        color_val = 'wind_gusts'
        color_continuous = 'Darkmint'
        category_order_val = 'daily_frequent_direction'
        hover_val = 'date'
        radar_chart = create_radar_chart(weekly_data, r, theta_val, color_val, color_continuous, category_order_val, hover_val)

        charts_layout = dbc.Row([
            dbc.Col(bar_chart),
            dbc.Col(radar_chart)
        ], className='chart-row')

        return cards_layout, charts_layout, selected_date

    except Exception as e:
        logging.error(f"Callback error: {e}")
        return (
        html.Div("Failed to update cards."),
        html.Div("Failed to load chart."),
        stored_date 
        )
