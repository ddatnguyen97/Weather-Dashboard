from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.components.cards import create_weather_card, generate_weekly_weather_cards
from dashboard.metrics import get_weekly_weather_data
from dashboard.charts import create_bar_chart, create_radar_chart

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

def layout(app):
    register_callbacks(app)

    return html.Div([
        dcc.Store(id='weekly-stored-date', data=None),
        html.Div(id='weather-cards-container'),
        html.Div(id='weather-charts-container'),
    ], className='weather-layout')

def register_callbacks(app):
    @app.callback(
        Output('weather-cards-container', 'children'),
        Output('weather-charts-container', 'children'),
        Output('weekly-stored-date', 'data'),
        Input('global-date-picker', 'date'),
        State('weekly-stored-date', 'data'),
        prevent_initial_call=False
    )
    def update_weather_layout(selected_date, stored_date):
        try:
            if not selected_date:
                raise PreventUpdate

            if selected_date == stored_date and stored_date is not None:
                raise PreventUpdate

            weekly_data = get_weekly_weather_data(selected_date, table_name, project_id)

            cards = generate_weekly_weather_cards(weekly_data, selected_date, create_weather_card)
            cards_layout = dbc.Row([dbc.Col(card) for card in cards], className='weather-card-row')

            y_axis = 'precipitation'
            x_axis = 'date'
            bar_chart_title = "Weekly Precipitation Report"
            color = ['#43C4E3']
            bar_chart = create_bar_chart(weekly_data, 
                                        x_axis, 
                                        y_axis, 
                                        bar_chart_title, 
                                        color)

            r = 'wind_speed'
            theta_val = 'daily_frequent_direction'
            color_val = 'wind_gusts'
            color_continuous = 'Darkmint'
            category_order_val = 'daily_frequent_direction'
            hover_val = 'date'
            radar_chart_title = "Weekly Wind Report"
            radar_chart = create_radar_chart(weekly_data,
                                            r, 
                                            theta_val,
                                            color_val, 
                                            color_continuous, 
                                            category_order_val, 
                                            hover_val, 
                                            radar_chart_title)

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

# def layout():
#     return html.Div([
#         dcc.Store(id='stored-date', data=None),
#         html.Div(id='weather-cards-container'),
#         html.Div(id='weather-charts-container'),
#     ], className='weather-layout')


# @callback(
#     Output('weather-cards-container', 'children'),
#     Output('weather-charts-container', 'children'),
#     Output('stored-date', 'data'),
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

#         weekly_data = get_weekly_weather_data(selected_date, table_name, project_id)

#         cards = generate_weekly_weather_cards(weekly_data, selected_date, create_weather_card)
#         cards_layout = dbc.Row([dbc.Col(card) for card in cards], className='weather-card-row')

#         y_axis = 'precipitation'
#         x_axis = 'date'
#         bar_chart_title = "Weekly Precipitation Report"
#         color = ['#43C4E3']
#         bar_chart = create_bar_chart(weekly_data, 
#                                     x_axis, 
#                                     y_axis, 
#                                     bar_chart_title, 
#                                     color)

#         r = 'wind_speed'
#         theta_val = 'daily_frequent_direction'
#         color_val = 'wind_gusts'
#         color_continuous = 'Darkmint'
#         category_order_val = 'daily_frequent_direction'
#         hover_val = 'date'
#         radar_chart_title = "Weekly Wind Report"
#         radar_chart = create_radar_chart(weekly_data,
#                                         r, 
#                                         theta_val,
#                                         color_val, 
#                                         color_continuous, 
#                                         category_order_val, 
#                                         hover_val, 
#                                         radar_chart_title)

#         charts_layout = dbc.Row([
#             dbc.Col(bar_chart),
#             dbc.Col(radar_chart)
#         ], className='chart-row')

#         return cards_layout, charts_layout, selected_date
#     except Exception as e:
#         logging.error(f"Callback error: {e}")
#         return (
#         html.Div("Failed to update cards."),
#         html.Div("Failed to load chart."),
#         stored_date 
#         )
