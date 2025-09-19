from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.metrics import get_daily_weather_data, get_daily_precipitation_data
from dashboard.utils import get_max_date, get_min_time_of_day, WEATHER_INFORMATION_ICON_MAP
from dashboard.components.slicer import create_hour_picker
from dashboard.components.cards import create_sun_times_card, create_min_max_temperature_card, create_weather_information_card
from dashboard.charts import create_combined_bar_line_chart, create_bar_chart
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
    dcc.Store(id='selected-hour', data=None),

    dcc.Loading(
        type="circle",
        children=html.Div([
            html.Div(id='hour-picker-container'),
            html.Div(id='weather-information-container'),
            html.Div(id='hourly-weather-charts-container')
        ])
    )
    ], className='weather-information-layout')

def register_callbacks(app):
    @app.callback(
        Output('hour-picker-container', 'children'),
        Input('global-date-picker', 'date'),
        State('daily-stored-date', 'data'),
        State('selected-hour', 'data'),
        prevent_initial_call=False
    )
    def update_weather_layout(selected_date, stored_date, stored_hour):
        try:
            if not selected_date:
                raise PreventUpdate

            if selected_date == stored_date and stored_date is not None:
                raise PreventUpdate

            daily_weather = get_daily_weather_data(selected_date, table_name, project_id)
            if daily_weather is None or daily_weather.empty:
                logging.warning("No daily weather data found.")
                return html.Div("No data available for selected date.")

            if stored_hour:
                initial_hour = stored_hour
            else:
                initial_date = get_max_date(table_name, project_id)
                initial_hour = get_min_time_of_day(table_name, initial_date, project_id)

            hour_picker = create_hour_picker(initial_hour)
            sun_times_card = create_sun_times_card(daily_weather, selected_date, project_id, table_name)
            min_max_temperature_card = create_min_max_temperature_card(daily_weather, selected_date, project_id, table_name)

            hour_cards_layout = dbc.Row([
                dbc.Col(sun_times_card),
                dbc.Col(min_max_temperature_card),
                dbc.Col(hour_picker, className="hour-picker-container")
            ], className='hour-card-row')

            return hour_cards_layout

        except PreventUpdate:
            raise
        except Exception as e:
            logging.error(f"Callback error: {e}")
            return html.Div("Failed to load information.")

    @app.callback(
            Output('selected-hour', 'data'),
            Input('global-hour-picker', 'value'),
            prevent_initial_call=False
        )
    def sync_hour_to_store(selected_hour):
        if not selected_hour:
            raise PreventUpdate
        return selected_hour

    @app.callback(
        Output('weather-information-container', 'children'),
        Input('global-date-picker', 'date'),
        Input('selected-hour', 'data'),
        prevent_initial_call=False
    )
    def update_information_card(selected_date, selected_hour):
        if not selected_date or not selected_hour:
            raise PreventUpdate

        df = get_daily_weather_data(selected_date, table_name, project_id)
        if df is None or df.empty:
            return dbc.CardBody("No data available for selected date.")

        information_card = create_weather_information_card(
            df, selected_date, selected_hour
        )

        information_card_layout = dbc.Row(
                                    (information_card,),
                                    className='weather-information-card-row'
                                )
        return information_card_layout
    
    @app.callback(
        Output('hourly-weather-charts-container', 'children'),
        Input('global-date-picker', 'date'),
        prevent_initial_call=False
    )
    def update_hourly_weather_chart(selected_date):
        try:
            if not selected_date:
                logging.warning("No date selected from global-date-picker")
                return html.Div("Please select a date to view the chart.")

            selected_date_str = str(selected_date)
            precipitation_data = get_daily_precipitation_data(
                selected_date_str, table_name, project_id
            )

            if precipitation_data is None or precipitation_data.empty:
                logging.warning(f"No daily precipitation data found for {selected_date_str}")
                return html.Div(f"No data available for {selected_date_str}.")

            precipitation_data['time'] = precipitation_data['time'].astype(str)

            bar_x='time'
            bar_y='precipitation'
            title=f"Hourly Precipitation Report"
            bar_color='#43C4E3'
            line_ys=['temperature', 'apparent_temperature']
            line_colors=['#FFA07A', '#C4175C']
            
            combined_chart = create_combined_bar_line_chart(
                precipitation_data,
                bar_x=bar_x,
                bar_y=bar_y,
                title=title,
                bar_color=bar_color,
                line_ys=line_ys,
                line_colors=line_colors
            )
            chart_layout = dbc.Row(combined_chart,
                                    className='daily-weather-chart-row'
                                    )
            return chart_layout

        except Exception as e:
            logging.error(f"Callback error while building hourly chart: {e}")
            return html.Div("Failed to load chart.")

