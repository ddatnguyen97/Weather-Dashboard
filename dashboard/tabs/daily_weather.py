from dash import html, dcc, Input, Output, callback, State
import dash_bootstrap_components as dbc
import logging
from dotenv import load_dotenv
import os
from dash.exceptions import PreventUpdate
from dashboard.components.cards import create_weather_information_card, generate_hour_picker_card, create_hour_picker_card
from dashboard.metrics import get_daily_weather_data
from dashboard.charts import create_bar_chart, create_radar_chart
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
        # html.Div(id='weather-information-card'),
        html.Div(id='hour-picker-container')
    ], className='weather-information-layout')

def register_callbacks(app):
    @app.callback(
        # Output('weather-information-card', 'children'),
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

            if daily_weather is None or daily_weather.empty:
                logging.warning("No daily weather data found.")
                return html.Div("No data available for selected date.")

            hour_picker_cards = generate_hour_picker_card(daily_weather, create_hour_picker_card)
            hour_cards_layout = html.Div(hour_picker_cards, className='hour-picker-row')
            # hour_cards_layout = dbc.Row(hour_picker_cards, className='hour-picker-row')

            # hours = hours.dropna().astype(int).unique()
            # hour_picker_cards = []
            # for hour in daily_weather['time'].dt.hour.unique():
            #     hour_str = f"{int(hour):02d}:00"
            #     is_active = "active" if hour_str == (selected_hour or "00:00") else ""
            #     card = html.Div(
            #         id={'type': 'hour-picker-card', 'index': hour_str},
            #         children=create_hour_picker_card(hour_str),
            #         className=f"hour-picker-card {is_active}",
            #         n_clicks=0
            #     )
            #     hour_picker_cards.append(card)

            return hour_cards_layout

        except PreventUpdate:
            raise
        except Exception as e:
            logging.error(f"Callback error: {e}")
            return html.Div("Failed to load information.")

    # @app.callback(
    #     Output('hour-picker-container', 'children'),
    #     Input({'type': 'hour-picker-card', 'index': ALL}, 'n_clicks'),
    #     State('hour-picker-container', 'children'),
    #     prevent_initial_call=True
    # )
    # def highlight_selected_hour(n_clicks, cards):
    #     if not n_clicks or all(v is None or v == 0 for v in n_clicks):
    #         raise PreventUpdate

    #     clicked_idx = max(
    #         (i for i, v in enumerate(n_clicks) if v is not None and v > 0),
    #         key=lambda i: n_clicks[i]
    #     )

    #     # Mark the clicked card as active by injecting CSS class
    #     for i, card in enumerate(cards):
    #         if 'props' in card and 'className' in card['props']:
    #             card['props']['className'] = card['props']['className'].replace(" active", "")
    #             if i == clicked_idx:
    #                 card['props']['className'] += " active"

    #     return cards
    @app.callback(
    Output({'type': 'hour-card', 'index': ALL}, 'className'),
    Input({'type': 'hour-card', 'index': ALL}, 'n_clicks'),
    State({'type': 'hour-card', 'index': ALL}, 'id'),
    prevent_initial_call=False
)
    def highlight_hour(n_clicks, ids):
        if not n_clicks:
            raise PreventUpdate

        # mặc định = 00:00 nếu chưa click
        if all(v == 0 or v is None for v in n_clicks):
            return ["hour-value active" if id['index'] == "00:00" else "hour-value" for id in ids]

        # tìm card được click gần nhất
        clicked_idx = max(
            (i for i, v in enumerate(n_clicks) if v),
            key=lambda i: n_clicks[i],
        )
        selected_hour = ids[clicked_idx]['index']

        # update className -> chỉ highlight cái được chọn
        return ["hour-value active" if id['index'] == selected_hour else "hour-value" for id in ids]
