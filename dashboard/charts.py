import dash_bootstrap_components as dbc
from dash import html

# def create_daily_weather_card(date, avg_temp_icon, avg_temp_content, humidity_icon, humidity_content, precipitation_icon, precipitation_content, ):
#     return dbc.Card(
#         [
#             dbc.CardHeader(
#                 html.H6(date, className='card-header-title'),
#                 className='card-header'
#             ),
#             dbc.CardBody([
#                 dbc.Row([
#                         html.Img(src=avg_temp_icon, className='card-icon'),
#                         html.H4(avg_temp_content, className='card-content')
#                 ]),
#             ]),
#             dbc.CardBody([
#                 dbc.Row([
#                         html.Img(src=humidity_icon, className='card-icon'),
#                         html.H4(humidity_content, className='card-content')
#                 ]),
#             ]),
#             dbc.CardBody([
#                 dbc.Row([
#                         html.Img(src=precipitation_icon, className='card-icon'),
#                         html.H4(precipitation_content, className='card-content')
#                 ]),
#             ]),
#         ],
#         className='weather-card'
#     )

def create_weather_card(date, data_row=None):
    if data_row is None:
        return dbc.Card([
            dbc.CardHeader(date),
            dbc.CardBody('No Data', className='text-muted')
        ],)
    
    return dbc.Card([
        dbc.CardHeader(date),
        dbc.CardBody([
            html.Img(src='assets/icons/thermometer.gif', className='card-icon'),
            html.P(f"Temperature: {data_row['avg_temperature']:.2f}°C"),
            html.P(f"Humidity: {data_row['avg_humidity']:.2f}%"),
            html.P(f"Precipitation: {data_row['avg_precipitation']:.2f}mm")
        ])
    ], className='weather-card')