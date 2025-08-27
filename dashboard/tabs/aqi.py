from dash import html, dcc
import dash_bootstrap_components as dbc
import logging

def layout():
    try:
        return html.Div([
            html.H2('This function is under construction. Please check back later!')
        ], className='aqi-layout')
    except Exception as e:
        logging.error(f'Error in aqi layout: {e}')
        return html.Div('Failed to load aqi tab.')