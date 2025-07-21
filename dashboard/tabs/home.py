from dash import html, dcc
import dash_bootstrap_components as dbc
import logging

def layout():
    try:
        return html.Div([
            html.H2('This is my personal project. Please enjoy exploring!')
        ], className='home-layout')
    except Exception as e:
        logging.error(f'Error in homw layout: {e}')
        return html.Div('Failed to load home tab.')