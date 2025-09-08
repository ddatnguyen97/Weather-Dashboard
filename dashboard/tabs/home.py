from dash import html, dcc
import dash_bootstrap_components as dbc
import logging

def layout():
    try:
        return html.Div([
            html.H2('This is my personal project. Please enjoy exploring!'),
            html.P('Min date from the db: 2020-01-01'),
            html.P('Max date from the db: 2025-08-24')
        ], className='home-layout')
    except Exception as e:
        logging.error(f'Error in home layout: {e}')
        return html.Div('Failed to load home tab.')