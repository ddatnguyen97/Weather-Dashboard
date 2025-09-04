import plotly.express as px
from dash import html, dcc

# def create_bar_chart(df, x, y, color_discrete_sequence):
#     figure = px.bar(df,
#                      x, 
#                      y, 
#                      color_discrete_sequence=color_discrete_sequence
#                     )
#     figure.update_layout(
#         xaxis_title=None,  
#         yaxis_title=None,  
#         margin=dict(l=30, r=30, t=50, b=30),  
#         font=dict(size=12),
#         title='Weekly Precipitation Report'
#     )
#     return dcc.Graph(figure=figure, className='bar-chart-layout')

# def create_radar_chart(df, theta_val, color_val, color_continuous):
#     figure = px.bar_polar(df,
#                           r=None,
#                           theta=theta_val,
#                           color=color_val,
#                           color_continuous_scale=color_continuous,
#                           hover_name="date",  
#                           hover_data={
#                             "date": True,
#                             theta_val: True,
#                             color_val: True
#                             }
#                         )
#     figure.update_layout(
#         margin=dict(l=20, r=20, t=40, b=20),
#         title='Weekly Wind Report'
#     )
#     return dcc.Graph(figure=figure, className='radar-chart-layout')

import plotly.express as px
from dash import dcc
import dash_bootstrap_components as dbc

def create_bar_chart(df, x, y, color_discrete_sequence):
    fig = px.bar(
        df,
        x=x,
        y=y,
        color_discrete_sequence=color_discrete_sequence
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        title="Weekly Precipitation Report",
        xaxis_title=None,
        yaxis_title=None,
        font=dict(size=12)
    )

    return dbc.Card(
        [
            dbc.CardBody(
                dcc.Graph(figure=fig, className="chart-graph")
            )
        ],
        className="chart-card"
    )


def create_radar_chart(df, theta_val, color_val, color_continuous):
    fig = px.bar_polar(
        df,
        r=None,
        theta=theta_val,
        color=color_val,
        color_continuous_scale=color_continuous,
        hover_name="date",
        hover_data={
            "date": True,
            theta_val: True,
            color_val: True
        }
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        title="Weekly Wind Report"
    )

    return dbc.Card(
        [
            dbc.CardBody(
                dcc.Graph(figure=fig, className="chart-graph")
            )
        ],
        className="chart-card"
    )
