import plotly.express as px
from dash import dcc
import dash_bootstrap_components as dbc

def create_bar_chart(df, x, y, title, color_discrete_sequence):
    fig = px.bar(
        df,
        x=x,
        y=y,
        color_discrete_sequence=color_discrete_sequence
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        title=title,
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

def create_radar_chart(df,
                        radial,
                        theta_val,
                        color_val, 
                        color_continuous, 
                        category_order_val,
                        hover_val,
                        title):
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    fig = px.bar_polar(
        df,
        r=radial,
        theta=theta_val,
        color=color_val,
        color_continuous_scale=color_continuous,
        category_orders={category_order_val: directions},
        hover_name=hover_val,
        hover_data={
            hover_val: True,
            radial: True,
            theta_val: True,
            color_val: True
        }
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        title=title
    )

    return dbc.Card(
        [
            dbc.CardBody(
                dcc.Graph(figure=fig, className="chart-graph")
            )
        ],
        className="chart-card"
    )
