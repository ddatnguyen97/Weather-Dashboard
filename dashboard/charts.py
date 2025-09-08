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


def create_radar_chart(df, radial, theta_val, color_val, color_continuous, category_order_val, hover_val):
    DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    fig = px.bar_polar(
        df,
        r=radial,
        theta=theta_val,
        color=color_val,
        color_continuous_scale=color_continuous,
        category_orders={category_order_val: DIRECTIONS},
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
