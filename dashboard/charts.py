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
        font=dict(size=12),
        xaxis_title=None, 
        yaxis_title=None
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

def create_combined_bar_line_chart(df, bar_x, bar_y, line_ys, title, bar_color=None, line_colors=None):
    fig = px.bar(
        df,
        x=bar_x,
        y=bar_y,
        color_discrete_sequence=[bar_color] if bar_color else None
    )
    if isinstance(line_ys, str):
        line_ys = [line_ys]

    for i, col in enumerate(line_ys):
        if col in df.columns:
            fig.add_scatter(
                x=df[bar_x],
                y=df[col],
                mode="lines+markers",
                name=col,
                line=dict(color=line_colors[i]) if line_colors and i < len(line_colors) else None,
                yaxis="y2"
            )
    fig.update_layout(
        title=title,
        margin=dict(l=20, r=20, t=40, b=20),
        font=dict(size=12),
        legend=dict(
        orientation="h",
        y=-0.2,
        x=0.5,
        xanchor="center",
        yanchor="top"
        ),
        xaxis=dict(title=None),
        yaxis=dict(
            title="Precipitation (mm)",
            showgrid=False
        ),
        yaxis2=dict(
            title="Temperature (°C)", 
            overlaying="y",
            side="right",
            showgrid=False
        ),
        barmode="group"
    )
    return dbc.Card(
        dbc.CardBody(
            dcc.Graph(figure=fig, className="chart-graph")
        ),
        className="chart-card"
    )
