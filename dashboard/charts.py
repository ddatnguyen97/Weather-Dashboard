import plotly.express as px

def create_bar_chart(df, x, y, color=None):
    figure = px.bar(df, x, y, color)

    figure.update_layout(
        xaxis_title=None,  
        yaxis_title=None,  
        # width=700,         
        # height=400,        
        margin=dict(l=30, r=30, t=50, b=30),  
        font=dict(size=12),
        title='Weekly Precipitation Report'
    )

    return figure

def create_radar_chart(df, theta_val, color_val, color_continuous):
    figure = px.bar_polar(df,
                          r=None,
                          theta=theta_val,
                          color=color_val,
                          color_continuous_scale=color_continuous,
                          )
    figure.update_layout(
        title='Weekly Wind Report'
    )
    return figure