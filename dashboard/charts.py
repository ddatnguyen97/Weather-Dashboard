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