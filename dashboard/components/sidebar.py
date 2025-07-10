from dash import html
import dash_bootstrap_components as dbc

# def create_sidebar():
#     return dbc.Nav([
#             html.Img(
#                     src="/assets/icons/Weather Talks Logo.png",
#                     className="sidebar-logo"
#                 ),
#             dbc.NavLink("Daily Weather",
#                         href="daily-weather",
#                         id="tab-weather", 
#                         active="exact", 
#                         className="sidebar-link"),
#             dbc.NavLink("Air Quality Index", 
#                         href="daily-aqi", 
#                         id="tab-aqi", 
#                         active="exact", 
#                         className="sidebar-link"),
#         ],
#         vertical=True,
#         pills=True,   
# )

def create_sidebar():
    return html.Div([
        html.Div(
            html.Img(
                src="/assets/icons/Weather Talks Logo.png",
                className="sidebar-logo"
            ),
            className="sidebar-logo-container"
        ),
        dbc.Nav([
            dbc.NavLink("Daily Weather",
                        href="/daily-weather",
                        active="exact",
                        className="sidebar-link",
                        id="tab-weather"),
            dbc.NavLink("Air Quality Index",
                        href="/daily-aqi",
                        active="exact",
                        className="sidebar-link",
                        id="tab-aqi"),
        ],
        vertical=True,
        pills=True,
        className="sidebar-nav")
    ],
    className="sidebar-container")
