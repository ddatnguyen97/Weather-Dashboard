from dash import html
import dash_bootstrap_components as dbc
from dashboard.components.slicer import create_date_picker
from dashboard.utils import get_date
from dotenv import load_dotenv
import os

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv("GG_CREDENTIALS")

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')

current_date = get_date(table_name, project_id)
initial_date = get_date(table_name, project_id)
date_picker = create_date_picker(current_date, initial_date)

def create_navbar():
    return dbc.Navbar([
            html.H2('HCM City Weather Dashboard', className='navbar-title'),
            date_picker],
    className='navbar-container',
)