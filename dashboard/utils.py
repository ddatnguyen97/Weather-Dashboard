from google.cloud import bigquery as bq
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv("GG_CREDENTIALS")

project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
table_name = os.getenv('AGGREGATED_TABLE')

def get_week_range(selected_date):
    try:
        selected_date = pd.to_datetime(selected_date)
        start_of_week = selected_date - timedelta(days=selected_date.weekday())  
        end_of_week = start_of_week + timedelta(days=6) 
        return start_of_week.date(), end_of_week.date()
    except Exception as e:
        logging.error(f'Error getting week range: {e}')
        return None, None

def get_date(table_name, project_id):
    try:
        client = bq.Client(project=project_id)
        query = f'''
            select
                max(date) as current_date
            from
                `{table_name}`
        '''
        df = client.query(query).to_dataframe()
        return df['current_date'].iloc[0]
    except Exception as e:
        logging.error(f'Error fetching current date: {e}')
        return None