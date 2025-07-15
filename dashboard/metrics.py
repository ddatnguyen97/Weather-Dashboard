from google.cloud import bigquery as bq
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

def get_week_range(selected_date):
    try:
        selected_date = pd.to_datetime(selected_date)
        start_of_week = selected_date - timedelta(days=selected_date.weekday())  
        end_of_week = start_of_week + timedelta(days=6) 
        return start_of_week.date(), end_of_week.date()
    except Exception as e:
        logging.error(f'Error getting week range: {e}')
        return None, None

def fetch_data_from_bq(query, project_id):
    try:
        client = bq.Client(project=project_id)
        df = client.query(query).to_dataframe()
        logging.info(f'Fetched {df.shape[0]} rows from BigQuery.')
        return df
    except Exception as e:
        logging.error(f'Error fetching data from BigQuery: {e}')
        return pd.DataFrame()

def get_daily_min_temperature(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            select 
                date,
                min(temperature_2m) as min_temperature
            from
                `{table_name}`
            where
                date >= '{start_date}' and date <= '{end_date}'
            group by
                date
        '''
        df = fetch_data_from_bq(query, project_id)
        if df.empty:
            logging.info('No data found for the specified date range.')
            return pd.DataFrame(columns=['date', 'min_temperature'])
        return df
    except Exception as e:
        logging.error(f'Error fetching daily min temperature: {e}')
        return None
    
selected_date = '2020-01-01'  # Example date
daily_min_temp_df = get_daily_min_temperature(selected_date, table_name, project_id)
print(daily_min_temp_df)