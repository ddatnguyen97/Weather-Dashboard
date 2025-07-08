import pandas as pd
import logging
from dotenv import load_dotenv
from google.cloud import bigquery as bq
import os
from pytz import timezone

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

logging.basicConfig(level=logging.INFO)

def get_time(start_time, end_time):
    try:
        st = pd.to_datetime(start_time)
        et = pd.to_datetime(end_time)
        time_range = pd.date_range(start=st, end=et, freq='H')
        df = pd.DataFrame({'time': time_range})
        logging.info(f'Data extracted: {df.shape[0]} rows')
        return df
    except Exception as e:
        logging.error(f'Error extracting time: {e}')
        raise

def transform_data(df, target_timezone='UTC'):
    try:
        if not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'])
        
        tz = timezone(target_timezone)
        df['time'] = df['time'].dt.tz_localize(tz)
        df['id'] = df['time'].dt.strftime('%H%M').astype(str)
        df['hour'] = df['time'].dt.hour

        df['id'] = df['id'].str.zfill(4)
        df = df.drop_duplicates()
        logging.info(f'dropped duplicates: {df.shape[0]} rows')
        return df
    except Exception as e:
        logging.error(f'error: {e}')
        raise

def load_data_to_bq(df, table_name, project_id):
    try:
        client = bq.Client(project=project_id)
        job_config = bq.LoadJobConfig(
            write_disposition='WRITE_APPEND',
        )

        job = client.load_table_from_dataframe(
            df,
            table_name,
            job_config=job_config
        )
        job.result()
        logging.info(f'Loaded {df.shape[0]} rows into table {table_name}.')
    except Exception as e:
        logging.error(f'Error loading data to BigQuery: {e}')
        raise

def execute_pipeline(start_time, end_time, table_name, project_id):
    df = get_time(start_time, end_time)
    df = transform_data(df)
    load_data_to_bq(df, table_name, project_id)

if __name__ == "__main__":
    start_time = '00:00:00'
    end_time = '23:59:59'
    table_name = os.getenv('TIME_TABLE')
    project_id = os.getenv('PROJECT_ID')
    execute_pipeline(start_time, end_time, table_name, project_id)