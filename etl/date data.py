import pandas as pd
import logging
from dotenv import load_dotenv
from google.cloud import bigquery as bq
import os

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

logging.basicConfig(level=logging.INFO)

def get_date(start_date, end_date):
    try:
        sd = pd.to_datetime(start_date)
        ed = pd.to_datetime(end_date)
        date_range = pd.date_range(start=sd, end=ed, freq='D')
        df = pd.DataFrame({'date': date_range})
        logging.info(f'Data extracted: {df.shape[0]} rows')
        return df
    except Exception as e:
        logging.error(f'Error extracting date: {e}')
        raise

def transform_data(df):
    try:
        df['id'] = df['date'].dt.strftime('%Y%m%d').astype(str)
        df['year'] = df['date'].dt.year
        df['quarter'] = df['date'].dt.quarter
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        logging.info(f'Data transformed: {df.shape[0]} rows')
        return df
    except Exception as e:
        logging.error(f'Error transforming data: {e}')
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

def execute_pipeline(start_date, end_date, table_name, project_id):
    df = get_date(start_date, end_date)
    trans_df = transform_data(df)
    load_data_to_bq(trans_df, table_name, project_id)

if __name__ == "__main__":
    start_date = '2020-01-01'
    end_date = '2025-06-30'
    table_name = os.getenv('DATE_TABLE')
    project_id = os.getenv('BQ_PROJECT_ID')
    execute_pipeline(start_date, end_date, table_name, project_id)