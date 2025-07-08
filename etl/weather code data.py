import pandas as pd
import logging
import os
from dotenv import load_dotenv
from google.cloud import bigquery as bq

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

logging.basicConfig(level=logging.INFO)

def extract_data(file_path, sheet_name):
    try:
        df = pd.read_excel(file_path, sheet_name, engine='openpyxl')
        logging.info(f'{file_path} has been read successfully.')
        return df
    
    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        return None

def transform_data(df):
    try:
        df['id'] = df['id'].astype(str)
        df['id'] = df['id'].apply(lambda x: '0' + x if len(x) == 1 else x)
        return df
    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        raise

def load_data_to_dw(df, table_name, connection_string):
    try:
        client = bq.Client()
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

def execute_pipeline(file_path, sheet_name, table_name):
    try:
        df = extract_data(file_path, sheet_name)
        if df is not None:
            transformed_df = transform_data(df)
            load_data_to_dw(transformed_df, table_name, None)
    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        raise

if __name__ == '__main__':
    file_path = 'weather mapping.xlsx'
    sheet_name = 'weather_code'
    table_name = os.getenv('WEATHER_CODE_TABLE')
    execute_pipeline(file_path, sheet_name, table_name)