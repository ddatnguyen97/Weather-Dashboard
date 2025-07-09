import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os
import logging
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

logging.basicConfig(level=logging.INFO)

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

start_date = (pd.Timestamp.now().normalize() - pd.DateOffset(days=1)).strftime('%Y-%m-%d')
end_date = (pd.Timestamp.now().normalize() - pd.DateOffset(days=1)).strftime('%Y-%m-%d')

API_URL = os.getenv('URL_PATH')
LOCATION = {'latitude': 10.762622, 'longitude': 106.660172}
DATE_RANGE = {
    'start_date': f'{start_date}',
    'end_date': f'{end_date}',
}

HOURLY_VARIABLES = [
        'temperature_2m',
        'relative_humidity_2m',
        'dew_point_2m',
        'apparent_temperature', 
        'precipitation',
        'weather_code', 
        'cloud_cover', 
        'wind_speed_10m', 
        'wind_direction_10m', 
        'wind_gusts_10m', 
        'is_day', 
        'sunshine_duration',
    ]

def fetch_weather_data():
    try:
        params = {
            **LOCATION,
            **DATE_RANGE,
            'hourly': HOURLY_VARIABLES,
            'timezone': 'auto',
            'wind_speed_unit': 'ms',
            'timeformat': 'unixtime'
        }
        responses = openmeteo.weather_api(API_URL, params=params)
        if not responses:
            logging.error('No weather data returned.')
            return None
        logging.info('Weather data fetched successfully.')
        return responses[0]
    except Exception as e:
        logging.error(f'An error occurred while fetching weather data: {e}')
        return None
    
def extract_data(response):
    try:
        hourly = response.Hourly()
        hourly_data = {
            variable: hourly.Variables(idx).ValuesAsNumpy() for idx, variable in enumerate(HOURLY_VARIABLES)
        }
        hourly_data['date'] = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit='s', utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit='s', utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive='left'
        )
        df = pd.DataFrame(hourly_data)
        logging.info(f'Extracted {df.shape[0]} hourly rows.')
        return df
    except Exception as e:
        logging.error(f'Error extracting data: {str(e)}')
        raise

def transform_data(df):
    try:
        df['date_id'] = df['date'].dt.strftime('%Y%m%d')
        df['time_id'] = df['date'].dt.strftime('%H%M')
        df['weather_code'] = df['weather_code'].astype(int).astype(str).str.zfill(2)
        df['is_day'] = df['is_day'].astype(int).astype(str).str.zfill(2)
        df.drop(columns=['date'], inplace=True)
        df['id'] = df['date_id'] + df['time_id']

        for col in ['id', 'date_id', 'time_id', 'weather_code', 'is_day']:
            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else str(x))

        for col in df.select_dtypes(include='number').columns:
            df[col] = df[col].apply(lambda x: float(x) if not pd.isnull(x) else None)

        logging.info(f'Transformed data with {df.shape[1]} columns.')
        return df
    except Exception as e:
        logging.error(f'Error transforming data: {str(e)}')
        raise

def load_data_to_bq(df, table_name, project_id):
    try:
        client = bigquery.Client(project=project_id)

        job_config = bigquery.LoadJobConfig(
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

def execute_pipeline():
    try:
        response = fetch_weather_data()
        if response is None:
            logging.error('No data to process.')
            return
        
        df = extract_data(response)
        transformed_df = transform_data(df)

        table_name = os.getenv('HOURLY_WEATHER_TABLE')
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        load_data_to_bq(transformed_df, table_name, project_id)
    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        raise

if __name__ == '__main__':
    execute_pipeline()
    logging.info('Pipeline execution completed.')
