import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from pytz import timezone

logging.basicConfig(level=logging.INFO)

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# start_date = (pd.Timestamp.now().normalize() - pd.DateOffset(days=1)).strftime('%Y-%m-%d')
# end_date = (pd.Timestamp.now().normalize() - pd.DateOffset(days=1)).strftime('%Y-%m-%d')

API_URL = os.getenv('URL_PATH')
LOCATION = {'latitude': 10.762622, 'longitude': 106.660172}
DATE_RANGE = {
    # 'start_date': f'{start_date}',
    # 'end_date': f'{end_date}',
    'start_date': '2020-01-01',
    'end_date': '2025-06-30',
}

DAILY_VARIABLES = [
    'weather_code',
    'sunrise',
    'sunset',
    'daylight_duration',
]

def fetch_daily_weather_data():
    try:
        params = {
            **LOCATION,
            **DATE_RANGE,
            'daily': DAILY_VARIABLES,
            'timezone': 'auto',
            'timeformat': 'unixtime'
        }
        responses = openmeteo.weather_api(API_URL, params=params)
        if not responses:
            logging.error('No daily weather data returned.')
            return None
        logging.info('Daily weather data fetched successfully.')
        return responses[0]
    except Exception as e:
        logging.error(f'Error fetching daily weather data: {e}')
        raise

def extract_data(response):
    try:
        daily = response.Daily()
        daily_data = {
            'weather_code': daily.Variables(0).ValuesAsNumpy(),
            'sunrise': daily.Variables(1).ValuesInt64AsNumpy(),
            'sunset': daily.Variables(2).ValuesInt64AsNumpy(),   
            'daylight_duration': daily.Variables(3).ValuesAsNumpy(),
        }
        daily_data['date'] = pd.date_range(
            start=pd.to_datetime(daily.Time(), unit='s', utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit='s', utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive='left'
        )
        df = pd.DataFrame(daily_data)
        logging.info(f'Extracted {df.shape[0]} daily rows.')
        return df
    except Exception as e:
        logging.error(f'Error extracting daily data: {str(e)}')
        raise

def transform_data(df, target_timezone='Asia/Ho_Chi_Minh'):
    try:
        df['date_id'] = df['date'].dt.strftime('%Y%m%d').astype(str)
        df['weather_code'] = df['weather_code'].astype(int).astype(str).str.zfill(2)
        df.drop(columns=['date'], inplace=True)
        tz = timezone(target_timezone)
        df['sunrise'] = pd.to_datetime(df['sunrise'], unit='s', utc=True).dt.tz_convert(tz).dt.strftime('%H:%M')
        df['sunset'] = pd.to_datetime(df['sunset'], unit='s', utc=True).dt.tz_convert(tz).dt.strftime('%H:%M')
        logging.info(f'Transformed data: {df.shape[0]} rows.')
        return df
    except Exception as e:
        logging.error(f'Error transforming daily data: {str(e)}')
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
        logging.error(f'Error loading daily data to BigQuery: {e}')
        raise

def execute_pipeline():
    try:
        response = fetch_daily_weather_data()
        if response is None:
            logging.error('No data to process.')
            return

        df = extract_data(response)
        transformed_df = transform_data(df)
        table_name = os.getenv('DAILY_WEATHER_TABLE')
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        load_data_to_bq(transformed_df, table_name, project_id)
    except Exception as e:
        logging.error(f'An error occurred: {str(e)}')
        raise

if __name__ == "__main__":
    execute_pipeline()
    logging.info('Daily weather data ETL pipeline completed successfully.')