from google.cloud import bigquery as bq
import pandas as pd
import logging
from utils import get_week_range

logging.basicConfig(level=logging.INFO)

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
    
def get_daily_avg_temperature(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            select 
                date,
                avg(temperature_2m) as avg_temperature
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
            return pd.DataFrame(columns=['date', 'avg_temperature'])
        return df
    except Exception as e:
        logging.error(f'Error fetching daily avg temperature: {e}')
        return None
    
def get_daily_max_temperature(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            select 
                date,
                max(temperature_2m) as max_temperature
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
            return pd.DataFrame(columns=['date', 'max_temperature'])
        return df
    except Exception as e:
        logging.error(f'Error fetching daily max temperature: {e}')
        return None
    
def get_daily_weather_data(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            select 
                date,
                overall_weather
            from
                `{table_name}`
            where
                date >= '{start_date}' and date <= '{end_date}'
        '''
        df = fetch_data_from_bq(query, project_id)
        if df.empty:
            logging.info('No data found for the specified date range.')
            return pd.DataFrame(columns=['date', 'overall_weather'])
        return df
    except Exception as e:
        logging.error(f'Error fetching daily weather data: {e}')
        return None
    
def get_daily_precipitation(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            select 
                date,
                sum(precipitation) as total_precipitation
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
            return pd.DataFrame(columns=['date', 'total_precipitation'])
        return df
    except Exception as e:
        logging.error(f'Error fetching daily precipitation: {e}')
        return None
    
def get_daily_humidity(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            select 
                date,
                avg(relative_humidity_2m) as avg_humidity
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
            return pd.DataFrame(columns=['date', 'avg_humidity'])
        return df
    except Exception as e:
        logging.error(f'Error fetching daily humidity: {e}')
        return None
    
def get_weekly_weather_data(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f''' 
            select 
                date,
                avg(temperature_2m) as temperature,
                avg(relative_humidity_2m) as humidity,
                sum(precipitation) as precipitation,
                overall_weather
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
            return pd.DataFrame(columns=['date', 'temperature', 'humidity', 'precipitation', 'overall_weather'])
        return df
    except Exception as e:
        logging.error(f'Error fetching weekly weather data: {e}')
        return None