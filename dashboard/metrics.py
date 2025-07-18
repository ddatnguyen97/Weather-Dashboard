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
    
def get_weekly_weather_data(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f''' 
            select 
                date,
                avg(temperature_2m) as temperature,
                avg(relative_humidity_2m) as humidity,
                sum(precipitation),
                max(overall_weather) as overall_weather
            from
                `{table_name}`
            where
                date >= '{start_date}' and date <= '{end_date}'
            group by
                date
        '''
        result = fetch_data_from_bq(query, project_id)
        df = pd.DataFrame(result)
        if df.empty:
            logging.info('No data found for the specified date range.')
            return pd.DataFrame(columns=['date', 'temperature', 'humidity', 'precipitation', 'overall_weather'])
        return df
    except Exception as e:
        logging.error(f'Error fetching weekly weather data: {e}')
        return None
    
