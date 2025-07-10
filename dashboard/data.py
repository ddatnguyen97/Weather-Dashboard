from google.cloud import bigquery as bq
import pandas as pd
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_CREDENTIALS')

query = f"""
    select
        d.date,
        d.quarter,
        d.month,
        d.year,
        t.time,
        ts.name as is_day,
        wc.name as weather_code,
        hw.temperature_2m,
        hw.relative_humidity_2m,
        hw.dew_point_2m,
        hw.apparent_temperature,
        hw.precipitation,
        hw.cloud_cover,
        hw.wind_speed_10m,
        hw.wind_gusts_10m,
        hw.wind_direction_10m,
        hw.sunshine_duration,
        dw.sunrise,
        dw.sunset,
        dw.daylight_duration
    from
        `{os.getenv('HOURLY_WEATHER_TABLE')}` as hw
    join
        `{os.getenv('TIMESHIFT_TABLE')}` as ts
        on hw.is_day = ts.id
    join
        `{os.getenv('DATE_TABLE')}` as d
        on hw.date_id = d.id
    join
        `{os.getenv('TIME_TABLE')}` as t
        on hw.time_id = t.id
    join
        `{os.getenv('WEATHER_CODE_TABLE')}` as wc
        on hw.weather_code = wc.id
    join
        `{os.getenv('DAILY_WEATHER_TABLE')}` as dw
        on hw.date_id = dw.date_id
""" 

def fetch_data(query):
    try:
        client = bq.Client()
        df = client.query(query).to_dataframe()
        logging.info(f"Fetched {df.shape[0]} rows from BigQuery.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    
if __name__ == "__main__":
    data = fetch_data(query)
    if not data.empty:
        logging.info(f"Data fetched successfully with {len(data)} rows.")
    else:
        logging.warning("No data fetched.")    