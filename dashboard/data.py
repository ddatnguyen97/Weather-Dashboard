'''
This script aggregates hourly weather data into daily summaries and loads the results into a BigQuery table.
This script is created because I can't save the data in BigQuery directly due to free tier limitations.
'''

from google.cloud import bigquery as bq
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

aggregation_query = f'''
    -- step 1: get mode weather_code per date
with weather_mode as (
  select
    d.date,
    hw.weather_code,
    count(*) as freq,
    row_number() over (partition by d.date order by count(*) desc) as rn
  from
    `personal-project-465307.weather_dashboard_development.hourly_weather_data` as hw
  join
    `personal-project-465307.weather_dashboard_development.dim_date` as d
    on hw.date_id = d.id
  group by
    d.date, hw.weather_code
),

-- step 2: keep only the top (mode) per date
mode_per_day as (
  select
    wm.date,
    wc.name as overall_weather
  from
    weather_mode wm
  join
    `personal-project-465307.weather_dashboard_development.weather_code` as wc
    on wm.weather_code = wc.id
  where wm.rn = 1
)

-- step 3: join with main table
select
    hw.date_id,
    hw.time_id,
    d.date,
    d.quarter,
    d.month,
    d.year,
    t.time,
    ts.name as is_day,
    wc.name as weather,
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
    dw.daylight_duration,
    mpd.overall_weather
from
    `personal-project-465307.weather_dashboard_development.hourly_weather_data` as hw
join
    `personal-project-465307.weather_dashboard_development.day_night` as ts
    on hw.is_day = ts.id
join
    `personal-project-465307.weather_dashboard_development.dim_date` as d
    on hw.date_id = d.id
join
    `personal-project-465307.weather_dashboard_development.dim_time` as t
    on hw.time_id = t.id
join
    `personal-project-465307.weather_dashboard_development.weather_code` as wc
    on hw.weather_code = wc.id
join
    `personal-project-465307.weather_dashboard_development.daily_weather_data` as dw
    on hw.date_id = dw.date_id
left join
    mode_per_day mpd
    on d.date = mpd.date
'''

table_schema = [
    bq.SchemaField('id', 'STRING'),        
        bq.SchemaField('date', 'DATE'),
        bq.SchemaField('time', 'TIME'),
        bq.SchemaField('weather', 'STRING'),
        bq.SchemaField('is_day', 'STRING'),
        bq.SchemaField('temperature_2m', 'FLOAT'),
        bq.SchemaField('dew_point_2m', 'FLOAT'),
        bq.SchemaField('apparent_temperature', 'FLOAT'),
        bq.SchemaField('precipitation', 'FLOAT'),
        bq.SchemaField('cloud_cover', 'FLOAT'),
        bq.SchemaField('wind_speed_10m', 'FLOAT'),
        bq.SchemaField('wind_gusts_10m', 'FLOAT'),
        bq.SchemaField('wind_direction_10m', 'FLOAT'),
        bq.SchemaField('sunshine_duration', 'FLOAT'),
        bq.SchemaField('sunrise', 'STRING'),
        bq.SchemaField('sunset', 'STRING'),
        bq.SchemaField('daylight_duration', 'FLOAT'),
        bq.SchemaField('overall_weather', 'STRING'),
    ]

def fetch_data(query, project_id):
    try:
        client = bq.Client(project=project_id)
        df = client.query(query).to_dataframe()
        logging.info(f"Fetched {df.shape[0]} rows from BigQuery.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def transform_df(df):
    try:
        df['id'] = df['date_id'] + '_' + df['time_id']
        df.drop(columns=['date_id', 'time_id'], inplace=True)
        return df
    except Exception as e:
        logging.error(f'Error transforming DataFrame: {e}')
        raise

def load_to_bq(df, table_name, project_id):
    try:
        client = bq.Client(project=project_id)
        
        job_config = bq.LoadJobConfig(
            write_disposition='WRITE_APPEND',
            schema=table_schema,
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
    
if __name__ == '__main__':
    table_name = os.getenv('AGGREGATED_TABLE')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

    data = fetch_data(aggregation_query, project_id)
    if data.empty:
        logging.warning('No data fetched from BigQuery.')

    transformed_data = transform_df(data)
    if not transformed_data.empty:
        logging.info(f'Data fetched successfully with {len(transformed_data)} rows.')
    else:
        logging.warning('No data fetched.')

    print(transformed_data.info())
    load_to_bq(transformed_data, table_name, project_id)
    logging.info('Aggregation and loading completed successfully.')
