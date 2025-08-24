from google.cloud import bigquery as bq
import os
import logging
from dotenv import load_dotenv

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GG_PROJECT_CREDS')

logging.basicConfig(level=logging.INFO)

hourly_weather_schema = [
    bq.SchemaField('id', 'STRING'),        
    bq.SchemaField('date_id', 'STRING'),
    bq.SchemaField('time_id', 'STRING'),
    bq.SchemaField('relative_humidity_2m', 'FLOAT'),
    bq.SchemaField('dew_point_2m', 'FLOAT'),
    bq.SchemaField('temperature_2m', 'FLOAT'),
    bq.SchemaField('apparent_temperature', 'FLOAT'),    
    bq.SchemaField('precipitation', 'FLOAT'),
    bq.SchemaField('cloud_cover', 'FLOAT'),
    bq.SchemaField('wind_speed_10m', 'FLOAT'),
    bq.SchemaField('wind_gusts_10m', 'FLOAT'),
    bq.SchemaField('wind_direction_10m', 'FLOAT'),
    bq.SchemaField('sunshine_duration', 'FLOAT'),
    bq.SchemaField('is_day', 'STRING'),
    bq.SchemaField('weather_code', 'STRING'),
    ]

date_schema = [
    bq.SchemaField('id', 'STRING', mode='REQUIRED'),        
    bq.SchemaField('date', 'DATE'),
    bq.SchemaField('year', 'INTEGER'),
    bq.SchemaField('quarter', 'INTEGER'),
    bq.SchemaField('month', 'INTEGER'),
    bq.SchemaField('day', 'INTEGER'),
]

time_schema = [
    bq.SchemaField('id', 'STRING', mode='REQUIRED'),        
    bq.SchemaField('time', 'TIME'),
    bq.SchemaField('hour', 'INTEGER'),
]

day_night_schema = [
    bq.SchemaField('id', 'STRING', mode='REQUIRED'),
    bq.SchemaField('name', 'STRING'),
]

weather_code_schema = [
    bq.SchemaField('id', 'STRING', mode='REQUIRED'),
    bq.SchemaField('name', 'STRING'),
]

daily_weather_schema = [
    bq.SchemaField('date_id', 'STRING', mode='REQUIRED'),
    bq.SchemaField('weather_code', 'STRING'),
    bq.SchemaField('sunrise', 'STRING'),
    bq.SchemaField('sunset', 'STRING'),
    bq.SchemaField('daylight_duration', 'FLOAT'),
]

def create_table(table_name, project_id, schema):
    try:
        client = bq.Client(project=project_id)
        table = bq.Table(table_name, schema=schema)
        table = client.create_table(table, exists_ok=True)
        logging.info(f"Created table {table.table_id}.")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")
        raise

if __name__ == "__main__":
    project_id = os.getenv("BQ_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")

    client = bq.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset}"

    tables = {
        "hourly_weather_data": hourly_weather_schema,
        "dim_date": date_schema,
        "dim_time": time_schema,
        "day_night": day_night_schema,
        "weather_code": weather_code_schema,
        "daily_weather_data": daily_weather_schema,
    }

    for table_name, schema in tables.items():
        full_table_name = f"{project_id}.{dataset}.{table_name}"
        create_table(full_table_name, project_id, schema)