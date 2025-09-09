from google.cloud import bigquery as bq
import pandas as pd
import logging
from datetime import datetime, timedelta
from scipy.stats import mode

logging.basicConfig(level=logging.INFO)

def get_week_range(selected_date):
    try:
        selected_date = pd.to_datetime(selected_date)
        start_of_week = selected_date - timedelta(days=selected_date.weekday())  
        end_of_week = start_of_week + timedelta(days=6) 
        return start_of_week.date(), end_of_week.date()
    except Exception as e:
        logging.error(f'Error getting week range: {e}')
        return None, None

def get_max_date(table_name, project_id):
    try:
        client = bq.Client(project=project_id)
        query = f'''
            select
                max(date) as current_date
            from
                `{table_name}`
        '''
        df = client.query(query).to_dataframe()
        return df['current_date'].iloc[0]
    except Exception as e:
        logging.error(f'Error fetching current date: {e}')
        return None
    
def get_min_time(table_name, project_id):
    try:
        client = bq.Client(project=project_id)
        query = f'''
            select
                min(time) as min_hour
            from
                `{table_name}`
        '''
        df = client.query(query).to_dataframe()
        return df['min_hour'].iloc[0]
    except Exception as e:
        logging.error(f'Error fetching minimum hour: {e}')
        return None

def get_weather_icon(description, default='day and night.gif'):
    icon_map = {
        'Clear sky': 'sun.gif',
        'Mainly clear': 'mainly cloud.gif',
        'Partly cloudy': 'mainly cloud.gif',
        'Overcast': 'clouds.gif',
        'Fog': 'foggy.gif',
        'Depositing rime fog': 'foggy.gif',
        'Drizzle: Light': 'drizzle.gif',
        'Drizzle: Moderate': 'drizzle.gif',
        'Drizzle: Dense': 'drizzle.gif',
        'Freezing Drizzle: Light': 'freezing.gif',
        'Freezing Drizzle: Dense': 'freezing.gif',
        'Rain: Slight': 'rainfall.gif',
        'Rain: Moderate': 'rainfall.gif',
        'Rain: Heavy': 'rainfall.gif',
        'Freezing Rain: Light': 'freezing.gif',
        'Freezing Rain: Heavy': 'freezing.gif',
        'Snow fall: Slight': 'snowfall.gif',
        'Snow fall: Moderate': 'snowfall.gif',
        'Snow fall: Heavy': 'snowfall.gif',
        'Snow Grain': 'snowfall.gif',
        'Rain showers: Slight': 'rain.gif',
        'Rain showers: Moderate': 'rain.gif',
        'Rain showers: Violent': 'rain.gif',
        'Snow showers: Slight': 'snow.gif',
        'Snow showers: Heavy': 'snow.gif',
        'Thunderstorm: Slight': 'storm.gif',
        'Thunderstorm: Slight with hail': 'storm.gif',
        'Thunderstorm: Violent with hail': 'storm.gif',
    }
    try:
        return icon_map.get(description, default)
    except Exception as e:
        logging.error(f'Error getting weather icon: {e}')
        return None

def get_wind_direction_label(degree):
    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    try:
        return directions[int((degree + 22.5) / 45) % 8]
    except Exception as e:
        logging.error(f'Error getting wind direction label: {e}')
        return None

def get_frequent_wind_direction(column):
    try:
        return mode(column.dropna()).mode[0]
    except Exception as e:
        logging.error(f'Error getting daily frequent wind direction: {e}')
        return None
    
def get_day_night_icon(description, default='day and night.gif'):
    icon_map={
        'Day': 'sun.gif',
        'Night': 'night.gif',
    }
    try:
        return icon_map.get(description, default)
    except Exception as e:
        logging.error(f'Error getting day night icon: {e}')
        return None
    