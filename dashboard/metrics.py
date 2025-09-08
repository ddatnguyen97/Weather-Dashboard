from google.cloud import bigquery as bq
import pandas as pd
import logging
from dashboard.utils import get_week_range

logging.basicConfig(level=logging.INFO)

def get_weekly_weather_data(selected_date, table_name, project_id):
    try:
        start_date, end_date = get_week_range(selected_date)
        query = f'''
            with weekly_data as (
                select 
                    date,
                    format_timestamp('%b %d', timestamp(date)) as formatted_date,
                    extract(dayofweek from date) as week_day,
                    avg(temperature_2m) as temperature,
                    avg(relative_humidity_2m) as humidity,
                    sum(precipitation) as precipitation,
                    avg(wind_speed_10m) as wind_speed,
                    avg(wind_gusts_10m) as wind_gusts,
                    daily_frequent_direction,
                    max(overall_weather) as overall_weather
                from
                    `{table_name}`
                where
                    date >= '{start_date}' and date <= '{end_date}'
                group by
                    date,
                    daily_frequent_direction
            ),
            weekday_label as (
                select *,
                    case extract(dayofweek from date)
                        when 1 then 'Sun'
                        when 2 then 'Mon'
                        when 3 then 'Tue'
                        when 4 then 'Wed'
                        when 5 then 'Thu'
                        when 6 then 'Fri'
                        when 7 then 'Sat'
                    end as weekday
                from weekly_data
            )
            
            select
                formatted_date as date,
                weekday,
                temperature,
                humidity,
                precipitation,
                wind_speed,
                wind_gusts,
                daily_frequent_direction,
                overall_weather
            from
                weekday_label
        '''
        client = bq.Client(project=project_id)
        result = client.query(query).to_dataframe()
        if result.empty:
            logging.info('No data found for the specified date range.')
            return result
        return result
    except Exception as e:
        logging.error(f'Error fetching weekly weather data: {e}')
        return None
    
def get_daily_weather_data(date, table_name, project_id):
    try:
        selected_date = date
        query = f'''
            with daily_data as (
                select 
                    date,
                    format_timestamp('%b %d', timestamp(date)) as formatted_date,
                    extract(dayofweek from date) as week_day,
                    time,
                    is_day,
                    weather,
                    temperature_2m as temperature,
                    relative_humidity_2m as humidity,
                    dew_point_2m as dew_point,
                    apparent_temperature,
                    precipitation,
                    cloud_cover,
                    wind_speed_10m as wind_speed,
                    wind_gusts_10m as wind_gusts,
                    wind_direction_label,
                    sunshine_duration,
                    sunrise,
                    sunset
                from
                    `{table_name}`
                where
                    date = '{selected_date}'
                group by
                    date,
                    daily_frequent_direction,
                    time,
                    is_day,
                    weather,
                    temperature,
                    humidity,
                    dew_point,
                    apparent_temperature,
                    precipitation,
                    cloud_cover,
                    wind_speed,
                    wind_gusts,
                    wind_direction_label,
                    sunshine_duration,
                    sunrise,
                    sunset
                ),
                weekday_label as (
                    select *,
                        case extract(dayofweek from date)
                            when 1 then 'Sun'
                            when 2 then 'Mon'
                            when 3 then 'Tue'
                            when 4 then 'Wed'
                            when 5 then 'Thu'
                            when 6 then 'Fri'
                            when 7 then 'Sat'
                        end as weekday
                    from daily_data
                )

                select
                    formatted_date as date,
                    weekday,
                    time,
                    weather,
                    is_day,
                    temperature,
                    humidity,
                    dew_point,
                    apparent_temperature,
                    precipitation,
                    cloud_cover,
                    wind_speed,
                    wind_gusts,
                    wind_direction_label,
                    sunshine_duration,
                    sunrise,
                    sunset
                from
                    weekday_label
        '''
        client = bq.Client(project=project_id)
        result = client.query(query).to_dataframe()
        if result.empty:
            logging.info('No data found for the specified date range.')
            return result
        return result
    except Exception as e:
        logging.error(f'Error fetching daily weather data: {e}')
        return None
