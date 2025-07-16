from metrics import get_daily_avg_temperature, get_daily_humidity, get_daily_weather_data, get_daily_precipitation
from charts import create_daily_weather_card
from utils import get_date
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv("GG_CREDENTIALS")

table_name = os.getenv('AGGREGATED_TABLE')
project_id = os.getenv('BQ_PROJECT_ID')
selected_date = get_date(table_name, project_id)

avg_temp_metric = get_daily_avg_temperature(selected_date, table_name, project_id)
avg_temp_icon = 'dashboard/assets/icons/thermometer.gif'
if not avg_temp_metric.empty:
    avg_temp_value = round(avg_temp_metric['avg_temperature'].iloc[0], 2)
    avg_temp_content = f'Temperature: {avg_temp_value}°C'
else:
    avg_temp_content = 'No Data'

humidity_metric = get_daily_humidity(selected_date, table_name, project_id)
humidity_icon = 'dashboard/assets/icons/drop.gif'
if not humidity_metric.empty:
    humidity_value = round(humidity_metric['avg_humidity'].iloc[0], 2)
    humidity_content = f'Humidity: {humidity_value}%'
else:
    humidity_content = 'No Data'

precipitation_metric = get_daily_precipitation(selected_date, table_name, project_id)
precipitation_icon = 'dashboard/assets/icons/ocean.gif'
if not precipitation_metric.empty:
    precipitation_value = round(precipitation_metric['total_precipitation'].iloc[0], 2)
    precipitation_content = f'Precipitation: {precipitation_value}mm'
else:
    precipitation_content = 'No Data'

weather_card = create_daily_weather_card(
    date=selected_date,
    avg_temp_icon=avg_temp_icon,
    avg_temp_content=avg_temp_content,
    humidity_icon=humidity_icon,
    humidity_content=humidity_content,
    precipitation_icon=precipitation_icon,
    precipitation_content=precipitation_content,
)

