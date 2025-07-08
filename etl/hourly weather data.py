import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os
import logging
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(level=logging.INFO)

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

API_URL = os.getenv('URL_PATH')
LOCATION = {'latitude': 10.762622, 'longitude': 106.660172}
DATE_RANGE = {
    'start_date': '2020-01-01',
    'end_date': '2025-06-30'
}

