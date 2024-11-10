from airflow.hooks.base import BaseHook
import json
import requests


def _get_stock_prices(url, symbol):
    api_data = BaseHook.get_connection("stock_api")
    url = f"{url}{symbol}/range/1/day/2023-01-09/2024-01-09?apiKey={api_data.extra_dejson['api_key']}"
    response = requests.get(url, headers=api_data.extra_dejson['headers'])
    return json.dumps(response.json())
