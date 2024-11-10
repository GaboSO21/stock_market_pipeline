from airflow.hooks.base import BaseHook
import json
import requests
from io import BytesIO
from minio import Minio


def _get_stock_prices(url, symbol):
    api_data = BaseHook.get_connection("stock_api")
    url = f"{url}{symbol}/range/1/day/2023-01-09/2024-01-09?apiKey={api_data.extra_dejson['api_key']}"
    response = requests.get(url, headers=api_data.extra_dejson['headers'])
    return json.dumps(response.json())


def _stock_prices(stock):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['ticker']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f"{objw.bucket_name}/{symbol}"
