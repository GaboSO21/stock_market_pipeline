from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import json
import requests
from io import BytesIO
from minio import Minio

BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


def _get_stock_prices(url, symbol):
    api_data = BaseHook.get_connection("stock_api")
    url = f"{url}{symbol}/range/1/day/2023-01-09/2024-01-09?apiKey={api_data.extra_dejson['api_key']}"
    response = requests.get(url, headers=api_data.extra_dejson['headers'])
    return json.dumps(response.json())


def _stock_prices(stock):
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['ticker']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f"{objw.bucket_name}/{symbol}"


def _get_formatted_csv(path: str):
    path = 'stock-market/AAPL'
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices"
    objects = client.list_objects(
        BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    raise AirflowNotFoundException("The csv file does not exist.")
