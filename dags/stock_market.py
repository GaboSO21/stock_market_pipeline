from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
# from airflow.providers.pagerduty.notifications.pagerduty import send_pagerduty_notification
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices, _stock_prices, _get_formatted_csv, BUCKET_NAME

SYMBOL = "AAPL"


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    # Test to see when assigned to None
    on_success_callback=None
    #     send_pagerduty_notification(
    #         pagerduty_events_conn_id="pager_duty",
    #         summary="The dag {{dag.dag_id}} failed",
    #         severity="critical",
    #         source="airflow dag_id: {{dag.dag_id}}",
    #         dedup_key="{{dag.dag_id}}-{{ti.task_id}}",
    #         group="{{dag.dag_id}}",
    #         component="airflow",
    #         class_type="Prod Data Pipeline"
    #     )
    # ]
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['apple_endpoint']}?apiKey={api.extra_dejson['api_key']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['status'] == "OK"
        return PokeReturnValue(is_done=condition, xcom_value=f"{api.host}{api.extra_dejson['endpoint']}")

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': '{{task_instance.xcom_pull(task_ids="is_api_available")}}',
            'symbol': SYMBOL}
    )

    stock_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_stock_prices,
        op_kwargs={
            'stock': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{task_instance.xcom_pull(task_ids="stock_prices")}}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{task_instance.xcom_pull(task_ids='get_formatted_csv')}}", conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        )
    )

    is_api_available() >> get_stock_prices >> stock_prices >> format_prices >> get_formatted_csv >> load_to_dw


stock_market()
