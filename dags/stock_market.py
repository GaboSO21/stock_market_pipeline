from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.pagerduty.notifications.pagerduty import send_pagerduty_notification
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices

SYMBOL = "APPL"


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=[
        send_pagerduty_notification(
            pagerduty_events_conn_id="pager_duty",
            summary="The dag {{dag.dag_id}} failed",
            severity="critical",
            source="airflow dag_id: {{dag.dag_id}}",
            dedup_key="{{dag.dag_id}}-{{ti.task_id}}",
            group="{{dag.dag_id}}",
            component="airflow",
            class_type="Prod Data Pipeline"
        )
    ]
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

    is_api_available() >> get_stock_prices


stock_market()
