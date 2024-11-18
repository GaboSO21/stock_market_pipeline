from airflow.decorators import dag, task
from datetime import datetime


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,
    tags=["test"],
)
def retail():

    @task
    def start():
        print("Hi")
        raise ValueError("Error")

    start()


retail()
