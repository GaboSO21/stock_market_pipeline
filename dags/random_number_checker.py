from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description="A simple DAG to generate and check random numbers",
    catchup=False
)
def random_number_checker():

    @task
    def generate_number():
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number

    @task
    def check_even_odd(value):
        result = "even" if value % 2 == 0 else "odd"
        print(f"The number {value} is {result}")

    check_even_odd(generate_number())


random_number_checker()
