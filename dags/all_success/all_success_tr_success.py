from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException
import time


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["all_success"],
)
def all_success_tr_success():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        return "hi"

    @task(trigger_rule="all_success")
    def all_success_task():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], all_success_task())


all_success_tr_success()
