from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException
import time


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["one_failed"],
)
def one_failed_tr_fail():
    @task
    def upstream_task_1():
        return "hi"

    @task(trigger_rule="one_failed")
    def one_failed_task_1():
        return "hi"

    @task
    def upstream_task_2():
        raise AirflowSkipException("Task skipped")

    @task(trigger_rule="one_failed")
    def one_failed_task_2():
        return "hi"

    chain(upstream_task_1(), one_failed_task_1())
    chain(upstream_task_2(), one_failed_task_2())


one_failed_tr_fail()
