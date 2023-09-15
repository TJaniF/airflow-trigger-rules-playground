from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException
import time


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["one_success"],
)
def one_success_tr_fail():
    @task
    def upstream_task_1():
        raise Exception("Task failed")

    @task(trigger_rule="one_success")
    def one_success_task_1():
        return "hi"

    @task
    def upstream_task_2():
        raise AirflowSkipException("Task skipped")

    @task(trigger_rule="one_success")
    def one_success_task_2():
        return "hi"

    @task
    def upstream_task_3():
        return Exception("Task failed")

    @task
    def upstream_task_4():
        return "hi"

    @task(trigger_rule="one_success")
    def one_success_task_3():
        return "hi"

    chain(upstream_task_1(), one_success_task_1())
    chain(upstream_task_2(), one_success_task_2())

    upstream_task_4_obj = upstream_task_4()
    chain(upstream_task_3(), upstream_task_4_obj)
    chain([upstream_task_4_obj], one_success_task_3())


one_success_tr_fail()
