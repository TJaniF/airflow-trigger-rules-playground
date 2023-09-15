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
def one_success_tr_success():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        time.sleep(30)
        raise Exception("Task failed")

    @task
    def upstream_task_3():
        time.sleep(30)
        raise AirflowSkipException("Task skipped")

    @task
    def upstream_task_4():
        time.sleep(30)
        raise Exception("Task failed")

    @task
    def upstream_task_5():
        return "hi"

    @task(trigger_rule="one_success")
    def oone_success_task():
        return "hi"

    upstream_task_5_obj = upstream_task_5()
    chain(upstream_task_4(), upstream_task_5_obj)

    chain(
        [upstream_task_1(), upstream_task_2(), upstream_task_3(), upstream_task_5_obj],
        oone_success_task(),
    )


one_success_tr_success()
