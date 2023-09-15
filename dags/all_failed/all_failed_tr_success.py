from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["all_failed"],
)
def all_failed_tr_success():
    @task
    def upstream_task_1():
        raise Exception("Task failed")

    @task
    def upstream_task_2():
        raise Exception("Task failed")

    @task
    def upstream_task_3():
        return "hi"

    @task(trigger_rule="all_failed")
    def all_failed_task():
        return "hi"

    upstream_task_3_obj = upstream_task_3()
    chain(upstream_task_2(), upstream_task_3_obj)

    chain(
        [upstream_task_1(), upstream_task_3_obj],
        all_failed_task(),
    )


all_failed_tr_success()
