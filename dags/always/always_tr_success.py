from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException
import time


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["always"],
)
def always_tr_success():
    @task
    def upstream_task_1():
        time.sleep(20)
        return "hi"

    @task
    def upstream_task_2():
        time.sleep(20)
        raise Exception("Task failed")

    @task
    def upstream_task_3():
        time.sleep(20)
        raise AirflowSkipException("Task skipped")

    @task
    def upstream_task_4():
        time.sleep(20)
        raise Exception("Task failed")

    @task
    def upstream_task_5():
        time.sleep(20)
        return "hi"

    @task(trigger_rule="always")
    def always_task():
        return "hi"

    upstream_task_5_obj = upstream_task_5()
    chain(upstream_task_4(), upstream_task_5_obj)

    chain(
        [upstream_task_1(), upstream_task_2(), upstream_task_3(), upstream_task_5_obj],
        always_task(),
    )


always_tr_success()