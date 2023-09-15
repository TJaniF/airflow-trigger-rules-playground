from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["none_skipped"],
)
def none_skipped_tr_fail():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        raise AirflowSkipException("Task skipped")

    @task(trigger_rule="none_skipped")
    def none_skipped_task():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], none_skipped_task())


none_skipped_tr_fail()
