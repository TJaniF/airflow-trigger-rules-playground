from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["all_skipped"],
)
def all_skipped_tr_success():
    @task
    def upstream_task_1():
        raise AirflowSkipException("Task skipped")

    @task
    def upstream_task_2():
        raise AirflowSkipException("Task skipped")

    @task(trigger_rule="all_skipped")
    def all_skipped_task():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], all_skipped_task())


all_skipped_tr_success()
