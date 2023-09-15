from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["all_failed"],
)
def all_failed_tr_fail():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        raise Exception("Task failed")

    @task
    def upstream_task_3():
        raise AirflowSkipException("Task skipped")

    @task
    def upstream_task_4():
        raise Exception("Task failed")

    @task(trigger_rule="all_failed")
    def all_failed_task_1():
        return "hi"

    @task(trigger_rule="all_failed")
    def all_failed_task_2():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], all_failed_task_1())
    chain([upstream_task_3(), upstream_task_4()], all_failed_task_2())


all_failed_tr_fail()
