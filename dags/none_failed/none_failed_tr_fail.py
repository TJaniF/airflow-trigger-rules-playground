from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.exceptions import AirflowSkipException


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["none_failed"],
)
def none_failed_tr_fail():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        raise Exception("Task failed")

    @task(trigger_rule="none_failed")
    def none_failed_task_1():
        return "hi"

    @task
    def upstream_task_3():
        return "hi"

    @task
    def upstream_task_4():
        raise Exception("Task failed")

    @task
    def upstream_task_5():
        return "hi"

    @task(trigger_rule="none_failed")
    def none_failed_task_2():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], none_failed_task_1())
    upstream_task_5_obj = upstream_task_5()
    chain(upstream_task_4(), upstream_task_5_obj)
    chain([upstream_task_3(), upstream_task_5_obj], none_failed_task_2())


none_failed_tr_fail()
