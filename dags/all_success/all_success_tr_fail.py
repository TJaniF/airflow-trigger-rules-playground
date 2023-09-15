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
def all_success_tr_fail():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        return AirflowSkipException("Task skipped")

    @task(trigger_rule="all_success")
    def all_success_task_1():
        return "hi"

    @task
    def upstream_task_3():
        return "hi"

    @task
    def upstream_task_4():
        raise Exception("Task failed")

    @task(trigger_rule="all_success")
    def all_success_task_2():
        return "hi"

    @task
    def upstream_task_5():
        return "hi"

    @task
    def upstream_task_6():
        raise Exception("Task failed")

    @task
    def upstream_task_7():
        return "hi"

    @task(trigger_rule="all_success")
    def all_success_task_3():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], all_success_task_1())
    chain([upstream_task_3(), upstream_task_4()], all_success_task_2())

    upstream_task_7_obj = upstream_task_7()
    chain(upstream_task_6(), upstream_task_7_obj)

    chain(
        [upstream_task_5(), upstream_task_7_obj],
        all_success_task_3(),
    )


all_success_tr_fail()
