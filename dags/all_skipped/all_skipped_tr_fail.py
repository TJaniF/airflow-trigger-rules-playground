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
def all_skipped_tr_fail():
    @task
    def upstream_task_1():
        return "hi"

    @task
    def upstream_task_2():
        raise AirflowSkipException("Task skipped")
    
    @task(trigger_rule="all_skipped")
    def all_skipped_task_1():
        return "hi"

    @task
    def upstream_task_3():
        raise Exception("Task failed")

    @task
    def upstream_task_4():
        raise AirflowSkipException("Task skipped")

    @task(trigger_rule="all_skipped")
    def all_skipped_task_2():
        return "hi"
    
    @task 
    def upstream_task_5():
        raise Exception("Task failed")
    
    @task
    def upstream_task_6():
        return "hi"
    
    @task 
    def upstream_task_7():
        raise AirflowSkipException("Task skipped")
    
    @task(trigger_rule="all_skipped")
    def all_skipped_task_3():
        return "hi"

    chain([upstream_task_1(), upstream_task_2()], all_skipped_task_1())
    chain([upstream_task_3(), upstream_task_4()], all_skipped_task_2())

    upstream_task_6_obj = upstream_task_6()
    chain(upstream_task_5(), upstream_task_6_obj)
    chain([upstream_task_7(), upstream_task_6_obj], all_skipped_task_3())


all_skipped_tr_fail()
