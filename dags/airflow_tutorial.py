import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Axelrod Gunnarson',
    'start_date': dt.datetime(2019, 3, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


def set_name(task_instance, xcom_author_key, **kwargs):
    task_instance.xcom_push(xcom_author_key, "Axelrod")


def print_world(task_instance, xcom_source_task, xcom_author_key, **kwargs):
    author_name = task_instance.xcom_pull(xcom_source_task, key=xcom_author_key)
    print('{} world'.format(author_name))


# Create a DAG as context manager dag_id airflow_tutorial_v01
with DAG('airflow_tutorial_v01',
         default_args=default_args,
         schedule_interval='0 * * * *',  # every hour
         catchup=False,  # do not run for past
         ) as dag:
    # Create three tasks: first 2 are bash
    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')
    # This is a python job
    set_name = PythonOperator(task_id="set_name",
                              provide_context=True,  # Allow to use xcom data
                              python_callable=set_name,
                              # pass data to the function
                              op_kwargs={
                                  # Data are passed as kwargs => this must be the same name
                                  "xcom_author_key": "author_name"}
                              )
    print_world = PythonOperator(task_id='print_world',
                                 provide_context=True,
                                 python_callable=print_world,
                                 op_kwargs={
                                     # xcom_pull needs the task_id that pushed that variable
                                     "xcom_source_task": "set_name",
                                     "xcom_author_key": "author_name"}
                                )
    # Generate the graph
    print_hello >> sleep >> set_name >> print_world
