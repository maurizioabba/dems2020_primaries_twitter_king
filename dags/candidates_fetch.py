"""
DAG fetching the list of candidates from WikiPedia page and comparing
it with the list of candidates stored in our current DB.

The dag will fail with email if the two lists are not the same.

The dag has three tasks:
    1. fetch candidates from remote
    2. fetch candidates from local
    3. compare the two lists
1 and 2 should be done in parallel, while 3 will have 1 and 2 as
requirements.
"""
import configparser
import datetime as dt

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts import fetch_candidate_list

CONFIG = configparser.ConfigParser()
CONFIG.read("twitter_king.ini")

DEFAULT_ARGS = {
    "owner": "Axelrod Gunnarson",
    "start_date": dt.datetime(2019, 3, 22),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "email": CONFIG.get("email", "receiver_email"),
    "email_on_failure": False,
    "email_on_retry": False,
}

SCHEDULE_INTERVAL = "0 * * * *"  # every hour

with DAG("fetch_candidates",
         default_args=DEFAULT_ARGS,
         schedule_interval=SCHEDULE_INTERVAL,
         catchup=False,
         max_active_runs=1,
         ) as dag:
    fetch_candidates_remote = PythonOperator(
        task_id="fetch_candidates_remote",
        provide_context=True,  # Allow to use xcom data
        python_callable=fetch_candidate_list.fetch_candidates_remote,
        op_kwargs={
            "xcom_push_remote_candidates": "candidates_remote",
        }
    )
    fetch_candidates_local = PythonOperator(
        task_id="fetch_candidates_local",
        provide_context=True,  # Allow to use xcom data
        python_callable=fetch_candidate_list.fetch_candidates_local,
        op_kwargs={
            "table": "candidates",
            "xcom_push_local_candidates": "candidates_local",
        }
    )
    compare_candidates_list = PythonOperator(
        task_id="compare_candidates_list",
        provide_context=True,
        python_callable=fetch_candidate_list.compare_candidates_list,
        # We don't need to retry, send an email on failure
        retries=0,
        email_on_failure=True,
        op_kwargs={
            "xcom_pull_remote_candidates": "candidates_remote",
            "xcom_pull_local_candidates": "candidates_local",
        },
    )

    # Not strictly needed, but nice to have
    end_operator = DummyOperator(task_id="end")

    # pylint:disable=pointless-statement
    [fetch_candidates_remote, fetch_candidates_local] >> compare_candidates_list >> end_operator
