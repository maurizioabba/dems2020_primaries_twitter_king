import configparser
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import LastExternalTaskSensor

from dbs import utils_db
from scripts import tweets



CONFIG = configparser.ConfigParser()
CONFIG.read("twitter_king.ini")

DEFAULT_ARGS = {
    "owner": "Maurizio Abba",
    "start_date": dt.datetime(2019, 3, 22),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    "email": CONFIG.get("email", "receiver_email"),
    "email_on_failure": False,
    "email_on_retry": False,
}

SCHEDULE_INTERVAL = "0 * * * *"  # every hour

# Create a DAG as context manager dag_id twitter_king
with DAG("twitter_king",
         default_args=DEFAULT_ARGS,
         schedule_interval=SCHEDULE_INTERVAL,
         catchup=False,  # do not run for past
         max_active_runs=1,
         ) as dag:
    # This dag depends on the succesful completion of "fetch_candidates" DAG at least once
    # over the last 24 hours
    fetch_candidates_sensor = LastExternalTaskSensor(
        task_id="fetch_candidates_sensor",
        external_dag_id="fetch_candidates",
        external_task_id="end",
        execution_delta=dt.timedelta(days=1),
        # Free the slot while waiting
        mode='reschedule',
    )

    twitter_authenticate = PythonOperator(
        task_id="tweepy_hdl",
        python_callable=tweets.get_auth_handler,
        provide_context=True,
        op_kwargs={
            "consumer_key": CONFIG.get("twitter", "consumer_key"),
            "consumer_secret": CONFIG.get("twitter", "consumer_secret"),
            "access_token": CONFIG.get("twitter", "access_token"),
            "access_secret_token": CONFIG.get("twitter", "access_secret_token"),
            "xcom_push_tweepy_hdl": "tweepy_hdl",
        })

    # pylint:disable=pointless-statement
    fetch_candidates_sensor >> twitter_authenticate

    # Dynamically adjust the number of fetcher according to the number of candidates
    candidates = [
       (name, handle) for name, handle in utils_db.get_candidates_name_handle("candidates")]

    list_features_keys = []
    # NOTE: it looks like this is the only way to get this information
    #       maybe there is another way to set downstream of every current leaf task in the dag?
    list_features_extraction_tasks = []

    for name, handle in candidates:
        handle_id = handle.replace("@", "").lower()
        tweet_storage = "candidates_tweet_{}".format(handle_id)
        twitter_fetcher = PythonOperator(
            task_id="fetch_tweets_{}".format(handle_id),
            python_callable=tweets.fetch_tweets,
            provide_context=True,
            op_kwargs={
                "candidate_handle": handle_id,
                "tweet_storage": tweet_storage,
                "xcom_pull_tweepy_hdl": "tweepy_hdl",
            })
        twitter_feature_extraction = PythonOperator(
            task_id="extract_features_{}".format(handle_id),
            python_callable=tweets.analyze_tweets,
            provide_context=True,
            op_kwargs={
                "candidate_handle": handle_id,
                "tweet_storage": tweet_storage,
                "xcom_push_tweet_features": handle
            })
        list_features_extraction_tasks.append(twitter_feature_extraction)

        # pylint:disable=pointless-statement
        twitter_authenticate >> twitter_fetcher >> twitter_feature_extraction


    ranker = PythonOperator(task_id="perform_ranking",
                            python_callable=tweets.perform_ranking,
                            provide_context=True,
                            op_kwargs={
                                "ranking_db": "winners.db",
                                "list_candidates": candidates,
                            })

    # Set the ranker as downstream to all extraction tasks
    # pylint:disable=pointless-statement
    list_features_extraction_tasks >> ranker
