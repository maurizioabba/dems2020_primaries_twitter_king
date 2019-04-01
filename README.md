# H1 Experimentation with airflow

This experiment is composed by two dags, mainly using PythonOperator. A detailed
view of what the dags are performing is provided below.

In addition, a LastExternalTaskSensor make possible to run a dag only if another
dag succeeded over the last N seconds. This plugin is inspired by
ExternalTaskSensor from airlow v 1.10.3.

*NOTE*: we currently use SQLite db. This is NOT correct. It will effectively
create a lock on the db and allow only one job to run at a time, effectively
making airflow work as a single worker.
*NOTE2* (more important): DO NOT COPY the way we perform SQLite queries. The
purpose of this code is to play with airflow and the LastExternalTaskSensor, not
performing SQLite queries. Queries are built via string concatenation, which is
a big NONO. Do not take inspiration from them.
*NOTE3*: pickle/unpickle is not safe. We rely on the fact that a tweepy.Status is
well formed object. Don't do it, in general.

# H1 Workflow details

1. one task on a dag
	fetch the list of candidates from wikipedia
	fetch the list of stored candidates from sqlite
	succeed if the two are the same

1. one task on a dag
	fetch the list of presidential candidates and associated twitter handle from sqlite db
	based on the number of twitter jobs, divide handles in uniform groups
2. N tasks run in parallel
	for each candidate/handle received:
		get the last 100 tweets of the candidate
        extract features
3. one task
	declare the winner over the last 100 tweets
