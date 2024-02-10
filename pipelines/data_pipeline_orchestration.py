"""Airflow data pipeline orchestration.

Runs the services in the following order:
1. Preprocess the data (`feed_preprocessing`)
2. Recommend the data (`recommendation`)
3. Postprocess the data (`feed_postprocessing`)
4. Manage the feeds (`manage_bluesky_feeds`)
"""
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_orchestration',
    default_args=default_args,
    description='Orchestrate data pipeline services',
    schedule_interval=timedelta(hours=2),
)

preprocess = SimpleHttpOperator(
    task_id='preprocess',
    http_conn_id='api_service',
    endpoint='preprocess',
    method='POST',
    data="{{ ti.xcom_pull(task_ids='trigger_task') }}",  # Assuming data to preprocess comes from another task
    headers={"Content-Type": "application/json"},
    dag=dag,
)

recommend = SimpleHttpOperator(
    task_id='recommend',
    http_conn_id='api_service',
    endpoint='recommend',
    method='POST',
    data="{{ ti.xcom_pull(task_ids='preprocess') }}",
    headers={"Content-Type": "application/json"},
    dag=dag,
)

postprocess = SimpleHttpOperator(
    task_id='postprocess',
    http_conn_id='api_service',
    endpoint='postprocess',
    method='POST',
    data="{{ ti.xcom_pull(task_ids='recommend') }}",
    headers={"Content-Type": "application/json"},
    dag=dag,
)

manage_feeds = SimpleHttpOperator(
    task_id='manage_feeds',
    http_conn_id='api_service',
    endpoint='manage_feeds',
    method='POST',
    data="{{ ti.xcom_pull(task_ids='postprocess') }}",
    headers={"Content-Type": "application/json"},
    dag=dag,
)

preprocess >> recommend >> postprocess >> manage_feeds
