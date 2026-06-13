"""
Skeleton DAG for Task 2.

In the final version, replace placeholder operator calls with Yandex Cloud/Airflow provider
operators that create a Yandex Data Processing cluster, submit the PySpark job, and delete
that cluster. Keep screenshots and final DAG run status in docs/images/.
"""
from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="etl_module4_applications_dataproc",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "dataproc", "spark", "module4"],
) as dag:
    start = EmptyOperator(task_id="start")
    create_cluster = EmptyOperator(task_id="create_dataproc_cluster")
    submit_pyspark = EmptyOperator(task_id="submit_process_applications_job")
    delete_cluster = EmptyOperator(task_id="delete_dataproc_cluster")
    finish = EmptyOperator(task_id="finish")

    start >> create_cluster >> submit_pyspark >> delete_cluster >> finish
