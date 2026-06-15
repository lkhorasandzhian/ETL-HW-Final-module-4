from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.yandex.operators.dataproc import (
        DataprocCreateClusterOperator,
        DataprocCreatePysparkJobOperator,
        DataprocDeleteClusterOperator,
    )
except ImportError:
    from airflow.providers.yandex.operators.yandexcloud_dataproc import (
        DataprocCreateClusterOperator,
        DataprocCreatePysparkJobOperator,
        DataprocDeleteClusterOperator,
    )


# ----------------------------
# Yandex Cloud infrastructure
# ----------------------------
FOLDER_ID = "b1gog0hrhv85805b40st"
SERVICE_ACCOUNT_ID = "ajeltear2h55110f7urb"
SUBNET_ID = "e9bhuqc10l6rbuet4dnv"
SECURITY_GROUP_ID = "enpr93b2ev5t8k7o3e9t"
ZONE = "ru-central1-a"
SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIH577Pg6LGu/C0JbNYySpex5kCOZAYbUmPwCI3TFRzfB etl-module4-dataproc"

# Default Yandex Cloud Airflow connection.
# In Managed Service for Apache Airflow it is usually available by default.
YC_CONNECTION_ID = "yandexcloud_default"


# ---------------------
# Object Storage paths
# ---------------------
BUCKET_NAME = "etl-transactions-v2-levon-20260615"

INPUT_PATH = (
    "s3a://etl-transactions-v2-levon-20260615"
    "/etl/task_02/input/applications.csv"
)

SCRIPT_PATH = (
    "s3a://etl-transactions-v2-levon-20260615"
    "/etl/task_02/scripts/process_applications.py"
)

OUTPUT_PATH = (
    "s3a://etl-transactions-v2-levon-20260615"
    "/etl/task_02/output/applications"
)


default_args = {
    "owner": "levon",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_applications_dataproc_task_02",
    description="Task 2: process applications.csv using Yandex Data Processing and PySpark",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "module-4", "task-02", "dataproc", "pyspark"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",

        folder_id=FOLDER_ID,
        cluster_name="etl-applications-task-02",
        cluster_description="Temporary Yandex Data Processing cluster for ETL module 4 task 2",
        cluster_image_version="2.1",

        ssh_public_keys=[SSH_PUBLIC_KEY],

        subnet_id=SUBNET_ID,
        security_group_ids=[SECURITY_GROUP_ID],
        zone=ZONE,

        service_account_id=SERVICE_ACCOUNT_ID,
        s3_bucket=BUCKET_NAME,

        services=("YARN", "SPARK"),

        masternode_resource_preset="s2.small",
        masternode_disk_size=20,
        masternode_disk_type="network-hdd",

        datanode_count=0,

        computenode_count=1,
        computenode_resource_preset="s2.small",
        computenode_disk_size=20,
        computenode_disk_type="network-hdd",

        connection_id=YC_CONNECTION_ID,

        labels={
            "project": "etl-module-4",
            "task": "task-02",
        },
    )

    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_applications_job",

        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster', key='cluster_id') }}",

        main_python_file_uri=SCRIPT_PATH,

        args=[
            "--input",
            INPUT_PATH,
            "--output",
            OUTPUT_PATH,
        ],

        properties={
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "8",
        },

        name="process-applications-csv",
        connection_id=YC_CONNECTION_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",

        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster', key='cluster_id') }}",

        connection_id=YC_CONNECTION_ID,

        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_pyspark_job >> delete_cluster
