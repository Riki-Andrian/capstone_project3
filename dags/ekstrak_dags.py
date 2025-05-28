import sys
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from upload_to_gcs import merge_and_upload, upload_extra_files 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    "upload_to_gcs_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["capstone3"],
) as dag:

    task_merge_upload = PythonOperator(
        task_id="merge_and_upload",
        python_callable=merge_and_upload,
    )

    task_upload_extra = PythonOperator(
        task_id="upload_extra_files",
        python_callable=upload_extra_files,
    )
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="gcs_to_bq",
    )

    task_merge_upload >> task_upload_extra >> trigger_load_dag
