from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    "gcs_to_bq",
    default_args=default_args,
    catchup=False,
    tags=["capstone3"],
) as dag:
    
    data_taxi = BigQueryInsertJobOperator(
        task_id='data_taxi',
        configuration={
            "load":{
                "sourceUris": [
                    "gs://jdeol003-bucket/capstone3_riki/merged_data_taxi.csv"
                ],
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_capstone3_riki",
                    "tableId": "data_taxi_riki",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
            }
        },
        location="asia-southeast2"
    )

    data_payment_type = BigQueryInsertJobOperator(
        task_id='data_payment_type',
        configuration={
            "load":{
                "sourceUris": [
                    "gs://jdeol003-bucket/capstone3_riki/payment_type.csv"
                ],
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_capstone3_riki",
                    "tableId": "data_payment_type_riki",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
            }
        },
        location="asia-southeast2"
    )
    data_taxi_zone = BigQueryInsertJobOperator(
        task_id='data_taxi_zone',
        configuration={
            "load":{
                "sourceUris": [
                    "gs://jdeol003-bucket/capstone3_riki/taxi_zone_lookup.csv"
                ],
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_capstone3_riki",
                    "tableId": "data_taxi_zone_lookup_riki",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE", 
                "autodetect": True,
            }
        },
        location="asia-southeast2"
    )
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="bigquery_transform",
    )

    data_taxi >> data_payment_type >> data_taxi_zone >> trigger_transform_dag
