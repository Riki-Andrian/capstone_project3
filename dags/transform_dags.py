from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    'bigquery_transform',
    default_args=default_args,
    schedule_interval= None,
    catchup=False,
    tags=["capstone3"],
) as dag:

    transform_query = """
    WITH base_data AS (
        SELECT *,
        EXTRACT(DAY FROM lpep_pickup_datetime) AS pickup_day
        FROM purwadika.jcdeol3_capstone3_riki.data_taxi_riki
        WHERE EXTRACT(DAY FROM lpep_pickup_datetime) = @yesterday_day
        ),
    unique_taxi_per_day AS (
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY pickup_day ORDER BY lpep_pickup_datetime) AS rn
            FROM base_data
          )
          WHERE rn = 1
        )
    SELECT
        s.name,
        t.VendorID AS vendor_id,
        t.lpep_pickup_datetime,
        t.lpep_dropoff_datetime,
        TIMESTAMP_DIFF(t.lpep_dropoff_datetime, t.lpep_pickup_datetime, SECOND) AS trip_duration_minutes,
        t.passenger_count,
        t.RatecodeID AS rate_code,
        t.store_and_fwd_flag,
        t.PULocationID AS pu_location_id,
        t.DOLocationID AS do_location_id,
        ROUND(t.trip_distance * 1.60934, 2) AS trip_distance,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.ehail_fee,
        t.improvement_surcharge,
        t.total_amount,
        pt.description AS payment_type,
        t.trip_type,
        t.congestion_surcharge
    FROM purwadika.jcdeol3_capstone3_riki.stream_data_taxi s
    LEFT JOIN unique_taxi_per_day t
        ON EXTRACT(DAY FROM s.pickup_date) = t.pickup_day
    LEFT JOIN purwadika.jcdeol3_capstone3_riki.data_payment_type_riki pt
        ON t.payment_type = pt.payment_type
    WHERE EXTRACT(DAY FROM s.pickup_date) = @yesterday_day
    """

    transform_task = BigQueryInsertJobOperator(
        task_id='transform_taxi_data',
        configuration={
            "query": {
                "query": transform_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "purwadika",
                    "datasetId": "jcdeol3_capstone3_riki",
                    "tableId": "transformed_data_taxi_riki"
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "yesterday_day",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ (execution_date - macros.timedelta(days=1)).day }}"},
                    }
                ]
            }
        },
        location='asia-southeast2',
        project_id='purwadika',
    )
