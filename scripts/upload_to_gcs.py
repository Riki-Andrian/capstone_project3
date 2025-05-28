import os
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
from tempfile import NamedTemporaryFile

# Path lokal
RAW_PATH = "/opt/airflow/data/raw/"
CSV_PATH = os.path.join(RAW_PATH, "csv")
JSON_PATH = os.path.join(RAW_PATH, "json")

# GCS info
BUCKET_NAME = "jdeol003-bucket"
DESTINATION_PATH = "capstone3_riki/merged_data_taxi.csv"

def upload_to_gcs(local_path, destination_path, hook):
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=destination_path,
        filename=local_path,
        timeout=300
    )
    print(f"Uploaded: {destination_path}")

def merge_and_upload():
    print("Starting memory-efficient merge_and_upload...")

    hook = GCSHook(gcp_conn_id="google_cloud_default")

    with NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as tmpfile:
        output_path = tmpfile.name
        first = True

        print(f"Reading and writing CSV files from: {CSV_PATH}")
        for filename in os.listdir(CSV_PATH):
            if filename.endswith(".csv"):
                print(f"Processing CSV: {filename}")
                for chunk in pd.read_csv(os.path.join(CSV_PATH, filename), chunksize=100_000):
                    chunk.to_csv(output_path, mode='a', index=False, header=first)
                    first = False

        print(f"Reading and writing JSON files from: {JSON_PATH}")
        for filename in os.listdir(JSON_PATH):
            if filename.endswith(".json"):
                print(f"Processing JSON: {filename}")
                df = pd.read_json(os.path.join(JSON_PATH, filename))
                df.to_csv(output_path, mode='a', index=False, header=first)
                first = False

        print(f"Uploading merged file directly to GCS...")
        upload_to_gcs(output_path, DESTINATION_PATH, hook)

    os.remove(output_path)
    print("Temporary file deleted.")

def upload_extra_files():
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    extra_files = ["payment_type.csv", "taxi_zone_lookup.csv"]
    for fname in extra_files:
        path = os.path.join(RAW_PATH, fname)
        dest_path = f"capstone3_riki/{fname}"
        upload_to_gcs(path, dest_path, hook)
        