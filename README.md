# CAPSTONE PROJECT MODULE 3

### Riki Andrian Nugraha
### JCDEOL 003

Struktur Folder
```
 capstone_project3/
 │-- config/
 │-- dags/             # Folder untuk menyimpan dag
 │-- data/
 |   │-- raw/
 |   │   │-- csv/      # Folder untuk menyimpan data CSV
 |   │   │-- json/     # Folder untuk menyimpan data JSON
 │-- gcp-creds/        # Folder untuk menyimpan service account
 │-- logs/       
 │-- plugins/         
 │-- script/           # Folder untuk menyimpan script python
 |   │-- pubsub/      

```


## Panduan Penggunaan
1. Tools
- Docker
- VSCode
- Web browser

<br>

2. Cara Setup Program
  - Pastikan sudah mempunyai google service account.
  - Buat folder :"logs", "config", "gcp-creds", "plugins"
  - Pindahkan service account kedalam folder gcp-creds dan rename menjadi "purwadika-key.json'.
  - Pastikan docker sudah aktif.
  - Buat docker image dengan perintah berikut:
```
docker login
docker-compose build
```
<br>

3. Cara Menjalankan Program
  - Jalankan dengan cara:
  
```
docker-compose up
```
  - Masuk ke locallhost:8080
  - Dibagian connection, buat koneksi baru dengan nama google_cloud_default dan untuk Keyfile JSON diisi dengan isi dari google service account
  - Trigger DAG "upload_to_gcs_pipeline" untuk menjalankan program

<br>
4. Proses Ekstrak
Pada tahap transformasi, data diekstrak(di upload) ke gsc:

Contoh Implementasi:
```
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
```
<br>
5. Proses Load
Pada tahap load, data di load dari gcs ke BQ.

Contoh Implementasi:
```
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
    ))
```
<br>
6. Proses Transform
Pada tahap transform, data di transform dari data yg sudah ada di BQ ke dataset BQ baru.

Contoh Implementasi:
```

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

```
<br>
7. Streaming data
Streaming data menggunakan pubsub dan dataflow

Contoh Implementasi:
```
topic = 'projects/purwadika/topics/capstone3_riki_taxi'

publisher = pubsub_v1.PublisherClient()
faker = Faker()

def generate_data():
    nama = faker.name()
    pickup_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        'name': nama,
        'pickup_date': pickup_date
    }


while True:
       print('Generating Log: ')
       log = generate_data()
       message = json.dumps(log)
       print(message)
       publisher.publish(topic, message.encode("utf-8"))
       print('Message published.')
       time.sleep(5)
```
