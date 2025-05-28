import apache_beam as beam
import os
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import json

INPUT_SUBSCRITION = "projects/purwadika/subscriptions/capstone3_riki_taxi_subs"
OUTPUT_TABLE = "purwadika.jcdeol3_capstone3_riki.stream_data_taxi"

beam_options_dict = {
    'project': 'purwadika',
    'runner': 'DataflowRunner',
    'region':'us-east1',
    'temp_location':'gs://jdeol003-bucket/capstone3_riki/temp',
    'job_name': 'streaming-job-riki',
    'streaming': True,
    'service_account_email': 'jdeol-03@purwadika.iam.gserviceaccount.com'
}
beam_options = PipelineOptions.from_dictionary(beam_options_dict)

if __name__ == '__main__':

   with beam.Pipeline(options=beam_options) as p: (
       p
       | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRITION)
       | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
       | "Parse JSON" >> beam.Map(json.loads)
       | 'Write to Table' >> beam.io.WriteToBigQuery(OUTPUT_TABLE,

           schema='name:STRING, pickup_date:TIMESTAMP',
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))  
   p.run()