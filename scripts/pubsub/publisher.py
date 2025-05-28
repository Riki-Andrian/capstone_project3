import time
import json
from faker import Faker
from datetime import datetime
from google.cloud import pubsub_v1

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