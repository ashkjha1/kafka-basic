import json
from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
load_dotenv()

bootstrap_servers = os.getenv('SERVER')
topic = os.getenv('TOPIC')

if __name__ == '__main__':
    # Kafka Consumer 
    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': 'consumer-group',
            'auto.offset.reset': 'earliest'
        }

    consumer = Consumer(conf)

    messages=consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        print('Received Key: {}'.format(msg.key()))
        print('Received message: {}'.format(msg.value()))