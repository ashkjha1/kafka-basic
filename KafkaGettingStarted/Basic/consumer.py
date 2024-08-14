import json
from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
load_dotenv()

def consume_messages(bootstrap_servers, topic, group_id):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                print(f"Consumed message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__=="__main__":
    bootstrap_servers = os.getenv('SERVER')
    topic = os.getenv('TOPIC')

    bootstrap_servers = bootstrap_servers
    topic = topic
    group_id = "consumer-test-group"
    consume_messages(bootstrap_servers, topic, group_id)
