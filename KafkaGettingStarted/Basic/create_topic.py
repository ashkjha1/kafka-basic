from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv
load_dotenv()

bootstrap_servers = os.getenv('SERVER')
topic = os.getenv('TOPIC')

def create_kafka_topic(topic_name, num_partitions=3, replication_factor=1, bootstrap_servers=bootstrap_servers):
    admin_client = AdminClient({
        "bootstrap.servers": bootstrap_servers
    })

    topic_list = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    fs = admin_client.create_topics(topic_list)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created successfully")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

# Example usage
create_kafka_topic(topic_name=topic, num_partitions=3, replication_factor=1)
