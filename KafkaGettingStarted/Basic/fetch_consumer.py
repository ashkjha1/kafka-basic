from confluent_kafka.admin import AdminClient
import os
from dotenv import load_dotenv
load_dotenv()

def fetch_consumer_groups(bootstrap_servers):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    consumer_groups = admin_client.list_consumer_groups()
    return consumer_groups

if __name__=="__main__":
    bootstrap_servers = os.getenv("SERVER")
    consumer_groups = fetch_consumer_groups(bootstrap_servers)
    print(consumer_groups)
