from confluent_kafka.admin import AdminClient
import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("SERVER")


def delete_kafka_topic(topic_name, bootstrap_servers=SERVER):
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    try:
        # Delete the topic
        fs = admin_client.delete_topics([topic_name], operation_timeout=30)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic '{topic}' deleted successfully")
            except Exception as e:
                print(f"Failed to delete topic '{topic}': {e}")
    except Exception as e:
        print(f"Failed to initiate topic deletion: {e}")


# Example usage
if __name__ == "__main__":
    topic = input("Enter the topic name which you want to delete: ")
    delete_kafka_topic(topic_name=topic)
