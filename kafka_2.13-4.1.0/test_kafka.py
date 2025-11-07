# test_kafka.py
from kafka import KafkaProducer, KafkaConsumer
import sys

def test_kafka_connection(broker):
    try:
        print(f"Testing connection to: {broker}")
        producer = KafkaProducer(bootstrap_servers=[broker])
        print("Kafka connection successful!")
        producer.close()
        return True
    except Exception as e:
        print(f"Kafka connection failed: {e}")
        return False

if __name__ == "__main__":
    broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    test_kafka_connection(broker)