import sys
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 kafka_debug_tool.py <broker_address>")
        sys.exit(1)
    
    broker_address = sys.argv[1]
    
    print("--- Kafka Debugging Tool ---")
    print(f"Connecting to broker at {broker_address}...")

    consumer = None
    
    # Retry connecting in case this starts before Kafka is fully ready
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=broker_address,
                auto_offset_reset='earliest', # Read all messages from the beginning
                group_id=f'kafka-debugger-{time.time()}', # Always be a new consumer
                consumer_timeout_ms=3000 # Stop iterating if no messages for 3s
            )
            # Try to list topics to confirm connection
            topics = consumer.topics()
            print("Connection successful.")
            break
        except NoBrokersAvailable:
            print(f"Broker not available. Retrying in 3s... ({i+1}/10)")
            time.sleep(3)
    
    if not consumer:
        print("CRITICAL: Could not connect to Kafka broker. Exiting.")
        sys.exit(1)

    # Get all topics and filter out the internal Kafka ones
    all_topics = consumer.topics()
    app_topics = [t for t in all_topics if not t.startswith('__')]
    
    if not app_topics:
        print("No application topics found! Waiting for them to be created...")
        # Poll for 10 seconds waiting for topics
        start_time = time.time()
        while time.time() - start_time < 10:
            app_topics = [t for t in consumer.topics() if not t.startswith('__')]
            if app_topics:
                break
            time.sleep(1)

    if not app_topics:
        print("CRITICAL: Still no application topics found. Exiting.")
        sys.exit(1)

    print(f"Found application topics: {app_topics}")
    
    # Subscribe to all of them
    consumer.subscribe(topics=app_topics)
    print("Subscribed. Waiting for messages...\n")

    try:
        # This loop will run forever, printing messages as they arrive
        while True:
            for message in consumer:
                print("--------------------------------------------------")
                print(f" Topic:     {message.topic}")
                print(f" Partition: {message.partition}")
                
                # Decode key and value if they exist
                key = message.key.decode('utf-8') if message.key else 'None'
                value = message.value.decode('utf-8') if message.value else 'None'
                
                print(f" Key:       {key}")
                print(f" Value:     {value}")
            
            print("\n(No new messages. Polling again...)\n")
            # The consumer_timeout_ms will break the inner loop,
            # this outer loop will make it try again.
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down debugger.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
