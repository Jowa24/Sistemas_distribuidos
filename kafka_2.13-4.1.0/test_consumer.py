from kafka import KafkaConsumer

# Connect to Kafka and subscribe
consumer = KafkaConsumer(
    'SD',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
)

print("Waiting for messages...")
for msg in consumer:
    print(f"Received: {msg.value.decode('utf-8')}")