from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10):
    msg = f"Message {i}".encode('utf-8')
    producer.send('SD', msg)
    print(f"Sent: {msg}")
    time.sleep(0.5)

producer.flush()
