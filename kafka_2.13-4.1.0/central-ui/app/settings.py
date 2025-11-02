import os
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "central-ui")
KAFKA_TOPICS = [t.strip() for t in os.getenv("KAFKA_TOPICS","central-events,engine-events,driver-events").split(",") if t.strip()]
WEBSOCKET_PATH = os.getenv("WEBSOCKET_PATH", "/ws")
