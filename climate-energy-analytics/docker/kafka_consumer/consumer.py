import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from batcher import BatchManager
from s3_writer import write_batch_to_s3


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = ["openweather_monthly", "noaa_weather", "eia_energy", "sensor_readings", "openmeteo_historical"]

print("Starting Kafka consumer...")
print(f"Connecting to Kafka broker at: {KAFKA_BOOTSTRAP_SERVERS}")

# -------------------------
# Retry until Kafka is ready
# -------------------------
for attempt in range(10):
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id="s3-writer-group",
            value_deserializer=lambda x: x.decode("utf-8")
        )
        print("Kafka consumer connected successfully")
        break
    except NoBrokersAvailable:
        print(f"Kafka not ready (attempt {attempt + 1}/10). Retrying in 5 seconds...")
        time.sleep(5)
else:
    raise RuntimeError("Kafka broker not available after retries")

# -------------------------
# Batch + write to S3
# -------------------------
batch_manager = BatchManager()

for message in consumer:
    print(f"Received message from topic: {message.topic}")

    batch_manager.add_message(message.topic, message.value)

    if batch_manager.ready_to_flush(message.topic):
        batch = batch_manager.flush(message.topic)
        write_batch_to_s3(
            batch,
            message.topic
        )
        consumer.commit()
