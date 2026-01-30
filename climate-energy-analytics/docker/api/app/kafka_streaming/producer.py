import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
)

_producer = None


def get_producer():
    global _producer
    retries = 10
    delay = 5

    for attempt in range(retries):
        try:
            if _producer is None:
                _producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    retries=5,
                    acks="all"
                )
            return _producer

        except NoBrokersAvailable:
            print(
                f"[KafkaProducer] Attempt {attempt + 1}/{retries}: "
                f"Kafka broker not available, retrying in {delay}s..."
            )
            time.sleep(delay)

    raise RuntimeError("Kafka broker not available")


# -------------------------------
# INTERNAL SAFE SEND METHOD
# -------------------------------
def _send(topic: str, source: str, data: dict, key: str = None):
    producer = get_producer()

    payload = {
        "source": source,
        "topic": topic,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data
    }

    producer.send(topic, key=key, value=payload)
    producer.flush()

    # Log produced record
    log_dir = "/opt/airflow/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "producer.log")
    with open(log_path, "a") as f:
        f.write(f"Produced 1 record to topic: {topic}\n")


# -------------------------------
# TOPIC-SPECIFIC PRODUCERS
# -------------------------------
def send_sensor_data(sensor_data: dict):
    _send(
        topic="sensor_readings",
        source="sensor",
        data=sensor_data,
        key=sensor_data.get("meter_id", "unknown")
    )


def send_openweather_data(weather_data: dict):
    _send(
        topic="openweather_monthly",
        source="openweather",
        data=weather_data,
        key=weather_data.get("city", "unknown")
    )


def send_noaa_weather(noaa_data: dict):
    _send(
        topic="noaa_weather",
        source="noaa",
        data=noaa_data,
        key=noaa_data.get("station_id", "unknown")
    )


def send_eia_energy(eia_data: dict):
    _send(
        topic="eia_energy",
        source="eia",
        data=eia_data,
        key=eia_data.get("series_id", "unknown")
    )


# New: Send OpenMeteo data to Kafka topic and S3
def send_openmeteo_data(weather_data: dict):
    _send(
        topic="openmeteo_historical",
        source="openmeteo",
        data=weather_data,
        key=weather_data.get("city", "unknown")
    )
    # Optionally, add S3 integration here for production
    # aws_s3_bucket = os.getenv("AWS_S3_BUCKET")
    # aws_enabled = os.getenv("AWS_S3_ENABLED", "false").lower() == "true"
    # if aws_enabled and aws_s3_bucket:
    #     import boto3
    #     import json
    #     s3 = boto3.client("s3")
    #     s3.put_object(
    #         Bucket=aws_s3_bucket,
    #         Key=f"openmeteo/{weather_data['city']}/{weather_data['dt']}.json",
    #         Body=json.dumps(weather_data)
    #     )
