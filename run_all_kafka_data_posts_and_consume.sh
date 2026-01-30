#!/bin/bash
# run_all_kafka_data_posts_and_consume.sh
# This script posts sample data to all FastAPI endpoints and consumes from all main Kafka topics.

set -e

API_URL="http://localhost:8000"

# Check if containers are up, if not, bring them up
if ! docker compose ps | grep -q "Up"; then
    echo "Containers are not running. Starting containers..."
    docker compose up -d
fi

# If docker-compose.yml changed since last build, rebuild
if [ -f .last_compose_build ]; then
    if ! cmp -s docker-compose.yml .last_compose_build; then
        echo "docker-compose.yml changed. Rebuilding containers..."
        docker compose up -d --build
        cp docker-compose.yml .last_compose_build
    fi
else
    cp docker-compose.yml .last_compose_build
fi

# Trigger continuous and ingest_all endpoints for full AWS S3 ingestion

echo "Triggering continuous sensor data ingestion..."
curl -X POST "$API_URL/sensor-data/continuous?count=100&delay=1.0"

echo "Triggering continuous OpenWeather data ingestion..."
curl -X POST "$API_URL/weather/openweather/continuous?count=100&delay=60.0"

echo "Triggering continuous NOAA data ingestion..."
curl -X POST "$API_URL/weather/noaa/continuous?count=100&delay=60.0"

echo "Triggering continuous EIA energy data ingestion..."
curl -X POST "$API_URL/energy/eia/continuous?count=100&delay=60.0"

echo "Triggering OpenMeteo ingest_all..."
curl -X POST "$API_URL/weather/openmeteo/ingest_all"


# Consume from all main topics
echo "\nConsuming from openweather_monthly..."
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic openweather_monthly --from-beginning --timeout-ms 5000

echo "\nConsuming from sensor_readings..."
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic sensor_readings --from-beginning --timeout-ms 5000

echo "\nConsuming from noaa_weather..."
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic noaa_weather --from-beginning --timeout-ms 5000

echo "\nConsuming from eia_energy..."
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic eia_energy --from-beginning --timeout-ms 5000

echo "\nConsuming from openmeteo_historical..."
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic openmeteo_historical --from-beginning --timeout-ms 5000

echo "All done."
