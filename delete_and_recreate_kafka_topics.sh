#!/bin/bash
# delete_and_recreate_kafka_topics.sh
# This script deletes and recreates all main Kafka topics for a clean start.

set -e

topics=(openweather_monthly sensor_readings noaa_weather eia_energy)

for topic in "${topics[@]}"; do
  echo "Deleting topic: $topic"
  docker exec kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic "$topic" || true
  # Wait for topic to be fully deleted
  for i in {1..20}; do
    sleep 1
    if ! docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -q "^$topic$"; then
      echo "Topic $topic deleted."
      break
    fi
    if [ "$i" -eq 20 ]; then
      echo "Timeout waiting for $topic to be deleted. Continuing..."
    fi
  done
  echo "Recreating topic: $topic"
  docker exec kafka kafka-topics --bootstrap-server localhost:29092 --create --topic "$topic" --partitions 1 --replication-factor 1 || true
  sleep 1
done

echo "All main topics deleted and recreated."
