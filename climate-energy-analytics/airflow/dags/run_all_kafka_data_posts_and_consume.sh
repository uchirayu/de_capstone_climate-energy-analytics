
#!/bin/bash
# run_all_kafka_data_posts_and_consume.sh
# This script posts sample data to all FastAPI endpoints and consumes from all main Kafka topics.


set -e
# Allow API_URL override for local runs, default to fastapi:8000 for Docker
API_URL="${API_URL_OVERRIDE:-http://fastapi:8000}"


# Assumes all required services are already running


# echo "Posting sensor data..."
# curl -X POST "$API_URL/sensor-data" \
#     -H "Content-Type: application/json" \
#     -d '{"meter_id": "MTR_001", "power_kw": 2.5, "voltage": 230, "location": "NY", "timestamp": 1768499999.0}'

# echo "Posting weather data..."
# curl -X POST "$API_URL/weather-data" \
#     -H "Content-Type: application/json" \
#     -d '{"city": "new york", "dt": 1768499999, "temperature_c": 22.5, "feels_like_c": 21.0, "humidity_pct": 60, "wind_speed_mps": 3.2, "visibility_m": 10000, "weather_main": "Clear"}'

# echo "Posting climate data..."
# curl -X POST "$API_URL/climate-data" \
#     -H "Content-Type: application/json" \
#     -d '{"station_id": "GHCND:USW00094728", "date": "2026-01-15", "datatype": "TMAX", "value": 25.0}'

# echo "Posting energy data..."
# curl -X POST "$API_URL/energy-data" \
#     -H "Content-Type: application/json" \
#     -d '{"series_id": "EIA-TEST", "period": "2026-01", "state": "NY", "sector": "residential", "price": 0.12}'

# # Post sample sensor data
# echo "Posting sensor data..."
# curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/sensor-data" \
#     -H "Content-Type: application/json" \
#     -d '{"meter_id": "MTR_001", "power_kw": 2.5, "voltage": 230, "location": "NY", "timestamp": 1768499999.0}'
# echo " (sensor-data POST complete)"

# # Post sample weather data
# echo "Posting weather data..."
# curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/weather-data" \
#     -H "Content-Type: application/json" \
#     -d '{"city": "new york", "dt": 1768499999, "temperature_c": 22.5, "feels_like_c": 21.0, "humidity_pct": 60, "wind_speed_mps": 3.2, "visibility_m": 10000, "weather_main": "Clear"}'
# echo " (weather-data POST complete)"

# # Post sample climate data
# echo "Posting climate data..."
# curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/climate-data" \
#     -H "Content-Type: application/json" \
#     -d '{"station_id": "GHCND:USW00094728", "date": "2026-01-15", "datatype": "TMAX", "value": 25.0}'
# echo " (climate-data POST complete)"

# # Post sample energy data
# echo "Posting energy data..."
# curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/energy-data" \
#     -H "Content-Type: application/json" \
#     -d '{"series_id": "EIA-TEST", "period": "2026-01", "state": "NY", "sector": "residential", "price": 0.12}'
# echo " (energy-data POST complete)"

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