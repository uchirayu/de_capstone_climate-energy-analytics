#!/bin/sh
set -e

# Wait for FastAPI health endpoint, then run the simulator
HEALTH_URL="${HEALTH_URL:-http://fastapi:8000/health}"
API_URL="${API_URL:-http://fastapi:8000/sensor-data}"
MAX_WAIT="${MAX_WAIT:-60}"
SLEEP="${SLEEP:-1}"

elapsed=0
while true; do
  if curl -sSf "$HEALTH_URL" >/dev/null 2>&1; then
    echo "FastAPI is healthy, starting simulator"
    break
  fi
  if [ "$elapsed" -ge "$MAX_WAIT" ]; then
    echo "Timed out waiting for $HEALTH_URL after $elapsed seconds" >&2
    exit 1
  fi
  echo "waiting for fastapi... ($elapsed/$MAX_WAIT)"
  sleep "$SLEEP"
  elapsed=$((elapsed + SLEEP))
done

exec python -u data_simulator/sensor_data.py --api-url "$API_URL"
