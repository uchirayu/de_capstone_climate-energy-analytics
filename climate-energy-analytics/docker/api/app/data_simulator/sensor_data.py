# Schema validation and normalization for sensor data
def validate_and_normalize_sensor(payload):
    # Example normalization (customize as needed)
    try:
        return {
            "meter_id": str(payload["meter_id"]),
            "power_kw": float(payload["power_kw"]),
            "voltage": int(payload["voltage"]),
            "location": str(payload["location"]),
            "timestamp": float(payload["timestamp"]),
            "sensor_id": str(payload["sensor_id"]),
            "timestamp": float(payload["timestamp"]),
            "temperature_c": float(payload["temperature_c"]),
            "humidity_pct": float(payload["humidity_pct"])
        }
    except (KeyError, ValueError, TypeError):
        return None
import random
import time
import requests
import argparse
import sys
from typing import Optional


def simulate_sensor_data(api_url: str = "http://fastapi:8000/sensor-data", interval: float = 5.0, once: bool = False) -> None:
    
    # wait for API to be resolvable/healthy
    health_url = api_url.rstrip("/sensor-data") + "/health"
    max_wait = 60
    waited = 0
    while waited < max_wait:
        try:
            r = requests.get(health_url, timeout=3)
            if r.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(2)
        waited += 2
    
    """Continuously (or once) send simulated sensor data to `api_url`.

    Args:
        api_url: Full URL to POST sensor data to.
        interval: Seconds to sleep between sends when running continuously.
        once: If True send a single payload and return.
    """
    while True:
        payload = {
            "meter_id": "MTR_001",
            "power_kw": round(random.uniform(0.5, 5.0), 2),
            "voltage": 230,
            "location": "NY",
            "timestamp": time.time(),
            "sensor_id": f"SENSOR_{random.randint(1, 100):03d}",
            "temperature_c": round(random.uniform(18.0, 30.0), 2),
            "humidity_pct": round(random.uniform(30.0, 70.0), 2)
        }
        try:
            resp = requests.post(api_url, json=payload, timeout=5)
            status = resp.status_code
            text = resp.text
            print(f"Sent data: {payload} -> status: {status}, response: {text}")
        except requests.RequestException as exc:
            print(f"Failed to post data to {api_url}: {exc}", file=sys.stderr)

        if once:
            return

        time.sleep(interval)


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate sensor data and POST to API")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--host", action="store_true", help="Use http://localhost:8000 as the API host")
    parser.add_argument("--api-url", type=str, help="Custom full API URL (overrides --host)")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between sends when running continuously")
    parser.add_argument("--once", action="store_true", help="Send one payload then exit")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()

    if args.api_url:
        api_url = args.api_url
    else:
        api_url = "http://localhost:8000/sensor-data" if args.host else "http://fastapi:8000/sensor-data"

    simulate_sensor_data(api_url=api_url, interval=args.interval, once=args.once)