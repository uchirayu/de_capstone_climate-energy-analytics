import os
import requests
import time
from datetime import date, timedelta
from kafka_streaming.producer import send_noaa_weather

# Schema validation and normalization for NOAA data
def validate_and_normalize_noaa(record):
    try:
        return {
            "date": record["date"],
            "datatype": record["datatype"],
            "station_id": record.get("station_id") or record.get("station"),
            "value": float(record["value"])
        }
    except (KeyError, ValueError, TypeError):
        return None

NOAA_TOKEN = os.getenv("NOAA_API_TOKEN")
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
REQUEST_TIMEOUT = int(os.getenv("NOAA_REQUEST_TIMEOUT", "30"))  # 30 seconds default
MAX_RETRIES = int(os.getenv("NOAA_MAX_RETRIES", "3"))
USE_MOCK_DATA = os.getenv("USE_MOCK_NOAA_DATA", "false").lower() == "true"

HEADERS = {
    "token": NOAA_TOKEN
}

# Mock data for development/testing
MOCK_NOAA_DATA = [
    {
        "date": "2024-12-31",
        "datatype": "TMAX",
        "station": "GHCND:USW00094728",
        "attributes": ",,W,",
        "value": 125
    },
    {
        "date": "2024-12-30",
        "datatype": "TMIN",
        "station": "GHCND:USW00094728",
        "attributes": ",,W,",
        "value": 75
    }
]

def fetch_weather_last_3_years(station_id="GHCND:USW00094728"):
    try:
        if USE_MOCK_DATA:
            print("[NOAA] Using mock data (USE_MOCK_NOAA_DATA=true)")
            for record in MOCK_NOAA_DATA:
                try:
                    normalized = validate_and_normalize_noaa(record)
                    if normalized:
                        send_noaa_weather(normalized)
                except Exception as e:
                    print(f"[NOAA] Warning: Failed to send mock data to Kafka: {e}")
            return {"status": "NOAA mock data sent to Kafka", "records": len(MOCK_NOAA_DATA), "mock": True}
        
        if not NOAA_TOKEN:
            return {"error": "NOAA_API_TOKEN environment variable not set", "records": 0}
            
        # Fetch data from January 2025 to today
        today = date.today()
        start_date = date(2025, 1, 1)
        end_date = today

        params = {
            "datasetid": "GHCND",
            "stationid": station_id,
            "startdate": start_date.isoformat(),
            "enddate": end_date.isoformat(),
            "limit": 1000
        }

        all_results = []
        offset = 1
        
        # Retry logic with exponential backoff
        for attempt in range(MAX_RETRIES):
            try:
                params["offset"] = offset
                print(f"[NOAA] Request (attempt {attempt + 1}/{MAX_RETRIES}): {start_date} to {end_date}")
                response = requests.get(
                    BASE_URL,
                    headers=HEADERS,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()
                results = data.get("results", [])
                if not results:
                    break

                all_results.extend(results)
                offset += 1000
                break  # Success, exit retry loop
                
            except requests.exceptions.Timeout:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                if attempt < MAX_RETRIES - 1:
                    print(f"[NOAA] Timeout on attempt {attempt + 1}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
            except requests.exceptions.ConnectionError as e:
                wait_time = 2 ** attempt
                if attempt < MAX_RETRIES - 1:
                    print(f"[NOAA] Connection error on attempt {attempt + 1}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise

        for record in all_results:
            try:
                normalized = validate_and_normalize_noaa(record)
                if normalized:
                    send_noaa_weather(normalized)
            except Exception as e:
                print(f"[NOAA] Warning: Failed to send record to Kafka: {e}")
        return {"status": "NOAA data sent to Kafka", "records": len(all_results)}
        
    except requests.exceptions.HTTPError as e:
        error_msg = str(e.response.text[:200]) if e.response.text else str(e)
        return {"error": f"NOAA API error {e.response.status_code}: {error_msg}", "records": 0}
    except requests.exceptions.Timeout:
        return {"error": f"NOAA API timeout after {REQUEST_TIMEOUT}s and {MAX_RETRIES} retries", "records": 0}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request error: {str(e)[:200]}", "records": 0}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)[:200]}", "records": 0}
