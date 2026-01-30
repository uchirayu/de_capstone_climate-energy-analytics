import os
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from kafka_streaming.producer import send_openmeteo_data

# Predefined coordinates for cities
CITIES = {
    "new york": {"lat": 40.7128, "lon": -74.0060},
    "texas": {"lat": 31.9686, "lon": -99.9018},
    "los angeles": {"lat": 34.0522, "lon": -118.2437}
}

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Normalize Open-Meteo response to match schema

def validate_and_normalize_openmeteo(data, city):
    if not data or "daily" not in data:
        return None
    daily = data["daily"]
    results = []
    for i in range(len(daily["time"])):
        results.append({
            "dt": daily["time"][i],
            "temperature_c": daily["temperature_2m_max"][i],
            "temperature_min_c": daily["temperature_2m_min"][i],
            "precipitation_mm": daily.get("precipitation_sum", [None]*len(daily["time"]))[i],
            "windspeed_max_mps": daily.get("windspeed_10m_max", [None]*len(daily["time"]))[i],
            "city": city
        })
    return results

def fetch_openmeteo_historical_weather(city: str):
    city_key = city.lower()
    if city_key not in CITIES:
        return {"error": f"City '{city}' not supported. Choose from: {list(CITIES.keys())}"}
    lat = CITIES[city_key]["lat"]
    lon = CITIES[city_key]["lon"]
    start_date = datetime(2025, 1, 1)
    end_date = datetime.utcnow()
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max"
        ],
        "timezone": "auto"
    }
    try:
        response = requests.get(OPENMETEO_URL, params=params)
        response.raise_for_status()
        data = response.json()
        normalized = validate_and_normalize_openmeteo(data, city)
        if normalized:
            seen = set()
            unique_records = []
            for record in normalized:
                key = (record["dt"], record["city"])
                if key not in seen:
                    send_openmeteo_data(record)
                    unique_records.append(record)
                    seen.add(key)
            return {"status": f"Historical weather data for {city} (01-2025 to now) sent to Kafka", "unique_days": len(unique_records)}
        else:
            return {"error": "No data returned from Open-Meteo."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {str(e)}"}
