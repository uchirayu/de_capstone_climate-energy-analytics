
import os
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from kafka_streaming.producer import send_openweather_data

# Schema validation and normalization for OpenWeather data
def validate_and_normalize_weather(data):
    # Example schema normalization (customize as needed)
    if not data or "current" not in data:
        return None
    current = data["current"]
    return {
        "dt": current.get("dt"),
        "temperature_c": current.get("temp"),
        "feels_like_c": current.get("feels_like"),
        "humidity_pct": current.get("humidity"),
        "wind_speed_mps": current.get("wind_speed"),
        "visibility_m": current.get("visibility"),
        "weather_main": current.get("weather", [{}])[0].get("main"),
        "city": data.get("timezone", "unknown")
    }

API_KEY = os.getenv("OPENWEATHER_API_KEY")
ONECALL_URL = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


# Predefined coordinates for cities
CITIES = {
    "new york": {"lat": 40.7128, "lon": -74.0060},
    "texas": {"lat": 31.9686, "lon": -99.9018},
    "los angeles": {"lat": 34.0522, "lon": -118.2437}
}

# Fetch monthly weather data from 01-2025 to now
def fetch_monthly_weather_from_2025(city: str):
    if not API_KEY:
        return {"error": "OPENWEATHER_API_KEY not configured"}
    city_key = city.lower()
    if city_key not in CITIES:
        return {"error": f"City '{city}' not supported. Choose from: {list(CITIES.keys())}"}
    lat = CITIES[city_key]["lat"]
    lon = CITIES[city_key]["lon"]
    results = []
    start_date = datetime(2025, 1, 1)
    now = datetime.utcnow()
    current = start_date
    seen = set()
    while current <= now:
        # OpenWeather's historical API only allows up to 5 days ago for free tier, but for demo, simulate monthly fetch
        dt = int(current.timestamp())
        params = {
            "lat": lat,
            "lon": lon,
            "dt": dt,
            "appid": API_KEY,
            "units": "metric"
        }
        try:
            response = requests.get(ONECALL_URL, params=params)
            response.raise_for_status()
            data = response.json()
            normalized = validate_and_normalize_weather(data)
            if normalized:
                key = (normalized["dt"], normalized["city"])
                if key not in seen:
                    send_openweather_data(normalized)
                    results.append(normalized)
                    seen.add(key)
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                return {"error": "Invalid OpenWeather API key. Check OPENWEATHER_API_KEY in .env"}
            else:
                return {"error": f"OpenWeather API error: {str(e)}"}
        except requests.exceptions.RequestException as e:
            return {"error": f"Request failed: {str(e)}"}
        current += relativedelta(months=1)
    return {"status": f"Monthly weather data for {city} (01-2025 to now) sent to Kafka", "unique_months": len(results)}

def fetch_historical_weather(city: str, days: int = 5):
    """
    Deprecated: Use fetch_monthly_weather_from_2025 for monthly data from 01-2025 onward.
    """
    return {"error": "Deprecated. Use fetch_monthly_weather_from_2025."}