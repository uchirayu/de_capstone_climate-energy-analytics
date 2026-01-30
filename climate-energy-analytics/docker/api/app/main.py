# Fix: Add missing import for time
import random
import asyncio
import time
# POST endpoint to ingest all OpenWeather monthly data for all supported cities
from fastapi import FastAPI
from data_clients.openweather_client import fetch_monthly_weather_from_2025, fetch_historical_weather
from data_clients.eia_client_data import fetch_energy_data
from data_clients.noaa_client import fetch_weather_last_3_years
from pydantic import BaseModel
from fastapi import BackgroundTasks
from data_clients.openmeteo_client import fetch_openmeteo_historical_weather
from kafka_streaming.producer import send_sensor_data, send_openweather_data, send_noaa_weather, send_eia_energy

app = FastAPI(title="Climate & Energy Data API")

class SensorData(BaseModel):
    meter_id: str
    power_kw: float
    voltage: int
    location: str
    timestamp: float
    sensor_id: str
    temperature_c: float
    humidity_pct: float
    
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/history")
def get_weather_history():
    """
    Fetch NOAA weather data from January 2025 to today (normalized and sent to Kafka).
    """
    return fetch_weather_last_3_years()


# Current weather endpoint (unchanged)
#@app.get("/weather/{city}")
#def get_weather(city: str):
#    return fetch_weather(city)


# Monthly weather endpoint for supported cities (from 01-2025 to now, normalized)
@app.get("/weather/monthly/{city}")
def get_monthly_weather(city: str):
    """
    Fetch monthly weather data for New York, Texas, or Los Angeles from 01-2025 to now (normalized).
    Example: /weather/monthly/new%20york
    """
    return fetch_monthly_weather_from_2025(city)

# (Optional) Keep old endpoint for backward compatibility, but mark as deprecated
@app.get("/weather/historical/{city}")
def get_historical_weather(city: str, days: int = 5):
    """
    Deprecated: Use /weather/monthly/{city} for monthly data from 01-2025 onward.
    """
    return fetch_historical_weather(city, days)

@app.get("/energy/{state}")
def get_energy_data(state: str):
    return fetch_energy_data(state)

#@app.post("/sensor-data")
#async def simulate_sensor_data(data: dict):
#    print(f"Received: {data}")
#    return {"status": "ACCEPTS POST", "data": data}
@app.get("/weather/openmeteo/historical/{city}")
def get_openmeteo_historical_weather(city: str):
    """
    Fetch 1 year historical weather data for a city from Open-Meteo (01-2025 to now), send to Kafka and S3.
    Example: /weather/openmeteo/historical/new%20york
    """
    return fetch_openmeteo_historical_weather(city)



# POST endpoint for sensor data (normalized and sent to Kafka)
@app.post("/sensor-data")
async def sensor_data(data: SensorData):
    send_sensor_data(data.dict())
    return {"status": "sensor data sent to Kafka"}

# POST endpoint for OpenWeather data
from fastapi import Body

@app.post("/weather-data")
async def weather_data(data: dict = Body(...)):
    send_openweather_data(data)
    return {"status": "weather data sent to Kafka"}

# POST endpoint for NOAA climate data
@app.post("/climate-data")
async def climate_data(data: dict = Body(...)):
    send_noaa_weather(data)
    return {"status": "climate data sent to Kafka"}

# POST endpoint for EIA energy data
@app.post("/energy-data")
async def energy_data(data: dict = Body(...)):
    send_eia_energy(data)
    return {"status": "energy data sent to Kafka"}

@app.post("/weather/openmeteo/data")
async def post_openmeteo_data(data: dict = Body(...)):
    """
    Send OpenMeteo weather data to Kafka topic openmeteo_historical and S3.
    """
    from kafka_streaming.producer import send_openmeteo_data
    send_openmeteo_data(data)
    return {"status": "OpenMeteo data sent to Kafka and S3"}


@app.post("/weather/openmeteo/ingest_all")
async def ingest_all_openmeteo_historical(background_tasks: BackgroundTasks):
    """
    Ingest all historical OpenMeteo data for all supported cities (from Jan 2025 to today).
    This will fetch and send all records to Kafka and S3 in the background.
    """
    from data_clients.openmeteo_client import fetch_openmeteo_historical_weather, CITIES
    results = {}
    def ingest_city(city):
        results[city] = fetch_openmeteo_historical_weather(city)
    for city in CITIES:
        background_tasks.add_task(ingest_city, city)
    return {"status": "Ingestion started for all cities.", "cities": list(CITIES.keys())}

@app.post("/weather/openweather/ingest_all")
async def ingest_all_openweather_monthly(background_tasks: BackgroundTasks):
    """
    Ingest all available OpenWeather monthly data for all supported cities (from Jan 2025 to now).
    This will fetch and send all records to Kafka and S3 in the background.
    """
    from data_clients.openweather_client import fetch_monthly_weather_from_2025, CITIES
    results = {}
    def ingest_city(city):
        results[city] = fetch_monthly_weather_from_2025(city)
    for city in CITIES:
        background_tasks.add_task(ingest_city, city)
    return {"status": "Ingestion started for all cities.", "cities": list(CITIES.keys())}

@app.post("/weather/noaa/continuous")
async def continuous_noaa_data(background_tasks: BackgroundTasks, count: int = 100, delay: float = 60.0):
    """
    Continuously ingest NOAA weather data into Kafka and S3.
    Params:
      count: number of messages to send (default 100)
      delay: seconds between fetches (default 60.0)
    """
    from data_clients.noaa_client import fetch_weather_last_3_years
    async def produce():
        for i in range(count):
            fetch_weather_last_3_years()
            await asyncio.sleep(delay)
    background_tasks.add_task(asyncio.run, produce())
    return {"status": f"Started continuous NOAA data ingestion for {count} cycles."}

# POST endpoint to continuously ingest real-time EIA energy data
@app.post("/energy/eia/continuous")
async def continuous_eia_energy(background_tasks: BackgroundTasks, count: int = 100, delay: float = 60.0):
    """
    Continuously ingest EIA energy data into Kafka and S3.
    Params:
      count: number of messages to send (default 100)
      delay: seconds between fetches (default 60.0)
    """
    from data_clients.eia_client_data import fetch_energy_data
    states = ["NY", "TX", "CA"]
    async def produce():
        for i in range(count):
            for state in states:
                fetch_energy_data(state)
            await asyncio.sleep(delay)
    background_tasks.add_task(asyncio.run, produce())
    return {"status": f"Started continuous EIA energy data ingestion for {count} cycles."}

@app.post("/sensor-data/continuous")
async def continuous_sensor_data(background_tasks: BackgroundTasks, count: int = 100, delay: float = 1.0):
    """
    Continuously generate and ingest sensor readings data into Kafka and S3.
    Params:
      count: number of messages to send (default 100)
      delay: seconds between messages (default 1.0)
    """
    from kafka_streaming.producer import send_sensor_data
    async def produce():
        for i in range(count):
            data = {
                "meter_id": f"SENS_{random.randint(1000,9999)}",
                "power_kw": round(random.uniform(1.0, 10.0), 2),
                "voltage": random.choice([220, 230, 240]),
                "location": random.choice(["NY", "CA", "TX"]),
                "timestamp": int(time.time()),
                "sensor_id": f"SENSOR_{random.randint(1, 100):03d}",
                "temperature_c": round(random.uniform(18.0, 30.0), 2),
                "humidity_pct": round(random.uniform(30.0, 70.0), 2)
            }
            send_sensor_data(data)
            await asyncio.sleep(delay)
    background_tasks.add_task(asyncio.run, produce())
    return {"status": f"Started continuous sensor data ingestion for {count} messages."}

@app.post("/weather/openweather/continuous")
async def continuous_openweather_data(background_tasks: BackgroundTasks, count: int = 100, delay: float = 60.0):
    """
    Continuously fetch and ingest current OpenWeather data for all supported cities into Kafka and S3.
    Params:
      count: number of messages to send (default 100)
      delay: seconds between fetches (default 60.0)
    """
    from data_clients.openweather_client import CITIES, API_KEY
    import requests
    from kafka_streaming.producer import send_openweather_data
    ONECALL_URL = "https://api.openweathermap.org/data/2.5/weather"
    async def produce():
        for i in range(count):
            for city, coords in CITIES.items():
                params = {
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                    "appid": API_KEY,
                    "units": "metric"
                }
                try:
                    response = requests.get(ONECALL_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                    normalized = {
                        "dt": data.get("dt"),
                        "temperature_c": data.get("main", {}).get("temp"),
                        "feels_like_c": data.get("main", {}).get("feels_like"),
                        "humidity_pct": data.get("main", {}).get("humidity"),
                        "wind_speed_mps": data.get("wind", {}).get("speed"),
                        "visibility_m": data.get("visibility"),
                        "weather_main": data.get("weather", [{}])[0].get("main"),
                        "city": city
                    }
                    send_openweather_data(normalized)
                except Exception as e:
                    print(f"Error fetching OpenWeather data for {city}: {e}")
            await asyncio.sleep(delay)
    background_tasks.add_task(asyncio.run, produce())
    return {"status": f"Started continuous OpenWeather data ingestion for {count} cycles."}


# GET endpoint for OpenMeteo historical weather data



