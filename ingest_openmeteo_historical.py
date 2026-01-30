import requests
import time

API_URL = "http://localhost:8000/weather/openmeteo/historical/{}"
CITIES = ["new york", "texas", "los angeles"]

for city in CITIES:
    print(f"Fetching and ingesting historical data for {city}...")
    url = API_URL.format(city.replace(" ", "%20"))
    try:
        response = requests.get(url)
        print(f"Response for {city}: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Error for {city}: {e}")
    time.sleep(2)  # Avoid hammering the API

print("All historical OpenMeteo data ingested.")
