# Schema validation and normalization for EIA data
def validate_and_normalize_eia(record):
    try:
        return {
            "period": record["period"],
            "state": record.get("stateid"),
            "sector": record.get("sectorid"),
            "price": float(record["price"]) if "price" in record else None
        }
    except (KeyError, ValueError, TypeError):
        return None
import os
import requests
from kafka_streaming.producer import send_eia_energy

EIA_API_KEY = os.getenv("EIA_API_KEY")
BASE_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"

def fetch_energy_data(state="NY"):
    if not EIA_API_KEY:
        print("[EIA] ERROR: EIA_API_KEY not configured")
        return {"error": "EIA_API_KEY not configured"}

    params = {
        "api_key": EIA_API_KEY,
        "frequency": "monthly",
        "data[0]": "price",
        "facets[stateid][]": state,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 12
    }
    try:
        print(f"[EIA] Requesting: {BASE_URL} with params {params}")
        response = requests.get(BASE_URL, params=params)
        print(f"[EIA] Response status: {response.status_code}")
        response.raise_for_status()
        json_data = response.json()
        print(f"[EIA] Response JSON: {json_data}")
        data = json_data.get("response", {}).get("data", [])

        if not data:
            print("[EIA] No data returned from API.")
        for record in data:
            normalized = validate_and_normalize_eia(record)
            if normalized:
                print(f"[EIA] Sending normalized record to Kafka: {normalized}")
                send_eia_energy(normalized)

        return {"status": "EIA data sent to Kafka", "records": len(data)}
    except requests.exceptions.HTTPError as e:
        print(f"[EIA] HTTPError: {e}, Response: {getattr(e.response, 'text', None)}")
        if response.status_code == 401:
            return {"error": "Invalid EIA API key. Check EIA_API_KEY in .env"}
        else:
            return {"error": f"EIA API error: {str(e)}"}
    except requests.exceptions.RequestException as e:
        print(f"[EIA] RequestException: {e}")
        return {"error": f"Request failed: {str(e)}"}
    except Exception as e:
        print(f"[EIA] Unexpected error: {e}")
        return {"error": f"Unexpected error: {str(e)}"}