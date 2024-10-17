import requests
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"

# List of cities to fetch weather data for
CITIES = {
    "London": {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503},
    "Sydney": {"lat": -33.8688, "lon": 151.2093},
    "Paris": {"lat": 48.8566, "lon": 2.3522}
}

def fetch_weather_data(city, lat, lon):
    """Fetch weather data for a given city."""
    params = {
        'lat': lat,
        'lon': lon,
        'appid': API_KEY,
        'units': 'metric'  # Use metric units
    }
    
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        print(f"Response content: {response.text}")
        return None

def main():
    while True:
        timestamp = datetime.now().isoformat()
        
        for city, coords in CITIES.items():
            data = fetch_weather_data(city, coords['lat'], coords['lon'])
            if data:
                # Add timestamp and city to the data
                data['timestamp'] = timestamp
                data['city'] = city
                
                # Print the data (in a real scenario, you'd send this to Snowflake)
                print(json.dumps(data, indent=2))
                print("\n" + "="*50 + "\n")
        
        # Wait for 5 minutes before the next fetch
        time.sleep(300)

if __name__ == "__main__":
    main()