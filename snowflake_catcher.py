import traceback
import requests
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"

# Snowflake configuration
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = 'WEATHER_DB'
SNOWFLAKE_SCHEMA = 'WEATHER_SCHEMA'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

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
        'units': 'metric'
    }
    
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        print(f"Response content: {response.text}")
        return None


def insert_into_snowflake(data_list):
    """Insert weather data into Snowflake."""
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role='ACCOUNTADMIN'
        )
        
        cursor = conn.cursor()
        print("Connected to Snowflake successfully.")
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR,
            timestamp TIMESTAMP,
            temperature FLOAT,
            feels_like FLOAT,
            humidity INT,
            wind_speed FLOAT,
            wind_deg INT,
            weather_main VARCHAR,
            weather_description VARCHAR,
            clouds INT,
            rain_1h FLOAT,
            snow_1h FLOAT,
            raw_data VARIANT
        )
        """
        cursor.execute(create_table_sql)
        
        # Insert data
        insert_sql = """
        INSERT INTO weather_data (city, timestamp, temperature, feels_like, humidity, wind_speed, wind_deg, 
                                  weather_main, weather_description, clouds, rain_1h, snow_1h, raw_data)
        SELECT
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
        """
        
        for data in data_list:
            cursor.execute(insert_sql, (
                data['city'],
                data['timestamp'],
                data['temperature'],
                data['feels_like'],
                data['humidity'],
                data['wind_speed'],
                data['wind_deg'],
                data['weather_main'],
                data['weather_description'],
                data['clouds'],
                data['rain_1h'],
                data['snow_1h'],
                json.dumps(data['raw_data'])
            ))
        
        conn.commit()
        print(f"Successfully inserted {len(data_list)} rows into Snowflake.")
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
        print("Error details:")
        print(traceback.format_exc())
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    while True:
        timestamp = datetime.now().isoformat()
        data_to_insert = []
        
        for city, coords in CITIES.items():
            data = fetch_weather_data(city, coords['lat'], coords['lon'])
            if data:
                current = data.get('current', {})
                weather = current.get('weather', [{}])[0]
                
                weather_data = {
                        'city': city,
                        'timestamp': timestamp,
                        'temperature': current.get('temp'),
                        'feels_like': current.get('feels_like'),
                        'humidity': current.get('humidity'),
                        'wind_speed': current.get('wind_speed'),
                        'wind_deg': current.get('wind_deg'),
                        'weather_main': weather.get('main'),
                        'weather_description': weather.get('description'),
                        'clouds': current.get('clouds'),
                        'rain_1h': current.get('rain', {}).get('1h', 0),
                        'snow_1h': current.get('snow', {}).get('1h', 0),
                        'raw_data': data  # Store the entire API response
                    }
                
                data_to_insert.append(weather_data)
        
        if data_to_insert:
            insert_into_snowflake(data_to_insert)
        
        # Wait for 5 minutes before the next fetch
        time.sleep(300)

if __name__ == "__main__":
    main()