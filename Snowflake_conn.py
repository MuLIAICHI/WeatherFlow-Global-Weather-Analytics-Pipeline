import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = 'WEATHER_DB'  # New database name
SNOWFLAKE_SCHEMA = 'WEATHER_SCHEMA'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

try:
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role='ACCOUNTADMIN'
    )
    
    cursor = conn.cursor()
    print("Connected to Snowflake successfully.")
    
    # Create and use the new database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    print(f"Using database: {SNOWFLAKE_DATABASE}")
    
    # Create and use schema
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    print(f"Using schema: {SNOWFLAKE_SCHEMA}")
    
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
    print("Table created or already exists.")
    
    # Test insert
    test_insert_sql = """
    INSERT INTO weather_data (city, timestamp, temperature)
    VALUES ('TestCity', CURRENT_TIMESTAMP(), 20.5)
    """
    cursor.execute(test_insert_sql)
    print("Test row inserted successfully.")
    
    conn.commit()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()

print("Script completed.")