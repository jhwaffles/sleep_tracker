import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import date, timedelta

load_dotenv()
ACCESS_TOKEN = os.getenv("OURA_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
db_engine=create_engine(DATABASE_URL)

BASE_URL = "https://api.ouraring.com/v2/usercollection/"

#PATH
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
DATA_DIR = Path(AIRFLOW_HOME) / "data" / "raw"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_oura_data(endpoint, params=None):
    """
    Gets JSON through request via URL + designated data (endpoint). Outputs a python dictionary
    """

    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    try:
        response = requests.get(BASE_URL + endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raise an HTTP Error for bad responses
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def fetch_sleep_data(start_date, end_date):  #Sleep Routes Multiple. Detailed Data.
    """
    Sleep Routes Multiple. Detailed Data. Provides endpoint and date range and feeds into fetch_oura_data.
    """
    endpoint = "sleep"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    return fetch_oura_data(endpoint, params)

def fetch_daily_sleep_data(start_date, end_date):  #Daily Sleep Routes Multiple. Summarized Data.
    """
    Daily Sleep Data. Summary Data. Provides endpoint and date range and feeds into fetch_oura_data.
    """
    endpoint = "daily_sleep"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    return fetch_oura_data(endpoint, params)

def get_and_cache_oura_data(endpoint, start_date, end_date=None):
    """
    TEMPORARY DEBUGGING FUNCTION to test only the database write.
    """
    table_name = f"raw_{endpoint}"
    print(f"--- STARTING DEBUG TEST for table: {table_name} ---")

    # 1. Create a simple, hardcoded DataFrame
    data = {'day': ['2025-10-01'], 'id': ['test-id-123'], 'score': [99]}
    df = pd.DataFrame(data)
    print("Created a test DataFrame:")
    print(df)

    # 2. Attempt to save this DataFrame
    try:
        print(f"Attempting to save to '{table_name}'...")
        with db_engine.connect() as connection:
            df.to_sql(
                table_name,
                con=connection,
                if_exists='append',
                index=False
            )
            connection.commit()
        print("SUCCESS: Save command executed and transaction committed.")
    except Exception as e:
        print(f"ERROR: An exception occurred during the save operation: {e}")

    # For this test, we just return the DataFrame we tried to save
    return df.to_dict('records')

if __name__ == "__main__":
    # Define the date range (format: YYYY-MM-DD)
    start_date = "2025-01-25"
    end_date = None

    print("--- Getting detailed sleep data ---")
    sleep_data = get_and_cache_oura_data("sleep", start_date, end_date)
    
    print("\n--- Getting daily sleep summary data ---")
    daily_sleep_data = get_and_cache_oura_data("daily_sleep", start_date, end_date)

    if sleep_data and daily_sleep_data:
        print("\nSuccessfully loaded all data.")


