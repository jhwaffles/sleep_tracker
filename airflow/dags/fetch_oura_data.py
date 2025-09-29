import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import date

load_dotenv()
ACCESS_TOKEN = os.getenv("OURA_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
db_engine=create_engine(DATABASE_URL)

BASE_URL = "https://api.ouraring.com/v2/usercollection/"

#PATH
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT/"source_data"/"raw"
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
    
def fetch_sleep_data(start_date, end_date):  #Sleep Routes Mulitiple. Detailed Data.
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
    Checks for a local JSON file in DATA_DIR. If it exists, returns the data.
    If not, it calls the Oura API, saves the result in db, and then returns the data.
    """
    if end_date is None:
        end_date = date.today().isoformat() #Gets today's date as "YYYY-MM-DD"

    table_name=f"raw_{endpoint}"

    try:
        query = text(f"SELECT 1 FROM {table_name} WHERE day BETWEEN :start AND :end LIMIT 1")
        with db_engine.connect() as connection:
            result = connection.execute(query, {"start": start_date, "end": end_date}).scalar()
        if result is not None:
            print(f"Reading data for '{endpoint}' from PostgreSQL cache...")
            read_query = text(f"SELECT * FROM {table_name} WHERE day BETWEEN :start AND :end")
            df = pd.read_sql(read_query, con=db_engine, params={"start": start_date, "end": end_date})
            return df.to_dict('records')
    except Exception as e:
        print(f"Could not read from table '{table_name}'. It might not exist yet.")
        data_exists=False
    
    print(f"Data not found in cache. Calling Oura Ring API for '{endpoint}'...")
    params={"start_date": start_date, "end_date":end_date}
    api_data=fetch_oura_data(endpoint, params)  #take 

    if api_data and 'data' in api_data and api_data['data']: #triple condition check
        df = pd.DataFrame(api_data['data'])

        for col in df.columns:
            if df[col].dropna().apply(lambda x: isinstance(x, (dict, list))).any():  #checks if the col is a dictinoary or list
                print(f"Converting nested column '{col}' to JSON string.")
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)  #dumps nested data into json text
        print(f"Saving {len(df)} records to {table_name} table...")
        
        try:
            with db_engine.connect() as connection:
                df.to_sql(
                    table_name,
                    con=db_engine,
                    if_exists='append',
                    index=False
                )
                connection.commit()
            print("Save successful.")
        except Exception as e:
            print(f"An error occured while saving to the database: {e}")
        return api_data['data']
    return None #if API calls fails or there's no data
# Main function to get the data (either from cache or API)
if __name__ == "__main__":
    # Define the date range (format: YYYY-MM-DD)
    start_date = "2025-01-25" #define
    end_date = "2025-09-23"  #override if necessary

    print("--- Getting detailed sleep data ---")
    sleep_data = get_and_cache_oura_data("sleep", start_date, end_date)
    
    print("\n--- Getting daily sleep summary data ---")
    daily_sleep_data = get_and_cache_oura_data("daily_sleep", start_date, end_date)

    if sleep_data and daily_sleep_data:
        print("\nSuccessfully loaded all data.")






    """
    def save_to_json_file(data, filename):
    try:
        full_path=DATA_DIR / filename
        with open(full_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")


    def load_sleep(data):
    single_points = []
    time_series = []

    time_series_fields = ['hrv', 'heart_rate', 'movement_30_sec', 'sleep_phase_5_min']

    print('generating files...')

    ###Navigates JSON structure to extract time series data and single point data
    for record in data:
        readiness=record.pop('readiness', {})
        readiness_contributors = readiness.pop('contributors', {})
        readiness_combined = {f"readiness_{k}": v for k, v in {**readiness_contributors, **readiness}.items()}
        record.update(readiness_combined)
        single_point = {k: v for k, v in record.items() if k not in time_series_fields}
        single_points.append(single_point)
        
        for field in time_series_fields:
            if field in record:
                content = record[field]
            # Handle different structures of time-series data
                if isinstance(content, dict) and "items" in content:  #applies for hrv and heart_rate
                    interval = content.get("interval", 300)  #300s, or 5 min
                    start_timestamp = pd.to_datetime(content["timestamp"])
                    for idx, value in enumerate(content["items"]):
                        if value is not None:  # Exclude nulls
                            timestamp = start_timestamp + pd.to_timedelta(idx * interval, unit="s")
                            time_series.append({
                                "day": record["day"],
                                "field": field,
                                "timestamp": timestamp,
                                "value": value
                            })
                elif isinstance(content, str):  # For movement_30_sec and sleep_phase_5_min
                    for idx, value in enumerate(content):
                        timestamp = pd.to_datetime(record["bedtime_start"]) + pd.to_timedelta(idx * 30, unit="s")
                        time_series.append({
                            "day": record["day"],
                            "field": field,
                            "timestamp": timestamp,
                            "value": value
                        })
    # Create DataFrames
    oura_data_single_points = pd.DataFrame(single_points)
    oura_data_time_series = pd.DataFrame(time_series)
    return oura_data_single_points, oura_data_time_series"""