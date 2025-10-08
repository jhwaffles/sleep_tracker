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
    Finds which dates are missing from the
    database within the requested range, fetches only those, and then returns
    the complete dataset from the database.
    """
    if end_date is None:
        end_date = date.today().isoformat() #isoformat date as "YYYY-MM-DD so the reset of SQL queries can use it).

    table_name=f"raw_{endpoint}"
    start_date_obj = date.fromisoformat(start_date)
    end_date_obj = date.fromisoformat(end_date)

    #1 Get a list of dates we need
    all_requested_dates = {
        (start_date_obj + timedelta(days=i)).isoformat()
        for i in range((end_date_obj - start_date_obj).days + 1)
    }

    #2 find which dates already exist in the database
    try:
        query = text(f"SELECT DISTINCT day FROM {table_name} WHERE day BETWEEN :start AND :end")
        with db_engine.connect() as connection:
            existing_dates_df = pd.read_sql(query, connection, params={'start': start_date, 'end': end_date})

        if existing_dates_df.empty:
            existing_dates = set()
        else:
            existing_dates = set(pd.to_datetime(existing_dates_df['day']).dt.date.astype(str))

    except Exception as e:
        print(f"Could not read from table '{table_name}'. Error: {e}")
        existing_dates = set()
    
    #3 determine missing dates
    missing_dates = sorted(list(all_requested_dates - existing_dates))

    #4 fetch only the missing dates from the API
    if missing_dates:
        print(f"Found {len(missing_dates)} missing days(s) of data. Fetching from Oura API.")
        for day_to_fetch in missing_dates:
            print(f" Fetching {endpoint} for {day_to_fetch}...")
            api_data = fetch_oura_data(endpoint, params={"start_date": day_to_fetch, "end_date": day_to_fetch})
            print(f"  --> API Response for {day_to_fetch}: {api_data}")
            if api_data and 'data' in api_data and api_data['data']: #triple condition check
                df = pd.DataFrame(api_data['data'])

                for col in df.columns:
                    if df[col].dropna().apply(lambda x: isinstance(x, (dict, list))).any():  #checks if the col is a dictinoary or list
                        print(f"Converting nested column '{col}' to JSON string.")
                        df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)  #dumps nested data into json text
                try:
                    with db_engine.connect() as connection:
                        df.to_sql(
                            table_name,
                            con=connection, 
                            if_exists='append',
                            index=False
                        )
                        connection.commit() 
                    print(f"  Save for {day_to_fetch} successful.")
                except Exception as e:
                    print(f"An error occurred while saving to the database: {e}")
        
    else:
        print("No missing dates found. Data is already up-to-date.")
    
    #5 return full complete dataset fro the database
    print(f"Reading complete data for '{endpoint}' from PostgreSQL...")
    read_query = text(f"SELECT * FROM {table_name} WHERE day BETWEEN :start AND :end")
    final_df = pd.read_sql(read_query, con=db_engine, params={"start": start_date, "end": end_date})
    return final_df.to_dict('records')

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


