# File: test_db_write.py

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# --- 1. SETUP ---
# Make sure your .env file is configured for localhost
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL") 
if not DATABASE_URL or "localhost" not in DATABASE_URL:
    print("ERROR: Please make sure your .env file has a DATABASE_URL for localhost.")
    exit()

print(f"Attempting to connect to: {DATABASE_URL}")
db_engine = create_engine(DATABASE_URL)

# --- 2. CREATE A SAMPLE DATAFRAME ---
data = {'col1': [1, 2], 'col2': ['A', 'B']}
df = pd.DataFrame(data)
table_name = 'test_table'
print(f"Attempting to save this DataFrame to '{table_name}':")
print(df)

# --- 3. ATTEMPT TO SAVE TO DATABASE ---
try:
    with db_engine.connect() as connection:
        df.to_sql(
            table_name,
            con=connection,
            if_exists='replace', # Use 'replace' for easy re-running of this test
            index=False
        )
        connection.commit()
    print("\nSUCCESS: Data was saved and transaction was committed.")
except Exception as e:
    print(f"\nERROR: An exception occurred: {e}")