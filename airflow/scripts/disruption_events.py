#disruption_events.py

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import ruptures as rpt

#load from disruption score table.

#apply mean segmentation thru ruptures
def generate_disruption_events():
    """
    Loads sleep epoch data, calculates a disruption score using PCA,
    saves the results, and generates a validation report.
    """
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("Error: DATABASE_URL not set.")
        return
    
    print("Connecting to the database...")
    engine = create_engine(DATABASE_URL)

    #1. load data
    print("Loading data from 'ml_features_simple'...")
    with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM ml_features_simple ORDER BY epoch_timestamp", connection)
    
    if df.empty:
        print("No data found in ml_features_simple. Exiting.")
        return
    
    df['epoch_timestamp'] = pd.to_datetime(df['epoch_timestamp'])

    #2. process data one night at at time
    all_events=[]
    for sleep_id, night_df in df.groupby('sleep_id'):
        print(f"\nProcessing sleep session: {sleep_id}")

        night_df = night_df.sort_values('epoch_timestamp').reset_index(drop=True)

        if night_df.empty:
            print("skip empty night.")
            continue
        signal=night_df['disruption_score_normalized'].to_numpy()
        #3. change point detection
        algo=rpt.Pelt(model="l2").fit(signal)
        change_points=algo.predict(pen=0.1)

        
        print(f"Detect {len(change_points)-1} change points.")
        segment_indices=[0]+change_points

        for i in range(len(segment_indices) - 1):
            start_idx = segment_indices[i]
            # The change point is the start of the next segment, so the end index is one before it
            end_idx = segment_indices[i+1] - 1
            # A failsafe for the very last segment
            if end_idx < 0:
                end_idx = start_idx

            # Get the actual timestamps using the indices
            start_timestamp = night_df.loc[start_idx, 'epoch_timestamp']
            end_timestamp = night_df.loc[end_idx, 'epoch_timestamp']
            
            # Calculate the average score for this segment
            avg_disruption = night_df.iloc[start_idx:end_idx + 1]['disruption_score_normalized'].mean()

            all_events.append({
                'sleep_id': sleep_id,
                'event_start': start_timestamp,
                'event_end': end_timestamp,
                'avg_disruption_score': avg_disruption
            })

    if not all_events:
        print('No events generated. Exiting.')
        return

    events_df=pd.DataFrame(all_events)
    output_table='fct_sleep_disruption_events'
    print(f"\nSaving {len(events_df)} disruption events to '{output_table}'...")
    with engine.connect() as connection:
        events_df.to_sql(
            output_table,
            con=connection,
            if_exists='replace',
            index=False
        )
        connection.commit()

    print("Disruption event generation complete. Results saved.")

if __name__ == "__main__":
    generate_disruption_events()

#