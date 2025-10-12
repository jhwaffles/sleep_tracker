import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def disruption_score():
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("Error: DATABASE_URL not set.")
        return
    
    print("connecting to the database...")
    engine=create_engine(DATABASE_URL)

    #LOAD DATA
    print("Loading feature data from 'fct_sleep_epochs'...")
    with engine.connect() as connection:
        df=pd.read_sql("SELECT * FROM fct_sleep_epochs", connection)
    
    if df.empty:
        print("No data found in fct_sleep_epochs. exiting.")
        return
    
    #Create Features
    df_ml_features= df.copy()
    feature_cols=['hrv','heart_rate','max_movement','avg_movement']
    df_ml_features=df_ml_features.dropna(subset=feature_cols).copy()

    for i in feature_cols:
        z_col_name = f"{i}_zscore"
        mean_val=df_ml_features[i].mean()
        std_val=df_ml_features[i].std(ddof=0)
        
        if std_val>0:
            df_ml_features[z_col_name]= (df_ml_features[i]-mean_val)/std_val
        else: #avoid divide by 0.
            df_ml_features[z_col_name] = 0
    print("created z-score columns")
    df_ml_features['disruption_score']=df_ml_features['max_movement_zscore']+df_ml_features['heart_rate_zscore']-df_ml_features['hrv_zscore']
    min_score=df_ml_features['disruption_score'].min()
    max_score=df_ml_features['disruption_score'].max()
    if (max_score - min_score) > 0: 
        df_ml_features['disruption_score_normalized']=(df_ml_features['disruption_score']-min_score)/(max_score-min_score)
    else:
        df_ml_features['disruption_score_normalized']=0.5 #avoid divide by 0, just make it halfway
    print("created disruption_scores")
  
    output_table='ml_features_simple'
    print(f"saving {len(df_ml_features)} records to '{output_table}'...")

    with engine.connect() as connection:
        df_ml_features.to_sql(
            output_table,
            con=connection,
            if_exists='replace',
            index=False
        )
        connection.commit()

if __name__ == "__main__":
    disruption_score()