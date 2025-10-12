import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import plotly.express as px
import plotly.graph_objects as go

def generate_pca_score():
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

    # 1. Load Data
    print("Loading feature data from 'fct_sleep_epochs'...")
    with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM fct_sleep_epochs", connection)
    
    if df.empty:
        print("No data found in fct_sleep_epochs. Exiting.")
        return
    
    # 2. Prepare Features for PCA
    feature_cols = ['hrv', 'heart_rate', 'avg_movement']
    df_for_pca = df.dropna(subset=feature_cols).copy()
    
    if df_for_pca.empty:
        print("No valid data for PCA after dropping nulls. Exiting.")
        return

    # 3. Standardize the features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df_for_pca[feature_cols])

    # 4. Apply PCA with all components to check variance
    print("Running PCA to analyze components...")
    pca = PCA() # Run with all components first for analysis
    pca.fit(scaled_features)

    # --- VALIDATION STEP 1: Get Loadings and Explained Variance ---
    loadings = pca.components_[0] # Loadings for the first component (PC1)
    explained_variance_ratio = pca.explained_variance_ratio_

    # Create a DataFrame for loadings to make it easy to read
    loadings_df = pd.DataFrame(
        {'feature': feature_cols, 'pc1_loading': loadings}
    ).sort_values(by='pc1_loading', ascending=False)
    
    print("\n--- PCA Validation ---")
    print(f"Explained Variance by PC1: {explained_variance_ratio[0]:.2%}")
    print("Loadings for PC1:")
    print(loadings_df)
    print("---------------------\n")

    # 5. Re-run PCA for only PC1 and Correct Sign
    pca_final = PCA(n_components=1)
    pc1_scores = pca_final.fit_transform(scaled_features)

    hrv_loading = pca_final.components_[0][feature_cols.index('hrv')]
    if hrv_loading > 0:
        print("Flipping sign of PC1 to align with disruption concept.")
        pc1_scores = -pc1_scores
    
    df_for_pca['disruption_score_pca'] = pc1_scores

    # 6. Normalize and Save... (rest of your code is the same)
    min_score = df_for_pca['disruption_score_pca'].min()
    max_score = df_for_pca['disruption_score_pca'].max()
    if (max_score - min_score) > 0:
        df_for_pca['disruption_score_pca_normalized'] = (
            (df_for_pca['disruption_score_pca'] - min_score) / (max_score - min_score)
        )
    else:
        df_for_pca['disruption_score_pca_normalized'] = 0.5
    
    print("Created PCA-based disruption scores.")

    # 7. Save Results
    output_table = 'ml_features_pca'
    output_df = df_for_pca[['sleep_id', 'epoch_timestamp', 'disruption_score_pca_normalized']]

    print(f"Saving {len(output_df)} records to '{output_table}'...")
    with engine.connect() as connection:
        output_df.to_sql(
            output_table, con=connection, if_exists='replace', index=False
        )
        connection.commit()
    print("Save successful.")
    
    # 8. --- GENERATE VALIDATION REPORT ---
    generate_validation_report(explained_variance_ratio, loadings_df, df_for_pca)


def generate_validation_report(explained_variance, loadings_df, results_df):
    """Generates an HTML report with validation plots."""
    print("Generating validation report...")
    report_filename = "pca_validation_report.html"
    
    with open(report_filename, 'w') as f:
        f.write("<html><head><title>PCA Validation Report</title></head><body>")
        f.write("<h1>PCA Disruption Score - Validation Report</h1>")
        
        # Explained Variance Plot
        fig_var = px.bar(
            x=[f"PC{i+1}" for i in range(len(explained_variance))],
            y=explained_variance,
            title="Explained Variance by Principal Component",
            labels={'x': 'Principal Component', 'y': 'Explained Variance Ratio'},
            text=[f"{var:.1%}" for var in explained_variance]
        )
        f.write(fig_var.to_html(full_html=False, include_plotlyjs='cdn'))
        
        # Loadings Table
        f.write("<h2>Loadings for PC1</h2>")
        f.write(loadings_df.to_html(index=False))
        
        # Disruption Score Histogram
        fig_hist = px.histogram(
            results_df,
            x='disruption_score_pca_normalized',
            title="Distribution of Normalized Disruption Scores",
            nbins=50
        )
        f.write(fig_hist.to_html(full_html=False, include_plotlyjs='cdn'))
        
        f.write("</body></html>")
    print(f"Validation report saved to '{report_filename}'")


if __name__ == "__main__":
    generate_pca_score()

