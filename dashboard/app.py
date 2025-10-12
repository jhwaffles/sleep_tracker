# File: dashboard/app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
import plotly.express as px

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Oura Sleep Dashboard",
    page_icon="😴",
    layout="wide"
)

# --- DATABASE CONNECTION ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("DATABASE_URL environment variable not set. Cannot connect to the database.")
    st.stop()

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    st.error(f"Failed to create database engine. Error: {e}")
    st.stop()


# --- DATA LOADING FUNCTIONS ---
@st.cache_data
def load_data(query):
    """Function to load general data from the database."""
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        return df
    except Exception as e:
        st.error(f"Failed to load data from database. Error: {e}")
        return pd.DataFrame()

@st.cache_data
def load_timeseries_data(selected_day):
    """Function to load time-series data for a specific day."""
    # Use parameterized query for safety
    query = text("SELECT * FROM ml_features_simple WHERE day = :day ORDER BY epoch_timestamp")
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params={'day': selected_day})
        return df
    except Exception as e:
        st.error(f"Failed to load time-series data. Error: {e}")
        return pd.DataFrame()


# --- Load initial data for selectors ---
df_daily_sleep = load_data("SELECT * FROM fct_daily_sleep ORDER BY day DESC")

if df_daily_sleep.empty:
    st.warning("No data found in fct_daily_sleep. Please run your dbt models.")
    st.stop()

df_daily_sleep['day'] = pd.to_datetime(df_daily_sleep['day']).dt.date


# --- DASHBOARD ---
st.title("Oura Sleep Analysis")

tab1, tab2, tab3 = st.tabs(["📅 Daily Summary", "📈 Nightly Time Series", "Disruption Events"])

# --- TAB 1: DAILY SUMMARY ---
with tab1:
    st.header("Daily Sleep Metrics")
    st.sidebar.header("Daily Chart Options")
    all_metrics = [
        'daily_sleep_score', 'readiness_score', 'deep_sleep_duration', 
        'rem_sleep_duration', 'total_sleep_duration', 'efficiency', 
        'latency_duration', 'resting_heart_rate'
    ]
    # Filter out columns that don't exist in the dataframe to avoid errors
    available_metrics = [m for m in all_metrics if m in df_daily_sleep.columns]
    
    selected_metrics = st.sidebar.multiselect(
        label="Select daily metrics:",
        options=available_metrics,
        default=[m for m in ['daily_sleep_score', 'readiness_score'] if m in available_metrics]
    )

    if not selected_metrics:
        st.warning("Please select at least one metric from the sidebar.")
    else:
        st.line_chart(
            df_daily_sleep.rename(columns={'day': 'index'}).set_index('index'),
            y=selected_metrics
        )
    
    st.header("Daily Sleep Data")
    st.dataframe(df_daily_sleep)


# --- TAB 2: NIGHTLY TIME SERIES (Refactored) ---
with tab2:
    st.header("Time Series Analysis for a Single Night")

    day_options = sorted(df_daily_sleep['day'].dropna().unique(), reverse=True)
    selected_day = st.selectbox("Select a day to view:", options=day_options)

    if selected_day:
        # Get sleep_id for the selected day
        sleep_id_for_day_series = df_daily_sleep[df_daily_sleep['day'] == selected_day]['sleep_id']
        
        if not sleep_id_for_day_series.empty:
            sleep_id_for_day = sleep_id_for_day_series.iloc[0]

            # Load data from the two tables
            epochs_df = load_data(
                "SELECT * FROM fct_sleep_epochs WHERE sleep_id = :sleep_id ORDER BY epoch_timestamp",
                params={'sleep_id': sleep_id_for_day}
            )
            ml_df = load_data(
                "SELECT * FROM ml_features_simple WHERE sleep_id = :sleep_id ORDER BY epoch_timestamp",
                params={'sleep_id': sleep_id_for_day}
            )

            if not epochs_df.empty and not ml_df.empty:
                # Join the two dataframes
                merged_df = pd.merge(epochs_df, ml_df, on=['sleep_id', 'epoch_timestamp'], how='inner')

                ts_metrics_options = ['hrv', 'heart_rate', 'max_movement', 'avg_movement', 'sleep_phase', 'disruption_score_normalized']
                selected_ts_metrics = st.multiselect(
                    "Select time-series metrics to display:",
                    options=ts_metrics_options,
                    default=['disruption_score_normalized', 'sleep_phase']
                )

                if selected_ts_metrics:
                    st.line_chart(merged_df, x='epoch_timestamp', y=selected_ts_metrics)
                else:
                    st.warning("Please select at least one metric.")
            else:
                st.warning(f"No time-series or ML feature data found for {selected_day}.")
        else:
            st.warning(f"Could not find a sleep_id for {selected_day}.")


# --- TAB 3: DISRUPTION EVENT VISUALIZATION (NEW) ---
with tab3:
    st.header("Sleep Disruption Event Detection")
    st.markdown("This tab visualizes the segments detected by the change point algorithm on the disruption score time series.")

    day_options_events = sorted(df_daily_sleep['day'].dropna().unique(), reverse=True)
    selected_day_events = st.selectbox("Select a day to visualize events:", options=day_options_events, key='event_day_select')

    if selected_day_events:
        # Find the corresponding sleep_id
        sleep_id_for_day_series = df_daily_sleep[df_daily_sleep['day'] == selected_day_events]['sleep_id']

        if not sleep_id_for_day_series.empty:
            sleep_id_for_day = sleep_id_for_day_series.iloc[0]

            # Load the disruption score time series and the detected events
            score_query = "SELECT epoch_timestamp, disruption_score_normalized FROM ml_features_simple WHERE sleep_id = :sleep_id ORDER BY epoch_timestamp"
            events_query = "SELECT event_start, event_end, avg_disruption_score FROM fct_sleep_disruption_events WHERE sleep_id = :sleep_id ORDER BY event_start"

            df_scores = load_data(score_query, params={'sleep_id': sleep_id_for_day})
            df_events = load_data(events_query, params={'sleep_id': sleep_id_for_day})

            if df_scores.empty:
                st.warning(f"No disruption score data found for {selected_day_events}.")
            else:
                # Create the base line chart of the disruption score
                fig = px.line(
                    df_scores, 
                    x='epoch_timestamp', 
                    y='disruption_score_normalized', 
                    title=f'Disruption Score and Detected Events for {selected_day_events}'
                )

                # Overlay the detected event segments as shaded regions
                if not df_events.empty:
                    for _, event in df_events.iterrows():
                        fig.add_vrect(
                            x0=event['event_start'], 
                            x1=event['event_end'], 
                            fillcolor="red", 
                            opacity=0.2, 
                            line_width=0,
                            annotation_text=f"Avg: {event['avg_disruption_score']:.2f}",
                            annotation_position="top left"
                        )

                st.plotly_chart(fig, use_container_width=True)
                
                if not df_events.empty:
                    st.subheader("Detected Event Segments")
                    st.dataframe(df_events)
                else:
                    st.info("No distinct disruption events were detected for this night.")
        else:
            st.warning(f"Could not find a sleep_id for {selected_day_events}.")