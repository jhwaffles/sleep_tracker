# File: dashboard/app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

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
    query = text("SELECT timestamp, metric_name, metric_value FROM int_sleep_timeseries WHERE day = :day ORDER BY timestamp")
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

tab1, tab2 = st.tabs(["📅 Daily Summary", "📈 Nightly Time Series"])

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
        df_timeseries = load_timeseries_data(selected_day)
        
        if df_timeseries.empty:
            st.warning(f"No time-series data found for {selected_day}.")
        else:
            # --- NEW: Loop to create four separate plots ---
            ts_metrics_to_plot = ['hrv', 'heart_rate', 'movement', 'sleep_phase']

            for metric in ts_metrics_to_plot:
                # Create a more descriptive title
                st.subheader(f"{metric.replace('_', ' ').title()} Trend")

                # Filter the dataframe for the specific metric
                metric_df = df_timeseries[df_timeseries['metric_name'] == metric]

                if not metric_df.empty:
                    # Plot the individual metric
                    st.line_chart(
                        metric_df,
                        x='timestamp',
                        y='metric_value'
                    )
                else:
                    st.write(f"No data available for '{metric}' on this day.")

            st.markdown("---")
            st.header("Raw Time-Series Data")
            st.dataframe(df_timeseries)