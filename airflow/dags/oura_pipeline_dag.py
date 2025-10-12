from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="oura_sleep_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["oura", "sleep"],
) as dag:
    
    # Task to run the data fetching script
    # This assumes fetch_oura_data.py is in the /opt/airflow/scripts directory
    fetch_data = BashOperator(
        task_id="fetch_raw_data",
        bash_command="python /opt/airflow/scripts/fetch_oura_data.py",
    )

    # Task to run dbt models
    # We navigate to the dbt project directory before running
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/dbt && dbt run",
    )

    # Task to run the simple disruption score model
    run_disruption_score = BashOperator(
        task_id="run_disruption_score",
        bash_command="python /opt/airflow/scripts/disruption_score.py",
    )

    # Task to run the PCA score model
    run_pca_score = BashOperator(
        task_id="run_pca_score",
        bash_command="python /opt/airflow/scripts/pca_score.py",
    )
    
    # Task to run the event detection model
    run_event_detection = BashOperator(
        task_id="run_event_detection",
        bash_command="python /opt/airflow/scripts/disruption_events.py",
    )

    # Define the dependencies to match your DAG
    fetch_data >> run_dbt >> [run_disruption_score, run_pca_score] >> run_event_detection

