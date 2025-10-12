FROM apache/airflow:2.8.0

USER root
# Install git for dbt dependencies if needed
RUN apt-get update && apt-get install -y git

USER airflow
# Install dbt and other python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt