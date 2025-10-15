FROM apache/airflow:2.9.3-python3.12 AS airflow-init

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential libpq-dev && rm -rf /var/lib/apt/lists/*
USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_MAJOR_MINOR=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt"

# Airflow-layer deps (already constrained ✅)
COPY requirements.airflow.txt /reqs/airflow.txt
RUN pip install --no-cache-dir -r /reqs/airflow.txt --constraint "${CONSTRAINT_URL}"

# Your app deps — now also constrained ✅
COPY requirements.app.txt /reqs/app.txt
RUN pip install --no-cache-dir --upgrade-strategy only-if-needed \
    -r /reqs/app.txt \
    --constraint "${CONSTRAINT_URL}" \
 && pip check
