/oura-sleep-analysis/
├── .env                  # <-- Your secrets live here (NEVER commit to Git)
├── .gitignore            # <-- Tell Git to ignore .env, __pycache__, etc.
├── docker-compose.yml    # <-- The master file to launch all your services
├── README.md             # <-- Your project's documentation
│
├── airflow/
│   ├── Dockerfile          # <-- Instructions to build the Airflow service
│   ├── dags/               # <-- Your main pipeline Python script goes here
│   │   └── oura_elt_dag.py
│   ├── logs/
│   └── requirements.txt    # <-- Python packages for your Airflow tasks
│
├── dbt/
│   ├── models/             # <-- Your SQL transformation files go here
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml     # <-- dbt project configuration
│
└── dashboard/
    ├── Dockerfile          # <-- Instructions to build the Streamlit service
    ├── app.py              # <-- The Python code for your Streamlit dashboard
    └── requirements.txt    # <-- Python packages for your dashboard

## Local Setup

1.  Clone this repository.
2.  Install `dbt` (`pip install dbt-postgres`).
3.  Find the `profiles.yml.example` file in this project.
4.  Copy its contents to your personal `~/.dbt/profiles.yml` file.
5.  Replace the placeholder values `[YOUR_USERNAME]` and `[YOUR_PASSWORD]` with your local PostgreSQL credentials.
6.  Run `dbt debug` from the project directory to confirm your connection is working.