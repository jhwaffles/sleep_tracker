/sleep_tracker/
├── .env                  
├── .gitignore            
├── docker-compose.yml    
├── README.md             
│
├── airflow/
│   ├── Dockerfile          
│   ├── dags/               
│   │   └── oura_elt_dag.py
│   ├── logs/
│   └── requirements.txt    
│
├── dbt/
│   ├── models/            
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml     
│
└── dashboard/
    ├── Dockerfile          
    ├── app.py              
    └── requirements.txt   

## Local Setup

1.  Clone this repository.
2.  Install `dbt` (`pip install dbt-postgres`).
3.  Find the `profiles.yml.example` file in this project.
4.  Copy its contents to your personal `~/.dbt/profiles.yml` file.
5.  Replace the placeholder values `[YOUR_USERNAME]` and `[YOUR_PASSWORD]` with your local PostgreSQL credentials.
6.  Run `dbt debug` from the project directory to confirm your connection is working.