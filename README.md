# Finance Transaction Data Pipeline

This project includes finance transaction data handling end-to-end project, powered by **Apache Airflow**, **PostgreSQL**, and **dbt**, all packaged with **Docker Compose**.

## Features
- **Modern orchestration** with Apache Airflow `3.0.3`
- **Data transformations** using `dbt`
- **PostgreSQL** for both orchestration metadata and pipeline storage
- **Docker Compose** setup with:
  - Airflow (Scheduler, API Server, Worker)
  - Redis (for Celery broker)
  - Two PostgreSQL databases (Airflow metadata + pipeline database, for separating the workflow).
- Sample `.env` configuration for credentials and connections.
- Modular, production-ready structure.

## Project Structure

```
finance-transaction/
│
├── dags/                       # Airflow DAG definitions
├── config/scripts/             # Setup & helper scripts
│ ├── datamart_finance_day/     # Store dbt project for Data Mart view
│ ├── ingest_finance_day/       # Store raw ingestion & dbt staging (Raw -> Ingest)
│ └── present_finance_day/      # Store dbt Intermediate Data source project
├── data/data_source/           # Raw data sources
├── dbt_project/                
│ └── staging/                  # Safe profiles.yaml data for dbt project
├── others/                     # Miscellaneous utilities
│
├── .env                        # Environment variables (sample provided)
├── docker-compose.yaml         # Service definitions for local setup
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

## Prerequisites
- **Docker** >= 20.x
- **Docker Compose** >= 1.29
- **Python** 3.10+ (if running dbt locally without Docker)
- **dbt-postgres** plugin (for running dbt with Postgres)
- **PostgreSQL**

### Setup Environment

#### Clone the Repository
```bash
git clone https://github.com/rizuchaa/finance-transaction.git
cd finance-transaction
```

#### Create credentials in .env
```
#Airflow database
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_PORT=5433
AIRFLOW_DB_NAME=airflow

#Pipeline database
PIPELINE_DB_USER=postgres
PIPELINE_DB_PASSWORD=postgres
PIPELINE_DB_HOST=finance-transaction-other-postgres-1
PIPELINE_DB_PORT=5432
PIPELINE_DB_NAME=staging_finance

#DBT Configuration
DBT_PROJECT_NAME=finance_transaction
DBT_PROFILE_NAME=finance_transaction_profile
DBT_TARGET=dev

#PIP Requirements
_PIP_ADDITIONAL_REQUIREMENTS=pandas pyarrow psycopg2-binary dbt-postgres python-dotenv
```

