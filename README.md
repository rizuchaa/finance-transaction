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
```bash
# POSTGRESQL CONFIGURATION
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=5433

# OTHER POSTGRESQL CONFIGURATION
OTHER_POSTGRES_USER=postgres
OTHER_POSTGRES_PASSWORD=new_postgres
OTHER_POSTGRES_DB=postgres
OTHER_POSTGRES_PORT=5432

# DBT Configuration
DBT_PROJECT_NAME=finance_transaction
DBT_PROFILE_NAME=finance_transaction_profile
DBT_TARGET=dev

# PIP Requirements
_PIP_ADDITIONAL_REQUIREMENTS=pandas pyarrow psycopg2-binary dbt-postgres python-dotenv
```

#### Launch Docker
```bash
docker-compose up -d
```

This launches:
- Postgres (Airflow metadata)
- Postgres (pipeline database)
- Redis
- Airflow Scheduler, Webserver, Worker

> Access Airflow UI at: http://localhost:8080

# Set-up & Running dbt

## 1. Install dbt
The dbt is already installed when you launch Docker (see PIP Requirements)

## 2. Configure dbt Profile
dbt uses a profiles.yml file (usually in ~/dbt_project/staging/profiles.yml):
```yaml
finance_transaction:
  target: dev
  outputs:
    dev:
      type: postgres
      host: <pipeline-postgre-container>
      user: postgres
      password: <password>
      port: 5432
      dbname: staging_finance
      schema: staging
    
    prod:
      type: postgres
      host: <pipeline-postgre-container>
      user: postgres
      password: <password>
      port: 5432
      dbname: staging_finance # This project aims all data recorded in the same db.
      schema: public
```
If running dbt inside Docker, host should be postgres-pipeline (the container name).

## 3. Run dbt (inside Docker)
```bash
docker compose exec -it finance-transaction-airflow-worker-1 bash
cd config/scripts/{desired project}

# Install dependencies (if any)
dbt deps

# Run all models
dbt run

# Test models
dbt test
```

# Workflow
- Airflow orchestrates data ingestion from data/data_source/ and loads into the pipeline PostgreSQL.
- dbt transforms raw data into structured, analytics-ready tables.
- Airflow DAGs trigger dbt runs after ingestion completes.
- the rest of dbt process from ingestion -> staging -> intermediate -> mar orchestrated by Airflow.

> [!TIP] 
[The DAG Structure explained here](!https://github.com/rizuchaa/finance-transaction/tree/main/dags#readme)

From the DAG structure, the data flow described as:
| Stage	|DAG	|Data Source	|Output Table
|:--|:--|:--|:--|
|Ingestion |	ingest_finance_day	| CSV	| staging.* tables |
|Presentation|	present_finance_day|	Standardized tables	| Curated USD & GEO transactions|
|Datamart|	datamart_finance_day|	Presentation tables	| Fraud analysis mart|




