from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv
import os

# Load .env
load_dotenv("/opt/airflow/.env", override=True)

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

with DAG(
    dag_id="ingest_finance_day",
    default_args=default_args,
    start_date=datetime(2025, 8, 8),
    schedule="30 17 * * *",  # runs at 17:30 UTC every day
    catchup=False,
) as dag:

    create_schema = BashOperator(
        task_id='create_schema',
        bash_command=(
            'psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "CREATE SCHEMA IF NOT EXISTS staging;"'
        ),
        env={
            "PGHOST": os.getenv("STAGING_HOST"),
            "PGUSER": os.getenv("STAGING_USER"),
            "PGPASSWORD": os.getenv("STAGING_PASSWORD"),
            "PGDATABASE": os.getenv("STAGING_DB"),
            "PGPORT": "5432",
        },
        dag=dag,
    )

    # RAW tasks
    raw_sheet_finance_transactions = BashOperator(
        task_id="ingest_finance_day_raw_sheet_finance_transactions",
        bash_command=(
            "python /opt/airflow/config/scripts/ingest_finance_day/raw/_sheet_finance_transactions/script.py "
            "{{ macros.ds_add(ds, -1)}}T17:30:00 postgres_staging_finance"
        )
    )

    raw_sheet_finance_accounts = BashOperator(
        task_id="ingest_finance_day_raw_sheet_finance_accounts",
        bash_command=(
            "python /opt/airflow/config/scripts/ingest_finance_day/raw/_sheet_finance_accounts/script.py "
            "{{ macros.ds_add(ds, -1)}}T17:30:00 postgres_staging_finance"
        )
    )

    raw_sheet_finance_customers = BashOperator(
        task_id="ingest_finance_day_raw_sheet_finance_customers",
        bash_command=(
            "python /opt/airflow/config/scripts/ingest_finance_day/raw/_sheet_finance_customers/script.py "
            "{{ macros.ds_add(ds, -1)}}T17:30:00 postgres_staging_finance"
        )
    )

    std_sheet_finance_transactions = BashOperator(
        task_id='ingest_finance_day_std_sheet_finance_transactions',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/config/scripts/ingest_finance_day/std/_sheet_finance_transactions &&
            dbt run \
                --select query \
                --profiles-dir /opt/airflow/dbt_project/staging \
                --no-use-colors \
                --log-format text \
                --quiet \
        """
    )

    std_sheet_finance_accounts = BashOperator(
        task_id='ingest_finance_day_std_sheet_finance_accounts',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/config/scripts/ingest_finance_day/std/_sheet_finance_accounts &&
            dbt run \
                --select query \
                --profiles-dir /opt/airflow/dbt_project/staging \
                --no-use-colors \
                --log-format text \
                --quiet \
        """
    )

    std_sheet_finance_customers = BashOperator(
        task_id='ingest_finance_day_std_sheet_finance_customers',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/config/scripts/ingest_finance_day/std/_sheet_finance_customers &&
            dbt run \
                --select query \
                --profiles-dir /opt/airflow/dbt_project/staging \
                --no-use-colors \
                --log-format text \
                --quiet \
        """
    )

    # end task
    end_task = EmptyOperator(
        task_id='end',
        trigger_rule='all_success',  # default, so you can omit this
        dag=dag,
    )

    # Dependencies
    create_schema >> [raw_sheet_finance_transactions,
                      raw_sheet_finance_accounts,
                      raw_sheet_finance_customers]

    raw_sheet_finance_transactions >> std_sheet_finance_transactions
    raw_sheet_finance_accounts >> std_sheet_finance_accounts
    raw_sheet_finance_customers >> std_sheet_finance_customers

    std_sheet_finance_transactions >> end_task
    std_sheet_finance_accounts >> end_task
    std_sheet_finance_customers >> end_task
