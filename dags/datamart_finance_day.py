from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    dag_id="datamart_finance_day",
    default_args=default_args,
    start_date=datetime(2025, 8, 8),
    schedule="30 19 * * *",  # runs at 17:30 UTC every day
    catchup=False,
) as dag:

    # Wait DAG
    wait_int_geo_transaction_mart_finance_fraud_transaction = ExternalTaskSensor(
        task_id='wait_present_finance_day_presented_finance_int_geo_transaction_datamart_finance_day_mart_finance_mart_finance_fraud_transaction',
        external_dag_id='present_finance_day',
        external_task_id='present_finance_day_presented_finance_int_geo_transaction',
        mode='reschedule',
        poke_interval=60,
        allowed_states=['success', 'skipped'],
        timeout=2500,
        retries=1,  # number of retries on failure
        retry_delay=timedelta(seconds=20),  # wait 20 secs before retry
        dag=dag,
    )

    # Presented
    mart_finance_mart_finance_fraud_transaction = BashOperator(
        task_id='present_finance_day_mart_finance_mart_finance_fraud_transaction',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/config/scripts/datamart_finance_day/mart_finance/mart_finance_fraud_transaction &&
            dbt run \
                --select query \
                --target prod \
                --profiles-dir /opt/airflow/dbt_project/staging \
                --no-use-colors \
                --log-format text \
                --quiet \
                --vars '{"run_date_suffix": "{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}"}'
        """
    )

    end_task = EmptyOperator(
        task_id='end',
        trigger_rule='all_success',  # default, so you can omit this
        dag=dag,
    )

    # Dependencies
    wait_int_geo_transaction_mart_finance_fraud_transaction >> mart_finance_mart_finance_fraud_transaction

    mart_finance_mart_finance_fraud_transaction >> end_task
