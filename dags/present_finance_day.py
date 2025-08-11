from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
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
    dag_id="present_finance_day",
    default_args = default_args,
    start_date=datetime(2025, 8, 8),
    schedule="30 18 * * *",  # runs at 17:30 UTC every day
    catchup=False,
) as dag:

    # Wait DAG

    # int_usd_transaction
    wait_std_transactions_int_usd_transaction = ExternalTaskSensor(
        task_id='wait_ingest_finance_day_std_sheet_finance_transactions_present_finance_day_presented_finance_int_usd_transaction',
        external_dag_id='ingest_finance_day',
        external_task_id='ingest_finance_day_std_sheet_finance_transactions',
        mode='reschedule',
        poke_interval=60,
        allowed_states=['success', 'skipped'],
        timeout=2500,
        retries=1,  # number of retries on failure
        retry_delay=timedelta(seconds=20),  # wait 20 secs before retry
        dag=dag,
    )

    # int_geo_transaction
    wait_std_accounts_int_geo_transaction = ExternalTaskSensor(
        task_id='wait_ingest_finance_day_std_sheet_finance_accounts_present_finance_day_presented_finance_int_geo_transaction',
        external_dag_id='ingest_finance_day',
        external_task_id='ingest_finance_day_std_sheet_finance_accounts',
        mode='reschedule',
        poke_interval=60,
        allowed_states=['success', 'skipped'],
        timeout=2500,
        retries=1,  # number of retries on failure
        retry_delay=timedelta(seconds=20),  # wait 20 secs before retry
        dag=dag,
    )

    wait_std_customers_int_geo_transaction = ExternalTaskSensor(
        task_id='wait_ingest_finance_day_std_sheet_finance_customers_present_finance_day_presented_finance_int_geo_transaction',
        external_dag_id='ingest_finance_day',
        external_task_id='ingest_finance_day_std_sheet_finance_customers',
        mode='reschedule',
        poke_interval=60,
        allowed_states=['success', 'skipped'],
        timeout=2500,
        retries=1,  # number of retries on failure
        retry_delay=timedelta(seconds=20),  # wait 20 secs before retry
        dag=dag,
    )

    wait_int_usd_transaction_int_geo_transaction = ExternalTaskSensor(
        task_id='wait_present_finance_day_presented_finance_int_usd_transaction_present_finance_day_presented_finance_int_geo_transaction',
        external_dag_id='present_finance_day',
        external_task_id='present_finance_day_presented_finance_int_usd_transaction',
        mode='reschedule',
        poke_interval=60,
        allowed_states=['success', 'skipped'],
        timeout=2500,
        retries=1,  # number of retries on failure
        retry_delay=timedelta(seconds=20),  # wait 20 secs before retry
        dag=dag,
    )

    # Presented
    presented_finance_int_usd_transaction = BashOperator(
        task_id='present_finance_day_presented_finance_int_usd_transaction',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/config/scripts/present_finance_day/presented_finance/int_usd_transaction &&
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

    presented_finance_int_geo_transaction = BashOperator(
        task_id='present_finance_day_presented_finance_int_geo_transaction',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow/config/scripts/present_finance_day/presented_finance/int_geo_transaction &&
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

    wait_std_transactions_int_usd_transaction >> presented_finance_int_usd_transaction
    presented_finance_int_usd_transaction >> presented_finance_int_geo_transaction

    wait_std_customers_int_geo_transaction >> presented_finance_int_geo_transaction
    wait_std_accounts_int_geo_transaction >> presented_finance_int_geo_transaction

    presented_finance_int_usd_transaction >> end_task
    presented_finance_int_geo_transaction >> end_task
