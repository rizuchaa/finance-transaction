import os
import sys
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import pytz

# Load .env
load_dotenv('/opt/airflow/.env')

# Create Connection
POSTGRES_HOST = os.getenv('STAGING_HOST')
POSTGRES_PORT = 5432
POSTGRES_DB = os.getenv('STAGING_DB')
POSTGRES_USER = os.getenv('STAGING_USER')
POSTGRES_PASSWORD = os.getenv('STAGING_PASSWORD')
CSV_PATH = '/opt/airflow/data/data_source/transactions.csv'

def map_dtype(dtype):
    """
    Map pandas dtype to PostgreSQL data type.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        # Default to TEXT for object or unknown types
        return "TEXT"


def format_datetime(datetime, timezone='UTC', datetime_format='%Y-%m-%d'):
    """
    Format datetime to string datetime based on datetime_format and timezone.

    Args:
        datetime (datetime): Datetime want to convert.
        timezone (str): Destination timezone to be converted.
        datetime_format (str): Datetime format.

    Returns:
        str: Result of the converted datetime.
    """
    if timezone != 'UTC':
        datetime = pytz.timezone('UTC').localize(datetime)
        datetime = datetime.astimezone(pytz.timezone(timezone))

    return datetime.strftime(datetime_format)

def main(file_path, run_date):
    if not POSTGRES_HOST:
        raise ValueError("POSTGRES_HOST is not set! Cannot connect to DB.")

    execution_date = datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
    partition_date = \
        format_datetime(execution_date, 'Asia/Jakarta', '%Y%m%d')
    
    table_name = f"raw_sheet_finance_transactions"

    # Reading and cleaning data from CSV
    df = pd.read_csv(file_path)
    
    columns_with_types = []
    for col in df.columns:
        pg_type = map_dtype(df[col].dtype)
        columns_with_types.append(f"{col} {pg_type}")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS staging.{table_name} ({', '.join(columns_with_types)});"
    
    try:
        with psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                print("Preparing database table...")

                cur.execute(create_table_sql)
                cur.execute(f"TRUNCATE TABLE staging.{table_name};")

                # Prepare insert statement placeholders and data
                cols = df.columns.tolist()
                placeholders = ", ".join(["%s"] * len(cols))
                insert_sql = f"INSERT INTO staging.{table_name} ({', '.join(cols)}) VALUES ({placeholders})"

                print("Inserting data in a single batch...")
                data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

                execute_batch(cur, insert_sql, data_to_insert)

                print("Data insertion complete.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except FileNotFoundError:
        print(f"Error: The file was not found at {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    run_date = sys.argv[1].replace('T', ' ')
    main(CSV_PATH, run_date=run_date)
