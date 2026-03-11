"""
Airflow DAG for ETL of fuel station transaction data.

This DAG:
1. Checks for new Parquet files in the data directory
2. Reads and transforms the data
3. Loads it into PostgreSQL
4. Moves processed files to a processed directory
"""

import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Paths
DATA_DIR = "/opt/airflow/data"
PROCESSED_DIR = "/opt/airflow/processed"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def get_new_files(**context):
    """Check for new Parquet files that haven't been processed yet."""
    import glob

    # Get list of parquet files in data directory
    pattern = os.path.join(DATA_DIR, "*.parquet")
    all_files = glob.glob(pattern)

    if not all_files:
        print("No parquet files found in data directory")
        return []

    # Get list of already processed files from database
    pg_hook = PostgresHook(postgres_conn_id="postgres_target")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT filename FROM processed_files")
    processed = {row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()

    # Filter to only new files
    new_files = [f for f in all_files if os.path.basename(f) not in processed]

    print(f"Found {len(new_files)} new files to process")
    for f in new_files:
        print(f"  - {os.path.basename(f)}")

    # Push to XCom for next task
    context["ti"].xcom_push(key="new_files", value=new_files)
    return new_files


def process_and_load_files(**context):
    """Read Parquet files and load data into PostgreSQL."""
    import pyarrow.parquet as pq

    # Get files from previous task
    ti = context["ti"]
    new_files = ti.xcom_pull(key="new_files", task_ids="check_for_new_files")

    if not new_files:
        print("No new files to process")
        return

    pg_hook = PostgresHook(postgres_conn_id="postgres_target")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    total_rows = 0

    for file_path in new_files:
        filename = os.path.basename(file_path)
        print(f"Processing file: {filename}")

        try:
            # Read parquet file
            table = pq.read_table(file_path)

            rows_loaded = 0

            # Convert to list of dicts for easier processing
            records = table.to_pydict()
            num_rows = table.num_rows

            for i in range(num_rows):
                # Extract dock struct fields
                dock = records["dock"][i]
                dock_bay = dock.get("bay") if isinstance(dock, dict) else None
                dock_level = dock.get("level") if isinstance(dock, dict) else None

                # Handle services array
                services = records["services"][i]
                if services is None:
                    services = []
                else:
                    services = list(services)

                # Insert row
                cursor.execute(
                    """
                    INSERT INTO fuel_transactions (
                        transaction_id, station_id, dock_bay, dock_level,
                        ship_name, franchise, captain_name, species,
                        fuel_type, fuel_units, price_per_unit, total_cost,
                        services, is_emergency, visited_at, arrival_date,
                        coords_x, coords_y, source_file
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    (
                        records["transaction_id"][i],
                        records["station_id"][i],
                        dock_bay,
                        dock_level,
                        records["ship_name"][i],
                        records["franchise"][i],
                        records["captain_name"][i],
                        records["species"][i],
                        records["fuel_type"][i],
                        float(records["fuel_units"][i]) if records["fuel_units"][i] is not None else None,
                        float(records["price_per_unit"][i]) if records["price_per_unit"][i] is not None else None,
                        float(records["total_cost"][i]) if records["total_cost"][i] is not None else None,
                        services,
                        bool(records["is_emergency"][i]),
                        records["visited_at"][i],
                        records["arrival_date"][i],
                        float(records["coords_x"][i]) if records["coords_x"][i] is not None else None,
                        float(records["coords_y"][i]) if records["coords_y"][i] is not None else None,
                        filename,
                    ),
                )
                rows_loaded += 1

            # Record file as processed
            cursor.execute(
                """
                INSERT INTO processed_files (filename, rows_loaded)
                VALUES (%s, %s)
                ON CONFLICT (filename) DO NOTHING
                """,
                (filename, rows_loaded),
            )

            conn.commit()
            total_rows += rows_loaded
            print(f"  Loaded {rows_loaded} rows from {filename}")

        except Exception as e:
            conn.rollback()
            print(f"  Error processing {filename}: {e}")
            raise

    cursor.close()
    conn.close()

    print(f"Total rows loaded: {total_rows}")
    ti.xcom_push(key="total_rows", value=total_rows)
    ti.xcom_push(key="files_processed", value=len(new_files))


def move_processed_files(**context):
    """Move processed files to the processed directory."""
    ti = context["ti"]
    new_files = ti.xcom_pull(key="new_files", task_ids="check_for_new_files")

    if not new_files:
        print("No files to move")
        return

    # Ensure processed directory exists
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for file_path in new_files:
        filename = os.path.basename(file_path)
        dest_path = os.path.join(PROCESSED_DIR, filename)

        try:
            shutil.move(file_path, dest_path)
            print(f"Moved {filename} to processed directory")
        except Exception as e:
            print(f"Error moving {filename}: {e}")


# Define the DAG
with DAG(
    dag_id="fuel_station_etl",
    default_args=default_args,
    description="ETL pipeline for fuel station transaction data",
    schedule_interval="* * * * *",  # Run every minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "fuel_station"],
) as dag:

    # Task 1: Check for new files
    check_files = PythonOperator(
        task_id="check_for_new_files",
        python_callable=get_new_files,
        provide_context=True,
    )

    # Task 2: Process and load files into PostgreSQL
    load_data = PythonOperator(
        task_id="process_and_load_data",
        python_callable=process_and_load_files,
        provide_context=True,
    )

    # Task 3: Move processed files
    move_files = PythonOperator(
        task_id="move_processed_files",
        python_callable=move_processed_files,
        provide_context=True,
    )

    # Define task dependencies
    check_files >> load_data >> move_files
