import pendulum
import pandas as pd
import duckdb
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DB_PATH = "/home/vscode/airflow/dev.duckdb"
CSV_PATH = "/workspaces/airflow-dbt-duckdb/data/raw_orders.csv"
DBT_DIR = "/workspaces/airflow-dbt-duckdb/dbt_project"

@dag(
    dag_id="ecom_elt_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["production", "dbt", "duckdb"],
)
def ecom_elt_pipeline():
    """
    ### Real-World E-Commerce ELT Pipeline
    1. Extract raw CSV & Load into DuckDB.
    2. Run dbt models to transform raw data into analytics tables.
    3. Validate data quality using Python & DuckDB.
    """

    @task()
    def load_raw_csv_to_duckdb():
        """Task 1: Extract & Load raw CSV into DuckDB"""
        conn = duckdb.connect(DB_PATH)
        df = pd.read_csv(CSV_PATH)
        
        # Write to DuckDB raw table
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.execute("CREATE TABLE IF NOT EXISTS raw.orders AS SELECT * FROM df;")
        conn.close()
        print("Raw data successfully ingested into DuckDB raw.orders!")

    # Task 2: Trigger dbt transformations via BashOperator
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    @task()
    def validate_data_quality():
        """Task 3: Run data quality assertion checks"""
        conn = duckdb.connect(DB_PATH)
        result = conn.execute("SELECT COUNT(*) FROM main.fct_daily_revenue").fetchone()[0]
        conn.close()

        if result == 0:
            raise ValueError("Data Quality Check Failed: fct_daily_revenue is empty!")
        
        print(f"Data Quality Check Passed! Found {result} aggregate records.")

    # Task Dependencies
    load_raw_csv_to_duckdb() >> run_dbt_models >> validate_data_quality()


# Instantiate the DAG
ecom_elt_pipeline()