from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import logging


# DEFAULT ARGUMENTS
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["pranathi2314@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# INCREMENTAL DATA CHECK
def check_incremental_data(**context):
    raw_path = "/opt/airflow/project/Data/raw"
    last_run = Variable.get("last_successful_run", default_var=None)

    csv_files = [f for f in os.listdir(raw_path) if f.endswith(".csv")]

    if not csv_files:
        logging.info("No raw CSV files found")
        return

    latest_modified_time = max(
        os.path.getmtime(os.path.join(raw_path, f)) for f in csv_files
    )

    if last_run is None or latest_modified_time > float(last_run):
        logging.info("New data detected. ETL will run.")
    else:
        logging.info("No new data detected.")


# UPDATE LAST RUN TIMESTAMP
def update_last_run():
    Variable.set("last_successful_run", str(datetime.now().timestamp()))
    logging.info("Updated last successful run timestamp")


# GOLD CALLBACKS (AIRFLOW NATIVE LOGGING)
def gold_on_execute(context):
    logging.info(
        f"Gold layer started | "
        f"Databricks job_id={context['task'].job_id} | "
        f"run_id={context['run_id']}"
    )

def gold_on_success(context):
    logging.info(
        f"Gold layer completed successfully | "
        f"Databricks job_id={context['task'].job_id} | "
        f"run_id={context['run_id']}"
    )

def gold_on_failure(context):
    logging.error(
        f"Gold layer FAILED | "
        f"Databricks job_id={context['task'].job_id} | "
        f"run_id={context['run_id']}",
        exc_info=True
    )


# DAG DEFINITION
with DAG(
    dag_id="stock_analytics_incremental_etl_pipeline_v2",
    default_args=default_args,
    description="Incremental Bronze-Silver-Gold ETL using Airflow & Databricks",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "silver", "gold", "incremental", "databricks"]
) as dag:

    check_data = PythonOperator(
        task_id="check_incremental_data",
        python_callable=check_incremental_data
    )

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python /opt/airflow/project/Data/Bronze/bronze_layer.py"
    )

    silver_cleaning = BashOperator(
        task_id="silver_cleaning",
        bash_command="python /opt/airflow/project/scripts/clean_transform.py"
    )

    gold_analytics = DatabricksRunNowOperator(
        task_id="gold_layer_analytics",
        databricks_conn_id="databricks_default",
        job_id=68731414949078,
        on_execute_callback=gold_on_execute,
        on_success_callback=gold_on_success,
        on_failure_callback=gold_on_failure
    )

    update_run_time = PythonOperator(
        task_id="update_last_run_time",
        python_callable=update_last_run
    )

    check_data >> bronze_ingestion >> silver_cleaning >> gold_analytics >> update_run_time
