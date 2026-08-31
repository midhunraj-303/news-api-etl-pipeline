from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.config import load_config

from scripts.ingest import run_ingestion
from scripts.profile import run_profile
from scripts.clean import run_clean
from scripts.transform import run_transform
from scripts.warehouse import run_warehouse
from scripts.star_schema import run_star_schema
from scripts.analytics import run_analytics


config = load_config("config/config_airflow.yaml")

with DAG(
    dag_id = "news_api_etl_pipeline",
    description="ETL pipeline for NewsAPI data using PySpark, PostgreSQL and Airflow",
    start_date = datetime(2026,8,25),
    schedule = None,
    catchup = False,

    #Retry configuration
    default_args = {
        "retries" : 2,
        "retry_delay" : timedelta(minutes=5)
    },

    tags = ["news","etl","pyspark","postgresql"],
)as dag:

    ingestion_task =PythonOperator(
        task_id = "ingestion",
        python_callable = run_ingestion,
        op_kwargs = {"config" : config},
    )

    profile_task = PythonOperator(
        task_id = "profile",
        python_callable = run_profile,
        op_kwargs = {"config" : config},
    )

    clean_task = PythonOperator(
        task_id = "clean",
        python_callable = run_clean,
        op_kwargs = {"config" : config},
    )

    transform_task = PythonOperator(
        task_id = "transform",
        python_callable = run_transform,
        op_kwargs ={"config" : config}
    )

    warehouse_task = PythonOperator(
        task_id = "warehouse",
        python_callable = run_warehouse,
        op_kwargs ={"config" : config}
    )

    star_schema_task = PythonOperator(
        task_id = "star_schema",
        python_callable = run_star_schema,
        op_kwargs ={"config" : config}
    )

    analytics_task = PythonOperator(
        task_id = "analytics",
        python_callable = run_analytics,
        op_kwargs ={"config" : config}
    )

    ingestion_task >> profile_task
    profile_task >> clean_task
    clean_task >> transform_task
    transform_task >> warehouse_task
    warehouse_task >> star_schema_task
    star_schema_task >> analytics_task