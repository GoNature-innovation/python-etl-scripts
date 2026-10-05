"""
Airflow DAG: build and run the silver3599 Docker pipeline daily at 12:00 AM.

Uses docker compose so the container gets:
- every variable from the project's .env file (env_file)
- the Google service account JSON mounted read-only

Requirements on the Airflow worker:
- Docker CLI + compose plugin, with access to the Docker daemon
- The project folder (Dockerfile, docker-compose.yml, .env,
  google-service-account.json) available at SILVER3599_PROJECT_DIR
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator


# Folder containing Dockerfile + docker-compose.yml + .env on the Airflow host.
# Override with Airflow Variable "silver3599_project_dir".
PROJECT_DIR = Variable.get(
    "silver3599_project_dir",
    default_var="/opt/airflow/projects/silver3599",
)

LOCAL_TZ = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver3599_razorpay_buyers",
    description="Google Sheets (Buyers) -> Azure Parquet via Docker",
    schedule="0 0 * * *",  # 12:00 AM daily
    start_date=pendulum.datetime(2026, 9, 27, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "razorpay", "docker"],
) as dag:

    build_image = BashOperator(
        task_id="build_image",
        bash_command="docker compose build",
        cwd=PROJECT_DIR,
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="docker compose run --rm silver3599",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )

    build_image >> run_pipeline
