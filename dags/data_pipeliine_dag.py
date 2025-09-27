import os, sys
from pathlib import Path

# Get the absolute path to the project root directory
# This handles both direct execution and Airflow's .airflow/dags/ execution
current_file = Path(__file__).resolve()
project_root = None

# Look for the project root by finding a directory with specific markers
for parent in current_file.parents:
    if (parent / 'utils' / 'airflow_tasks.py').exists():
        project_root = str(parent)
        break

if project_root is None:
    # Fallback: assume the project root is one level up from dags
    project_root = str(current_file.parent.parent)

utils_path = os.path.join(project_root, 'utils')

# Add both paths to sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from airflow import DAG
from airflow.utils import timezone
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow_tasks import validate_input_data, run_data_pipeline


# DAG ===> Validate input data ----> Run data pipeline

default_arguments = {
                    'owner' : 'Maneth',
                    'depends_on_past' : False,
                    'start_date': timezone.datetime(2025, 9, 27, 7, 0),
                    'email_on_failuer': False,
                    'email_on_retry': False,
                    'retries': 0,
                    }

with DAG(dag_id = 'data_pipeline_dag',
        schedule_interval='*/5 * * * *',
        catchup=False,
        max_active_runs=1,
        default_args = default_arguments,
        description='Data Pipeline - Every 5 Minutes Sheduled',
        tags=['pyspark', 'mllib', 'mlflow', 'batch-processing']

        ) as dag:
    
    # Step 1
    validate_input_data_task = PythonOperator(
                                            task_id='validate_input_data',
                                            python_callable=validate_input_data,
                                            execution_timeout=timedelta(minutes=2)
                                            )
    
    # Step 2
    run_data_pipeline_task = PythonOperator(
                                            task_id='run_data_pipeline',
                                            python_callable=run_data_pipeline,
                                            execution_timeout=timedelta(minutes=15)
                                            )


    validate_input_data_task >> run_data_pipeline_task


