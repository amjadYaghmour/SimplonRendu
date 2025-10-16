from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'olympic_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@yearly',
    catchup=False
)

task = BashOperator(
    task_id='check_data',
    bash_command='echo "Checking Olympic data - runs every year (simulating 4-year cycle)"',
    dag=dag
)

