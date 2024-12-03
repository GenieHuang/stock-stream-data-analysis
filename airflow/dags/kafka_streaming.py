from datetime import datetime
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow/script/stock_data_extraction")
from stock_data_producer import send_stock_dataset

default_args = {
    'start_date' : datetime(2024, 10, 20, 19, 52)
}

with DAG(
    'stock-stream-data-pipeline',
    default_args = default_args,
    schedule_interval = '* * * * *',
    catchup=False) as dag:

    produce_stock_data = BashOperator(
        task_id = 'stock_real_time_data',
        bash_command=f"python3 /opt/airflow/script/stock_data_extraction/stock_data_producer.py"
    )

    produce_stock_data

