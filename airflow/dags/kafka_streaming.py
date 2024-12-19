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
    schedule_interval = '@daily',
    catchup=False) as dag:

    produce_stock_data = BashOperator(
        task_id = 'stock_real_time_data',
        bash_command=f"python3 /opt/airflow/script/stock_data_extraction/stock_data_producer.py"
    )

    # spark_transformation = BashOperator(
    #     task_id = 'spark_transformation',
    #     bash_command=f"spark-submit   
    #     --jars pyspark_data_transformation/jar_files/spark-sql-kafka-0-10_2.12-3.4.4.jar,pyspark_data_transformation/jar_files/spark-token-provider-kafka-0-10_2.12-3.4.4.jar,pyspark_data_transformation/jar_files/kafka-clients-3.3.2.jar,pyspark_data_transformation/jar_files/commons-pool2-2.11.1.jar   
    #     --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 
    #     --master spark://localhost:7077  
    #     pyspark_data_transformation/spark_processing.py"
    # )

    produce_stock_data # >> spark_transformation

