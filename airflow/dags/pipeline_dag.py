# dags/kafka_to_gcs.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Sherouk',
    'email_on_failure': False,
}

dag = DAG(
    'kafka_to_gcs',
    default_args=default_args,
    description='Kafka to GCS pipeline orchestration',
    schedule_interval='@hourly',  
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Run products CDC consumer
products_consumer = BashOperator(
    task_id='run_products_consumer',
    bash_command='python "C:/Users/Asus/Temp/Personal_Projects/ecommerce_data_pipeline/kafka/consumers/kafka_to_bronze_products.py"',
    dag=dag,
)

# Task 2: Run clickstream consumer
clickstream_consumer = BashOperator(
    task_id='run_clickstream_consumer',
    bash_command='python "C:/Users/Asus/Temp/Personal_Projects/ecommerce_data_pipeline/kafka/consumers/kafka_to_bronze_clickstream.py"',
    dag=dag,
)

products_consumer >> clickstream_consumer
