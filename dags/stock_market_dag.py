from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'stock_market_dag',
    default_args=default_args,
    description='A simple DAG to run Kafka producer and Spark consumer',
    schedule_interval='@daily',
)

# Task to run Kafka producer
run_kafka_producer = BashOperator(
    task_id='run_kafka_producer',
    bash_command='python /usr/local/airflow/dags/kafka_producer.py',
    dag=dag,
)

# Task to run Spark consumer
run_spark_consumer = BashOperator(
    task_id='run_spark_consumer',
    bash_command='python /usr/local/airflow/dags/kafka_spark_consumer.py',
    dag=dag,
)

# Set task dependencies
run_kafka_producer >> run_spark_consumer