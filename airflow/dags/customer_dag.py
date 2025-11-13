"""Customer Department End-to-End Pipeline DAG"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from scripts.customer_ingestion import ingest_customer_data

default_args = {'owner': 'data-engineering', 'depends_on_past': False, 'retries': 2, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='customer_department_pipeline',
    default_args=default_args,
    description='Customer Department Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['department', 'customer']
) as dag:

    ingest_task = PythonOperator(task_id='ingest_customer_data', python_callable=lambda **ctx: ingest_customer_data(ctx['ds']))
    bronze_task = TriggerDagRunOperator(task_id='trigger_bronze_customer', trigger_dag_id='bronze_layer_complete', conf={'department': 'customer'}, wait_for_completion=False)
    silver_task = TriggerDagRunOperator(task_id='trigger_silver_customer', trigger_dag_id='silver_layer_complete', conf={'department': 'customer'}, wait_for_completion=False)
    gold_task = TriggerDagRunOperator(task_id='trigger_gold_customer', trigger_dag_id='gold_layer_simple_working', conf={'department': 'customer'}, wait_for_completion=False)

    ingest_task >> bronze_task >> silver_task >> gold_task
