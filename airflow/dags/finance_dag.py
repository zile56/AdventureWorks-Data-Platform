"""Finance Department End-to-End Pipeline DAG"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from scripts.finance_ingestion import ingest_finance_data

default_args = {'owner': 'data-engineering', 'depends_on_past': False, 'retries': 2, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='finance_department_pipeline',
    default_args=default_args,
    description='Finance Department Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['department', 'finance']
) as dag:

    ingest_task = PythonOperator(task_id='ingest_finance_data', python_callable=lambda **ctx: ingest_finance_data(ctx['ds']))
    bronze_task = TriggerDagRunOperator(task_id='trigger_bronze_finance', trigger_dag_id='bronze_layer_complete', conf={'department': 'finance'}, wait_for_completion=False)
    silver_task = TriggerDagRunOperator(task_id='trigger_silver_finance', trigger_dag_id='silver_layer_complete', conf={'department': 'finance'}, wait_for_completion=False)
    gold_task = TriggerDagRunOperator(task_id='trigger_gold_finance', trigger_dag_id='gold_layer_simple_working', conf={'department': 'finance'}, wait_for_completion=False)

    ingest_task >> bronze_task >> silver_task >> gold_task
