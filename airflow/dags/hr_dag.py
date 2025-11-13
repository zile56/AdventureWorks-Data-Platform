"""HR Department End-to-End Pipeline DAG"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from scripts.hr_ingestion import ingest_hr_data

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='hr_department_pipeline',
    default_args=default_args,
    description='HR Department End-to-End Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['department', 'hr']
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_hr_data',
        python_callable=lambda **ctx: ingest_hr_data(ctx['ds'])
    )

    bronze_task = TriggerDagRunOperator(
        task_id='trigger_bronze_hr',
        trigger_dag_id='bronze_layer_complete',
        conf={'department': 'hr'},
        wait_for_completion=False  # Don't wait - bronze processes all departments
    )

    silver_task = TriggerDagRunOperator(
        task_id='trigger_silver_hr',
        trigger_dag_id='silver_layer_complete',
        conf={'department': 'hr'},
        wait_for_completion=False  # Don't wait - silver processes all departments
    )

    gold_task = TriggerDagRunOperator(
        task_id='trigger_gold_hr',
        trigger_dag_id='gold_layer_simple_working',
        conf={'department': 'hr'},
        wait_for_completion=False
    )

    ingest_task >> bronze_task >> silver_task >> gold_task
