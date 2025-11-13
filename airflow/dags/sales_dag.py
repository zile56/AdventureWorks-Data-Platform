"""
Sales Department End-to-End Pipeline DAG
Orchestrates Landing → Bronze → Silver → Gold for Sales department
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts to path
sys.path.append('/opt/airflow')
from scripts.sales_ingestion import ingest_sales_data

# Default args
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
with DAG(
    dag_id='sales_department_pipeline',
    default_args=default_args,
    description='Sales Department End-to-End Data Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['department', 'sales', 'end-to-end']
) as dag:

    def run_sales_ingestion(**context):
        """Run sales data ingestion"""
        execution_date = context['ds']
        print(f"Running sales ingestion for {execution_date}")
        
        results = ingest_sales_data(execution_date)
        
        print(f"Ingestion complete: {results['success']} success, {results['failed']} failed")
        print(f"Tables: {', '.join(results['tables'])}")
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='ingestion_results', value=results)
        
        if results['failed'] > 0:
            raise Exception(f"Some ingestions failed: {results['failed']}")
        
        return results

    # Task 1: Run sales ingestion (Landing layer)
    sales_ingestion_task = PythonOperator(
        task_id='ingest_sales_data',
        python_callable=run_sales_ingestion,
        provide_context=True
    )

    # Task 2: Trigger Bronze layer for sales department
    trigger_bronze_task = TriggerDagRunOperator(
        task_id='trigger_bronze_sales',
        trigger_dag_id='bronze_layer_complete',
        conf={'department': 'sales'},
        wait_for_completion=False
    )

    # Task 3: Trigger Silver layer for sales department
    trigger_silver_task = TriggerDagRunOperator(
        task_id='trigger_silver_sales',
        trigger_dag_id='silver_layer_complete',
        conf={'department': 'sales'},
        wait_for_completion=False
    )

    # Task 4: Trigger Gold layer for sales department
    trigger_gold_task = TriggerDagRunOperator(
        task_id='trigger_gold_sales',
        trigger_dag_id='gold_layer_simple_working',
        conf={'department': 'sales'},
        wait_for_completion=False
    )

    # Task dependencies
    sales_ingestion_task >> trigger_bronze_task >> trigger_silver_task >> trigger_gold_task
