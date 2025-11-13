from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values
from io import BytesIO
import logging

# ------------------- Configuration -------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET_NAME = "adventureworks"

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "1004"
}

# ------------------- Default DAG args -------------------
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ------------------- Schema Definitions for Bronze Layer -------------------
# Schema enforcement: Map table names to their proper column names
BRONZE_SCHEMAS = {
    'sales_customer': ['CustomerID', 'PersonID', 'StoreID', 'TerritoryID', 'AccountNumber', 'rowguid', 'ModifiedDate'],
    'hr_employee': ['EmployeeID', 'NationalIDNumber', 'LoginID', 'OrganizationLevel', 'JobTitle', 'BirthDate', 'MaritalStatus', 'Gender', 'HireDate', 'SalariedFlag', 'VacationHours', 'SickLeaveHours', 'CurrentFlag', 'rowguid', 'ModifiedDate'],
    'hr_person': ['PersonID', 'PersonType', 'NameStyle', 'Title', 'FirstName', 'MiddleName', 'LastName', 'Suffix', 'EmailPromotion', 'AdditionalContactInfo', 'Demographics', 'rowguid', 'ModifiedDate'],
    'sales_product': ['ProductID', 'Name', 'ProductNumber', 'Color', 'StandardCost', 'ListPrice', 'Size', 'Weight', 'ProductCategoryID', 'ProductModelID', 'SellStartDate', 'SellEndDate', 'DiscontinuedDate', 'ThumbNailPhoto', 'ThumbnailPhotoFileName', 'rowguid', 'ModifiedDate'],
    'marketing_product_category': ['ProductCategoryID', 'ParentProductCategoryID', 'Name', 'rowguid', 'ModifiedDate'],
    'marketing_product_subcategory': ['ProductSubcategoryID', 'ProductCategoryID', 'Name', 'rowguid', 'ModifiedDate'],
    'sales_sales_order_detail': ['SalesOrderID', 'SalesOrderDetailID', 'OrderQty', 'ProductID', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'rowguid', 'ModifiedDate'],
    'sales_sales_order_header': ['SalesOrderID', 'RevisionNumber', 'OrderDate', 'DueDate', 'ShipDate', 'Status', 'OnlineOrderFlag', 'SalesOrderNumber', 'PurchaseOrderNumber', 'AccountNumber', 'CustomerID', 'ShipToAddressID', 'BillToAddressID', 'ShipMethod', 'CreditCardApprovalCode', 'SubTotal', 'TaxAmt', 'Freight', 'TotalDue', 'Comment', 'rowguid', 'ModifiedDate'],
    'sales_sales_territory': ['TerritoryID', 'Name', 'CountryRegionCode', 'Group', 'SalesYTD', 'SalesLastYear', 'CostYTD', 'CostLastYear', 'rowguid', 'ModifiedDate'],
    'operations_address': ['AddressID', 'AddressLine1', 'AddressLine2', 'City', 'StateProvince', 'CountryRegion', 'PostalCode', 'rowguid', 'ModifiedDate'],
    # AdventureWorks database tables (same schema as sales tables)
    'adventureworks_salesorderdetail': ['SalesOrderID', 'SalesOrderDetailID', 'OrderQty', 'ProductID', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'rowguid', 'ModifiedDate'],
}

# ------------------- DAG definition -------------------
with DAG(
    dag_id='bronze_layer_complete',
    default_args=default_args,
    start_date=datetime(2025, 11, 6),
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    description='Bronze Layer: Read from MinIO, apply schema enforcement, load to PostgreSQL'
) as dag:

    def discover_landing_files(**context):
        """Discover all Parquet files in MinIO landing layer"""
        execution_date = context['ds']
        
        # Get department filter from DAG run conf (if triggered by department DAG)
        department = context.get('dag_run').conf.get('department') if context.get('dag_run') and context.get('dag_run').conf else None
        
        if department:
            print(f"\n🔍 Discovering files in MinIO for {execution_date} (Department: {department})...")
        else:
            print(f"\n🔍 Discovering files in MinIO for {execution_date} (All departments)...")
        
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        
        # List all objects in landing layer for this date (filtered by department if specified)
        prefix = f"landing/{department}/" if department else "landing/"
        objects = client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)
        
        files_to_process = []
        seen_tables = set()  # Track unique tables to avoid duplicates
        
        for obj in objects:
            # Look for parquet files with the execution date in path
            if execution_date in obj.object_name and '.parquet' in obj.object_name:
                # Extract unique table path (department/table/date)
                parts = obj.object_name.split('/')
                if len(parts) >= 4:  # landing/dept/table/date/...
                    table_key = '/'.join(parts[:4])  # landing/dept/table/date
                    if table_key not in seen_tables:
                        seen_tables.add(table_key)
                        files_to_process.append(table_key)
        
        print(f"   📊 Found {len(files_to_process)} tables to process")
        
        for file_path in files_to_process:
            print(f"      - {file_path}")
        
        # Store in XCom for next tasks
        context['task_instance'].xcom_push(key='files_to_process', value=files_to_process)
        
        return len(files_to_process)

    def process_to_bronze(**context):
        """
        Read Parquet files from MinIO, apply transformations,
        and load to PostgreSQL Bronze layer using Pandas
        """
        execution_date = context['ds']
        print(f"\n🔄 Processing files to Bronze for {execution_date}...")
        
        # Get files from XCom
        files_to_process = context['task_instance'].xcom_pull(
            task_ids='discover_landing_files',
            key='files_to_process'
        )
        
        if not files_to_process:
            print("   ⚠️  No files to process")
            return {'success': 0, 'failed': 0}
        
        # Connect to MinIO
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        success_count = 0
        failed_count = 0
        
        for file_path in files_to_process:
            try:
                # Extract table info from path
                # Path format: landing/<department>/<table_name>/<date>/
                parts = file_path.split('/')
                department = parts[1]
                table_name = parts[2]
                
                print(f"\n   📂 Processing: {department}/{table_name}")
                
                # List all parquet files in this directory
                objects = minio_client.list_objects(
                    BUCKET_NAME, 
                    prefix=f"{file_path}/",
                    recursive=True
                )
                
                # Read all parquet files and combine
                dfs = []
                for obj in objects:
                    if obj.object_name.endswith('.parquet'):
                        print(f"      📖 Reading: {obj.object_name}")
                        response = minio_client.get_object(BUCKET_NAME, obj.object_name)
                        parquet_data = BytesIO(response.read())
                        df = pd.read_parquet(parquet_data)
                        dfs.append(df)
                        response.close()
                        response.release_conn()
                
                if not dfs:
                    print(f"      ⚠️  No parquet files found")
                    failed_count += 1
                    continue
                
                # Combine all dataframes
                df = pd.concat(dfs, ignore_index=True)
                initial_count = len(df)
                print(f"      📊 Initial records: {initial_count:,}")
                
                # ===== SCHEMA ENFORCEMENT =====
                # Check if we have a schema definition for this table
                table_key = f"{department}_{table_name}"
                
                if table_key in BRONZE_SCHEMAS:
                    expected_cols = BRONZE_SCHEMAS[table_key]
                    print(f"      🔧 Applying schema enforcement for {table_key}")
                    
                    # Check if current columns look like data (numeric or UUID-like)
                    current_cols = df.columns.tolist()
                    metadata_cols = ['ingestion_timestamp', 'ingestion_date', 'source_system']
                    data_cols_only = [c for c in current_cols if c not in metadata_cols]
                    
                    # If columns look malformed (contain numbers, UUIDs, etc.), apply schema
                    if len(data_cols_only) > 0 and (
                        any(str(col).replace('_', '').replace('.', '').replace(':', '').isdigit() for col in data_cols_only[:3]) or
                        'unnamed' in str(data_cols_only).lower()
                    ):
                        print(f"      ⚠️  Detected malformed columns: {data_cols_only[:5]}")
                        print(f"      ✅ Applying proper schema: {expected_cols[:5]}...")
                        
                        # Separate metadata columns
                        metadata_df = df[metadata_cols] if all(c in df.columns for c in metadata_cols) else pd.DataFrame()
                        
                        # Get only data columns (exclude metadata)
                        data_df = df[data_cols_only]
                        
                        # Rename data columns to proper schema
                        if len(data_df.columns) == len(expected_cols):
                            data_df.columns = expected_cols
                            print(f"      ✅ Renamed {len(expected_cols)} columns to proper schema")
                        else:
                            print(f"      ⚠️  Column count mismatch: got {len(data_df.columns)}, expected {len(expected_cols)}")
                            # Take only the columns we have schema for
                            cols_to_use = min(len(data_df.columns), len(expected_cols))
                            data_df = data_df.iloc[:, :cols_to_use]
                            data_df.columns = expected_cols[:cols_to_use]
                        
                        # Recombine with metadata
                        if not metadata_df.empty:
                            df = pd.concat([data_df, metadata_df], axis=1)
                        else:
                            df = data_df
                    else:
                        print(f"      ✅ Columns already properly formatted")
                else:
                    print(f"      ℹ️  No schema definition for {table_key}, using existing columns")
                
                # Clean column names (PostgreSQL compatible)
                df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('.', '_').replace(':', '_') 
                             for col in df.columns]
                
                # Remove rows where all data columns are null
                data_cols = [c for c in df.columns if c not in ['ingestion_timestamp', 'ingestion_date', 'source_system', 'load_timestamp', 'bronze_ingestion_date']]
                if data_cols:
                    df = df.dropna(how='all', subset=data_cols)
                
                # Deduplicate
                df = df.drop_duplicates()
                dedup_count = initial_count - len(df)
                
                if dedup_count > 0:
                    print(f"      🧹 Removed {dedup_count:,} duplicates")
                
                # Add Bronze metadata
                df['load_timestamp'] = pd.Timestamp.now()
                df['bronze_ingestion_date'] = pd.to_datetime(execution_date).date()
                
                final_count = len(df)
                print(f"      📊 Final records: {final_count:,}")
                print(f"      ✅ Data quality: {(final_count/initial_count)*100:.2f}% retained")
                
                # Create Bronze table name
                bronze_table = f"bronze_{department}_{table_name}"
                print(f"      💾 Writing to: {bronze_table}")
                
                # Drop table if exists and create new one
                cursor.execute(f"DROP TABLE IF EXISTS {bronze_table}")
                
                # Write to PostgreSQL using pandas
                from sqlalchemy import create_engine
                engine = create_engine(f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
                df.to_sql(bronze_table, engine, if_exists='replace', index=False, method='multi', chunksize=1000)
                
                print(f"      ✅ Successfully loaded {final_count:,} records")
                success_count += 1
                
            except Exception as e:
                print(f"      ❌ FAILED: {e}")
                import traceback
                traceback.print_exc()
                failed_count += 1
                # Continue processing other files
        
        cursor.close()
        conn.close()
        
        print(f"\n   📊 Bronze Processing Summary:")
        print(f"      ✅ Success: {success_count}")
        print(f"      ❌ Failed: {failed_count}")
        
        return {'success': success_count, 'failed': failed_count}

    def validate_bronze_layer(**context):
        """Validate Bronze layer data quality"""
        execution_date = context['ds']
        print(f"\n🔍 Validating Bronze layer for {execution_date}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        try:
            # Get list of all bronze tables
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'bronze_%'
            """
            
            cursor.execute(query)
            bronze_tables = [row[0] for row in cursor.fetchall()]
            
            print(f"   📊 Found {len(bronze_tables)} Bronze tables")
            
            results = {}
            total_records = 0
            
            for table_name in bronze_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    results[table_name] = count
                    total_records += count
                    
                    print(f"      ✅ {table_name}: {count:,} records")
                    
                except Exception as e:
                    print(f"      ❌ {table_name}: Failed to read - {e}")
            
            print(f"\n   📊 BRONZE LAYER SUMMARY:")
            print(f"      Total tables: {len(results)}")
            print(f"      Total records: {total_records:,}")
            
            if total_records == 0:
                raise ValueError("Bronze layer has no data!")
            
            print(f"\n   ✅ Bronze layer validation passed!")
            
            return results
            
        except Exception as e:
            print(f"   ❌ Validation failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ==================== TASK DEFINITIONS ====================
    
    discover_task = PythonOperator(
        task_id='discover_landing_files',
        python_callable=discover_landing_files,
        provide_context=True
    )
    
    process_task = PythonOperator(
        task_id='process_to_bronze',
        python_callable=process_to_bronze,
        provide_context=True
    )
    
    validate_task = PythonOperator(
        task_id='validate_bronze_layer',
        python_callable=validate_bronze_layer,
        provide_context=True
    )
    
    # ==================== TASK DEPENDENCIES ====================
    
    discover_task >> process_task >> validate_task
