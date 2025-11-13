from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from minio import Minio
from io import BytesIO
import psycopg2
from sqlalchemy import create_engine
import logging

# ------------------- Default DAG args -------------------
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ------------------- Configuration -------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET_NAME = "adventureworks"

# API Configuration - GrapeCity Demo Data
API_BASE_URL = "https://demodata.grapecity.com/adventureworks/api/v1"

# PostgreSQL Configuration (for database dumps)
PG_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': '1004'
}

# Kaggle Files Location (adjust to your path)
KAGGLE_FILES_PATH = "/opt/airflow/dags/data_kaggle/files/kaggle"

# ------------------- DAG definition -------------------
with DAG(
    dag_id='landing_layer_complete',
    default_args=default_args,
    start_date=datetime(2025, 11, 6),
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    description='Landing Layer: Complete data ingestion from APIs, Files, and Database dumps to MinIO'
) as dag:

    def get_minio_client():
        """Create MinIO client"""
        return Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

    def create_bucket(**context):
        """Create MinIO bucket if not exists"""
        print(f"\n🪣 Creating/Checking MinIO bucket...")
        
        try:
            client = get_minio_client()
            
            if not client.bucket_exists(BUCKET_NAME):
                client.make_bucket(BUCKET_NAME)
                print(f"   ✅ Created bucket: {BUCKET_NAME}")
            else:
                print(f"   ✅ Bucket exists: {BUCKET_NAME}")
                
            return True
            
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            raise

    def ingest_api_data(**context):
        """Ingest data from API endpoints"""
        execution_date = context['ds']
        print(f"\n🌐 Ingesting API data for {execution_date}...")
        
        client = get_minio_client()
        date_partition = execution_date
        
        # API endpoints mapping - GrapeCity AdventureWorks API
        api_endpoints = {
            # Customer & Person Data
            'customers': {'url': f'{API_BASE_URL}/Customers', 'department': 'sales'},
            'persons': {'url': f'{API_BASE_URL}/Persons', 'department': 'hr'},
            
            # Product Data
            'products': {'url': f'{API_BASE_URL}/Products', 'department': 'sales'},
            'product_categories': {'url': f'{API_BASE_URL}/ProductCategories', 'department': 'marketing'},
            'product_subcategories': {'url': f'{API_BASE_URL}/ProductSubcategories', 'department': 'marketing'},
            
            # Sales Data
            'sales_order_headers': {'url': f'{API_BASE_URL}/SalesOrderHeaders', 'department': 'sales'},
            'sales_order_details': {'url': f'{API_BASE_URL}/SalesOrderDetails', 'department': 'sales'},
            'sales_territories': {'url': f'{API_BASE_URL}/SalesTerritories', 'department': 'sales'},
            
            # HR Data
            'employees': {'url': f'{API_BASE_URL}/Employees', 'department': 'hr'},
            
            # Operations Data
            'addresses': {'url': f'{API_BASE_URL}/Addresses', 'department': 'operations'},
        }
        
        success_count = 0
        failed_count = 0
        
        for table_name, config in api_endpoints.items():
            try:
                print(f"\n   📡 Fetching: {table_name}")
                
                # Fetch from API
                response = requests.get(config['url'], timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    print(f"      ⚠️  No data returned")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(data)
                print(f"      📊 Fetched {len(df)} records")
                
                # Add metadata
                df['ingestion_timestamp'] = datetime.now()
                df['ingestion_date'] = execution_date
                df['source_system'] = 'api'
                
                # Save to MinIO as Parquet
                # Path: landing/<department>/<table_name>/<date>/data.parquet
                object_path = f"landing/{config['department']}/{table_name}/{date_partition}/data.parquet"
                
                buffer = BytesIO()
                df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
                buffer.seek(0)
                
                client.put_object(
                    BUCKET_NAME,
                    object_path,
                    buffer,
                    buffer.getbuffer().nbytes,
                    content_type='application/parquet'
                )
                
                print(f"      ✅ Saved to MinIO: {object_path}")
                success_count += 1
                
            except Exception as e:
                print(f"      ❌ Failed: {e}")
                failed_count += 1
        
        print(f"\n   📊 API Ingestion Summary:")
        print(f"      ✅ Success: {success_count}")
        print(f"      ❌ Failed: {failed_count}")
        
        return {'success': success_count, 'failed': failed_count}

    def ingest_kaggle_files(**context):
        """Ingest Kaggle flat files (CSV/Parquet)"""
        execution_date = context['ds']
        print(f"\n📁 Ingesting Kaggle files for {execution_date}...")
        
        client = get_minio_client()
        date_partition = execution_date
        
        # Kaggle files mapping - these are FOLDER names (parquet folders)
        kaggle_files = {
            'AdventureWorks_Customer.parquet': {'department': 'sales', 'table_name': 'customer'},
            'AdventureWorks_Product_Categories.parquet': {'department': 'marketing', 'table_name': 'product_categories'},
            'AdventureWorks_Product_Subcategories-2.parquet': {'department': 'marketing', 'table_name': 'product_subcategories'},
            'AdventureWorks_Products.parquet': {'department': 'sales', 'table_name': 'products'},
            'AdventureWorks_Territories.parquet': {'department': 'sales', 'table_name': 'territories'},
            'Sales.parquet': {'department': 'sales', 'table_name': 'sales'},
        }
        
        success_count = 0
        failed_count = 0
        
        # Fixed path to Kaggle files
        import os
        source_folder = "/opt/airflow/dags/data_kaggle/files/kaggle/2025-11-06"
        
        if not os.path.exists(source_folder):
            print(f"   ⚠️  Kaggle files folder not found: {source_folder}")
            return {'success': 0, 'failed': 0, 'skipped': True}
        
        print(f"   📂 Found Kaggle files in: {source_folder}")
        
        # Get all items in directory
        try:
            items = os.listdir(source_folder)
            print(f"   📋 Found {len(items)} items")
        except Exception as e:
            print(f"   ❌ Cannot list directory: {e}")
            return {'success': 0, 'failed': len(kaggle_files), 'skipped': True}
        
        for item_name in items:
            if item_name not in kaggle_files:
                continue
            
            try:
                print(f"\n   📄 Processing: {item_name}")
                
                item_path = os.path.join(source_folder, item_name)
                config = kaggle_files[item_name]
                
                # These are parquet folders
                if os.path.isdir(item_path):
                    print(f"      📁 Reading parquet folder: {item_path}")
                    df = pd.read_parquet(item_path, engine='pyarrow')
                else:
                    print(f"      ⚠️  Expected folder but found file, skipping")
                    continue
                
                print(f"      📊 Loaded {len(df)} records")
                
                # Add metadata
                df['ingestion_timestamp'] = datetime.now()
                df['ingestion_date'] = execution_date
                df['source_system'] = 'kaggle'
                
                # Save to MinIO
                object_path = f"landing/{config['department']}/{config['table_name']}/{date_partition}/data.parquet"
                
                buffer = BytesIO()
                df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
                buffer.seek(0)
                
                client.put_object(
                    BUCKET_NAME,
                    object_path,
                    buffer,
                    buffer.getbuffer().nbytes,
                    content_type='application/parquet'
                )
                
                print(f"      ✅ Saved to MinIO: {object_path}")
                success_count += 1
                
            except Exception as e:
                print(f"      ❌ Failed: {e}")
                import traceback
                traceback.print_exc()
                failed_count += 1
        
        print(f"\n   📊 Kaggle Files Summary:")
        print(f"      ✅ Success: {success_count}")
        print(f"      ❌ Failed: {failed_count}")
        
        return {'success': success_count, 'failed': failed_count}

    def ingest_database_dumps(**context):
        """Ingest database dumps from CSV files"""
        execution_date = context['ds']
        print(f"\n🗄️  Ingesting database CSV exports for {execution_date}...")
        
        client = get_minio_client()
        date_partition = execution_date
        
        # CSV files location (mounted in Docker)
        CSV_EXPORTS_PATH = "/opt/airflow/csv_exports"
        
        # Database CSV files mapping with proper column names
        db_files = {
            'Address.csv': {
                'department': 'operations', 
                'table_name': 'address',
                'columns': ['AddressID', 'AddressLine1', 'AddressLine2', 'City', 'StateProvince', 'CountryRegion', 'PostalCode', 'rowguid', 'ModifiedDate']
            },
            'Customer.csv': {
                'department': 'sales', 
                'table_name': 'customer',
                'columns': ['CustomerID', 'PersonID', 'StoreID', 'TerritoryID', 'AccountNumber', 'rowguid', 'ModifiedDate']
            },
            'Employee.csv': {
                'department': 'hr', 
                'table_name': 'employee',
                'columns': ['EmployeeID', 'NationalIDNumber', 'LoginID', 'OrganizationLevel', 'JobTitle', 'BirthDate', 'MaritalStatus', 'Gender', 'HireDate', 'SalariedFlag', 'VacationHours', 'SickLeaveHours', 'CurrentFlag', 'rowguid', 'ModifiedDate']
            },
            'Person.csv': {
                'department': 'hr', 
                'table_name': 'person',
                'columns': ['PersonID', 'PersonType', 'NameStyle', 'Title', 'FirstName', 'MiddleName', 'LastName', 'Suffix', 'EmailPromotion', 'AdditionalContactInfo', 'Demographics', 'rowguid', 'ModifiedDate']
            },
            'Product.csv': {
                'department': 'sales', 
                'table_name': 'product',
                'columns': ['ProductID', 'Name', 'ProductNumber', 'Color', 'StandardCost', 'ListPrice', 'Size', 'Weight', 'ProductCategoryID', 'ProductModelID', 'SellStartDate', 'SellEndDate', 'DiscontinuedDate', 'ThumbNailPhoto', 'ThumbnailPhotoFileName', 'rowguid', 'ModifiedDate']
            },
            'ProductCategory.csv': {
                'department': 'marketing', 
                'table_name': 'product_category',
                'columns': ['ProductCategoryID', 'ParentProductCategoryID', 'Name', 'rowguid', 'ModifiedDate']
            },
            'ProductSubcategory.csv': {
                'department': 'marketing', 
                'table_name': 'product_subcategory',
                'columns': ['ProductSubcategoryID', 'ProductCategoryID', 'Name', 'rowguid', 'ModifiedDate']
            },
            'SalesOrderDetail.csv': {
                'department': 'sales', 
                'table_name': 'sales_order_detail',
                'columns': ['SalesOrderID', 'SalesOrderDetailID', 'OrderQty', 'ProductID', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'rowguid', 'ModifiedDate']
            },
            'SalesOrderHeader.csv': {
                'department': 'sales', 
                'table_name': 'sales_order_header',
                'columns': ['SalesOrderID', 'RevisionNumber', 'OrderDate', 'DueDate', 'ShipDate', 'Status', 'OnlineOrderFlag', 'SalesOrderNumber', 'PurchaseOrderNumber', 'AccountNumber', 'CustomerID', 'ShipToAddressID', 'BillToAddressID', 'ShipMethod', 'CreditCardApprovalCode', 'SubTotal', 'TaxAmt', 'Freight', 'TotalDue', 'Comment', 'rowguid', 'ModifiedDate']
            },
            'SalesTerritory.csv': {
                'department': 'sales', 
                'table_name': 'sales_territory',
                'columns': ['TerritoryID', 'Name', 'CountryRegionCode', 'Group', 'SalesYTD', 'SalesLastYear', 'CostYTD', 'CostLastYear', 'rowguid', 'ModifiedDate']
            },
        }
        
        success_count = 0
        failed_count = 0
        
        # Check if CSV exports folder exists
        import os
        print(f"   📂 Looking for CSV files in: {CSV_EXPORTS_PATH}")
        
        if not os.path.exists(CSV_EXPORTS_PATH):
            print(f"   ❌ CSV exports folder not found: {CSV_EXPORTS_PATH}")
            print(f"   ⚠️  Skipping database dumps...")
            return {'success': 0, 'failed': 0, 'skipped': True}
        
        # List files in directory
        try:
            files_in_dir = os.listdir(CSV_EXPORTS_PATH)
            print(f"   📋 Found {len(files_in_dir)} files in directory")
            print(f"   📄 Files: {files_in_dir}")
        except Exception as e:
            print(f"   ❌ Cannot list directory: {e}")
            return {'success': 0, 'failed': 0, 'skipped': True}
        
        for csv_file, config in db_files.items():
            try:
                print(f"\n   📄 Reading: {csv_file}")
                
                csv_path = os.path.join(CSV_EXPORTS_PATH, csv_file)
                
                # Check if file exists
                if not os.path.exists(csv_path):
                    print(f"      ⚠️  File not found, skipping")
                    continue
                
                # Read CSV file WITHOUT headers (first row is data)
                df = pd.read_csv(csv_path, header=None, names=config['columns'])
                
                if df.empty:
                    print(f"      ⚠️  File is empty, skipping")
                    continue
                
                print(f"      📊 Loaded {len(df)} records with proper column names: {list(df.columns[:5])}...")
                
                # Add metadata
                df['ingestion_timestamp'] = datetime.now()
                df['ingestion_date'] = execution_date
                df['source_system'] = 'database'
                
                # Save to MinIO
                object_path = f"landing/{config['department']}/{config['table_name']}/{date_partition}/data.parquet"
                
                buffer = BytesIO()
                df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
                buffer.seek(0)
                
                client.put_object(
                    BUCKET_NAME,
                    object_path,
                    buffer,
                    buffer.getbuffer().nbytes,
                    content_type='application/parquet'
                )
                
                print(f"      ✅ Saved to MinIO: {object_path}")
                success_count += 1
                
            except Exception as e:
                print(f"      ❌ Failed: {e}")
                failed_count += 1
        
        print(f"\n   📊 Database CSV Exports Summary:")
        print(f"      ✅ Success: {success_count}")
        print(f"      ❌ Failed: {failed_count}")
        
        return {'success': success_count, 'failed': failed_count}

    def validate_landing_layer(**context):
        """Validate all data was ingested to MinIO"""
        execution_date = context['ds']
        print(f"\n🔍 Validating landing layer for {execution_date}...")
        
        client = get_minio_client()
        date_partition = execution_date
        
        # List all objects in landing layer for this date
        objects = client.list_objects(BUCKET_NAME, prefix=f"landing/", recursive=True)
        
        today_objects = [obj for obj in objects if date_partition in obj.object_name]
        
        print(f"\n   📊 Landing Layer Summary:")
        print(f"      Total files for {date_partition}: {len(today_objects)}")
        
        # Group by department
        departments = {}
        for obj in today_objects:
            parts = obj.object_name.split('/')
            if len(parts) >= 2:
                dept = parts[1]
                departments[dept] = departments.get(dept, 0) + 1
        
        for dept, count in departments.items():
            print(f"      📁 {dept}: {count} files")
        
        if len(today_objects) == 0:
            raise ValueError("No files ingested to landing layer!")
        
        print(f"\n   ✅ Landing layer validation passed!")
        
        return {'total_files': len(today_objects), 'departments': departments}

    # ==================== TASK DEFINITIONS ====================
    
    create_bucket_task = PythonOperator(
        task_id='create_bucket',
        python_callable=create_bucket,
        provide_context=True
    )
    
    ingest_api_task = PythonOperator(
        task_id='ingest_api_data',
        python_callable=ingest_api_data,
        provide_context=True
    )
    
    ingest_kaggle_task = PythonOperator(
        task_id='ingest_kaggle_files',
        python_callable=ingest_kaggle_files,
        provide_context=True
    )
    
    ingest_database_task = PythonOperator(
        task_id='ingest_database_dumps',
        python_callable=ingest_database_dumps,
        provide_context=True
    )
    
    validate_task = PythonOperator(
        task_id='validate_landing_layer',
        python_callable=validate_landing_layer,
        provide_context=True
    )
    
    # ==================== TASK DEPENDENCIES ====================
    
    # Create bucket first, then ingest all sources in parallel, then validate
    create_bucket_task >> [ingest_api_task, ingest_kaggle_task, ingest_database_task] >> validate_task
