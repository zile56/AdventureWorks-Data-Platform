"""
Configuration module for AdventureWorks data pipeline
"""

from datetime import timedelta

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 'minio:9000',
    'access_key': 'admin',
    'secret_key': 'admin123',
    'bucket_name': 'adventureworks',
    'secure': False
}

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': '1004'
}

# API Configuration
API_CONFIG = {
    'base_url': 'https://demodata.grapecity.com/adventureworks/api/v1',
    'timeout': 60
}

# File Paths
FILE_PATHS = {
    'csv_exports': '/opt/airflow/csv_exports',
    'kaggle_files': '/opt/airflow/dags/data_kaggle/files/kaggle',
    'local_temp': '/tmp'
}

# Department Data Sources
DEPARTMENT_SOURCES = {
    'sales': {
        'api_endpoints': ['Customers', 'Products', 'SalesOrderHeaders', 'SalesOrderDetails', 'SalesTerritories'],
        'csv_files': ['Customer.csv', 'Product.csv', 'SalesOrderHeader.csv', 'SalesOrderDetail.csv', 'SalesTerritory.csv'],
        'tables': ['customer', 'product', 'sales_order_header', 'sales_order_detail', 'sales_territory']
    },
    'hr': {
        'api_endpoints': ['Persons', 'Employees'],
        'csv_files': ['Person.csv', 'Employee.csv'],
        'tables': ['person', 'employee']
    },
    'finance': {
        'api_endpoints': [],
        'csv_files': ['SalesOrderHeader.csv', 'SalesOrderDetail.csv'],
        'tables': ['sales_order_header', 'sales_order_detail']
    },
    'production': {
        'api_endpoints': ['Products', 'ProductCategories', 'ProductSubcategories'],
        'csv_files': ['Product.csv', 'ProductCategory.csv', 'ProductSubcategory.csv'],
        'tables': ['product', 'product_category', 'product_subcategory']
    },
    'customer': {
        'api_endpoints': ['Customers', 'Persons', 'Addresses'],
        'csv_files': ['Customer.csv', 'Person.csv', 'Address.csv'],
        'tables': ['customer', 'person', 'address']
    }
}

# Airflow Default Args
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Layer DAG IDs
LAYER_DAGS = {
    'landing': 'landing_layer_complete',
    'bronze': 'bronze_layer_complete',
    'silver': 'silver_layer_complete',
    'gold': 'gold_layer_production'
}
