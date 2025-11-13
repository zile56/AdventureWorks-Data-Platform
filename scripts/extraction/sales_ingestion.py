"""
Sales Department Data Ingestion Script
Extracts sales data from APIs and CSV files, uploads to MinIO
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import requests
import logging
from datetime import datetime
from utils.config import MINIO_CONFIG, API_CONFIG, FILE_PATHS, DEPARTMENT_SOURCES
from utils.minio_utils import get_minio_client, ensure_bucket_exists, upload_dataframe_to_minio, get_landing_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_sales_api_data(endpoint):
    """Extract sales data from API endpoint"""
    try:
        url = f"{API_CONFIG['base_url']}/{endpoint}"
        logger.info(f"Fetching data from {url}")
        
        response = requests.get(url, timeout=API_CONFIG['timeout'])
        response.raise_for_status()
        data = response.json()
        
        if not data:
            logger.warning(f"No data returned from {endpoint}")
            return None
        
        df = pd.DataFrame(data)
        logger.info(f"Extracted {len(df)} records from {endpoint}")
        return df
    except Exception as e:
        logger.error(f"Error extracting from API {endpoint}: {e}")
        return None


def extract_sales_csv_data(csv_file):
    """Extract sales data from CSV file"""
    try:
        csv_path = os.path.join(FILE_PATHS['csv_exports'], csv_file)
        
        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found: {csv_path}")
            return None
        
        df = pd.read_csv(csv_path, header=None, on_bad_lines='skip')
        logger.info(f"Extracted {len(df)} records from {csv_file}")
        return df
    except Exception as e:
        logger.error(f"Error extracting from CSV {csv_file}: {e}")
        return None


def add_metadata(df, source_type):
    """Add metadata columns to DataFrame"""
    df['ingestion_timestamp'] = datetime.now()
    df['ingestion_date'] = datetime.now().strftime('%Y-%m-%d')
    df['source_system'] = source_type
    return df


def upload_to_minio(df, department, table_name, execution_date=None):
    """Upload DataFrame to MinIO"""
    try:
        client = get_minio_client(MINIO_CONFIG)
        ensure_bucket_exists(client, MINIO_CONFIG['bucket_name'])
        
        object_path = get_landing_path(department, table_name, execution_date)
        upload_dataframe_to_minio(client, MINIO_CONFIG['bucket_name'], df, object_path)
        
        logger.info(f"Successfully uploaded to {object_path}")
        return True
    except Exception as e:
        logger.error(f"Error uploading to MinIO: {e}")
        return False


def ingest_sales_data(execution_date=None):
    """
    Main function to ingest all sales department data
    
    Args:
        execution_date: Date string (YYYY-MM-DD), defaults to today
    
    Returns:
        Dictionary with ingestion results
    """
    logger.info("=" * 70)
    logger.info("SALES DEPARTMENT DATA INGESTION")
    logger.info("=" * 70)
    
    results = {
        'success': 0,
        'failed': 0,
        'tables': []
    }
    
    department = 'sales'
    sources = DEPARTMENT_SOURCES[department]
    
    # Ingest from API
    for endpoint in sources['api_endpoints']:
        try:
            df = extract_sales_api_data(endpoint)
            if df is not None:
                df = add_metadata(df, 'api')
                table_name = endpoint.lower()
                
                if upload_to_minio(df, department, table_name, execution_date):
                    results['success'] += 1
                    results['tables'].append(table_name)
                else:
                    results['failed'] += 1
        except Exception as e:
            logger.error(f"Error processing {endpoint}: {e}")
            results['failed'] += 1
    
    # Ingest from CSV
    for csv_file in sources['csv_files']:
        try:
            df = extract_sales_csv_data(csv_file)
            if df is not None:
                df = add_metadata(df, 'csv')
                table_name = csv_file.replace('.csv', '').lower()
                
                if upload_to_minio(df, department, table_name, execution_date):
                    results['success'] += 1
                    results['tables'].append(table_name)
                else:
                    results['failed'] += 1
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")
            results['failed'] += 1
    
    logger.info("=" * 70)
    logger.info(f"SALES INGESTION COMPLETE")
    logger.info(f"Success: {results['success']}, Failed: {results['failed']}")
    logger.info(f"Tables: {', '.join(results['tables'])}")
    logger.info("=" * 70)
    
    return results


def main():
    """Main entry point for CLI execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sales Department Data Ingestion')
    parser.add_argument('--date', type=str, help='Execution date (YYYY-MM-DD)', default=None)
    args = parser.parse_args()
    
    results = ingest_sales_data(args.date)
    
    if results['failed'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
