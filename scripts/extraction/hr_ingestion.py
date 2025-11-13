"""
HR Department Data Ingestion Script
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


def extract_api_data(endpoint):
    """Extract data from API"""
    try:
        url = f"{API_CONFIG['base_url']}/{endpoint}"
        response = requests.get(url, timeout=API_CONFIG['timeout'])
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) if data else None
    except Exception as e:
        logger.error(f"API error {endpoint}: {e}")
        return None


def extract_csv_data(csv_file):
    """Extract data from CSV"""
    try:
        csv_path = os.path.join(FILE_PATHS['csv_exports'], csv_file)
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path, header=None, on_bad_lines='skip')
        return None
    except Exception as e:
        logger.error(f"CSV error {csv_file}: {e}")
        return None


def ingest_hr_data(execution_date=None):
    """Ingest HR department data"""
    logger.info("HR DEPARTMENT DATA INGESTION")
    
    results = {'success': 0, 'failed': 0, 'tables': []}
    department = 'hr'
    sources = DEPARTMENT_SOURCES[department]
    
    client = get_minio_client(MINIO_CONFIG)
    ensure_bucket_exists(client, MINIO_CONFIG['bucket_name'])
    
    # API ingestion
    for endpoint in sources['api_endpoints']:
        df = extract_api_data(endpoint)
        if df is not None:
            df['ingestion_timestamp'] = datetime.now()
            df['source_system'] = 'api'
            table_name = endpoint.lower()
            object_path = get_landing_path(department, table_name, execution_date)
            
            try:
                upload_dataframe_to_minio(client, MINIO_CONFIG['bucket_name'], df, object_path)
                results['success'] += 1
                results['tables'].append(table_name)
            except:
                results['failed'] += 1
    
    # CSV ingestion
    for csv_file in sources['csv_files']:
        df = extract_csv_data(csv_file)
        if df is not None:
            df['ingestion_timestamp'] = datetime.now()
            df['source_system'] = 'csv'
            table_name = csv_file.replace('.csv', '').lower()
            object_path = get_landing_path(department, table_name, execution_date)
            
            try:
                upload_dataframe_to_minio(client, MINIO_CONFIG['bucket_name'], df, object_path)
                results['success'] += 1
                results['tables'].append(table_name)
            except:
                results['failed'] += 1
    
    logger.info(f"HR COMPLETE - Success: {results['success']}, Failed: {results['failed']}")
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description='HR Data Ingestion')
    parser.add_argument('--date', type=str, default=None)
    args = parser.parse_args()
    
    results = ingest_hr_data(args.date)
    sys.exit(0 if results['failed'] == 0 else 1)


if __name__ == '__main__':
    main()
