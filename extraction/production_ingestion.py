"""Production Department Data Ingestion Script"""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import requests
import logging
from datetime import datetime
from utils.config import MINIO_CONFIG, API_CONFIG, FILE_PATHS, DEPARTMENT_SOURCES
from utils.minio_utils import get_minio_client, ensure_bucket_exists, upload_dataframe_to_minio, get_landing_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_production_data(execution_date=None):
    logger.info("PRODUCTION DEPARTMENT DATA INGESTION")
    results = {'success': 0, 'failed': 0, 'tables': []}
    client = get_minio_client(MINIO_CONFIG)
    ensure_bucket_exists(client, MINIO_CONFIG['bucket_name'])
    
    # API data
    for endpoint in DEPARTMENT_SOURCES['production']['api_endpoints']:
        try:
            url = f"{API_CONFIG['base_url']}/{endpoint}"
            response = requests.get(url, timeout=API_CONFIG['timeout'])
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['ingestion_timestamp'] = datetime.now()
                df['source_system'] = 'api'
                table_name = endpoint.lower()
                object_path = get_landing_path('production', table_name, execution_date)
                upload_dataframe_to_minio(client, MINIO_CONFIG['bucket_name'], df, object_path)
                results['success'] += 1
                results['tables'].append(table_name)
        except Exception as e:
            logger.error(f"API error: {e}")
            results['failed'] += 1
    
    # CSV data
    for csv_file in DEPARTMENT_SOURCES['production']['csv_files']:
        try:
            csv_path = os.path.join(FILE_PATHS['csv_exports'], csv_file)
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, header=None, on_bad_lines='skip')
                df['ingestion_timestamp'] = datetime.now()
                df['source_system'] = 'csv'
                table_name = csv_file.replace('.csv', '').lower()
                object_path = get_landing_path('production', table_name, execution_date)
                upload_dataframe_to_minio(client, MINIO_CONFIG['bucket_name'], df, object_path)
                results['success'] += 1
                results['tables'].append(table_name)
        except Exception as e:
            logger.error(f"CSV error: {e}")
            results['failed'] += 1
    
    logger.info(f"PRODUCTION COMPLETE - Success: {results['success']}")
    return results

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default=None)
    results = ingest_production_data(parser.parse_args().date)
    sys.exit(0 if results['failed'] == 0 else 1)

if __name__ == '__main__':
    main()
