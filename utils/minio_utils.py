"""
MinIO utility functions for data pipeline
"""

from minio import Minio
from io import BytesIO
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def get_minio_client(config):
    """Create and return MinIO client"""
    return Minio(
        config['endpoint'],
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=config['secure']
    )


def ensure_bucket_exists(client, bucket_name):
    """Ensure MinIO bucket exists, create if not"""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket exists: {bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Error ensuring bucket exists: {e}")
        raise


def upload_dataframe_to_minio(client, bucket_name, df, object_path):
    """
    Upload pandas DataFrame to MinIO as Parquet
    
    Args:
        client: MinIO client
        bucket_name: Target bucket
        df: pandas DataFrame
        object_path: S3 path (e.g., 'landing/sales/customers/2025-01-01/data.parquet')
    """
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        client.put_object(
            bucket_name,
            object_path,
            buffer,
            buffer.getbuffer().nbytes,
            content_type='application/parquet'
        )
        
        logger.info(f"Uploaded {len(df)} records to {object_path}")
        return True
    except Exception as e:
        logger.error(f"Error uploading to MinIO: {e}")
        raise


def download_parquet_from_minio(client, bucket_name, object_path):
    """
    Download Parquet file from MinIO and return as DataFrame
    
    Args:
        client: MinIO client
        bucket_name: Source bucket
        object_path: S3 path to parquet file
    
    Returns:
        pandas DataFrame
    """
    try:
        response = client.get_object(bucket_name, object_path)
        parquet_data = BytesIO(response.read())
        df = pd.read_parquet(parquet_data)
        response.close()
        response.release_conn()
        
        logger.info(f"Downloaded {len(df)} records from {object_path}")
        return df
    except Exception as e:
        logger.error(f"Error downloading from MinIO: {e}")
        raise


def list_objects_by_prefix(client, bucket_name, prefix):
    """
    List all objects in MinIO with given prefix
    
    Args:
        client: MinIO client
        bucket_name: Bucket name
        prefix: Object prefix (e.g., 'landing/sales/')
    
    Returns:
        List of object names
    """
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        object_list = [obj.object_name for obj in objects]
        logger.info(f"Found {len(object_list)} objects with prefix {prefix}")
        return object_list
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        raise


def get_landing_path(department, table_name, date=None):
    """
    Generate standardized landing path
    
    Args:
        department: Department name (sales, hr, etc.)
        table_name: Table name
        date: Date string (YYYY-MM-DD), defaults to today
    
    Returns:
        S3 path string
    """
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    return f"landing/{department}/{table_name}/{date}/data.parquet"
