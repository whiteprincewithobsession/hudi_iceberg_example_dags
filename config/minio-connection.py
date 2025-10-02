from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def create_minio_connection():
    hook = S3Hook(aws_conn_id='minio_logs')
    return hook