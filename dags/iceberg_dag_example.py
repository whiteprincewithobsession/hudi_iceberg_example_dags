from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SPARK_CONF = {
    "spark.master": "local[*]",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type": "hadoop",
    "spark.sql.catalog.spark_catalog.warehouse": "s3a://iceberg-warehouse/",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "spark_catalog",
    
    "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.iceberg.vectorization.enabled": "true",

    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin", 
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    
    "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.spark_catalog.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.spark_catalog.s3.path-style-access": "true",
}

with DAG(
    "iceberg_dag_example",
    default_args=default_args,
    description="ETL Bronze->Silver->Gold with Iceberg tables for Salary Data (Spark 3.5.7 + MinIO)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["iceberg", "etl", "minio", "spark-3.5", "salary"],
) as dag:
    
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver_iceberg",
        application="/opt/airflow/shared_data/iceberg_dag/bronze_to_silver.py",
        packages=ICEBERG_PACKAGES,
        conn_id="spark_default",
        verbose=True,
        conf=SPARK_CONF,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold_iceberg",
        application="/opt/airflow/shared_data/iceberg_dag/silver_to_gold.py",
        packages=ICEBERG_PACKAGES,
        conn_id="spark_default",
        verbose=True,
        conf=SPARK_CONF,
    )

    bronze_to_silver >> silver_to_gold