from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

HUDI_PACKAGES = (
    "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.696"
)

SPARK_CONF = {
    "spark.master": "local[*]",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",

    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",

    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    
    "spark.hadoop.fs.s3a.connection.maximum": "1000",
    "spark.hadoop.fs.s3a.attempts.maximum": "10",
    "spark.hadoop.fs.s3a.retry.limit": "10",
    "spark.hadoop.fs.s3a.retry.interval": "1s",
    "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
    
    "hoodie.metadata.enable": "false",
    "hoodie.embed.timeline.server": "false",
    "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
}

with DAG(
    "hudi_dag_example",
    default_args=default_args,
    description="ETL Bronze->Silver->Gold with Hudi tables (Spark 3.5.7 + MinIO)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["hudi", "etl", "minio", "spark-3.5"],
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver_hudi",
        application="/opt/airflow/shared_data/hudi_dag/bronze_to_silver.py",
        packages=HUDI_PACKAGES,
        conn_id="spark_default",
        verbose=True,
        conf=SPARK_CONF,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold_hudi",
        application="/opt/airflow/shared_data/hudi_dag/silver_to_gold.py",
        packages=HUDI_PACKAGES,
        conn_id="spark_default",
        verbose=True,
        conf=SPARK_CONF,
    )

    bronze_to_silver >> silver_to_gold