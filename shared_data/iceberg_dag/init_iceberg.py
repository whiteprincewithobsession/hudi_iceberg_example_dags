from pyspark.sql import SparkSession
import time

def init_iceberg_catalog():
    spark = None
    try:        
        spark = SparkSession.builder \
            .appName("InitIcebergCatalog") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
            .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.warehouse.dir", "s3a://iceberg-warehouse/") \
            .getOrCreate()
        
        print("Spark session created successfully")
        
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "minioadmin")
        hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        print("S3A configuration set for MinIO")
        time.sleep(3)
        
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.default")
            print("Default namespace verified")
        except Exception as e:
            print(f"Default namespace: {e}")
        
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.silver")
            print("Silver namespace created/verified")
        except Exception as e:
            print(f"Failed to create silver namespace: {e}")
            raise
        
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.gold")
            print("Gold namespace created/verified")
        except Exception as e:
            print(f"Failed to create gold namespace: {e}")
            raise
        
        try:
            namespaces = spark.sql("SHOW NAMESPACES IN spark_catalog").collect()
            print("Available namespaces in spark_catalog:")
            for ns in namespaces:
                print(f"   - {ns['namespace']}")
        except Exception as e:
            print(f"Cannot list namespaces: {e}")
        
        print("Iceberg catalog initialization completed successfully!")
        
    except Exception as e:
        print(f"Error during Iceberg initialization: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    init_iceberg_catalog()