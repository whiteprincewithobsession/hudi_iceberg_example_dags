from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, year, month, 
    when, lit, concat, sha2, row_number
)
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilverHudi_Spark35") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def write_hudi_table(df, table_name, base_path, partition_field=None, table_type="COPY_ON_WRITE"):
    
    record_key = 'customer_id' if 'customer' in table_name else 'article_id'
    
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.partitionpath.field': partition_field if partition_field else '',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'update_timestamp',
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.hive_style_partitioning': 'true' if partition_field else 'false',
        
        'hoodie.datasource.write.row.writer.enable': 'true',
        'hoodie.write.concurrency.mode': 'single_writer',
        'hoodie.write.lock.provider': 'org.apache.hudi.client.transaction.lock.InProcessLockProvider',
        
        'hoodie.upsert.shuffle.parallelism': '4',
        'hoodie.insert.shuffle.parallelism': '4',
        'hoodie.bulkinsert.shuffle.parallelism': '4',
        'hoodie.delete.shuffle.parallelism': '4',
        
        'hoodie.metadata.enable': 'true',
        'hoodie.metadata.index.column.stats.enable': 'true',
        'hoodie.metadata.index.bloom.filter.enable': 'true',
        'hoodie.index.type': 'BLOOM',
        
        'hoodie.clean.automatic': 'true',
        'hoodie.clean.commits.retained': '10',
        'hoodie.keep.min.commits': '11',
        'hoodie.keep.max.commits': '20',
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
        
        'hoodie.compact.inline': 'true' if table_type == "MERGE_ON_READ" else 'false',
        'hoodie.compact.inline.max.delta.commits': '5',
        
        'hoodie.parquet.compression.codec': 'snappy',
        'hoodie.parquet.max.file.size': '134217728',
        'hoodie.parquet.small.file.limit': '104857600',
        
        'hoodie.clustering.inline': 'true',
        'hoodie.clustering.inline.max.commits': '10',
        'hoodie.clustering.plan.strategy.target.file.max.bytes': '1073741824',
        'hoodie.clustering.plan.strategy.small.file.limit': '629145600'
    }
    
    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(base_path)

def process_customers(spark, bronze_path):
    df = spark.read.option("header", "true").csv(bronze_path)
    
    window_spec = Window.partitionBy("customer_id").orderBy(col("customer_id"))
    
    df_processed = df \
        .withColumn("age", col("age").cast("int")) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("etl_date", to_date(current_timestamp())) \
        .withColumn("year", year("etl_date")) \
        .withColumn("month", month("etl_date")) \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num") \
        .withColumn("customer_hash", sha2(concat(col("customer_id"), col("age")), 256)) \
        .withColumn("age_category", 
                   when(col("age") < 18, "junior")
                   .when(col("age") < 35, "young_adult")
                   .when(col("age") < 50, "middle_age")
                   .when(col("age") < 65, "senior")
                   .otherwise("elder")) \
        .filter(col("customer_id").isNotNull())
    
    return df_processed

def process_articles(spark, bronze_path):
    df = spark.read.option("header", "true").csv(bronze_path)
    
    df_processed = df \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("etl_date", to_date(current_timestamp())) \
        .withColumn("product_category", 
                   when(col("product_type_name").isNotNull(), col("product_type_name"))
                   .otherwise("UNKNOWN")) \
        .withColumn("data_completeness_score", 
                   (col("article_id").isNotNull().cast("int") + 
                    col("product_type_name").isNotNull().cast("int") + 
                    col("product_type_no").isNotNull().cast("int") +
                    col("product_group_name").isNotNull().cast("int") +
                    col("graphical_appearance_no").isNotNull().cast("int") +
                    col("colour_group_code").isNotNull().cast("int") +
                    col("colour_group_name").isNotNull().cast("int") +
                    col("perceived_colour_value_id").isNotNull().cast("int") +
                    col("perceived_colour_value_name").isNotNull().cast("int") +
                    col("perceived_colour_master_id").isNotNull().cast("int") +
                    col("perceived_colour_master_name").isNotNull().cast("int") +
                    col("department_no").isNotNull().cast("int") +
                    col("department_name").isNotNull().cast("int") +
                    col("index_code").isNotNull().cast("int") +
                    col("index_name").isNotNull().cast("int") +
                    col("index_group_no").isNotNull().cast("int") +
                    col("index_group_name").isNotNull().cast("int") +
                    col("section_no").isNotNull().cast("int") +
                    col("section_name").isNotNull().cast("int") +
                    col("garment_group_no").isNotNull().cast("int") +
                    col("garment_group_name").isNotNull().cast("int") +
                    col("detail_desc").isNotNull().cast("int")) / 22.0) \
        .filter(col("article_id").isNotNull()) \
        .dropDuplicates(["article_id"])
    
    return df_processed

def main():
    spark = create_spark_session()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    bronze_customers = "s3a://bronze-layer/customers/customers.csv"
    bronze_articles = "s3a://bronze-layer/articles/articles.csv"
    silver_customers = "s3a://silver-bucket/hudi_customers"
    silver_articles = "s3a://silver-bucket/hudi_articles"
    
    try:
        df_customers = process_customers(spark, bronze_customers)
        
        write_hudi_table(
            df_customers, 
            "customers_hudi", 
            silver_customers, 
            "year,month",
            "COPY_ON_WRITE"
        )
        
        df_articles = process_articles(spark, bronze_articles)
        
        write_hudi_table(
            df_articles, 
            "articles_hudi", 
            silver_articles, 
            "product_category",
            "MERGE_ON_READ"
        )
        
        customers_hudi = spark.read.format("hudi").load(silver_customers)
        customers_hudi.printSchema()
        customers_hudi.select(
            "customer_id", "age", "age_category", 
            "club_member_status", "_hoodie_commit_time"
        ).show(5, truncate=False)
        
        articles_hudi = spark.read.format("hudi").load(silver_articles)
        articles_hudi.select(
            "article_id", "product_category", 
            "data_completeness_score", "_hoodie_commit_time"
        ).show(5, truncate=False)
        
        customers_hudi.select("year", "month").distinct().orderBy("year", "month").show()
        
        print("Articles partitions (top 10):")
        articles_hudi.select("product_category").distinct().show(10)
        
        print("\nBronze -> Silver: Done")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()