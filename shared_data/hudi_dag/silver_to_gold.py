from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, avg, sum as spark_sum, 
    max as spark_max, min as spark_min, 
    current_timestamp, count, stddev, variance,
    percentile_approx, collect_list, collect_set,
    when, lit, concat, round as spark_round, coalesce
)

def create_spark_session():
    return SparkSession.builder \
        .appName("SilverToGoldHudi_Spark35") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

def write_gold_hudi_table(df, table_name, base_path, record_key, partition_path=None):
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'calculation_timestamp',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',

        'hoodie.metadata.enable': 'true',
        'hoodie.metadata.index.column.stats.enable': 'true',
        'hoodie.metadata.index.bloom.filter.enable': 'true',

        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',

        'hoodie.index.type': 'GLOBAL_BLOOM',
        'hoodie.bloom.index.num.entries': '100000',
        'hoodie.bloom.index.fpp': '0.000001',
        'hoodie.global.bloom.index.update.partition.path': 'true',

        'hoodie.parquet.compression.codec': 'snappy',
        'hoodie.parquet.max.file.size': '268435456',
        'hoodie.parquet.dictionary.enabled': 'true',
        'hoodie.parquet.writelegacyformat.enabled': 'false',

        'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
        'hoodie.cleaner.fileversions.retained': '5',
        'hoodie.keep.min.commits': '6',
        'hoodie.keep.max.commits': '10',

        'hoodie.clustering.inline': 'true',
        'hoodie.clustering.inline.max.commits': '5',
        'hoodie.clustering.plan.strategy.max.num.groups': '30',
    }

    if partition_path:
        hudi_options['hoodie.datasource.write.partitionpath.field'] = partition_path
        hudi_options['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.SimpleKeyGenerator'

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(base_path)

def read_hudi_with_optimization(spark, base_path, columns=None):
    reader = spark.read.format("hudi")
    
    reader = reader \
        .option("hoodie.datasource.read.incr.path.glob", "true") \
        .option("hoodie.file.index.enable", "true")
    
    df = reader.load(base_path)
    
    if columns:
        df = df.select(*columns)
    
    df.cache()
    
    return df

def create_customer_analytics(df_customers):
    
    df_customers_clean = df_customers.withColumn(
        "club_member_status", 
        coalesce(col("club_member_status"), lit("UNKNOWN"))
    )
    customer_stats = df_customers_clean.groupBy("club_member_status") \
        .agg(
            count("customer_id").alias("total_customers"),
            avg("age").alias("avg_age"),
            spark_min("age").alias("min_age"),
            spark_max("age").alias("max_age"),
            stddev("age").alias("stddev_age"),
            variance("age").alias("variance_age"),
            percentile_approx("age", 0.25).alias("q1_age"),
            percentile_approx("age", 0.5).alias("median_age"),
            percentile_approx("age", 0.75).alias("q3_age")
        ) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .withColumn("metric_version", lit("v2.0")) \
        .withColumn("iqr_age", col("q3_age") - col("q1_age"))
    
    age_category_stats = df_customers.groupBy("club_member_status", "age_category") \
        .agg(
            count("customer_id").alias("category_count")
        ) \
        .groupBy("club_member_status") \
        .agg(
            collect_list("age_category").alias("age_categories"),
            spark_sum("category_count").alias("total_in_categories")
        )
    
    final_stats = customer_stats.join(
        age_category_stats, 
        on="club_member_status", 
        how="left"
    )
    
    return final_stats

def create_article_analytics(df_articles): 
    if df_articles.rdd.isEmpty():
        print("No article data for analytics")
        return df_articles
    
    df_articles_clean = df_articles.withColumn(
        "product_category", 
        coalesce(col("product_category"), lit("UNCATEGORIZED"))
    )   
    article_stats = df_articles_clean.groupBy("product_category") \
        .agg(
            count("article_id").alias("total_articles"),
            countDistinct("colour_group_name").alias("unique_colours"),
            countDistinct("perceived_colour_master_name").alias("unique_perceived_colours"),
            avg("data_completeness_score").alias("avg_completeness_score"),
            spark_min("data_completeness_score").alias("min_completeness_score"),
            spark_max("data_completeness_score").alias("max_completeness_score"),
            collect_set("garment_group_name").alias("garment_groups"),
            countDistinct("index_name").alias("unique_indexes")
        ) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .withColumn("diversity_score", 
                   spark_round(
                       (col("unique_colours") + col("unique_perceived_colours") + col("unique_indexes")) / 3, 
                       2
                   )) \
        .withColumn("complexity_index", 
                   spark_round(
                       col("unique_colours") * col("total_articles") / 100, 
                       2
                   )) \
        .orderBy(col("total_articles").desc())
    
    return article_stats

def create_cross_analytics(df_customers, df_articles):
    
    customer_summary = df_customers \
        .agg(
            count("customer_id").alias("total_customers"),
            countDistinct("club_member_status").alias("unique_member_statuses"),
            avg("age").alias("overall_avg_age")
        ) \
        .withColumn("customer_metric_type", lit("customers"))
    
    article_summary = df_articles \
        .agg(
            count("article_id").alias("total_articles"),
            countDistinct("product_category").alias("unique_categories"),
            avg("data_completeness_score").alias("overall_avg_completeness")
        ) \
        .withColumn("article_metric_type", lit("articles"))
    
    cross_metrics = customer_summary.crossJoin(article_summary) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .withColumn("potential_combinations", 
                   col("total_customers") * col("total_articles")) \
        .withColumn("data_quality_index",
                   spark_round(
                       (col("overall_avg_completeness") * 100), 
                       2
                   ))
    
    return cross_metrics

def main():
    spark = create_spark_session()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    silver_customers = "s3a://silver-bucket/hudi_customers"
    silver_articles = "s3a://silver-bucket/hudi_articles"
    gold_customer_stats = "s3a://gold-bucket/hudi_customer_stats"
    gold_article_stats = "s3a://gold-bucket/hudi_article_stats"
    gold_cross_metrics = "s3a://gold-bucket/hudi_cross_metrics"
    
    try:
        df_customers = read_hudi_with_optimization(spark, silver_customers)
        df_articles = read_hudi_with_optimization(spark, silver_articles)
        
        print(f"Loaded {df_customers.count()} customers and {df_articles.count()} articles")
        
        customer_analytics = create_customer_analytics(df_customers)
        print("Customer analytics preview:")
        customer_analytics.show(5, truncate=False)
        
        article_analytics = create_article_analytics(df_articles)
        print("Article analytics preview (top 10):")
        article_analytics.show(10, truncate=False)
        
        cross_analytics = create_cross_analytics(df_customers, df_articles)
        print("Cross analytics preview:")
        cross_analytics.show(truncate=False)
                
        write_gold_hudi_table(
            customer_analytics, 
            "customer_stats_gold", 
            gold_customer_stats, 
            "club_member_status",           
            partition_path="club_member_status" 
        )
        
        write_gold_hudi_table(
            article_analytics, 
            "article_stats_gold", 
            gold_article_stats, 
            "product_category"
        )
        
        cross_analytics_with_key = cross_analytics.withColumn(
            "metric_id", 
            concat(lit("cross_"), current_timestamp().cast("long"))
        )
        write_gold_hudi_table(
            cross_analytics_with_key, 
            "cross_metrics_gold", 
            gold_cross_metrics, 
            "metric_id"
        )
        
        gold_customer_df = spark.read.format("hudi").load(gold_customer_stats)
        print("Customer stats in Gold layer:")
        gold_customer_df.select(
            "club_member_status", 
            "total_customers", 
            "avg_age", 
            "median_age",
            "_hoodie_commit_time"
        ).show()
        
        gold_article_df = spark.read.format("hudi").load(gold_article_stats)
        print(f"\nTotal product categories in Gold: {gold_article_df.count()}")
        
        commits = gold_customer_df.select("_hoodie_commit_time").distinct().collect()
        print(f"Number of commits: {len(commits)}")
        for commit in commits[:5]: 
            print(f"  - {commit['_hoodie_commit_time']}")
        
        print("\nSilver -> Gold: Done")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise e
    finally:
        spark.catalog.clearCache()
        spark.stop()

if __name__ == "__main__":
    main()