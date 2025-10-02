from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg,  
    current_timestamp, countDistinct,
    stddev, percentile_approx, lit, 
    round as spark_round, collect_list,
    dense_rank, median,
    min as spark_min, max as spark_max
)
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("SilverToGoldIceberg_Salary") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/") \
        .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "spark_catalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def write_gold_table(df, table_name, partition_fields=None):
    writer = df.write \
        .format("iceberg") \
        .mode("overwrite")
    
    if partition_fields:
        writer = writer.partitionBy(partition_fields)
    
    writer.saveAsTable(table_name)

def create_salary_trends_analysis(spark):
    salary_df = spark.table("spark_catalog.silver.salary_detailed")
    
    experience_analysis = salary_df.groupBy("experience_level") \
        .agg(
            count("*").alias("sample_size"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            spark_round(spark_min("salary"), 2).alias("min_salary"),
            spark_round(spark_max("salary"), 2).alias("max_salary"),
            spark_round(stddev("salary"), 2).alias("salary_stddev"),
            spark_round(percentile_approx("salary", 0.5), 2).alias("median_salary"),
            spark_round(percentile_approx("salary", 0.25), 2).alias("q1_salary"),
            spark_round(percentile_approx("salary", 0.75), 2).alias("q3_salary"),
            countDistinct("job_title").alias("unique_job_titles")
        ) \
        .withColumn("salary_range", col("max_salary") - col("min_salary")) \
        .withColumn("iqr_salary", col("q3_salary") - col("q1_salary")) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .orderBy("avg_experience")
    
    return experience_analysis

def create_education_impact_analysis(spark):
    salary_df = spark.table("spark_catalog.silver.salary_detailed")
    
    education_impact = salary_df.groupBy("education_level") \
        .agg(
            count("*").alias("sample_size"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            spark_round(spark_max("salary"), 2).alias("max_salary"),
            countDistinct("job_title").alias("unique_positions"),
            collect_list("job_title").alias("common_positions")
        ) \
        .withColumn("premium_score", 
                   spark_round((col("avg_salary") - 65000) / 10000, 2)) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .orderBy(col("avg_salary").desc())
    
    return education_impact

def create_gender_pay_analysis(spark):
    salary_df = spark.table("spark_catalog.silver.salary_detailed")
    
    gender_pay = salary_df.groupBy("gender") \
        .agg(
            count("*").alias("sample_size"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            spark_round(median("salary"), 2).alias("median_salary"),
            countDistinct("job_title").alias("unique_positions"),
            countDistinct("education_level").alias("education_levels")
        ) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .orderBy("avg_salary")
    
    gender_education_pay = salary_df.groupBy("gender", "education_level") \
        .agg(
            count("*").alias("sample_size"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience")
        ) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .orderBy("education_level", "gender")
    
    return gender_pay, gender_education_pay

def create_job_title_analysis(spark):
    salary_df = spark.table("spark_catalog.silver.salary_detailed")
    
    window_spec = Window.orderBy(col("avg_salary").desc())
    
    job_analysis = salary_df.groupBy("job_title") \
        .agg(
            count("*").alias("sample_size"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            spark_round(spark_min("salary"), 2).alias("min_salary"),
            spark_round(spark_max("salary"), 2).alias("max_salary"),
            countDistinct("education_level").alias("education_variety"),
            spark_round(stddev("salary"), 2).alias("salary_stddev")
        ) \
        .filter(col("sample_size") >= 3) \
        .withColumn("pay_rank", dense_rank().over(window_spec)) \
        .withColumn("salary_range", col("max_salary") - col("min_salary")) \
        .withColumn("calculation_timestamp", current_timestamp()) \
        .orderBy(col("avg_salary").desc())
    
    return job_analysis

def create_industry_benchmarks(spark):
    salary_df = spark.table("spark_catalog.silver.salary_detailed")
    
    benchmarks = salary_df.agg(
        count("*").alias("total_records"),
        spark_round(avg("salary"), 2).alias("industry_avg_salary"),
        spark_round(avg("years_of_experience"), 2).alias("industry_avg_experience"),
        spark_round(median("salary"), 2).alias("industry_median_salary"),
        spark_round(stddev("salary"), 2).alias("industry_salary_stddev"),
        countDistinct("job_title").alias("total_job_titles"),
        countDistinct("education_level").alias("total_education_levels"),
        countDistinct("gender").alias("gender_categories")
    ) \
    .withColumn("data_quality_score", 
               spark_round((col("total_records") / 1000) * 100, 2)) \
    .withColumn("calculation_timestamp", current_timestamp()) \
    .withColumn("benchmark_id", lit("industry_overall"))
    
    return benchmarks

def main():
    spark = create_spark_session()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.gold")
    
    try:
        experience_analysis = create_salary_trends_analysis(spark)
        print("Salary trends preview:")
        experience_analysis.show(truncate=False)
        
        education_impact = create_education_impact_analysis(spark)
        education_impact.show(truncate=False)
        
        gender_pay, gender_education_pay = create_gender_pay_analysis(spark)
        gender_pay.show(truncate=False)
        
        job_analysis = create_job_title_analysis(spark)
        job_analysis.filter(col("pay_rank") <= 10).show(truncate=False)
        
        benchmarks = create_industry_benchmarks(spark)
        benchmarks.show(truncate=False)
        
        
        write_gold_table(
            experience_analysis,
            "spark_catalog.gold.salary_experience_trends"
        )
        
        write_gold_table(
            education_impact,
            "spark_catalog.gold.education_salary_impact"
        )
        
        write_gold_table(
            gender_pay,
            "spark_catalog.gold.gender_pay_overview"
        )
        
        write_gold_table(
            gender_education_pay,
            "spark_catalog.gold.gender_pay_by_education",
            ["education_level"]
        )
        
        write_gold_table(
            job_analysis,
            "spark_catalog.gold.job_title_salary_analysis"
        )
        
        write_gold_table(
            benchmarks,
            "spark_catalog.gold.industry_benchmarks"
        )
        
        print("Gold layer tables created:")
        gold_tables = spark.sql("SHOW TABLES IN spark_catalog.gold")
        gold_tables.show()
        
        print("\nTop 10 highest paying jobs:")
        top_jobs = spark.sql("""
            SELECT job_title, avg_salary, sample_size, pay_rank
            FROM spark_catalog.gold.job_title_salary_analysis
            WHERE pay_rank <= 10
            ORDER BY pay_rank
        """)
        top_jobs.show(truncate=False)
        
        print("\nEducation level impact on salary:")
        education_impact_view = spark.sql("""
            SELECT education_level, avg_salary, sample_size, premium_score
            FROM spark_catalog.gold.education_salary_impact
            ORDER BY avg_salary DESC
        """)
        education_impact_view.show(truncate=False)
        
        print("\nIndustry benchmarks:")
        benchmark_view = spark.sql("""
            SELECT industry_avg_salary, industry_median_salary, 
                   total_job_titles, total_records
            FROM spark_catalog.gold.industry_benchmarks
        """)
        benchmark_view.show(truncate=False)
        
        print("\nTable history for experience trends:")
        history_df = spark.sql("""
            SELECT * FROM spark_catalog.gold.salary_experience_trends.history
        """)
        history_df.show(truncate=False)
        
        print("\nSilver -> Gold: Salary analytics completed successfully!")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()