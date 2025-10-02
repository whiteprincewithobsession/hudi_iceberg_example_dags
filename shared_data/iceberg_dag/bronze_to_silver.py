from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_date, year, month, 
    when, lit, concat, sha2, row_number, regexp_replace,
    upper, trim, round as spark_round, count, avg,
    min as spark_min, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

def create_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilverIceberg_Salary") \
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

def write_iceberg_table(df, table_name, partition_fields=None):
    writer = df.write \
        .format("iceberg") \
        .mode("overwrite")
    
    if partition_fields:
        writer = writer.partitionBy(partition_fields)
        
    writer.saveAsTable(table_name)

def clean_and_standardize_education_level(education):
    return when(education.isNull(), "UNKNOWN") \
           .when(upper(education).contains("BACHELOR"), "Bachelor's") \
           .when(upper(education).contains("MASTER"), "Master's") \
           .when(upper(education).contains("PHD") | upper(education).contains("DOCTORATE"), "PhD") \
           .when(upper(education).contains("HIGH SCHOOL"), "High School") \
           .when(upper(education).contains("ASSOCIATE"), "Associate") \
           .otherwise(trim(education))

def clean_and_standardize_job_title(job_title):
    return when(job_title.isNull(), "Unknown Position") \
           .otherwise(trim(upper(job_title)))

def process_salary_data(spark, bronze_path):
    df = spark.read.option("header", "true").csv(bronze_path)
    
    print("🔍 ORIGINAL DATA SCHEMA:")
    df.printSchema()
    print("🔍 ORIGINAL DATA SAMPLE:")
    df.show(10, truncate=False)
    
    # Проверим что колонки существуют
    print("🔍 COLUMNS IN ORIGINAL DF:", df.columns)
    
    # Постепенно применяем трансформации и проверяем
    df_step1 = df.withColumn("age", col("Age").cast(IntegerType()))
    print("🔍 AFTER age transformation:")
    df_step1.printSchema()
    
    df_step2 = df_step1.withColumn("salary", regexp_replace(col("Salary"), "[^0-9.]", "").cast(DoubleType()))
    print("🔍 AFTER salary transformation:")
    df_step2.printSchema()
    print("🔍 SALARY VALUES:")
    df_step2.select("Salary", "salary").show(10, truncate=False)
    
    df_processed = df_step2 \
        .withColumn("gender", 
                   when(upper(col("Gender")) == "M", "Male")
                   .when(upper(col("Gender")) == "F", "Female")
                   .otherwise(trim(col("Gender")))) \
        .withColumn("education_level", clean_and_standardize_education_level(col("Education Level"))) \
        .withColumn("job_title", clean_and_standardize_job_title(col("Job Title"))) \
        .withColumn("years_of_experience", 
                   col("Years of Experience").cast(DoubleType())) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("etl_date", to_date(current_timestamp())) \
        .withColumn("year", year("etl_date")) \
        .withColumn("month", month("etl_date")) \
        .withColumn("experience_level",
                   when(col("years_of_experience") < 2, "Entry")
                   .when(col("years_of_experience") < 5, "Mid")
                   .when(col("years_of_experience") < 10, "Senior")
                   .otherwise("Expert")) \
        .withColumn("age_group",
                   when(col("age") < 25, "18-24")
                   .when(col("age") < 35, "25-34")
                   .when(col("age") < 45, "35-44")
                   .when(col("age") < 55, "45-54")
                   .otherwise("55+")) \
        .withColumn("salary_bucket",
                   when(col("salary") < 50000, "Under 50k")
                   .when(col("salary") < 75000, "50k-75k")
                   .when(col("salary") < 100000, "75k-100k")
                   .when(col("salary") < 150000, "100k-150k")
                   .otherwise("Over 150k")) \
        .withColumn("data_hash", 
                   sha2(concat(col("age"), col("gender"), col("education_level"), 
                              col("job_title"), col("years_of_experience")), 256)) \
        .dropDuplicates(["age", "gender", "education_level", "job_title", "years_of_experience"]) \
        .select("age", "gender", "education_level", "job_title", "years_of_experience", "salary", 
        "update_timestamp", "etl_date", "year", "month", "experience_level", 
        "age_group", "salary_bucket", "data_hash") \
        .filter(col("age").isNotNull() & col("salary").isNotNull())
    
    print("🔍 FINAL PROCESSED SCHEMA:")
    df_processed.printSchema()
    print("🔍 FINAL COLUMNS:", df_processed.columns)
    print("🔍 FINAL DATA SAMPLE:")
    df_processed.show(10, truncate=False)
    
    return df_processed

def create_salary_aggregates(spark, salary_df):
    education_stats = salary_df.groupBy("education_level") \
        .agg(
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            count("*").alias("record_count"),
            spark_round(spark_min("salary"), 2).alias("min_salary"),
            spark_round(spark_max("salary"), 2).alias("max_salary")
        ) \
        .withColumn("stat_type", lit("education_level")) \
        .withColumn("update_timestamp", current_timestamp())
    
    job_stats = salary_df.groupBy("job_title") \
        .agg(
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            count("*").alias("record_count"),
            spark_round(spark_min("salary"), 2).alias("min_salary"),
            spark_round(spark_max("salary"), 2).alias("max_salary")
        ) \
        .withColumn("stat_type", lit("job_title")) \
        .withColumn("update_timestamp", current_timestamp())
    
    gender_stats = salary_df.groupBy("gender") \
        .agg(
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_round(avg("years_of_experience"), 2).alias("avg_experience"),
            count("*").alias("record_count"),
            spark_round(spark_min("salary"), 2).alias("min_salary"),
            spark_round(spark_max("salary"), 2).alias("max_salary")
        ) \
        .withColumn("stat_type", lit("gender")) \
        .withColumn("update_timestamp", current_timestamp())
    
    return education_stats, job_stats, gender_stats

def main():
    spark = create_spark_session()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.gold")
    
    bronze_salary_path = "s3a://bronze-layer/salary_data/salary_data.csv"
    
    try:
        salary_df = process_salary_data(spark, bronze_salary_path)
        
        print("Processed salary data schema:")
        salary_df.printSchema()
        salary_df.show(10, truncate=False)
        
        education_stats, job_stats, gender_stats = create_salary_aggregates(spark, salary_df)
        
        write_iceberg_table(
            salary_df,
            "spark_catalog.silver.salary_detailed",
            ["year", "month"]
        )
        
        write_iceberg_table(
            education_stats,
            "spark_catalog.silver.salary_education_stats",
            ["education_level"]
        )
        
        write_iceberg_table(
            job_stats,
            "spark_catalog.silver.salary_job_stats"
        )
        
        write_iceberg_table(
            gender_stats,
            "spark_catalog.silver.salary_gender_stats"
        )
    
        
        print("Silver layer tables created:")
        silver_tables = spark.sql("SHOW TABLES IN spark_catalog.silver")
        silver_tables.show()
        
        sample_data = spark.sql("""
            SELECT age, gender, education_level, job_title, years_of_experience, salary 
            FROM spark_catalog.silver.salary_detailed 
            LIMIT 10
        """)
        print("Sample data from silver layer:")
        sample_data.show(truncate=False)
        
        education_sample = spark.sql("""
            SELECT * FROM spark_catalog.silver.salary_education_stats
        """)
        print("Education statistics:")
        education_sample.show(truncate=False)
        
        print("\nBronze -> Silver: Salary data processing completed successfully!")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()