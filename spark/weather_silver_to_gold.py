from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH="s3a://weather-data/silver/weather_silver"
GOLD_DAILY_PATH="s3a://weather-data/gold/weather_gold_daily"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

spark = (
    SparkSession.builder
    .appName("weather_bronze_to_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

silver_for_gold = spark.read.format("delta").load(SILVER_PATH)

gold_daily_df = (
    silver_for_gold
    .groupBy(
        "year",
        "month",
        "date",
        "city"
    )
    .agg(
        F.avg("temperature_c").alias("avg_temp_c"),
        F.min("temperature_c").alias("min_temp_c"),
        F.max("temperature_c").alias("max_temp_c"),
        F.avg("wind_speed_ms").alias("avg_wind_speed_ms"),
        F.max("wind_speed_ms").alias("max_wind_speed_ms")
    )
)

print("Gold (daily) schema:")
gold_daily_df.printSchema()


(
    gold_daily_df
    .write
    .format("delta")
    .mode("overwrite")          
    .partitionBy("year", "month")
    .save(GOLD_DAILY_PATH)
)

print("Gold daily table written.")

spark.stop()