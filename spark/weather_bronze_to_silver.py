from pyspark.sql import SparkSession
from pyspark.sql import functions as F

bronze_path="s3a://weather-data/delta/weather_all"
silver_path="s3a://weather-data/silver/weather_silver"

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

spark.sparkContext.setLogLevel("INFO")

print(f"Reading Bronze Delta table from: {bronze_path}")

bronze_df=spark.read.format("delta").load(bronze_path)
print("Bronze schema:")
bronze_df.printSchema()

silver_df = (
    bronze_df
    .withColumn("timestamp_utc", F.col("timestamp").cast("timestamp"))
    .withColumn("date",F.to_date("timestamp_utc"))
    .withColumn("year",F.year("timestamp_utc"))
    .withColumn("month",F.month("timestamp_utc"))
    .withColumn("hour", F.hour("timestamp_utc"))
    .select(
        F.col("city").cast("string").alias("city"),
        "timestamp_utc",
        "date",
        "year",
        "month",
        "hour",
        F.col("temperature_c").cast("double").alias("temperature_c"),
        F.col("wind_speed_ms").cast("double").alias("wind_speed_ms")
    )
    .dropDuplicates(["city", "timestamp_utc"])
)


print("Silver schema:")
silver_df.printSchema()

silver_df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(silver_path)

print("Silver table written.")

