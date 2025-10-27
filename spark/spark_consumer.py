from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Create SparkSession
spark = (SparkSession.builder
    .appName("WeatherStreamingConsumer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate())

# 2. Read from Kafka, used spark
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_stream")
    .option("startingOffsets", "latest")
    .load())

# 3. Define schema for JSON in Kafka 'value'
schema = (StructType()
    .add("city", StringType())
    .add("temperature_c", DoubleType())
    .add("wind_speed_ms", DoubleType())
    .add("timestamp", StringType()))

# 4. Parse the JSON and extract columns, used kafka_df and schema
parsed_df = (kafka_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*"))

# Write to Delta Lake on MinIO
output_path = "s3a://weather-data/delta/weather"
checkpoint_path = "s3a://weather-data/delta/_checkpoints/weather"

query = (
    parsed_df.writeStream
        .format("delta")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
)

query.awaitTermination()
