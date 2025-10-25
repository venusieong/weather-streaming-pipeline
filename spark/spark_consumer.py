from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Create SparkSession
spark = (SparkSession.builder
    .appName("WeatherStreamingConsumer")
    .getOrCreate())

# 2. Read from Kafka
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

# 4. Parse the JSON and extract columns
parsed_df = (kafka_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*"))

# 5. Write results to console
query = (parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start())

query.awaitTermination()
