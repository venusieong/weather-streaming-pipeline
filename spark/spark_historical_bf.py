import requests
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.utils import AnalysisException


# ===============================
# 1. CONFIG – MATCHES YOUR PIPELINE
# ===============================

# Existing streaming Delta table path (from your consumer)
BRONZE_STREAM_PATH   = "s3a://weather-data/delta/weather"

# New backfill-only path
BRONZE_BACKFILL_PATH = "s3a://weather-data/delta/weather_backfill"

# Unified “single source of truth” path
BRONZE_ALL_PATH      = "s3a://weather-data/delta/weather_all"

CITIES = [
    {"name": "London",  "lat": 51.5072, "lon": -0.1276},
    {"name": "New York","lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo",   "lat": 35.6895, "lon": 139.6917},
]

# Backfill range: last 90 days
BACKFILL_DAYS = 90


# ===============================
# 2. START SPARK SESSION
# ===============================

spark = (
    SparkSession.builder
    .appName("WeatherBackfill_90Days")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

print("Streaming path:   ", BRONZE_STREAM_PATH)
print("Backfill path:    ", BRONZE_BACKFILL_PATH)
print("Unified all path: ", BRONZE_ALL_PATH)


# ===============================
# 3. DATE RANGE FOR BACKFILL
# ===============================

end_date   = datetime.utcnow().date()
start_date = (datetime.utcnow() - timedelta(days=BACKFILL_DAYS)).date()

print(f"Backfilling from {start_date} to {end_date} (UTC dates)")


# ===============================
# 4. API CALL (Open-Meteo Archive Example)
# ===============================

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_city_history(city_name: str, lat: float, lon: float, start_date, end_date):
    """
    Fetch hourly historical weather for one city between start_date and end_date.
    Returns list of dicts matching your streaming schema:
      city, temperature_c, wind_speed_ms, timestamp
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join([
            "temperature_2m",
            "wind_speed_10m",
        ]),
        "timezone": "UTC",
    }

    print(f"Requesting {city_name} from {params['start_date']} to {params['end_date']}...")
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_10m", [])

    rows = []
    for i, ts in enumerate(times):
        rows.append({
            "city":          city_name,
            "timestamp":     ts,  # cast to timestamp later
            "temperature_c": float(temps[i]) if temps else None,
            "wind_speed_ms": float(winds[i]) if winds else None,
        })

    print(f"  -> got {len(rows)} rows for {city_name}")
    return rows


# ===============================
# 5. COLLECT BACKFILL ROWS FOR ALL CITIES
# ===============================

all_rows = []

for c in CITIES:
    city_rows = fetch_city_history(
        city_name=c["name"],
        lat=c["lat"],
        lon=c["lon"],
        start_date=start_date,
        end_date=end_date,
    )
    all_rows.extend(city_rows)

print(f"Total backfill rows across all cities: {len(all_rows)}")


# ===============================
# 6. BUILD SPARK DATAFRAME & WRITE BACKFILL
# ===============================

# Schema EXACTLY matching your streaming schema (same column names)
schema = StructType([
    StructField("city",          StringType(), False),
    StructField("timestamp",     StringType(), False),  # cast to TIMESTAMP after
    StructField("temperature_c", DoubleType(), True),
    StructField("wind_speed_ms", DoubleType(), True),
])

backfill_df = spark.createDataFrame(
    [Row(**r) for r in all_rows],
    schema=schema
).withColumn("timestamp", to_timestamp(col("timestamp")))

print("Backfill DataFrame sample:")
backfill_df.show(5, truncate=False)
backfill_df.printSchema()

# Write/overwrite backfill Delta table
(
    backfill_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(BRONZE_BACKFILL_PATH)
)

print(f"✅ Backfill written to {BRONZE_BACKFILL_PATH}")


# ===============================
# 7. UNIFY STREAM + BACKFILL INTO SINGLE TABLE
# ===============================

# Try to read existing streaming data
try:
    bronze_stream_df = spark.read.format("delta").load(BRONZE_STREAM_PATH)
    print("✅ Loaded existing streaming data from", BRONZE_STREAM_PATH)
except AnalysisException:
    print(f"⚠️ No existing streaming data at {BRONZE_STREAM_PATH}, using only backfill.")
    bronze_stream_df = None

bronze_backfill_df = spark.read.format("delta").load(BRONZE_BACKFILL_PATH)
print("✅ Loaded backfill data from", BRONZE_BACKFILL_PATH)

if bronze_stream_df is not None:
    bronze_all_df = (
        bronze_stream_df
        .unionByName(bronze_backfill_df, allowMissingColumns=True)
        .dropDuplicates(["city", "timestamp"])
    )
else:
    bronze_all_df = bronze_backfill_df.dropDuplicates(["city", "timestamp"])

print("Unified DataFrame sample:")
bronze_all_df.show(5, truncate=False)

(
    bronze_all_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(BRONZE_ALL_PATH)
)

print(f"✅ Unified bronze_all written to {BRONZE_ALL_PATH}")
print("Done.")
spark.stop()
