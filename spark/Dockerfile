FROM apache/spark:3.5.1

WORKDIR /app
ENV PYTHONUNBUFFERED=1
USER root

# Ensure Ivy cache is writable (for --packages)
RUN mkdir -p /home/spark/.ivy2/cache && chmod -R 777 /home/spark/.ivy2

RUN pip install requests

# Copy the consumer script
COPY spark_consumer.py /app/spark_consumer.py
COPY spark_historical_bf.py /app/spark_historical_bf.py
COPY weather_bronze_to_silver.py /app/weather_bronze_to_silver.py
COPY weather_silver_to_gold.py /app/weather_silver_to_gold.py


# Copy all required JARs into Spark classpath
COPY jars/*.jar /opt/spark/jars/

# Install netcat for waiting on Cassandra
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Wait for Cassandra before running Spark
ENTRYPOINT ["bash", "-c", "echo 'Waiting for Cassandra to be ready...' && while ! nc -z cassandra 9042; do sleep 3; done; echo 'Cassandra is up! Starting Spark...' && /opt/spark/bin/spark-submit --jars /opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.772.jar,/opt/spark/jars/java-driver-core-4.15.0.jar,/opt/spark/jars/java-driver-query-builder-4.15.0.jar,/opt/spark/jars/java-driver-mapper-runtime-4.15.0.jar,/opt/spark/jars/java-driver-shaded-guava-25.1-jre.jar,/opt/spark/jars/java-driver-shaded-netty-4.15.0.jar,/opt/spark/jars/spark-cassandra-connector-driver_2.12-3.4.1.jar,/opt/spark/jars/spark-cassandra-connector-commons_2.12-3.4.1.jar,/opt/spark/jars/spark-cassandra-connector_2.12-3.5.0.jar,/opt/spark/jars/config-1.4.2.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.cassandra.connection.host=cassandra --conf spark.cassandra.connection.port=9042 /app/spark_consumer.py"]






