FROM apache/spark:3.5.1

WORKDIR /app
ENV PYTHONUNBUFFERED=1
USER root

# Ensure Ivy cache is writable
RUN mkdir -p /home/spark/.ivy2/cache && chmod -R 777 /home/spark/.ivy2

# Copy the consumer script
COPY spark_consumer.py /app/spark_consumer.py

# Copy all required JARs into Spark classpath
COPY jars/*.jar /opt/spark/jars/

# Run Spark with Kafka + Delta + S3A support
CMD ["/opt/spark/bin/spark-submit", "--jars", "/opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.772.jar", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000", "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin", "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin", "--conf", "spark.hadoop.fs.s3a.path.style.access=true", "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem", "/app/spark_consumer.py"]






