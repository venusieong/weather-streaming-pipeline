FROM apache/spark:3.5.1

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Switch to root for write permissions
USER root

# Make Ivy cache directory writable
RUN mkdir -p /home/spark/.ivy2/cache && chmod -R 777 /home/spark/.ivy2

# Copy your Spark Structured Streaming consumer
COPY spark_consumer.py /app/spark_consumer.py

# Run Spark with the Kafka connector for Scala 2.12
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/app/spark_consumer.py"]
