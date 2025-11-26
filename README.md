🌦️ Weather Streaming Pipeline

Real-time data engineering project using Kafka, Spark Structured Streaming, Delta Lake (MinIO), Cassandra, and Grafana
Built fully with Docker Compose

------------

⭐ Project Overview 
This project demonstrates a real-time end-to-end data streaming pipeline, built exactly like a modern production data engineering system. It ingests live weather data from the Open-Meteo REST API, streams it through Kafka, processes it using Spark Structured Streaming, lands it into a Delta Lakehouse on MinIO, and serves the latest metrics from Cassandra, visualized in Grafana.
The goal is to show real Data Engineering skills across:
- Real-time ingestion
- Stream processing & ETL
- Lakehouse architecture
- Serving layer with Cassandra
- Analytics dashboard
- Infrastructure-as-code with Docker Compose
- Delta Lake (ACID + time travel)
This pipeline can be extended later with Kubernetes, Airflow orchestration, and Prometheus alerting.


