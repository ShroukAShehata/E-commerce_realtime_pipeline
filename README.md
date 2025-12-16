# E-commerce Real-Time Data Pipeline (MongoDB + Kafka + PySpark + GCS + Airflow)

## Overview

This project demonstrates building a real-time pipeline for e-commerce data using MongoDB, Kafka, PySpark, GCS, and Airflow. It covers:

- Capturing Change Data Capture (CDC) from MongoDB using Debezium
- Simulating clickstream events and producing them to Kafka
- Streaming and transforming data using PySpark Structured Streaming
- Add ingestion timestamps and partitioning by date
- Storing raw Bronze layer data in Google Cloud Storage (GCS)
- Orchestrating end-to-end workflows using Apache Airflow

## Dataset

The project uses two sources:

- Products Data (MongoDB)
Captured via Debezium CDC connector and streamed to Kafka topic products

- Clickstream Events
Simulated user activity data produced to Kafka topic clickstream_raw


## Workflow

1-Data Ingestion

MongoDB → Debezium → Kafka

Debezium captures changes from the MongoDB products collection and streams them to Kafka topic products

Clickstream Simulation → Kafka

Python producer simulates real-time events and writes to Kafka topic clickstream_raw


2-Stream Processing (PySpark)

Kafka → PySpark Streaming → GCS

- Read from Kafka topics using PySpark Structured Streaming
- Parse JSON payloads and select required columns
- Enrich data with ingestion time fields
- Store as Parquet in GCS Bronze layer
- Partition by ingest_date for optimized querying


3-Orchestration (Airflow)

Airflow DAGs schedule PySpark consumers to:

- Stream and process products topic
- Stream and process clickstream_raw topic


4-Bronze Layer Storage (GCS)

All raw data is persisted in Google Cloud Storage:
Bucket: gs://ecommerce-datalake

Bronze layer paths:
- gs://ecommerce-datalake/bronze/products/
- gs://ecommerce-datalake/bronze/clickstream/

