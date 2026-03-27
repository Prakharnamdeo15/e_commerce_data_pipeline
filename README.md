E-Commerce Real-World Data Engineering Solution
This project replicates a production-grade data pipeline for an e-commerce platform, focusing on hybrid ingestion patterns and the Medallion Architecture.

🛠 Requirements
Docker (Containerization of the entire stack)

Python 3.x

Apache Kafka

Apache Spark (Structured Streaming & Batch)

Delta Lake

🏗 High-Level Overview
The architecture follows a decoupled Producer-Consumer model to handle both historical auditing and real-time insights:

Data Generation & Ingestion

Producer: A Python script generates synthetic e-commerce events (orders, clicks, user activity) and pushes them to a Kafka Topic.

Broker: Kafka Server acts as the distributed messaging backbone.

Consumption Layers

Batch Ingestion: Consumers pull data from Kafka for long-term storage and complex transformations.

Structured Spark Streaming: Provides low-latency processing for real-time analytics.

📊 Data Pipeline (Medallion Architecture)
We utilize the Delta Format across all layers to ensure ACID compliance, time travel, and schema enforcement via Delta logs.

1. Batch Processing Flow
Bronze Layer: Raw data landed directly from Kafka. Minimal transformation; preserves the "source of truth."

Silver Layer: Data is cleaned, filtered, and joined. Schema is enforced, and duplicates are removed.

Gold Layer: Business-level aggregates (e.g., daily revenue per category, user lifetime value).

2. Real-Time Flow
Spark Streaming → Real-time Gold: High-velocity data is processed in-memory to populate live dashboards for immediate decision-making.
