This project creates end to end pipeline to analyse the climate energy data from the year of 2025 for selected USA states

# Climate & Energy Analytics Platform

A **production-grade, end-to-end data engineering platform** that ingests **real-time and historical climate & energy data**, processes it using **Kafka, dbt, and AWS Athena**, and visualizes insights using **Grafana dashboards**.

This project demonstrates how modern organizations build **scalable data lake architectures** using **streaming ingestion, batch processing, orchestration, and analytics** on cloud infrastructure.

---

## Project Overview

Energy demand, pricing, and sustainability decisions are heavily influenced by **weather conditions** and **regional climate patterns**. However, climate and energy data:

- Comes from multiple APIs
- Arrives in different formats
- Is both real-time and historical
- Is difficult to analyze without normalization

This platform solves these challenges by creating a **unified climate–energy analytics system**.

---

## Real-World Problem Solved

This project enables:

- Understanding **how weather impacts energy prices**
- Comparing **seasonal energy trends (summer vs winter)**
- Correlating **sensor-level energy usage with climate data**
- Building **analytics-ready datasets** for dashboards and reporting

### Target Users
- Energy utilities
- Climate researchers
- Data analysts
- Smart-grid & IoT teams
- Sustainability and policy teams

---

## High-Level Architecture

graph TD
    subgraph "Data Sources (External APIs & IoT)"
        DS1["OpenWeather API"]
        DS2["OpenMeteo API"]
        DS3["NOAA Climate Data"]
        DS4["EIA Energy Prices"]
        DS5["Simulated Sensor / IoT"]
    end

    DS1 & DS2 & DS3 & DS4 & DS5 --> API

    subgraph "Ingestion & Streaming"
        API["FastAPI Ingestion Layer<br/>(API Gateway)"]
        KAFKA["Kafka + Zookeeper<br/>(Streaming Backbone)"]
        CONS["Kafka Consumer<br/>(Batching + Retries)"]
    end

    API --> KAFKA
    KAFKA --> CONS

    subgraph "Storage & Data Lakehouse"
        S3B["AWS S3 – Bronze Layer<br/>(Raw JSON Data)"]
        DBT["dbt Transformations<br/>(Bronze → Silver → Gold)"]
        GLUE["AWS Glue + Athena<br/>(Metadata + SQL Analytics)"]
    end

    CONS --> S3B
    S3B --> DBT
    DBT --> GLUE

    subgraph "Visualization & Monitoring"
        GRAFANA["Grafana Dashboards<br/>(Visualization Layer)"]
    end

    GLUE --> GRAFANA

    %% Styling
    style API fill:#f9f,stroke:#333,stroke-width:2px
    style KAFKA fill:#bbf,stroke:#333,stroke-width:2px
    style S3B fill:#dfd,stroke:#333,stroke-width:2px
    style GRAFANA fill:#fdb,stroke:#333,stroke-width:2px


**All services run inside Docker containers**

---

## Data Lake Design (AWS S3)

s3://climate-energy-raw-data/
│
├── bronze/ → Raw JSON data (immutable)
│ ├── openweather/
│ ├── openmeteo/
│ ├── noaa/
│ ├── eia_energy/
│ └── sensor_readings/
│
├── silver/ → Cleaned, normalized Parquet
│ ├── openweather/
│ ├── openmeteo/
│ ├── noaa/
│ ├── eia/
│ └── sensor/
│
├── gold/ → Analytics-ready datasets
│ └── weather_data/
│
└── athena_results/


---

## Data Layers Explained

### Bronze Layer (Raw)
- Format: **JSON**
- Immutable raw ingestion
- Written by Kafka consumers
- Used for replay and debugging

---

### Silver Layer (Clean & Structured)
- Format: **Parquet**
- Schema normalization
- Type casting
- Timestamp standardization
- Partitioned by `observation_date`
- Created using **dbt**

---

### Gold Layer (Analytics Ready)
- Format: **Parquet**
- Unified datasets for analysis
- Cross-source joins
- Used by Grafana dashboards

---

## Core Technologies Used

### Kafka
- High-throughput event streaming
- Decouples ingestion from storage
- Enables scalable real-time pipelines

### Zookeeper
- Manages Kafka metadata
- Broker coordination and leader election

### FastAPI
- API ingestion layer
- Fetches climate & energy data
- Publishes messages to Kafka topics

### Kafka Consumer
- Reads Kafka topics
- Implements batching and retries
- Writes raw data to S3 Bronze layer

### AWS S3
- Central data lake storage
- Hosts Bronze, Silver, and Gold layers

### AWS Athena + Glue
- Athena: serverless SQL query engine
- Glue: metadata catalog and schema registry

### dbt (Data Build Tool)
- SQL-based transformations
- Bronze → Silver → Gold
- Modular, testable models

### Apache Airflow
- Workflow orchestration
- Schedules ingestion and transformations
- Components:
  - Webserver (UI & monitoring)
  - Scheduler (DAG execution)

### Grafana
- Visualization and dashboards
- Connects to Athena
- Displays energy and weather insights

---

## Example Analytics & Dashboards

- Energy price trends by state
- Summer vs winter energy pricing
- Weather impact on energy demand
- City-level climate patterns
- Sensor energy usage vs temperature

---

## Dockerized Services

All components are fully containerized using Docker Compose:

- Kafka & Zookeeper
- FastAPI
- Kafka Consumer
- Airflow Webserver & Scheduler
- dbt
- Grafana
- AWS Localstack (for local testing)

---

## Conclusion

This project demonstrates a **real-world, production-ready data engineering system** integrating:

- Streaming ingestion
- Cloud-based data lake
- SQL transformations
- Workflow orchestration
- Analytics and visualization

It follows **industry best practices** used in modern data platforms.

---

✨ Built for **learning, scalability, and real-world impact**



