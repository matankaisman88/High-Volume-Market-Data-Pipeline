# High-Volume Market Data Pipeline

A robust, end-to-end data engineering pipeline designed to ingest, process, and analyze high-volume cryptocurrency market data. This project implements a **Medallion Architecture** (Bronze, Silver, Gold) using **Apache Spark**, **Delta Lake**, and an external **Hive Metastore** backed by **PostgreSQL**.

## Tech Stack

| Category | Technology | Description |
| :--- | :--- | :--- |
| **Processing Engine** | **Apache Spark 3.5.0** | Distributed data processing. |
| **Storage Format** | **Delta Lake 3.0.0** | ACID transactions and scalable metadata. |
| **Database/Metastore** | **PostgreSQL 16** | External Hive Metastore for table cataloging. |
| **Containerization** | **Docker & Compose** | Simplifies deployment of Spark clusters. |
| **Orchestration** | **PowerShell** | Automated pipeline execution script. |

## Architecture & Data Layers

The pipeline follows the Medallion design pattern to ensure data quality and reliability:

* **Bronze Layer (Raw):** Ingests raw JSON data from the **CoinGecko API**.
* **Silver Layer (Cleaned):** Performs data type casting and deduplication using **Spark Window Functions**.
* **Gold Layer (Aggregated):** Calculates high-level business metrics such as **Total Market Cap** and **Average Price**.

## Getting Started

### Prerequisites
* Docker and Docker Compose installed.
* PowerShell.

### Setup and Execution (PowerShell)

1.  **Spin up infrastructure:**
    ```powershell
    docker-compose up -d
    ```

2.  **Option A: Run the full pipeline (Recommended)**
    ```powershell
    powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1
    ```

3.  **Option B: Run the Fast Pipeline (Quick Test)**
    ```powershell
    docker cp fast_pipeline.py spark-master:/opt/spark/
    docker exec -it spark-master /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.0.0 /opt/spark/fast_pipeline.py
    ```
    This script generates `Final_Report.csv` in the `./data` directory.

4.  **Verify Data:**
    ```powershell
    docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/src/utils/check_bronze_data.py
    ```