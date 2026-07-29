# 🛒 Enterprise Retail Lakehouse: Walmart Data Platform

---

## 📋 Table of Contents

1. [Project Overview](https://www.google.com/search?q=%23-project-overview)
2. [System Architecture](https://www.google.com/search?q=%23-system-architecture)
3. [Data Sources & Ingestion](https://www.google.com/search?q=%23-1-data-sources--ingestion)
4. [Data Transformation (The dbt Engine)](https://www.google.com/search?q=%23-2-data-transformation-the-dbt-engine)
5. [Compute & Storage Engine](https://www.google.com/search?q=%23-3-compute--storage-engine-databricks--delta-lake)
6. [Cloud Integration (AWS S3)](https://www.google.com/search?q=%23-4-aws-s3-cloud-integration)
7. [Orchestration (Docker & Airflow)](https://www.google.com/search?q=%23-5-isolated-orchestration-docker--airflow)
8. [Technology Stack](https://www.google.com/search?q=%23-technologystack)
9. [Setup & Execution](https://www.google.com/search?q=%23-setup--execution)

---

## 🎯 1. Project Overview

This project is an end-to-end, production-grade **Medallion Lakehouse Platform** designed to process highly fragmented retail data. The platform integrates real-time transactional data from a live Agentic Database alongside asynchronous flat files from an AWS S3 Data Lake into a unified, AI-ready Databricks environment.

> **Core Engineering Pillars:** Cloud Security (Zero-hardcoded credentials), Compute Cost Optimization, and Complete Environment Isolation.

---

## 🏗️ 2. System Architecture
<img width="1446" height="720" alt="image" src="https://github.com/user-attachments/assets/55060de9-d297-475b-8d03-45a579465b00" />

---

## 📥 3. Data Sources & Ingestion

* **⚡ Agentic DB (PostgreSQL-based):** A live operational database powering a SQL Chatbot. Changes are captured incrementally via **CDC (Change Data Capture)**.
<img width="1774" height="887" alt="ChatGPT-Image-Apr-29-2026-04_09_02-PM (1)" src="https://github.com/user-attachments/assets/d7f1e5f3-38ce-4cf5-a217-d9505d14d8bc" />

* **📂 AWS S3 Data Lake:** An asynchronous landing zone for external supplementary files (e.g., product reviews, logs).
<img width="1280" height="720" alt="maxresdefault" src="https://github.com/user-attachments/assets/24500968-f91d-4ab1-b624-a742882a6e46" />

* 
* **🥉 Bronze Layer:** Ingests CDC streams and utilizes **Databricks Auto Loader (Streaming Tables)** to efficiently ingest only new files dropped into AWS S3, writing them natively in Delta format.

---

## 🔄 4. Data Transformation (The dbt Engine)

Once the raw data lands in the Bronze Layer, it undergoes heavy cleaning and modeling using **dbt-core**.

* 🧹 **Silver Layer (Cleansing & OBT):** Split into Technical Silver (casting data types and deduplication) and Business Silver (business logic and schema enforcement). Complex relational data is denormalized into a **One-Big Table (OBT)** to eliminate expensive runtime joins and accelerate BI queries.
* 💰 **Compute Cost Optimization (Incremental Models):** Uses the `is_incremental()` macro with optimized logic so Databricks processes only the *delta* (new data) and executes an Upsert (Merge) against the target table.
* ⏳ **Tracking History (SCD Type 2 & Snapshots):** Implements **dbt Snapshots** to track historical changes (e.g., product price changes) using `valid_from` and `valid_to` timestamps.
* 💾 **Storage Optimization (Ephemeral Models):** Intermediate transformations are materialized as `ephemeral` to keep the data warehouse clean, avoiding physical table writes and injecting SQL logic as CTEs at runtime.
<img width="677" height="705" alt="image" src="https://github.com/user-attachments/assets/128aa308-fb70-4f68-9e04-14de80ee74f0" />
<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%" />
---

## ⚡ 5. Compute & Storage Engine (Databricks & Delta Lake)

All transformations run on **Databricks** backed by **Delta Lake**.

* Brings **ACID transactions** to cloud object storage, making incremental models and snapshots safe and reliable under the hood.

---

## ☁️ 6. AWS S3 Cloud Integration

AWS S3 serves as the scalable object storage layer for semi-structured data (`reviews.csv`).

* 🔒 **Secure Access (IAM + External Locations):** Eliminated hardcoded AWS Access Keys by configuring a Databricks External Location backed by an **AWS CloudFormation** stack to establish a secure IAM Trust Relationship.
* 🚀 **Auto Loader Streaming:** Automatically detects and ingests new files from S3 with built-in checkpointing.

---

## 🐳 7. Isolated Orchestration (Docker & Airflow)

* 📦 **Containerization:** Built a custom `Dockerfile` extending the official Airflow image to package `dbt-core` and `dbt-databricks`.
* 🔑 **Credential Security:** Utilized **Docker Bind Mounts** to safely mount the `profiles.yml` file from the host server at runtime, avoiding credential leakage inside the image.
* 🔗 **Task Lineage & Safety:** Strict task dependency ordering (`>>`) combined with `catchup=False` and UTC scheduling via `pendulum` (`0 11 * * *`) to prevent cluster crashes and timezone drifts.
<img width="1280" height="494" alt="image" src="https://github.com/user-attachments/assets/f30f873e-b40e-41b4-9bb2-61d0a4a84e80" />
<img width="1280" height="335" alt="image" src="https://github.com/user-attachments/assets/b726c1f1-2ccb-4c99-9f1f-9cfff86a768b" />
---

## 🛠️ 8. Technology Stack

* 💻 **Compute & Storage:** Databricks, Delta Lake, Apache Spark
* 🔄 **Transformation:** dbt (Data Build Tool), SQL, Python
* ⚙️ **Orchestration:** Apache Airflow
* ☁️ **Infrastructure:** AWS S3, AWS IAM, CloudFormation
* 🐳 **Containerization:** Docker

---

## 🚀 9. Setup & Execution

### Step 1: Clone the repository

```bash
git clone https://github.com/your-username/walmart-retail-lakehouse.git
cd walmart-retail-lakehouse

```

### Step 2: Configure your Databricks Profile

Create a `profiles.yml` file securely on your host machine:

```yaml
walmart_project:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: hive_metastore
      schema: default
      host: <your-databricks-host>
      http_path: <your-http-path>
      token: <your-personal-access-token>
      threads: 4

```

### Step 3: Build and Run via Docker Compose

Ensure your `docker-compose.yml` bind-mounts your local `profiles.yml` to `/opt/airflow/profiles`.

```bash
docker-compose build
docker-compose up -d

```

### Step 4: Access Airflow

* Open `http://localhost:8080` in your browser.
* Trigger the `walmart_lakehouse_pipeline` DAG.

---

#DataEngineering #Databricks #Spark #Lakehouse #DeltaLake #dbt #ApacheAirflow #AWS #DataArchitecture #Python #SQL
