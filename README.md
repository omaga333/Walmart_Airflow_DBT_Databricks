# 🛒 Enterprise Retail Lakehouse: Walmart Data Platform

## 📌 Project Overview

This project is an end-to-end, production-grade **Medallion Lakehouse Platform** designed to process highly fragmented retail data. The platform integrates real-time transactional data from a live Agentic Database alongside asynchronous flat files from an AWS S3 Data Lake into a unified, AI-ready Databricks environment.

Rather than focusing solely on data movement, this architecture prioritizes **Cloud Security (Zero-hardcoded credentials), Compute Cost Optimization, and Environment Isolation**.

## 🏗️ System Architecture

<img width="1446" height="720" alt="image" src="https://github.com/user-attachments/assets/55060de9-d297-475b-8d03-45a579465b00" />

### 1. The Data Sources

* **Agentic DB (PostgreSQL-based):** A live operational database powering a SQL Chatbot. Changes are captured incrementally via **CDC (Change Data Capture)**.
* **AWS S3 Data Lake:** An asynchronous landing zone for external supplementary files (e.g., product reviews, logs).

### 2. Medallion Data Modeling (dbt & Databricks)

* 🥉 **Bronze Layer (Ingestion):**
* Ingests CDC streams from the Agentic DB.
* Utilizes **Databricks Auto Loader (Streaming Tables)** to efficiently ingest only new files dropped into AWS S3, writing them natively in Delta format.


* 🥈 **Silver Layer (Cleansing & OBT):**
* Standardizes data types, handles deduplication, and enforces schema consistency.
* **One-Big Table (OBT):** Complex relational data is denormalized into a highly optimized OBT. This architectural decision eliminates expensive runtime joins and massively accelerates downstream BI queries.


* 🥇 **Gold Layer (Analytics & AI-Ready):**
* A refined **Star Schema** built on top of the OBT.
* Utilizes **SCD Type 2 (dbt Snapshots)** to track historical changes in business entities (e.g., product price changes) using `valid_from` and `valid_to` timestamps.



## ⚙️ Key Engineering Decisions

### ☁️ Secure Cloud Integration (Zero-Hardcoded Keys)

Connecting Databricks to AWS S3 was handled using strict IAM security protocols. Instead of hardcoding AWS Access Keys, I configured a **Databricks External Location** which deployed an **AWS CloudFormation** stack. This established a secure IAM Trust Relationship, allowing Databricks to read S3 buckets securely without exposing credentials.

### 💰 Compute Cost Optimization

Cloud data warehouses charge by compute time. To minimize costs:

* Implemented **dbt Incremental Models** (`is_incremental()`) ensuring that the Databricks cluster only processes and upserts newly arrived data (the delta) rather than performing full table scans.
* Leveraged **Ephemeral Materializations** for intermediate transformations to save storage space.

## 2. Data Transformation (The dbt Engine)

Once the raw data lands in the Bronze Layer, it needs heavy cleaning and modeling. Instead of writing massive, unmaintainable PySpark scripts, we used **dbt-core** to handle the logic.

* **Cleansing & Formatting (Silver Layer):** We split this layer into a Technical Silver (for casting data types and deduplication) and a Business Silver (for actual business logic and schema enforcement).
* **Compute Cost Optimization (Incremental Models):** This is a critical systems engineering move. Cloud data warehouses charge for compute time. If we used standard table materializations, dbt would drop and rebuild massive tables every day. Instead, we used the `is_incremental()` macro with highly optimized `where` and `and` clauses. Databricks now only processes the *delta* (newly arrived data) and runs an Upsert (Merge) against the target table, drastically slashing compute costs.
* **Tracking History (SCD Type 2 & Snapshots):** If a product's price changes in Walmart, we cannot overwrite the old price, or we will corrupt historical sales reports. We implemented **dbt Snapshots** to handle Slowly Changing Dimensions (SCD Type 2). The system preserves the old record and inserts the new one with updated `valid_from` and `valid_to` timestamps.
* **Storage Optimization (Ephemeral Models):** Certain intermediate calculations don't need to be saved as physical tables taking up storage space. We materialized them as `ephemeral`, meaning dbt simply injects their SQL logic as CTEs (Common Table Expressions) at runtime.
<img width="677" height="705" alt="image" src="https://github.com/user-attachments/assets/128aa308-fb70-4f68-9e04-14de80ee74f0" />
<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%" />

## 3. The Compute & Storage Engine (Databricks & Delta Lake)

All the dbt transformations run on top of Databricks, which uses **Delta Lake** as the underlying storage layer.
Standard Data Lakes (like pure S3 or Azure Data Lake) do not allow you to easily UPDATE or DELETE rows inside Parquet files. By using the Delta format, we bring **ACID transactions** to cloud storage, which is exactly what makes our dbt Incremental models and Snapshots possible under the hood.
<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%" />

## 4.🐳 Isolated Orchestration (Docker + Airflow)

To automate this entire platform without human intervention, we built an isolated infrastructure using Apache Airflow.

* **Containerization (Custom Docker Image):** Good systems engineers don't install tools directly on the host machine. We utilized Docker, writing a custom `Dockerfile` that extends the official Airflow image to install `dbt-core` and the `dbt-databricks` adapter. This guarantees the environment will run identically on any server in the world without dependency conflicts.
* **Credential Security (Bind Mounts):** How does dbt inside a Docker container authenticate with Databricks without baking the access token into the Docker Image (which is a massive security risk)? We used **Docker Bind Mounts**. We kept the `profiles.yml` file (containing the secrets) safely on the host server and dynamically mounted it to `/opt/airflow/profiles` only at runtime.
* **DAG Architecture (Lineage):**
* We used the `BashOperator` to execute dbt commands (`run`, `test`), passing the `--profiles-dir` flag to point to our secure mount.
* We enforced strict task lineage using shift operators (`>>`). The Gold layer physically cannot execute if the Silver layer fails, and Silver won't run if the `Source Freshness` test fails.
<img width="1280" height="494" alt="image" src="https://github.com/user-attachments/assets/f30f873e-b40e-41b4-9bb2-61d0a4a84e80" />
<img width="1280" height="335" alt="image" src="https://github.com/user-attachments/assets/b726c1f1-2ccb-4c99-9f1f-9cfff86a768b" />


* **Precision Scheduling & Catchup Logic:**
* The DAG is scheduled to run daily at 11:00 AM using cron syntax (`0 11 * * *`).
* We utilized the `pendulum` library to lock the timezone to UTC, preventing server timezone drifts from breaking the schedule.
* **The Catchup Rule:** We explicitly set `catchup=False`. If the Airflow server goes down for a month and comes back online, a default Airflow setup would attempt to run 30 back-to-back pipelines to "catch up," which would overwhelm the Databricks cluster. Setting this to `False` ensures it only executes the current and future runs.

Let’s zoom in specifically on the **AWS S3** component. This part of the architecture is a perfect example of how modern cloud platforms interact securely without relying on bad practices like hardcoded passwords.

Here is the deep-dive systems breakdown of the **AWS S3 Cloud Integration**:
<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif" width="100%" />

## ☁️ AWS S3 Data Lake Integration

AWS S3 is used as the scalable Object Storage layer for semi-structured data (`reviews.csv`). The main challenge was not storage, but building a secure connection between S3 and Databricks.

### 🔹 Secure Cloud Access (IAM + External Locations)

Instead of using hardcoded AWS Access Keys (security risk), we implemented a secure **Trust Relationship** using Databricks External Locations:

- Databricks generated a secure setup flow through AWS CloudFormation.
- CloudFormation automatically created IAM Roles and Policies.
- Databricks assumes the IAM Role to access S3 securely without storing credentials.

### 🔹 Data Ingestion with Auto Loader

After establishing secure access:

- Created a Databricks Streaming Table connected to the S3 External Location.
- Used **Auto Loader** with checkpointing to track processed files.
- Only new files uploaded to S3 are automatically detected and ingested.
- Data is stored as optimized Delta Tables.

### 🔹 Benefits

This architecture provides:
- Secure cloud integration using IAM.
- Incremental data loading instead of full reloads.
- Scalable ingestion for future data growth.
- Independent S3 pipeline execution without affecting the main SQL Server CDC pipeline.


## 🛠️ Technology Stack

* **Compute & Storage:** Databricks, Delta Lake
* **Transformation:** dbt (Data Build Tool), SQL, Python
* **Orchestration:** Apache Airflow
* **Infrastructure:** AWS S3, AWS IAM, CloudFormation
* **Containerization:** Docker

## 🚀 Setup & Execution

**1. Clone the repository**

```bash
git clone https://github.com/your-username/walmart-retail-lakehouse.git
cd walmart-retail-lakehouse

```

**2. Configure your Databricks Profile**
Create a `profiles.yml` file on your local machine (outside the project directory for security):

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

**3. Build and Start the Dockerized Airflow Environment**
Ensure you update the `docker-compose.yml` to bind-mount your local `profiles.yml` path to `/opt/airflow/profiles`.

```bash
docker-compose build
docker-compose up -d

```

**4. Access Airflow**

* Navigate to `http://localhost:8080`
* Trigger the `walmart_lakehouse_pipeline` DAG to execute the end-to-end ingestion and transformation workflow.
