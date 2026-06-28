# Apache Airflow ETL Pipeline – E-Commerce Domain

---

# Project Overview

This project implements an end-to-end **ETL (Extract, Transform, Load)** pipeline using **Apache Airflow**, **Python**, and **MySQL** for processing E-Commerce transactional data.

The pipeline:

* Extracts raw order data from CSV
* Cleans and transforms dirty data
* Loads data into MySQL schemas
* Generates analytical KPI reports

---

# Tech Stack

* Apache Airflow 3.2.2
* Python 3.x
* Docker & Docker Compose
* MySQL 8.x
* Pandas
* VS Code

---

# Project Structure

```bash
2547245_E_Airflow_ETL/
│
├── dags/
│   ├── etl_e_2547245.py
│   ├── domain_5_ecommerce.csv
│   └── cleaned_ecommerce.csv
│
├── screenshots/
│   ├── SS1_graph.png
│   ├── SS2_success.png
│   ├── SS3_logs.png
│   ├── SS4_xcom.png
│   └── SS5_mysql.png
│
├── requirements.txt
├── report_2547245.pdf
└── README.md
```

---

# Prerequisites

Install the following before running:

## Required Software

* Docker Desktop
* MySQL 8.x
* Python 3.x
* VS Code (optional)

---

# Python Requirements

Install dependencies using:

```bash
pip install -r requirements.txt
```

Contents of requirements.txt:

```txt
apache-airflow==3.2.2
pandas==2.3.0
mysql-connector-python==9.3.0
SQLAlchemy==2.0.41
```

---

# MySQL Setup

Create 3 schemas:

```sql
CREATE DATABASE raw;
CREATE DATABASE staging;
CREATE DATABASE reporting;
```

Required tables:

* raw.ecommerce_orders
* staging.ecommerce_cleaned
* reporting.category_sales
* reporting.city_sales
* reporting.kpi_summary

---

# Docker Setup for Airflow

## Step 1: Download Airflow Compose File

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```

---

## Step 2: Create Required Folders

```bash
mkdir -p ./dags ./logs ./plugins
```

---

## Step 3: Create Environment File

Mac/Linux:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

---

## Step 4: Initialize Airflow

```bash
docker compose up airflow-init
```

---

## Step 5: Start Airflow

```bash
docker compose up -d
```

---

# Access Airflow UI

Open browser:

```text
http://localhost:8080
```

Default credentials:

* Username: `admin`
* Password: `admin`

---

# Running the ETL Pipeline

## Step 1

Place DAG file inside:

```text
dags/
```

Files required:

* etl_e_2547245.py
* domain_5_ecommerce.csv

---

## Step 2

Open Airflow UI.

---

## Step 3

Locate DAG:

```text
ecommerce_etl_pipeline
```

---

## Step 4

Trigger DAG manually.

Execution flow:

```text
Extract Orders
    ↓
Extract Metadata
    ↓
Branch Validation
    ↓
Transform Data
    ↓
Load to MySQL
    ↓
Notify Success
```

---

# Features Implemented

## Extract

* CSV ingestion
* Row count extraction
* Metadata extraction
* XCom push

## Transform

* Shifted row repair
* Null handling
* Remove test rows
* Negative price correction
* Date conversion
* Revenue calculation
* Delivery delay detection
* Return flag generation

## Load

Data loaded into:

* Raw Layer
* Staging Layer
* Reporting Layer

---

# KPIs Generated

* Total Orders
* Total Revenue
* Returned Orders
* Average Order Value
* Delayed Orders
* Category-wise Sales
* City-wise Revenue

---

# Monitoring Features

* DAG Graph View
* Grid View
* Task Logs
* XCom
* Retry Handling

---

# Retry Configuration

```python
retries = 3
retry_delay = timedelta(minutes=2)
```

---

# Schedule

Cron expression:

```text
0 2 * * *
```

Meaning:

Pipeline runs daily at **2:00 AM**.

---

# Stopping Airflow

To stop containers:

```bash
docker compose down
```

---

# Restarting Airflow

```bash
docker compose up -d
```

---

# Author

**Sachin Sidd**
MCA – Christ University
