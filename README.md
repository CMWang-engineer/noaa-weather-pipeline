# 🌍 Global Weather Data Engineering Pipeline

An end-to-end data engineering project analyzing global temperature trends using NOAA weather station data from 10 countries (2020–2023).

---

## 📋 Table of Contents

- [Problem Description](#problem-description)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Dataset](#dataset)
- [Pipeline Overview](#pipeline-overview)
- [Data Warehouse](#data-warehouse)
- [dbt Transformations](#dbt-transformations)
- [Dashboard](#dashboard)
- [Workflow Orchestration](#workflow-orchestration)
- [How to Reproduce](#how-to-reproduce)

---

## Problem Description

This project answers the question: **How have global surface temperatures varied across different countries between 2020 and 2023?**

Using daily weather summaries from 200 NOAA weather stations across 10 countries, the pipeline ingests raw CSV data, processes it with Apache Spark, stores it in Google Cloud Storage as partitioned Parquet files, loads it into BigQuery, and transforms it with dbt into analysis-ready models for visualization in Looker Studio.

**Key questions explored:**
- Which countries have the highest and lowest average temperatures?
- How do year-over-year temperature trends differ by country?
- What are the monthly temperature patterns across regions?

---

## Architecture

```
NOAA GSOD API
     │
     ▼
Python + curl (Ingestion)
     │
     ▼
Apache Spark (Batch Processing)
  - Clean missing values (999.9 → null)
  - Convert Fahrenheit → Celsius
  - Extract country codes
  - Partition by YEAR
     │
     ▼
Google Cloud Storage (Data Lake)
  gs://summer-demo-kestra20260403/noaa/
  └── noaa_processed/YEAR=20xx/*.parquet
     │
     ▼
BigQuery (Data Warehouse)
  ├── external_noaa      (External Table)
  └── noaa_partitioned   (Partitioned + Clustered)
     │
     ▼
dbt (Transformations)
  ├── stg_noaa           (Staging)
  ├── mart_monthly_temp  (Monthly Aggregations)
  └── mart_yearly_trend  (Yearly Trend)
     │
     ▼
Looker Studio (Visualization)
```

---

## Technology Stack

| Layer | Tool | Details |
|-------|------|---------|
| Ingestion | Python + curl + gsutil | Downloads NOAA CSV, uploads to GCS |
| Batch Processing | Apache Spark (PySpark 4.1.1) | Cleaning, transformation, Parquet conversion |
| Data Lake | Google Cloud Storage | Parquet files partitioned by YEAR |
| Data Warehouse | BigQuery | Partitioned by DATE, clustered by COUNTRY |
| Transformation | dbt 1.11.7 + dbt-bigquery | Staging and mart models |
| Visualization | Looker Studio | Connected to BigQuery mart views |
| Orchestration | Kestra (Docker) | Orchestrates full pipeline stages |
| Containerization | Docker | Runs Kestra locally |
| Cloud Platform | Google Cloud Platform | GCS + BigQuery (region: europe-west2) |

---

## Dataset

**Source:** [NOAA Global Summary of the Day (GSOD)](https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/)

| Metric | Value |
|--------|-------|
| Stations | 200 (10 countries × 20 stations) |
| Countries | US, CA, UK, FR, GM, JA, IN, BR, AU, SZ |
| Time Range | 2020–2023 |
| Total Rows | 238,264 |
| Raw Size | 35 MB (CSV) → 2.6 MB (Parquet) |
| Missing Value Flag | 999.9 (replaced with null in Spark) |

---

## Pipeline Overview

### 1. Data Ingestion
Raw daily weather CSV files are downloaded from the NOAA GSOD endpoint for 200 selected stations across 10 countries. Files are merged into a single dataset and uploaded to GCS.

### 2. Batch Processing (Spark)
PySpark reads the raw CSVs and performs:
- Replaces `999.9` missing value flags with `null`
- Converts temperatures from Fahrenheit to Celsius (`TEMP_C`, `MAX_C`, `MIN_C`)
- Extracts 2-letter country code from station `NAME` field
- Writes output as Parquet partitioned by `YEAR`

```bash
# Output structure
gs://summer-demo-kestra20260403/noaa/noaa_processed/
├── YEAR=2020/*.parquet
├── YEAR=2021/*.parquet
├── YEAR=2022/*.parquet
└── YEAR=2023/*.parquet
```

### 3. Data Warehouse (BigQuery)
Two tables are created in the `noaa_weather` dataset (region: `europe-west2`):

- `external_noaa` — External table pointing to GCS Parquet files
- `noaa_partitioned` — Native table, **partitioned by DATE** (monthly), **clustered by COUNTRY**

---

## Data Warehouse

### Schema: `noaa_partitioned`

| Field | Type | Description |
|-------|------|-------------|
| STATION | INTEGER | Weather station ID |
| DATE | DATE | Observation date (partition key) |
| LATITUDE | FLOAT | Station latitude |
| LONGITUDE | FLOAT | Station longitude |
| ELEVATION | FLOAT | Station elevation (m) |
| NAME | STRING | Station name |
| COUNTRY | STRING | 2-letter country code (cluster key) |
| MONTH | INTEGER | Month number |
| TEMP_C | FLOAT | Average temperature (°C) |
| MAX_C | FLOAT | Maximum temperature (°C) |
| MIN_C | FLOAT | Minimum temperature (°C) |
| PRCP | FLOAT | Precipitation |
| WDSP | FLOAT | Average wind speed |

> **Note:** `YEAR` is a Parquet partition directory, not a column. Use `EXTRACT(YEAR FROM DATE)` in queries.

Partitioning by DATE reduces query cost when filtering by time range. Clustering by COUNTRY speeds up country-level aggregations.

---

## dbt Transformations

### Models

```
models/
├── staging/
│   ├── sources.yml
│   ├── stg_noaa.yml    (5 data tests)
│   └── stg_noaa.sql
└── marts/
    ├── mart_monthly_temp.sql
    └── mart_yearly_trend.sql
```

### Lineage Graph

```
noaa_weather.noaa_partitioned
        │
        ▼
    stg_noaa
    (staging: clean + type cast)
        │
        ├──▶ mart_monthly_temp
        │    (avg/max/min temp by country + month)
        │
        └──▶ mart_yearly_trend
             (avg/max/min temp by country + year)
```

> <img width="830" height="400" alt="image" src="https://github.com/user-attachments/assets/d4016c21-b36a-4dd4-a9df-32ee2d90be5d" />



### Data Tests
All 5 tests pass:
- `STATION` not null
- `DATE` not null
- `COUNTRY` not null
- `COUNTRY` accepted values (US, CA, UK, FR, GM, JA, IN, BR, AU, SZ)
- `TEMP_C` not null

### Run dbt

```bash
cd ~/DE-Zoomcamp && source dbt-env/bin/activate
cd noaa-weather-pipeline/noaa_dbt
dbt run
dbt test
dbt docs generate && dbt docs serve --port 8081
```

---

## Dashboard

Built in Looker Studio connected to BigQuery mart views.

**Chart 1: Global Temperature Trends (Line Chart)**
- Dimensions: YEAR (x-axis), COUNTRY (breakdown)
- Metric: avg_temp_c
- Shows year-over-year temperature trends per country (2020–2023)

> <img width="832" height="616" alt="image" src="https://github.com/user-attachments/assets/5469f986-2766-4b91-84d9-ad0dc548ee3f" />


**Chart 2: Temperature by Country (Bar Chart)**
- Dimension: COUNTRY
- Breakdown: YEAR
- Metric: avg_temp_c, sorted descending
- Shows relative temperature distribution across all 10 countries

> <img width="832" height="592" alt="image" src="https://github.com/user-attachments/assets/256acc8f-b83e-42bf-9571-1d07e25c8d45" />


**Key findings:**
- BR (Brazil) is consistently the warmest (~27°C average)
- CA (Canada) and US have the lowest averages, near or below 0°C
- Most countries show stable year-over-year trends with minor variation

---

## Workflow Orchestration

Kestra is used to orchestrate the full pipeline. It runs locally via Docker.

### Start Kestra

```bash
docker run --pull=always --rm -it \
  -p 8080:8080 \
  --user=root \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp/kestra-data:/app/storage \
  kestra/kestra:latest server local
```

Open `http://localhost:8080`, create the flow from `kestra/noaa-weather-pipeline.yml`.

### Flow stages

| Task | Description |
|------|-------------|
| `ingest_raw_data` | Download NOAA CSV, upload to GCS |
| `spark_processing` | Spark batch transform → Parquet |
| `load_to_bigquery` | Create external + partitioned tables |
| `dbt_transform` | Run dbt models and tests |
| `pipeline_summary` | Log final status |

> <img width="830" height="416" alt="image" src="https://github.com/user-attachments/assets/d025fd9d-df55-4a3e-8024-381213b5fc56" />


---

## How to Reproduce

### Prerequisites
- Google Cloud account with a project and service account key (BigQuery + GCS permissions)
- Docker Desktop
- Python 3.12+
- Java 17 (for Spark)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/CMWang-engineer/noaa-weather-pipeline.git
cd noaa-weather-pipeline

# 2. Create and activate Python virtual environment
python -m venv dbt-env
source dbt-env/bin/activate
pip install dbt-core dbt-bigquery pyspark

# 3. Place your GCP service account key in the project root
# e.g. my-project-key.json

# 4. Run Spark processing
export SPARK_HOME=~/spark-3.5.1-bin-hadoop3
python spark_transform.py

# 5. Upload to GCS
gsutil cp -r noaa_processed/ gs://<your-bucket>/noaa/

# 6. Create BigQuery tables
# Run the SQL scripts in bq_setup/

# 7. Run dbt
cd noaa_dbt
dbt run
dbt test

# 8. (Optional) Start Kestra orchestration
docker run --pull=always --rm -it \
  -p 8080:8080 --user=root \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp/kestra-data:/app/storage \
  kestra/kestra:latest server local
```

### Environment

| Tool | Version |
|------|---------|
| Python | 3.12 |
| PySpark | 4.1.1 |
| Spark | 3.5.1 |
| Java | 17.0.19 |
| dbt-core | 1.11.7 |
| dbt-bigquery | 1.11.1 |
| Docker | 29.3.0 |
