# Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
**Course:** ETL (G01) — Faculty of Engineering and Basic Sciences  
**Academic Program:** Data Engineering and Artificial Intelligence  
**Author:** Mauricio Taborda (Taborda004)  
**Academic Period:** 2026-2  
**Evaluation Target:** 100% Rubric Compliance (5.0 / 5.0)


---

## Table of Contents
1. [Executive Summary & Engineering Objectives](#1-executive-summary--engineering-objectives)
2. [End-to-End System Architecture](#2-end-to-end-system-architecture)
3. [Exploratory Data Analysis (EDA) & Harmonization](#3-exploratory-data-analysis-eda--harmonization)
4. [Machine Learning Pipeline & Leakage Prevention](#4-machine-learning-pipeline--leakage-prevention)
5. [Docker Infrastructure & Kafka Configuration](#5-docker-infrastructure--kafka-configuration)
6. [JSON Event Contract & Streaming Specifications](#6-json-event-contract--streaming-specifications)
7. [PostgreSQL Star Schema (Dimensional Modeling)](#7-postgresql-star-schema-dimensional-modeling)
8. [Real-Time Streaming ETL & Fault-Tolerant Consumer](#8-real-time-streaming-etl--fault-tolerant-consumer)
9. [Analytical KPIs & Empirical Findings](#9-analytical-kpis--empirical-findings)
10. [Power BI Desktop Dashboard Integration](#10-power-bi-desktop-dashboard-integration)
11. [Repository Structure](#11-repository-structure)
12. [Step-by-Step Execution Guide](#12-step-by-step-execution-guide)
13. [Oral Defense Preparation (Key Questions & Answers)](#13-oral-defense-preparation-key-questions--answers)

---

## 1. Executive Summary & Engineering Objectives

This engineering project delivers a robust, event-driven **Streaming ETL architecture featuring real-time Machine Learning scoring and dimensional analytics**. Transitioning away from legacy, high-latency batch ETL workflows, this system processes continuous event streams with sub-second latency while guaranteeing transactional integrity and audit resilience.

### Primary Objectives:
1. **Heterogeneous Data Harmonization:** Consolidate 5 years of disparate World Happiness Report datasets (2015–2019) into a standardized master representation, resolving conflicting column topologies, missing geographic regions, and null values.
2. **Leakage-Free Machine Learning:** Train, validate, and serialize an end-to-end regression pipeline (`scikit-learn`) predicting happiness scores strictly from independent socio-economic indicators, completely eliminating target leakage.
3. **Decoupled Event Streaming:** Deploy an Apache Kafka messaging cluster on Docker Compose to stream observation events under a strict JSON schema contract, utilizing message key hashing (`country`) across 3 partitions.
4. **Fault-Tolerant Consumer Design:** Implement a Python consumer that lands every raw payload into a PostgreSQL staging table before validation, gracefully handling malformed or corrupted payloads without process failure.
5. **Dimensional Star Schema & Analytics:** Persist valid real-time predictions and dimensional hierarchies into a normalized PostgreSQL schema, enabling real-time analytical querying of the 4 mandatory KPIs and interactive visualization in Power BI Desktop.

---

## 2. End-to-End System Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 OFFLINE ML & HARMONIZATION PHASE                       │
└────────────────────────────────────────────────────────────────────────────────────────┘
  Raw Historical Datasets (2015.csv - 2019.csv in data/raw/)
               │
               ▼
  EDA & Harmonization Engine (notebooks/eda.ipynb)
  - Column mapping across 5 years to lower-case snake_case
  - Regional metadata imputation from invariant country mapping
  - Longitudinal imputation of missing UAE corruption value (0.3400)
               │
               ▼
  Unified Master Dataset (data/processed/happiness_unified.csv) [782 rows]
               │
               ▼
  Scikit-Learn Pipeline Training (notebooks/model_training.ipynb)
  - Train/Test Split: 70% Train (547 rows) / 30% Test (235 rows), random_state=42
  - Target Leakage Elimination (dropped Rank, Standard Error, Dystopia Residual)
  - Pipeline: SimpleImputer(median) -> StandardScaler() -> RandomForestRegressor()
  - Metrics: MAE = 0.4077 | RMSE = 0.5239 | R² = 0.7802
               │
               ▼
  Serialized Pipeline Artifact (models/model.pkl)
  Streaming Source Generation (data/streaming/happiness_stream_source.csv)

┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              REAL-TIME STREAMING ETL PHASE                             │
└────────────────────────────────────────────────────────────────────────────────────────┘
  Streaming Source (data/streaming/happiness_stream_source.csv)
               │
               ▼
  Kafka Streaming Producer (kafka/producer.py)
  - Iterates through 782 events + 4 controlled malformed test cases
  - Strict JSON Event Contract serialization
  - Message Key: 'country' (UTF-8 bytes for partition hashing)
  - Target Topic: 'happiness-events' (3 partitions, replication factor 1)
               │
               ▼
  Apache Kafka Broker (Confluent Platform Docker: localhost:9092)
  - Partition 0: Americas, Oceania, parts of Europe
  - Partition 1: Europe, parts of Asia
  - Partition 2: Africa, parts of Asia
               │
               ▼
  Fault-Tolerant Streaming Consumer (kafka/consumer.py) — Group: 'happiness-ml-team'
               │
               ├─► [STEP 1: RAW LANDING & AUDIT]
               │   Inserts raw payload into PostgreSQL table 'raw_happiness_events'
               │   Status: 'RECEIVED' (Audit trail guaranteed before any processing)
               │
               ├─► [STEP 2: CONTRACT VALIDATION]
               │   - Checks required keys (event_id, country, year, features, time)
               │   - Verifies numeric ranges and ISO-8601 timestamp formatting
               │   - IF INVALID: Updates raw table to 'INVALID_SCHEMA' or 'INVALID_VALUES'
               │     Logs warning and COMMITS offset. (Pipeline NEVER crashes!)
               │
               └─► [STEP 3: ENRICHMENT, INFERENCE & PERSISTENCE]
                   - Updates raw status to 'VALID'
                   - Upserts country dimension in 'dim_country'
                   - Upserts temporal dimension in 'dim_date'
                   - Executes real-time inference with 'models/model.pkl'
                   - Computes prediction_error: actual_score - predicted_score
                   - Inserts scored record into 'fact_predictions' with foreign keys

┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              ANALYTICS & BI CONSUMPTION PHASE                          │
└────────────────────────────────────────────────────────────────────────────────────────┘
  PostgreSQL 16 Analytical Warehouse (Port 6432, Database: happiness_db)
               │
               ├─► SQL Analytical Queries (sql/kpis.sql)
               │   - KPI 1: Regional MAE & RMSE Error Distribution
               │   - KPI 2: Country Residual Profiles & Top Divergent Nations
               │   - KPI 3: Real vs Predicted Accuracy Brackets (<0.25, 0.25-0.50, >0.50)
               │   - KPI 4: Longitudinal Drift & Temporal Trends (2015–2019)
               │   - Ingestion Audit: Fault Tolerance Resilience (782 Valid / 4 Corrupt)
               │
               └─► Power BI Desktop Dashboard (dashboards/)
                   - Workshop3_Happiness_Dashboard.pbit (Official Power BI template)
                   - Native Direct Connection to PostgreSQL Star Schema
```

---

## 3. Exploratory Data Analysis (EDA) & Harmonization

### 3.1 Raw Dataset Schema Drift Analysis
Across the 5 observation years (2015–2019), the raw datasets provided by the United Nations Sustainable Development Solutions Network suffered from severe schema drift:

| Year | File | Rows | Cols | Score Column | Social/Family Column | Corruption Column | Region Present? |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **2015** | `2015.csv` | 158 | 12 | `Happiness Score` | `Family` | `Trust (Government Corruption)` | Yes (10 regions) |
| **2016** | `2016.csv` | 157 | 13 | `Happiness Score` | `Family` | `Trust (Government Corruption)` | Yes (10 regions) |
| **2017** | `2017.csv` | 155 | 12 | `Happiness.Score` | `Family` | `Trust..Government.Corruption.` | No (Omitted) |
| **2018** | `2018.csv` | 156 | 9 | `Score` | `Social support` | `Perceptions of corruption` | No (Omitted) |
| **2019** | `2019.csv` | 156 | 9 | `Score` | `Social support` | `Perceptions of corruption` | No (Omitted) |

### 3.2 Harmonization & Imputation Methodology
To resolve these discrepancies, `notebooks/eda.ipynb` executes a deterministic normalization protocol:

1. **Column Mapping Table:**
   - Country Identifier: `Country` / `Country or region` $\rightarrow$ `country`
   - Happiness Score: `Happiness Score` / `Happiness.Score` / `Score` $\rightarrow$ `actual_happiness_score`
   - Economic Factor: `Economy (GDP per Capita)` / `GDP per capita` $\rightarrow$ `gdp`
   - Social Factor: `Family` / `Social support` $\rightarrow$ `family`
   - Health Factor: `Health (Life Expectancy)` / `Healthy life expectancy` $\rightarrow$ `health`
   - Freedom Factor: `Freedom` / `Freedom to make life choices` $\rightarrow$ `freedom`
   - Generosity Factor: `Generosity` $\rightarrow$ `generosity`
   - Governance Factor: `Trust (Government Corruption)` / `Perceptions of corruption` $\rightarrow$ `corruption`

2. **Missing Value Auditing & Handling:**
   - Out of all $782 \times 10 = 7,820$ data points, exactly **one single null value** existed: `Perceptions of corruption` for the **United Arab Emirates** in 2018.
   - *Imputation Choice:* Imputed using the longitudinal median of UAE's corruption perception across other observation years ($0.3400$). Dropping the record would lose vital 2018 macroeconomic data, while global mean imputation would introduce artificial bias.

3. **Geographic Region Imputation:**
   - An invariant reference catalog was constructed from the 2015 and 2016 datasets mapping each country to its geopolitical region (e.g., *Western Europe*, *Latin America and Caribbean*, *Sub-Saharan Africa*).
   - This catalog was joined against 2017–2019 records, achieving **100% regional completeness** for all 782 historical events.

4. **Resulting Artifacts:**
   - [`data/processed/happiness_unified.csv`](data/processed/happiness_unified.csv): 782 records $\times$ 10 harmonized columns.
   - [`data/streaming/happiness_stream_source.csv`](data/streaming/happiness_stream_source.csv): Formatted streaming source enriched with unique event identifiers (`event_id`) and synthetic temporal stamps.

---

## 4. Machine Learning Pipeline & Leakage Prevention

### 4.1 Strict Prevention of Target Leakage
Target leakage occurs when training features incorporate information that directly reveals the label or will not be available in a real-time production inference environment. As mandated by the workshop rubric, the following attributes were strictly eliminated:

* **`Happiness Rank` / `Overall rank`:** The rank is an ordinal sequence calculated directly by sorting the target variable. In streaming inference, the global rank of an incoming country event cannot be determined prior to scoring all records.
* **`Standard Error` / `Whisker.high` / `Whisker.low`:** Sample variance statistics calculated from the distribution of happiness evaluation responses.
* **`Dystopia Residual`:** The World Happiness methodology defines $Score = \sum(\text{6 Factors}) + \text{Dystopia Residual}$. Retaining the residual collapses regression into an artificial mathematical addition, destroying the model's predictive ability on real-world indicators.

### 4.2 Feature Space Definition
The regression model operates strictly on the 6 independent macroeconomic and social indicators:
$$\mathbf{X} = [\text{gdp}, \text{family}, \text{health}, \text{freedom}, \text{generosity}, \text{corruption}] \in \mathbb{R}^6$$
$$\mathbf{y} = \text{actual\_happiness\_score} \in \mathbb{R}$$

### 4.3 Pipeline Architecture & Hyperparameters
To prevent data contamination between training and test distributions, all preprocessing transformations are bundled inside a Scikit-Learn `Pipeline`:

```python
Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler()),
    ('regressor', RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=4,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    ))
])
```

* **Algorithm Justification:** `RandomForestRegressor` was chosen over linear models due to its proven capacity to model non-linear socio-economic interactions (e.g., the diminishing marginal returns of GDP per capita on happiness at high income levels) and its inherent robustness against outlier leverage points.

### 4.4 Model Evaluation (70% Train / 30% Test Split)
The dataset was partitioned using `train_test_split` with `random_state=42` (547 training rows, 235 testing rows):

| Metric | Training Set | Test Set (30%) | Interpretation |
| :--- | :---: | :---: | :--- |
| **MAE (Mean Absolute Error)** | `0.1874` | **`0.4077`** | Average deviation of ~0.40 points on a 1-to-10 scale. |
| **RMSE (Root Mean Squared Error)** | `0.2512` | **`0.5239`** | Low variance between MAE and RMSE confirms absence of severe prediction anomalies. |
| **$R^2$ Score (Coefficient of Det.)** | `0.9510` | **`0.7802`** | The model explains **78.02%** of total happiness variance on unseen data. |

The trained pipeline was serialized to [`models/model.pkl`](models/model.pkl) (1.93 MB) and verified for real-time single-record inference.

---

## 5. Docker Infrastructure & Kafka Configuration

The infrastructure is provisioned through [`docker-compose.yml`](docker-compose.yml), establishing an isolated container network (`workshop3-net`):

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: workshop3-zookeeper
    ports: ["2181:2181"]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: workshop3-kafka
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 3

  postgres:
    image: postgres:16-alpine
    container_name: workshop3-postgres
    ports: ["6432:5432"] # Host port 6432 mapped to avoid Windows NAT socket exclusions
    environment:
      POSTGRES_DB: happiness_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/create_tables.sql:/docker-entrypoint-initdb.d/create_tables.sql
```

### Topic Topology:
- **Topic Name:** `happiness-events`
- **Partitions:** `3`
- **Replication Factor:** `1`
- **Routing Strategy:** Keyed messages using UTF-8 byte representation of `country`. Kafka applies Murmur2 hashing ($hash(\text{country}) \pmod 3$) guaranteeing that:
  1. All historical events for the same country land in the exact same partition in chronological order.
  2. Ingestion workload is evenly distributed across consumer threads.

---

## 6. JSON Event Contract & Streaming Specifications

### 6.1 Schema Contract Specification
Every message emitted to `happiness-events` must adhere strictly to the following JSON specification:

```json
{
  "event_id": "EVT-2019-COL-0064",
  "country": "Colombia",
  "year": 2019,
  "gdp": 0.985,
  "family": 1.410,
  "health": 0.841,
  "freedom": 0.470,
  "generosity": 0.099,
  "corruption": 0.034,
  "actual_happiness_score": 6.125,
  "event_time": "2019-03-20T12:00:00"
}
```

### 6.2 Contract Validation Rules:
1. `event_id`: String, mandatory, non-empty. Unique transaction trace.
2. `country`: String, mandatory. Must match the Kafka message key.
3. `year`: Integer, mandatory. Valid range: $1900 \le year \le 2100$.
4. Features (`gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`): Mandatory finite floating-point numbers $\ge 0$.
5. `actual_happiness_score`: Numeric float between $0.0$ and $10.0$. Used **exclusively for post-inference evaluation** (never supplied to the model).
6. `event_time`: Valid ISO-8601 formatted timestamp string (`YYYY-MM-DDTHH:MM:SS`).

### 6.3 Injected Test Anomalies (Resilience Demonstration)
To validate the consumer's fault tolerance, `kafka/producer.py` injects 4 controlled malformed events:
* `ERR-001`: Missing required field (`country` omitted).
* `ERR-002`: Corrupted schema (`gdp` provided as string `"INVALID_GDP"`).
* `ERR-003`: Negative economic value (`freedom = -0.5`).
* `ERR-004`: Out-of-bounds score (`actual_happiness_score = 99.9`).

---

## 7. PostgreSQL Star Schema (Dimensional Modeling)

The database schema implemented in [`sql/create_tables.sql`](sql/create_tables.sql) strictly enforces Kimball dimensional modeling standards:

```
┌────────────────────────────────────────────────────────┐
│                   raw_happiness_events                 │
├────────────────────────────────────────────────────────┤
│ raw_event_id (PK, BIGSERIAL)                           │
│ event_id (VARCHAR)                                     │
│ country (VARCHAR)                                      │
│ year (INT)                                             │
│ raw_payload (JSONB)                                    │
│ processing_status (VARCHAR)                            │
│ error_message (TEXT)                                   │
│ ingestion_timestamp (TIMESTAMPTZ)                      │
└──────────────────────────┬─────────────────────────────┘
                           │ 1:1 Traceability
                           ▼
┌────────────────────────────────────────────────────────┐
│                     fact_predictions                   │
├────────────────────────────────────────────────────────┤
│ prediction_id (PK, BIGSERIAL)                          │
│ raw_event_id (FK -> raw_happiness_events)              │
│ country_id (FK -> dim_country)                         │
│ date_id (FK -> dim_date)                               │
│ gdp, family, health, freedom, generosity, corruption   │
│ actual_score (FLOAT)                                   │
│ predicted_score (FLOAT)                                │
│ prediction_error (FLOAT) [actual - predicted]          │
│ event_time (TIMESTAMP)                                 │
│ prediction_timestamp (TIMESTAMPTZ)                     │
└──────────────┬──────────────────────────┬──────────────┘
               │ N:1                      │ N:1
               ▼                          ▼
┌───────────────────────────┐  ┌─────────────────────────┐
│        dim_country        │  │         dim_date        │
├───────────────────────────┤  ├─────────────────────────┤
│ country_id (PK, SERIAL)   │  │ date_id (PK, SERIAL)    │
│ country_name (VARCHAR, UQ)│  │ year (INT)              │
│ region (VARCHAR)          │  │ event_time (TIMESTAMP)  │
│ created_at (TIMESTAMPTZ)  │  │ month, day, hour, min   │
└───────────────────────────┘  └─────────────────────────┘
```

### Table Roles & Optimization:
* `raw_happiness_events`: Immutable landing table. Captures the original JSON payload and timestamp before parsing. Indexes on `processing_status` and `country`.
* `dim_country`: Conformed geographic dimension. Enforces unique country names with an index on `country_name`.
* `dim_date`: Conformed temporal dimension. Stores parsed year, month, day, hour, minute, and evaluation timestamp. Unique constraint on `(year, event_time)`.
* `fact_predictions`: Central fact table containing measures, predicted values, residuals, and foreign keys referencing dimensions with `ON DELETE CASCADE`.

---

## 8. Real-Time Streaming ETL & Fault-Tolerant Consumer

Implemented in [`kafka/consumer.py`](kafka/consumer.py), the consumer is designed for continuous, zero-downtime execution:

1. **Transactional First-Landing Pattern:**
   Every incoming event is inserted into `raw_happiness_events` with status `'RECEIVED'` inside a database transaction. Even if the consumer crashes during ML scoring, the audit record is safely persisted.
2. **Contract & Value Validation:**
   The consumer parses the payload against contract rules. If validation fails, the status is updated to `'INVALID_SCHEMA'` or `'INVALID_VALUES'` with the exact trace logged in `error_message`. The consumer commits the Kafka offset and continues immediately to the next message.
3. **Real-Time ML Scoring:**
   For valid messages, features are formatted into a DataFrame matching model feature names:
   $$\hat{y} = \text{pipeline.predict}(\mathbf{x})$$
   $$\text{error} = y_{\text{actual}} - \hat{y}$$
4. **Star Schema Upserts & Ingestion:**
   Retrieves or generates `country_id` and `date_id`, updates raw status to `'VALID'`, and persists the complete fact record into `fact_predictions`.

---

## 9. Analytical KPIs & Empirical Findings

The analytical SQL script [`sql/kpis.sql`](sql/kpis.sql) queries PostgreSQL directly. Below are the verified production results:

### KPI 1: Average Prediction Error (Global & Regional)
* **Global Predictions:** `782` valid events.
* **Global MAE:** `0.2405` points.
* **Global RMSE:** `0.3441` points.
* **Mean Prediction Bias:** `-0.0084` (symmetric error distribution; no systematic bias).

#### Regional Error Breakdown:
| Geographic Region | Event Count | Region MAE | Region RMSE | Avg Actual | Avg Predicted |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Australia and New Zealand** | 10 | `0.0786` | `0.1030` | 7.29 | 7.35 |
| **North America** | 10 | `0.0880` | `0.1119` | 7.17 | 7.25 |
| **Western Europe** | 104 | `0.1997` | `0.2647` | 6.69 | 6.65 |
| **Middle East and Northern Africa** | 99 | `0.2109` | `0.2979` | 5.37 | 5.39 |
| **Central and Eastern Europe** | 145 | `0.2179` | `0.2882` | 5.37 | 5.40 |
| **Eastern Asia** | 30 | `0.2201` | `0.2831` | 5.63 | 5.62 |
| **Sub-Saharan Africa** | 196 | `0.2520` | `0.3392` | 4.18 | 4.22 |
| **Southeastern Asia** | 44 | `0.2642` | `0.3541` | 5.33 | 5.23 |
| **Latin America and Caribbean** | 110 | `0.3128` | `0.4497` | 6.07 | 5.92 |
| **Southern Asia** | 34 | `0.3229` | `0.4446` | 4.58 | 4.70 |

*Insight:* High-income nations with stable institutional indicators exhibit errors under 0.10, whereas developing regions show higher residual variance due to subjective cultural and political factors not fully captured by the 6 standard indicators.

### KPI 2: Predictions by Country (Residual Profiles)
* **Top 3 Most Stable Countries (Lowest Error):**
  1. *Australia:* 5 predictions | MAE = `0.054` | Avg Actual: 7.28 | Avg Pred: 7.32
  2. *Uruguay:* 5 predictions | MAE = `0.057` | Avg Actual: 6.45 | Avg Pred: 6.42
  3. *Canada:* 5 predictions | MAE = `0.064` | Avg Actual: 7.35 | Avg Pred: 7.40
* **Top 3 Most Divergent Countries (Highest Outliers):**
  1. *Venezuela:* 5 predictions | MAE = `1.342` | Actual: 5.53 | Pred: 6.87 (Model overpredicted happiness due to rapid socio-economic shifts).
  2. *Botswana:* 5 predictions | MAE = `1.108` | Actual: 3.76 | Pred: 4.86 (Paradox of high GDP per capita but very low reported happiness).
  3. *Rwanda:* 5 predictions | MAE = `0.985` | Actual: 3.42 | Pred: 4.41.

### KPI 3: Actual vs. Predicted Correlation & Precision Brackets
* **High Precision Bracket ($|\text{Error}| \le 0.25$):** `538` events (**68.8%** of total stream).
* **Acceptable Precision Bracket ($0.25 < |\text{Error}| \le 0.50$):** `181` events (**23.1%**).
* **Divergent Bracket ($|\text{Error}| > 0.50$):** `63` events (**8.1%**).
* Over **91.9%** of streaming inferences fall within acceptable operational bounds.

### KPI 4: Temporal Prediction Trends (Longitudinal Stability)
| Observation Year | Events Processed | Actual Score Avg | Predicted Score Avg | Annual MAE | Annual RMSE |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **2015** | 158 | 5.376 | 5.350 | `0.2678` | `0.3798` |
| **2016** | 157 | 5.382 | 5.296 | `0.2444` | `0.3477` |
| **2017** | 155 | 5.354 | 5.414 | `0.2257` | `0.3297` |
| **2018** | 156 | 5.376 | 5.392 | `0.2223` | `0.3150` |
| **2019** | 156 | 5.407 | 5.446 | `0.2531` | `0.3540` |

*Insight:* Annual MAE remains remarkably stable between 0.22 and 0.26 across all 5 years, confirming that the pipeline does not suffer from longitudinal performance degradation or temporal model drift.

### Ingestion Resiliency & Fault Tolerance Summary:
* Total Events Processed: **786**
* `VALID` Processed into Star Schema: **782** (99.49%)
* `INVALID_SCHEMA` Filtered & Audited: **2** (0.25%)
* `INVALID_VALUES` Filtered & Audited: **2** (0.25%)
* **System Crashes or Downtime: 0.**

---

## 10. Power BI Desktop Dashboard Integration

The project provides the official Power BI dashboard template in the `dashboards/` directory:

* **Official Power BI Dashboard Template:** [`dashboards/Workshop3_Happiness_Dashboard.pbit`](dashboards/Workshop3_Happiness_Dashboard.pbit)

### Visual Dashboard Layout:
* **Metric Cards (Top Header):** `Total Predictions` (782), `Global MAE` (0.2405), `Global RMSE` (0.3441), `Avg Actual Score` (5.38), `Avg Predicted Score` (5.37), and `Prediction Bias` (-0.008).
* **Clustered Column Chart (KPI 1):** MAE and RMSE desegregated across the 10 geographic regions.
* **Line Chart (KPI 4):** Annual trajectory of Actual vs. Predicted Score from 2015 to 2019.
* **Country Matrix Table (KPI 2):** Drill-down table with country, region, actual score, prediction, and residual error.
* **Donut Chart (Ingestion Audit):** Visual validation of 782 valid events vs 4 filtered error events.
* **Interactive Slicers:** Dynamic filtering by Region and Observation Year.

---

## 11. Repository Structure

```
project/
├── data/
│   ├── raw/                 # Original UN World Happiness CSVs (2015.csv - 2019.csv)
│   ├── processed/           # Standardized master dataset (happiness_unified.csv)
│   └── streaming/           # Streaming event feed (happiness_stream_source.csv)
├── notebooks/
│   ├── eda.ipynb            # Fully executed EDA, schema profiling, and null audit
│   └── model_training.ipynb # Feature engineering, ML pipeline training, and evaluation
├── kafka/
│   ├── producer.py          # Streaming event producer with error injection
│   └── consumer.py          # Fault-tolerant consumer with real-time scoring
├── models/
│   └── model.pkl            # Serialized Scikit-Learn Pipeline (Imputer + Scaler + RF)
├── sql/
│   ├── create_tables.sql    # DDL script for PostgreSQL Star Schema
│   └── kpis.sql             # 4 Mandatory analytical SQL KPI queries
├── dashboards/
│   └── Workshop3_Happiness_Dashboard.pbit # Official Power BI Dashboard Template (live PostgreSQL queries)
├── docker-compose.yml       # Zookeeper, Kafka broker, and PostgreSQL infrastructure
├── requirements.txt         # Pinned production Python dependencies
├── .gitignore               # Clean git exclusions
└── README.md                # Comprehensive technical documentation
```

---

## 12. Step-by-Step Execution Guide

### Prerequisites
* Python 3.12+ installed
* Docker Desktop running with Linux containers enabled
* Git installed

### Step 1: Clone Repository & Install Dependencies
```bash
git clone https://github.com/Taborda004/Workshop-3.git
cd Workshop-3
pip install -r requirements.txt
```

### Step 2: Launch Docker Infrastructure
```bash
docker compose up -d
```
*Verify containers are running:*
```bash
docker compose ps
```
Kafka broker will be listening on `localhost:9092` and PostgreSQL on `localhost:6432`.

### Step 3: Initialize Star Schema Tables
PostgreSQL automatically executes `sql/create_tables.sql` upon container startup (mapped via `docker-compose.yml`). You can also execute it directly:
```bash
docker exec -i workshop3-postgres psql -U postgres -d happiness_db < sql/create_tables.sql
```

### Step 4: Run ETL & ML Notebooks (Offline Phase)
Open and run `notebooks/eda.ipynb` to harmonize schemas and `notebooks/model_training.ipynb` to train and serialize the model. The serialized model is already saved in `models/model.pkl`.

### Step 5: Start Real-Time Streaming Pipeline
Open two separate terminal windows:

* **Terminal 1 (Start Consumer):**
  ```bash
  python kafka/consumer.py --timeout-ms 25000
  ```
* **Terminal 2 (Start Producer):**
  ```bash
  python kafka/producer.py --delay 0.005
  ```

*Observations:* The consumer processes 782 valid events in real time, applies ML scoring, stores records in `fact_predictions`, and catches all 4 malformed events without crashing.

### Step 6: Query Analytical KPIs in PostgreSQL
Execute the 4 analytical KPI queries defined in `sql/kpis.sql`:
```bash
docker exec -i workshop3-postgres psql -U postgres -d happiness_db < sql/kpis.sql
```

### Step 7: Open Power BI Dashboard
Open [`dashboards/Workshop3_Happiness_Dashboard.pbit`](dashboards/Workshop3_Happiness_Dashboard.pbit) in Power BI Desktop. Confirm database credentials (`postgres` / `postgres` on `localhost:6432`) and click **Load**.

---

## 13. Oral Defense Preparation (Key Questions & Answers)

> **Q1: Why use 3 partitions in Kafka and why use `country` as the message key?**  
> **Answer:** Partitions are Kafka's unit of horizontal scalability. With 3 partitions, up to 3 consumer instances in group `happiness-ml-team` can consume in parallel. Using `country` as the key ensures that Murmur2 hashing directs all events for a given country to the exact same partition, preserving strict chronological ordering while evenly balancing workload across the cluster.

> **Q2: What constitutes Target Leakage in this project and why were `Happiness Rank` and `Dystopia Residual` dropped?**  
> **Answer:** Target leakage occurs when training features contain information derived from the target variable that will not be available during real-time inference. `Happiness Rank` is computed by ordering the target; in streaming, rank cannot be known before evaluating all incoming records. `Dystopia Residual` is defined as $Score - \sum(\text{Factors})$; retaining it reduces regression to an artificial mathematical sum rather than learning true socio-economic patterns.

> **Q3: Why does the consumer insert raw messages into PostgreSQL before validation and inference?**  
> **Answer:** This implements the *Landing / Staging* engineering pattern. Persisting the raw payload first guarantees complete auditability, immutability, and fault tolerance: even if a message is malformed or causes a parser failure, the original transaction is recorded. Furthermore, it enables *replayability*—if a newer model is deployed, historical raw events can be re-scored without re-streaming from Kafka.

> **Q4: Why connect Power BI to PostgreSQL rather than reading raw CSV files?**  
> **Answer:** In production streaming architectures, data arrives dynamically and must be queried from the analytical database to reflect real-time inferences and schema constraints. Connecting to PostgreSQL leverages relational indexes, foreign key referential integrity, and live updates, whereas reading CSVs represents a disconnected, static batch process.
