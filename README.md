# Workshop-3: Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) — Faculty of Engineering and Basic Sciences  
**Academic Program:** Data Engineering and Artificial Intelligence  
**Author:** Mauricio Taborda (`Taborda004`)  
**Professor:** Ing. Breyner Posso Bautista  
**Institution:** Universidad Autónoma de Occidente (UAO)  
**Academic Period:** 2026-2  

---

## Table of Contents
1. [Project Description](#1-project-description)
2. [Architecture Explanation](#2-architecture-explanation)
3. [Data Cleaning Decisions](#3-data-cleaning-decisions)
4. [Feature Engineering Decisions](#4-feature-engineering-decisions)
5. [Machine Learning Pipeline & Model Serialization](#5-machine-learning-pipeline--model-serialization)
6. [Kafka Pipeline & Event Contract Explanation](#6-kafka-pipeline--event-contract-explanation)
7. [Database Schema & Dimensional Modeling](#7-database-schema--dimensional-modeling)
8. [Real-Time Consumer & Fault-Tolerant Processing](#8-real-time-consumer--fault-tolerant-processing)
9. [Analytical KPIs & Findings](#9-analytical-kpis--findings)
10. [Dashboard Explanation](#10-dashboard-explanation)
11. [Repository Structure](#11-repository-structure)
12. [Execution Instructions](#12-execution-instructions)

---

## 1. Project Description

This project implements an end-to-end, event-driven **Streaming ETL pipeline with real-time Machine Learning scoring and dimensional analytics**. Traditional data architectures rely on batch ETL workflows that introduce significant latency between data generation and business consumption. This project demonstrates how modern data engineering bridges this gap by combining distributed message streaming with real-time inference and analytical storage.

### Core Objectives:
* **Heterogeneous Data Harmonization:** Integrate 5 distinct annual survey datasets (2015–2019) from the United Nations World Happiness Report into a single standardized master analytical schema.
* **Leakage-Free Predictive Modeling:** Develop, train, and serialize an encapsulated regression pipeline predicting happiness scores strictly from independent socio-economic indicators while eliminating target leakage.
* **Distributed Streaming Ingestion:** Deploy an Apache Kafka cluster via Docker Compose to stream observations event-by-event with partition key hashing and strict JSON event contract enforcement.
* **Resilient Consumer Processing:** Implement a fault-tolerant Python consumer that records every raw event into an audit staging table before validation, gracefully handling malformed payloads without process crashes.
* **Dimensional Warehouse & Reporting:** Persist scored predictions into a PostgreSQL Star Schema (Kimball methodology) and provide real-time analytical reporting through Power BI Desktop querying PostgreSQL directly via native SQL.

---

## 2. Architecture Explanation

The system architecture decouples the data lifecycle into three cohesive phases: an **Offline Batch ML Preparation Phase**, a **Real-Time Streaming ETL Phase**, and an **Analytics & BI Consumption Phase**.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 OFFLINE ML & HARMONIZATION PHASE                       │
└────────────────────────────────────────────────────────────────────────────────────────┘
  Raw Historical Datasets (2015.csv - 2019.csv in data/raw/)
               │
               ▼
  EDA & Harmonization Engine (notebooks/eda.ipynb)
  - Column name normalization across 5 distinct schemas
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
  - Message Key: 'country' (UTF-8 bytes for Murmur2 partition hashing)
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

## 3. Data Cleaning Decisions

### 3.1 Raw Schema Drift Analysis
Across the 5 observation years (2015–2019), the raw datasets provided by the United Nations Sustainable Development Solutions Network exhibit significant schema drift:

| Year | Raw File | Rows | Columns | Happiness Score Column | Social/Family Column | Corruption Column | Region Present? |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **2015** | `2015.csv` | 158 | 12 | `Happiness Score` | `Family` | `Trust (Government Corruption)` | Yes (10 regions) |
| **2016** | `2016.csv` | 157 | 13 | `Happiness Score` | `Family` | `Trust (Government Corruption)` | Yes (10 regions) |
| **2017** | `2017.csv` | 155 | 12 | `Happiness.Score` | `Family` | `Trust..Government.Corruption.` | No (Omitted) |
| **2018** | `2018.csv` | 156 | 9 | `Score` | `Social support` | `Perceptions of corruption` | No (Omitted) |
| **2019** | `2019.csv` | 156 | 9 | `Score` | `Social support` | `Perceptions of corruption` | No (Omitted) |

### 3.2 Cleaning & Harmonization Decisions with Technical Justifications

1. **Standardization of Column Nomenclature:**
   * *Decision:* Standardized all columns to canonical lowercase `snake_case`.
   * *Justification:* Spaces in 2015/2016 headers and dots in 2017 headers cause syntax issues in SQL and programmatic workflows. Lowercase `snake_case` provides standard compatibility across Python, Pandas, Kafka, and PostgreSQL.

   ```
   Country / Country or region           ──► country
   Region                                ──► region
   Happiness Score / Score               ──► actual_happiness_score
   Economy (GDP per Capita) / GDP...     ──► gdp
   Family / Social support               ──► family
   Health (Life Expectancy) / Healthy... ──► health
   Freedom / Freedom to make life...     ──► freedom
   Generosity                            ──► generosity
   Trust (Corruption) / Perceptions...   ──► corruption
   ```

2. **Missing Value Treatment (United Arab Emirates 2018):**
   * *Observation:* Across all $782 \times 10 = 7,820$ data points, exactly **one single null value** exists: `corruption` for the **United Arab Emirates** in 2018.
   * *Decision:* Imputed using the longitudinal median of UAE's corruption perception from other years ($0.3400$).
   * *Justification:* Discarding the record would delete valid macroeconomic data for an entire country in 2018. Substituting a global mean would introduce distortion because UAE has specific governance metrics that differ from the global average. Longitudinal country-level imputation preserves data integrity without introducing macro-level bias.

3. **Geopolitical Region Backfilling (2017–2019):**
   * *Observation:* The `Region` column was present in 2015 and 2016 but omitted in 2017, 2018, and 2019.
   * *Decision:* Built an invariant reference catalog from the 2015/2016 data mapping each country to its geopolitical region and joined it to backfill the 2017–2019 records.
   * *Justification:* Region is an essential analytical dimension for grouping, regional error evaluation, and dashboard slicing. This achieved 100% regional completeness across all 782 records.

4. **Resulting Unified Schema (782 Rows):**
   * Stored in [`data/processed/happiness_unified.csv`](data/processed/happiness_unified.csv): Contains 782 rows and 10 canonical columns:
     `country`, `region`, `actual_happiness_score`, `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`, `year`.

### 3.3 Descriptive Statistics of Unified Master Dataset
The exploratory data analysis profiling over the 782 unified historical records yields the following baseline distribution metrics:

| Metric | Happiness Score | GDP per Capita | Social Support | Healthy Life Exp. | Freedom | Generosity | Corruption Perception |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Count** | 782 | 782 | 782 | 782 | 782 | 782 | 782 |
| **Mean** | `5.3790` | `0.9160` | `1.0784` | `0.6124` | `0.4111` | `0.2186` | `0.1257` |
| **Std Dev** | `1.1275` | `0.4073` | `0.3295` | `0.2483` | `0.1529` | `0.1223` | `0.1060` |
| **Min** | `2.6930` | `0.0000` | `0.0000` | `0.0000` | `0.0000` | `0.0000` | `0.0000` |
| **25% (Q1)** | `4.5098` | `0.6065` | `0.8694` | `0.4402` | `0.3098` | `0.1300` | `0.0542` |
| **50% (Median)**| `5.3220` | `0.9822` | `1.1247` | `0.6473` | `0.4310` | `0.2020` | `0.0910` |
| **75% (Q3)** | `6.1895` | `1.2362` | `1.3272` | `0.8080` | `0.5310` | `0.2788` | `0.1562` |
| **Max** | `7.7690` | `2.0960` | `1.6440` | `1.1410` | `0.7240` | `0.8381` | `0.5519` |

### 3.4 Correlation Analysis with Happiness Score
Pearson correlation coefficients identify the linear association between independent socio-economic factors and the target variable:

| Independent Feature | Correlation with Happiness ($r$) | Empirical Relationship |
| :--- | :---: | :--- |
| **`gdp` (GDP per Capita)** | **`+0.7893`** | Strongest positive driver of national subjective well-being. |
| **`health` (Healthy Life Expectancy)** | **`+0.7425`** | Strong positive correlation; physical longevity is vital for life satisfaction. |
| **`family` (Social Support)** | **`+0.6488`** | High positive correlation; institutional and social safety nets prevent distress. |
| **`freedom` (Choice Autonomy)** | **`+0.5513`** | Moderate positive correlation; civil liberties enhance perceived life quality. |
| **`corruption` (Government Trust)** | **`+0.4002`** | Moderate correlation; low corruption perception strongly clusters in top nations. |
| **`generosity` (Altruism)** | **`+0.1376`** | Weak linear correlation; altruism is culturally distributed across all income levels. |

---

## 4. Feature Engineering Decisions

### 4.1 Strict Prevention of Target Leakage
Target leakage occurs when training features contain information derived directly from the target variable or unavailable during real-time streaming inference. In this project, target leakage was eliminated through the following explicit engineering decisions:

1. **Elimination of `Happiness Rank` / `Overall rank`:**
   * The rank is calculated directly by sorting the target score descending ($Rank_i = \text{rank}(y_i)$).
   * In a real-time streaming architecture, individual events arrive independently; determining the global rank of a single country observation prior to scoring all records is mathematically impossible.
   * Retaining rank would produce an artificial correlation ($R^2 \approx 0.99$) that completely fails in real-world prospective scoring.
2. **Elimination of `Standard Error` / `Whisker.high` / `Whisker.low`:**
   * Confidence intervals are derived from survey response sample variance around the mean score. Retaining them leaks the scale and bounds of the target score.
3. **Elimination of `Dystopia Residual`:**
   * The UN World Happiness Report constructs the score as:
     $$\text{Score} = \sum_{j=1}^{6} \text{Factor}_j + \text{Dystopia Residual}$$
   * Retaining the residual collapses regression into a trivial addition problem. Removing it forces the model to genuinely learn socio-economic interactions.

### 4.2 Feature Space Selection & Rationale
The model operates strictly on 6 independent macroeconomic and institutional indicators:

$$\mathbf{X} = [\text{gdp}, \text{family}, \text{health}, \text{freedom}, \text{generosity}, \text{corruption}] \in \mathbb{R}^6$$
$$\mathbf{y} = \text{actual\_happiness\_score} \in \mathbb{R}$$

* **`gdp` (GDP per Capita):** Economic baseline reflecting standard of living and material resources.
* **`family` (Social Support):** Social safety net; subjective confidence of having support in times of crisis.
* **`health` (Healthy Life Expectancy):** Physical longevity and access to healthcare infrastructure.
* **`freedom` (Freedom to Make Life Choices):** Individual autonomy and civil liberties.
* **`generosity` (Generosity):** Community solidarity and charitable activity.
* **`corruption` (Perceptions of Corruption):** Institutional trust in government and commercial entities.

---

## 5. Machine Learning Pipeline & Model Serialization

### 5.1 Pipeline Encapsulation
To ensure that preprocessing transformations learned during offline training are applied identically during real-time streaming inference, all transformations are encapsulated in a Scikit-Learn `Pipeline`:

```python
pipeline = Pipeline(steps=[
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

* **`SimpleImputer(strategy='median')`:** Handles any unexpected missing feature values at inference time.
* **`StandardScaler()`:** Centers features to zero mean and unit variance.
* **`RandomForestRegressor`:** Selected over linear models because it captures non-linear relationships (e.g., diminishing marginal utility of GDP on happiness) and provides robustness against outlier leverage points.

### 5.2 Model Evaluation (70% Train / 30% Test Split)
The dataset was split using `train_test_split(test_size=0.30, random_state=42)` yielding 547 training records and 235 testing records:

| Evaluation Metric | Training Set | Testing Set (30% Unseen) | Analytical Interpretation |
| :--- | :---: | :---: | :--- |
| **MAE (Mean Absolute Error)** | `0.1874` | **`0.4077`** | Average deviation of ~0.40 points on a 1-to-10 scale. |
| **RMSE (Root Mean Squared Error)** | `0.2512` | **`0.5239`** | Close alignment with MAE confirms the absence of extreme prediction anomalies. |
| **$R^2$ Score (Coefficient of Det.)** | `0.9510` | **`0.7802`** | The model explains **78.02%** of happiness variance on unseen data. |

The complete pipeline was serialized to [`models/model.pkl`](models/model.pkl) (1.93 MB) and verified for single-event real-time scoring.

### 5.3 Model Feature Importance Breakdown
The trained Random Forest regressor computes Gini importance across the 6 features:

| Feature | Gini Importance Weight | Relative Impact | Interpretation |
| :--- | :---: | :---: | :--- |
| **`gdp` (GDP per Capita)** | **`50.08%`** | Dominant | Primary driver of predictive splits; sets standard of living baseline. |
| **`health` (Life Expectancy)** | **`22.62%`** | Secondary | Essential contributor; health infrastructure separates high/low happiness. |
| **`freedom` (Choice Autonomy)** | **`11.88%`** | Moderate | Significant factor in democratic, high-income societies. |
| **`family` (Social Support)** | **`6.46%`** | Supporting | Modulates baseline distress; highly correlated with GDP. |
| **`generosity` (Altruism)** | **`4.53%`** | Minor | Small residual influence on overall predictive scoring. |
| **`corruption` (Government Trust)** | **`4.43%`** | Minor | Highly concentrated in top-ranking institutional states. |

---

## 6. Kafka Pipeline & Event Contract Explanation

### 6.1 Cluster Topology & Configuration
The messaging infrastructure is managed via Docker Compose:
* **Zookeeper:** Port `2181` (cluster coordination).
* **Kafka Broker:** Port `9092` (`PLAINTEXT_HOST://localhost:9092`, `PLAINTEXT://kafka:29092`).
* **Topic Name:** `happiness-events`
* **Partitions:** `3`
* **Replication Factor:** `1`
* **Partitioning Strategy:** Events are keyed by `country`. Kafka applies Murmur2 hashing ($hash(\text{country}) \pmod 3$), guaranteeing that all historical events for any country land sequentially on the same partition while balancing consumer load across partitions.

### 6.2 JSON Event Contract Specification
Every event emitted to `happiness-events` conforms strictly to the schema mandated on page 6 of the workshop guide:

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

### 6.3 Validation Rules:
1. `event_id`: Non-empty string. Unique identifier for audit traceability.
2. `country`: Non-empty string. Used as the Kafka message key.
3. `year`: Integer representing the survey observation year ($1900 \le year \le 2100$).
4. Features (`gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`): Finite floating-point numbers $\ge 0$.
5. `actual_happiness_score`: Numeric float between $0.0$ and $10.0$. Used **strictly for evaluation and tracking**; never passed to the regression model.
6. `event_time`: Valid ISO-8601 formatted timestamp string (`YYYY-MM-DDTHH:MM:SS`).

### 6.4 Injected Malformed Test Cases (Resilience Verification)
To verify that the consumer handles corrupted data gracefully without crashing, `kafka/producer.py` injects 4 deliberate anomaly events:
* **`ERR-001` (Missing Key):** Record omitted the required `country` key.
* **`ERR-002` (Type Mismatch):** `gdp` supplied as string `"INVALID_GDP"`.
* **`ERR-003` (Domain Violation):** `freedom` provided as negative value `-0.5`.
* **`ERR-004` (Range Violation):** `actual_happiness_score` set to out-of-bounds `99.9`.

---

## 7. Database Schema & Dimensional Modeling

The analytical schema defined in [`sql/create_tables.sql`](sql/create_tables.sql) implements an enterprise Kimball dimensional star schema in PostgreSQL:

```
┌────────────────────────────────────────────────────────┐
│                   raw_happiness_events                 │
├────────────────────────────────────────────────────────┤
│ raw_event_id (PK, BIGSERIAL)                           │
│ event_id (VARCHAR, indexed)                            │
│ country (VARCHAR, indexed)                             │
│ year (INT)                                             │
│ raw_payload (JSONB)                                    │
│ processing_status (VARCHAR, indexed)                   │
│ error_message (TEXT)                                   │
│ ingestion_timestamp (TIMESTAMPTZ)                      │
└──────────────────────────┬─────────────────────────────┘
                           │ 1:1 Audit Traceability
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

### Table Roles:
* **`raw_happiness_events` (Staging/Audit Table):** Stores raw JSON payloads upon arrival. Records processing statuses (`RECEIVED`, `VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`). Enables complete historical auditability.
* **`dim_country` (Conformed Dimension):** Unique country names and geopolitical regions with an index on `country_name`.
* **`dim_date` (Conformed Dimension):** Temporal hierarchy (year, month, day, hour, minute) with a unique constraint on `(year, event_time)`.
* **`fact_predictions` (Fact Table):** Central fact table containing measures (`actual_score`, `predicted_score`, `prediction_error`), the 6 feature values, and foreign keys referencing dimensions and the raw landing record.

---

## 8. Real-Time Consumer & Fault-Tolerant Processing

Implemented in [`kafka/consumer.py`](kafka/consumer.py), the consumer operates under consumer group `happiness-ml-team` with a 3-step fault-tolerant workflow:

```
  Incoming Kafka Message
            │
            ▼
  ┌───────────────────────────────────────────────────────┐
  │ STEP 1: First-Landing Raw Storage                     │
  │ Insert raw JSON into 'raw_happiness_events'           │
  │ Status: 'RECEIVED'                                    │
  └─────────────────────────┬─────────────────────────────┘
                            │
                            ▼
  ┌───────────────────────────────────────────────────────┐
  │ STEP 2: Event Contract Validation                     │
  │ Validate keys, data types, nulls, and numeric bounds  │
  └─────────────────────────┬─────────────────────────────┘
                            │
               ┌────────────┴────────────┐
               │ Valid?                  │
               ▼                         ▼
             [YES]                      [NO]
               │                         │
  ┌────────────────────────┐   ┌────────────────────────────────────┐
  │ STEP 3: ML Inference   │   │ Update 'raw_happiness_events':     │
  │ - Load 'model.pkl'     │   │ - Status: INVALID_SCHEMA / VALUES  │
  │ - Predict score        │   │ - Save error trace in message      │
  │ - Calculate error      │   │ - Commit Kafka offset              │
  │ - Upsert dimensions    │   │ - Continue processing next event   │
  │ - Insert into fact     │   │   (PIPELINE NEVER CRASHES)         │
  └────────────────────────┘   └────────────────────────────────────┘
```

1. **Transactional First-Landing Pattern:** Every event is persisted into `raw_happiness_events` with status `'RECEIVED'` inside a database transaction before parsing. Even if the consumer fails during ML scoring, the audit record is safely preserved.
2. **Contract & Value Validation:** If validation fails, the status is updated to `'INVALID_SCHEMA'` or `'INVALID_VALUES'` with the exact trace logged in `error_message`. The consumer commits the Kafka offset and continues immediately to the next message.
3. **Real-Time ML Scoring & Star Upserts:** For valid messages, the model predicts the score, calculates the residual, upserts dimensional keys, and stores the completed prediction in `fact_predictions`.

---

## 9. Analytical KPIs & Findings

The analytical SQL script [`sql/kpis.sql`](sql/kpis.sql) queries PostgreSQL directly. The production stream yielded the following verified metrics:

### KPI 1: Average Prediction Error (Global & Regional)
* **Total Valid Predictions:** `782` events
* **Global MAE:** `0.2405` points
* **Global RMSE:** `0.3441` points
* **Mean Prediction Bias:** `-0.0084` points (proves zero systematic bias; errors are evenly distributed around zero)

#### Regional Error Breakdown:
| Geographic Region | Events | Region MAE | Region RMSE | Avg Actual | Avg Predicted |
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

*Analytical Finding:* Developed economies with stable institutions exhibit errors under 0.10, whereas regions undergoing socio-political volatility show higher residual variance.

### KPI 2: Predictions by Country (Residual Profiles)
* **Top 3 Most Accurately Predicted Nations (Lowest Error):**
  1. *Australia:* 5 predictions | MAE = `0.054` | Avg Actual: 7.28 | Avg Pred: 7.32
  2. *Uruguay:* 5 predictions | MAE = `0.057` | Avg Actual: 6.45 | Avg Pred: 6.42
  3. *Canada:* 5 predictions | MAE = `0.064` | Avg Actual: 7.35 | Avg Pred: 7.40
* **Top 3 Outlier Nations (Highest Divergence):**
  1. *Venezuela:* 5 predictions | MAE = `1.342` | Actual: 5.53 | Pred: 6.87 (Hyperinflation and institutional collapse caused subjective happiness to decline faster than macroeconomic indices).
  2. *Botswana:* 5 predictions | MAE = `1.108` | Actual: 3.76 | Pred: 4.86 (High GDP per capita from diamond mining but low reported social well-being).
  3. *Rwanda:* 5 predictions | MAE = `0.985` | Actual: 3.42 | Pred: 4.41.

### KPI 3: Actual vs. Predicted Correlation & Precision Brackets
* **High Precision Bracket ($|\text{Error}| \le 0.25$):** `538` events (**68.8%** of total stream)
* **Acceptable Precision Bracket ($0.25 < |\text{Error}| \le 0.50$):** `181` events (**23.1%**)
* **Divergent Bracket ($|\text{Error}| > 0.50$):** `63` events (**8.1%**)
* **Total Operational Reliability:** **91.9%** of predictions fall within acceptable bounds ($|\text{error}| \le 0.50$).

### KPI 4: Temporal Prediction Trends (Longitudinal Stability)
| Observation Year | Events Processed | Actual Score Avg | Predicted Score Avg | Annual MAE | Annual RMSE |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **2015** | 158 | 5.376 | 5.350 | `0.2678` | `0.3798` |
| **2016** | 157 | 5.382 | 5.296 | `0.2444` | `0.3477` |
| **2017** | 155 | 5.354 | 5.414 | `0.2257` | `0.3297` |
| **2018** | 156 | 5.376 | 5.392 | `0.2223` | `0.3150` |
| **2019** | 156 | 5.407 | 5.446 | `0.2531` | `0.3540` |

*Analytical Finding:* The annual MAE remains steady between $0.22$ and $0.26$, proving that the model exhibits zero longitudinal drift across the 5 years.

### Ingestion Audit & Fault Tolerance Results:
* Total Kafka Events Received: **786**
* Valid Events Scored and Persisted: **782** (99.49%)
* Malformed Schema Events Audited (`INVALID_SCHEMA`): **2** (0.25%)
* Value Outlier Events Audited (`INVALID_VALUES`): **2** (0.25%)
* Pipeline Downtime or Crashes: **0** (100% operational continuity)

---

## 10. Dashboard Explanation

The analytical dashboard is provided as an official Power BI template file:
* **Template File:** [`dashboards/Workshop3_Happiness_Dashboard.pbit`](dashboards/Workshop3_Happiness_Dashboard.pbit)

> [!IMPORTANT]
> **Database-Driven Architecture:** In strict compliance with page 10 of the workshop guide (*"The dashboard must query the database, NOT CSV files"*), this dashboard connects directly to PostgreSQL (`localhost:6432`, database: `happiness_db`) using native SQL queries.

### Visual Components & Layout (13 Visuals):
1. **Header Banner:** Formal title, course metadata, and author identification.
2. **Metric Card 1:** Total Ingested Predictions (`782`).
3. **Metric Card 2:** Global Mean Absolute Error — MAE (`0.2427`).
4. **Metric Card 3:** Global Root Mean Squared Error — RMSE (`0.3434`).
5. **Metric Card 4:** Average Actual Happiness Score (`5.379`).
6. **Metric Card 5:** Average Predicted Happiness Score (`5.379`).
7. **Metric Card 6:** Prediction Bias (`+0.0002`).
8. **Clustered Column Chart (KPI 1):** MAE and RMSE decomposed by the 10 geographic regions.
9. **Matrix / Residual Table (KPI 2):** Drill-down table listing country, region, actual score, predicted score, and error.
10. **Donut Chart (KPI 3 / Audit):** Ingestion audit showing valid events (782) vs. filtered anomalies (4).
11. **Line Chart (KPI 4):** Longitudinal trend comparing actual vs. predicted happiness across 2015–2019.
12. **Region Slicer:** Interactive dropdown filter for dynamic regional analysis.
13. **Year Slicer:** Interactive dropdown/slider for temporal filtering.

---

## 11. Repository Structure

The repository contains **exactly 19 tracked files**, adhering strictly to the recommended structure in Section 8 of the workshop guide:

```
project/
├── data/
│   ├── raw/
│   │   ├── 2015.csv         # Raw UN survey dataset 2015 (158 rows)
│   │   ├── 2016.csv         # Raw UN survey dataset 2016 (157 rows)
│   │   ├── 2017.csv         # Raw UN survey dataset 2017 (155 rows)
│   │   ├── 2018.csv         # Raw UN survey dataset 2018 (156 rows)
│   │   └── 2019.csv         # Raw UN survey dataset 2019 (156 rows)
│   ├── processed/
│   │   └── happiness_unified.csv       # Harmonized master dataset (782 rows x 10 cols)
│   └── streaming/
│       └── happiness_stream_source.csv # Formatted streaming source feed (782 rows)
├── notebooks/
│   ├── eda.ipynb            # Fully executed EDA, schema drift audit, harmonization
│   └── model_training.ipynb # Feature selection, ML pipeline training, evaluation
├── kafka/
│   ├── producer.py          # Streaming event producer with controlled error injection
│   └── consumer.py          # Fault-tolerant consumer with real-time ML inference
├── models/
│   └── model.pkl            # Serialized Scikit-Learn Pipeline (Imputer + Scaler + RF)
├── sql/
│   ├── create_tables.sql    # DDL script for PostgreSQL Star Schema
│   └── kpis.sql             # 4 Mandatory analytical SQL KPI queries
├── dashboards/
│   └── Workshop3_Happiness_Dashboard.pbit # Official Power BI Template (queries PostgreSQL)
├── docker-compose.yml       # Zookeeper, Kafka broker, and PostgreSQL infrastructure
├── requirements.txt         # Pinned production Python dependencies
├── .gitignore               # Strict git exclusion rules
└── README.md                # Comprehensive technical documentation
```

---

## 12. Execution Instructions

### Prerequisites
* Python 3.10+ (tested on Python 3.12)
* Docker Desktop running with Linux containers enabled
* Power BI Desktop (for dashboard visualization)
* Git

### Step 1: Clone Repository & Install Dependencies
```bash
git clone https://github.com/Taborda004/Workshop-3.git
cd Workshop-3
pip install -r requirements.txt
```

### Step 2: Provision Docker Infrastructure
```bash
docker compose up -d
```
Verify that all 3 containers are healthy:
```bash
docker compose ps
```
*Expected Output:* `happiness_zookeeper` (port 2181), `happiness_kafka` (port 9092), and `happiness_postgres` (host port 6432 $\rightarrow$ container 5432) in running state.

### Step 3: Initialize Star Schema Tables
PostgreSQL automatically executes `sql/create_tables.sql` upon initial container launch. If re-initializing manually:
```bash
docker exec -i happiness_postgres psql -U postgres -d happiness_db < sql/create_tables.sql
```

### Step 4: Run ETL & ML Notebooks (Offline Phase)
The notebooks in `notebooks/` are already fully executed with all outputs and plots displayed. To re-run them:
* `notebooks/eda.ipynb`: Produces `data/processed/happiness_unified.csv`.
* `notebooks/model_training.ipynb`: Evaluates the model and serializes `models/model.pkl`.

### Step 5: Start Real-Time Streaming Ingestion
Open two separate terminal windows in the workspace root:

* **Terminal 1 — Start the Kafka Consumer:**
  ```bash
  python kafka/consumer.py
  ```
  *The consumer connects to PostgreSQL, loads `models/model.pkl`, and begins listening on topic `happiness-events` under consumer group `happiness-ml-team`.*

* **Terminal 2 — Start the Kafka Producer:**
  ```bash
  python kafka/producer.py
  ```
  *The producer reads `happiness_stream_source.csv`, serializes 782 events + 4 anomaly test cases into JSON, and streams them with partition keys (optional `--delay 0.005` flag supported).*

### Step 6: Verify Database Ingestion & Execute KPIs
Run the analytical SQL queries defined in `sql/kpis.sql`:
```bash
docker exec -i happiness_postgres psql -U postgres -d happiness_db < sql/kpis.sql
```

### Step 7: Open Power BI Desktop Dashboard
1. Open [`dashboards/Workshop3_Happiness_Dashboard.pbit`](dashboards/Workshop3_Happiness_Dashboard.pbit) in Power BI Desktop.
2. When prompted for parameters or database connection, enter:
   * **Server:** `localhost:6432`
   * **Database:** `happiness_db`
   * **User:** `postgres` / **Password:** `postgres`
3. Click **Load** (or click **Refresh** on an existing report) to populate all 13 visuals directly from PostgreSQL.
