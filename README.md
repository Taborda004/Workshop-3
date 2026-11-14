# Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
**Course:** ETL (G01) — Faculty of Engineering and Basic Sciences  
**Academic Program:** Data Engineering and Artificial Intelligence  
**Author:** Mauricio Taborda (Taborda004)  
*Note: For the comprehensive Spanish version of this documentation, see [README_ES.md](README_ES.md).*

---

## 1. Project Overview
This project transitions traditional batch ETL pipelines to an event-driven **Streaming ETL architecture with real-time Machine Learning inference**. The system ingests heterogeneous World Happiness datasets (2015–2019), harmonizes their conflicting schemas, trains a leakage-free Scikit-Learn regression pipeline, streams records through **Apache Kafka** using a strict JSON Event Contract, applies real-time ML scoring, and loads predictions and dimensional metadata into a **PostgreSQL Star Schema**.

---

## 2. End-to-End System Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        OFFLINE BATCH ML PIPELINE                       │
└────────────────────────────────────────────────────────────────────────┘
  Historical CSVs (2015.csv - 2019.csv in data/raw/)
            │
            ▼
  Auditing & Schema Harmonization (notebooks/eda.ipynb, src/harmonization.py)
            │
            ▼
  Harmonized Master Dataset (data/processed/happiness_unified.csv)
            │
            ▼
  Leakage-Free Train/Test Split (70% Train / 30% Test, random_state=42)
            │
            ▼
  Scikit-Learn Pipeline (SimpleImputer + StandardScaler + RandomForestRegressor)
            │
            ▼
  Model Evaluation (MAE: 0.4077 | RMSE: 0.5239 | R²: 0.7802)
            │
            ▼
  Serialized Pipeline Artifact (models/model.pkl)
            │
            ▼
  Streaming Event Source (data/streaming/happiness_stream_source.csv)

┌────────────────────────────────────────────────────────────────────────┐
│                   REAL-TIME STREAMING ETL & ANALYTICS                  │
└────────────────────────────────────────────────────────────────────────┘
  Streaming Source (data/streaming/happiness_stream_source.csv)
            │
            ▼
  Kafka Producer (kafka/producer.py)
  - Emits JSON records one-by-one with configurable delay
  - Message Key: 'country' (UTF-8 bytes)
  - Topic: 'happiness-events' (3 partitions, replication factor 1)
  - Injects controlled edge-case malformed events to verify fault tolerance
            │
            ▼
  Apache Kafka Broker (Confluent Platform in Docker Compose)
            │
            ▼
  Kafka Consumer (kafka/consumer.py) — Consumer Group: 'happiness-ml-team'
            │
            ├─► 1. IMMEDIATE AUDIT LANDING:
            │      Inserts raw message into PostgreSQL 'raw_happiness_events'
            │      with status 'RECEIVED' before any validation or inference.
            │
            ├─► 2. EVENT CONTRACT VALIDATION:
            │      - Checks required fields (event_id, country, year, features, time)
            │      - Checks data types and numeric ranges
            │      - If Invalid: updates status ('INVALID_SCHEMA' or 'INVALID_VALUES')
            │        and continues without crashing!
            │
            └─► 3. REAL-TIME ML INFERENCE & STAR SCHEMA LOADING:
                   - Updates status to 'VALID' in raw table
                   - Upserts country in 'dim_country' & date in 'dim_date'
                   - Formats feature vector and runs inference with 'models/model.pkl'
                   - Calculates prediction_error: actual_score - predicted_score
                   - Inserts fact record into 'fact_predictions' (with FKs)
            │
            ▼
  PostgreSQL 16 Analytical Database (Star Schema)
            │
            ▼
  SQL Analytics & Dashboards (sql/kpis.sql & dashboards/)
  - KPI 1: Mean Absolute Error (MAE) and RMSE globally and by region
  - KPI 2: Predictions volume and performance by country
  - KPI 3: Actual vs. Predicted scores and residual brackets
  - KPI 4: Temporal trends across observation years
```

---

## 3. Data Cleaning and Schema Harmonization Decisions

### Heterogeneous Raw Datasets
The 5 raw annual datasets (2015–2019) exhibited major schema shifts:
- **Column count variations:** 12 columns in 2015, 13 in 2016, 12 in 2017, and 9 in 2018–2019.
- **Naming inconsistencies:** e.g., `Happiness Score` (2015/16) vs. `Happiness.Score` (2017) vs. `Score` (2018/19); `Family` (2015-17) vs. `Social support` (2018/19); `Country` vs. `Country or region`.
- **Missing geographic attributes:** `Region` was present only in 2015 and 2016.
- **Missing values:** Across all 782 historical observations, exactly **one single missing value** existed (`Perceptions of corruption` for the `United Arab Emirates` in 2018).

### Technical Justification of Cleaning Actions:
1. **Target Schema:** Standardized to clean, lower-case `snake_case` matching the Kafka Event Contract: `country`, `region`, `year`, `actual_happiness_score`, `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`.
2. **Missing Value Imputation:** UAE's missing 2018 corruption value was imputed using the median of UAE's reported values in other years (0.3400), preserving longitudinal consistency rather than dropping the row or using an arbitrary global mean.
3. **Region Enrichment:** Built an invariant country-to-region reference map from 2015/2016 to enrich 2017–2019 observations, ensuring complete coverage in `dim_country`.

---

## 4. Feature Engineering & Strict Target Leakage Prevention

### Preventing Target Leakage:
The rubric strictly penalizes models using features derived from the target variable. We systematically eliminated:
- `Happiness Rank` / `Overall rank`: Directly computed by ranking the happiness score. In a streaming production pipeline, ranking is impossible to know prior to predicting the score.
- `Standard Error`, `Whisker.high`, `Whisker.low`: Confidence interval boundaries derived from the sample variance of the happiness score.
- `Dystopia Residual`: In the World Happiness Report formula, $Score = \sum(\text{factors}) + \text{Dystopia Residual}$. Including the residual reduces regression to an artificial mathematical sum.

### Final Selected Feature Vector:
The regression model strictly consumes 6 independent socio-economic indicators:
$$\mathbf{X} = [\text{gdp}, \text{family}, \text{health}, \text{freedom}, \text{generosity}, \text{corruption}]$$
$$\mathbf{y} = \text{actual\_happiness\_score}$$

### Pipeline Implementation:
Using `scikit-learn.pipeline.Pipeline`, data preprocessing and model inference are serialized together:
1. `SimpleImputer(strategy='median')`: Ensures real-time fault tolerance against missing numerical values during streaming.
2. `StandardScaler()`: Standardizes scale variance across indicators.
3. `RandomForestRegressor(n_estimators=100, max_depth=10, min_samples_split=4, random_state=42)`: Models complex non-linear interactions.

### Evaluation Metrics (30% Test Set, 235 Records):
- **MAE (Mean Absolute Error):** `0.4077` (Average deviation of ~0.4 points on a 1-to-10 scale).
- **RMSE (Root Mean Squared Error):** `0.5239` (Low penalty for outlier deviations).
- **$R^2$ Score:** `0.7802` (Explains >78% of total variance).

---

## 5. Kafka Pipeline & Event Contract Specification

### Topic Configuration
- **Topic Name:** `happiness-events`
- **Partitions:** 3 (allows parallel consumer scaling and key-based hash routing).
- **Replication Factor:** 1 (single-broker local environment).
- **Consumer Group:** `happiness-ml-team`

### Event Contract (JSON Specification)
Each message published by `kafka/producer.py` uses `country` as the Kafka message key and adheres strictly to:

```json
{
  "event_id": "E0001",
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2,
  "event_time": "2026-11-08T20:30:00"
}
```

### Event Contract Validation Rules:
1. `event_id`: Non-empty string; unique event identifier.
2. `country`: Non-empty string; matches Kafka event key.
3. `year`: Integer between 1900 and 2100.
4. `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`: Valid finite numerical values.
5. `actual_happiness_score`: Numeric value used **strictly for post-inference evaluation** (never passed to the model).
6. `event_time`: Valid ISO-8601 formatted timestamp string.

### Fault Tolerance & Resiliency:
- **First-Landing Audit:** Every incoming Kafka message is immediately saved to `raw_happiness_events` with status `RECEIVED`.
- **Invalid Records:** If an event violates schema or value rules, it is updated to `INVALID_SCHEMA` or `INVALID_VALUES` with the explicit error explanation in `error_message`.
- **Pipeline Continuity:** The consumer skips prediction for invalid records and **never crashes**, ensuring zero downtime.

---

## 6. PostgreSQL Database Schema (Star Schema)

The database schema in `sql/create_tables.sql` implements a formal dimensional model:

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
                           │ 1:1
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

---

## 7. Analytical KPIs and Results

The SQL queries in `sql/kpis.sql` execute directly against PostgreSQL:

### KPI 1: Average Prediction Error
- **Global MAE:** `0.2427` points
- **Global RMSE:** `0.3434` points
- **Mean Bias:** `-0.0002` (virtually zero systematic over/under-estimation)
- **Regional Error Breakdown:**
  - *Lowest Error:* Australia & New Zealand (MAE: `0.0786`), North America (MAE: `0.0880`), Western Europe (MAE: `0.1997`).
  - *Moderate Error:* Central & Eastern Europe (MAE: `0.2179`), Middle East & North Africa (MAE: `0.2109`).
  - *Higher Variance:* Latin America (MAE: `0.3128`), Southern Asia (MAE: `0.3229`).

### KPI 2: Predictions by Country
Processed 5 annual observations for over 150 nations with average errors consistently below 0.10 points for high-stability nations (e.g., Australia MAE: 0.054, Uruguay MAE: 0.057, Canada MAE: 0.064).

### KPI 3: Predicted vs. Actual Score (Precision Brackets)
- **High Precision ($|\text{Error}| \le 0.25$):** >68% of streamed records.
- **Acceptable ($0.25 < |\text{Error}| \le 0.50$):** >23% of streamed records.
- **Divergent ($|\text{Error}| > 0.50$):** <9% of records.

### KPI 4: Temporal Prediction Trends (2015–2019)
- 2015: 158 events | Actual Avg: 5.376 | Pred Avg: 5.350 | MAE: 0.2678
- 2016: 157 events | Actual Avg: 5.382 | Pred Avg: 5.296 | MAE: 0.2444
- 2017: 155 events | Actual Avg: 5.354 | Pred Avg: 5.414 | MAE: 0.2257
- 2018: 156 events | Actual Avg: 5.376 | Pred Avg: 5.392 | MAE: 0.2223
- 2019: 156 events | Actual Avg: 5.407 | Pred Avg: 5.446 | MAE: 0.2531

### Ingestion Resiliency Audit
- Total Events Ingested: **786**
- `VALID` Events: 782 (99.49%)
- `INVALID_SCHEMA` Filtered: 2 (0.25%)
- `INVALID_VALUES` Filtered: 2 (0.25%)
- **System Downtime: 0 crashes.**

---

## 8. Repository Structure

```
workshop_3/
├── data/
│   ├── raw/                 # Original 2015.csv - 2019.csv
│   ├── processed/           # happiness_unified.csv
│   └── streaming/           # happiness_stream_source.csv
├── notebooks/
│   ├── eda.ipynb            # Step 1 & 2: Profiling, cleaning, and schema proposal
│   └── model_training.ipynb # Step 3 & 4: Feature engineering, pipeline, evaluation
├── src/
│   ├── harmonization.py     # Schema standardization and cleaning module
│   ├── train_model.py       # Standalone training and pipeline serialization
│   ├── run_kpis.py          # PostgreSQL KPI query execution and markdown report
│   ├── generate_dashboard_charts.py # Publication-quality visual figures
│   └── verify_db.py         # PostgreSQL schema and constraint validation
├── kafka/
│   ├── producer.py          # Kafka streaming producer with error injection
│   └── consumer.py          # Fault-tolerant real-time inference consumer
├── models/
│   └── model.pkl            # Serialized Scikit-Learn Pipeline
├── sql/
│   ├── create_tables.sql    # PostgreSQL Star Schema DDL
│   └── kpis.sql             # 4 Mandatory analytical SQL KPI queries
├── dashboards/              # Power BI (.pbids, DAX, M-queries), KPI charts, and dashboard.html
│   ├── happiness_analytics.pbids # 1-Click Power BI Data Source Connection
│   ├── POWER_BI_GUIDE.md    # Step-by-step Power BI setup guide
│   ├── power_bi_dax_measures.dax # DAX formulas for all 4 KPIs
│   ├── power_bi_queries.m   # Power Query M scripts for PostgreSQL
│   ├── dashboard.html       # Interactive standalone browser dashboard
│   ├── kpis_report.md       # Comprehensive analytical KPI report
│   └── *.png, *.csv         # High-resolution figures and data exports
├── docker-compose.yml       # Zookeeper, Kafka, PostgreSQL infrastructure
├── requirements.txt         # Pinned project dependencies
├── README.md                # Comprehensive technical documentation in English
├── README_ES.md             # Complete technical documentation in Spanish
└── GUIA_EXPLICACION_PROYECTO.md # Pedagogical defense guide in Spanish (30% grade)
```

---

## 9. Step-by-Step Execution Guide

### Prerequisites
- Python 3.12+
- Docker & Docker Compose

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Launch Infrastructure (Docker Compose)
```bash
docker compose up -d
```
*Kafka is available at `localhost:9092` and PostgreSQL at `localhost:6432` (`happiness_db`).*

### 3. Verify Database Tables
```bash
python src/verify_db.py
```

### 4. Execute Data Harmonization & EDA
```bash
python src/harmonization.py
jupyter nbconvert --to notebook --execute --inplace notebooks/eda.ipynb
```

### 5. Train & Serialize ML Pipeline
```bash
python src/train_model.py
jupyter nbconvert --to notebook --execute --inplace notebooks/model_training.ipynb
```

### 6. Run Real-Time Streaming Pipeline
In Terminal 1 (Start Consumer):
```bash
python kafka/consumer.py --timeout-ms 25000
```

In Terminal 2 (Start Producer):
```bash
python kafka/producer.py --delay 0.005
```

### 7. Run SQL KPIs and Generate Visual Dashboards
```bash
python src/run_kpis.py
python src/generate_dashboard_charts.py
```
All KPI reports and high-resolution visuals are output to `dashboards/`.
