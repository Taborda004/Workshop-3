-- ============================================================================
-- Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka & Machine Learning
-- Database Schema: PostgreSQL Star Schema for Real-Time Prediction Analytics
-- Author: Mauricio Taborda
-- ============================================================================

-- Drop existing tables in reverse dependency order for idempotency
DROP TABLE IF EXISTS fact_predictions CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;
DROP TABLE IF EXISTS raw_happiness_events CASCADE;

-- ----------------------------------------------------------------------------
-- 1. Raw Staging Table: Complete Ingestion Audit & Fault Tolerance
-- Every incoming Kafka event is persisted here BEFORE schema validation or ML inference.
-- Supports full auditability, fault tolerance, and replayability.
-- ----------------------------------------------------------------------------
CREATE TABLE raw_happiness_events (
    raw_event_id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    country VARCHAR(100),
    year INT,
    raw_payload JSONB NOT NULL,
    processing_status VARCHAR(50) NOT NULL DEFAULT 'RECEIVED',
    error_message TEXT,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for filtering processing statuses (VALID, INVALID_SCHEMA, INVALID_VALUES, PREDICTION_ERROR)
CREATE INDEX idx_raw_processing_status ON raw_happiness_events(processing_status);
CREATE INDEX idx_raw_country ON raw_happiness_events(country);
CREATE INDEX idx_raw_event_id ON raw_happiness_events(event_id);

-- ----------------------------------------------------------------------------
-- 2. Dimension Table: dim_country
-- Stores standardized country metadata and geographic regions.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL,
    region VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_country_name ON dim_country(country_name);

-- ----------------------------------------------------------------------------
-- 3. Dimension Table: dim_date
-- Stores temporal dimensions extracted from historical observation year and simulated event timestamp.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    month INT,
    day INT,
    hour INT,
    minute INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_date UNIQUE (year, event_time)
);

CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_event_time ON dim_date(event_time);

-- ----------------------------------------------------------------------------
-- 4. Fact Table: fact_predictions
-- Stores valid ML predictions, error metrics, and foreign keys to raw audit & dimensions.
-- ----------------------------------------------------------------------------
CREATE TABLE fact_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    raw_event_id BIGINT NOT NULL REFERENCES raw_happiness_events(raw_event_id) ON DELETE CASCADE,
    country_id INT NOT NULL REFERENCES dim_country(country_id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dim_date(date_id) ON DELETE CASCADE,
    
    -- Explanatory Features
    gdp DOUBLE PRECISION,
    family DOUBLE PRECISION,
    health DOUBLE PRECISION,
    freedom DOUBLE PRECISION,
    generosity DOUBLE PRECISION,
    corruption DOUBLE PRECISION,
    
    -- Targets and ML Metrics
    actual_score DOUBLE PRECISION NOT NULL,
    predicted_score DOUBLE PRECISION NOT NULL,
    prediction_error DOUBLE PRECISION NOT NULL, -- actual_score - predicted_score
    
    -- Event Timestamps
    event_time TIMESTAMP NOT NULL,
    prediction_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_country_id ON fact_predictions(country_id);
CREATE INDEX idx_fact_date_id ON fact_predictions(date_id);
CREATE INDEX idx_fact_raw_event_id ON fact_predictions(raw_event_id);
CREATE INDEX idx_fact_prediction_timestamp ON fact_predictions(prediction_timestamp);
