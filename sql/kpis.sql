-- ============================================================================
-- Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka & Machine Learning
-- File: sql/kpis.sql
-- Description: 4 Mandatory KPIs for Real-Time Streaming Analytics & Dashboards
-- Author: Mauricio Taborda
-- ============================================================================

-- ----------------------------------------------------------------------------
-- KPI 1: AVERAGE PREDICTION ERROR (OVERALL AND BY REGION)
-- Purpose: Measure real-time accuracy and bias (MAE, RMSE, Mean Error).
-- ----------------------------------------------------------------------------
-- 1.1 Global Metric Summary
SELECT 
    COUNT(*) AS total_predictions,
    ROUND(AVG(ABS(prediction_error))::numeric, 4) AS global_mae,
    ROUND(SQRT(AVG(POWER(prediction_error, 2)))::numeric, 4) AS global_rmse,
    ROUND(AVG(prediction_error)::numeric, 4) AS mean_prediction_bias,
    ROUND(MIN(prediction_error)::numeric, 4) AS min_error,
    ROUND(MAX(prediction_error)::numeric, 4) AS max_error
FROM fact_predictions;

-- 1.2 Average Prediction Error Grouped by Geographic Region
SELECT 
    c.region,
    COUNT(f.prediction_id) AS prediction_count,
    ROUND(AVG(ABS(f.prediction_error))::numeric, 4) AS region_mae,
    ROUND(SQRT(AVG(POWER(f.prediction_error, 2)))::numeric, 4) AS region_rmse,
    ROUND(AVG(f.actual_score)::numeric, 2) AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::numeric, 2) AS avg_predicted_score
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.region
ORDER BY region_mae ASC;


-- ----------------------------------------------------------------------------
-- KPI 2: PREDICTIONS BY COUNTRY (VOLUME & ERROR PROFILES)
-- Purpose: Distribution of events processed per country, identifying countries
--          with highest prediction consistency or highest divergence.
-- ----------------------------------------------------------------------------
SELECT 
    c.country_name,
    c.region,
    COUNT(f.prediction_id) AS total_country_predictions,
    ROUND(AVG(f.actual_score)::numeric, 3) AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::numeric, 3) AS avg_predicted_score,
    ROUND(AVG(ABS(f.prediction_error))::numeric, 3) AS country_mae
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name, c.region
ORDER BY total_country_predictions DESC, country_mae ASC;


-- ----------------------------------------------------------------------------
-- KPI 3: PREDICTED VS ACTUAL SCORE (ACCURACY AUDIT & RESIDUALS)
-- Purpose: Granular comparison between ground-truth and streaming inference,
--          including relative percentage error and evaluation status.
-- ----------------------------------------------------------------------------
SELECT 
    f.prediction_id,
    c.country_name,
    d.year,
    ROUND(f.actual_score::numeric, 3) AS actual_score,
    ROUND(f.predicted_score::numeric, 3) AS predicted_score,
    ROUND(f.prediction_error::numeric, 3) AS error_residual,
    ROUND((ABS(f.prediction_error) / NULLIF(f.actual_score, 0) * 100)::numeric, 2) AS relative_error_pct,
    CASE 
        WHEN ABS(f.prediction_error) <= 0.25 THEN 'HIGH_PRECISION'
        WHEN ABS(f.prediction_error) <= 0.50 THEN 'ACCEPTABLE'
        ELSE 'DIVERGENT'
    END AS error_bracket,
    f.event_time
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY f.prediction_id ASC;


-- ----------------------------------------------------------------------------
-- KPI 4: PREDICTION TRENDS OVER TIME (ANNUAL EVOLUTION & STREAMING HEALTH)
-- Purpose: Monitor how model performance behaves across observation years
--          and detect temporal degradation or drift.
-- ----------------------------------------------------------------------------
SELECT 
    d.year,
    COUNT(f.prediction_id) AS events_processed,
    ROUND(AVG(f.actual_score)::numeric, 3) AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::numeric, 3) AS avg_predicted_score,
    ROUND(AVG(ABS(f.prediction_error))::numeric, 4) AS yearly_mae,
    ROUND(SQRT(AVG(POWER(f.prediction_error, 2)))::numeric, 4) AS yearly_rmse,
    MIN(f.prediction_timestamp) AS first_prediction_at,
    MAX(f.prediction_timestamp) AS last_prediction_at
FROM fact_predictions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year ASC;


-- ----------------------------------------------------------------------------
-- BONUS KPI: RAW EVENT INGESTION HEALTH & FAULT TOLERANCE AUDIT
-- Purpose: Validates that invalid records were safely landed and audited in raw
--          without crashing consumer processing.
-- ----------------------------------------------------------------------------
SELECT 
    processing_status,
    COUNT(*) AS total_events,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS percentage_of_total,
    MIN(ingestion_timestamp) AS first_ingested,
    MAX(ingestion_timestamp) AS last_ingested
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY total_events DESC;
