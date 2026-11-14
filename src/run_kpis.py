"""
KPI Execution and Analytical Dashboard Verification Script
Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
Author: Mauricio Taborda

Executes the 4 mandatory KPIs + Raw Ingestion Health Audit directly on PostgreSQL.
"""

import os
import psycopg2
import pandas as pd


DB_CONFIG = {
    'host': 'localhost',
    'port': 6432,
    'dbname': 'happiness_db',
    'user': 'postgres',
    'password': 'postgres'
}


def run_kpi_queries(output_dir='dashboards'):
    os.makedirs(output_dir, exist_ok=True)
    conn = psycopg2.connect(**DB_CONFIG)

    queries = {
        "KPI 1.1 - Global Error Summary": """
            SELECT 
                COUNT(*) AS total_predictions,
                ROUND(AVG(ABS(prediction_error))::numeric, 4) AS global_mae,
                ROUND(SQRT(AVG(POWER(prediction_error, 2)))::numeric, 4) AS global_rmse,
                ROUND(AVG(prediction_error)::numeric, 4) AS mean_bias,
                ROUND(MIN(prediction_error)::numeric, 4) AS min_error,
                ROUND(MAX(prediction_error)::numeric, 4) AS max_error
            FROM fact_predictions;
        """,
        "KPI 1.2 - Error by Geographic Region": """
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
        """,
        "KPI 2 - Predictions by Country (Top 15 sample)": """
            SELECT 
                c.country_name,
                c.region,
                COUNT(f.prediction_id) AS total_predictions,
                ROUND(AVG(f.actual_score)::numeric, 3) AS avg_actual_score,
                ROUND(AVG(f.predicted_score)::numeric, 3) AS avg_predicted_score,
                ROUND(AVG(ABS(f.prediction_error))::numeric, 3) AS country_mae
            FROM fact_predictions f
            JOIN dim_country c ON f.country_id = c.country_id
            GROUP BY c.country_name, c.region
            ORDER BY total_predictions DESC, country_mae ASC
            LIMIT 15;
        """,
        "KPI 3 - Predicted vs Actual Score (Sample 10)": """
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
                END AS error_bracket
            FROM fact_predictions f
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            ORDER BY f.prediction_id ASC
            LIMIT 10;
        """,
        "KPI 4 - Prediction Trends Over Time (By Year)": """
            SELECT 
                d.year,
                COUNT(f.prediction_id) AS events_processed,
                ROUND(AVG(f.actual_score)::numeric, 3) AS avg_actual_score,
                ROUND(AVG(f.predicted_score)::numeric, 3) AS avg_predicted_score,
                ROUND(AVG(ABS(f.prediction_error))::numeric, 4) AS yearly_mae,
                ROUND(SQRT(AVG(POWER(f.prediction_error, 2)))::numeric, 4) AS yearly_rmse
            FROM fact_predictions f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year
            ORDER BY d.year ASC;
        """,
        "Raw Ingestion Audit & Fault Tolerance": """
            SELECT 
                processing_status,
                COUNT(*) AS total_events,
                ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS percentage
            FROM raw_happiness_events
            GROUP BY processing_status
            ORDER BY total_events DESC;
        """
    }

    report_md = ["# LIVE POSTGRESQL KPI EXECUTION REPORT\n"]

    for title, sql in queries.items():
        print(f"\n========================================================")
        print(f"Executing: {title}")
        print(f"========================================================")
        df = pd.read_sql_query(sql, conn)
        print(df.to_string(index=False))
        
        # Save CSV
        safe_name = title.lower().replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '')
        csv_path = os.path.join(output_dir, f"{safe_name}.csv")
        df.to_csv(csv_path, index=False)
        
        # Append to Markdown
        report_md.append(f"## {title}\n")
        report_md.append(df.to_markdown(index=False))
        report_md.append("\n\n")

    conn.close()

    report_path = os.path.join(output_dir, "kpis_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_md))
    print(f"\n[KPI EXECUTION] Full Markdown report saved to {report_path}")


if __name__ == '__main__':
    run_kpi_queries()
