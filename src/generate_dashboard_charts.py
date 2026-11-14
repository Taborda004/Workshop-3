"""
Generate Visual Dashboard Figures for KPIs
Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
Author: Mauricio Taborda

Produces publication-grade dashboard figures for the 4 mandatory KPIs.
"""

import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


DB_CONFIG = {
    'host': 'localhost',
    'port': 6432,
    'dbname': 'happiness_db',
    'user': 'postgres',
    'password': 'postgres'
}


def create_dashboard_visuals(output_dir='dashboards'):
    os.makedirs(output_dir, exist_ok=True)
    conn = psycopg2.connect(**DB_CONFIG)

    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    plt.rcParams['font.sans-serif'] = 'DejaVu Sans'

    # -------------------------------------------------------------------------
    # Chart 1: KPI 1 - Regional MAE & RMSE
    # -------------------------------------------------------------------------
    df_reg = pd.read_sql_query("""
        SELECT 
            c.region,
            ROUND(AVG(ABS(f.prediction_error))::numeric, 4) AS region_mae,
            ROUND(SQRT(AVG(POWER(f.prediction_error, 2)))::numeric, 4) AS region_rmse
        FROM fact_predictions f
        JOIN dim_country c ON f.country_id = c.country_id
        GROUP BY c.region
        ORDER BY region_mae ASC;
    """, conn)

    fig, ax = plt.subplots(figsize=(10, 6))
    y_indices = range(len(df_reg))
    ax.barh([y - 0.2 for y in y_indices], df_reg['region_mae'], height=0.4, label='Regional MAE', color='#2b5c8f')
    ax.barh([y + 0.2 for y in y_indices], df_reg['region_rmse'], height=0.4, label='Regional RMSE', color='#e07a5f')
    ax.set_yticks(list(y_indices))
    ax.set_yticklabels(df_reg['region'])
    ax.set_xlabel('Prediction Error (Happiness Score units)')
    ax.set_title('KPI 1: Mean Absolute Error (MAE) and RMSE by Region', fontsize=13, fontweight='bold')
    ax.legend()
    plt.tight_layout()
    chart1_path = os.path.join(output_dir, 'kpi1_region_error.png')
    plt.savefig(chart1_path, dpi=300)
    plt.close()

    # -------------------------------------------------------------------------
    # Chart 2: KPI 3 - Actual vs Predicted Streaming Inferences
    # -------------------------------------------------------------------------
    df_preds = pd.read_sql_query("""
        SELECT actual_score, predicted_score, prediction_error,
               CASE 
                   WHEN ABS(prediction_error) <= 0.25 THEN 'High Precision (|err| <= 0.25)'
                   WHEN ABS(prediction_error) <= 0.50 THEN 'Acceptable (0.25 < |err| <= 0.50)'
                   ELSE 'Divergent (|err| > 0.50)'
               END AS precision_tier
        FROM fact_predictions;
    """, conn)

    fig, ax = plt.subplots(figsize=(8, 8))
    palette = {
        'High Precision (|err| <= 0.25)': '#2a9d8f',
        'Acceptable (0.25 < |err| <= 0.50)': '#e76f51',
        'Divergent (|err| > 0.50)': '#d62828'
    }
    sns.scatterplot(
        data=df_preds, x='actual_score', y='predicted_score',
        hue='precision_tier', palette=palette, alpha=0.75, s=40, ax=ax
    )
    min_s = min(df_preds['actual_score'].min(), df_preds['predicted_score'].min())
    max_s = max(df_preds['actual_score'].max(), df_preds['predicted_score'].max())
    ax.plot([min_s, max_s], [min_s, max_s], 'k--', lw=1.5, label='Ideal 1:1 Parity')
    ax.set_title('KPI 3: Streaming Inferences: Actual vs. Predicted Score', fontsize=13, fontweight='bold')
    ax.set_xlabel('Actual Ground-Truth Happiness Score')
    ax.set_ylabel('Real-Time Predicted Score')
    ax.legend(loc='upper left')
    plt.tight_layout()
    chart2_path = os.path.join(output_dir, 'kpi3_actual_vs_predicted_stream.png')
    plt.savefig(chart2_path, dpi=300)
    plt.close()

    # -------------------------------------------------------------------------
    # Chart 3: KPI 4 - Temporal Trends (2015 - 2019)
    # -------------------------------------------------------------------------
    df_time = pd.read_sql_query("""
        SELECT 
            d.year,
            ROUND(AVG(f.actual_score)::numeric, 3) AS avg_actual_score,
            ROUND(AVG(f.predicted_score)::numeric, 3) AS avg_predicted_score,
            ROUND(AVG(ABS(f.prediction_error))::numeric, 4) AS yearly_mae
        FROM fact_predictions f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year
        ORDER BY d.year ASC;
    """, conn)

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax2 = ax1.twinx()

    line1 = ax1.plot(df_time['year'], df_time['avg_actual_score'], marker='o', lw=2.5, color='#1d3557', label='Avg Actual Score')
    line2 = ax1.plot(df_time['year'], df_time['avg_predicted_score'], marker='s', lw=2.5, linestyle='--', color='#457b9d', label='Avg Predicted Score')
    bar1 = ax2.bar(df_time['year'], df_time['yearly_mae'], alpha=0.3, width=0.3, color='#e63946', label='Yearly MAE')

    ax1.set_xlabel('Observation Year')
    ax1.set_ylabel('Mean Happiness Score', color='#1d3557')
    ax2.set_ylabel('Mean Absolute Error (MAE)', color='#e63946')
    ax1.set_xticks(df_time['year'])
    ax1.set_title('KPI 4: Temporal Score Trends & Prediction Error (2015-2019)', fontsize=13, fontweight='bold')

    lines = line1 + line2 + [bar1]
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper center')
    plt.tight_layout()
    chart3_path = os.path.join(output_dir, 'kpi4_temporal_trends.png')
    plt.savefig(chart3_path, dpi=300)
    plt.close()

    # -------------------------------------------------------------------------
    # Chart 4: Raw Ingestion Health & Fault Tolerance
    # -------------------------------------------------------------------------
    df_status = pd.read_sql_query("""
        SELECT processing_status, COUNT(*) as count
        FROM raw_happiness_events
        GROUP BY processing_status;
    """, conn)

    fig, ax = plt.subplots(figsize=(7, 7))
    colors = ['#2a9d8f', '#f4a261', '#e76f51']
    wedges, texts, autotexts = ax.pie(
        df_status['count'], labels=df_status['processing_status'],
        autopct='%1.2f%%', startangle=140, colors=colors[:len(df_status)],
        explode=[0.05 if s != 'VALID' else 0 for s in df_status['processing_status']]
    )
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)
    ax.set_title('Kafka Ingestion Health: Contract Validation & Resiliency', fontsize=13, fontweight='bold')
    plt.tight_layout()
    chart4_path = os.path.join(output_dir, 'kpi_ingestion_health.png')
    plt.savefig(chart4_path, dpi=300)
    plt.close()

    conn.close()
    print("[CHARTS] Successfully generated all 4 KPI dashboard figures in:", output_dir)


if __name__ == '__main__':
    create_dashboard_visuals()
