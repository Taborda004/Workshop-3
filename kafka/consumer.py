"""
Kafka Consumer for World Happiness Real-Time ML Inference & Storage
Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
Author: Mauricio Taborda

Requirements fulfilled:
1. Consumer group: 'happiness-ml-team'.
2. Receives streaming events from 'happiness-events'.
3. Stores raw incoming event into PostgreSQL 'raw_happiness_events' BEFORE any transformation.
4. Validates event against the Event Contract (Schema, Types, Values).
5. Fault-tolerant: Invalid events are flagged and stored without crashing the pipeline.
6. Real-time ML inference with models/model.pkl and calculation of prediction error.
7. Inserts predictions and updates dimensions in PostgreSQL Star Schema (fact_predictions).
"""

import os
import sys
import json
import argparse
from datetime import datetime
import joblib
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


DEFAULT_TOPIC = 'happiness-events'
DEFAULT_BOOTSTRAP = 'localhost:9092'
DEFAULT_CONSUMER_GROUP = 'happiness-ml-team'
DEFAULT_DB_CONFIG = {
    'host': 'localhost',
    'port': 6432,
    'dbname': 'happiness_db',
    'user': 'postgres',
    'password': 'postgres'
}

REQUIRED_CONTRACT_FIELDS = [
    'event_id', 'country', 'year', 'gdp', 'family',
    'health', 'freedom', 'generosity', 'corruption',
    'actual_happiness_score', 'event_time'
]

NUMERIC_FIELDS = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
MODEL_FEATURE_ORDER = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']


def get_project_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(current_dir, '..'))


def get_db_connection(db_config=DEFAULT_DB_CONFIG):
    """Establishes and returns a psycopg2 database connection."""
    return psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )


def load_ml_model(model_path=None):
    """Loads and returns the serialized scikit-learn pipeline."""
    if model_path is None:
        model_path = os.path.join(get_project_root(), 'models', 'model.pkl')
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model artifact not found at {model_path}. Train model first.")
    pipeline = joblib.load(model_path)
    print(f"[CONSUMER] Loaded ML pipeline from {model_path}")
    return pipeline


def validate_event_contract(payload):
    """
    Validates the event payload against the contract.
    Returns: (is_valid, status, error_message, parsed_data)
    """
    if not isinstance(payload, dict):
        return False, 'INVALID_SCHEMA', 'Payload is not a valid JSON object', None

    # 1. Required Fields Check
    missing_fields = [f for f in REQUIRED_CONTRACT_FIELDS if f not in payload]
    if missing_fields:
        return False, 'INVALID_SCHEMA', f"Missing required contract fields: {missing_fields}", None

    # 2. Country validation
    country = str(payload.get('country', '')).strip()
    if not country:
        return False, 'INVALID_VALUES', "Field 'country' must be a non-empty string", None

    # 3. Year validation
    try:
        year = int(payload['year'])
        if year < 1900 or year > 2100:
            return False, 'INVALID_VALUES', f"Field 'year' value ({year}) is outside realistic bounds", None
    except (ValueError, TypeError):
        return False, 'INVALID_VALUES', f"Field 'year' must be an integer, got: {payload.get('year')}", None

    # 4. Numerical Features & Actual Score Validation
    parsed_nums = {}
    for col in NUMERIC_FIELDS + ['actual_happiness_score']:
        val = payload.get(col)
        try:
            val_float = float(val)
            if np.isnan(val_float) or np.isinf(val_float):
                return False, 'INVALID_VALUES', f"Field '{col}' has invalid float value: {val}", None
            parsed_nums[col] = val_float
        except (ValueError, TypeError):
            return False, 'INVALID_VALUES', f"Field '{col}' must be numeric, got: {val}", None

    # 5. Timestamp validation
    event_time_raw = str(payload.get('event_time', ''))
    try:
        event_dt = datetime.fromisoformat(event_time_raw)
    except Exception as e:
        return False, 'INVALID_SCHEMA', f"Invalid ISO-8601 timestamp in 'event_time': {event_time_raw} ({e})", None

    parsed_data = {
        'event_id': str(payload['event_id']),
        'country': country,
        'year': year,
        'features': parsed_nums,
        'event_time': event_dt
    }
    return True, 'VALID', None, parsed_data


# Load country-to-region reference map from processed dataset if available
COUNTRY_REGION_MAP = {}
try:
    ref_path = os.path.join(get_project_root(), 'data', 'processed', 'happiness_unified.csv')
    if os.path.exists(ref_path):
        ref_df = pd.read_csv(ref_path)
        COUNTRY_REGION_MAP = ref_df[['country', 'region']].drop_duplicates().set_index('country')['region'].to_dict()
except Exception:
    pass


def get_or_create_country(cur, country_name):
    """Upserts dim_country and returns country_id."""
    region = COUNTRY_REGION_MAP.get(country_name, 'Other')
    cur.execute("""
        INSERT INTO dim_country (country_name, region)
        VALUES (%s, %s)
        ON CONFLICT (country_name) DO UPDATE 
        SET region = COALESCE(dim_country.region, EXCLUDED.region)
        RETURNING country_id;
    """, (country_name, region))
    return cur.fetchone()[0]


def get_or_create_date(cur, year, event_time):
    """Upserts dim_date and returns date_id."""
    month = event_time.month
    day = event_time.day
    hour = event_time.hour
    minute = event_time.minute
    
    cur.execute("""
        INSERT INTO dim_date (year, event_time, month, day, hour, minute)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (year, event_time) DO UPDATE
        SET event_time = EXCLUDED.event_time
        RETURNING date_id;
    """, (year, event_time, month, day, hour, minute))
    return cur.fetchone()[0]


def run_consumer(
    topic=DEFAULT_TOPIC,
    bootstrap_servers=DEFAULT_BOOTSTRAP,
    consumer_group=DEFAULT_CONSUMER_GROUP,
    db_config=DEFAULT_DB_CONFIG,
    max_messages=None,
    timeout_ms=60000,
    auto_offset_reset='earliest'
):
    print(f"\n========================================================")
    print(f"Starting Kafka Consumer -> Topic: '{topic}' | Group: '{consumer_group}'")
    print(f"PostgreSQL target: {db_config['host']}:{db_config['port']}/{db_config['dbname']}")
    print(f"Auto Offset Reset: {auto_offset_reset} | Timeout: {timeout_ms}ms")
    print(f"========================================================\n")

    pipeline = load_ml_model()
    conn = get_db_connection(db_config)
    conn.autocommit = False
    cur = conn.cursor()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(','),
        group_id=consumer_group,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
    )

    processed_count = 0
    valid_count = 0
    invalid_count = 0

    try:
        for msg in consumer:
            processed_count += 1
            payload = msg.value

            # Extract basic identifiers if available
            event_id = payload.get('event_id') if isinstance(payload, dict) else None
            country_raw = payload.get('country') if isinstance(payload, dict) else None
            year_raw = payload.get('year') if isinstance(payload, dict) else None
            year_val = int(year_raw) if str(year_raw).isdigit() else None

            # -----------------------------------------------------------------
            # Step 1: Immediate Landing into raw_happiness_events BEFORE any logic
            # -----------------------------------------------------------------
            cur.execute("""
                INSERT INTO raw_happiness_events (
                    event_id, country, year, raw_payload, processing_status
                ) VALUES (%s, %s, %s, %s, 'RECEIVED')
                RETURNING raw_event_id;
            """, (event_id, country_raw, year_val, Json(payload)))
            raw_event_id = cur.fetchone()[0]

            # -----------------------------------------------------------------
            # Step 2: Event Contract Validation
            # -----------------------------------------------------------------
            is_valid, status, error_msg, parsed = validate_event_contract(payload)

            if not is_valid:
                # Update status of raw record
                invalid_count += 1
                cur.execute("""
                    UPDATE raw_happiness_events
                    SET processing_status = %s, error_message = %s
                    WHERE raw_event_id = %s;
                """, (status, error_msg, raw_event_id))
                conn.commit()

                print(f"[CONSUMER-WARNING] Invalid event #{processed_count} (id={event_id}): "
                      f"Status={status} | Reason: {error_msg}")
                continue  # Gracefully skip prediction without crashing!

            # -----------------------------------------------------------------
            # Step 3: ML Inference & Dimensional Star Schema Insertion
            # -----------------------------------------------------------------
            try:
                # Update raw status to VALID
                cur.execute("""
                    UPDATE raw_happiness_events
                    SET processing_status = 'VALID'
                    WHERE raw_event_id = %s;
                """, (raw_event_id,))

                # Resolve Dimensions
                country_id = get_or_create_country(cur, parsed['country'])
                date_id = get_or_create_date(cur, parsed['year'], parsed['event_time'])

                # Format feature vector for ML model
                feat_dict = parsed['features']
                feature_row = [[
                    feat_dict['gdp'],
                    feat_dict['family'],
                    feat_dict['health'],
                    feat_dict['freedom'],
                    feat_dict['generosity'],
                    feat_dict['corruption']
                ]]
                feature_df = pd.DataFrame(feature_row, columns=MODEL_FEATURE_ORDER)

                # Predict
                predicted_score = float(pipeline.predict(feature_df)[0])
                actual_score = float(feat_dict['actual_happiness_score'])
                prediction_error = float(actual_score - predicted_score)

                # Insert into fact_predictions
                cur.execute("""
                    INSERT INTO fact_predictions (
                        raw_event_id, country_id, date_id,
                        gdp, family, health, freedom, generosity, corruption,
                        actual_score, predicted_score, prediction_error,
                        event_time, prediction_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING prediction_id;
                """, (
                    raw_event_id, country_id, date_id,
                    feat_dict['gdp'], feat_dict['family'], feat_dict['health'],
                    feat_dict['freedom'], feat_dict['generosity'], feat_dict['corruption'],
                    actual_score, predicted_score, prediction_error,
                    parsed['event_time']
                ))
                prediction_id = cur.fetchone()[0]
                conn.commit()
                valid_count += 1

                if valid_count % 50 == 0 or valid_count <= 5:
                    print(f"[CONSUMER] Processed #{valid_count} (pred_id={prediction_id}): "
                          f"Country={parsed['country']} | Actual={actual_score:.2f} | "
                          f"Pred={predicted_score:.2f} | Error={prediction_error:+.3f}")

            except Exception as ml_err:
                conn.rollback()
                cur.execute("""
                    UPDATE raw_happiness_events
                    SET processing_status = 'PREDICTION_ERROR', error_message = %s
                    WHERE raw_event_id = %s;
                """, (str(ml_err), raw_event_id))
                conn.commit()
                print(f"[CONSUMER-ERROR] ML inference failure on event {raw_event_id}: {ml_err}")

            if max_messages and processed_count >= max_messages:
                print(f"\n[CONSUMER] Reached requested max_messages limit ({max_messages}). Stopping.")
                break

    except KeyboardInterrupt:
        print("\n[CONSUMER] Consumer loop interrupted by user.")
    finally:
        cur.close()
        conn.close()
        consumer.close()
        print(f"\n========================================================")
        print(f"[CONSUMER SUMMARY] Total Received: {processed_count} | Valid & Predicted: {valid_count} | Invalid Filtered: {invalid_count}")
        print(f"========================================================\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Consumer for Real-Time ML Predictions")
    parser.add_argument('--topic', default=DEFAULT_TOPIC, help='Kafka topic to consume')
    parser.add_argument('--bootstrap', default=DEFAULT_BOOTSTRAP, help='Kafka bootstrap servers')
    parser.add_argument('--group', default=DEFAULT_CONSUMER_GROUP, help='Consumer group id')
    parser.add_argument('--max-messages', type=int, default=None, help='Stop after N messages')
    parser.add_argument('--timeout-ms', type=int, default=60000, help='Polling timeout in ms')
    args = parser.parse_args()

    run_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap,
        consumer_group=args.group,
        max_messages=args.max_messages,
        timeout_ms=args.timeout_ms
    )
