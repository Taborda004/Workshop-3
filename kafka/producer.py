"""
Kafka Producer for World Happiness Streaming Simulation
Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
Author: Mauricio Taborda

Requirements fulfilled:
1. Reads records one by one from historical data.
2. Serializes events as JSON matching the strict Event Contract.
3. Uses 'country' as the Kafka message key.
4. Simulates real-time event arrival with a configurable delay.
5. Injects controlled malformed test events to demonstrate consumer fault tolerance.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer


DEFAULT_TOPIC = 'happiness-events'
DEFAULT_BOOTSTRAP = 'localhost:9092'


def get_project_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(current_dir, '..'))


def create_producer(bootstrap_servers=DEFAULT_BOOTSTRAP):
    """Initializes and returns a configured KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(','),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )


def generate_error_events():
    """Generates synthetic malformed events to test consumer fault-tolerance."""
    base_time = datetime.now().isoformat()
    return [
        {
            # Error 1: Missing required field 'gdp'
            "description": "SCHEMA ERROR: Missing 'gdp' field",
            "payload": {
                "event_id": "ERR-SCHEMA-001",
                "country": "Testland_InvalidSchema",
                "year": 2026,
                "family": 0.85,
                "health": 0.75,
                "freedom": 0.60,
                "generosity": 0.20,
                "corruption": 0.10,
                "actual_happiness_score": 5.5,
                "event_time": base_time
            }
        },
        {
            # Error 2: Empty country string
            "description": "VALUE ERROR: Empty country string",
            "payload": {
                "event_id": "ERR-VAL-002",
                "country": "",
                "year": 2026,
                "gdp": 1.10,
                "family": 0.80,
                "health": 0.70,
                "freedom": 0.50,
                "generosity": 0.20,
                "corruption": 0.05,
                "actual_happiness_score": 6.1,
                "event_time": base_time
            }
        },
        {
            # Error 3: Non-numeric corruption value
            "description": "VALUE ERROR: Non-numeric 'corruption' string value",
            "payload": {
                "event_id": "ERR-VAL-003",
                "country": "Corruptia",
                "year": 2026,
                "gdp": 0.95,
                "family": 0.70,
                "health": 0.65,
                "freedom": 0.40,
                "generosity": 0.15,
                "corruption": "UNKNOWN_CORRUPTION_STRING",
                "actual_happiness_score": 4.8,
                "event_time": base_time
            }
        },
        {
            # Error 4: Invalid timestamp format
            "description": "SCHEMA ERROR: Invalid ISO-8601 event_time",
            "payload": {
                "event_id": "ERR-SCHEMA-004",
                "country": "Timeless_Republic",
                "year": 2026,
                "gdp": 1.05,
                "family": 0.90,
                "health": 0.80,
                "freedom": 0.55,
                "generosity": 0.25,
                "corruption": 0.12,
                "actual_happiness_score": 6.3,
                "event_time": "NOT_A_VALID_DATE_STRING"
            }
        }
    ]


def run_producer(
    csv_path=None,
    topic=DEFAULT_TOPIC,
    bootstrap_servers=DEFAULT_BOOTSTRAP,
    delay=0.01,
    max_records=None,
    inject_errors=True
):
    root = get_project_root()
    if csv_path is None:
        csv_path = os.path.join(root, 'data', 'streaming', 'happiness_stream_source.csv')

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source streaming CSV not found at: {csv_path}")

    df = pd.read_csv(csv_path)
    total_available = len(df)
    limit = min(max_records, total_available) if max_records else total_available

    print(f"\n========================================================")
    print(f"Starting Kafka Producer -> Topic: '{topic}' | Bootstrap: '{bootstrap_servers}'")
    print(f"Dataset: {csv_path} ({total_available} total records)")
    print(f"Target streaming volume: {limit} records | Delay: {delay}s | Inject Errors: {inject_errors}")
    print(f"========================================================\n")

    producer = create_producer(bootstrap_servers)
    emitted_count = 0

    try:
        # Ingest valid records
        for idx in range(limit):
            row = df.iloc[idx]
            event_id = f"E{idx + 1:04d}"
            country = str(row['country']).strip()
            
            # Construct standard JSON event matching contract
            event_payload = {
                "event_id": event_id,
                "country": country,
                "year": int(row['year']),
                "gdp": round(float(row['gdp']), 4),
                "family": round(float(row['family']), 4),
                "health": round(float(row['health']), 4),
                "freedom": round(float(row['freedom']), 4),
                "generosity": round(float(row['generosity']), 4),
                "corruption": round(float(row['corruption']), 4),
                "actual_happiness_score": round(float(row['actual_happiness_score']), 4),
                "event_time": datetime.now().isoformat()
            }
            
            # Send with country as key
            future = producer.send(topic, key=country, value=event_payload)
            metadata = future.get(timeout=10)
            emitted_count += 1
            
            if emitted_count % 50 == 0 or emitted_count == limit or emitted_count <= 5:
                print(f"[PRODUCER] Sent Event #{emitted_count}: id={event_id} | country={country} "
                      f"| partition={metadata.partition} | offset={metadata.offset}")
                
            if delay > 0:
                time.sleep(delay)

        # Ingest controlled test error events if enabled
        if inject_errors:
            print("\n[PRODUCER] >>> Ingesting controlled error events to verify consumer fault-tolerance...")
            error_events = generate_error_events()
            for err in error_events:
                p = err['payload']
                key = p.get('country', 'UNKNOWN_KEY')
                future = producer.send(topic, key=key, value=p)
                meta = future.get(timeout=10)
                emitted_count += 1
                print(f"[PRODUCER-FAULT-INJECTION] Injected {err['description']} -> "
                      f"id={p['event_id']} | partition={meta.partition} | offset={meta.offset}")
                time.sleep(delay)

        producer.flush()
        print(f"\n[PRODUCER] Finished successfully! Total events published: {emitted_count}")

    except KeyboardInterrupt:
        print("\n[PRODUCER] Stream interrupted by user.")
    finally:
        producer.close()
        print("[PRODUCER] Kafka Producer closed cleanly.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Producer for World Happiness Stream")
    parser.add_argument('--topic', default=DEFAULT_TOPIC, help='Target Kafka topic')
    parser.add_argument('--bootstrap', default=DEFAULT_BOOTSTRAP, help='Kafka bootstrap servers')
    parser.add_argument('--delay', type=float, default=0.01, help='Inter-record delay in seconds')
    parser.add_argument('--max-records', type=int, default=None, help='Max records to stream')
    parser.add_argument('--no-errors', action='store_true', help='Disable fault injection')
    args = parser.parse_args()

    run_producer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap,
        delay=args.delay,
        max_records=args.max_records,
        inject_errors=not args.no_errors
    )
