"""
Model Training & Serialization Pipeline
Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
Author: Mauricio Taborda

This script trains a scikit-learn regression pipeline on the harmonized happiness dataset,
evaluates performance metrics (MAE, RMSE, R2), prevents target leakage, and serializes
the end-to-end model pipeline into models/model.pkl.
"""

import os
import joblib
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


FEATURE_COLUMNS = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
TARGET_COLUMN = 'actual_happiness_score'


def get_project_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(current_dir, '..'))


def train_and_serialize_model(
    data_path=None,
    model_output_path=None,
    test_size=0.3,
    random_state=42
):
    root = get_project_root()
    if data_path is None:
        data_path = os.path.join(root, 'data', 'processed', 'happiness_unified.csv')
    if model_output_path is None:
        model_output_path = os.path.join(root, 'models', 'model.pkl')

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Processed dataset not found at {data_path}. Run harmonization first.")

    df = pd.read_csv(data_path)
    print(f"Loaded dataset from {data_path} (Shape: {df.shape})")

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    # Strictly reproducible 70% train / 30% test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    print(f"Split data into Train: {X_train.shape[0]} samples, Test: {X_test.shape[0]} samples")

    # Scikit-Learn Pipeline encapsulating imputation, scaling, and regression
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler()),
        ('regressor', RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=4,
            random_state=random_state
        ))
    ])

    print("Training Random Forest Regressor Pipeline...")
    pipeline.fit(X_train, y_train)

    # Evaluation
    train_preds = pipeline.predict(X_train)
    test_preds = pipeline.predict(X_test)

    train_mae = mean_absolute_error(y_train, train_preds)
    train_rmse = np.sqrt(mean_squared_error(y_train, train_preds))
    train_r2 = r2_score(y_train, train_preds)

    test_mae = mean_absolute_error(y_test, test_preds)
    test_rmse = np.sqrt(mean_squared_error(y_test, test_preds))
    test_r2 = r2_score(y_test, test_preds)

    metrics = {
        'train_mae': float(train_mae),
        'train_rmse': float(train_rmse),
        'train_r2': float(train_r2),
        'test_mae': float(test_mae),
        'test_rmse': float(test_rmse),
        'test_r2': float(test_r2)
    }

    print("\n================ MODEL EVALUATION METRICS ================")
    print(f"Train Metrics -> MAE: {train_mae:.4f} | RMSE: {train_rmse:.4f} | R2: {train_r2:.4f}")
    print(f"Test Metrics  -> MAE: {test_mae:.4f}  | RMSE: {test_rmse:.4f}  | R2: {test_r2:.4f}")
    print("==========================================================\n")

    # Serialize pipeline
    os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
    joblib.dump(pipeline, model_output_path)
    print(f"Successfully serialized pipeline to {model_output_path}")

    # Validate pipeline with sample event contract dict
    sample_event = {
        'event_id': 'TEST-001',
        'country': 'Colombia',
        'year': 2019,
        'gdp': 1.2,
        'family': 0.8,
        'health': 0.9,
        'freedom': 0.6,
        'generosity': 0.3,
        'corruption': 0.1,
        'actual_happiness_score': 6.2,
        'event_time': '2026-10-30T15:30:00'
    }
    pred_val = predict_single_event(pipeline, sample_event)
    print(f"Verification Test Prediction on sample event: {pred_val:.4f} (Actual: {sample_event['actual_happiness_score']})")

    return pipeline, metrics


def predict_single_event(model_or_path, event_dict):
    """
    Accepts a loaded pipeline (or path) and an event dictionary matching the Kafka Event Contract.
    Extracts features in exact required order and returns the float prediction.
    """
    if isinstance(model_or_path, str):
        pipeline = joblib.load(model_or_path)
    else:
        pipeline = model_or_path

    # Extract features in strict order
    feature_values = [[
        float(event_dict['gdp']),
        float(event_dict['family']),
        float(event_dict['health']),
        float(event_dict['freedom']),
        float(event_dict['generosity']),
        float(event_dict['corruption'])
    ]]
    feature_df = pd.DataFrame(feature_values, columns=FEATURE_COLUMNS)
    prediction = pipeline.predict(feature_df)[0]
    return float(prediction)


if __name__ == '__main__':
    train_and_serialize_model()
