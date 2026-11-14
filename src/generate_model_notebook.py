"""
Generator script for notebooks/model_training.ipynb
"""

import nbformat as nbf
import os

def create_model_notebook():
    nb = nbf.v4.new_notebook()
    cells = []

    # Cell 1: Header & Problem formulation
    cells.append(nbf.v4.new_markdown_cell("""# Workshop-3: Machine Learning Regression Pipeline
## Course: ETL (G01) - Data Engineering & AI
### Objectives:
1. Feature engineering and strict target leakage prevention.
2. Train-test split (70% training, 30% testing).
3. Build a reproducible `scikit-learn` Pipeline combining preprocessing (imputation, scaling) and regression.
4. Model evaluation on test data (MAE, RMSE, $R^2$).
5. Serialization to `models/model.pkl` for streaming inference.
"""))

    # Cell 2: Imports
    cells.append(nbf.v4.new_code_cell("""import os
import sys
import joblib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['figure.figsize'] = (10, 6)
print('Libraries loaded successfully.')
"""))

    # Cell 3: Load Data
    cells.append(nbf.v4.new_markdown_cell("""## 1. Load Harmonized Dataset
We load `../data/processed/happiness_unified.csv` created during the data harmonization stage.
"""))

    cells.append(nbf.v4.new_code_cell("""data_path = '../data/processed/happiness_unified.csv'
df = pd.read_csv(data_path)
print(f'Dataset Shape: {df.shape}')
df.head()
"""))

    # Cell 4: Feature Selection & Leakage
    cells.append(nbf.v4.new_markdown_cell("""## 2. Feature Selection & Target Leakage Prevention

### Features Selected:
- `gdp`: Economy (GDP per Capita)
- `family`: Social/family support score
- `health`: Healthy life expectancy
- `freedom`: Freedom to make life choices
- `generosity`: Generosity perception
- `corruption`: Absence of government corruption

### Features Excluded (Target Leakage):
- `Happiness Rank`: Computed directly from the target score. In production streaming, ranking is unavailable before score prediction.
- `Dystopia Residual`: Directly summed into the score formula ($Score = Factors + DystopiaResidual$).
- `Standard Error` / `Confidence Intervals`: Variances of the target measurement.
"""))

    cells.append(nbf.v4.new_code_cell("""FEATURE_COLS = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
TARGET_COL = 'actual_happiness_score'

X = df[FEATURE_COLS]
y = df[TARGET_COL]

print('Features shape:', X.shape)
print('Target shape:', y.shape)
"""))

    # Cell 5: Train Test Split
    cells.append(nbf.v4.new_markdown_cell("""## 3. Train-Test Split (70% Train / 30% Test)
We use a fixed random seed (`random_state=42`) to guarantee experimental reproducibility.
"""))

    cells.append(nbf.v4.new_code_cell("""X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)
print(f'Training set size: {X_train.shape[0]} records')
print(f'Testing set size:  {X_test.shape[0]} records')
"""))

    # Cell 6: Model Training & Evaluation
    cells.append(nbf.v4.new_markdown_cell("""## 4. Building the Scikit-Learn Pipeline
The pipeline bundles:
1. `SimpleImputer(strategy='median')`: Handles any potential missing numerical values at inference time.
2. `StandardScaler()`: Standardizes feature scales ($z = (x - \mu)/\sigma$).
3. `RandomForestRegressor()`: Non-linear ensemble model capturing complex interactions between economic and social factors.
"""))

    cells.append(nbf.v4.new_code_cell("""rf_pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler()),
    ('regressor', RandomForestRegressor(n_estimators=100, max_depth=10, min_samples_split=4, random_state=42))
])

rf_pipeline.fit(X_train, y_train)

# Predictions
y_train_pred = rf_pipeline.predict(X_train)
y_test_pred = rf_pipeline.predict(X_test)

# Metrics
train_mae = mean_absolute_error(y_train, y_train_pred)
train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
train_r2 = r2_score(y_train, y_train_pred)

test_mae = mean_absolute_error(y_test, y_test_pred)
test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
test_r2 = r2_score(y_test, y_test_pred)

metrics_df = pd.DataFrame({
    'Metric': ['MAE', 'RMSE', 'R2 Score'],
    'Train': [train_mae, train_rmse, train_r2],
    'Test': [test_mae, test_rmse, test_r2]
})
print(metrics_df.to_string(index=False))
"""))

    # Cell 7: Visualizations - Actual vs Predicted
    cells.append(nbf.v4.new_markdown_cell("""## 5. Model Evaluation Visualizations
"""))

    cells.append(nbf.v4.new_code_cell("""fig, ax = plt.subplots(figsize=(8, 8))
ax.scatter(y_test, y_test_pred, alpha=0.7, edgecolors='k', color='teal', label='Test Predictions')
min_val = min(y_test.min(), y_test_pred.min())
max_val = max(y_test.max(), y_test_pred.max())
ax.plot([min_val, max_val], [min_val, max_val], 'r--', lw=2, label='Perfect Fit Line (y=x)')
ax.set_title(f'Actual vs. Predicted Happiness Score (Test R2: {test_r2:.3f})', fontsize=14, fontweight='bold')
ax.set_xlabel('Actual Happiness Score')
ax.set_ylabel('Predicted Happiness Score')
ax.legend()
plt.tight_layout()
os.makedirs('../dashboards', exist_ok=True)
plt.savefig('../dashboards/actual_vs_predicted.png', dpi=300)
plt.show()
"""))

    # Cell 8: Residual Distribution
    cells.append(nbf.v4.new_code_cell("""residuals = y_test - y_test_pred
fig, ax = plt.subplots(figsize=(10, 5))
sns.histplot(residuals, kde=True, color='purple', bins=25, ax=ax)
ax.axvline(0, color='red', linestyle='--', label='Zero Error')
ax.set_title(f'Residual Distribution (Test MAE: {test_mae:.3f})', fontsize=14, fontweight='bold')
ax.set_xlabel('Prediction Error (Actual - Predicted)')
ax.legend()
plt.tight_layout()
plt.savefig('../dashboards/residual_distribution.png', dpi=300)
plt.show()
"""))

    # Cell 9: Feature Importances
    cells.append(nbf.v4.new_code_cell("""regressor = rf_pipeline.named_steps['regressor']
importances = regressor.feature_importances_
feat_imp = pd.Series(importances, index=FEATURE_COLS).sort_values(ascending=True)

fig, ax = plt.subplots(figsize=(10, 6))
feat_imp.plot(kind='barh', color='steelblue', ax=ax)
ax.set_title('Random Forest Feature Importance Analysis', fontsize=14, fontweight='bold')
ax.set_xlabel('Relative Importance')
plt.tight_layout()
plt.savefig('../dashboards/feature_importances.png', dpi=300)
plt.show()
"""))

    # Cell 10: Serialization & In-Memory Verification
    cells.append(nbf.v4.new_markdown_cell("""## 6. Pipeline Serialization & Real-Time Inference Verification
We serialize the entire pipeline (imputer + scaler + regressor) into `../models/model.pkl`.
"""))

    cells.append(nbf.v4.new_code_cell("""model_path = '../models/model.pkl'
os.makedirs(os.path.dirname(model_path), exist_ok=True)
joblib.dump(rf_pipeline, model_path)
print(f'Pipeline successfully serialized to: {model_path}')

# Load and test inference with sample record
loaded_pipeline = joblib.load(model_path)
sample_event = {
    'country': 'Colombia',
    'gdp': 1.2,
    'family': 0.8,
    'health': 0.9,
    'freedom': 0.6,
    'generosity': 0.3,
    'corruption': 0.1
}

input_df = pd.DataFrame([[
    sample_event['gdp'],
    sample_event['family'],
    sample_event['health'],
    sample_event['freedom'],
    sample_event['generosity'],
    sample_event['corruption']
]], columns=FEATURE_COLS)

predicted_val = loaded_pipeline.predict(input_df)[0]
print(f'Test prediction for {sample_event[\"country\"]}: {predicted_val:.4f}')
"""))

    nb.cells = cells
    nb_path = os.path.join('notebooks', 'model_training.ipynb')
    with open(nb_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    print(f'Generated {nb_path}')

if __name__ == '__main__':
    create_model_notebook()
